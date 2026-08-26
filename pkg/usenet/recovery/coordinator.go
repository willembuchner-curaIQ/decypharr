package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
	"golang.org/x/sync/singleflight"
)

const (
	manifestFlushBatch          = uint64(32)
	maxAutomaticDownloadPercent = 25
)

// Policy bounds all automatic recovery activity. When neither download bound
// is positive, metadata/patch reuse remains available but NNTP BODY is denied.
type Policy struct {
	Enabled            bool
	MaxDownloadPercent int
	MaxDownloadBytes   int64
	MaxStorageBytes    int64
}

// FetchFunc is the testable NNTP boundary. Production callers normally omit
// WithFetchFunc and let the coordinator use Client.ExecuteWithFailover.
type FetchFunc func(context.Context, string) ([]byte, *nntp.YencMetadata, error)

// RecoverBatchOptions controls background reconstruction resource use.
type RecoverBatchOptions struct {
	NumGoroutines int
}

// RecoveredArticle is one batch item's durable reconstruction result.
type RecoveredArticle struct {
	Bytes int
	Err   error
}

// RecoverBatchResult reports per-article outcomes and the shared traffic cost.
type RecoverBatchResult struct {
	Articles             []RecoveredArticle
	ModeledDownloadBytes int64
}

type CoordinatorOption func(*Coordinator) error

func WithFetchFunc(fetch FetchFunc) CoordinatorOption {
	return func(c *Coordinator) error {
		if fetch == nil {
			return &ValidationError{Field: "recovery fetch function", Reason: "must not be nil"}
		}
		c.fetch = fetch
		c.backgroundFetch = fetch
		return nil
	}
}

// RepairFailureStats assigns every completed failed repair attempt to one
// primary reason.
type RepairFailureStats struct {
	Canceled               uint64 `json:"canceled"`
	DeadlineExceeded       uint64 `json:"deadline_exceeded"`
	DownloadBudgetExceeded uint64 `json:"download_budget_exceeded"`
	StorageBudgetExceeded  uint64 `json:"storage_budget_exceeded"`
	UnboundedTraffic       uint64 `json:"unbounded_traffic"`
	NoRecoverySet          uint64 `json:"no_recovery_set"`
	LayoutUnavailable      uint64 `json:"layout_unavailable"`
	AmbiguousMapping       uint64 `json:"ambiguous_mapping"`
	ArticleNotFound        uint64 `json:"article_not_found"`
	ProviderUnavailable    uint64 `json:"provider_unavailable"`
	ProviderRejected       uint64 `json:"provider_rejected"`
	CorruptData            uint64 `json:"corrupt_data"`
	Unsupported            uint64 `json:"unsupported"`
	Other                  uint64 `json:"other"`
}

// CoordinatorStats is a monotonic process-lifetime snapshot. Modeled download
// bytes are NZB posted bytes reserved before BODY, not decoded payload bytes.
type CoordinatorStats struct {
	RepairAttempts       uint64             `json:"repair_attempts"`
	RepairSuccesses      uint64             `json:"repair_successes"`
	RepairFailures       uint64             `json:"repair_failures"`
	FailureReasons       RepairFailureStats `json:"failure_reasons"`
	PatchHits            uint64             `json:"patch_hits"`
	Observations         uint64             `json:"observations"`
	ManifestFlushes      uint64             `json:"manifest_flushes"`
	BODYCalls            uint64             `json:"body_calls"`
	ModeledDownloadBytes uint64             `json:"modeled_download_bytes"`
	RecoveryPayloadBytes uint64             `json:"recovery_payload_bytes"`
	PatchBytes           uint64             `json:"patch_bytes"`
	LocalSourceBytes     uint64             `json:"local_source_bytes"`
	Store                Stats              `json:"store"`
}

type coordinatorCounters struct {
	repairAttempts       atomic.Uint64
	repairSuccesses      atomic.Uint64
	repairFailures       atomic.Uint64
	failureReasons       repairFailureCounters
	patchHits            atomic.Uint64
	observations         atomic.Uint64
	manifestFlushes      atomic.Uint64
	bodyCalls            atomic.Uint64
	modeledDownloadBytes atomic.Uint64
	recoveryPayloadBytes atomic.Uint64
	patchBytes           atomic.Uint64
	localSourceBytes     atomic.Uint64
}

type repairFailureCounters struct {
	canceled               atomic.Uint64
	deadlineExceeded       atomic.Uint64
	downloadBudgetExceeded atomic.Uint64
	storageBudgetExceeded  atomic.Uint64
	unboundedTraffic       atomic.Uint64
	noRecoverySet          atomic.Uint64
	layoutUnavailable      atomic.Uint64
	ambiguousMapping       atomic.Uint64
	articleNotFound        atomic.Uint64
	providerUnavailable    atomic.Uint64
	providerRejected       atomic.Uint64
	corruptData            atomic.Uint64
	unsupported            atomic.Uint64
	other                  atomic.Uint64
}

type repairFailureReason uint8

const (
	repairFailureOther repairFailureReason = iota
	repairFailureCanceled
	repairFailureDeadlineExceeded
	repairFailureDownloadBudgetExceeded
	repairFailureStorageBudgetExceeded
	repairFailureUnboundedTraffic
	repairFailureNoRecoverySet
	repairFailureLayoutUnavailable
	repairFailureAmbiguousMapping
	repairFailureArticleNotFound
	repairFailureProviderUnavailable
	repairFailureProviderRejected
	repairFailureCorruptData
	repairFailureUnsupported
)

func classifyRepairFailure(err error) repairFailureReason {
	switch {
	case errors.Is(err, context.Canceled):
		return repairFailureCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return repairFailureDeadlineExceeded
	case errors.Is(err, ErrBudgetExceeded):
		return repairFailureDownloadBudgetExceeded
	case errors.Is(err, ErrStorageBudget):
		return repairFailureStorageBudgetExceeded
	case errors.Is(err, ErrUnboundedTraffic):
		return repairFailureUnboundedTraffic
	case errors.Is(err, ErrNoRecoverySet), errors.Is(err, par2.ErrNotEnoughRecovery), errors.Is(err, par2.ErrSingularSelection):
		return repairFailureNoRecoverySet
	case errors.Is(err, ErrLayoutUnavailable):
		return repairFailureLayoutUnavailable
	case errors.Is(err, ErrAmbiguousMapping):
		return repairFailureAmbiguousMapping
	case errors.Is(err, ErrCorrupt), errors.Is(err, ErrChecksumMismatch),
		errors.Is(err, par2.ErrInvalidMagic), errors.Is(err, par2.ErrInvalidLength),
		errors.Is(err, par2.ErrPacketTooLarge), errors.Is(err, par2.ErrPacketHash),
		errors.Is(err, par2.ErrTruncated), errors.Is(err, par2.ErrInvalidPacket):
		return repairFailureCorruptData
	case errors.Is(err, ErrLegacyManifestUnsupported), errors.Is(err, ErrUnsupported), errors.Is(err, ErrInvalid),
		errors.Is(err, par2.ErrInvalidPlan), errors.Is(err, par2.ErrInvalidRange), errors.Is(err, par2.ErrUnsafePath):
		return repairFailureUnsupported
	}

	if nntpErr, ok := errors.AsType[*nntp.Error](err); ok {
		switch nntpErr.Type {
		case nntp.ErrorTypeArticleNotFound:
			return repairFailureArticleNotFound
		case nntp.ErrorTypeConnection, nntp.ErrorTypeTimeout, nntp.ErrorTypeServerBusy, nntp.ErrorTypeNoAvailableConnection:
			return repairFailureProviderUnavailable
		case nntp.ErrorTypeAuthentication, nntp.ErrorTypePermissionDenied, nntp.ErrorTypeGroupNotFound:
			return repairFailureProviderRejected
		case nntp.ErrorTypeYencDecode:
			return repairFailureCorruptData
		default:
			return repairFailureOther
		}
	}
	return repairFailureOther
}

func (c *coordinatorCounters) recordRepairResult(err error) {
	if err == nil {
		c.repairSuccesses.Add(1)
		return
	}
	c.repairFailures.Add(1)
	c.failureReasons.add(classifyRepairFailure(err))
}

func (c *repairFailureCounters) add(reason repairFailureReason) {
	switch reason {
	case repairFailureCanceled:
		c.canceled.Add(1)
	case repairFailureDeadlineExceeded:
		c.deadlineExceeded.Add(1)
	case repairFailureDownloadBudgetExceeded:
		c.downloadBudgetExceeded.Add(1)
	case repairFailureStorageBudgetExceeded:
		c.storageBudgetExceeded.Add(1)
	case repairFailureUnboundedTraffic:
		c.unboundedTraffic.Add(1)
	case repairFailureNoRecoverySet:
		c.noRecoverySet.Add(1)
	case repairFailureLayoutUnavailable:
		c.layoutUnavailable.Add(1)
	case repairFailureAmbiguousMapping:
		c.ambiguousMapping.Add(1)
	case repairFailureArticleNotFound:
		c.articleNotFound.Add(1)
	case repairFailureProviderUnavailable:
		c.providerUnavailable.Add(1)
	case repairFailureProviderRejected:
		c.providerRejected.Add(1)
	case repairFailureCorruptData:
		c.corruptData.Add(1)
	case repairFailureUnsupported:
		c.unsupported.Add(1)
	default:
		c.other.Add(1)
	}
}

func (c *repairFailureCounters) stats() RepairFailureStats {
	return RepairFailureStats{
		Canceled: c.canceled.Load(), DeadlineExceeded: c.deadlineExceeded.Load(),
		DownloadBudgetExceeded: c.downloadBudgetExceeded.Load(), StorageBudgetExceeded: c.storageBudgetExceeded.Load(),
		UnboundedTraffic: c.unboundedTraffic.Load(), NoRecoverySet: c.noRecoverySet.Load(),
		LayoutUnavailable: c.layoutUnavailable.Load(), AmbiguousMapping: c.ambiguousMapping.Load(),
		ArticleNotFound: c.articleNotFound.Load(), ProviderUnavailable: c.providerUnavailable.Load(),
		ProviderRejected: c.providerRejected.Load(), CorruptData: c.corruptData.Load(),
		Unsupported: c.unsupported.Load(), Other: c.other.Load(),
	}
}

type manifestState struct {
	manifest *Manifest
	dirty    uint64
	flushed  uint64
}

// Coordinator performs narrowly-scoped, on-demand reconstruction. It does
// not own the Store or NNTP Client; Close only flushes its manifest updates.
type Coordinator struct {
	store           *Store
	client          *nntp.Client
	policy          Policy
	fetch           FetchFunc
	backgroundFetch FetchFunc

	mu        sync.Mutex
	lifecycle sync.RWMutex
	closed    bool
	manifests map[string]*manifestState
	flights   singleflight.Group
	// operationLocks serializes different repair ranges within one NZB. This
	// prevents duplicate parity downloads and parsed-set read/modify/write
	// races while preserving concurrency across unrelated NZBs.
	operationLocks sync.Map // map[string]*sync.Mutex
	counters       coordinatorCounters
	storageMu      sync.Mutex
	// storageBase is the on-disk footprint observed at construction;
	// storageReserved monotonically accounts for this coordinator's writes.
	// Replacement records may be over-counted, which is safely conservative.
	storageBase, storageReserved int64
}

var (
	_ reader.ArticleRecovery            = (*Coordinator)(nil)
	_ reader.SourceAwareArticleRecovery = (*Coordinator)(nil)
)

func NewCoordinator(store *Store, client *nntp.Client, policy Policy, options ...CoordinatorOption) (*Coordinator, error) {
	if store == nil {
		return nil, &ValidationError{Field: "recovery store", Reason: "must not be nil"}
	}
	if policy.MaxDownloadPercent < 0 || policy.MaxDownloadPercent > maxAutomaticDownloadPercent {
		return nil, &ValidationError{Field: "maximum PAR2 download percent", Reason: "must be between 0 and 25"}
	}
	if policy.MaxDownloadBytes < 0 || policy.MaxStorageBytes < 0 {
		return nil, &ValidationError{Field: "PAR2 byte limits", Reason: "must not be negative"}
	}
	c := &Coordinator{store: store, client: client, policy: policy, manifests: make(map[string]*manifestState), storageBase: store.Stats().DiskBytes}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(c); err != nil {
			return nil, err
		}
	}
	if c.fetch == nil && client != nil {
		c.fetch = func(ctx context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
			var body []byte
			var metadata *nntp.YencMetadata
			err := client.ExecuteWithFailover(ctx, func(connection *nntp.Connection) error {
				var err error
				body, metadata, err = connection.GetDecodedBodyWithMetadata(messageID)
				return err
			})
			return body, metadata, err
		}
	}
	if c.backgroundFetch == nil && client != nil {
		c.backgroundFetch = func(ctx context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
			var body []byte
			var metadata *nntp.YencMetadata
			err := client.ExecuteBackgroundWithFailover(ctx, func(connection *nntp.Connection) error {
				var err error
				body, metadata, err = connection.GetDecodedBodyWithMetadata(messageID)
				return err
			})
			return body, metadata, err
		}
	}
	return c, nil
}

// RegisterManifest durably records the raw NZB topology before logical files
// can request recovery. The coordinator retains the synchronized Manifest
// pointer so parser metadata learned during the same import is immediately
// visible; callers re-register after processing to persist that enrichment.
func (c *Coordinator) RegisterManifest(manifest *Manifest) error {
	if c == nil || c.store == nil {
		return ErrStoreClosed
	}
	if manifest == nil {
		return &ValidationError{Field: "manifest", Reason: "must not be nil"}
	}
	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	c.mu.Lock()
	closed := c.closed
	state := c.manifests[manifest.NZBID]
	var persistedGeneration uint64
	if state != nil {
		persistedGeneration = state.dirty
	}
	c.mu.Unlock()
	if closed {
		return ErrStoreClosed
	}
	// The exact encoded size is deliberately internal to Store. This
	// conservative topology estimate prevents a manifest from bypassing a
	// configured storage cap while avoiding duplicate encoding here.
	estimated := manifestStorageEstimate(manifest)
	if err := c.checkStorage(estimated); err != nil {
		return err
	}
	if err := c.store.PutManifest(manifest); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrStoreClosed
	}
	if state := c.manifests[manifest.NZBID]; state != nil {
		state.manifest = manifest
		if persistedGeneration > state.flushed {
			state.flushed = persistedGeneration
		}
	} else {
		c.manifests[manifest.NZBID] = &manifestState{manifest: manifest}
	}
	return nil
}

func storedSetStorageEstimate(set StoredSet, aliases map[FileID][]RawFileKey) int64 {
	estimated := int64(256)
	for _, source := range set.Files {
		estimated = saturatingAddInt64(estimated, int64(192+len(source.Name)))
		estimated = saturatingAddInt64(estimated, saturatingMulInt64(int64(len(source.SliceChecksums)), 32))
		estimated = saturatingAddInt64(estimated, saturatingMulInt64(int64(len(aliases[source.FileID])), 8))
	}
	estimated = saturatingAddInt64(estimated, saturatingMulInt64(int64(len(set.Recovery)), 96))
	return estimated
}

func manifestStorageEstimate(manifest *Manifest) int64 {
	estimated := int64(256)
	manifest.mu.RLock()
	defer manifest.mu.RUnlock()
	estimated = saturatingAddInt64(estimated, int64(len(manifest.NZBID)+len(manifest.Name)))
	for i := range manifest.Files {
		file := &manifest.Files[i]
		estimated = saturatingAddInt64(estimated, 160)
		estimated = saturatingAddInt64(estimated, int64(len(file.Subject)+len(file.SubjectFilename)+len(file.BaseFilename)+len(file.ActualFilename)+len(file.DetectedType)))
		for _, group := range file.Groups {
			estimated = saturatingAddInt64(estimated, int64(len(group)+16))
		}
		for _, article := range file.Articles {
			estimated = saturatingAddInt64(estimated, int64(len(article.MessageID)+96))
		}
	}
	return estimated
}

func saturatingMulInt64(left, right int64) int64 {
	if left < 0 || right < 0 || (right != 0 && left > math.MaxInt64/right) {
		return math.MaxInt64
	}
	return left * right
}

// ObserveArticle learns only exact yEnc topology. body is never retained or
// modified; its length is used to reject inconsistent metadata.
func (c *Coordinator) ObserveArticle(_ context.Context, nzbID string, segment reader.SegmentMeta, body []byte, metadata *nntp.YencMetadata) {
	if c == nil || nzbID == "" || metadata == nil {
		return
	}
	if metadata.Offset < 0 || metadata.PartSize <= 0 || (body != nil && metadata.PartSize != int64(len(body))) {
		return
	}
	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	manifest, err := c.manifest(nzbID)
	if err != nil {
		return
	}
	key := RawFileKey(segment.RawFileKey)
	if key == 0 {
		if file, _, ok := manifest.FindArticle(segment.MessageID); ok {
			key = file.Key
		}
	}
	if key == 0 {
		return
	}
	manifest.UpdateArticleLayout(key, segment.Number, metadata.Offset, metadata.PartSize, LayoutExact)
	if metadata.Name != "" {
		manifest.UpdateClassification(key, detectedType(metadata.Name), metadata.Name, isPAR2Filename(metadata.Name))
	}
	c.counters.observations.Add(1)
	c.markManifestDirty(nzbID)
}

func (c *Coordinator) RecoverArticle(ctx context.Context, nzbID string, segment reader.SegmentMeta) ([]byte, error) {
	return c.recoverArticleWithSource(ctx, nzbID, segment, nil)
}

// RecoverArticleWithSource reuses verified ranges already resident in the
// caller's active stream cache. Local ranges are removed from the modeled
// NNTP plan before its atomic budget reservation.
func (c *Coordinator) RecoverArticleWithSource(ctx context.Context, nzbID string, segment reader.SegmentMeta, source reader.ArticleRangeSource) ([]byte, error) {
	return c.recoverArticleWithSource(ctx, nzbID, segment, source)
}

func (c *Coordinator) recoverArticleWithSource(ctx context.Context, nzbID string, segment reader.SegmentMeta, source reader.ArticleRangeSource) ([]byte, error) {
	if c == nil || c.store == nil {
		return nil, ErrStoreClosed
	}
	if !c.policy.Enabled {
		return nil, ErrRecoveryDisabled
	}
	if err := validateRecoverySegment(segment); err != nil {
		return nil, err
	}
	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrStoreClosed
	}
	key := fmt.Sprintf("%s/%d/%d/%d/%d", nzbID, segment.RawFileKey, segment.RawOffset, segment.RawLength, segment.SegmentDataStart)
	value, err, _ := c.flights.Do(key, func() (any, error) {
		operationLock := c.operationLock(nzbID)
		operationLock.Lock()
		defer operationLock.Unlock()
		manifest, err := c.manifest(nzbID)
		if err != nil {
			return nil, err
		}
		op := &repairOperation{
			coordinator: c, ctx: ctx, nzbID: nzbID, manifest: manifest,
			meter: newTrafficMeter(c.policy, manifest), articleCache: make(map[string]fetchedArticle),
			reserved: make(map[string]bool), parsedRaw: make(map[RawFileKey]bool), aliases: make(map[FileID][]RawFileKey),
			local: source, fetch: c.fetch,
		}
		c.counters.repairAttempts.Add(1)
		data, recoverErr := op.recoverArticle(segment)
		c.counters.recordRepairResult(recoverErr)
		return data, recoverErr
	})
	if err != nil {
		return nil, err
	}
	// singleflight shares values between callers; each gets owned memory.
	return bytes.Clone(value.([]byte)), nil
}

// RecoverArticles repairs several known-missing ranges under one NZB-wide
// traffic budget and routes network reads through the background repair pool.
func (c *Coordinator) RecoverArticles(ctx context.Context, nzbID string, segments []reader.SegmentMeta, options RecoverBatchOptions) (RecoverBatchResult, error) {
	result := RecoverBatchResult{Articles: make([]RecoveredArticle, 0, len(segments))}
	if c == nil || c.store == nil {
		return result, ErrStoreClosed
	}
	if !c.policy.Enabled {
		return result, ErrRecoveryDisabled
	}
	if options.NumGoroutines < 0 {
		return result, &ValidationError{Field: "PAR2 recovery goroutines", Reason: "must not be negative"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.lifecycle.RLock()
	defer c.lifecycle.RUnlock()
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return result, ErrStoreClosed
	}
	manifest, err := c.manifest(nzbID)
	if err != nil {
		return result, err
	}

	meter := newTrafficMeter(c.policy, manifest)
	articleCache := make(map[string]fetchedArticle)
	reserved := make(map[string]bool)
	knownMissing := make(map[string]struct{}, len(segments))
	for _, segment := range segments {
		if segment.MessageID != "" {
			knownMissing[segment.MessageID] = struct{}{}
		}
	}
	parsedRaw := make(map[RawFileKey]bool)
	aliases := make(map[FileID][]RawFileKey)
	operationLock := c.operationLock(nzbID)
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			result.ModeledDownloadBytes = meter.used
			return result, err
		}
		if err := validateRecoverySegment(segment); err != nil {
			result.Articles = append(result.Articles, RecoveredArticle{Err: err})
			continue
		}
		op := &repairOperation{
			coordinator: c, ctx: ctx, nzbID: nzbID, manifest: manifest,
			meter: meter, articleCache: articleCache, reserved: reserved, knownMissing: knownMissing,
			parsedRaw: parsedRaw, aliases: aliases, fetch: c.backgroundFetch,
			numWorkers: options.NumGoroutines,
		}
		operationLock.Lock()
		c.counters.repairAttempts.Add(1)
		data, recoverErr := op.recoverArticle(segment)
		c.counters.recordRepairResult(recoverErr)
		operationLock.Unlock()
		result.Articles = append(result.Articles, RecoveredArticle{Bytes: len(data), Err: recoverErr})
	}
	result.ModeledDownloadBytes = meter.used
	return result, nil
}

func validateRecoverySegment(segment reader.SegmentMeta) error {
	if segment.RawFileKey == 0 {
		return &LayoutError{Reason: "logical segment has no raw-file origin"}
	}
	if segment.RawOffset < 0 || segment.RawLength <= 0 || segment.SegmentDataStart < 0 {
		return &LayoutError{RawFile: RawFileKey(segment.RawFileKey), Offset: segment.RawOffset, Length: segment.RawLength, Reason: "invalid raw article range"}
	}
	if segment.RawOffset > math.MaxInt64-segment.RawLength || segment.RawLength > int64(maxInt()) || segment.SegmentDataStart > int64(maxInt())-segment.RawLength {
		return &LayoutError{RawFile: RawFileKey(segment.RawFileKey), Offset: segment.RawOffset, Length: segment.RawLength, Reason: "raw article-shaped range exceeds addressable memory"}
	}
	return nil
}

func (c *Coordinator) operationLock(nzbID string) *sync.Mutex {
	lock, _ := c.operationLocks.LoadOrStore(nzbID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (c *Coordinator) Close() error {
	if c == nil {
		return nil
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ids := make([]string, 0, len(c.manifests))
	for id, state := range c.manifests {
		if state.dirty != state.flushed {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	var result error
	for _, id := range ids {
		result = errors.Join(result, c.flushManifest(id, true))
	}
	result = errors.Join(result, c.store.Sync())
	return result
}

// DeleteNZB waits for in-flight recovery writes, invalidates coordinator-owned
// manifest state, and then deletes every durable recovery record. This keeps a
// completing repair or later Close from resurrecting deleted state.
func (c *Coordinator) DeleteNZB(nzbID string) error {
	if c == nil || c.store == nil {
		return ErrStoreClosed
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrStoreClosed
	}
	delete(c.manifests, nzbID)
	c.mu.Unlock()
	err := c.store.DeleteNZB(nzbID)
	c.operationLocks.Delete(nzbID)
	return err
}

func (c *Coordinator) Stats() CoordinatorStats {
	if c == nil {
		return CoordinatorStats{}
	}
	return CoordinatorStats{
		RepairAttempts: c.counters.repairAttempts.Load(), RepairSuccesses: c.counters.repairSuccesses.Load(),
		RepairFailures: c.counters.repairFailures.Load(), FailureReasons: c.counters.failureReasons.stats(),
		PatchHits: c.counters.patchHits.Load(), Observations: c.counters.observations.Load(),
		ManifestFlushes: c.counters.manifestFlushes.Load(), BODYCalls: c.counters.bodyCalls.Load(),
		ModeledDownloadBytes: c.counters.modeledDownloadBytes.Load(),
		RecoveryPayloadBytes: c.counters.recoveryPayloadBytes.Load(), PatchBytes: c.counters.patchBytes.Load(),
		LocalSourceBytes: c.counters.localSourceBytes.Load(),
		Store:            c.store.Stats(),
	}
}

func (c *Coordinator) manifest(nzbID string) (*Manifest, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrStoreClosed
	}
	if state := c.manifests[nzbID]; state != nil {
		manifest := state.manifest
		c.mu.Unlock()
		return manifest, nil
	}
	c.mu.Unlock()
	manifest, err := c.store.GetManifest(nzbID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if existing := c.manifests[nzbID]; existing != nil {
		manifest = existing.manifest
	} else {
		c.manifests[nzbID] = &manifestState{manifest: manifest}
	}
	c.mu.Unlock()
	return manifest, nil
}

func (c *Coordinator) markManifestDirty(nzbID string) {
	c.mu.Lock()
	state := c.manifests[nzbID]
	if state != nil {
		state.dirty++
		shouldFlush := state.dirty-state.flushed >= manifestFlushBatch && !c.closed
		c.mu.Unlock()
		if shouldFlush {
			_ = c.flushManifest(nzbID, false)
		}
		return
	}
	c.mu.Unlock()
}

func (c *Coordinator) flushManifest(nzbID string, allowClosed bool) error {
	c.mu.Lock()
	state := c.manifests[nzbID]
	if state == nil || state.dirty == state.flushed {
		c.mu.Unlock()
		return nil
	}
	if c.closed && !allowClosed {
		c.mu.Unlock()
		return ErrStoreClosed
	}
	manifest, generation := state.manifest, state.dirty
	c.mu.Unlock()
	if err := c.checkStorage(manifestStorageEstimate(manifest)); err != nil {
		return err
	}
	if err := c.store.PutManifest(manifest); err != nil {
		return err
	}
	c.mu.Lock()
	if state := c.manifests[nzbID]; state != nil && generation > state.flushed {
		state.flushed = generation
	}
	c.mu.Unlock()
	c.counters.manifestFlushes.Add(1)
	return nil
}

func (c *Coordinator) checkStorage(requested int64) error {
	if requested < 0 {
		return &ValidationError{Field: "recovery storage write", Reason: "negative size"}
	}
	// Zero is intentionally unlimited at this layer. Application defaults
	// always pass an explicit cap; tests and embedders may opt out with zero.
	if c.policy.MaxStorageBytes == 0 {
		return nil
	}
	// Reserve framing/index headroom and serialize check+reservation so two
	// repairs cannot both pass against the same remaining capacity.
	if requested > math.MaxInt64-256 {
		requested = math.MaxInt64
	} else {
		requested += 256
	}
	c.storageMu.Lock()
	defer c.storageMu.Unlock()
	used := c.storageBase + c.storageReserved
	if requested > c.policy.MaxStorageBytes-used {
		return &StorageBudgetError{Limit: c.policy.MaxStorageBytes, Used: used, Requested: requested}
	}
	c.storageReserved += requested
	return nil
}

func isPAR2Filename(name string) bool {
	return strings.EqualFold(filepath.Ext(strings.TrimSpace(name)), ".par2")
}

func detectedType(name string) string {
	if isPAR2Filename(name) {
		return "par2"
	}
	return strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), ".")
}

type trafficMeter struct {
	limit int64
	used  int64
}

func newTrafficMeter(policy Policy, manifest *Manifest) *trafficMeter {
	var total int64
	byMessageID := make(map[string]int64)
	manifest.mu.RLock()
	for i := range manifest.Files {
		hasPositiveArticle := false
		for _, article := range manifest.Files[i].Articles {
			if article.PostedBytes <= 0 {
				continue
			}
			hasPositiveArticle = true
			if article.MessageID == "" {
				total = saturatingAddInt64(total, article.PostedBytes)
				continue
			}
			if article.PostedBytes > byMessageID[article.MessageID] {
				byMessageID[article.MessageID] = article.PostedBytes
			}
		}
		if !hasPositiveArticle && manifest.Files[i].PostedBytes > 0 {
			total = saturatingAddInt64(total, manifest.Files[i].PostedBytes)
		}
	}
	manifest.mu.RUnlock()
	for _, posted := range byMessageID {
		total = saturatingAddInt64(total, posted)
	}
	var percentLimit int64
	if policy.MaxDownloadPercent > 0 && total > 0 {
		percentLimit = total / 100 * int64(policy.MaxDownloadPercent)
		percentLimit += total % 100 * int64(policy.MaxDownloadPercent) / 100
	}
	limit := percentLimit
	if policy.MaxDownloadBytes > 0 && (limit == 0 || policy.MaxDownloadBytes < limit) {
		limit = policy.MaxDownloadBytes
	}
	return &trafficMeter{limit: limit}
}

func saturatingAddInt64(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func (m *trafficMeter) reserve(rawFile RawFileKey, article Article) error {
	if article.PostedBytes <= 0 {
		return &UnboundedTrafficError{RawFile: rawFile, MessageID: article.MessageID}
	}
	if m.limit <= 0 || article.PostedBytes > m.limit-m.used {
		return &BudgetExceededError{Limit: m.limit, Used: m.used, Requested: article.PostedBytes}
	}
	m.used += article.PostedBytes
	return nil
}

func (m *trafficMeter) reserveTotal(requested int64) error {
	if requested < 0 {
		return &UnboundedTrafficError{}
	}
	if m.limit <= 0 || requested > m.limit-m.used {
		return &BudgetExceededError{Limit: m.limit, Used: m.used, Requested: requested}
	}
	m.used += requested
	return nil
}
