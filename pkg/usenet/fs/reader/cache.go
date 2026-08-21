package reader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/buffer"
)

// SegmentCache is a usenet-segment-aware view over a buffer.Buffer.
//
// The storage layer is the buffer: RAM blocks in memory mode (the default),
// a sparse disk file plus page-cache discipline and hole punching in disk
// mode, and in both, the bookkeeping for what bytes are present.
// SegmentCache adds the usenet-specific policy on top:
//
//   - State machine per segment (Empty / Fetching / OnDisk / Evicting /
//     Failed)
//   - Pin counts so in-flight reads rarely race eviction (advisory — see
//     PinRange)
//   - Per-segment access timestamps and the consumed-floor high-water mark
//     driving the sliding-window sweeper
//   - Disk mode only: a hard budget drain on top of the proactive sweeper
//
// All the actual byte movement (write, read, discard) goes through the
// buffer. That's the entire integration boundary.
type SegmentCache struct {
	// Segment metadata
	segments   []SegmentMeta
	segCount   int
	segOffsets []int64 // cumulative byte offsets for binary-search lookup
	totalSize  int64
	segLengths []atomic.Int64 // bytes actually stored per segment

	// Per-segment state
	states     []atomic.Uint32
	pinCounts  []atomic.Int32
	errors     []atomic.Pointer[error]
	accessTime []atomic.Int64

	// Storage layer.
	buf        *buffer.Buffer
	memoryMode bool // RAM-backed buffer (the default); false = buffer_to_disk

	// memBudget is this stream's own RAM ceiling in memory mode (0 in disk
	// mode); segBytesHint is the nominal decoded size of one segment. Both
	// feed MaxPrefetchSegments.
	memBudget    int64
	segBytesHint int64
	diskPath     string // remembered for RemoveAll on Close

	// cachedBytes tracks the bytes currently stored across all OnDisk
	// segments (RAM blocks in memory mode, file bytes in disk mode).
	// diskBudget bounds it in disk mode via drainOverBudget; in memory mode
	// it is 0 (the buffer's own inline drop-oldest is the RAM bound) and
	// cachedBytes is observability only.
	diskBudget     int64
	cachedBytes    atomic.Int64
	maintainSignal chan struct{}
	evictMu        sync.Mutex // serializes hard-budget scans and hole punching
	evictCursor    int        // findEvictableBatch wrap-once scan position; under evictMu
	maintainWg     sync.WaitGroup

	// Sliding-window state: the slowest active consumer's delivered offset
	// (see SetConsumedFloor and sweepWindow). backWindow and retentionAge
	// are fixed at construction per mode — memory mode derives the window
	// from its RAM budget so the sweeper reclaims BEFORE the budget fills
	// (see NewSegmentCache).
	consumedFloor atomic.Int64
	backWindow    int64
	retentionAge  time.Duration
	// sweepCursor skips the already-evicted prefix; reset when the cutoff
	// regresses (a seek-back may re-fetch behind it). Only touched by the
	// maintainLoop goroutine.
	sweepCursor     int
	sweepLastCutoff int64

	// Sharded waiters: readers blocking on WaitForSegment park on one of
	// numShards condition variables to avoid global wakeup storms.
	shardMu   [numShards]sync.Mutex
	shardCond [numShards]*sync.Cond

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	logger zerolog.Logger

	stats *ReaderStats
}

const (
	numShards = 64
	shardMask = numShards - 1

	// Sliding-window eviction tunables. Hardcoded — the cache is internal
	// temporary storage; exposing knobs invites mis-tuning.
	//
	// backWindowBytes keeps a generous slice of recently-played history
	// pinned so brief scrub-back gestures don't trigger a re-fetch. ~170
	// segments at 750 KB each ≈ 25 s of 1080p / 12 s of 4K — covers
	// typical "10 second rewind" buttons in media players with margin.
	// Disk mode uses it as-is; memory mode caps it at half the RAM budget
	// so the sweeper reclaims before the budget fills (see NewSegmentCache).
	backWindowBytes = 128 << 20

	// segmentMinRetentionAge is the minimum time a segment must go unread
	// (reads refresh the clock — see ReadRangeInto) before it is eligible
	// for window-based eviction. Defends the pause-and-resume case: even if
	// a segment is technically "behind" the last delivered offset, a player
	// still drawing from the same area keeps it alive. Disk mode only;
	// memory mode runs with no age gate because its RAM budget, not time,
	// is the binding constraint.
	segmentMinRetentionAge = 30 * time.Second

	// segmentSweepInterval is how often the proactive sliding-window
	// evictor wakes.
	segmentSweepInterval = 5 * time.Second

	// segmentSweepBatch caps how many segments a single sweep evicts so
	// a large jump in playback position doesn't punch holes for thousands
	// of segments in one burst. Sweeps are cheap; the next tick picks up
	// the rest.
	segmentSweepBatch = 128

	// pressureBackWindowBytes is the back-window a memory-mode sweep keeps
	// while the shared pool is nearly exhausted (poolMemoryPressured):
	// enough for a token scrub-back, small enough that idle streams and
	// played-out RAR volumes donate their history back to the pool.
	pressureBackWindowBytes = 4 << 20
)

// memoryWindowSize sizes the memory-mode cache budget per stream from the
// configured read-ahead: 1× the prefetch window ahead of playback plus 2×
// for the history behind it. It also returns the prefetch window in bytes so
// the caller can reserve it when deriving the sweep back-window. A stream
// whose live window outgrows the budget sheds cached segments (they re-fetch
// on demand) — the disk file is never a fallback. Clamped to [floor,
// MaxDisk]; the floor is 32MB for streaming readers and 8MB for probe-style
// readers (read-ahead disabled, PrefetchAhead 0), which touch small
// scattered ranges and would otherwise be pure pool pressure — library
// scans and imports open many at once.
func memoryWindowSize(cfg Config, segments []SegmentMeta) (budget, ahead int64) {
	floor := int64(32 << 20)
	if cfg.PrefetchAhead <= 0 {
		floor = 8 << 20
	}
	segBytes := int64(750 * 1024) // typical usenet segment, same fallback as PrefetchAheadSegments
	if len(segments) > 0 && segments[0].Bytes > 0 {
		segBytes = segments[0].Bytes
	}
	ahead = int64(cfg.PrefetchAhead) * segBytes
	budget = 3 * ahead
	if budget < floor {
		budget = floor
	}
	if cfg.MaxDisk > 0 && budget > cfg.MaxDisk {
		budget = cfg.MaxDisk
	}
	return budget, ahead
}

// NewSegmentCache creates a new segment cache backed by a freshly-created
// buffer.Buffer: RAM blocks in memory mode (the default), or a sparse disk
// file under config.DiskPath (or a temp dir) when config buffer_to_disk is
// set.
func NewSegmentCache(
	ctx context.Context,
	segments []SegmentMeta,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) (*SegmentCache, error) {
	ctx, cancel := context.WithCancel(ctx)
	segCount := len(segments)

	offsets := computeOffsets(segments)
	totalSize := int64(0)
	if len(offsets) > 0 {
		totalSize = offsets[len(offsets)-1]
	}

	// sc is referenced by the buffer's OnEvict closure; assigned just below
	// before any read/write can trigger an eviction callback.
	var sc *SegmentCache

	bufCfg := buffer.Config{
		// Fires on a pool-driven disk punch (only if the usenet pool is ever
		// given a disk limit) and, in memory mode, on a block drop under
		// RAM pressure. Either way the covered segments are marked Empty so
		// they re-fetch instead of pointing at data that no longer exists.
		OnEvict: func(off, length int64) {
			if sc != nil {
				sc.onBufferEvict(off, length)
			}
		},
		// Pins are what make eviction safe against the read in progress:
		// readAtPlain pins its segments for the whole read, so vetoing
		// their blocks here means the buffer can never destroy bytes a
		// reader is about to copy out. Without the veto the pin was
		// advisory only, and a drop under pool pressure would strand the
		// reader on a segment nothing was fetching.
		CanDrop: func(off, length int64) bool {
			return sc == nil || !sc.rangePinned(off, length)
		},
	}
	// cacheBudget bounds the cached bytes the sliding-window sweeper and the
	// hard-budget drain maintain. In memory mode the buffer's own inline
	// drop-oldest is the budget enforcement, so the drain is disabled (0)
	// and only the playback-driven sweeper runs.
	cacheBudget := config.MaxDisk
	diskPath := ""
	var aheadBytes int64
	if config.MemoryBuffer {
		// Memory mode (the default): the sliding window lives as resident
		// RAM blocks sized from the configured read-ahead; sweep discards
		// free blocks as playback advances, and when the window outgrows
		// its budget the buffer drops a victim block inline, steered away
		// from the read head (OnEvict above marks the covered segments
		// Empty for re-fetch). No directory, no file — disk is never
		// touched.
		bufCfg.Mode = buffer.ModeMemory
		bufCfg.MemorySize, aheadBytes = memoryWindowSize(config, segments)
		cacheBudget = 0
	} else {
		// Disk mode (buffer_to_disk): decoded segments pwrite straight to a
		// sparse file in a fresh per-cache directory we own for the cache's
		// lifetime and remove on Close; the kernel page cache serves warm
		// re-reads and completed segments take the lock-free pread path.
		var err error
		diskPath = config.DiskPath
		if diskPath == "" {
			diskPath, err = os.MkdirTemp("", "usenet-cache-*")
		} else {
			if err = os.MkdirAll(diskPath, 0o755); err == nil {
				diskPath, err = os.MkdirTemp(diskPath, "cache-*")
			}
		}
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
		bufCfg.Mode = buffer.ModeDisk
		bufCfg.DiskPath = filepath.Join(diskPath, "segments.bin")
		bufCfg.TotalSize = totalSize
	}
	buf, err := usenetBufferPool().NewBuffer(bufCfg)
	if err != nil {
		cancel()
		if diskPath != "" {
			_ = os.RemoveAll(diskPath)
		}
		return nil, fmt.Errorf("create buffer: %w", err)
	}

	// The sweep window adapts to the mode. Disk mode keeps the generous
	// fixed back-window plus a retention age. Memory mode derives the
	// window from its RAM budget: the sweeper must reclaim BEFORE the
	// budget fills, or the buffer's block-granular drop becomes the primary
	// evictor and every drop wounds segments that cost a re-download to
	// heal. The prefetch window ahead of the cursor is reserved first —
	// when the budget clamp bites (large read-ahead), budget/2 of history
	// would leave less than the prefetcher writes ahead and the stream
	// would evict its own prefetch — history gets what remains, capped at
	// half the budget and the disk-mode window. The age gate is dropped
	// because the budget, not time, is the binding constraint.
	backWindow := int64(backWindowBytes)
	retention := segmentMinRetentionAge
	if config.MemoryBuffer {
		const slack = int64(8 << 20) // in-flight decodes and block rounding
		backWindow = min(int64(backWindowBytes), bufCfg.MemorySize/2, bufCfg.MemorySize-aheadBytes-slack)
		backWindow = max(backWindow, 4<<20)
		retention = 0
	}

	segBytesHint := int64(750 * 1024)
	if len(segments) > 0 && segments[0].Bytes > 0 {
		segBytesHint = segments[0].Bytes
	}

	sc = &SegmentCache{
		segments:       segments,
		segCount:       segCount,
		segOffsets:     offsets,
		totalSize:      totalSize,
		segLengths:     make([]atomic.Int64, segCount),
		states:         make([]atomic.Uint32, segCount),
		pinCounts:      make([]atomic.Int32, segCount),
		errors:         make([]atomic.Pointer[error], segCount),
		accessTime:     make([]atomic.Int64, segCount),
		buf:            buf,
		memoryMode:     config.MemoryBuffer,
		memBudget:      bufCfg.MemorySize,
		segBytesHint:   segBytesHint,
		diskPath:       diskPath,
		diskBudget:     cacheBudget,
		backWindow:     backWindow,
		retentionAge:   retention,
		maintainSignal: make(chan struct{}, 1),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger.With().Str("component", "cache").Logger(),
		stats:          stats,
	}

	for i := range numShards {
		sc.shardCond[i] = sync.NewCond(&sc.shardMu[i])
	}

	// One background goroutine owns all cache maintenance: the sliding
	// window sweep (both modes) and the disk-budget drain (disk mode).
	sc.maintainWg.Add(1)
	go sc.maintainLoop()

	return sc, nil
}

// computeOffsets calculates cumulative byte offsets for segment lookup.
func computeOffsets(segments []SegmentMeta) []int64 {
	offsets := make([]int64, len(segments)+1)
	if len(segments) > 0 && segments[0].EndOffset > 0 && offsetsAscend(segments) {
		for i, seg := range segments {
			offsets[i] = seg.StartOffset
		}
		if len(segments) > 0 {
			offsets[len(segments)] = segments[len(segments)-1].EndOffset + 1
		}
	} else {
		cumulative := int64(0)
		for i, seg := range segments {
			offsets[i] = cumulative
			size := seg.Bytes
			if size <= 0 {
				size = 750 * 1024
			}
			cumulative += size
		}
		offsets[len(segments)] = cumulative
	}
	return offsets
}

// offsetsAscend reports whether the stored segment offsets cover disjoint,
// ascending byte ranges. binarySearchSegment and readFromCache both depend on
// that; a table that breaks it makes reads double-count bytes and report more
// than the caller's buffer holds. Parsing rejects such files now, but .meta
// written by older versions can still carry zero-filled or out-of-order slots,
// so offsets from metadata are only used when they hold up. The cumulative
// fallback stays self-consistent: every read and write goes through
// segOffsets.
func offsetsAscend(segments []SegmentMeta) bool {
	for i := 1; i < len(segments); i++ {
		if segments[i].StartOffset <= segments[i-1].EndOffset {
			return false
		}
	}
	return segments[len(segments)-1].EndOffset >= segments[len(segments)-1].StartOffset-1
}

// ReadRangeInto is the zero-amplification read path: copies only the
// requested [segOffset, segOffset+length) slice of the segment.
func (sc *SegmentCache) ReadRangeInto(segIdx int, segOffset, length int64, dst []byte) (int, bool) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return 0, false
	}
	if SegmentState(sc.states[segIdx].Load()) != StateOnDisk {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	if segOffset < 0 || length < 0 || int64(len(dst)) < length {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}

	size := sc.SegmentDataSize(segIdx)
	if segOffset > size {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	if segOffset+length > size {
		length = size - segOffset
	}
	if length <= 0 {
		sc.stats.CacheHits.Add(1)
		return 0, true
	}

	absoluteOffset := sc.segOffsets[segIdx] + segOffset
	n, err := sc.buf.ReadAt(dst[:length], absoluteOffset)
	if err != nil {
		if !errors.Is(err, buffer.ErrNotPresent) {
			sc.logger.Warn().Err(err).Int("segment", segIdx).Msg("buffer read failed")
		}
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	// Reads refresh the access time so retentionAge means "unread for",
	// not "fetched more than" — a paused player re-reading one area keeps
	// those segments out of the sweep.
	sc.touchSegment(segIdx)
	sc.stats.CacheHits.Add(1)
	return n, true
}

// SegmentDataSize returns the stored or expected size of a segment.
func (sc *SegmentCache) SegmentDataSize(segIdx int) int64 {
	if segIdx < 0 || segIdx >= sc.segCount {
		return 0
	}
	size := sc.segLengths[segIdx].Load()
	if size <= 0 {
		size = sc.segments[segIdx].Bytes
		if size <= 0 {
			size = sc.segOffsets[segIdx+1] - sc.segOffsets[segIdx]
		}
	}
	return size
}

// segmentWriter is the contract doFetch uses to stream a segment body into
// the cache. Exactly one of Finalize/Discard is called per writer.
type segmentWriter interface {
	Write(p []byte) (int, error)
	Finalize()
	Discard()
}

// StreamWriter returns a buffer-backed writer for the segment. The writer
// skips the yEnc dataStart header and caps writes at the segment's max
// expected size.
func (sc *SegmentCache) StreamWriter(segIdx int) segmentWriter {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}

	if sc.diskBudget > 0 && sc.cachedBytes.Load() > sc.diskBudget {
		sc.drainOverBudget()
	}

	seg := sc.segments[segIdx]
	return &bufferStreamWriter{
		buf:       sc.buf,
		offset:    sc.segOffsets[segIdx],
		dataStart: seg.SegmentDataStart,
		maxBytes:  seg.Bytes,
		cache:     sc,
		segIdx:    segIdx,
	}
}

// bufferStreamWriter pipes decoded body bytes from NNTP into the buffer at
// the segment's reserved offset. Writes that exceed maxBytes are silently
// dropped (the decoder may include some trailing padding).
type bufferStreamWriter struct {
	buf       *buffer.Buffer
	offset    int64
	dataStart int64
	maxBytes  int64
	skipped   int64
	written   int64
	cache     *SegmentCache
	segIdx    int
}

func (w *bufferStreamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	consumed := 0

	if w.skipped < w.dataStart {
		skip := min(w.dataStart-w.skipped, int64(len(p)))
		w.skipped += skip
		consumed += int(skip)
		p = p[skip:]
		if len(p) == 0 {
			return consumed, nil
		}
	}

	if w.written >= w.maxBytes {
		return consumed + len(p), nil
	}

	remaining := w.maxBytes - w.written
	writeLen := min(int64(len(p)), remaining)

	n, err := w.buf.WriteAt(p[:writeLen], w.offset+w.written)
	if err != nil {
		return consumed + n, err
	}
	w.written += int64(n)
	return consumed + len(p), nil
}

// Discard is a no-op for the buffer writer: the buffer slot is fixed-offset
// and gets overwritten in place on the next attempt, so there's nothing
// to release on a failed/partial write.
func (w *bufferStreamWriter) Discard() {}

// Finalize commits the segment to the cache: state to OnDisk, length
// recorded, waiters woken.
func (w *bufferStreamWriter) Finalize() {
	if w.cache == nil || w.segIdx < 0 || w.written <= 0 {
		return
	}
	w.cache.cachedBytes.Add(w.written)
	w.cache.segLengths[w.segIdx].Store(w.written)
	w.cache.states[w.segIdx].Store(uint32(StateOnDisk))
	w.cache.touchSegment(w.segIdx)
	w.cache.wakeWaiters(w.segIdx)
	w.cache.signalMaintain()
}

// PinRange marks segments as in-use so the sweep and drain skip them, and
// (memory mode) so the buffer's CanDrop veto refuses to drop the blocks
// covering them. The pin is still not a perfect fence: an evictor that
// sampled the pin count just before PinRange returned may already have
// reserved the segment. The state machine keeps that case safe — the reader
// observes a miss, WaitForSegment reports ErrSegmentEvicted, and the read
// re-fetches — so pinning removes the common eviction race rather than every
// one of them.
func (sc *SegmentCache) PinRange(start, end int) {
	for i := start; i <= end && i < sc.segCount; i++ {
		sc.pinCounts[i].Add(1)
	}
}

// UnpinRange decrements the pin count for the range.
func (sc *SegmentCache) UnpinRange(start, end int) {
	for i := start; i <= end && i < sc.segCount; i++ {
		sc.pinCounts[i].Add(-1)
	}
}

// rangePinned reports whether any segment overlapping [off, off+length) is
// pinned by an in-progress read. Wired to the buffer's CanDrop veto, so it
// runs under the buffer lock: atomics only, no call back into the buffer.
func (sc *SegmentCache) rangePinned(off, length int64) bool {
	start, end := sc.SegmentsForRange(off, length)
	for i := start; i <= end && i < sc.segCount; i++ {
		if sc.pinCounts[i].Load() > 0 {
			return true
		}
	}
	return false
}

// IsPinned returns true if the segment has a positive pin count.
func (sc *SegmentCache) IsPinned(segIdx int) bool {
	if segIdx < 0 || segIdx >= sc.segCount {
		return false
	}
	return sc.pinCounts[segIdx].Load() > 0
}

// MaxPrefetchSegments reports how far ahead of the cursor this stream may
// read ahead without outrunning the RAM it is allowed to hold. In memory
// mode the ceiling is the smaller of the stream's own budget and its current
// share of the shared pool (see buffer.Buffer.Share), less a scrub-back
// reserve.
//
// Clamping the prefetcher is what stops the thrash: with many streams open
// the fair share falls below the configured read-ahead, and an unclamped
// prefetcher downloads segments that must be dropped to admit the next
// one — burning connections and bandwidth to re-fetch data it just threw
// away, while the player waits. Disk mode has no RAM ceiling and is
// unclamped.
func (sc *SegmentCache) MaxPrefetchSegments() int {
	if !sc.memoryMode || sc.memBudget <= 0 || sc.segBytesHint <= 0 {
		return math.MaxInt32
	}
	budget := sc.memBudget
	if share := sc.buf.Share(); share > 0 && share < budget {
		budget = share
	}
	usable := budget - pressureBackWindowBytes
	if usable < sc.segBytesHint {
		// Too little to reserve history from: keep a minimal window moving
		// rather than stalling the prefetcher completely.
		return 1
	}
	return int(usable / sc.segBytesHint)
}

// GetState returns the current state of a segment.
func (sc *SegmentCache) GetState(segIdx int) SegmentState {
	if segIdx < 0 || segIdx >= sc.segCount {
		return StateEmpty
	}
	return SegmentState(sc.states[segIdx].Load())
}

// SetState sets the state of a segment.
func (sc *SegmentCache) SetState(segIdx int, state SegmentState) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	sc.states[segIdx].Store(uint32(state))
}

// MarkFetching atomically transitions Empty → Fetching. Returns true if
// the transition succeeded (caller owns the fetch).
func (sc *SegmentCache) MarkFetching(segIdx int) bool {
	if segIdx < 0 || segIdx >= sc.segCount {
		return false
	}
	return sc.states[segIdx].CompareAndSwap(uint32(StateEmpty), uint32(StateFetching))
}

// MarkFailed records a permanent fetch failure.
func (sc *SegmentCache) MarkFailed(segIdx int, err error) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	sc.errors[segIdx].Store(&err)
	sc.states[segIdx].Store(uint32(StateFailed))
	sc.wakeWaiters(segIdx)
}

// GetError returns the error for a failed segment.
func (sc *SegmentCache) GetError(segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}
	if errPtr := sc.errors[segIdx].Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

// ResetFailed transitions Failed → Empty so a retry can re-fetch the segment.
// It is a CAS, not a blind store: a concurrent reader may have successfully
// fetched the segment between attempts, and flipping OnDisk → Empty would both
// force a spurious re-download and leak the segment's bytes out of the curDisk
// accounting (inflating it for the life of the reader, making the budget
// backstop over-evict). It must also never clobber another fetcher's Fetching.
func (sc *SegmentCache) ResetFailed(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	if sc.states[segIdx].CompareAndSwap(uint32(StateFailed), uint32(StateEmpty)) {
		sc.errors[segIdx].Store(nil)
	}
}

// ReleaseFetching transitions Fetching → Empty. Only the fetcher that owns the
// Fetching state (won MarkFetching) may call it, on its cancellation paths.
func (sc *SegmentCache) ReleaseFetching(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	if sc.states[segIdx].CompareAndSwap(uint32(StateFetching), uint32(StateEmpty)) {
		// Wake anyone parked on this fetch: the slot is Empty now and no
		// further transition is coming, so a waiter left asleep here would
		// never be woken again.
		sc.wakeWaiters(segIdx)
	}
}

// ErrSegmentEvicted reports that a segment is not cached and nobody is
// fetching it, so waiting would block forever. It is a normal outcome, not a
// failure: the caller re-fetches and retries.
var ErrSegmentEvicted = errors.New("segment evicted before read")

// WaitForSegment blocks while a segment is being fetched (or is mid-evict)
// and returns once it is OnDisk, has failed, or the context is canceled. An
// Empty segment returns ErrSegmentEvicted immediately rather than parking:
// nothing in the cache ever fetches on a waiter's behalf, so parking on
// Empty waits for an event that will not come. That was the deadlock behind
// the memory-mode playback stalls — a block drop flipped a segment the
// reader had already ensured from OnDisk back to Empty, and the reader slept
// on it forever while the prefetch window moved on.
func (sc *SegmentCache) WaitForSegment(ctx context.Context, segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return fmt.Errorf("segment index out of range: %d", segIdx)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	state := SegmentState(sc.states[segIdx].Load())
	switch state {
	case StateOnDisk:
		return nil
	case StateEmpty:
		return ErrSegmentEvicted
	case StateFailed:
		if err := sc.GetError(segIdx); err != nil {
			return err
		}
		return fmt.Errorf("segment %d failed", segIdx)
	}

	shardIdx := segIdx & shardMask
	cond := sc.shardCond[shardIdx]
	mu := &sc.shardMu[shardIdx]

	wakeShard := func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}
	ctxStopper := context.AfterFunc(ctx, wakeShard)
	defer ctxStopper()
	cacheStopper := context.AfterFunc(sc.ctx, wakeShard)
	defer cacheStopper()

	mu.Lock()
	defer mu.Unlock()

	for {
		state = SegmentState(sc.states[segIdx].Load())
		switch state {
		case StateOnDisk:
			return nil
		case StateEmpty:
			return ErrSegmentEvicted
		case StateFailed:
			if err := sc.GetError(segIdx); err != nil {
				return err
			}
			return fmt.Errorf("segment %d failed", segIdx)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.ctx.Done():
			return sc.ctx.Err()
		default:
		}

		cond.Wait()
	}
}

// WaitForEvictionRelease blocks while the segment is in StateEvicting, returning
// once the evictor has finished punching its range and dropped it to Empty (or
// the context/cache is canceled). Callers in the fetch path use this so a
// re-fetch never starts writing into a range mid-Discard.
func (sc *SegmentCache) WaitForEvictionRelease(ctx context.Context, segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return fmt.Errorf("segment index out of range: %d", segIdx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if SegmentState(sc.states[segIdx].Load()) != StateEvicting {
		return nil
	}

	shardIdx := segIdx & shardMask
	cond := sc.shardCond[shardIdx]
	mu := &sc.shardMu[shardIdx]

	wakeShard := func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}
	ctxStopper := context.AfterFunc(ctx, wakeShard)
	cacheStopper := context.AfterFunc(sc.ctx, wakeShard)
	defer ctxStopper()
	defer cacheStopper()

	mu.Lock()
	defer mu.Unlock()

	for SegmentState(sc.states[segIdx].Load()) == StateEvicting {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.ctx.Done():
			return sc.ctx.Err()
		default:
		}
		cond.Wait()
	}
	return nil
}

// invalidateForRefetch forces a segment that is marked OnDisk but whose backing
// bytes are unreadable back to Empty so the next Fetch actually re-downloads it
// instead of trusting the stale OnDisk state and short-circuiting. The CAS
// guarantees the disk accounting is rolled back exactly once even if two readers
// hit the same wedged segment concurrently. Safe to call on a pinned segment —
// the subsequent re-fetch overwrites the slot in place.
func (sc *SegmentCache) invalidateForRefetch(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	if sc.states[segIdx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEmpty)) {
		if size := sc.segLengths[segIdx].Load(); size > 0 {
			sc.cachedBytes.Add(-size)
		}
	}
	sc.errors[segIdx].Store(nil)
}

// wakeWaiters wakes any WaitForSegment callers parked on this segment's shard.
func (sc *SegmentCache) wakeWaiters(segIdx int) {
	shardIdx := segIdx & shardMask
	sc.shardMu[shardIdx].Lock()
	sc.shardCond[shardIdx].Broadcast()
	sc.shardMu[shardIdx].Unlock()
}

// touchSegment records the current time as the last access for a segment.
func (sc *SegmentCache) touchSegment(segIdx int) {
	sc.accessTime[segIdx].Store(time.Now().UnixNano())
}

// signalMaintain pokes the maintenance goroutine (non-blocking).
func (sc *SegmentCache) signalMaintain() {
	select {
	case sc.maintainSignal <- struct{}{}:
	default:
	}
}

// maintainLoop is the cache's single background goroutine: the sliding
// window sweep (both modes) plus the disk-budget drain backstop (disk mode
// only — a no-op otherwise). It wakes on every segment completion so
// eviction tracks inflow instead of waiting out the ticker; the ticker is
// the idle fallback (e.g. a paused player whose retention ages out). One
// goroutine owning both jobs also serializes the sweep cursor state for
// free.
func (sc *SegmentCache) maintainLoop() {
	defer sc.maintainWg.Done()
	ticker := time.NewTicker(segmentSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.ctx.Done():
			return
		case <-sc.maintainSignal:
		case <-ticker.C:
		}
		sc.sweepWindow()
		sc.drainOverBudget()
	}
}

// drainOverBudget is the hard disk-budget backstop (disk mode only).
func (sc *SegmentCache) drainOverBudget() {
	if sc.diskBudget <= 0 {
		return
	}

	// StreamWriter's inline call and the maintenance loop can notice the
	// same overshoot concurrently. Let one caller do the scan and punching
	// while the other waits; once it acquires the lock the budget is
	// normally already satisfied. Without this guard, concurrent segment
	// completions can each scan the full segment table and race to evict
	// the same batch.
	sc.evictMu.Lock()
	defer sc.evictMu.Unlock()

	for sc.cachedBytes.Load() > sc.diskBudget {
		batch := sc.findEvictableBatch(segmentSweepBatch)
		if len(batch) == 0 {
			break
		}
		sc.evictBatch(batch)
	}
}

// findEvictableBatch returns up to maxN unpinned OnDisk segments, scanning
// at most one lap from a persistent cursor. For streaming, index order ≈ age
// order, which is close enough for this hard-budget backstop; the precise
// window policy lives in sweepWindow. Used by drainOverBudget only, with
// evictMu held (which also guards evictCursor).
func (sc *SegmentCache) findEvictableBatch(maxN int) []int {
	if maxN <= 0 || sc.segCount == 0 {
		return nil
	}

	var out []int
	for scanned := 0; scanned < sc.segCount && len(out) < maxN; scanned++ {
		idx := (sc.evictCursor + scanned) % sc.segCount
		if sc.pinCounts[idx].Load() > 0 {
			continue
		}
		if SegmentState(sc.states[idx].Load()) != StateOnDisk {
			continue
		}
		out = append(out, idx)
	}
	if len(out) > 0 {
		sc.evictCursor = (out[len(out)-1] + 1) % sc.segCount
	}
	return out
}

// SetConsumedFloor publishes the delivered-offset of the slowest active
// consumer (the minimum across the reader's cursors). It drives the
// sliding-window sweeper's cutoff and the buffer's read head, so eviction
// never runs ahead of a consumer that is still behind. Not monotonic: a
// seek-back pulls the floor (and the protected window) back.
func (sc *SegmentCache) SetConsumedFloor(off int64) {
	if off < 0 {
		return
	}
	sc.consumedFloor.Store(off)
	if sc.buf != nil {
		sc.buf.SetReadHead(off)
	}
}

// sweepWindow picks segments that are both:
//
//  1. Behind the back-window (segEnd < consumedFloor - sc.backWindow), and
//  2. Untouched for at least sc.retentionAge (0 in memory mode: the RAM
//     budget, not time, is the binding constraint there).
//
// Both conditions must hold — see the package comment in cache.go for the
// rationale behind each.
func (sc *SegmentCache) sweepWindow() {
	consumedHi := sc.consumedFloor.Load()
	if consumedHi <= 0 {
		return
	}
	backWindow := sc.backWindow
	if sc.memoryMode && poolMemoryPressured() {
		// The shared pool is nearly exhausted — likely many concurrent
		// streams, or a multi-volume RAR whose played-out volumes each
		// still hold a full back-window of history. Trailing history is
		// the cheapest RAM to give back: tighten to a token scrub window
		// so every cache donates before the buffers start dropping live
		// blocks under pool pressure.
		backWindow = pressureBackWindowBytes
	}
	cutoffOff := consumedHi - backWindow
	if cutoffOff <= 0 {
		return
	}
	// A cutoff regression means a seek-back may have re-fetched segments
	// behind the cursor; rescan from the start.
	if cutoffOff < sc.sweepLastCutoff {
		sc.sweepCursor = 0
	}
	sc.sweepLastCutoff = cutoffOff
	cutoffAccessNs := time.Now().Add(-sc.retentionAge).UnixNano()

	indices := make([]int, 0, segmentSweepBatch)
	advanceCursor := true
	for i := sc.sweepCursor; i < sc.segCount && len(indices) < segmentSweepBatch; i++ {
		// Offsets are monotonic: past the cutoff nothing further qualifies.
		if sc.segOffsets[i+1] > cutoffOff {
			break
		}
		if SegmentState(sc.states[i].Load()) != StateOnDisk {
			// A contiguous evicted prefix never needs revisiting (barring a
			// cutoff regression, handled above).
			if advanceCursor {
				sc.sweepCursor = i + 1
			}
			continue
		}
		advanceCursor = false // OnDisk (possibly pinned/young): must revisit
		if sc.pinCounts[i].Load() > 0 {
			continue
		}
		if sc.accessTime[i].Load() > cutoffAccessNs {
			continue
		}
		indices = append(indices, i)
	}
	if len(indices) == 0 {
		return
	}
	sc.evictBatch(indices)
}

// evictBatch transitions the given segments out of the cache and releases
// their byte ranges through the buffer. Adjacent ranges are coalesced into
// fewer Discard calls — for sequential playback eviction, ~dozen segments
// merge into one buffer.Discard (and thus one fallocate(PUNCH_HOLE)).
//
// Each segment moves OnDisk -> Evicting -> (Discard) -> Empty. The Evicting
// hold is what makes eviction safe against a concurrent re-fetch: MarkFetching
// only transitions Empty -> Fetching, so no fetcher can begin writing into a
// segment's range while we are punching it. Only after the Discard completes do
// we drop the slot to Empty and wake any reader/fetcher that parked on it; that
// re-fetch then writes into a freshly-punched, no-longer-contended range.
//
// Previously the slot went straight to Empty before the (deferred, coalesced)
// Discard, so a reader could re-download the segment in the gap and have its
// bytes punched right back out — leaving the slot OnDisk but unreadable and the
// "segment N still missing after re-fetch" wedge.
func (sc *SegmentCache) evictBatch(indices []int) {
	type rng struct {
		off  int64
		size int64
	}
	pieces := make([]rng, 0, len(indices))
	evicted := make([]int, 0, len(indices))

	for _, idx := range indices {
		if sc.pinCounts[idx].Load() > 0 {
			continue
		}
		// Reserve the segment for eviction. The CAS from OnDisk fences out both
		// a concurrent re-fetch (MarkFetching needs Empty) and another evictor.
		if !sc.states[idx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEvicting)) {
			continue
		}
		size := sc.segLengths[idx].Load()
		if size <= 0 {
			size = sc.segments[idx].Bytes
			if size <= 0 {
				size = sc.segOffsets[idx+1] - sc.segOffsets[idx]
			}
		}
		sc.cachedBytes.Add(-size)
		sc.stats.Evictions.Add(1)
		pieces = append(pieces, rng{sc.segOffsets[idx], size})
		evicted = append(evicted, idx)
	}
	if len(pieces) == 0 {
		return
	}

	sort.Slice(pieces, func(a, b int) bool { return pieces[a].off < pieces[b].off })

	// Coalesce adjacent ranges into the fewest possible Discard calls.
	merged := pieces[:1]
	for _, r := range pieces[1:] {
		last := &merged[len(merged)-1]
		if last.off+last.size == r.off {
			last.size += r.size
		} else {
			merged = append(merged, r)
		}
	}
	for _, r := range merged {
		if err := sc.buf.Discard(r.off, r.size); err != nil {
			sc.logger.Debug().
				Err(err).
				Int64("offset", r.off).
				Int64("size", r.size).
				Msg("buffer discard failed; slot will be overwritten on next fetch")
		}
	}

	// The disk ranges are gone; release the slots and wake anyone waiting so
	// they re-fetch into the now-punched (and no-longer-contended) range.
	for _, idx := range evicted {
		sc.states[idx].Store(uint32(StateEmpty))
		sc.wakeWaiters(idx)
	}
}

// onBufferEvict is invoked when the buffer released bytes on its own
// initiative: a memory-mode block drop under RAM pressure, or (disk mode
// with a pool disk limit — off by default) a punch behind the read head. It
// marks the affected segments Empty so they re-fetch instead of pointing at
// data that no longer exists.
//
// The overlap rule differs by mode. A memory-mode drop destroys every byte
// it covered, and a segment missing ANY byte is unreadable — so any overlap
// marks the segment Empty, pinned or not (the bytes are already gone;
// marking now lets the prefetcher heal the hole asynchronously instead of
// the player discovering it and stalling on a synchronous re-fetch). A
// disk-mode punch only covers present ranges wholly behind the back-window,
// so a partial overlap there means the segment straddles the window
// boundary and its bytes are still on disk — keep it.
func (sc *SegmentCache) onBufferEvict(off, length int64) {
	end := off + length
	startIdx, endIdx := sc.SegmentsForRange(off, length)
	for idx := startIdx; idx <= endIdx && idx < sc.segCount; idx++ {
		segStart := sc.segOffsets[idx]
		segEnd := sc.segOffsets[idx+1]
		if sc.memoryMode {
			if segStart >= end || segEnd <= off {
				continue // no overlap
			}
		} else {
			if segStart < off || segEnd > end {
				continue // not fully contained
			}
			if sc.pinCounts[idx].Load() > 0 {
				continue
			}
		}
		if !sc.states[idx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEmpty)) {
			continue
		}
		size := sc.segLengths[idx].Load()
		if size <= 0 {
			size = segEnd - segStart
		}
		sc.cachedBytes.Add(-size)
		sc.stats.Evictions.Add(1)
		// A reader may be parked on this segment; it must observe the Empty
		// state and re-fetch rather than sleep through the drop.
		sc.wakeWaiters(idx)
	}
}

// SegmentsForRange returns the segment indices covering [offset, offset+length).
func (sc *SegmentCache) SegmentsForRange(offset, length int64) (int, int) {
	if sc.segCount == 0 {
		return 0, 0
	}
	endOffset := offset + length - 1
	startIdx := sc.binarySearchSegment(offset)
	if startIdx >= sc.segCount {
		startIdx = sc.segCount - 1
	}
	endIdx := sc.binarySearchSegment(endOffset)
	if endIdx >= sc.segCount {
		endIdx = sc.segCount - 1
	}
	return startIdx, endIdx
}

// binarySearchSegment finds the segment containing the given offset.
func (sc *SegmentCache) binarySearchSegment(offset int64) int {
	lo, hi := 0, sc.segCount
	for lo < hi {
		mid := (lo + hi) / 2
		if sc.segOffsets[mid+1] <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// GetSegment returns segment metadata by index.
func (sc *SegmentCache) GetSegment(segIdx int) *SegmentMeta {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}
	return &sc.segments[segIdx]
}

// SegmentCount returns the total number of segments.
func (sc *SegmentCache) SegmentCount() int { return sc.segCount }

// TotalSize returns the total size of all segments.
func (sc *SegmentCache) TotalSize() int64 { return sc.totalSize }

// SegmentOffset returns the byte offset of a segment.
func (sc *SegmentCache) SegmentOffset(segIdx int) int64 {
	if segIdx < 0 || segIdx > sc.segCount {
		return 0
	}
	return sc.segOffsets[segIdx]
}

// Close releases all resources.
func (sc *SegmentCache) Close() error {
	if sc.closed.Swap(true) {
		return nil
	}
	sc.cancel()

	for i := range numShards {
		sc.shardMu[i].Lock()
		sc.shardCond[i].Broadcast()
		sc.shardMu[i].Unlock()
	}

	sc.maintainWg.Wait()

	if sc.buf != nil {
		_ = sc.buf.Close()
	}
	if sc.diskPath != "" {
		_ = os.RemoveAll(sc.diskPath)
	}
	return nil
}
