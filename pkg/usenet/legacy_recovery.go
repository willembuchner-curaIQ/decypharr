package usenet

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

const maxLegacyNZBMetadataBytes = int64(100 << 20)

var (
	// ErrLegacyNZBSourceUnavailable means the original local NZB metadata was
	// already removed. Scheduled repair may still reacquire it through an Arr.
	ErrLegacyNZBSourceUnavailable = errors.New("legacy NZB source is unavailable")
	// ErrLegacyNZBIdentityMismatch prevents parity from a different post from
	// ever being attached to an existing logical segment map.
	ErrLegacyNZBIdentityMismatch = errors.New("legacy NZB article identity mismatch")
	// ErrLegacyNZBNoPAR2 means the exact release was found but contains no
	// identifiable PAR2 files, so replacement remains the only repair option.
	ErrLegacyNZBNoPAR2 = errors.New("legacy NZB contains no PAR2 recovery files")
)

type legacySegmentIdentity struct {
	messageID        string
	number           int
	bytes            int64
	segmentDataStart int64
}

type legacySegmentOrigin struct {
	rawFileKey uint32
	rawOffset  int64
	rawLength  int64
}

// LoadLegacyNZBSource returns a still-present source file without scanning or
// consuming the watched NZB directory. Completed imports normally delete this
// file, but interrupted upgrades and queued entries can still have it locally.
func (u *Usenet) LoadLegacyNZBSource(nzoID string) (string, []byte, error) {
	if u == nil || u.nzbStorage == nil || strings.TrimSpace(nzoID) == "" {
		return "", nil, ErrLegacyNZBSourceUnavailable
	}

	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrLegacyNZBSourceUnavailable, err)
	}
	sourceName := nzb.Name
	if sourceName == "" {
		sourceName = nzoID
	}
	if !strings.EqualFold(filepath.Ext(sourceName), ".nzb") {
		sourceName += ".nzb"
	}

	paths := make([]string, 0, 4)
	if nzb.Path != "" {
		paths = append(paths, nzb.Path)
	}
	if u.metadataDir != "" {
		paths = append(paths,
			filepath.Join(u.metadataDir, nzoID+".nzb"),
			filepath.Join(u.metadataDir, nzoID+".queued"),
			// Completed imports keep only the compressed archive.
			u.nzbArchivePath(nzoID),
		)
	}

	seen := make(map[string]struct{}, len(paths))
	var readErr error
	for _, path := range paths {
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		if !pathWithinDir(path, u.metadataDir) {
			continue
		}
		content, err := readBoundedNZBMetadata(path)
		if err == nil {
			return sourceName, content, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			readErr = errors.Join(readErr, err)
		}
	}
	if readErr != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrLegacyNZBSourceUnavailable, readErr)
	}
	return "", nil, ErrLegacyNZBSourceUnavailable
}

func pathWithinDir(path, dir string) bool {
	if path == "" || dir == "" {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func readBoundedNZBMetadata(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("NZB source is a directory")
	}

	var source io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		// Only the decompressed length is meaningful for an archive, and the
		// LimitReader below is what actually bounds it: a small archive can
		// still expand past the limit.
		reader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("open NZB archive: %w", err)
		}
		defer reader.Close()
		source = reader
	} else if info.Size() > maxLegacyNZBMetadataBytes {
		return nil, fmt.Errorf("NZB metadata exceeds %d-byte limit", maxLegacyNZBMetadataBytes)
	}

	content, err := io.ReadAll(io.LimitReader(source, maxLegacyNZBMetadataBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maxLegacyNZBMetadataBytes {
		return nil, fmt.Errorf("NZB metadata exceeds %d-byte limit", maxLegacyNZBMetadataBytes)
	}
	return content, nil
}

// HydrateLegacyNZB safely attaches raw PAR2 provenance from an exact NZB to an
// existing logical NZB. The source XML itself is never retained. Persistence
// is keyed by the existing NZB ID, and any failure restores the legacy segment
// map and removes provisional recovery records.
func (u *Usenet) HydrateLegacyNZB(ctx context.Context, nzoID, sourceName string, content []byte) error {
	if u == nil || u.nntp == nil || u.nzbStorage == nil || u.par2Store == nil || u.par2Recovery == nil {
		return errors.New("usenet PAR2 hydration is unavailable")
	}
	if strings.TrimSpace(nzoID) == "" {
		return errors.New("NZB id is empty")
	}
	if len(content) == 0 {
		return errors.New("NZB content is empty")
	}
	if int64(len(content)) > maxLegacyNZBMetadataBytes {
		return fmt.Errorf("NZB metadata exceeds %d-byte limit", maxLegacyNZBMetadataBytes)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	_, err, _ := u.legacyHydrationFlights.Do(nzoID, func() (any, error) {
		return nil, u.hydrateLegacyNZB(ctx, nzoID, sourceName, content)
	})
	return err
}

// NeedsPAR2Hydration reports whether an NZB predates recovery provenance.
func (u *Usenet) NeedsPAR2Hydration(nzoID string) (bool, error) {
	if u == nil || u.nzbStorage == nil {
		return false, errors.New("usenet NZB storage is unavailable")
	}
	if strings.TrimSpace(nzoID) == "" {
		return false, errors.New("NZB id is empty")
	}

	_, missing, err := u.nzbStorage.RecoveryOriginState(nzoID)
	if err != nil {
		return false, fmt.Errorf("load NZB metadata: %w", err)
	}
	return missing, nil
}

// LegacyNZBIDs returns a stable snapshot of stored NZB identifiers for the
// background provenance migration. Reading the individual records happens
// outside the storage directory lock so a large legacy library cannot block
// new imports for the duration of the scan.
func (u *Usenet) LegacyNZBIDs() ([]string, error) {
	if u == nil || u.nzbStorage == nil {
		return nil, errors.New("usenet NZB storage is unavailable")
	}
	ids, err := u.nzbStorage.GetAllNZBIDs()
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// LegacyNZBHydrationCandidate reports the Arr association and whether one
// stored NZB still lacks PAR2 provenance. It intentionally examines one NZB at
// a time so the startup migration can pace disk and allocation pressure.
func (u *Usenet) LegacyNZBHydrationCandidate(nzoID string) (arrName string, needed bool, err error) {
	if u == nil || u.nzbStorage == nil {
		return "", false, errors.New("usenet NZB storage is unavailable")
	}
	return u.nzbStorage.RecoveryOriginState(nzoID)
}

func (u *Usenet) hydrateLegacyNZB(ctx context.Context, nzoID, sourceName string, content []byte) (resultErr error) {
	if err := validateNZB(content); err != nil {
		return fmt.Errorf("invalid reacquired NZB: %w", err)
	}
	legacy, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return fmt.Errorf("load legacy NZB metadata: %w", err)
	}
	previousManifest, previousErr := u.par2Store.GetManifest(nzoID)
	hadManifest := previousErr == nil
	if previousErr != nil && !errors.Is(previousErr, recovery.ErrNotFound) {
		return fmt.Errorf("load existing PAR2 manifest: %w", previousErr)
	}
	if hadManifest && previousManifest.HasPAR2() && hasCompleteRecoveryOrigins(legacy) {
		return nil
	}
	if sourceName == "" {
		sourceName = legacy.Name + ".nzb"
	}

	prs := parser.NewParser(u.nntp, max(1, u.processingMaxConnections), u.logger.With().Str("component", "legacy-par2-hydration").Logger())
	preflight, err := prs.ParseRecoveryManifest(sourceName, content)
	if err != nil {
		return fmt.Errorf("parse reacquired NZB topology: %w", err)
	}
	preflight.SetNZBID(nzoID)
	if err := validateLegacyArticleIdentity(legacy, preflight); err != nil {
		return err
	}

	// Only after the zero-bandwidth identity proof may normal parsing inspect
	// yEnc headers/magic and archive metadata.
	candidate, groups, manifest, err := prs.ParseWithManifest(ctx, sourceName, content)
	if err != nil {
		return fmt.Errorf("parse reacquired NZB: %w", err)
	}
	candidate.ID = nzoID
	manifest.SetNZBID(nzoID)
	if err := validateLegacyArticleIdentity(legacy, manifest); err != nil {
		return err
	}
	if !manifest.HasPAR2() {
		return ErrLegacyNZBNoPAR2
	}

	registered := false
	committed := false
	defer func() {
		if registered && !committed {
			if hadManifest {
				resultErr = errors.Join(resultErr, u.par2Recovery.RegisterManifest(previousManifest))
			} else {
				resultErr = errors.Join(resultErr, u.par2Recovery.DeleteNZB(nzoID))
			}
		}
	}()
	if err := u.par2Recovery.RegisterManifest(manifest); err != nil {
		return fmt.Errorf("persist provisional PAR2 manifest: %w", err)
	}
	registered = true

	prs.SetArticleRecovery(nzoID, u.par2Recovery)
	processed, processErr := prs.Process(ctx, candidate, groups)
	if processErr != nil {
		// A directly-posted media file may have every article missing. In that
		// case no yEnc header survives for the normal parser, but the legacy
		// logical map already records every full decoded article range. Use it
		// only when it covers each article of every referenced raw source; archive
		// members are sliced views and are intentionally ineligible.
		processed, err = deriveDirectMediaOrigins(legacy, manifest)
		if err != nil {
			return errors.Join(
				fmt.Errorf("process reacquired NZB: %w", processErr),
				fmt.Errorf("derive direct-media provenance: %w", err),
				recovery.ErrLayoutUnavailable,
			)
		}
	}
	enriched := parser.ManifestFromGroups(groups)
	if enriched == nil || !enriched.HasPAR2() {
		return ErrLegacyNZBNoPAR2
	}

	if err := u.par2Recovery.RegisterManifest(enriched); err != nil {
		return fmt.Errorf("persist enriched PAR2 manifest: %w", err)
	}

	mapped := 0
	if err := u.nzbStorage.UpdateNZB(nzoID, func(current *storage.NZB) error {
		if err := validateLegacyLogicalTopology(legacy, current); err != nil {
			return err
		}
		var applyErr error
		mapped, applyErr = applyLegacySegmentOrigins(current, processed)
		return applyErr
	}); err != nil {
		return fmt.Errorf("persist hydrated NZB metadata: %w", err)
	}

	committed = true
	u.invalidateIdleNZBFileSystems(nzoID)
	u.logger.Info().
		Str("nzb_id", nzoID).
		Int("mapped_segments", mapped).
		Msg("Hydrated legacy NZB with PAR2 recovery provenance")
	return nil
}

func deriveDirectMediaOrigins(legacy *storage.NZB, manifest *recovery.Manifest) (*storage.NZB, error) {
	if legacy == nil || manifest == nil {
		return nil, ErrLegacyNZBIdentityMismatch
	}
	type sourceArticle struct {
		rawKey recovery.RawFileKey
		number int
	}
	rawByMessageID := make(map[string]sourceArticle)
	rawCounts := make(map[string]int)
	for _, raw := range manifest.Files {
		for _, article := range raw.Articles {
			messageID := canonicalLegacyMessageID(article.MessageID)
			if messageID == "" {
				continue
			}
			rawCounts[messageID]++
			rawByMessageID[messageID] = sourceArticle{rawKey: raw.Key, number: article.Number}
		}
	}

	type fullArticleRange struct {
		number int
		bytes  int64
	}
	ranges := make(map[recovery.RawFileKey]map[string]fullArticleRange)
	for i := range legacy.Files {
		file := &legacy.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		if file.FileType != storage.NZBFileTypeMedia || file.InternalPath != "" {
			return nil, fmt.Errorf("legacy file %q is not directly posted media", file.Name)
		}
		for _, segment := range file.Segments {
			messageID := canonicalLegacyMessageID(segment.MessageID)
			raw := rawByMessageID[messageID]
			if messageID == "" || rawCounts[messageID] != 1 || raw.rawKey == 0 || raw.number != segment.Number ||
				segment.Bytes <= 0 || segment.SegmentDataStart != 0 {
				return nil, fmt.Errorf("legacy media segment is not one complete raw article")
			}
			if ranges[raw.rawKey] == nil {
				ranges[raw.rawKey] = make(map[string]fullArticleRange)
			}
			value := fullArticleRange{number: segment.Number, bytes: segment.Bytes}
			if previous, ok := ranges[raw.rawKey][messageID]; ok && previous != value {
				return nil, fmt.Errorf("legacy media article range is ambiguous")
			}
			ranges[raw.rawKey][messageID] = value
		}
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("legacy NZB has no direct media ranges")
	}

	origins := make(map[string]legacySegmentOrigin)
	for _, raw := range manifest.Files {
		articles, referenced := ranges[raw.Key]
		if !referenced {
			continue
		}
		var offset int64
		for _, article := range raw.Articles {
			messageID := canonicalLegacyMessageID(article.MessageID)
			logical, ok := articles[messageID]
			if !ok || logical.number != article.Number || logical.bytes <= 0 {
				return nil, fmt.Errorf("legacy media does not cover every raw article")
			}
			origins[messageID] = legacySegmentOrigin{rawFileKey: uint32(raw.Key), rawOffset: offset, rawLength: logical.bytes}
			manifest.UpdateArticleLayout(raw.Key, article.Number, offset, logical.bytes, recovery.LayoutExact)
			offset += logical.bytes
			if offset < 0 {
				return nil, fmt.Errorf("legacy media raw offset overflow")
			}
		}
		if len(articles) != len(raw.Articles) {
			return nil, fmt.Errorf("legacy media article coverage is ambiguous")
		}
	}

	result := cloneNZBMetadata(legacy)
	for i := range result.Files {
		file := &result.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for j := range file.Segments {
			origin, ok := origins[canonicalLegacyMessageID(file.Segments[j].MessageID)]
			if !ok {
				return nil, fmt.Errorf("legacy media article origin is missing")
			}
			file.Segments[j].RawFileKey = origin.rawFileKey
			file.Segments[j].RawOffset = origin.rawOffset
			file.Segments[j].RawLength = origin.rawLength
		}
	}
	return result, nil
}

func hasCompleteRecoveryOrigins(nzb *storage.NZB) bool {
	found, missing := recoveryOriginCoverage(nzb)
	return found && !missing
}

func hasMissingRecoveryOrigins(nzb *storage.NZB) bool {
	_, missing := recoveryOriginCoverage(nzb)
	return missing
}

func recoveryOriginCoverage(nzb *storage.NZB) (found, missing bool) {
	if nzb == nil {
		return false, false
	}
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for _, segment := range file.Segments {
			found = true
			if segment.RawFileKey == 0 || segment.RawOffset < 0 || segment.RawLength <= 0 {
				return true, true
			}
		}
	}
	return found, false
}

func validateLegacyLogicalTopology(expected, current *storage.NZB) error {
	if expected == nil || current == nil || expected.ID == "" || expected.ID != current.ID {
		return fmt.Errorf("%w: stored NZB identity changed during hydration", ErrLegacyNZBIdentityMismatch)
	}
	if len(expected.Files) != len(current.Files) {
		return fmt.Errorf("%w: logical file count changed during hydration", ErrLegacyNZBIdentityMismatch)
	}
	for i := range expected.Files {
		left, right := &expected.Files[i], &current.Files[i]
		if left.Name != right.Name || left.InternalPath != right.InternalPath || left.FileType != right.FileType ||
			left.IsDeleted != right.IsDeleted || len(left.Segments) != len(right.Segments) {
			return fmt.Errorf("%w: logical file topology changed during hydration", ErrLegacyNZBIdentityMismatch)
		}
		for j := range left.Segments {
			lseg, rseg := left.Segments[j], right.Segments[j]
			if canonicalLegacyMessageID(lseg.MessageID) != canonicalLegacyMessageID(rseg.MessageID) ||
				lseg.Number != rseg.Number || lseg.Bytes != rseg.Bytes || lseg.StartOffset != rseg.StartOffset ||
				lseg.EndOffset != rseg.EndOffset || lseg.SegmentDataStart != rseg.SegmentDataStart || lseg.Group != rseg.Group {
				return fmt.Errorf("%w: logical segment topology changed during hydration", ErrLegacyNZBIdentityMismatch)
			}
		}
	}
	return nil
}

// invalidateIdleNZBFileSystems evicts readers built from pre-hydration segment
// metadata. Active readers remain pinned and safe; once idle, the normal
// janitor retires them. The usual scheduled-repair case has no active reader,
// so the next open immediately reloads the new raw origins.
func (u *Usenet) invalidateIdleNZBFileSystems(nzoID string) {
	if u == nil || u.fs == nil || nzoID == "" {
		return
	}
	prefix := nzoID + "::"
	u.fs.Range(func(key string, entry *fsEntry) bool {
		if !strings.HasPrefix(key, prefix) || entry == nil || !entry.claimForCleanup() {
			return true
		}
		u.fs.Delete(key)
		entry.cleanup()
		return true
	})
}

func validateLegacyArticleIdentity(legacy *storage.NZB, manifest *recovery.Manifest) error {
	if legacy == nil || manifest == nil {
		return fmt.Errorf("%w: metadata is missing", ErrLegacyNZBIdentityMismatch)
	}
	counts := make(map[string]int)
	for _, raw := range manifest.Files {
		for _, article := range raw.Articles {
			if messageID := canonicalLegacyMessageID(article.MessageID); messageID != "" {
				counts[messageID]++
			}
		}
	}

	checked := 0
	seen := make(map[string]struct{})
	for i := range legacy.Files {
		file := &legacy.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for _, segment := range file.Segments {
			messageID := canonicalLegacyMessageID(segment.MessageID)
			if messageID == "" {
				return fmt.Errorf("%w: stored segment has no message id", ErrLegacyNZBIdentityMismatch)
			}
			if _, ok := seen[messageID]; ok {
				continue
			}
			seen[messageID] = struct{}{}
			checked++
			if counts[messageID] != 1 {
				return fmt.Errorf("%w: stored article is absent or ambiguous", ErrLegacyNZBIdentityMismatch)
			}
		}
	}
	if checked == 0 {
		return fmt.Errorf("%w: stored NZB has no content articles", ErrLegacyNZBIdentityMismatch)
	}
	return nil
}

func applyLegacySegmentOrigins(legacy, candidate *storage.NZB) (int, error) {
	if legacy == nil || candidate == nil {
		return 0, fmt.Errorf("%w: logical metadata is missing", ErrLegacyNZBIdentityMismatch)
	}
	origins := make(map[legacySegmentIdentity]legacySegmentOrigin)
	ambiguous := make(map[legacySegmentIdentity]struct{})
	for i := range candidate.Files {
		file := &candidate.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for _, segment := range file.Segments {
			if segment.MessageID == "" || segment.RawFileKey == 0 || segment.RawOffset < 0 || segment.RawLength <= 0 || segment.RawLength != segment.Bytes {
				continue
			}
			key := legacySegmentIdentity{canonicalLegacyMessageID(segment.MessageID), segment.Number, segment.Bytes, segment.SegmentDataStart}
			origin := legacySegmentOrigin{segment.RawFileKey, segment.RawOffset, segment.RawLength}
			if previous, ok := origins[key]; ok && previous != origin {
				ambiguous[key] = struct{}{}
				continue
			}
			origins[key] = origin
		}
	}

	mapped := 0
	for i := range legacy.Files {
		file := &legacy.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for j := range file.Segments {
			segment := &file.Segments[j]
			key := legacySegmentIdentity{canonicalLegacyMessageID(segment.MessageID), segment.Number, segment.Bytes, segment.SegmentDataStart}
			if _, ok := ambiguous[key]; ok {
				return 0, fmt.Errorf("%w: logical article range is ambiguous", ErrLegacyNZBIdentityMismatch)
			}
			origin, ok := origins[key]
			if !ok {
				return 0, fmt.Errorf("%w: logical article range was not reproduced", ErrLegacyNZBIdentityMismatch)
			}
			segment.RawFileKey = origin.rawFileKey
			segment.RawOffset = origin.rawOffset
			segment.RawLength = origin.rawLength
			mapped++
		}
	}
	if mapped == 0 {
		return 0, fmt.Errorf("%w: no logical ranges were mapped", ErrLegacyNZBIdentityMismatch)
	}
	return mapped, nil
}

func canonicalLegacyMessageID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return ""
	}
	return nntp.FormatMessageID(messageID)
}

func cloneNZBMetadata(source *storage.NZB) *storage.NZB {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Groups = append([]string(nil), source.Groups...)
	clone.Files = make([]storage.NZBFile, len(source.Files))
	for i := range source.Files {
		clone.Files[i] = source.Files[i]
		clone.Files[i].Groups = append([]string(nil), source.Files[i].Groups...)
		clone.Files[i].Segments = append([]storage.NZBSegment(nil), source.Files[i].Segments...)
		clone.Files[i].EncryptionKey = append([]byte(nil), source.Files[i].EncryptionKey...)
		clone.Files[i].EncryptionIV = append([]byte(nil), source.Files[i].EncryptionIV...)
	}
	return &clone
}
