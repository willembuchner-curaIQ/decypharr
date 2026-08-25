package recovery

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/sirrobot01/appendstore"
)

const (
	attributeNZBID = "nzb_id"
	attributeKind  = "kind"

	kindManifest      = "manifest"
	kindParsedSet     = "parsed_set"
	kindRecoverySlice = "recovery_slice"
	kindPatch         = "patch"
)

var recoverySlicePacketType = [16]byte{'P', 'A', 'R', ' ', '2', '.', '0', 0, 'R', 'e', 'c', 'v', 'S', 'l', 'i', 'c'}

// Store keeps recovery-only data in one append-only database. Its own lock
// makes multi-key operations such as DeleteNZB atomic with respect to other
// calls through the same Store instance.
type Store struct {
	mu      sync.RWMutex
	db      *appendstore.Store
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	closed  bool
}

// Stats is a cheap snapshot of recovery record counts, disk/memory footprint,
// and append-store operational counters.
type Stats struct {
	Closed         bool
	Entries        int
	DiskBytes      int64
	MemoryBytes    int64
	Manifests      int
	ParsedSets     int
	RecoverySlices int
	Patches        int
	Writes         int64
	Reads          int64
	CacheHits      int64
	CacheMisses    int64
	Deletes        int64
	Compactions    int64
}

// Open opens or creates the recovery database at path. The database is
// intentionally a caller-selected file (normally <main>/usenet/par2.db), so it
// can be placed and backed up independently of general application metadata.
func Open(path string) (*Store, error) {
	encoder, decoder, err := newMetadataCodec()
	if err != nil {
		return nil, err
	}
	db, err := appendstore.Open(path, appendstore.Options{
		CacheSize:           32,
		SyncInterval:        time.Second,
		CompactionThreshold: 0.35,
		AutoCompact:         true,
		IndexedFields:       []string{attributeNZBID, attributeKind},
	})
	if err != nil {
		_ = encoder.Close()
		decoder.Close()
		return nil, fmt.Errorf("open PAR2 recovery store: %w", err)
	}
	return &Store{db: db, encoder: encoder, decoder: decoder}, nil
}

// Close flushes and closes the database. It is safe to call more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.db.Close()
	err = errors.Join(err, s.encoder.Close())
	s.decoder.Close()
	return err
}

// Sync flushes all appended recovery records to stable storage.
func (s *Store) Sync() error {
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	return s.db.Sync()
}

// Stats returns a snapshot. After Close it returns the zero snapshot with
// Closed set, rather than invoking the closed append-store.
func (s *Store) Stats() Stats {
	if s == nil {
		return Stats{Closed: true}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Stats{Closed: true}
	}
	operational := s.db.Stats()
	return Stats{
		Entries:        s.db.Len(),
		DiskBytes:      s.db.DiskSize(),
		MemoryBytes:    s.db.MemoryUsage(),
		Manifests:      s.db.CountBy(attributeKind, kindManifest),
		ParsedSets:     s.db.CountBy(attributeKind, kindParsedSet),
		RecoverySlices: s.db.CountBy(attributeKind, kindRecoverySlice),
		Patches:        s.db.CountBy(attributeKind, kindPatch),
		Writes:         operational.Writes,
		Reads:          operational.Reads,
		CacheHits:      operational.CacheHits,
		CacheMisses:    operational.CacheMisses,
		Deletes:        operational.Deletes,
		Compactions:    operational.Compactions,
	}
}

// PutManifest stores a compact, zstd-compressed snapshot containing every raw
// NZB file and article.
func (s *Store) PutManifest(manifest *Manifest) error {
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	logical, nzbID, err := encodeManifest(manifest)
	if err != nil {
		return err
	}
	framed, err := wrapRecord(recordManifest, codecZstd, logical, s.encoder)
	if err != nil {
		return err
	}
	return s.putLocked(manifestKey(nzbID), nzbID, kindManifest, framed)
}

// GetManifest returns an owned manifest snapshot. A missing record returns a
// ManifestUnavailableError because legacy NZBs cannot be safely repaired
// without raw file/article provenance.
func (s *Store) GetManifest(nzbID string) (*Manifest, error) {
	if err := validateNZBID(nzbID); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}
	key := manifestKey(nzbID)
	framed, err := s.db.Get(key)
	if errors.Is(err, appendstore.ErrKeyNotFound) {
		return nil, &ManifestUnavailableError{NZBID: nzbID}
	}
	if err != nil {
		return nil, fmt.Errorf("read recovery manifest: %w", err)
	}
	logical, err := unwrapRecord(framed, recordManifest, s.decoder)
	if err != nil {
		return nil, decodeError(kindManifest, key, err)
	}
	manifest, err := decodeManifest(logical)
	if err != nil {
		return nil, decodeError(kindManifest, key, err)
	}
	if manifest.NZBID != nzbID {
		return nil, &CorruptionError{Kind: kindManifest, Key: key, Cause: fmt.Errorf("contains NZB ID %q", manifest.NZBID)}
	}
	return manifest, nil
}

// PutParsedSet stores an engine-independent parsed PAR2 set. Metadata is
// compact binary plus zstd; recovery payload bytes remain separate and lazy.
func (s *Store) PutParsedSet(nzbID string, set StoredSet) error {
	if err := validateNZBID(nzbID); err != nil {
		return err
	}
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	logical, err := encodeStoredSet(set)
	if err != nil {
		return err
	}
	framed, err := wrapRecord(recordParsedSet, codecZstd, logical, s.encoder)
	if err != nil {
		return err
	}
	return s.putLocked(parsedSetKey(nzbID, set.SetID), nzbID, kindParsedSet, framed)
}

// GetParsedSet returns an owned parsed-set value.
func (s *Store) GetParsedSet(nzbID string, setID SetID) (StoredSet, error) {
	if err := validateNZBID(nzbID); err != nil {
		return StoredSet{}, err
	}
	if s == nil {
		return StoredSet{}, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return StoredSet{}, err
	}
	return s.getParsedSetLocked(nzbID, setID)
}

// ListParsedSets returns every parsed set for one NZB, ordered by set ID.
func (s *Store) ListParsedSets(nzbID string) ([]StoredSet, error) {
	if err := validateNZBID(nzbID); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}
	prefix := nzbKeyPrefix(nzbID) + "/set/"
	keys := s.db.KeysBy(attributeNZBID, nzbID)
	sort.Strings(keys)
	sets := make([]StoredSet, 0)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		setID, err := setIDFromKey(strings.TrimPrefix(key, prefix))
		if err != nil {
			return nil, &CorruptionError{Kind: kindParsedSet, Key: key, Cause: err}
		}
		set, err := s.getParsedSetLocked(nzbID, setID)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}
	return sets, nil
}

// PutRecoverySlice stores a recovery payload only after matching it against a
// trusted descriptor in the parsed set. The high-entropy payload is framed but
// deliberately not compressed.
func (s *Store) PutRecoverySlice(nzbID string, setID SetID, exponent uint32, payload []byte) error {
	if err := validateNZBID(nzbID); err != nil {
		return err
	}
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	set, err := s.getParsedSetLocked(nzbID, setID)
	if err != nil {
		return err
	}
	descriptor, err := recoveryDescriptor(set, exponent)
	if err != nil {
		return err
	}
	if uint64(len(payload)) != descriptor.PayloadLength {
		return &ValidationError{Field: "recovery payload", Reason: fmt.Sprintf("length is %d, descriptor requires %d", len(payload), descriptor.PayloadLength)}
	}
	actual := recoveryPacketMD5(setID, exponent, payload)
	if actual != descriptor.PacketMD5 {
		return &ChecksumMismatchError{Kind: kindRecoverySlice, Expected: descriptor.PacketMD5, Actual: actual}
	}
	logical := make([]byte, 36+len(payload))
	copy(logical[:16], setID[:])
	binary.BigEndian.PutUint32(logical[16:20], exponent)
	copy(logical[20:36], descriptor.PacketMD5[:])
	copy(logical[36:], payload)
	framed, err := wrapRecord(recordRecoverySlice, codecRaw, logical, nil)
	if err != nil {
		return err
	}
	return s.putLocked(recoverySliceKey(nzbID, setID, exponent), nzbID, kindRecoverySlice, framed)
}

// GetRecoverySlice returns an owned payload and re-verifies both its storage
// checksum and the MD5 from the current parsed-set descriptor.
func (s *Store) GetRecoverySlice(nzbID string, setID SetID, exponent uint32) ([]byte, error) {
	if err := validateNZBID(nzbID); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}
	set, err := s.getParsedSetLocked(nzbID, setID)
	if err != nil {
		return nil, err
	}
	descriptor, err := recoveryDescriptor(set, exponent)
	if err != nil {
		return nil, err
	}
	key := recoverySliceKey(nzbID, setID, exponent)
	framed, err := s.db.Get(key)
	if errors.Is(err, appendstore.ErrKeyNotFound) {
		return nil, &NotFoundError{Kind: kindRecoverySlice, NZBID: nzbID, ID: fmt.Sprintf("%x/%d", setID, exponent)}
	}
	if err != nil {
		return nil, fmt.Errorf("read recovery slice: %w", err)
	}
	logical, err := unwrapRecord(framed, recordRecoverySlice, s.decoder)
	if err != nil {
		return nil, decodeError(kindRecoverySlice, key, err)
	}
	if len(logical) < 36 || !bytes.Equal(logical[:16], setID[:]) || binary.BigEndian.Uint32(logical[16:20]) != exponent {
		return nil, &CorruptionError{Kind: kindRecoverySlice, Key: key, Cause: errors.New("stored identity does not match key")}
	}
	var storedPacketMD5 [16]byte
	copy(storedPacketMD5[:], logical[20:36])
	payload := logical[36:]
	actual := recoveryPacketMD5(setID, exponent, payload)
	if storedPacketMD5 != descriptor.PacketMD5 || actual != descriptor.PacketMD5 || uint64(len(payload)) != descriptor.PayloadLength {
		return nil, &CorruptionError{Kind: kindRecoverySlice, Key: key, Cause: errors.New("payload does not match parsed-set descriptor")}
	}
	return payload, nil
}

// HasRecoverySlice reports whether a payload key is present. GetRecoverySlice
// remains the authoritative integrity check.
func (s *Store) HasRecoverySlice(nzbID string, setID SetID, exponent uint32) (bool, error) {
	if err := validateNZBID(nzbID); err != nil {
		return false, err
	}
	if s == nil {
		return false, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return false, err
	}
	return s.db.Exists(recoverySliceKey(nzbID, setID, exponent)), nil
}

// PutPatch stores one repaired source-file range. A later write at the same
// offset atomically replaces it. Patch bytes are framed but not compressed.
func (s *Store) PutPatch(nzbID string, setID SetID, fileID FileID, offset uint64, data []byte) error {
	if err := validateNZBID(nzbID); err != nil {
		return err
	}
	if len(data) == 0 {
		return &ValidationError{Field: "repair patch", Reason: "must not be empty"}
	}
	if len(data) > maxRawRecordBytes-40 {
		return &ValidationError{Field: "repair patch", Reason: "exceeds append-store record limit"}
	}
	if offset > ^uint64(0)-uint64(len(data)) {
		return &ValidationError{Field: "repair patch", Reason: "range overflows uint64"}
	}
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	set, err := s.getParsedSetLocked(nzbID, setID)
	if err != nil {
		return err
	}
	source, ok := sourceFile(set, fileID)
	if !ok {
		return &NotFoundError{Kind: "source file descriptor", NZBID: nzbID, ID: fmt.Sprintf("%x/%x", setID, fileID)}
	}
	if offset+uint64(len(data)) > source.Length {
		return &ValidationError{Field: "repair patch", Reason: fmt.Sprintf("range ends at %d beyond source length %d", offset+uint64(len(data)), source.Length)}
	}
	logical := make([]byte, 40+len(data))
	copy(logical[:16], setID[:])
	copy(logical[16:32], fileID[:])
	binary.BigEndian.PutUint64(logical[32:40], offset)
	copy(logical[40:], data)
	framed, err := wrapRecord(recordPatch, codecRaw, logical, nil)
	if err != nil {
		return err
	}
	return s.putLocked(patchKey(nzbID, setID, fileID, offset), nzbID, kindPatch, framed)
}

// ReadRepairedRange returns data only when patches cover every requested byte.
// Adjacent and overlapping patches are assembled without exposing partial
// results when a gap exists.
func (s *Store) ReadRepairedRange(nzbID string, setID SetID, fileID FileID, offset, length uint64) ([]byte, error) {
	if err := validateNZBID(nzbID); err != nil {
		return nil, err
	}
	if length > uint64(maxRawRecordBytes) || length > uint64(maxInt()) {
		return nil, &ValidationError{Field: "repaired range length", Reason: "exceeds local byte-slice limit"}
	}
	if offset > ^uint64(0)-length {
		return nil, &ValidationError{Field: "repaired range", Reason: "overflows uint64"}
	}
	if s == nil {
		return nil, ErrStoreClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	set, err := s.getParsedSetLocked(nzbID, setID)
	if err != nil {
		return nil, err
	}
	source, ok := sourceFile(set, fileID)
	if !ok {
		return nil, &NotFoundError{Kind: "source file descriptor", NZBID: nzbID, ID: fmt.Sprintf("%x/%x", setID, fileID)}
	}
	end := offset + length
	if end > source.Length {
		return nil, &ValidationError{Field: "repaired range", Reason: fmt.Sprintf("ends at %d beyond source length %d", end, source.Length)}
	}
	prefix := patchKeyPrefix(nzbID, setID, fileID)
	keys := s.db.KeysBy(attributeNZBID, nzbID)
	patches := make([]storedPatch, 0)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		keyOffset, err := patchOffsetFromKey(strings.TrimPrefix(key, prefix))
		if err != nil {
			return nil, &CorruptionError{Kind: kindPatch, Key: key, Cause: err}
		}
		if keyOffset >= end {
			continue
		}
		framed, err := s.db.Get(key)
		if err != nil {
			return nil, fmt.Errorf("read repair patch: %w", err)
		}
		logical, err := unwrapRecord(framed, recordPatch, s.decoder)
		if err != nil {
			return nil, decodeError(kindPatch, key, err)
		}
		if len(logical) <= 40 || !bytes.Equal(logical[:16], setID[:]) || !bytes.Equal(logical[16:32], fileID[:]) {
			return nil, &CorruptionError{Kind: kindPatch, Key: key, Cause: errors.New("stored identity does not match key")}
		}
		storedOffset := binary.BigEndian.Uint64(logical[32:40])
		if storedOffset != keyOffset || storedOffset > ^uint64(0)-uint64(len(logical)-40) {
			return nil, &CorruptionError{Kind: kindPatch, Key: key, Cause: errors.New("stored range does not match key")}
		}
		patchEnd := storedOffset + uint64(len(logical)-40)
		if patchEnd <= offset {
			continue
		}
		patches = append(patches, storedPatch{start: storedOffset, end: patchEnd, data: logical[40:]})
	}
	result := make([]byte, int(length))
	cursor := offset
	for cursor < end {
		best := -1
		bestEnd := cursor
		for i := range patches {
			if patches[i].start <= cursor && patches[i].end > bestEnd {
				best = i
				bestEnd = patches[i].end
			}
		}
		if best < 0 {
			return nil, &RangeCoverageError{Start: offset, End: end, FirstMissing: cursor}
		}
		copyEnd := bestEnd
		if copyEnd > end {
			copyEnd = end
		}
		patch := &patches[best]
		copy(result[int(cursor-offset):int(copyEnd-offset)], patch.data[int(cursor-patch.start):int(copyEnd-patch.start)])
		cursor = copyEnd
	}
	return result, nil
}

// ReadRepairedAt is an allocation-safe exact-coverage convenience wrapper. p
// is left untouched when any requested byte is unavailable.
func (s *Store) ReadRepairedAt(nzbID string, setID SetID, fileID FileID, p []byte, offset uint64) error {
	data, err := s.ReadRepairedRange(nzbID, setID, fileID, offset, uint64(len(p)))
	if err != nil {
		return err
	}
	copy(p, data)
	return nil
}

// DeleteNZB removes every manifest, parsed set, recovery slice, and repair
// patch scoped to nzbID.
func (s *Store) DeleteNZB(nzbID string) error {
	if err := validateNZBID(nzbID); err != nil {
		return err
	}
	if s == nil {
		return ErrStoreClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	keys := s.db.KeysBy(attributeNZBID, nzbID)
	var result error
	for _, key := range keys {
		if err := s.db.Delete(key); err != nil && !errors.Is(err, appendstore.ErrKeyNotFound) {
			result = errors.Join(result, fmt.Errorf("delete recovery key %q: %w", key, err))
		}
	}
	return result
}

type storedPatch struct {
	start uint64
	end   uint64
	data  []byte
}

func (s *Store) ensureOpenLocked() error {
	if s.closed || s.db == nil {
		return ErrStoreClosed
	}
	return nil
}

func (s *Store) putLocked(key, nzbID, kind string, value []byte) error {
	return s.db.Put(key, value, &appendstore.PutOptions{Attributes: map[string]string{
		attributeNZBID: nzbID,
		attributeKind:  kind,
	}})
}

func (s *Store) getParsedSetLocked(nzbID string, setID SetID) (StoredSet, error) {
	key := parsedSetKey(nzbID, setID)
	framed, err := s.db.Get(key)
	if errors.Is(err, appendstore.ErrKeyNotFound) {
		return StoredSet{}, &NotFoundError{Kind: kindParsedSet, NZBID: nzbID, ID: fmt.Sprintf("%x", setID)}
	}
	if err != nil {
		return StoredSet{}, fmt.Errorf("read parsed PAR2 set: %w", err)
	}
	logical, err := unwrapRecord(framed, recordParsedSet, s.decoder)
	if err != nil {
		return StoredSet{}, decodeError(kindParsedSet, key, err)
	}
	set, err := decodeStoredSet(logical)
	if err != nil {
		return StoredSet{}, decodeError(kindParsedSet, key, err)
	}
	if set.SetID != setID {
		return StoredSet{}, &CorruptionError{Kind: kindParsedSet, Key: key, Cause: errors.New("stored set ID does not match key")}
	}
	return set, nil
}

func decodeError(kind, key string, err error) error {
	if errors.Is(err, ErrUnsupported) {
		return err
	}
	return &CorruptionError{Kind: kind, Key: key, Cause: err}
}

func recoveryDescriptor(set StoredSet, exponent uint32) (RecoverySliceDescriptor, error) {
	for i := range set.Recovery {
		if set.Recovery[i].Exponent == exponent {
			return set.Recovery[i], nil
		}
	}
	return RecoverySliceDescriptor{}, &NotFoundError{Kind: "recovery slice descriptor", ID: strconv.FormatUint(uint64(exponent), 10)}
}

func recoveryPacketMD5(setID SetID, exponent uint32, payload []byte) [16]byte {
	hash := md5.New()
	_, _ = hash.Write(setID[:])
	_, _ = hash.Write(recoverySlicePacketType[:])
	var encodedExponent [4]byte
	binary.LittleEndian.PutUint32(encodedExponent[:], exponent)
	_, _ = hash.Write(encodedExponent[:])
	_, _ = hash.Write(payload)
	var digest [16]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func sourceFile(set StoredSet, fileID FileID) (StoredSourceFile, bool) {
	for i := range set.Files {
		if set.Files[i].FileID == fileID {
			return set.Files[i], true
		}
	}
	return StoredSourceFile{}, false
}

func nzbKeyPrefix(nzbID string) string {
	return "p2/1/" + base64.RawURLEncoding.EncodeToString([]byte(nzbID))
}

func manifestKey(nzbID string) string {
	return nzbKeyPrefix(nzbID) + "/manifest"
}

func parsedSetKey(nzbID string, setID SetID) string {
	return nzbKeyPrefix(nzbID) + "/set/" + hex.EncodeToString(setID[:])
}

func recoverySliceKey(nzbID string, setID SetID, exponent uint32) string {
	return fmt.Sprintf("%s/recovery/%x/%08x", nzbKeyPrefix(nzbID), setID, exponent)
}

func patchKeyPrefix(nzbID string, setID SetID, fileID FileID) string {
	return fmt.Sprintf("%s/patch/%x/%x/", nzbKeyPrefix(nzbID), setID, fileID)
}

func patchKey(nzbID string, setID SetID, fileID FileID, offset uint64) string {
	return patchKeyPrefix(nzbID, setID, fileID) + fmt.Sprintf("%016x", offset)
}

func setIDFromKey(value string) (SetID, error) {
	var setID SetID
	if len(value) != hex.EncodedLen(len(setID)) {
		return setID, errors.New("invalid set ID key length")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return setID, fmt.Errorf("decode set ID key: %w", err)
	}
	copy(setID[:], decoded)
	return setID, nil
}

func patchOffsetFromKey(value string) (uint64, error) {
	if len(value) != 16 {
		return 0, errors.New("invalid patch offset key length")
	}
	offset, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("decode patch offset key: %w", err)
	}
	return offset, nil
}
