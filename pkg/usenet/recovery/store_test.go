package recovery

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sirrobot01/appendstore"
)

func TestStoreRestartAndOwnedBuffers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "par2.db")
	store := openTestStore(t, path)
	manifest := testManifest("nzb/restart")
	wantManifest := manifestFields(manifest)
	setID := testSetID(0x11)
	fileID := testFileID(0x22)
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	set := testStoredSet(setID, fileID, payload)

	if err := store.PutManifest(manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	if err := store.PutParsedSet(manifest.NZBID, set); err != nil {
		t.Fatalf("PutParsedSet: %v", err)
	}
	payloadInput := bytes.Clone(payload)
	if err := store.PutRecoverySlice(manifest.NZBID, setID, 7, payloadInput); err != nil {
		t.Fatalf("PutRecoverySlice: %v", err)
	}
	patchInput := []byte("repair-data")
	if err := store.PutPatch(manifest.NZBID, setID, fileID, 0, patchInput); err != nil {
		t.Fatalf("PutPatch: %v", err)
	}

	// Caller-owned inputs may be reused as soon as Put returns.
	manifest.UpdateClassification(1, "changed", "changed.bin", false)
	payloadInput[0] ^= 0xff
	patchInput[0] = 'X'
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store = openTestStore(t, path)
	defer store.Close()
	gotManifest, err := store.GetManifest("nzb/restart")
	if err != nil {
		t.Fatalf("GetManifest after restart: %v", err)
	}
	if got := manifestFields(gotManifest); !reflect.DeepEqual(got, wantManifest) {
		t.Fatalf("manifest mismatch after restart\n got: %#v\nwant: %#v", got, wantManifest)
	}
	gotSet, err := store.GetParsedSet("nzb/restart", setID)
	if err != nil {
		t.Fatalf("GetParsedSet after restart: %v", err)
	}
	if !reflect.DeepEqual(gotSet, set) {
		t.Fatalf("parsed set mismatch after restart\n got: %#v\nwant: %#v", gotSet, set)
	}
	gotPayload, err := store.GetRecoverySlice("nzb/restart", setID, 7)
	if err != nil {
		t.Fatalf("GetRecoverySlice after restart: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("recovery payload = %x, want %x", gotPayload, payload)
	}
	gotPayload[0] ^= 0xff
	gotPayloadAgain, err := store.GetRecoverySlice("nzb/restart", setID, 7)
	if err != nil {
		t.Fatalf("second GetRecoverySlice: %v", err)
	}
	if !bytes.Equal(gotPayloadAgain, payload) {
		t.Fatal("mutating returned recovery payload changed stored bytes")
	}
	gotPatch, err := store.ReadRepairedRange("nzb/restart", setID, fileID, 0, uint64(len("repair-data")))
	if err != nil {
		t.Fatalf("ReadRepairedRange after restart: %v", err)
	}
	if string(gotPatch) != "repair-data" {
		t.Fatalf("patch = %q, want repair-data", gotPatch)
	}
	stats := store.Stats()
	if stats.Entries != 4 || stats.Manifests != 1 || stats.ParsedSets != 1 || stats.RecoverySlices != 1 || stats.Patches != 1 {
		t.Fatalf("unexpected stats after restart: %+v", stats)
	}
	if stats.DiskBytes <= 0 || stats.MemoryBytes <= 0 {
		t.Fatalf("store footprint not reported: %+v", stats)
	}
}

func TestManifestCodecIsCompactAndComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "par2.db")
	store := openTestStore(t, path)
	defer store.Close()
	manifest := testManifest("nzb-compact")
	manifest.Files[0].Articles = make([]Article, 300)
	for i := range manifest.Files[0].Articles {
		manifest.Files[0].Articles[i] = Article{
			Number:        i + 1,
			MessageID:     "repeated-prefix-article-000000000000000000@example.invalid",
			PostedBytes:   768000,
			DecodedOffset: int64(i) * 700000,
			DecodedSize:   700000,
			Layout:        LayoutExact,
		}
	}
	manifest.Files[0].TotalSegments = len(manifest.Files[0].Articles)
	if err := store.PutManifest(manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	framed, err := store.db.Get(manifestKey(manifest.NZBID))
	if err != nil {
		t.Fatalf("read framed manifest: %v", err)
	}
	if framed[6] != byte(codecZstd) {
		t.Fatalf("manifest codec = %d, want zstd", framed[6])
	}
	logical, err := unwrapRecord(framed, recordManifest, store.decoder)
	if err != nil {
		t.Fatalf("unwrap manifest: %v", err)
	}
	for _, jsonField := range [][]byte{[]byte(`"message_id"`), []byte(`"posted_bytes"`), []byte(`"articles"`)} {
		if bytes.Contains(logical, jsonField) {
			t.Fatalf("compact manifest repeats JSON field name %q", jsonField)
		}
	}
	jsonValue, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal comparison JSON: %v", err)
	}
	if len(logical) >= len(jsonValue) {
		t.Fatalf("compact binary is not smaller than JSON: binary=%d JSON=%d", len(logical), len(jsonValue))
	}
	if len(framed) >= len(logical) {
		t.Fatalf("repetitive manifest was not compressed: framed=%d logical=%d", len(framed), len(logical))
	}
	got, err := store.GetManifest(manifest.NZBID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if len(got.Files) != len(manifest.Files) || len(got.Files[0].Articles) != 300 {
		t.Fatalf("raw files/articles were skipped: files=%d articles=%d", len(got.Files), len(got.Files[0].Articles))
	}
}

func TestManifestRoundTripsZeroBasedArticleNumbers(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	defer store.Close()
	manifest := testManifest("nzb-zero-based")
	manifest.Files[0].Articles[0].Number = 0
	if err := store.PutManifest(manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	got, err := store.GetManifest(manifest.NZBID)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if got.Files[0].Articles[0].Number != 0 {
		t.Fatalf("article number = %d, want 0", got.Files[0].Articles[0].Number)
	}
}

func TestReplacementAndDescriptorReverification(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	defer store.Close()
	nzbID := "nzb-replace"
	manifest := testManifest(nzbID)
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.UpdateClassification(1, "video", "final.mkv", false)
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	gotManifest, err := store.GetManifest(nzbID)
	if err != nil {
		t.Fatal(err)
	}
	gotFile, ok := gotManifest.File(1)
	if !ok || gotFile.ActualFilename != "final.mkv" {
		t.Fatalf("manifest replacement not visible: %+v", gotFile)
	}

	setID := testSetID(0x31)
	fileID := testFileID(0x32)
	oldPayload := []byte("old!")
	set := testStoredSet(setID, fileID, oldPayload)
	if err := store.PutParsedSet(nzbID, set); err != nil {
		t.Fatal(err)
	}
	if err := store.PutRecoverySlice(nzbID, setID, 7, oldPayload); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPatch(nzbID, setID, fileID, 0, []byte("old-patch")); err != nil {
		t.Fatal(err)
	}
	if err := store.PutPatch(nzbID, setID, fileID, 0, []byte("new-patch")); err != nil {
		t.Fatal(err)
	}
	patch, err := store.ReadRepairedRange(nzbID, setID, fileID, 0, uint64(len("new-patch")))
	if err != nil || string(patch) != "new-patch" {
		t.Fatalf("patch replacement = %q, %v", patch, err)
	}

	newPayload := []byte("new!")
	set.Files[0].Name = "renamed-source.bin"
	set.Recovery[0].PacketMD5 = recoveryPacketMD5(setID, 7, newPayload)
	if err := store.PutParsedSet(nzbID, set); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRecoverySlice(nzbID, setID, 7); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stale recovery payload error = %v, want ErrCorrupt", err)
	}
	if err := store.PutRecoverySlice(nzbID, setID, 7, newPayload); err != nil {
		t.Fatalf("replace recovery slice: %v", err)
	}
	gotPayload, err := store.GetRecoverySlice(nzbID, setID, 7)
	if err != nil || !bytes.Equal(gotPayload, newPayload) {
		t.Fatalf("recovery replacement = %x, %v", gotPayload, err)
	}
	gotSet, err := store.GetParsedSet(nzbID, setID)
	if err != nil || gotSet.Files[0].Name != "renamed-source.bin" {
		t.Fatalf("parsed-set replacement = %+v, %v", gotSet, err)
	}
	stats := store.Stats()
	if stats.Entries != 4 || stats.Manifests != 1 || stats.ParsedSets != 1 || stats.RecoverySlices != 1 || stats.Patches != 1 {
		t.Fatalf("replacement created duplicate live records: %+v", stats)
	}
}

func TestNZBIsolationAndDeleteNZB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "par2.db")
	store := openTestStore(t, path)
	setID := testSetID(0x41)
	fileID := testFileID(0x42)
	for _, fixture := range []struct {
		nzbID   string
		payload []byte
		patch   string
	}{
		{nzbID: "nzb/A", payload: []byte("AAAA"), patch: "patch-A"},
		{nzbID: "nzb/B", payload: []byte("BBBB"), patch: "patch-B"},
	} {
		if err := store.PutManifest(testManifest(fixture.nzbID)); err != nil {
			t.Fatal(err)
		}
		if err := store.PutParsedSet(fixture.nzbID, testStoredSet(setID, fileID, fixture.payload)); err != nil {
			t.Fatal(err)
		}
		if err := store.PutRecoverySlice(fixture.nzbID, setID, 7, fixture.payload); err != nil {
			t.Fatal(err)
		}
		if err := store.PutPatch(fixture.nzbID, setID, fileID, 5, []byte(fixture.patch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteNZB("nzb/A"); err != nil {
		t.Fatalf("DeleteNZB: %v", err)
	}
	if err := store.DeleteNZB("not-present"); err != nil {
		t.Fatalf("DeleteNZB absent: %v", err)
	}
	if _, err := store.GetManifest("nzb/A"); !errors.Is(err, ErrLegacyManifestUnsupported) {
		t.Fatalf("deleted manifest error = %v", err)
	}
	if _, err := store.GetParsedSet("nzb/A", setID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted parsed set error = %v", err)
	}
	if has, err := store.HasRecoverySlice("nzb/A", setID, 7); err != nil || has {
		t.Fatalf("deleted recovery slice still present: has=%v err=%v", has, err)
	}
	if _, err := store.ReadRepairedRange("nzb/A", setID, fileID, 5, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted patch read error = %v, want not found", err)
	}

	payload, err := store.GetRecoverySlice("nzb/B", setID, 7)
	if err != nil || string(payload) != "BBBB" {
		t.Fatalf("other NZB recovery slice = %q, %v", payload, err)
	}
	patch, err := store.ReadRepairedRange("nzb/B", setID, fileID, 5, uint64(len("patch-B")))
	if err != nil || string(patch) != "patch-B" {
		t.Fatalf("other NZB patch = %q, %v", patch, err)
	}
	stats := store.Stats()
	if stats.Entries != 4 || stats.Manifests != 1 || stats.ParsedSets != 1 || stats.RecoverySlices != 1 || stats.Patches != 1 {
		t.Fatalf("DeleteNZB scope mismatch: %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openTestStore(t, path)
	defer store.Close()
	if _, err := store.GetManifest("nzb/A"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted NZB returned after restart: %v", err)
	}
	if _, err := store.GetManifest("nzb/B"); err != nil {
		t.Fatalf("isolated NZB missing after restart: %v", err)
	}
}

func TestRecoverySliceMustMatchTrustedDescriptor(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	defer store.Close()
	nzbID := "nzb-verify"
	setID := testSetID(0x51)
	fileID := testFileID(0x52)
	set := testStoredSet(setID, fileID, []byte("good"))
	if err := store.PutParsedSet(nzbID, set); err != nil {
		t.Fatal(err)
	}
	if err := store.PutRecoverySlice(nzbID, setID, 7, []byte("evil")); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("bad checksum error = %v, want ErrChecksumMismatch", err)
	}
	if has, err := store.HasRecoverySlice(nzbID, setID, 7); err != nil || has {
		t.Fatalf("unverified payload was retained: has=%v err=%v", has, err)
	}
	if err := store.PutRecoverySlice(nzbID, setID, 7, []byte("bad")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad length error = %v, want ErrInvalid", err)
	}
	if err := store.PutRecoverySlice(nzbID, setID, 99, []byte("good")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown exponent error = %v, want ErrNotFound", err)
	}
	if err := store.PutRecoverySlice("missing-set", setID, 7, []byte("good")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing set error = %v, want ErrNotFound", err)
	}
}

func TestRecoveryPacketMD5CanonicalVector(t *testing.T) {
	setID := testSetID(0x5a)
	const exponent uint32 = 0x0203
	payload := []byte{1, 3, 5, 7}
	want := [16]byte{0x68, 0x10, 0xb8, 0xc5, 0xe0, 0x8c, 0xb1, 0x84, 0x6b, 0x4e, 0xd4, 0x9d, 0x0f, 0x2b, 0x12, 0xe9}
	if got := recoveryPacketMD5(setID, exponent, payload); got != want {
		t.Fatalf("canonical recovery packet MD5 = %x, want %x", got, want)
	}
}

func TestRepairedRangeExactCoverage(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	defer store.Close()
	nzbID := "nzb-ranges"
	setID := testSetID(0x61)
	fileID := testFileID(0x62)
	if err := store.PutParsedSet(nzbID, testStoredSet(setID, fileID, []byte("par!"))); err != nil {
		t.Fatal(err)
	}
	for _, patch := range []struct {
		offset uint64
		data   string
	}{
		{offset: 0, data: "abcd"},
		{offset: 4, data: "efgh"},
		{offset: 2, data: "cdefghij"},
		{offset: 12, data: "mnop"},
	} {
		if err := store.PutPatch(nzbID, setID, fileID, patch.offset, []byte(patch.data)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ReadRepairedRange(nzbID, setID, fileID, 1, 9)
	if err != nil {
		t.Fatalf("covered range: %v", err)
	}
	if string(got) != "bcdefghij" {
		t.Fatalf("assembled range = %q, want bcdefghij", got)
	}
	got[0] = 'X'
	again, err := store.ReadRepairedRange(nzbID, setID, fileID, 1, 1)
	if err != nil || string(again) != "b" {
		t.Fatalf("mutating result changed stored patch: %q, %v", again, err)
	}

	destination := []byte("untouched")
	err = store.ReadRepairedAt(nzbID, setID, fileID, destination, 7)
	var coverage *RangeCoverageError
	if !errors.As(err, &coverage) || !errors.Is(err, ErrRangeNotCovered) {
		t.Fatalf("gap error = %v, want typed coverage error", err)
	}
	if coverage.FirstMissing != 10 {
		t.Fatalf("first missing byte = %d, want 10", coverage.FirstMissing)
	}
	if string(destination) != "untouched" {
		t.Fatalf("partial range modified destination: %q", destination)
	}
	empty, err := store.ReadRepairedRange(nzbID, setID, fileID, 0, 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty range = %v, %v", empty, err)
	}
}

func TestTypedLegacyManifestResult(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	defer store.Close()
	_, err := store.GetManifest("legacy-nzb")
	var unavailable *ManifestUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("missing manifest error type = %T (%v)", err, err)
	}
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, ErrLegacyManifestUnsupported) {
		t.Fatalf("missing manifest error does not expose not-found/legacy semantics: %v", err)
	}
}

func TestApplicationRecordCorruption(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		key     func(string, SetID, FileID) string
		corrupt func(*testing.T, *Store, string, SetID, FileID)
	}{
		{
			name: "manifest", kind: kindManifest,
			key: func(nzb string, _ SetID, _ FileID) string { return manifestKey(nzb) },
			corrupt: func(t *testing.T, s *Store, nzb string, _ SetID, _ FileID) {
				_, err := s.GetManifest(nzb)
				assertCorrupt(t, err)
			},
		},
		{
			name: "parsed set", kind: kindParsedSet,
			key: func(nzb string, set SetID, _ FileID) string { return parsedSetKey(nzb, set) },
			corrupt: func(t *testing.T, s *Store, nzb string, set SetID, _ FileID) {
				_, err := s.GetParsedSet(nzb, set)
				assertCorrupt(t, err)
			},
		},
		{
			name: "recovery slice", kind: kindRecoverySlice,
			key: func(nzb string, set SetID, _ FileID) string { return recoverySliceKey(nzb, set, 7) },
			corrupt: func(t *testing.T, s *Store, nzb string, set SetID, _ FileID) {
				_, err := s.GetRecoverySlice(nzb, set, 7)
				assertCorrupt(t, err)
			},
		},
		{
			name: "repair patch", kind: kindPatch,
			key: func(nzb string, set SetID, file FileID) string { return patchKey(nzb, set, file, 0) },
			corrupt: func(t *testing.T, s *Store, nzb string, set SetID, file FileID) {
				_, err := s.ReadRepairedRange(nzb, set, file, 0, 1)
				assertCorrupt(t, err)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nzbID := "nzb-corrupt"
			setID := testSetID(0x71)
			fileID := testFileID(0x72)
			store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
			defer store.Close()
			if err := store.PutManifest(testManifest(nzbID)); err != nil {
				t.Fatal(err)
			}
			if err := store.PutParsedSet(nzbID, testStoredSet(setID, fileID, []byte("data"))); err != nil {
				t.Fatal(err)
			}
			if err := store.PutRecoverySlice(nzbID, setID, 7, []byte("data")); err != nil {
				t.Fatal(err)
			}
			if err := store.PutPatch(nzbID, setID, fileID, 0, []byte("patch")); err != nil {
				t.Fatal(err)
			}
			corruptStoreValue(t, store, test.key(nzbID, setID, fileID), nzbID, test.kind)
			test.corrupt(t, store, nzbID, setID, fileID)
		})
	}
}

func TestOnDiskCorruptionFailsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "par2.db")
	store := openTestStore(t, path)
	if err := store.PutManifest(testManifest("nzb-disk-corrupt")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	last := []byte{0}
	if _, err := file.ReadAt(last, info.Size()-1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	last[0] ^= 0xff
	if _, err := file.WriteAt(last, info.Size()-1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path); err == nil {
		reopened.Close()
		t.Fatal("Open accepted a checksum-corrupted append log")
	}
}

func TestValidationAndClose(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "par2.db"))
	if err := store.PutManifest(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil manifest error = %v", err)
	}
	badManifest := testManifest("valid-id")
	badManifest.Files[0].Key = 0
	if err := store.PutManifest(badManifest); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero raw key error = %v", err)
	}
	if _, err := store.GetManifest(""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty NZB ID error = %v", err)
	}
	set := testStoredSet(testSetID(0x81), testFileID(0x82), []byte("data"))
	set.Recovery = append(set.Recovery, set.Recovery[0])
	set.Recovery[1].PacketMD5 = md5.Sum([]byte("else"))
	if err := store.PutParsedSet("nzb-invalid", set); !errors.Is(err, ErrInvalid) {
		t.Fatalf("conflicting descriptor error = %v", err)
	}
	if err := store.PutPatch("nzb-invalid", set.SetID, set.Files[0].FileID, 0, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty patch error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Sync after Close = %v", err)
	}
	if stats := store.Stats(); !stats.Closed {
		t.Fatalf("Stats after Close = %+v", stats)
	}
	if _, err := store.ReadRepairedRange("nzb-invalid", SetID{}, FileID{}, 0, 0); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("empty read after Close = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	return store
}

func testManifest(nzbID string) *Manifest {
	manifest := NewManifest(nzbID, "Example Release")
	manifest.Files = []RawFile{
		{
			Key: 1, Ordinal: 0, Subject: "sample.bin (1/2)", SubjectFilename: "sample.bin",
			BaseFilename: "sample.bin", Date: 1_700_000_001, Groups: []string{"alt.binaries.test", "alt.binaries.example"},
			PostedBytes: 11, TotalSegments: 2, DetectedType: "media",
			Articles: []Article{
				{Number: 1, MessageID: "source-1@example.invalid", PostedBytes: 6, DecodedOffset: 0, DecodedSize: 4, Layout: LayoutExact},
				{Number: 2, MessageID: "source-2@example.invalid", PostedBytes: 5, DecodedOffset: 4, DecodedSize: 4, Layout: LayoutEstimated},
			},
		},
		{
			Key: 2, Ordinal: 1, Subject: "release.vol00+01.par2", SubjectFilename: "release.vol00+01.par2",
			BaseFilename: "release.vol00+01.par2", ActualFilename: "release.vol00+01.par2", Date: 1_700_000_002,
			Groups: []string{"alt.binaries.test"}, PostedBytes: 4, TotalSegments: 1, DetectedType: "par2", IsPAR2: true,
			Articles: []Article{{Number: 1, MessageID: "par2-1@example.invalid", PostedBytes: 4, DecodedOffset: 0, DecodedSize: 4, Layout: LayoutExact}},
		},
	}
	return manifest
}

func testStoredSet(setID SetID, fileID FileID, recoveryPayload []byte) StoredSet {
	first := []byte("abcd")
	second := []byte("efgh")
	return StoredSet{
		Version: StoredSetVersion, SetID: setID, SliceSize: 4,
		Files: []StoredSourceFile{{
			FileID: fileID, RawFile: 1, Name: "sample.bin", Length: 16,
			FullMD5: md5.Sum([]byte("abcdefghijklmnop")), First16KMD5: md5.Sum([]byte("abcdefghijklmnop")),
			SliceChecksums: []SliceChecksum{
				{MD5: md5.Sum(first), CRC32: 0x10203040},
				{MD5: md5.Sum(second), CRC32: 0x50607080},
				{MD5: md5.Sum([]byte("ijkl")), CRC32: 0x90a0b0c0},
				{MD5: md5.Sum([]byte("mnop")), CRC32: 0xd0e0f000},
			},
		}},
		Recovery: []RecoverySliceDescriptor{{
			Exponent: 7, RawFile: 2, PayloadOffset: 68, PayloadLength: 4, PacketMD5: recoveryPacketMD5(setID, 7, recoveryPayload),
		}},
	}
}

func testSetID(value byte) SetID {
	var id SetID
	for i := range id {
		id[i] = value + byte(i)
	}
	return id
}

func testFileID(value byte) FileID {
	var id FileID
	for i := range id {
		id[i] = value + byte(i)
	}
	return id
}

type comparableManifest struct {
	Version uint32
	NZBID   string
	Name    string
	Files   []RawFile
}

func manifestFields(manifest *Manifest) comparableManifest {
	manifest.mu.RLock()
	defer manifest.mu.RUnlock()
	files := make([]RawFile, len(manifest.Files))
	for i := range manifest.Files {
		files[i] = cloneRawFile(manifest.Files[i])
	}
	return comparableManifest{Version: manifest.Version, NZBID: manifest.NZBID, Name: manifest.Name, Files: files}
}

func corruptStoreValue(t *testing.T, store *Store, key, nzbID, kind string) {
	t.Helper()
	value, err := store.db.Get(key)
	if err != nil {
		t.Fatalf("Get corruption target %q: %v", key, err)
	}
	value[len(value)-1] ^= 0xff
	if err := store.db.Put(key, value, &appendstore.PutOptions{Attributes: map[string]string{
		attributeNZBID: nzbID,
		attributeKind:  kind,
	}}); err != nil {
		t.Fatalf("replace corruption target %q: %v", key, err)
	}
}

func assertCorrupt(t *testing.T, err error) {
	t.Helper()
	var corruption *CorruptionError
	if !errors.Is(err, ErrCorrupt) || !errors.As(err, &corruption) {
		t.Fatalf("error = %v, want typed ErrCorrupt", err)
	}
}
