package recovery

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/par2test"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

type memoryArticleRangeSource struct {
	rawFileKey uint32
	messageID  string
	offset     int64
	data       []byte
}

func (s memoryArticleRangeSource) HasArticleRange(rawFileKey uint32, messageID string, rawOffset, length int64) bool {
	return rawFileKey == s.rawFileKey && messageID == s.messageID && rawOffset >= s.offset && length >= 0 && rawOffset-s.offset <= int64(len(s.data))-length
}

func (s memoryArticleRangeSource) ReadArticleRange(rawFileKey uint32, messageID string, rawOffset int64, dst []byte) (bool, error) {
	if !s.HasArticleRange(rawFileKey, messageID, rawOffset, int64(len(dst))) {
		return false, nil
	}
	start := rawOffset - s.offset
	copy(dst, s.data[start:start+int64(len(dst))])
	return true, nil
}

func TestCoordinatorRecoversArticleRangeAndReusesPatch(t *testing.T) {
	testCoordinatorRange(t, 0)
	testCoordinatorRange(t, 11) // archive member prefix/cropping path
}

func TestCoordinatorReusesCachedSourceWithoutNNTPBody(t *testing.T) {
	const sliceSize = 64
	target := testBytes(sliceSize, 31)
	healthy := testBytes(sliceSize, 117)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	store, _ := coordinatorFixture(t, [][]byte{target, healthy}, [][]byte{parity})
	var calls atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		return nil, nil, errors.New("unexpected NNTP BODY")
	}))
	if err != nil {
		t.Fatal(err)
	}
	segment := reader.SegmentMeta{MessageID: "missing", Number: 1, RawFileKey: 1, RawOffset: 7, RawLength: 23}
	source := memoryArticleRangeSource{rawFileKey: 2, messageID: "healthy", data: healthy}
	got, err := coordinator.RecoverArticleWithSource(context.Background(), "fixture", segment, source)
	if err != nil {
		t.Fatalf("RecoverArticleWithSource: %v", err)
	}
	if !slices.Equal(got, target[7:30]) {
		t.Fatalf("cached-source repair=%x, want %x", got, target[7:30])
	}
	stats := coordinator.Stats()
	if calls.Load() != 0 || stats.BODYCalls != 0 || stats.ModeledDownloadBytes != 0 || stats.LocalSourceBytes == 0 {
		t.Fatalf("cached-source stats=%+v fetch calls=%d", stats, calls.Load())
	}
}

func TestCoordinatorSerializesDifferentRangesWithinOneNZB(t *testing.T) {
	const sliceSize = 64
	target := testBytes(sliceSize, 41)
	healthy := testBytes(sliceSize, 131)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	store, _ := coordinatorFixture(t, [][]byte{target, healthy}, [][]byte{parity})
	var active, maximum atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1 << 20}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		current := active.Add(1)
		for observed := maximum.Load(); current > observed && !maximum.CompareAndSwap(observed, current); observed = maximum.Load() {
		}
		defer active.Add(-1)
		time.Sleep(20 * time.Millisecond)
		return slices.Clone(healthy), &nntp.YencMetadata{Name: "healthy.bin", Size: sliceSize, Offset: 0, PartSize: sliceSize}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for _, offset := range []int64{2, 34} {
		go func() {
			_, recoverErr := coordinator.RecoverArticle(context.Background(), "fixture", reader.SegmentMeta{RawFileKey: 1, RawOffset: offset, RawLength: 8})
			errs <- recoverErr
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 1 {
		t.Fatalf("same-NZB BODY concurrency=%d, want 1", maximum.Load())
	}
}

func testCoordinatorRange(t *testing.T, dataStart int64) {
	t.Helper()
	const sliceSize = 64
	target := testBytes(sliceSize, 3)
	healthy := testBytes(sliceSize, 101)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	store, set := coordinatorFixture(t, [][]byte{target, healthy}, [][]byte{parity})

	var calls atomic.Int64
	fetch := func(_ context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		if messageID != "healthy" {
			t.Fatalf("unexpected BODY %q", messageID)
		}
		return slices.Clone(healthy), &nntp.YencMetadata{Name: "healthy.bin", Size: sliceSize, Offset: 0, PartSize: sliceSize}, nil
	}
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1 << 20}, WithFetchFunc(fetch))
	if err != nil {
		t.Fatal(err)
	}
	segment := reader.SegmentMeta{MessageID: "missing", Number: 1, RawFileKey: 1, RawOffset: 7, RawLength: 23, SegmentDataStart: dataStart}
	got, err := coordinator.RecoverArticle(context.Background(), "fixture", segment)
	if err != nil {
		t.Fatalf("RecoverArticle: %v", err)
	}
	if len(got) != int(dataStart+segment.RawLength) || !slices.Equal(got[dataStart:], target[7:30]) {
		t.Fatalf("article-shaped repair mismatch: len=%d data=%x", len(got), got)
	}
	if calls.Load() != 1 {
		t.Fatalf("BODY calls=%d, want one healthy source article", calls.Load())
	}

	// The exact raw patch is authoritative on subsequent reads, including in a
	// fresh operation with no article cache.
	got, err = coordinator.RecoverArticle(context.Background(), "fixture", segment)
	if err != nil {
		t.Fatalf("patch RecoverArticle: %v", err)
	}
	if !slices.Equal(got[dataStart:], target[7:30]) || calls.Load() != 1 {
		t.Fatalf("patch reuse got %x, BODY calls=%d", got, calls.Load())
	}
	if stats := coordinator.Stats(); stats.PatchHits != 1 || stats.RecoveryPayloadBytes != 0 {
		t.Fatalf("unexpected coordinator stats: %+v", stats)
	}
	if _, err := store.GetParsedSet("fixture", set.SetID); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorAtomicallyRejectsOverBudgetPlan(t *testing.T) {
	const sliceSize = 64
	target := testBytes(sliceSize, 9)
	healthy := testBytes(sliceSize, 77)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	store, _ := coordinatorFixture(t, [][]byte{target, healthy}, [][]byte{parity})
	var calls atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 99}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		return healthy, &nntp.YencMetadata{Name: "healthy.bin", Size: sliceSize, Offset: 0, PartSize: sliceSize}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.RecoverArticle(context.Background(), "fixture", reader.SegmentMeta{RawFileKey: 1, RawOffset: 4, RawLength: 8})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("error=%v, want ErrBudgetExceeded", err)
	}
	if calls.Load() != 0 || coordinator.Stats().BODYCalls != 0 {
		t.Fatalf("over-budget plan issued BODY: fetch=%d stats=%d", calls.Load(), coordinator.Stats().BODYCalls)
	}
}

func TestAlignRecoveryRangesIncludesGF16EdgeBytes(t *testing.T) {
	got := alignRecoveryRanges([]par2.ByteRange{{Offset: 7, Length: 23}, {Offset: 63, Length: 1}}, 64)
	want := []par2.ByteRange{{Offset: 6, Length: 24}, {Offset: 62, Length: 2}}
	if !slices.Equal(got, want) {
		t.Fatalf("aligned ranges=%v, want %v", got, want)
	}
}

func TestCoordinatorRejectsPolicyAboveHardTrafficCeiling(t *testing.T) {
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	_, err = NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadPercent: maxAutomaticDownloadPercent + 1})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewCoordinator error=%v, want ErrInvalid", err)
	}
}

func TestCoordinatorColdStartFetchesSmallestBaseMinimalVolumeAndSource(t *testing.T) {
	fixture := newColdCoordinatorFixture(t)
	coordinator, calls := fixture.coordinator(t, 1<<20)
	segment := reader.SegmentMeta{MessageID: "target-missing", Number: 1, RawFileKey: 1, RawOffset: 9, RawLength: 19, SegmentDataStart: 4}
	got, err := coordinator.RecoverArticle(context.Background(), "cold", segment)
	if err != nil {
		t.Fatalf("RecoverArticle: %v", err)
	}
	if !slices.Equal(got[4:], fixture.target[9:28]) {
		t.Fatalf("cold repair=%x, want %x", got[4:], fixture.target[9:28])
	}
	if want := []string{"base", "volume", "healthy"}; !slices.Equal(*calls, want) {
		t.Fatalf("BODY order=%v, want %v", *calls, want)
	}
	sets, err := fixture.store.ListParsedSets("cold")
	if err != nil || len(sets) != 1 || len(sets[0].Recovery) != 1 {
		t.Fatalf("persisted parsed set=%+v err=%v", sets, err)
	}
	if payload, err := fixture.store.GetRecoverySlice("cold", sets[0].SetID, 0); err != nil || !slices.Equal(payload, fixture.parity) {
		t.Fatalf("persisted recovery payload mismatch: err=%v", err)
	}
}

func TestCoordinatorColdStartOverBudgetStopsAfterBase(t *testing.T) {
	fixture := newColdCoordinatorFixture(t)
	// The base bootstrap itself fits. The atomically planned volume+healthy
	// source batch does not, so neither of those BODY calls may begin.
	coordinator, calls := fixture.coordinator(t, fixture.basePosted+fixture.healthyPosted)
	_, err := coordinator.RecoverArticle(context.Background(), "cold", reader.SegmentMeta{RawFileKey: 1, RawOffset: 2, RawLength: 8})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("error=%v, want ErrBudgetExceeded", err)
	}
	if want := []string{"base"}; !slices.Equal(*calls, want) {
		t.Fatalf("over-budget cold-start BODY calls=%v, want base bootstrap only", *calls)
	}
}

func TestCoordinatorPreflightsAllMainMappingProbesBeforeFirstProbe(t *testing.T) {
	dataA, dataB := testBytes(64, 5), testBytes(64, 91)
	setID := SetID{2, 4, 6, 8}
	idA, idB := coordinatorFileID(dataA, "a.bin"), coordinatorFileID(dataB, "b.bin")
	mainBody := make([]byte, 44)
	binary.LittleEndian.PutUint64(mainBody[:8], 64)
	binary.LittleEndian.PutUint32(mainBody[8:12], 2)
	copy(mainBody[12:28], idA[:])
	copy(mainBody[28:44], idB[:])
	base := slices.Concat(
		coordinatorPacket(setID, "PAR 2.0\x00Main\x00\x00\x00\x00", mainBody),
		coordinatorFileDescriptionPacket(setID, idA, dataA, "a.bin"),
		coordinatorFileDescriptionPacket(setID, idB, dataB, "b.bin"),
	)
	basePosted := int64(len(base) + 20)
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	manifest := &Manifest{Version: ManifestVersion, NZBID: "mapping-budget", Files: []RawFile{
		{Key: 1, BaseFilename: "a.bin", Articles: []Article{{MessageID: "a-1", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutEstimated}}},
		{Key: 2, BaseFilename: "a.bin", Articles: []Article{{MessageID: "a-2", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutEstimated}}},
		{Key: 3, BaseFilename: "b.bin", Articles: []Article{{MessageID: "b-1", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutEstimated}}},
		{Key: 4, BaseFilename: "b.bin", Articles: []Article{{MessageID: "b-2", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutEstimated}}},
		{Key: 5, BaseFilename: "release.par2", IsPAR2: true, Articles: []Article{{MessageID: "base", PostedBytes: basePosted, DecodedOffset: 0, DecodedSize: int64(len(base)), Layout: LayoutExact}}},
	}}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	var calls []string
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: basePosted + 399}, WithFetchFunc(func(_ context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
		calls = append(calls, messageID)
		if messageID != "base" {
			t.Fatalf("mapping probe %q started before union budget approval", messageID)
		}
		return slices.Clone(base), &nntp.YencMetadata{Name: "release.par2", Size: int64(len(base)), Offset: 0, PartSize: int64(len(base))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.RecoverArticle(context.Background(), "mapping-budget", reader.SegmentMeta{RawFileKey: 1, RawLength: 8})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("error=%v, want ErrBudgetExceeded", err)
	}
	if want := []string{"base"}; !slices.Equal(calls, want) {
		t.Fatalf("BODY calls=%v, want base only", calls)
	}
}

func TestCoordinatorDynamicallyAddsCorruptSourceShard(t *testing.T) {
	const sliceSize = 64
	target := testBytes(sliceSize, 11)
	corrupt := testBytes(sliceSize, 199)
	parity := par2test.RecoverySet([][]byte{target, corrupt}, 2)
	store, _ := coordinatorFixture(t, [][]byte{target, corrupt}, parity)
	var calls atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1 << 20}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		return nil, nil, nntp.NewYencDecodeError(errors.New("CRC mismatch"))
	}))
	if err != nil {
		t.Fatal(err)
	}
	segment := reader.SegmentMeta{RawFileKey: 1, RawOffset: 13, RawLength: 17, SegmentDataStart: 5}
	got, err := coordinator.RecoverArticle(context.Background(), "fixture", segment)
	if err != nil {
		t.Fatalf("RecoverArticle: %v", err)
	}
	if !slices.Equal(got[5:], target[13:30]) || calls.Load() != 1 {
		t.Fatalf("dynamic corruption recovery mismatch, calls=%d data=%x", calls.Load(), got)
	}
}

func TestCoordinatorRepairsFinalPartialSliceWithZeroPaddedSources(t *testing.T) {
	const sliceSize = 64
	targetFile := testBytes(70, 41)
	healthyFile := testBytes(67, 131)
	shards := [][]byte{
		slices.Clone(targetFile[:64]), append(slices.Clone(targetFile[64:]), make([]byte, 58)...),
		slices.Clone(healthyFile[:64]), append(slices.Clone(healthyFile[64:]), make([]byte, 61)...),
	}
	parity := par2test.Recovery(shards, 0)
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	manifest := &Manifest{Version: ManifestVersion, NZBID: "tails", Files: []RawFile{
		{Key: 1, BaseFilename: "target.bin", Articles: []Article{
			{Number: 1, MessageID: "target-good", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutExact},
			{Number: 2, MessageID: "target-missing", PostedBytes: 100, DecodedOffset: 64, DecodedSize: 6, Layout: LayoutExact},
		}},
		{Key: 2, BaseFilename: "healthy.bin", Articles: []Article{
			{Number: 1, MessageID: "healthy-full", PostedBytes: 100, DecodedOffset: 0, DecodedSize: 64, Layout: LayoutExact},
			{Number: 2, MessageID: "healthy-tail", PostedBytes: 100, DecodedOffset: 64, DecodedSize: 3, Layout: LayoutExact},
		}},
	}}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	set := StoredSet{Version: StoredSetVersion, SetID: SetID{4}, SliceSize: sliceSize, Files: []StoredSourceFile{
		{FileID: FileID{1}, RawFile: 1, Name: "target.bin", Length: 70},
		{FileID: FileID{2}, RawFile: 2, Name: "healthy.bin", Length: 67},
	}, Recovery: []RecoverySliceDescriptor{{Exponent: 0, RawFile: 3, PayloadLength: sliceSize, PacketMD5: recoveryPacketMD5(SetID{4}, 0, parity)}}}
	if err := store.PutParsedSet("tails", set); err != nil {
		t.Fatal(err)
	}
	if err := store.PutRecoverySlice("tails", set.SetID, 0, parity); err != nil {
		t.Fatal(err)
	}
	bodies := map[string]struct {
		data   []byte
		name   string
		size   int64
		offset int64
	}{
		"target-good":  {targetFile[:64], "target.bin", 70, 0},
		"healthy-full": {healthyFile[:64], "healthy.bin", 67, 0},
		"healthy-tail": {healthyFile[64:], "healthy.bin", 67, 64},
	}
	var calls atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1 << 20}, WithFetchFunc(func(_ context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		body, ok := bodies[messageID]
		if !ok {
			t.Fatalf("unexpected BODY %q", messageID)
		}
		return slices.Clone(body.data), &nntp.YencMetadata{Name: body.name, Size: body.size, Offset: body.offset, PartSize: int64(len(body.data))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	segment := reader.SegmentMeta{RawFileKey: 1, RawOffset: 65, RawLength: 5, SegmentDataStart: 9}
	got, err := coordinator.RecoverArticle(context.Background(), "tails", segment)
	if err != nil {
		t.Fatalf("RecoverArticle: %v", err)
	}
	if !slices.Equal(got[9:], targetFile[65:70]) {
		t.Fatalf("partial-tail repair=%x, want %x", got[9:], targetFile[65:70])
	}
	if calls.Load() != 3 {
		t.Fatalf("BODY calls=%d, want only three planned healthy ranges", calls.Load())
	}
}

func TestCoordinatorLegacyManifestAndNilBodyObservation(t *testing.T) {
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		t.Fatal("legacy recovery must not fetch")
		return nil, nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.RecoverArticle(context.Background(), "legacy", reader.SegmentMeta{RawFileKey: 1, RawLength: 1})
	if !errors.Is(err, ErrLegacyManifestUnsupported) {
		t.Fatalf("legacy error=%v", err)
	}

	manifest := &Manifest{Version: ManifestVersion, NZBID: "observed", Files: []RawFile{{Key: 1, Articles: []Article{{Number: 0, MessageID: "article", PostedBytes: 100}}}}}
	if err := coordinator.RegisterManifest(manifest); err != nil {
		t.Fatal(err)
	}
	coordinator.ObserveArticle(context.Background(), "observed", reader.SegmentMeta{RawFileKey: 1, Number: 0}, nil,
		&nntp.YencMetadata{Name: "movie.mkv", Size: 400, Offset: 20, PartSize: 30})
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetManifest("observed")
	if err != nil {
		t.Fatal(err)
	}
	file, ok := stored.File(1)
	if !ok || file.Articles[0].Layout != LayoutExact || file.Articles[0].DecodedOffset != 20 || file.ActualFilename != "movie.mkv" {
		t.Fatalf("nil-body observation was not persisted: %+v", file)
	}
}

func TestCoordinatorSharesAndPersistsParserManifestEnrichment(t *testing.T) {
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{Version: ManifestVersion, NZBID: "parser-enrichment", Files: []RawFile{{
		Key: 1, Articles: []Article{{Number: 1, MessageID: "part", PostedBytes: 100}},
	}}}
	if err := coordinator.RegisterManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.UpdateClassification(1, "rar", "release.part01.rar", false)
	manifest.UpdateArticleLayout(1, 1, 200, 50, LayoutExact)
	live, err := coordinator.manifest(manifest.NZBID)
	if err != nil {
		t.Fatal(err)
	}
	file, _ := live.File(1)
	if file.ActualFilename != "release.part01.rar" || file.Articles[0].DecodedOffset != 200 {
		t.Fatalf("coordinator did not see live parser enrichment: %+v", file)
	}
	if err := coordinator.RegisterManifest(manifest); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetManifest(manifest.NZBID)
	if err != nil {
		t.Fatal(err)
	}
	file, _ = stored.File(1)
	if file.ActualFilename != "release.part01.rar" || file.Articles[0].Layout != LayoutExact || file.Articles[0].DecodedOffset != 200 {
		t.Fatalf("parser enrichment was not persisted: %+v", file)
	}
}

func TestCoordinatorDeleteNZBInvalidatesDirtyManifest(t *testing.T) {
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var calls atomic.Int64
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		calls.Add(1)
		return nil, nil, errors.New("unexpected fetch")
	}))
	if err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{Version: ManifestVersion, NZBID: "delete-me", Files: []RawFile{{Key: 1, Articles: []Article{{Number: 1, MessageID: "gone", PostedBytes: 100}}}}}
	if err := coordinator.RegisterManifest(manifest); err != nil {
		t.Fatal(err)
	}
	coordinator.ObserveArticle(context.Background(), "delete-me", reader.SegmentMeta{RawFileKey: 1, Number: 1}, nil,
		&nntp.YencMetadata{Name: "gone.bin", Size: 10, Offset: 0, PartSize: 10})
	if err := coordinator.DeleteNZB("delete-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RecoverArticle(context.Background(), "delete-me", reader.SegmentMeta{RawFileKey: 1, RawLength: 1}); !errors.Is(err, ErrLegacyManifestUnsupported) {
		t.Fatalf("recovery after delete error=%v", err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetManifest("delete-me"); !errors.Is(err, ErrLegacyManifestUnsupported) {
		t.Fatalf("Close resurrected deleted manifest: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("delete path issued %d BODY calls", calls.Load())
	}
}

func TestCoordinatorDeleteWaitsForInflightRepairAndRemovesItsWrites(t *testing.T) {
	const sliceSize = 64
	target, healthy := testBytes(sliceSize, 17), testBytes(sliceSize, 89)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	store, _ := coordinatorFixture(t, [][]byte{target, healthy}, [][]byte{parity})
	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxDownloadBytes: 1 << 20}, WithFetchFunc(func(context.Context, string) ([]byte, *nntp.YencMetadata, error) {
		close(fetchStarted)
		<-releaseFetch
		return slices.Clone(healthy), &nntp.YencMetadata{Name: "healthy.bin", Size: sliceSize, Offset: 0, PartSize: sliceSize}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	repairDone := make(chan error, 1)
	go func() {
		_, repairErr := coordinator.RecoverArticle(context.Background(), "fixture", reader.SegmentMeta{RawFileKey: 1, RawOffset: 3, RawLength: 12})
		repairDone <- repairErr
	}()
	<-fetchStarted
	deleteStarted := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		close(deleteStarted)
		deleteDone <- coordinator.DeleteNZB("fixture")
	}()
	<-deleteStarted
	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteNZB returned before in-flight repair: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	close(releaseFetch)
	if err := <-repairDone; err != nil {
		t.Fatalf("in-flight repair: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteNZB: %v", err)
	}
	if stats := store.Stats(); stats.Entries != 0 {
		t.Fatalf("orphaned recovery entries after delete: %+v", stats)
	}
}

func TestBasePAR2FilesFallsBackToSmallestUnparsedVolume(t *testing.T) {
	manifest := &Manifest{Version: ManifestVersion, NZBID: "volumes", Files: []RawFile{
		{Key: 1, Ordinal: 0, BaseFilename: "set.vol00+01.par2", IsPAR2: true, PostedBytes: 900},
		{Key: 2, Ordinal: 1, BaseFilename: "set.vol01+02.par2", IsPAR2: true, PostedBytes: 400},
	}}
	op := &repairOperation{manifest: manifest, parsedRaw: map[RawFileKey]bool{}}
	files := op.basePAR2Files()
	if len(files) != 2 || files[0].Key != 2 {
		t.Fatalf("volume-only bootstrap order=%v", rawKeys(files))
	}
	op.parsedRaw[2] = true
	files = op.basePAR2Files()
	if len(files) != 1 || files[0].Key != 1 {
		t.Fatalf("parsed volume selected again: %v", rawKeys(files))
	}
}

func TestSelectVolumesMinimizesPostedBytesAcrossStandardVolumes(t *testing.T) {
	manifest := &Manifest{Version: ManifestVersion, NZBID: "minimum-volumes", Files: []RawFile{
		{Key: 1, Ordinal: 0, BaseFilename: "set.vol00+01.par2", IsPAR2: true, Articles: []Article{{MessageID: "row-1", PostedBytes: 100}}},
		{Key: 2, Ordinal: 1, BaseFilename: "set.vol01+02.par2", IsPAR2: true, Articles: []Article{{MessageID: "rows-2", PostedBytes: 200}}},
		{Key: 3, Ordinal: 2, BaseFilename: "set.vol03+04.par2", IsPAR2: true, Articles: []Article{{MessageID: "rows-4", PostedBytes: 400}}},
	}}
	op := &repairOperation{nzbID: manifest.NZBID, manifest: manifest, parsedRaw: make(map[RawFileKey]bool)}
	selected, err := op.selectVolumes(StoredSet{}, 3)
	if err != nil {
		t.Fatalf("selectVolumes: %v", err)
	}
	if want := []RawFileKey{1, 2}; !slices.Equal(rawKeys(selected), want) {
		t.Fatalf("selected volumes=%v, want %v", rawKeys(selected), want)
	}
}

func TestSelectVolumesSkipsUnboundedCandidateWhenBoundedAlternativeExists(t *testing.T) {
	manifest := &Manifest{Version: ManifestVersion, NZBID: "bounded-volume", Files: []RawFile{
		{Key: 1, Ordinal: 0, BaseFilename: "set.vol00+02.par2", IsPAR2: true, Articles: []Article{{MessageID: "unknown-size"}}},
		{Key: 2, Ordinal: 1, BaseFilename: "set.vol02+01.par2", IsPAR2: true, Articles: []Article{{MessageID: "bounded", PostedBytes: 100}}},
	}}
	op := &repairOperation{nzbID: manifest.NZBID, manifest: manifest, parsedRaw: make(map[RawFileKey]bool)}
	selected, err := op.selectVolumes(StoredSet{}, 1)
	if err != nil {
		t.Fatalf("selectVolumes: %v", err)
	}
	if want := []RawFileKey{2}; !slices.Equal(rawKeys(selected), want) {
		t.Fatalf("selected volumes=%v, want bounded %v", rawKeys(selected), want)
	}
}

func TestTrafficMeterDeduplicatesLiteralManifestMessageIDs(t *testing.T) {
	manifest := &Manifest{Version: ManifestVersion, NZBID: "duplicate-denominator", Files: []RawFile{
		{Key: 1, Articles: []Article{{MessageID: "same", PostedBytes: 100}}},
		{Key: 2, Articles: []Article{{MessageID: "same", PostedBytes: 120}}},
		{Key: 3, PostedBytes: 30}, // fallback only because it has no sized article
	}}
	meter := newTrafficMeter(Policy{MaxDownloadPercent: 10}, manifest)
	if meter.limit != 15 { // max duplicate (120) + fallback (30), then 10%
		t.Fatalf("percentage limit=%d, want 15", meter.limit)
	}
}

func TestStoredSetStorageEstimateAccountsForIFSCPayload(t *testing.T) {
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	set := StoredSet{Version: StoredSetVersion, SetID: SetID{1}, SliceSize: 64, Files: []StoredSourceFile{{
		FileID: FileID{1}, RawFile: 1, Name: "large-ifsc.bin", Length: 64 * 200, SliceChecksums: make([]SliceChecksum, 200),
	}}}
	estimate := storedSetStorageEstimate(set, map[FileID][]RawFileKey{FileID{1}: {1, 2}})
	withoutChecksums := int64(256 + 192 + len("large-ifsc.bin") + 16)
	if estimate <= withoutChecksums+20*200 {
		t.Fatalf("estimate=%d does not conservatively include IFSC/checksum framing", estimate)
	}
	base := store.Stats().DiskBytes
	coordinator, err := NewCoordinator(store, nil, Policy{Enabled: true, MaxStorageBytes: base + estimate - 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.checkStorage(estimate); !errors.Is(err, ErrStorageBudget) {
		t.Fatalf("under-cap storage check=%v, want ErrStorageBudget", err)
	}
}

func TestStoredSourceAliasesDuplicateSubjectButRejectsUnrelatedSameName(t *testing.T) {
	set := StoredSet{Version: StoredSetVersion, SetID: SetID{1}, SliceSize: 64, Files: []StoredSourceFile{{
		FileID: FileID{7}, RawFile: 1, Name: "joined.bin", Length: 64,
	}}}
	manifest := &Manifest{Version: ManifestVersion, NZBID: "aliases", Files: []RawFile{
		{Key: 1, Subject: "same post", BaseFilename: "joined.bin", Articles: []Article{{MessageID: "part-1", DecodedOffset: 0, DecodedSize: 32, Layout: LayoutEstimated}}},
		{Key: 2, Subject: "same post", BaseFilename: "joined.bin", Articles: []Article{{MessageID: "part-2", DecodedOffset: 32, DecodedSize: 32, Layout: LayoutEstimated}}},
	}}
	op := &repairOperation{manifest: manifest, aliases: make(map[FileID][]RawFileKey)}
	binding, ok, err := op.findTarget([]StoredSet{set}, 2)
	if err != nil || !ok || binding.source.FileID != (FileID{7}) {
		t.Fatalf("duplicate-subject alias was not resolved: binding=%+v ok=%t err=%v", binding, ok, err)
	}

	manifest.Files[1].Subject = "unrelated post"
	manifest.Files[1].Articles[0].DecodedOffset = 0
	manifest.Files[1].Articles[0].Layout = LayoutExact
	manifest.Files[0].Articles[0].Layout = LayoutExact
	op.aliases = make(map[FileID][]RawFileKey)
	_, _, err = op.findTarget([]StoredSet{set}, 2)
	if !errors.Is(err, ErrAmbiguousMapping) {
		t.Fatalf("unrelated same-name mapping error=%v, want ErrAmbiguousMapping", err)
	}
}

func coordinatorFixture(t *testing.T, data, parity [][]byte) (*Store, StoredSet) {
	t.Helper()
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	manifest := &Manifest{Version: ManifestVersion, NZBID: "fixture", Files: []RawFile{
		{Key: 1, Ordinal: 0, BaseFilename: "target.bin", PostedBytes: 100, Articles: []Article{{Number: 1, MessageID: "missing", PostedBytes: 100, DecodedOffset: 0, DecodedSize: int64(len(data[0])), Layout: LayoutExact}}},
		{Key: 2, Ordinal: 1, BaseFilename: "healthy.bin", PostedBytes: 100, Articles: []Article{{Number: 1, MessageID: "healthy", PostedBytes: 100, DecodedOffset: 0, DecodedSize: int64(len(data[1])), Layout: LayoutExact}}},
	}}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	set := StoredSet{Version: StoredSetVersion, SetID: SetID{1}, SliceSize: uint64(len(data[0])), Files: []StoredSourceFile{
		{FileID: FileID{1}, RawFile: 1, Name: "target.bin", Length: uint64(len(data[0]))},
		{FileID: FileID{2}, RawFile: 2, Name: "healthy.bin", Length: uint64(len(data[1]))},
	}}
	for exponent, payload := range parity {
		set.Recovery = append(set.Recovery, RecoverySliceDescriptor{Exponent: uint32(exponent), RawFile: RawFileKey(10 + exponent), PayloadLength: uint64(len(payload)), PacketMD5: recoveryPacketMD5(set.SetID, uint32(exponent), payload)})
	}
	if err := store.PutParsedSet("fixture", set); err != nil {
		t.Fatal(err)
	}
	for exponent, payload := range parity {
		if err := store.PutRecoverySlice("fixture", set.SetID, uint32(exponent), payload); err != nil {
			t.Fatal(err)
		}
	}
	return store, set
}

type coldCoordinatorFixture struct {
	store                     *Store
	target, healthy, parity   []byte
	base, volume              []byte
	basePosted, healthyPosted int64
	volumePosted              int64
}

func newColdCoordinatorFixture(t *testing.T) *coldCoordinatorFixture {
	t.Helper()
	const sliceSize = 64
	target := testBytes(sliceSize, 23)
	healthy := testBytes(sliceSize, 151)
	parity := par2test.Recovery([][]byte{target, healthy}, 0)
	setID := SetID{8, 7, 6, 5}
	targetID := coordinatorFileID(target, "target.bin")
	healthyID := coordinatorFileID(healthy, "healthy.bin")
	mainBody := make([]byte, 12+32)
	binary.LittleEndian.PutUint64(mainBody[:8], sliceSize)
	binary.LittleEndian.PutUint32(mainBody[8:12], 2)
	copy(mainBody[12:28], targetID[:])
	copy(mainBody[28:44], healthyID[:])
	base := slices.Concat(
		coordinatorPacket(setID, "PAR 2.0\x00Main\x00\x00\x00\x00", mainBody),
		coordinatorFileDescriptionPacket(setID, targetID, target, "target.bin"),
		coordinatorFileDescriptionPacket(setID, healthyID, healthy, "healthy.bin"),
	)
	recoveryBody := make([]byte, 4+len(parity))
	binary.LittleEndian.PutUint32(recoveryBody[:4], 0)
	copy(recoveryBody[4:], parity)
	volume := coordinatorPacket(setID, "PAR 2.0\x00RecvSlic", recoveryBody)
	store, err := Open(t.TempDir() + "/par2.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	basePosted, volumePosted, healthyPosted := int64(len(base)+40), int64(len(volume)+40), int64(100)
	manifest := &Manifest{Version: ManifestVersion, NZBID: "cold", Files: []RawFile{
		{Key: 1, Ordinal: 0, BaseFilename: "target.bin", Articles: []Article{{Number: 1, MessageID: "target-missing", PostedBytes: 100, DecodedOffset: 0, DecodedSize: sliceSize, Layout: LayoutExact}}},
		{Key: 2, Ordinal: 1, BaseFilename: "healthy.bin", Articles: []Article{{Number: 1, MessageID: "healthy", PostedBytes: healthyPosted, DecodedOffset: 0, DecodedSize: sliceSize, Layout: LayoutExact}}},
		{Key: 3, Ordinal: 2, BaseFilename: "release.par2", IsPAR2: true, Articles: []Article{{Number: 1, MessageID: "base", PostedBytes: basePosted, DecodedOffset: 0, DecodedSize: int64(len(base)), Layout: LayoutExact}}},
		{Key: 4, Ordinal: 3, BaseFilename: "release.vol00+01.par2", IsPAR2: true, Articles: []Article{{Number: 1, MessageID: "volume", PostedBytes: volumePosted, DecodedOffset: 0, DecodedSize: int64(len(volume)), Layout: LayoutExact}}},
		{Key: 5, Ordinal: 4, BaseFilename: "unrelated.bin", Articles: []Article{{Number: 1, MessageID: "unrelated", PostedBytes: 10_000, DecodedOffset: 0, DecodedSize: 9_000, Layout: LayoutEstimated}}},
	}}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	return &coldCoordinatorFixture{store: store, target: target, healthy: healthy, parity: parity, base: base, volume: volume, basePosted: basePosted, volumePosted: volumePosted, healthyPosted: healthyPosted}
}

func (f *coldCoordinatorFixture) coordinator(t *testing.T, limit int64) (*Coordinator, *[]string) {
	t.Helper()
	calls := new([]string)
	coordinator, err := NewCoordinator(f.store, nil, Policy{Enabled: true, MaxDownloadBytes: limit}, WithFetchFunc(func(_ context.Context, messageID string) ([]byte, *nntp.YencMetadata, error) {
		*calls = append(*calls, messageID)
		switch messageID {
		case "base":
			return slices.Clone(f.base), &nntp.YencMetadata{Name: "release.par2", Size: int64(len(f.base)), Offset: 0, PartSize: int64(len(f.base))}, nil
		case "volume":
			return slices.Clone(f.volume), &nntp.YencMetadata{Name: "release.vol00+01.par2", Size: int64(len(f.volume)), Offset: 0, PartSize: int64(len(f.volume))}, nil
		case "healthy":
			return slices.Clone(f.healthy), &nntp.YencMetadata{Name: "healthy.bin", Size: int64(len(f.healthy)), Offset: 0, PartSize: int64(len(f.healthy))}, nil
		default:
			t.Fatalf("unexpected BODY %q", messageID)
			return nil, nil, errors.New("unexpected BODY")
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, calls
}

func coordinatorFileID(data []byte, filename string) FileID {
	first16K := md5.Sum(data[:min(len(data), 16<<10)])
	hash := md5.New()
	_, _ = hash.Write(first16K[:])
	var encodedLength [8]byte
	binary.LittleEndian.PutUint64(encodedLength[:], uint64(len(data)))
	_, _ = hash.Write(encodedLength[:])
	_, _ = hash.Write([]byte(filename))
	var result FileID
	copy(result[:], hash.Sum(nil))
	return result
}

func coordinatorFileDescriptionPacket(setID SetID, fileID FileID, data []byte, filename string) []byte {
	paddedNameLength := (len(filename) + 3) &^ 3
	body := make([]byte, 56+paddedNameLength)
	copy(body[:16], fileID[:])
	full := md5.Sum(data)
	first16K := md5.Sum(data[:min(len(data), 16<<10)])
	copy(body[16:32], full[:])
	copy(body[32:48], first16K[:])
	binary.LittleEndian.PutUint64(body[48:56], uint64(len(data)))
	copy(body[56:], filename)
	return coordinatorPacket(setID, "PAR 2.0\x00FileDesc", body)
}

func coordinatorPacket(setID SetID, packetType string, body []byte) []byte {
	packet := make([]byte, 64+len(body))
	copy(packet[:8], []byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'})
	binary.LittleEndian.PutUint64(packet[8:16], uint64(len(packet)))
	copy(packet[32:48], setID[:])
	copy(packet[48:64], packetType)
	copy(packet[64:], body)
	hash := md5.New()
	_, _ = hash.Write(packet[32:])
	copy(packet[16:32], hash.Sum(nil))
	return packet
}

func testBytes(length, seed int) []byte {
	result := make([]byte, length)
	for i := range result {
		result[i] = byte(seed + i*17)
	}
	return result
}
