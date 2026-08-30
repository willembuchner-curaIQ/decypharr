package parser

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

func TestProbeContentAvailabilityReportsAllMissingContent(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := map[string]*FileGroup{
		"b": {BaseName: "b", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "b@example"}}}}},
		"a": {BaseName: "a", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "a@example"}}}}},
	}
	missing := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "missing"}

	var calls []string
	err := p.probeContentAvailability(context.Background(), groups, func(_ context.Context, messageID string) error {
		calls = append(calls, messageID)
		return missing
	})
	if !errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("all-missing availability error = %v", err)
	}
	if want := []string{"a@example", "b@example"}; !slices.Equal(calls, want) {
		t.Fatalf("STAT calls = %v, want %v", calls, want)
	}
}

func TestProbeContentAvailabilityAcceptsAnotherAvailableGroup(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := map[string]*FileGroup{
		"a": {BaseName: "a", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "missing@example"}}}}},
		"b": {BaseName: "b", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "available@example"}}}}},
	}
	missing := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "missing"}
	err := p.probeContentAvailability(context.Background(), groups, func(_ context.Context, messageID string) error {
		if messageID == "missing@example" {
			return missing
		}
		return nil
	})
	if err != nil {
		t.Fatalf("availability probe = %v", err)
	}
}

func TestProbeContentAvailabilityStopsOnOperationalError(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := map[string]*FileGroup{
		"a": {BaseName: "a", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "a@example"}}}}},
		"b": {BaseName: "b", Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "b@example"}}}}},
	}
	wantErr := errors.New("authentication failed")
	calls := 0
	err := p.probeContentAvailability(context.Background(), groups, func(context.Context, string) error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("operational error probe = calls %d, err %v", calls, err)
	}
}

func TestHeaderProbeOrderCoversEveryPartOnce(t *testing.T) {
	if got := headerProbeOrder(0); len(got) != 0 {
		t.Fatalf("zero-part order = %v", got)
	}
	if got := headerProbeOrder(1); !slices.Equal(got, []int{0}) {
		t.Fatalf("one-part order = %v", got)
	}
	if got := headerProbeOrder(5); !slices.Equal(got, []int{0, 2, 4, 1, 3}) {
		t.Fatalf("five-part order = %v", got)
	}
}

func TestDecodedPartSizeDoesNotInventByteForMissingRange(t *testing.T) {
	if got := decodedPartSize(&nntp.YencMetadata{}); got != 0 {
		t.Fatalf("empty yEnc range size = %d, want 0", got)
	}
	if got := decodedPartSize(&nntp.YencMetadata{Begin: 101, End: 200}); got != 100 {
		t.Fatalf("inclusive yEnc range size = %d, want 100", got)
	}
	if got := decodedPartSize(&nntp.YencMetadata{PartSize: 75, Begin: 1, End: 100}); got != 75 {
		t.Fatalf("explicit yEnc part size = %d, want 75", got)
	}
}

func TestArchiveAndIgnoredFileTypeDetection(t *testing.T) {
	p := &NZBParser{}
	if got := p.detectFileTypeFromContent([]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}); got != storage.NZBFileTypeIgnore {
		t.Fatalf("parity data detected as %q, want ignored", got)
	}
	for _, name := range []string{"archive.r99", "archive.r123", "archive.s00", "archive.part100.rar"} {
		if got := p.detectFileType(name); got != storage.NZBFileTypeRar {
			t.Errorf("%q detected as %q, want rar", name, got)
		}
	}
	for _, name := range []string{"archive.z01", "archive.z001"} {
		if got := p.detectFileType(name); got != storage.NZBFileTypeZip {
			t.Errorf("%q detected as %q, want zip", name, got)
		}
	}
	if getZIPVolumeOrder("archive.z99") >= getZIPVolumeOrder("archive.z100") ||
		getZIPVolumeOrder("archive.z100") >= getZIPVolumeOrder("archive.zip") {
		t.Fatal("split ZIP volume ordering is not numeric with .zip last")
	}
}

func TestProcessMediaRebasesLogicalOffsets(t *testing.T) {
	first := nzbparser.NzbFile{Filename: "movie.mkv", Number: 1, Groups: []string{"alt.test"}, Segments: nzbparser.NzbSegments{{Number: 1, Id: "one@example", Bytes: 5}}}
	second := nzbparser.NzbFile{Filename: "movie.mkv", Number: 2, Groups: []string{"alt.test"}, Segments: nzbparser.NzbSegments{{Number: 1, Id: "two@example", Bytes: 4}}}
	group := &FileGroup{
		BaseName: "movie", ActualFilename: "movie.mkv", Type: storage.NZBFileTypeMedia,
		Files: []nzbparser.NzbFile{first, second}, metadata: &fileAnalysisResult{fileSize: 4, lastFileSize: 3, segmentSize: 4},
		Groups: map[string]struct{}{"alt.test": {}},
	}
	file := (&NZBParser{logger: zerolog.Nop()}).processMediaFile(group, "")
	if file == nil || len(file.Segments) != 2 {
		t.Fatalf("processed file = %#v", file)
	}
	if got := [2]int64{file.Segments[0].StartOffset, file.Segments[0].EndOffset}; got != [2]int64{0, 3} {
		t.Fatalf("first logical range = %v", got)
	}
	if got := [2]int64{file.Segments[1].StartOffset, file.Segments[1].EndOffset}; got != [2]int64{4, 6} {
		t.Fatalf("second logical range = %v", got)
	}
}

func TestArchiveSlicersRejectPartiallyCoveredRanges(t *testing.T) {
	base := []storage.NZBSegment{{Number: 1, MessageID: "article@example", Bytes: 10}}
	if _, err := sliceSegmentsForRangeSimple(base, 8, 4); err == nil {
		t.Fatal("simple archive slicer accepted a truncated range")
	}
	volumeInfos := []storage.ArchiveVolumeInfo{{Size: 10, SegmentStart: 0, SegmentEnd: 1}}
	if _, err := sliceSegmentsForRange(base, volumeInfos, 8, 4); err == nil {
		t.Fatal("7z archive slicer accepted a truncated range")
	}
	part := &types.RARVolumePart{PartNumber: 0, DataOffset: 8, UnpackedSize: 4}
	if _, err := (&RARParser{logger: zerolog.Nop()}).buildSegmentsForVolumePart(part, base, map[int]int64{0: 0}); err == nil {
		t.Fatal("RAR volume slicer accepted a truncated range")
	}
}

func TestArchiveBuildersRejectIncompleteVolume(t *testing.T) {
	group := &FileGroup{
		BaseName: "archive", metadata: &fileAnalysisResult{fileSize: 8, lastFileSize: 8, segmentSize: 4},
		Files: []nzbparser.NzbFile{
			{Filename: "archive.rar", Segments: nzbparser.NzbSegments{{Number: 1, Id: "a", Bytes: 5}, {Number: 2, Id: "b", Bytes: 5}}},
			{Filename: "archive.r00", Segments: nzbparser.NzbSegments{{Number: 1, Id: "c", Bytes: 5}, {Number: 3, Id: "d", Bytes: 5}}},
		},
	}
	if _, err := buildArchiveVolumeDescriptors(group); err == nil {
		t.Fatal("expected incomplete archive volume error")
	}
	if _, _, _, err := buildBaseSegments(group); err == nil {
		t.Fatal("expected incomplete base segment error")
	}
}

func TestObfuscatedRARMergeDoesNotCombineNamedStandaloneArchives(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := map[string]*FileGroup{
		"movie":  {BaseName: "movie", ActualFilename: "Movie.rar", Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{{Filename: "Movie.rar"}}},
		"extras": {BaseName: "extras", ActualFilename: "Extras.rar", Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{{Filename: "Extras.rar"}}},
	}
	if got := p.mergeObfuscatedRarGroups(groups); len(got) != 2 {
		t.Fatalf("named standalone RAR group count = %d, want 2", len(got))
	}
}
