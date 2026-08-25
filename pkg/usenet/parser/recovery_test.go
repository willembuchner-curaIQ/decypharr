package parser

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

func TestProbeContentAvailabilityDefersAllMissingContentToPAR2(t *testing.T) {
	p := &NZBParser{logger: zerolog.Nop()}
	groups := map[string]*FileGroup{
		"b": {
			BaseName: "b",
			Files:    []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "b@example"}}}},
		},
		"a": {
			BaseName: "a",
			Files:    []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Number: 1, Id: "a@example"}}}},
		},
	}
	manifest := recovery.NewManifest("nzb-id", "release")
	manifest.Files = []recovery.RawFile{{Key: 1, Ordinal: 0, IsPAR2: true}}
	missing := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "missing"}

	var calls []string
	deferred, err := p.probeContentAvailability(context.Background(), groups, manifest, func(_ context.Context, messageID string) error {
		calls = append(calls, messageID)
		return missing
	})
	if err != nil {
		t.Fatalf("recoverable availability probe failed: %v", err)
	}
	if !deferred {
		t.Fatal("all-missing content with PAR2 was not deferred")
	}
	if got, want := calls, []string{"a@example", "b@example"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("STAT calls = %v, want %v", got, want)
	}

	manifest.Files[0].IsPAR2 = false
	deferred, err = p.probeContentAvailability(context.Background(), groups, manifest, func(context.Context, string) error {
		return missing
	})
	if deferred || !errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("all-missing content without PAR2 = deferred %t, err %v", deferred, err)
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
	deferred, err := p.probeContentAvailability(context.Background(), groups, recovery.NewManifest("nzb-id", "release"), func(context.Context, string) error {
		calls++
		return wantErr
	})
	if deferred || !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("operational error probe = deferred %t, calls %d, err %v", deferred, calls, err)
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

func TestRawManifestPreservesLiteralDuplicateSubjectFiles(t *testing.T) {
	content := []byte(`<?xml version="1.0" encoding="utf-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="poster" date="1" subject="&quot;movie.mkv&quot; yEnc (1/2)">
    <groups><group>alt.test</group></groups>
    <segments><segment bytes="5" number="1">one@example</segment></segments>
  </file>
  <file poster="poster" date="1" subject="&quot;movie.mkv&quot; yEnc (1/2)">
    <groups><group>alt.test</group></groups>
    <segments><segment bytes="5" number="2">two@example</segment></segments>
  </file>
</nzb>`)

	rawFiles, err := parseRawNZBFiles(content)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(rawFiles); got != 2 {
		t.Fatalf("literal raw file count = %d, want 2", got)
	}
	manifest := buildRawManifest("nzb-id", "movie", rawFiles, (&NZBParser{}).detectFileType)
	if manifest.Files[0].SubjectFilename != "movie.mkv" || manifest.Files[1].SubjectFilename != "movie.mkv" {
		t.Fatalf("subject filenames were not retained: %#v", manifest.Files)
	}

	logical, err := nzbparser.Parse(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(logical.Files); got != 1 {
		t.Fatalf("normalized logical file count = %d, want 1", got)
	}
	p := &NZBParser{logger: zerolog.Nop()}
	groups := p.groupProcessedFiles([]contentResult{{
		file:           logical.Files[0],
		rawFileKey:     1,
		fileType:       storage.NZBFileTypeMedia,
		actualFilename: "movie.mkv",
	}}, manifest)
	var group *FileGroup
	for _, candidate := range groups {
		group = candidate
		break
	}
	if group == nil {
		t.Fatal("logical media group was not built")
	}
	group.metadata = &fileAnalysisResult{fileSize: 8, lastFileSize: 8, segmentSize: 4}
	_, segments := getNZBSegments(0, group.Files[0], group)
	if len(segments) != 2 {
		t.Fatalf("logical segment count = %d, want 2", len(segments))
	}
	if segments[0].RawFileKey != 1 || segments[1].RawFileKey != 2 {
		t.Fatalf("literal raw origins = %d, %d; want 1, 2", segments[0].RawFileKey, segments[1].RawFileKey)
	}
	if segments[0].RawOffset != 0 || segments[1].RawOffset != 4 {
		t.Fatalf("protected-source offsets = %d, %d; want 0, 4", segments[0].RawOffset, segments[1].RawOffset)
	}
}

func TestBuildRawManifestPreservesAllFilesAndArticleOrder(t *testing.T) {
	p := &NZBParser{}
	files := nzbparser.NzbFiles{
		{
			Subject:       "ignored subject",
			Filename:      "notes.nfo",
			Basefilename:  "notes",
			Groups:        []string{"alt.test", "alt.backup"},
			Date:          1234,
			TotalSegments: 2,
			Segments: nzbparser.NzbSegments{
				{Number: 2, Id: "second@example", Bytes: 120},
				{Number: 1, Id: "first@example", Bytes: 100},
			},
		},
		{
			Subject:  "parity subject",
			Filename: "release.vol00+01.par2",
			Segments: nzbparser.NzbSegments{{Number: 1, Id: "parity@example", Bytes: 80}},
		},
		{
			Subject: "opaque source",
		},
	}

	manifest := buildRawManifest("nzb-id", "release", files, p.detectFileType)
	if got := len(manifest.Files); got != 3 {
		t.Fatalf("raw file count = %d, want 3", got)
	}
	for i, file := range manifest.Files {
		if file.Key != recovery.RawFileKey(i+1) {
			t.Fatalf("file %d key = %d, want %d", i, file.Key, i+1)
		}
	}
	if got := manifest.Files[0].PostedBytes; got != 220 {
		t.Fatalf("posted bytes = %d, want 220", got)
	}
	if got := manifest.Files[0].Articles[0].MessageID; got != "first@example" {
		t.Fatalf("first ordered article = %q", got)
	}
	if !manifest.Files[1].IsPAR2 || manifest.Files[1].DetectedType != string(storage.NZBFileTypePar2) {
		t.Fatalf("PAR2 classification missing: %+v", manifest.Files[1])
	}
	if manifest.Files[2].DetectedType != string(storage.NZBFileTypeUnknown) {
		t.Fatalf("unknown file classification = %q", manifest.Files[2].DetectedType)
	}
}

func TestUnknownYEncNameDoesNotDowngradeManifestClassification(t *testing.T) {
	file := nzbparser.NzbFile{
		Subject:  "media",
		Filename: "movie.mkv",
		Segments: nzbparser.NzbSegments{{Number: 1, Id: "media@example", Bytes: 10}},
	}
	p := &NZBParser{}
	manifest := buildRawManifest("nzb-id", "movie", nzbparser.NzbFiles{file}, p.detectFileType)
	updateManifestClassificationForFile(manifest, file, storage.NZBFileTypeUnknown, "opaque_name")
	raw, ok := manifest.File(1)
	if !ok {
		t.Fatal("manifest raw file missing")
	}
	if raw.DetectedType != string(storage.NZBFileTypeMedia) || raw.ActualFilename != "opaque_name" {
		t.Fatalf("classification after opaque yEnc name = %+v", raw)
	}
}

func TestRecoveryRelevantFileTypeDetection(t *testing.T) {
	p := &NZBParser{}
	if got := p.detectFileTypeFromContent([]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}); got != storage.NZBFileTypePar2 {
		t.Fatalf("PAR2 magic detected as %q", got)
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

func TestProcessMediaRebasesLogicalOffsetsAndKeepsRawOrigins(t *testing.T) {
	first := nzbparser.NzbFile{
		Subject:  "first",
		Filename: "movie.mkv",
		Number:   1,
		Groups:   []string{"alt.test"},
		Segments: nzbparser.NzbSegments{{Number: 1, Id: "one@example", Bytes: 5}},
	}
	second := nzbparser.NzbFile{
		Subject:  "second",
		Filename: "movie.mkv",
		Number:   2,
		Groups:   []string{"alt.test"},
		Segments: nzbparser.NzbSegments{{Number: 1, Id: "two@example", Bytes: 4}},
	}
	manifest := buildRawManifest("nzb-id", "movie", nzbparser.NzbFiles{first, second}, (&NZBParser{}).detectFileType)
	group := &FileGroup{
		BaseName:       "movie",
		ActualFilename: "movie.mkv",
		Type:           storage.NZBFileTypeMedia,
		Files:          []nzbparser.NzbFile{first, second},
		metadata:       &fileAnalysisResult{fileSize: 4, lastFileSize: 3, segmentSize: 4},
		rawFileKeys: map[string]recovery.RawFileKey{
			rawFileIdentity(first):  1,
			rawFileIdentity(second): 2,
		},
		manifest: manifest,
		Groups:   map[string]struct{}{"alt.test": {}},
	}
	p := &NZBParser{logger: zerolog.Nop()}
	file := p.processMediaFile(group, "")
	if file == nil || len(file.Segments) != 2 {
		t.Fatalf("processed file = %#v", file)
	}
	if got := [2]int64{file.Segments[0].StartOffset, file.Segments[0].EndOffset}; got != [2]int64{0, 3} {
		t.Fatalf("first logical range = %v", got)
	}
	if got := [2]int64{file.Segments[1].StartOffset, file.Segments[1].EndOffset}; got != [2]int64{4, 6} {
		t.Fatalf("second logical range = %v", got)
	}
	if file.Segments[0].RawFileKey != 1 || file.Segments[1].RawFileKey != 2 {
		t.Fatalf("raw file keys = %d, %d", file.Segments[0].RawFileKey, file.Segments[1].RawFileKey)
	}
	if file.Segments[0].RawOffset != 0 || file.Segments[1].RawOffset != 0 {
		t.Fatalf("raw offsets = %d, %d", file.Segments[0].RawOffset, file.Segments[1].RawOffset)
	}
}

func TestSliceSegmentsPropagatesRawOrigin(t *testing.T) {
	base := []storage.NZBSegment{{
		Number:     1,
		MessageID:  "article@example",
		Bytes:      10,
		RawFileKey: 7,
		RawOffset:  100,
		RawLength:  10,
	}}
	sliced, err := sliceSegmentsForRangeSimple(base, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(sliced) != 1 {
		t.Fatalf("slice count = %d", len(sliced))
	}
	if sliced[0].RawFileKey != 7 || sliced[0].RawOffset != 102 || sliced[0].RawLength != 4 {
		t.Fatalf("raw origin = %+v", sliced[0])
	}
}

func TestArchiveSlicersRejectPartiallyCoveredRanges(t *testing.T) {
	base := []storage.NZBSegment{{
		Number:     1,
		MessageID:  "article@example",
		Bytes:      10,
		RawFileKey: 7,
		RawOffset:  0,
		RawLength:  10,
	}}
	if _, err := sliceSegmentsForRangeSimple(base, 8, 4); err == nil {
		t.Fatal("simple archive slicer accepted a truncated range")
	}
	volumeInfos := []storage.ArchiveVolumeInfo{{Size: 10, SegmentStart: 0, SegmentEnd: 1}}
	if _, err := sliceSegmentsForRange(base, volumeInfos, 8, 4); err == nil {
		t.Fatal("7z archive slicer accepted a truncated range")
	}
	rarParser := &RARParser{logger: zerolog.Nop()}
	part := &types.RARVolumePart{PartNumber: 0, DataOffset: 8, UnpackedSize: 4}
	if _, err := rarParser.buildSegmentsForVolumePart(part, base, map[int]int64{0: 0}); err == nil {
		t.Fatal("RAR volume slicer accepted a truncated range")
	}
}

func TestArchiveBuildersRejectIncompleteVolume(t *testing.T) {
	group := &FileGroup{
		BaseName: "archive",
		metadata: &fileAnalysisResult{fileSize: 8, lastFileSize: 8, segmentSize: 4},
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
