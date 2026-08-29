package usenet

import (
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestNZBCodecRoundTripsSegmentOrigin(t *testing.T) {
	want := &storage.NZB{
		ID: "nzb-1",
		Files: []storage.NZBFile{{
			Name: "movie.mkv",
			Segments: []storage.NZBSegment{{
				Number:      3,
				MessageID:   "part-3@example",
				Bytes:       700000,
				Group:       "alt.binaries.test",
				RawFileKey:  42,
				RawOffset:   1_400_000,
				RawLength:   700_000,
				StartOffset: 1_400_000,
				EndOffset:   2_099_999,
			}},
		}},
	}

	encoded, err := encodeNZBV2(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeNZBV2(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	segment := got.Files[0].Segments[0]
	if segment.RawFileKey != 42 || segment.RawOffset != 1_400_000 || segment.RawLength != 700_000 {
		t.Fatalf("origin = (%d, %d, %d), want (42, 1400000, 700000)", segment.RawFileKey, segment.RawOffset, segment.RawLength)
	}
}

func TestNZBCodecReadsPreRecoveryV2Segments(t *testing.T) {
	// This is the original v2 segment region: group table followed by the six
	// columns, with no trailing origin extension.
	legacy := &byteWriter{}
	legacy.uvarint(1)
	legacy.str("alt.binaries.test")
	legacy.varint(1)  // number
	legacy.varint(10) // bytes
	legacy.varint(0)  // start
	legacy.varint(9)  // end
	legacy.varint(0)  // data start
	legacy.uvarint(0) // group index
	messageIDs := &byteWriter{}
	messageIDs.str("legacy@example")

	nzb := &storage.NZB{ID: "legacy", Files: []storage.NZBFile{{Name: "legacy.bin"}}}
	if err := decodeSegments(nzb, []int{1}, legacy.buf, messageIDs.buf); err != nil {
		t.Fatalf("decode legacy v2 segment region: %v", err)
	}
	segment := nzb.Files[0].Segments[0]
	if segment.RawFileKey != 0 || segment.RawOffset != 0 || segment.RawLength != 0 {
		t.Fatalf("legacy origin should be unknown, got %+v", segment)
	}
}

// The hydration scan reads origin state through a partial decode. It must agree
// with the full decode on every shape, or NZBs get hydrated that do not need it
// (or worse, skipped when they do).
func TestRecoveryOriginStateMatchesFullDecode(t *testing.T) {
	origin := func(n int) []storage.NZBSegment {
		segs := make([]storage.NZBSegment, n)
		for i := range segs {
			segs[i] = storage.NZBSegment{
				Number: i + 1, MessageID: "id@example", Bytes: 100, Group: "alt.binaries.test",
				RawFileKey: uint32(i + 1), RawOffset: int64(i * 100), RawLength: 100,
			}
		}
		return segs
	}
	legacy := func(n int) []storage.NZBSegment {
		segs := origin(n)
		for i := range segs {
			segs[i].RawFileKey, segs[i].RawOffset, segs[i].RawLength = 0, 0, 0
		}
		return segs
	}

	tests := []struct {
		name string
		nzb  *storage.NZB
	}{
		{"complete origins", &storage.NZB{ID: "a", Category: "sonarr", Files: []storage.NZBFile{
			{Name: "a.mkv", FileType: storage.NZBFileTypeMedia, Segments: origin(4)},
		}}},
		{"missing origins", &storage.NZB{ID: "b", Category: "radarr", Files: []storage.NZBFile{
			{Name: "b.mkv", FileType: storage.NZBFileTypeMedia, Segments: legacy(4)},
		}}},
		{"par2 lacks origins but content has them", &storage.NZB{ID: "c", Category: "radarr", Files: []storage.NZBFile{
			{Name: "c.mkv", FileType: storage.NZBFileTypeMedia, Segments: origin(3)},
			{Name: "c.par2", FileType: storage.NZBFileTypePar2, Segments: legacy(2)},
		}}},
		{"deleted file lacks origins", &storage.NZB{ID: "d", Category: "sonarr", Files: []storage.NZBFile{
			{Name: "d.mkv", FileType: storage.NZBFileTypeMedia, Segments: origin(3)},
			{Name: "gone.mkv", FileType: storage.NZBFileTypeMedia, IsDeleted: true, Segments: legacy(2)},
		}}},
		{"second file missing origins", &storage.NZB{ID: "e", Category: "sonarr", Files: []storage.NZBFile{
			{Name: "e1.mkv", FileType: storage.NZBFileTypeMedia, Segments: origin(3)},
			{Name: "e2.mkv", FileType: storage.NZBFileTypeMedia, Segments: legacy(2)},
		}}},
		{"ignored file only", &storage.NZB{ID: "f", Category: "sonarr", Files: []storage.NZBFile{
			{Name: "f.nfo", FileType: storage.NZBFileTypeIgnore, Segments: legacy(2)},
		}}},
		{"no files", &storage.NZB{ID: "g", Category: "sonarr"}},
		{"file with no segments", &storage.NZB{ID: "h", Category: "sonarr", Files: []storage.NZBFile{
			{Name: "h.mkv", FileType: storage.NZBFileTypeMedia},
		}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := encodeNZBV2(test.nzb)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			full, err := decodeNZBV2(encoded)
			if err != nil {
				t.Fatalf("full decode: %v", err)
			}
			wantMissing := hasMissingRecoveryOrigins(full)

			category, missing, err := decodeNZBV2RecoveryOriginState(encoded)
			if err != nil {
				t.Fatalf("origin state: %v", err)
			}
			if missing != wantMissing {
				t.Errorf("missing = %v, full decode says %v", missing, wantMissing)
			}
			if category != full.Category {
				t.Errorf("category = %q, want %q", category, full.Category)
			}
		})
	}
}

// A blob written before the origin extension must read as legacy, not as an
// error and not as complete.
func TestRecoveryOriginStateHandlesPreExtensionBlobs(t *testing.T) {
	nzb := &storage.NZB{ID: "old", Category: "sonarr", Files: []storage.NZBFile{{
		Name: "old.mkv", FileType: storage.NZBFileTypeMedia,
		Segments: []storage.NZBSegment{{Number: 1, MessageID: "a@b", Bytes: 10, Group: "g"}},
	}}}
	encoded, err := encodeNZBV2(nzb)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	category, missing, err := decodeNZBV2RecoveryOriginState(encoded)
	if err != nil {
		t.Fatalf("origin state: %v", err)
	}
	if !missing {
		t.Fatal("pre-extension blob reported complete origins")
	}
	if category != "sonarr" {
		t.Fatalf("category = %q", category)
	}
}

func benchBlob(segs int) []byte {
	s := make([]storage.NZBSegment, segs)
	for i := range s {
		s[i] = storage.NZBSegment{
			Number: i + 1, Bytes: 768000, Group: "alt.binaries.test",
			MessageID:  fmt.Sprintf("part%d.of%d@news.example.com", i+1, segs),
			RawFileKey: uint32(i%40 + 1), RawOffset: int64(i) * 768000, RawLength: 768000,
		}
	}
	nzb := &storage.NZB{ID: "bench", Category: "radarr", Files: []storage.NZBFile{
		{Name: "big.mkv", FileType: storage.NZBFileTypeMedia, Segments: s},
	}}
	b, err := encodeNZBV2(nzb)
	if err != nil {
		panic(err)
	}
	return b
}

func BenchmarkOriginStateFullDecode(b *testing.B) {
	blob := benchBlob(24000)
	b.ReportAllocs()
	for b.Loop() {
		nzb, err := decodeNZBV2(blob)
		if err != nil {
			b.Fatal(err)
		}
		_ = hasMissingRecoveryOrigins(nzb)
	}
}

func BenchmarkOriginStatePartialDecode(b *testing.B) {
	blob := benchBlob(24000)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := decodeNZBV2RecoveryOriginState(blob); err != nil {
			b.Fatal(err)
		}
	}
}
