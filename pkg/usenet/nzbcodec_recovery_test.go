package usenet

import (
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
