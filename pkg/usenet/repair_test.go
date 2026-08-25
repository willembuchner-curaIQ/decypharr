package usenet

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestCollectArticleRepairGroups(t *testing.T) {
	shared := storage.NZBSegment{
		MessageID:        "shared@example",
		RawFileKey:       7,
		RawOffset:        100,
		RawLength:        50,
		SegmentDataStart: 3,
	}
	nzb := &storage.NZB{Files: []storage.NZBFile{
		{Name: "movie.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{
			shared,
			{MessageID: "shared@example", RawFileKey: 7, RawOffset: 150, RawLength: 25, SegmentDataStart: 53},
			{MessageID: "unique@example", RawFileKey: 8, RawLength: 100},
			{RawFileKey: 9, RawLength: 100},
		}},
		{Name: "duplicate.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{shared}},
		{Name: "ignored.par2", FileType: storage.NZBFileTypePar2, Segments: []storage.NZBSegment{
			{MessageID: "parity@example", RawFileKey: 10, RawLength: 100},
		}},
		{Name: "deleted.mkv", FileType: storage.NZBFileTypeMedia, IsDeleted: true, Segments: []storage.NZBSegment{
			{MessageID: "deleted@example", RawFileKey: 11, RawLength: 100},
		}},
	}}

	groups := collectArticleRepairGroups(nzb)
	if len(groups) != 2 {
		t.Fatalf("groups=%d, want 2: %#v", len(groups), groups)
	}
	if groups[0].messageID != "shared@example" || len(groups[0].targets) != 2 {
		t.Fatalf("shared group=%#v", groups[0])
	}
	if groups[1].messageID != "unique@example" || len(groups[1].targets) != 1 {
		t.Fatalf("unique group=%#v", groups[1])
	}
}
