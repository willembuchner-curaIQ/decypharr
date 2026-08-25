package reader

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

func TestCacheArticleRangeSourceAssemblesCroppedCachedRanges(t *testing.T) {
	segments := []SegmentMeta{
		{MessageID: "article", Bytes: 4, StartOffset: 0, EndOffset: 3, RawFileKey: 7, RawOffset: 0, RawLength: 4},
		{MessageID: "article", Bytes: 4, StartOffset: 4, EndOffset: 7, RawFileKey: 7, RawOffset: 4, RawLength: 4},
	}
	cache, err := NewSegmentCache(context.Background(), segments, Config{Retention: RetentionWindow}, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	for index, data := range [][]byte{[]byte("abcd"), []byte("efgh")} {
		writer := cache.StreamWriter(index)
		if _, err := writer.Adopt(data); err != nil {
			t.Fatal(err)
		}
		writer.Finalize()
	}

	source := cacheArticleRangeSource{cache: cache}
	if !source.HasArticleRange(7, "article", 2, 4) {
		t.Fatal("cached range was not reported as available")
	}
	dst := make([]byte, 4)
	present, err := source.ReadArticleRange(7, "article", 2, dst)
	if err != nil || !present || string(dst) != "cdef" {
		t.Fatalf("ReadArticleRange present=%v data=%q err=%v", present, dst, err)
	}
	if source.HasArticleRange(8, "article", 2, 4) || source.HasArticleRange(7, "other", 2, 4) {
		t.Fatal("cache source crossed raw-file or message-ID identity")
	}
}
