package reader

import "math"

// cacheArticleRangeSource adapts one active logical stream cache into raw
// source ranges. Archive readers may contain several cropped ranges from the
// same NNTP article; coverage is therefore assembled interval by interval.
type cacheArticleRangeSource struct {
	cache *SegmentCache
}

func (s cacheArticleRangeSource) HasArticleRange(rawFileKey uint32, messageID string, rawOffset, length int64) bool {
	if length < 0 || rawOffset < 0 || rawOffset > math.MaxInt64-length {
		return false
	}
	cursor, end := rawOffset, rawOffset+length
	for cursor < end {
		_, coveredEnd, ok := s.coveringSegment(rawFileKey, messageID, cursor, end)
		if !ok {
			return false
		}
		cursor = coveredEnd
	}
	return true
}

func (s cacheArticleRangeSource) ReadArticleRange(rawFileKey uint32, messageID string, rawOffset int64, dst []byte) (bool, error) {
	length := int64(len(dst))
	if !s.HasArticleRange(rawFileKey, messageID, rawOffset, length) {
		return false, nil
	}
	cursor, end := rawOffset, rawOffset+length
	for cursor < end {
		index, coveredEnd, ok := s.coveringSegment(rawFileKey, messageID, cursor, end)
		if !ok {
			return false, nil
		}
		segment := s.cache.segments[index]
		chunk := coveredEnd - cursor
		n, present := s.cache.ReadRangeInto(index, cursor-segment.RawOffset, chunk, dst[cursor-rawOffset:coveredEnd-rawOffset])
		if !present || int64(n) != chunk {
			return false, nil
		}
		cursor = coveredEnd
	}
	return true, nil
}

func (s cacheArticleRangeSource) coveringSegment(rawFileKey uint32, messageID string, cursor, requestedEnd int64) (int, int64, bool) {
	if s.cache == nil || rawFileKey == 0 || messageID == "" {
		return 0, 0, false
	}
	bestIndex, bestEnd := -1, cursor
	for index, segment := range s.cache.segments {
		if segment.RawFileKey != rawFileKey || segment.MessageID != messageID || segment.RawOffset < 0 || segment.RawLength <= 0 {
			continue
		}
		if SegmentState(s.cache.states[index].Load()) != StateOnDisk {
			continue
		}
		available := min(segment.RawLength, s.cache.SegmentDataSize(index))
		if available <= 0 || segment.RawOffset > math.MaxInt64-available {
			continue
		}
		segmentEnd := segment.RawOffset + available
		if segment.RawOffset > cursor || segmentEnd <= cursor {
			continue
		}
		if segmentEnd > bestEnd {
			bestIndex = index
			bestEnd = min(segmentEnd, requestedEnd)
		}
	}
	return bestIndex, bestEnd, bestIndex >= 0
}
