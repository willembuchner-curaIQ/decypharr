package usenet

import (
	"sort"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

type articleRepairTarget struct {
	fileName string
	segment  storage.NZBSegment
}

type articleRepairGroup struct {
	messageID string
	targets   []articleRepairTarget
}

func collectArticleRepairGroups(nzb *storage.NZB) []articleRepairGroup {
	if nzb == nil {
		return nil
	}

	type rangeKey struct {
		messageID        string
		rawFileKey       uint32
		rawOffset        int64
		rawLength        int64
		segmentDataStart int64
	}

	targets := make(map[string][]articleRepairTarget)
	seen := make(map[rangeKey]struct{})
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for _, segment := range file.Segments {
			if segment.MessageID == "" {
				continue
			}
			key := rangeKey{
				messageID:        segment.MessageID,
				rawFileKey:       segment.RawFileKey,
				rawOffset:        segment.RawOffset,
				rawLength:        segment.RawLength,
				segmentDataStart: segment.SegmentDataStart,
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			targets[segment.MessageID] = append(targets[segment.MessageID], articleRepairTarget{
				fileName: file.Name,
				segment:  segment,
			})
		}
	}

	messageIDs := make([]string, 0, len(targets))
	for messageID := range targets {
		messageIDs = append(messageIDs, messageID)
	}
	sort.Strings(messageIDs)

	groups := make([]articleRepairGroup, len(messageIDs))
	for i, messageID := range messageIDs {
		groups[i] = articleRepairGroup{messageID: messageID, targets: targets[messageID]}
	}
	return groups
}
