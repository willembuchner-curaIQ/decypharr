package usenet

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

// NZBRepairReport summarizes one exhaustive availability and PAR2 repair pass.
type NZBRepairReport struct {
	Articles             int   `json:"articles"`
	AvailableArticles    int   `json:"available_articles"`
	MissingArticles      int   `json:"missing_articles"`
	UnknownArticles      int   `json:"unknown_articles"`
	RepairRanges         int   `json:"repair_ranges"`
	RepairedRanges       int   `json:"repaired_ranges"`
	FailedRanges         int   `json:"failed_ranges"`
	ModeledDownloadBytes int64 `json:"modeled_download_bytes"`
}

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

// RepairNZB audits every distinct logical article and repairs every range that
// is definitively unavailable across all configured providers.
func (u *Usenet) RepairNZB(ctx context.Context, nzoID string) (NZBRepairReport, error) {
	if u == nil || u.nntp == nil || u.nzbStorage == nil || u.par2Recovery == nil {
		return NZBRepairReport{}, errors.New("usenet PAR2 repair is unavailable")
	}
	if nzoID == "" {
		return NZBRepairReport{}, errors.New("NZB id is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err, _ := u.repairFlights.Do(nzoID, func() (any, error) {
		return u.repairNZB(ctx, nzoID)
	})
	if value == nil {
		return NZBRepairReport{}, err
	}
	return value.(NZBRepairReport), err
}

func (u *Usenet) repairNZB(ctx context.Context, nzoID string) (NZBRepairReport, error) {
	var report NZBRepairReport
	if u.backgroundRepairSlots != nil {
		select {
		case u.backgroundRepairSlots <- struct{}{}:
			defer func() { <-u.backgroundRepairSlots }()
		case <-ctx.Done():
			return report, ctx.Err()
		}
	}
	if !u.canRecoverWithPAR2(nzoID) {
		return report, recovery.ErrNoRecoverySet
	}
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return report, fmt.Errorf("load NZB metadata: %w", err)
	}
	groups := collectArticleRepairGroups(nzb)
	report.Articles = len(groups)
	if len(groups) == 0 {
		return report, errors.New("NZB contains no repairable article ranges")
	}

	messageIDs := make([]string, len(groups))
	for i := range groups {
		messageIDs[i] = groups[i].messageID
	}
	stat, err := u.nntp.BatchStatAll(ctx, messageIDs)
	if err != nil {
		return report, fmt.Errorf("audit NZB articles: %w", err)
	}

	targets := make([]articleRepairTarget, 0)
	failures := make([]error, 0)
	for i := range groups {
		if i >= len(stat.Results) {
			report.UnknownArticles++
			failures = append(failures, fmt.Errorf("STAT %s returned no result", groups[i].messageID))
			continue
		}
		result := stat.Results[i]
		switch {
		case result.Available:
			report.AvailableArticles++
		case nntp.IsArticleNotFoundError(result.Error):
			report.MissingArticles++
			targets = append(targets, groups[i].targets...)
		default:
			report.UnknownArticles++
			if result.Error == nil {
				failures = append(failures, fmt.Errorf("STAT %s had no terminal result", groups[i].messageID))
			} else {
				failures = append(failures, fmt.Errorf("STAT %s: %w", groups[i].messageID, result.Error))
			}
		}
	}
	report.RepairRanges = len(targets)
	if len(targets) == 0 {
		return report, errors.Join(failures...)
	}

	segments := make([]reader.SegmentMeta, len(targets))
	for i := range targets {
		segments[i] = reader.NewSegmentMeta(targets[i].segment)
	}
	batch, batchErr := u.par2Recovery.RecoverArticles(ctx, nzoID, segments, recovery.RecoverBatchOptions{
		NumGoroutines: max(1, runtime.GOMAXPROCS(0)/2),
	})
	report.ModeledDownloadBytes = batch.ModeledDownloadBytes
	for i, result := range batch.Articles {
		if result.Err != nil {
			report.FailedRanges++
			failures = append(failures, fmt.Errorf("repair %q article %s: %w", targets[i].fileName, targets[i].segment.MessageID, result.Err))
			continue
		}
		if result.Bytes != int(targets[i].segment.SegmentDataStart+targets[i].segment.RawLength) {
			report.FailedRanges++
			failures = append(failures, fmt.Errorf("repair %q article %s returned %d bytes", targets[i].fileName, targets[i].segment.MessageID, result.Bytes))
			continue
		}
		report.RepairedRanges++
	}
	if batchErr != nil {
		failures = append(failures, batchErr)
	}
	return report, errors.Join(failures...)
}
