package usenet

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sort"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

// NZBRepairReport summarizes one exhaustive availability and PAR2 repair pass.
type NZBRepairReport struct {
	Articles             int      `json:"articles"`
	AvailableArticles    int      `json:"available_articles"`
	MissingArticles      int      `json:"missing_articles"`
	UnknownArticles      int      `json:"unknown_articles"`
	RepairRanges         int      `json:"repair_ranges"`
	RepairedRanges       int      `json:"repaired_ranges"`
	FailedRanges         int      `json:"failed_ranges"`
	ModeledDownloadBytes int64    `json:"modeled_download_bytes"`
	AffectedFiles        []string `json:"affected_files,omitempty"`
	UnknownFiles         []string `json:"unknown_files,omitempty"`
	RepairedFiles        []string `json:"repaired_files,omitempty"`
	FailedFiles          []string `json:"failed_files,omitempty"`
	fileErrors           map[string]error
}

func (r NZBRepairReport) FileError(name string) error {
	return r.fileErrors[name]
}

type articleRepairTarget struct {
	fileNames []string
	segment   storage.NZBSegment
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
	type targetLocation struct {
		messageID string
		index     int
	}

	targets := make(map[string][]articleRepairTarget)
	seen := make(map[rangeKey]targetLocation)
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
			if location, ok := seen[key]; ok {
				target := &targets[location.messageID][location.index]
				if !slices.Contains(target.fileNames, file.Name) {
					target.fileNames = append(target.fileNames, file.Name)
				}
				continue
			}
			seen[key] = targetLocation{messageID: segment.MessageID, index: len(targets[segment.MessageID])}
			targets[segment.MessageID] = append(targets[segment.MessageID], articleRepairTarget{
				fileNames: []string{file.Name},
				segment:   segment,
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

func (u *Usenet) repairNZB(ctx context.Context, nzoID string) (report NZBRepairReport, resultErr error) {
	affectedFiles := make(map[string]struct{})
	unknownFiles := make(map[string]struct{})
	repairedFiles := make(map[string]struct{})
	failedFiles := make(map[string]struct{})
	report.fileErrors = make(map[string]error)
	defer func() {
		report.AffectedFiles = slices.Sorted(maps.Keys(affectedFiles))
		report.UnknownFiles = slices.Sorted(maps.Keys(unknownFiles))
		report.RepairedFiles = slices.Sorted(maps.Keys(repairedFiles))
		report.FailedFiles = slices.Sorted(maps.Keys(failedFiles))
		for fileName := range repairedFiles {
			if _, failed := failedFiles[fileName]; failed {
				continue
			}
			if _, unknown := unknownFiles[fileName]; unknown {
				continue
			}
			if u.failedFiles != nil {
				u.failedFiles.Delete(fsKey(nzoID, fileName))
			}
		}
		if report.RepairedRanges > 0 {
			u.invalidateIdleNZBFileSystems(nzoID)
		}
	}()
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
		report.UnknownArticles = len(groups)
		for i := range groups {
			for _, target := range groups[i].targets {
				for _, fileName := range target.fileNames {
					unknownFiles[fileName] = struct{}{}
					report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], err)
				}
			}
		}
		return report, fmt.Errorf("audit NZB articles: %w", err)
	}

	targets := make([]articleRepairTarget, 0)
	failures := make([]error, 0)
	for i := range groups {
		if i >= len(stat.Results) {
			report.UnknownArticles++
			failure := fmt.Errorf("STAT %s returned no result", groups[i].messageID)
			failures = append(failures, failure)
			for _, target := range groups[i].targets {
				for _, fileName := range target.fileNames {
					unknownFiles[fileName] = struct{}{}
					report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], failure)
				}
			}
			continue
		}
		result := stat.Results[i]
		switch {
		case result.Available:
			report.AvailableArticles++
		case nntp.IsArticleNotFoundError(result.Error):
			report.MissingArticles++
			targets = append(targets, groups[i].targets...)
			for _, target := range groups[i].targets {
				for _, fileName := range target.fileNames {
					affectedFiles[fileName] = struct{}{}
				}
			}
		default:
			report.UnknownArticles++
			failure := result.Error
			if result.Error == nil {
				failure = fmt.Errorf("STAT %s had no terminal result", groups[i].messageID)
			} else {
				failure = fmt.Errorf("STAT %s: %w", groups[i].messageID, result.Error)
			}
			failures = append(failures, failure)
			for _, target := range groups[i].targets {
				for _, fileName := range target.fileNames {
					unknownFiles[fileName] = struct{}{}
					report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], failure)
				}
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
	for i, target := range targets {
		if i >= len(batch.Articles) {
			failure := batchErr
			if failure == nil {
				failure = fmt.Errorf("repair files %q article %s returned no result", target.fileNames, target.segment.MessageID)
				failures = append(failures, failure)
			}
			for _, fileName := range target.fileNames {
				unknownFiles[fileName] = struct{}{}
				report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], failure)
			}
			continue
		}
		result := batch.Articles[i]
		if result.Err != nil {
			report.FailedRanges++
			u.logger.Debug().Err(result.Err).
				Str("nzb_id", nzoID).
				Str("message_id", target.segment.MessageID).
				Uint32("raw_file", target.segment.RawFileKey).
				Int64("raw_offset", target.segment.RawOffset).
				Int64("raw_length", target.segment.RawLength).
				Msg("PAR2 range recovery failed")
			failure := fmt.Errorf("repair files %q article %s: %w", target.fileNames, target.segment.MessageID, result.Err)
			failures = append(failures, failure)
			for _, fileName := range target.fileNames {
				failedFiles[fileName] = struct{}{}
				report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], result.Err)
			}
			continue
		}
		if result.Bytes != int(target.segment.SegmentDataStart+target.segment.RawLength) {
			report.FailedRanges++
			// Recovery succeeded but the reconstruction does not match the
			// stored provenance. That points at the segment map rather than at
			// the parity, so record both sides to tell them apart.
			u.logger.Debug().
				Str("nzb_id", nzoID).
				Str("message_id", target.segment.MessageID).
				Uint32("raw_file", target.segment.RawFileKey).
				Int64("raw_offset", target.segment.RawOffset).
				Int64("raw_length", target.segment.RawLength).
				Int64("segment_data_start", target.segment.SegmentDataStart).
				Int("recovered_bytes", result.Bytes).
				Int64("expected_bytes", target.segment.SegmentDataStart+target.segment.RawLength).
				Msg("PAR2 range recovered the wrong length; provenance and parity disagree")
			failure := fmt.Errorf("repair files %q article %s returned %d bytes", target.fileNames, target.segment.MessageID, result.Bytes)
			failures = append(failures, failure)
			for _, fileName := range target.fileNames {
				failedFiles[fileName] = struct{}{}
				report.fileErrors[fileName] = errors.Join(report.fileErrors[fileName], failure)
			}
			continue
		}
		report.RepairedRanges++
		for _, fileName := range target.fileNames {
			repairedFiles[fileName] = struct{}{}
		}
	}
	if batchErr != nil {
		failures = append(failures, batchErr)
	}
	// PAR2 repair was silent before this: a run could report failed ranges with
	// no record of why, which left the only real failure we ever saw
	// undiagnosable. One line per NZB keeps that visible without echoing every
	// range at info level.
	if report.FailedRanges > 0 || report.RepairedRanges > 0 {
		event := u.logger.Info()
		if report.FailedRanges > 0 {
			event = u.logger.Warn()
		}
		event.
			Str("nzb_id", nzoID).
			Int("ranges", report.RepairRanges).
			Int("repaired", report.RepairedRanges).
			Int("failed", report.FailedRanges).
			Int("missing_articles", report.MissingArticles).
			Int64("download_bytes", report.ModeledDownloadBytes).
			Msg("PAR2 repair pass finished")
	}
	return report, errors.Join(failures...)
}
