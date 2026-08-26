package repair

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (r *Service) probeEntry(ctx context.Context, runID string, c *candidate, heal *errorCache, nzb *nzbProber, opts RunOptions, autoRepair bool) (*storage.EntryHealth, par2ProbeStats) {
	s := r.storage
	if c.item == nil {
		item, err := s.GetEntryItem(c.name)
		if err != nil || item == nil || len(item.Files) == 0 {
			return nil, par2ProbeStats{}
		}
		c.item = item
	}

	h, _ := s.GetEntryHealth(c.name)
	if h == nil {
		h = &storage.EntryHealth{EntryName: c.name}
	}
	previous := h.Status

	h.PreviousStatus = previous
	h.Status = storage.HealthRepairing
	h.ActiveRunID = runID
	h.Protocol = ""
	r.saveHealth(h)

	deepNZB := r.shouldDeepNZB(h.LastPAR2AuditAt, opts.DeepNZB, autoRepair, time.Now())

	names := orderedFilenames(c.item)
	results := r.probeFiles(ctx, c, names, nzb, opts, autoRepair, deepNZB)
	if autoRepair {
		r.autoHealResults(ctx, results, heal)
	}

	var par2Stats par2ProbeStats
	var healthMissing, healthRepaired int
	completedAudit := false
	seenOutcomes := make(map[*nzbRepairOutcome]struct{})
	for i := range results {
		outcome := results[i].par2
		if outcome == nil {
			continue
		}
		if _, ok := seenOutcomes[outcome]; ok {
			continue
		}
		seenOutcomes[outcome] = struct{}{}
		report := outcome.report
		healthMissing += report.MissingArticles
		healthRepaired += report.RepairedRanges
		completedAudit = completedAudit || report.Articles > 0 && report.UnknownArticles == 0 &&
			!errors.Is(outcome.err, context.Canceled) && !errors.Is(outcome.err, context.DeadlineExceeded)
		if !outcome.counted.CompareAndSwap(false, true) {
			continue
		}
		par2Stats.articles += report.Articles
		par2Stats.missing += report.MissingArticles
		par2Stats.repairedRanges += report.RepairedRanges
		par2Stats.failedRanges += report.FailedRanges
		par2Stats.downloadBytes += report.ModeledDownloadBytes
		if report.MissingArticles > 0 && report.FailedRanges == 0 && report.RepairedRanges == report.RepairRanges {
			par2Stats.repairedNZBs++
		}
	}

	broken := r.brokenFiles(c, results)
	final := rollupStatus(results)

	h.Status = final
	h.FileCount = len(names)
	h.BrokenFiles = broken
	h.BrokenCount = len(broken)
	h.Fingerprint = storage.EntryItemRepairFingerprint(c.item)
	h.LastCheckedAt = time.Now()
	h.NextCheckDueAt = h.LastCheckedAt.Add(r.recheckInterval())
	h.PAR2MissingArticles = healthMissing
	h.PAR2RepairedRanges = healthRepaired
	h.Dirty = false
	h.DirtyReason = ""
	h.ActiveRunID = ""
	h.PreviousStatus = ""
	if proto := firstProtocol(results); proto != "" {
		h.Protocol = proto
	}
	switch final {
	case storage.HealthHealthy:
		h.LastOKAt = h.LastCheckedAt
		h.FailureReason = ""
	case storage.HealthBroken:
		h.LastFailedAt = h.LastCheckedAt
		h.FailureReason = topReason(broken)
	}
	if healthRepaired > 0 {
		h.LastRepairAt = h.LastCheckedAt
	}
	if completedAudit {
		h.LastPAR2AuditAt = h.LastCheckedAt
	}

	r.saveHealth(h)
	return h, par2Stats
}

func (r *Service) probeFiles(ctx context.Context, c *candidate, names []string, nzb *nzbProber, opts RunOptions, autoRepair, deepNZB bool) []fileResult {
	results := make([]fileResult, len(names))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(repairFilesPerEntry)
	for i, name := range names {
		g.Go(func() error {
			if gctx.Err() != nil {
				results[i] = fileResult{name: name, reason: "context_cancelled"}
				return nil
			}
			results[i] = r.probeFile(gctx, c, name, nzb, opts, autoRepair, deepNZB)
			return nil
		})
	}
	_ = g.Wait()
	return results
}

func (r *Service) probeFile(ctx context.Context, c *candidate, name string, nzb *nzbProber, opts RunOptions, autoRepair, deepNZB bool) fileResult {
	res := fileResult{name: name}
	if c == nil || c.item == nil {
		res.reason = "entry_not_found"
		return res
	}
	file := c.item.Files[name]

	if file == nil || file.InfoHash == "" {
		res.reason = "missing_infohash"
		return res
	}
	res.infoHash = file.InfoHash

	entry, err := r.storage.Get(file.InfoHash)
	if err != nil || entry == nil {
		res.reason = "entry_not_found"
		return res
	}
	res.protocol = entry.Protocol
	if !repairProtocolMatches(r.effectiveProtocolScope(opts), entry.Protocol) {
		res.reason = "protocol_skipped"
		return res
	}

	if entry.IsNZB() {
		source := r.nzbHydrationSource(c, entry, name)
		return nzb.probe(ctx, nzbProbeRequest{
			nzbID:         entry.InfoHash,
			fileName:      name,
			source:        source,
			autoRepair:    autoRepair,
			deepAudit:     deepNZB,
			verifyContent: opts.VerifyContent != nil && *opts.VerifyContent,
		})
	}
	return r.probeTorrentFile(ctx, entry, file, name, res, opts)
}
