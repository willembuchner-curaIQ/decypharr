package repair

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/nntp"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

type nzbRepairClient interface {
	NeedsPAR2Hydration(string) (bool, error)
	CheckFile(context.Context, string, string) error
	CanRecoverWithPAR2(string) bool
	RepairNZB(context.Context, string) (usenetpkg.NZBRepairReport, error)
	VerifyFile(context.Context, string, string) error
}

type nzbHydrationSource struct {
	arrName   string
	mediaID   int
	entryName string
}

type nzbProbeRequest struct {
	nzbID         string
	fileName      string
	source        nzbHydrationSource
	autoRepair    bool
	deepAudit     bool
	verifyContent bool
	// skipHydration suppresses in-place hydration for an entry whose last
	// attempt failed for a reason that will not resolve on its own. A manual
	// hydration request always leaves this false so the user can force a retry.
	skipHydration bool
}

type nzbRepairOutcome struct {
	report  usenetpkg.NZBRepairReport
	err     error
	counted atomic.Bool
}

type nzbRepairCache struct {
	flights singleflight.Group
	results *xsync.Map[string, *nzbRepairOutcome]
}

type nzbLegacyPreparation struct {
	needed bool
	err    error
}

type nzbLegacyPreparationCache struct {
	flights singleflight.Group
	results *xsync.Map[string, *nzbLegacyPreparation]
}

func newNZBLegacyPreparationCache() *nzbLegacyPreparationCache {
	return &nzbLegacyPreparationCache{results: xsync.NewMap[string, *nzbLegacyPreparation]()}
}

func (c *nzbLegacyPreparationCache) do(nzbID string, prepare func() *nzbLegacyPreparation) *nzbLegacyPreparation {
	if c == nil || nzbID == "" {
		return prepare()
	}
	if result, ok := c.results.Load(nzbID); ok {
		return result
	}
	value, _, _ := c.flights.Do(nzbID, func() (any, error) {
		if result, ok := c.results.Load(nzbID); ok {
			return result, nil
		}
		result := prepare()
		c.results.Store(nzbID, result)
		return result, nil
	})
	return value.(*nzbLegacyPreparation)
}

func newNZBRepairCache() *nzbRepairCache {
	return &nzbRepairCache{results: xsync.NewMap[string, *nzbRepairOutcome]()}
}

func (c *nzbRepairCache) do(nzbID string, repair func() (usenetpkg.NZBRepairReport, error)) *nzbRepairOutcome {
	if c == nil || nzbID == "" {
		report, err := repair()
		return &nzbRepairOutcome{report: report, err: err}
	}
	if result, ok := c.results.Load(nzbID); ok {
		return result
	}
	value, _, _ := c.flights.Do(nzbID, func() (any, error) {
		if result, ok := c.results.Load(nzbID); ok {
			return result, nil
		}
		report, err := repair()
		result := &nzbRepairOutcome{report: report, err: err}
		c.results.Store(nzbID, result)
		return result, nil
	})
	return value.(*nzbRepairOutcome)
}

type nzbProber struct {
	client       nzbRepairClient
	hydrate      func(context.Context, string, nzbHydrationSource) error
	preparations *nzbLegacyPreparationCache
	repairs      *nzbRepairCache
	logger       zerolog.Logger
}

func newNZBProber(client nzbRepairClient, hydrate func(context.Context, string, nzbHydrationSource) error, logger zerolog.Logger) *nzbProber {
	return &nzbProber{
		client:       client,
		hydrate:      hydrate,
		preparations: newNZBLegacyPreparationCache(),
		repairs:      newNZBRepairCache(),
		logger:       logger,
	}
}

func (p *nzbProber) probe(ctx context.Context, request nzbProbeRequest) fileResult {
	result := fileResult{
		name:     request.fileName,
		infoHash: request.nzbID,
		protocol: config.ProtocolNZB,
	}
	if p == nil || p.client == nil {
		result.reason = "usenet_client_not_configured"
		return result
	}

	// Hydration now happens here rather than in a background worker: an NZB
	// without PAR2 provenance is hydrated before it is probed, so one pass
	// settles the file instead of deferring it to a later run.
	legacy := p.prepareLegacy(ctx, request)
	result.hydrationErr = legacy.err

	probeErr := p.client.CheckFile(ctx, request.nzbID, request.fileName)
	sampledMissing := errors.Is(probeErr, customerror.UsenetSegmentMissingError)

	canRecover := !legacy.needed && p.client.CanRecoverWithPAR2(request.nzbID)
	shouldRepair := request.autoRepair && canRecover && (sampledMissing || request.deepAudit && probeErr == nil)
	if shouldRepair {
		outcome := p.repairs.do(request.nzbID, func() (usenetpkg.NZBRepairReport, error) {
			return p.client.RepairNZB(ctx, request.nzbID)
		})
		result.par2 = outcome
		if done := applyNZBRepairOutcome(&result, outcome, request.fileName, sampledMissing); done {
			return result
		}
		probeErr = nil
	}

	if probeErr == nil && request.verifyContent {
		probeErr = p.client.VerifyFile(ctx, request.nzbID, request.fileName)
	}
	if probeErr == nil {
		result.healthy = true
		return result
	}

	switch {
	case errors.Is(probeErr, customerror.UsenetSegmentMissingError):
		result.broken = true
		result.reason = "usenet_segment_missing"
	case errors.Is(probeErr, customerror.UsenetCorruptContentError):
		result.broken = true
		result.reason = "usenet_corrupt_content"
	default:
		result.reason = "usenet_probe_error"
	}
	return result
}

// prepareLegacy resolves an NZB's PAR2 provenance, hydrating it in place when
// it is missing. Detection and hydration share one flight per NZB so the files
// of a multi-file entry cannot hydrate the same release concurrently.
func (p *nzbProber) prepareLegacy(ctx context.Context, request nzbProbeRequest) *nzbLegacyPreparation {
	return p.preparations.do(request.nzbID, func() *nzbLegacyPreparation {
		needsHydration, err := p.client.NeedsPAR2Hydration(request.nzbID)
		if err != nil {
			return &nzbLegacyPreparation{needed: true, err: err}
		}
		if !needsHydration {
			return &nzbLegacyPreparation{}
		}
		if p.hydrate == nil || request.skipHydration {
			return &nzbLegacyPreparation{needed: true}
		}
		if err := p.hydrate(ctx, request.nzbID, request.source); err != nil {
			return &nzbLegacyPreparation{needed: true, err: err}
		}
		return &nzbLegacyPreparation{}
	})
}

func applyNZBRepairOutcome(result *fileResult, outcome *nzbRepairOutcome, fileName string, sampledMissing bool) bool {
	report, repairErr := outcome.report, outcome.err
	fileUnknown := slices.Contains(report.UnknownFiles, fileName)
	fileFailed := slices.Contains(report.FailedFiles, fileName)
	fileRepaired := nzbRepairConfirmedForFile(report, fileName)
	unattributed := len(report.AffectedFiles) == 0 && len(report.UnknownFiles) == 0 &&
		len(report.RepairedFiles) == 0 && len(report.FailedFiles) == 0

	switch {
	case fileRepaired, repairErr == nil && !sampledMissing:
		return false
	case repairErr == nil:
		result.broken = true
		result.reason = "usenet_segment_missing"
	case errors.Is(repairErr, errLegacyNZBHydration):
		result.broken = true
		result.reason = "usenet_segment_missing"
	case errors.Is(repairErr, context.Canceled), errors.Is(repairErr, context.DeadlineExceeded), fileUnknown, unattributed:
		result.reason = "usenet_par2_probe_error"
	case fileFailed || sampledMissing:
		fileErr := report.FileError(fileName)
		if fileErr == nil {
			fileErr = repairErr
		}
		if reason, definitive := par2RepairFailureReason(fileErr); definitive {
			result.broken = true
			result.reason = reason
		} else {
			result.reason = "usenet_par2_repair_error"
		}
	default:
		return false
	}
	return true
}

func nzbRepairConfirmedForFile(report usenetpkg.NZBRepairReport, name string) bool {
	return slices.Contains(report.RepairedFiles, name) &&
		!slices.Contains(report.UnknownFiles, name) &&
		!slices.Contains(report.FailedFiles, name)
}

func par2RepairFailureReason(err error) (string, bool) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", false
	}
	if nntpErr, ok := errors.AsType[*nntp.Error](err); ok {
		switch nntpErr.Type {
		case nntp.ErrorTypeArticleNotFound:
			return "usenet_par2_insufficient", true
		case nntp.ErrorTypeYencDecode:
			return "usenet_par2_corrupt", true
		default:
			return "", false
		}
	}
	switch {
	case errors.Is(err, recovery.ErrBudgetExceeded), errors.Is(err, recovery.ErrUnboundedTraffic):
		return "usenet_par2_budget_exceeded", true
	case errors.Is(err, recovery.ErrStorageBudget):
		return "usenet_par2_storage_exceeded", true
	case errors.Is(err, recovery.ErrNoRecoverySet), errors.Is(err, par2.ErrNotEnoughRecovery), errors.Is(err, par2.ErrSingularSelection):
		return "usenet_par2_insufficient", true
	case errors.Is(err, recovery.ErrLayoutUnavailable), errors.Is(err, recovery.ErrAmbiguousMapping), errors.Is(err, recovery.ErrLegacyManifestUnsupported), errors.Is(err, recovery.ErrUnsupported), errors.Is(err, recovery.ErrInvalid):
		return "usenet_par2_unsupported", true
	case errors.Is(err, recovery.ErrCorrupt), errors.Is(err, recovery.ErrChecksumMismatch), errors.Is(err, par2.ErrInvalidMagic), errors.Is(err, par2.ErrInvalidLength), errors.Is(err, par2.ErrPacketTooLarge), errors.Is(err, par2.ErrPacketHash), errors.Is(err, par2.ErrTruncated), errors.Is(err, par2.ErrInvalidPacket):
		return "usenet_par2_corrupt", true
	default:
		return "", false
	}
}
