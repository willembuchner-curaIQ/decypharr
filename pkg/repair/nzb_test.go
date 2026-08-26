package repair

import (
	"context"
	"slices"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

type stubNZBRepairClient struct {
	needsHydration func(string) (bool, error)
	checkFile      func(context.Context, string, string) error
	canRecover     func(string) bool
	repair         func(context.Context, string) (usenetpkg.NZBRepairReport, error)
	verify         func(context.Context, string, string) error
}

func (c stubNZBRepairClient) NeedsPAR2Hydration(nzbID string) (bool, error) {
	return c.needsHydration(nzbID)
}

func (c stubNZBRepairClient) CheckFile(ctx context.Context, nzbID, fileName string) error {
	return c.checkFile(ctx, nzbID, fileName)
}

func (c stubNZBRepairClient) CanRecoverWithPAR2(nzbID string) bool {
	return c.canRecover(nzbID)
}

func (c stubNZBRepairClient) RepairNZB(ctx context.Context, nzbID string) (usenetpkg.NZBRepairReport, error) {
	return c.repair(ctx, nzbID)
}

func (c stubNZBRepairClient) VerifyFile(ctx context.Context, nzbID, fileName string) error {
	return c.verify(ctx, nzbID, fileName)
}

func TestNZBProberEnqueuesLegacyNZBAndKeepsHealthyProbe(t *testing.T) {
	events := make([]string, 0, 4)
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) {
			events = append(events, "detect-legacy")
			return true, nil
		},
		checkFile: func(context.Context, string, string) error {
			events = append(events, "probe")
			return nil
		},
		canRecover: func(string) bool {
			t.Fatal("legacy probe checked PAR2 before background hydration")
			return false
		},
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			t.Fatal("healthy detect-only probe attempted repair")
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(nzbID string, source nzbHydrationSource) legacyNZBHydrationDisposition {
		if nzbID != "legacy" || source.arrName != "Sonarr" {
			t.Fatalf("enqueue(%q, %+v)", nzbID, source)
		}
		events = append(events, "enqueue")
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", source: nzbHydrationSource{arrName: "Sonarr"},
	})
	if !result.healthy || result.broken || result.deferred {
		t.Fatalf("result = %+v", result)
	}
	want := []string{"detect-legacy", "enqueue", "probe"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestNZBProberSkipsHydrationForModernNZB(t *testing.T) {
	enqueued := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile:      func(context.Context, string, string) error { return nil },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		enqueued = true
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "modern", fileName: "movie.mkv"})
	if !result.healthy || enqueued {
		t.Fatalf("result = %+v, enqueued = %t", result, enqueued)
	}
}

func TestNZBProberDetectsLegacyNZBOnceAndRefreshesQueueSource(t *testing.T) {
	detections := 0
	enqueues := 0
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) {
			detections++
			return true, nil
		},
		checkFile:  func(context.Context, string, string) error { return nil },
		canRecover: func(string) bool { return true },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		enqueues++
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	for _, fileName := range []string{"movie.mkv", "extras.mkv"} {
		result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: fileName})
		if !result.healthy {
			t.Fatalf("%s result = %+v", fileName, result)
		}
	}
	if detections != 1 || enqueues != 2 {
		t.Fatalf("detections = %d, enqueues = %d; want 1 detection and 2 source refreshes", detections, enqueues)
	}
}

func TestNZBProberDefersMissingLegacyNZBWhileHydrationIsPending(t *testing.T) {
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile:      func(context.Context, string, string) error { return customerror.UsenetSegmentMissingError },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: "movie.mkv"})
	if !result.deferred || result.healthy || result.broken || result.reason != "legacy_hydration_pending" {
		t.Fatalf("result = %+v", result)
	}
}

func TestNZBProberTreatsMissingLegacyNZBAsBrokenWhenHydrationIsUnavailable(t *testing.T) {
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile:      func(context.Context, string, string) error { return customerror.UsenetSegmentMissingError },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		return legacyNZBHydrationUnavailable
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: "movie.mkv"})
	if !result.broken || result.deferred || result.reason != "usenet_segment_missing" {
		t.Fatalf("result = %+v", result)
	}
}

func TestNZBProberDefersLegacyDeepAuditUntilHydrationFinishes(t *testing.T) {
	repaired := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile:      func(context.Context, string, string) error { return nil },
		canRecover: func(string) bool {
			t.Fatal("checked recovery before hydration")
			return false
		},
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			repaired = true
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", autoRepair: true, deepAudit: true,
	})
	if !result.deferred || result.healthy || result.broken || repaired {
		t.Fatalf("result = %+v, repaired = %t", result, repaired)
	}
}

func TestNZBProberDoesNotHydrateModernMissingNZB(t *testing.T) {
	enqueued := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile:      func(context.Context, string, string) error { return customerror.UsenetSegmentMissingError },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		enqueued = true
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "modern", fileName: "movie.mkv", autoRepair: true,
	})
	if !result.broken || result.deferred || enqueued {
		t.Fatalf("result = %+v, enqueued = %t", result, enqueued)
	}
}

func TestNZBProberQueuesUnexpectedLegacyManifestDuringDeepAudit(t *testing.T) {
	enqueues := 0
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile:      func(context.Context, string, string) error { return nil },
		canRecover:     func(string) bool { return true },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, recovery.ErrLegacyManifestUnsupported
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(string, nzbHydrationSource) legacyNZBHydrationDisposition {
		enqueues++
		return legacyNZBHydrationPending
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", autoRepair: true, deepAudit: true,
	})
	if !result.deferred || result.broken || enqueues != 1 {
		t.Fatalf("result = %+v, enqueues = %d", result, enqueues)
	}
}

func TestRollupStatusKeepsDeferredLegacyEntryUnknown(t *testing.T) {
	results := []fileResult{
		{name: "healthy.mkv", healthy: true},
		{name: "legacy.mkv", deferred: true, reason: "legacy_hydration_pending"},
	}
	if got := rollupStatus(results); got != storage.HealthUnknown {
		t.Fatalf("status = %q, want unknown", got)
	}
	if got := firstDeferredReason(results); got != "legacy_hydration_pending" {
		t.Fatalf("deferred reason = %q", got)
	}
	results = append(results, fileResult{name: "broken.mkv", broken: true})
	if got := rollupStatus(results); got != storage.HealthBroken {
		t.Fatalf("status with definitive failure = %q, want broken", got)
	}
}
