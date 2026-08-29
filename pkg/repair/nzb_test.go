package repair

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
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

func TestNZBProberHydratesLegacyNZBBeforeProbing(t *testing.T) {
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
		canRecover: func(string) bool { return true },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			t.Fatal("healthy detect-only probe attempted repair")
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(_ context.Context, nzbID string, source nzbHydrationSource) error {
		if nzbID != "legacy" || source.arrName != "Sonarr" {
			t.Fatalf("hydrate(%q, %+v)", nzbID, source)
		}
		events = append(events, "hydrate")
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", source: nzbHydrationSource{arrName: "Sonarr"},
	})
	if !result.healthy || result.broken || result.hydrationErr != nil {
		t.Fatalf("result = %+v", result)
	}
	// Hydration has to precede the probe, or the probe reads stale provenance.
	want := []string{"detect-legacy", "hydrate", "probe"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestNZBProberSkipsHydrationForModernNZB(t *testing.T) {
	hydrated := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile:      func(context.Context, string, string) error { return nil },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		hydrated = true
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "modern", fileName: "movie.mkv"})
	if !result.healthy || hydrated {
		t.Fatalf("result = %+v, hydrated = %t", result, hydrated)
	}
}

// A multi-file entry must hydrate its release once, not once per file.
func TestNZBProberHydratesOncePerNZB(t *testing.T) {
	detections, hydrations := 0, 0
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
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		hydrations++
		return nil
	}, zerolog.Nop())

	for _, fileName := range []string{"movie.mkv", "extras.mkv"} {
		if result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: fileName}); !result.healthy {
			t.Fatalf("%s result = %+v", fileName, result)
		}
	}
	if detections != 1 || hydrations != 1 {
		t.Fatalf("detections = %d, hydrations = %d; want 1 each", detections, hydrations)
	}
}

// Hydration failure must not hide a real fault: the probe still runs and the
// file is reported broken, with the hydration cause carried for the entry.
func TestNZBProberReportsMissingLegacyNZBWhenHydrationFails(t *testing.T) {
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile:      func(context.Context, string, string) error { return customerror.UsenetSegmentMissingError },
		canRecover: func(string) bool {
			t.Fatal("attempted PAR2 recovery on an un-hydrated NZB")
			return false
		},
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		return errLegacyNZBNoArrSource
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: "movie.mkv", autoRepair: true})
	if !result.broken || result.reason != "usenet_segment_missing" {
		t.Fatalf("result = %+v", result)
	}
	if !errors.Is(result.hydrationErr, errLegacyNZBNoArrSource) {
		t.Fatalf("hydration error = %v", result.hydrationErr)
	}
}

// After a successful hydration the same pass may go on to repair.
func TestNZBProberRepairsAfterHydratingInPlace(t *testing.T) {
	repaired := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile:      func(context.Context, string, string) error { return nil },
		canRecover:     func(string) bool { return true },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			repaired = true
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error { return nil }, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", autoRepair: true, deepAudit: true,
	})
	if !repaired || result.broken {
		t.Fatalf("result = %+v, repaired = %t", result, repaired)
	}
}

func TestNZBProberDoesNotHydrateModernMissingNZB(t *testing.T) {
	hydrated := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile:      func(context.Context, string, string) error { return customerror.UsenetSegmentMissingError },
		canRecover:     func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		hydrated = true
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "modern", fileName: "movie.mkv", autoRepair: true,
	})
	if !result.broken || hydrated {
		t.Fatalf("result = %+v, hydrated = %t", result, hydrated)
	}
}

func TestRollupStatusKeepsUnresolvedEntryUnknown(t *testing.T) {
	results := []fileResult{
		{name: "healthy.mkv", healthy: true},
		{name: "unresolved.mkv", deferred: true, reason: "usenet_probe_error"},
	}
	if got := rollupStatus(results); got != storage.HealthUnknown {
		t.Fatalf("status = %q, want unknown", got)
	}
	results = append(results, fileResult{name: "broken.mkv", broken: true})
	if got := rollupStatus(results); got != storage.HealthBroken {
		t.Fatalf("status with definitive failure = %q, want broken", got)
	}
}
