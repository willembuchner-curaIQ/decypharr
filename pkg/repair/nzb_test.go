package repair

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
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

func TestNZBProberHydratesLegacyNZBBeforeHealthyProbe(t *testing.T) {
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
			events = append(events, "recovery-status")
			return true
		},
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			t.Fatal("healthy detect-only probe attempted repair")
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		events = append(events, "hydrate")
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: "movie.mkv"})
	if !result.healthy || result.broken {
		t.Fatalf("result = %+v", result)
	}
	want := []string{"detect-legacy", "hydrate", "probe", "recovery-status"}
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

func TestNZBProberHydratesLegacyNZBOnceAcrossFiles(t *testing.T) {
	detections := 0
	hydrations := 0
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
		result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: fileName})
		if !result.healthy {
			t.Fatalf("%s result = %+v", fileName, result)
		}
	}
	if detections != 1 || hydrations != 1 {
		t.Fatalf("detections = %d, hydrations = %d; want 1 each", detections, hydrations)
	}
}

func TestNZBProberContinuesAfterLegacyHydrationFailure(t *testing.T) {
	probed := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return true, nil },
		checkFile: func(context.Context, string, string) error {
			probed = true
			return nil
		},
		canRecover: func(string) bool { return false },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			return usenetpkg.NZBRepairReport{}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		return errors.New("original NZB unavailable")
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{nzbID: "legacy", fileName: "movie.mkv"})
	if !probed || !result.healthy || result.broken {
		t.Fatalf("probed = %t, result = %+v", probed, result)
	}
}

func TestNZBProberHydratesBeforeDeepAudit(t *testing.T) {
	events := make([]string, 0, 5)
	recoverable := false
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
			events = append(events, "recovery-status")
			return recoverable
		},
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			events = append(events, "deep-audit")
			return usenetpkg.NZBRepairReport{Articles: 5}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		events = append(events, "hydrate")
		recoverable = true
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "legacy", fileName: "movie.mkv", autoRepair: true, deepAudit: true,
	})
	if !result.healthy || result.par2 == nil {
		t.Fatalf("result = %+v", result)
	}
	want := []string{"detect-legacy", "hydrate", "probe", "recovery-status", "deep-audit"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestNZBProberStillHydratesMissingNZBWithoutLegacyMarker(t *testing.T) {
	recoverable := false
	client := stubNZBRepairClient{
		needsHydration: func(string) (bool, error) { return false, nil },
		checkFile: func(context.Context, string, string) error {
			return customerror.UsenetSegmentMissingError
		},
		canRecover: func(string) bool { return recoverable },
		repair: func(context.Context, string) (usenetpkg.NZBRepairReport, error) {
			if !recoverable {
				return usenetpkg.NZBRepairReport{}, errors.New("repair ran before hydration")
			}
			return usenetpkg.NZBRepairReport{RepairedFiles: []string{"movie.mkv"}}, nil
		},
		verify: func(context.Context, string, string) error { return nil },
	}
	hydrations := 0
	prober := newNZBProber(client, func(context.Context, string, nzbHydrationSource) error {
		hydrations++
		recoverable = true
		return nil
	}, zerolog.Nop())

	result := prober.probe(t.Context(), nzbProbeRequest{
		nzbID: "modern", fileName: "movie.mkv", autoRepair: true,
	})
	if !result.healthy || result.broken || hydrations != 1 {
		t.Fatalf("result = %+v, hydrations = %d", result, hydrations)
	}
}
