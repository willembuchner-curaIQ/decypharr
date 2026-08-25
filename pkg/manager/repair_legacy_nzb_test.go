package manager

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
)

func TestHydrateLegacyNZBFromSourcesPrefersLocalSource(t *testing.T) {
	local := []byte("local NZB")
	reacquired := atomic.Bool{}
	hydrated := atomic.Bool{}

	err := hydrateLegacyNZBFromSources(
		context.Background(),
		"nzb-id",
		func(nzbID string) (string, []byte, error) {
			if nzbID != "nzb-id" {
				t.Fatalf("load NZB ID = %q", nzbID)
			}
			return "original.nzb", local, nil
		},
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return []byte("remote NZB"), nil
		},
		func(_ context.Context, nzbID, sourceName string, content []byte) error {
			hydrated.Store(true)
			if nzbID != "nzb-id" || sourceName != "original.nzb" || string(content) != string(local) {
				t.Fatalf("hydrate(%q, %q, %q)", nzbID, sourceName, content)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reacquired.Load() {
		t.Fatal("Arr reacquisition ran despite a usable local source")
	}
	if !hydrated.Load() {
		t.Fatal("local source was not hydrated")
	}
}

func TestHydrateLegacyNZBFromSourcesFallsBackToArr(t *testing.T) {
	remote := []byte("remote NZB")
	reacquired := atomic.Bool{}

	err := hydrateLegacyNZBFromSources(
		context.Background(),
		"nzb-id",
		func(string) (string, []byte, error) {
			return "", nil, usenetpkg.ErrLegacyNZBSourceUnavailable
		},
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return remote, nil
		},
		func(_ context.Context, nzbID, sourceName string, content []byte) error {
			if nzbID != "nzb-id" || sourceName != "nzb-id.nzb" || string(content) != string(remote) {
				t.Fatalf("hydrate(%q, %q, %q)", nzbID, sourceName, content)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reacquired.Load() {
		t.Fatal("Arr reacquisition did not run after local source unavailability")
	}
}

func TestHydrateLegacyNZBFromSourcesPreservesUnexpectedLocalError(t *testing.T) {
	loadErr := errors.New("local read failed")
	reacquired := atomic.Bool{}
	err := hydrateLegacyNZBFromSources(
		context.Background(),
		"nzb-id",
		func(string) (string, []byte, error) { return "", nil, loadErr },
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return nil, nil
		},
		func(context.Context, string, string, []byte) error { return nil },
	)
	if !errors.Is(err, loadErr) {
		t.Fatalf("error = %v, want wrapped local error", err)
	}
	if reacquired.Load() {
		t.Fatal("Arr reacquisition ran after an unexpected local loader error")
	}
}

func TestLegacyNZBRecoveryStateCachesStableFailureUntilExpiry(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	state := newLegacyNZBRecoveryState()
	state.cooldown = 24 * time.Hour
	state.now = func() time.Time { return now }
	failure := errors.New("no matching release")
	calls := 0
	attempt := func() error {
		calls++
		return failure
	}

	if err := state.do("arr\x00nzb", attempt); !errors.Is(err, failure) {
		t.Fatalf("first error = %v", err)
	}
	if err := state.do("arr\x00nzb", attempt); !errors.Is(err, errLegacyNZBCooldown) || !errors.Is(err, failure) {
		t.Fatalf("cached error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("attempt calls during cooldown = %d, want 1", calls)
	}

	now = now.Add(24*time.Hour + time.Second)
	if err := state.do("arr\x00nzb", attempt); !errors.Is(err, failure) || errors.Is(err, errLegacyNZBCooldown) {
		t.Fatalf("post-expiry error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("attempt calls after expiry = %d, want 2", calls)
	}
}

func TestLegacyNZBRecoveryStateDoesNotCacheOperationalFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "cancelled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "upstream 500", err: &arr.NZBMetadataError{Stage: "request", Status: http.StatusBadGateway}},
		{name: "throttled", err: &arr.NZBMetadataError{Stage: "request", Status: http.StatusTooManyRequests}},
		{name: "release search 503", err: errors.New("release search failed: 503 Service Unavailable")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newLegacyNZBRecoveryState()
			calls := 0
			for range 2 {
				if err := state.do("arr\x00nzb", func() error {
					calls++
					return test.err
				}); !errors.Is(err, test.err) {
					t.Fatalf("error = %v, want %v", err, test.err)
				}
			}
			if calls != 2 {
				t.Fatalf("attempt calls = %d, want 2", calls)
			}
		})
	}
}

func TestLegacyNZBRecoveryStateCoalescesConcurrentAttempts(t *testing.T) {
	const callers = 24
	state := newLegacyNZBRecoveryState()
	var calls atomic.Int64
	start := make(chan struct{})
	release := make(chan struct{})
	ready := sync.WaitGroup{}
	done := sync.WaitGroup{}
	ready.Add(callers)
	done.Add(callers)
	errs := make([]error, callers)

	for i := range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errs[i] = state.do("arr\x00nzb", func() error {
				calls.Add(1)
				<-release
				return errors.New("permanent failure")
			})
		}()
	}
	ready.Wait()
	close(start)
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	done.Wait()

	if calls.Load() != 1 {
		t.Fatalf("attempt calls = %d, want 1", calls.Load())
	}
	for i, err := range errs {
		if err == nil {
			t.Fatalf("caller %d received nil error", i)
		}
	}
}

func TestLegacyNZBMediaID(t *testing.T) {
	content := arr.ContentFile{Id: 41, EpisodeId: 82}
	if got := legacyNZBMediaID(arr.Sonarr, content); got != 82 {
		t.Fatalf("Sonarr media ID = %d, want 82", got)
	}
	if got := legacyNZBMediaID(arr.Radarr, content); got != 41 {
		t.Fatalf("Radarr media ID = %d, want 41", got)
	}
	if got := legacyNZBMediaID(arr.Lidarr, content); got != 0 {
		t.Fatalf("unsupported Arr media ID = %d, want 0", got)
	}
}

func TestNZBRepairConfirmedForFile(t *testing.T) {
	if !nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{RepairedFiles: []string{"movie.mkv"}}, "movie.mkv") {
		t.Fatal("fully repaired file was not confirmed")
	}
	if nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{
		RepairedFiles: []string{"movie.mkv"},
		FailedFiles:   []string{"movie.mkv"},
	}, "movie.mkv") {
		t.Fatal("partially failed file was confirmed")
	}
	if nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{RepairedFiles: []string{"other.mkv"}}, "movie.mkv") {
		t.Fatal("unaffected file was confirmed")
	}
}
