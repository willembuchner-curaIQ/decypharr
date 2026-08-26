package repair

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type memoryLegacyNZBHydrationStore struct {
	mu      sync.Mutex
	records map[string]*storage.LegacyNZBHydration
}

func newMemoryLegacyNZBHydrationStore() *memoryLegacyNZBHydrationStore {
	return &memoryLegacyNZBHydrationStore{records: make(map[string]*storage.LegacyNZBHydration)}
}

func (s *memoryLegacyNZBHydrationStore) SaveLegacyNZBHydration(record *storage.LegacyNZBHydration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *record
	s.records[record.NZBID] = &copy
	return nil
}

func (s *memoryLegacyNZBHydrationStore) DeleteLegacyNZBHydration(nzbID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, nzbID)
	return nil
}

func (s *memoryLegacyNZBHydrationStore) ListLegacyNZBHydrations() ([]*storage.LegacyNZBHydration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]*storage.LegacyNZBHydration, 0, len(s.records))
	for _, record := range s.records {
		copy := *record
		records = append(records, &copy)
	}
	return records, nil
}

func (s *memoryLegacyNZBHydrationStore) get(nzbID string) *storage.LegacyNZBHydration {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[nzbID]
	if record == nil {
		return nil
	}
	copy := *record
	return &copy
}

func newTestLegacyNZBHydrationWorker(
	ids []string,
	store legacyNZBHydrationStateStore,
	hydrate func(context.Context, string, nzbHydrationSource) error,
) *legacyNZBHydrationWorker {
	w := newLegacyNZBHydrationWorker(legacyNZBHydrationWorkerDeps{
		listIDs: func() ([]string, error) { return ids, nil },
		inspect: func(string) (string, bool, error) { return "Sonarr", true, nil },
		hydrate: hydrate,
		store:   store,
		logger:  zerolog.Nop(),
	})
	w.startupDelay = 0
	w.scanInterval = 0
	w.itemInterval = 0
	w.baseBackoff = time.Millisecond
	w.maxBackoff = 10 * time.Millisecond
	w.itemTimeout = time.Second
	return w
}

func TestLegacyNZBHydrationWorkerRunsOneAtATimeAndPacesAttempts(t *testing.T) {
	type call struct {
		nzbID string
		at    time.Time
	}
	calls := make(chan call, 2)
	var active atomic.Int32
	var maximum atomic.Int32
	worker := newTestLegacyNZBHydrationWorker(
		[]string{"b", "a"},
		newMemoryLegacyNZBHydrationStore(),
		func(_ context.Context, nzbID string, _ nzbHydrationSource) error {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			calls <- call{nzbID: nzbID, at: time.Now()}
			time.Sleep(5 * time.Millisecond)
			active.Add(-1)
			return nil
		},
	)
	worker.itemInterval = 35 * time.Millisecond
	worker.start(t.Context())
	defer worker.stop()

	first := receiveLegacyNZBCall(t, calls)
	second := receiveLegacyNZBCall(t, calls)
	if first.nzbID != "a" || second.nzbID != "b" {
		t.Fatalf("attempt order = %q, %q; want a, b", first.nzbID, second.nzbID)
	}
	if elapsed := second.at.Sub(first.at); elapsed < 30*time.Millisecond {
		t.Fatalf("attempt spacing = %s, want at least 30ms", elapsed)
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent hydrations = %d, want 1", maximum.Load())
	}
	eventuallyLegacyNZB(t, time.Second, func() bool {
		return worker.status().Hydrated == 2
	})
}

func TestLegacyNZBHydrationWorkerPausesAndCancelsForRepair(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan error, 1)
	resumed := make(chan struct{})
	var calls atomic.Int32
	worker := newTestLegacyNZBHydrationWorker(
		[]string{"legacy"},
		newMemoryLegacyNZBHydrationStore(),
		func(ctx context.Context, _ string, _ nzbHydrationSource) error {
			if calls.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				canceled <- context.Cause(ctx)
				return ctx.Err()
			}
			close(resumed)
			return nil
		},
	)
	worker.start(t.Context())
	defer worker.stop()

	receiveLegacyNZBSignal(t, started)
	resume := worker.pause()
	defer resume()
	if cause := receiveLegacyNZBError(t, canceled); !errors.Is(cause, errLegacyNZBHydrationPaused) {
		t.Fatalf("cancellation cause = %v", cause)
	}
	select {
	case <-resumed:
		t.Fatal("hydration resumed while repair pause was active")
	case <-time.After(40 * time.Millisecond):
	}
	resume()
	receiveLegacyNZBSignal(t, resumed)
	eventuallyLegacyNZB(t, time.Second, func() bool {
		return worker.status().Hydrated == 1
	})
}

func TestLegacyNZBHydrationWorkerPersistsTransientBackoff(t *testing.T) {
	state := newMemoryLegacyNZBHydrationStore()
	worker := newTestLegacyNZBHydrationWorker(nil, state, func(context.Context, string, nzbHydrationSource) error { return nil })
	worker.baseBackoff = 5 * time.Minute
	worker.maxBackoff = 20 * time.Minute
	worker.itemInterval = time.Minute
	worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr"}, false)

	now := time.Date(2026, 8, 26, 7, 0, 0, 0, time.UTC)
	attempt, attemptCtx, cleanup, _ := worker.takeNext(t.Context(), now)
	if attempt == nil {
		t.Fatal("expected hydration attempt")
	}
	cause := context.Cause(attemptCtx)
	cleanup()
	worker.finishAttempt(*attempt, errors.New("temporary local failure"), cause, now)

	record := state.get("legacy")
	if record == nil || record.State != storage.LegacyNZBHydrationRetrying || record.Attempts != 1 {
		t.Fatalf("persisted record = %+v", record)
	}
	wantRetryAt := now.Add(5 * time.Minute)
	if !record.RetryAt.Equal(wantRetryAt) {
		t.Fatalf("retry at = %s, want %s", record.RetryAt, wantRetryAt)
	}
	if attempt, _, _, waitUntil := worker.takeNext(t.Context(), now.Add(4*time.Minute)); attempt != nil || !waitUntil.Equal(wantRetryAt) {
		t.Fatalf("early attempt = %+v, wait until = %s", attempt, waitUntil)
	}
	worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr", mediaID: 42}, true)
	record = state.get("legacy")
	if record == nil || record.MediaID != 42 || record.State != storage.LegacyNZBHydrationRetrying || !record.RetryAt.Equal(wantRetryAt) {
		t.Fatalf("enriched retry record = %+v", record)
	}
}

func TestLegacyNZBHydrationWorkerPersistsUnavailableAndRevivesWithBetterSource(t *testing.T) {
	state := newMemoryLegacyNZBHydrationStore()
	worker := newTestLegacyNZBHydrationWorker(nil, state, func(context.Context, string, nzbHydrationSource) error { return nil })
	worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr"}, false)
	now := time.Date(2026, 8, 26, 7, 0, 0, 0, time.UTC)
	attempt, attemptCtx, cleanup, _ := worker.takeNext(t.Context(), now)
	cause := context.Cause(attemptCtx)
	cleanup()
	worker.finishAttempt(*attempt, &arr.ReleaseMatchError{Stage: "identifier"}, cause, now)

	if record := state.get("legacy"); record == nil || record.State != storage.LegacyNZBHydrationUnavailable {
		t.Fatalf("persisted record = %+v", record)
	}
	if disposition := worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr"}, true); disposition != legacyNZBHydrationUnavailable {
		t.Fatalf("same-source disposition = %d, want unavailable", disposition)
	}
	if disposition := worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr", mediaID: 42}, true); disposition != legacyNZBHydrationPending {
		t.Fatalf("improved-source disposition = %d, want pending", disposition)
	}
	if record := state.get("legacy"); record != nil {
		t.Fatalf("unavailable record was not cleared: %+v", record)
	}
	status := worker.status()
	if status.Pending != 1 || status.Unavailable != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func TestLegacyNZBHydrationWorkerNotifiesWaitingEntryAfterSuccess(t *testing.T) {
	state := newMemoryLegacyNZBHydrationStore()
	worker := newTestLegacyNZBHydrationWorker(nil, state, func(context.Context, string, nzbHydrationSource) error { return nil })
	marked := make(chan string, 1)
	worker.deps.markReady = func(entryName string) { marked <- entryName }
	worker.enqueue("legacy", nzbHydrationSource{arrName: "Sonarr", entryName: "Series/Season 01"}, true)

	now := time.Date(2026, 8, 26, 7, 0, 0, 0, time.UTC)
	attempt, attemptCtx, cleanup, _ := worker.takeNext(t.Context(), now)
	cause := context.Cause(attemptCtx)
	cleanup()
	worker.finishAttempt(*attempt, nil, cause, now)

	if got := receiveLegacyNZBCall(t, marked); got != "Series/Season 01" {
		t.Fatalf("marked entry = %q", got)
	}
}

func TestLegacyNZBHydrationScanOnlyPrunesRestoredStaleJobs(t *testing.T) {
	worker := newTestLegacyNZBHydrationWorker(
		nil,
		newMemoryLegacyNZBHydrationStore(),
		func(context.Context, string, nzbHydrationSource) error { return nil },
	)
	worker.enqueue("restored-stale", nzbHydrationSource{arrName: "Sonarr"}, false)
	worker.enqueue("foreground", nzbHydrationSource{arrName: "Sonarr", entryName: "Series/Season 01"}, true)

	worker.scan(t.Context(), map[string]struct{}{"restored-stale": {}})

	worker.mu.Lock()
	_, staleTracked := worker.jobs["restored-stale"]
	foreground := worker.jobs["foreground"]
	worker.mu.Unlock()
	if staleTracked {
		t.Fatal("restored job absent from the startup snapshot was not pruned")
	}
	if foreground == nil {
		t.Fatal("foreground job enqueued outside the restored set was pruned")
	}
}

func receiveLegacyNZBCall[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for legacy NZB hydration call")
		var zero T
		return zero
	}
}

func receiveLegacyNZBSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	receiveLegacyNZBCall(t, ch)
}

func receiveLegacyNZBError(t *testing.T, ch <-chan error) error {
	t.Helper()
	return receiveLegacyNZBCall(t, ch)
}

func eventuallyLegacyNZB(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
