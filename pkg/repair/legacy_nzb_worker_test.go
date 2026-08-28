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
	w.baseBackoff = time.Millisecond
	w.maxBackoff = 10 * time.Millisecond
	w.itemTimeout = time.Second
	return w
}

func TestLegacyNZBHydrationWorkerRunsConcurrentlyUpToLimit(t *testing.T) {
	const concurrency = 4
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	var active, maximum atomic.Int32
	var once sync.Once
	reached := make(chan struct{})
	worker := newTestLegacyNZBHydrationWorker(
		ids,
		newMemoryLegacyNZBHydrationStore(),
		func(ctx context.Context, _ string, _ nzbHydrationSource) error {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			// Hold every attempt until the pool is saturated so the observed
			// maximum reflects the limit rather than scheduling luck.
			if current >= concurrency {
				once.Do(func() { close(reached) })
			}
			select {
			case <-reached:
			case <-ctx.Done():
			}
			active.Add(-1)
			return nil
		},
	)
	worker.concurrency = concurrency
	worker.start(t.Context())
	defer worker.stop()

	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("hydration never reached %d concurrent attempts, peaked at %d", concurrency, maximum.Load())
	}
	eventuallyLegacyNZB(t, 5*time.Second, func() bool {
		return worker.status().Hydrated == len(ids)
	})
	if peak := maximum.Load(); peak > concurrency {
		t.Fatalf("maximum concurrent hydrations = %d, want at most %d", peak, concurrency)
	}
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
	if record.ArrBackoff || record.BackoffFailures != 0 {
		t.Fatalf("item-local failure persisted Arr backoff: %+v", record)
	}
	worker.mu.Lock()
	arrBackoffs := len(worker.arrBackoffs)
	worker.mu.Unlock()
	if arrBackoffs != 0 {
		t.Fatalf("item-local failure created %d Arr backoffs", arrBackoffs)
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

func TestLegacyNZBHydrationWorkerArrFailureBacksOffSiblingJobs(t *testing.T) {
	state := newMemoryLegacyNZBHydrationStore()
	worker := newTestLegacyNZBHydrationWorker(nil, state, func(context.Context, string, nzbHydrationSource) error { return nil })
	worker.baseBackoff = 5 * time.Minute
	worker.maxBackoff = 20 * time.Minute
	worker.enqueue("a", nzbHydrationSource{arrName: "Sonarr"}, false)
	worker.enqueue("b", nzbHydrationSource{arrName: "Sonarr"}, false)

	now := time.Date(2026, 8, 26, 7, 0, 0, 0, time.UTC)
	attempt, attemptCtx, cleanup, _ := worker.takeNext(t.Context(), now)
	if attempt == nil || attempt.nzbID != "a" {
		t.Fatalf("first attempt = %+v, want a", attempt)
	}
	cause := context.Cause(attemptCtx)
	cleanup()
	worker.finishAttempt(*attempt, &arr.NZBMetadataError{Stage: "request", Status: 502}, cause, now)

	record := state.get("a")
	if record == nil || !record.ArrBackoff || record.BackoffFailures != 1 {
		t.Fatalf("persisted Arr backoff = %+v", record)
	}
	wantRetryAt := now.Add(5 * time.Minute)
	if next, _, _, waitUntil := worker.takeNext(t.Context(), now.Add(4*time.Minute)); next != nil || !waitUntil.Equal(wantRetryAt) {
		t.Fatalf("sibling bypassed Arr backoff: next=%+v wait_until=%s", next, waitUntil)
	}
}

func TestLegacyNZBHydrationWorkerItemFailureDoesNotBackOffSiblingJobs(t *testing.T) {
	worker := newTestLegacyNZBHydrationWorker(nil, newMemoryLegacyNZBHydrationStore(), func(context.Context, string, nzbHydrationSource) error { return nil })
	worker.baseBackoff = 5 * time.Minute
	worker.maxBackoff = 20 * time.Minute
	worker.enqueue("a", nzbHydrationSource{arrName: "Sonarr"}, false)
	worker.enqueue("b", nzbHydrationSource{arrName: "Sonarr"}, false)

	now := time.Date(2026, 8, 26, 7, 0, 0, 0, time.UTC)
	attempt, attemptCtx, cleanup, _ := worker.takeNext(t.Context(), now)
	if attempt == nil || attempt.nzbID != "a" {
		t.Fatalf("first attempt = %+v, want a", attempt)
	}
	cause := context.Cause(attemptCtx)
	cleanup()
	worker.finishAttempt(*attempt, errors.New("temporary local failure"), cause, now)

	next, nextCtx, nextCleanup, _ := worker.takeNext(t.Context(), now)
	if next == nil || next.nzbID != "b" {
		t.Fatalf("next attempt = %+v, want unaffected sibling b", next)
	}
	nextCause := context.Cause(nextCtx)
	nextCleanup()
	worker.finishAttempt(*next, nil, nextCause, now)
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
