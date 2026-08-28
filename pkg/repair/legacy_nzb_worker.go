package repair

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

const (
	legacyNZBHydrationStartupDelay = 15 * time.Second
	legacyNZBHydrationBaseBackoff  = time.Minute
	legacyNZBHydrationMaxBackoff   = 15 * time.Minute
	legacyNZBHydrationItemTimeout  = 10 * time.Minute
	legacyNZBHydrationStopTimeout  = 30 * time.Second
	legacyNZBHydrationErrorLimit   = 2048
	legacyNZBHydrationConcurrency  = 8
)

var errLegacyNZBHydrationPaused = errors.New("legacy NZB hydration paused for repair")

type legacyNZBHydrationDisposition uint8

const (
	legacyNZBHydrationPending legacyNZBHydrationDisposition = iota
	legacyNZBHydrationUnavailable
)

type legacyNZBHydrationJobState uint8

const (
	legacyNZBHydrationJobPending legacyNZBHydrationJobState = iota
	legacyNZBHydrationJobRetrying
	legacyNZBHydrationJobUnavailable
)

type legacyNZBHydrationStateStore interface {
	SaveLegacyNZBHydration(*storage.LegacyNZBHydration) error
	DeleteLegacyNZBHydration(string) error
	ListLegacyNZBHydrations() ([]*storage.LegacyNZBHydration, error)
}

type legacyNZBHydrationWorkerDeps struct {
	listIDs   func() ([]string, error)
	inspect   func(string) (arrName string, needed bool, err error)
	hydrate   func(context.Context, string, nzbHydrationSource) error
	markReady func(string)
	store     legacyNZBHydrationStateStore
	logger    zerolog.Logger
}

type legacyNZBHydrationJob struct {
	nzbID           string
	source          nzbHydrationSource
	state           legacyNZBHydrationJobState
	attempts        int
	arrBackoff      bool
	backoffFailures int
	retryAt         time.Time
	lastAttemptAt   time.Time
	priority        bool
	inFlight        bool
	version         uint64
	lastError       string
	entryNames      map[string]struct{}
}

type legacyNZBArrBackoff struct {
	failures int
	retryAt  time.Time
}

type legacyNZBHydrationAttempt struct {
	nzbID   string
	source  nzbHydrationSource
	version uint64
}

// LegacyNZBHydrationStatus is exposed through the repair status endpoint.
// It describes migration activity, not the health of any particular entry.
type LegacyNZBHydrationStatus struct {
	Running       bool       `json:"running"`
	Paused        bool       `json:"paused"`
	ScanComplete  bool       `json:"scan_complete"`
	Pending       int        `json:"pending"`
	Retrying      int        `json:"retrying"`
	Unavailable   int        `json:"unavailable"`
	Hydrated      int        `json:"hydrated"`
	InFlight      int        `json:"in_flight"`
	CurrentNZBID  string     `json:"current_nzb_id,omitempty"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
}

// legacyNZBHydrationInFlight tracks one hydration attempt so that a repair
// pause can cancel it and wait for it to unwind.
type legacyNZBHydrationInFlight struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// legacyNZBHydrationWorker drains the legacy NZB backlog with a bounded pool.
// A single dispatcher picks jobs so ordering and Arr backoff stay centralised;
// only the network call itself runs concurrently. Failures still back off per
// job, and per Arr, which is what protects Sonarr/Radarr from a stampede.
type legacyNZBHydrationWorker struct {
	deps legacyNZBHydrationWorkerDeps

	startupDelay time.Duration
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	itemTimeout  time.Duration
	concurrency  int

	mu           sync.Mutex
	jobs         map[string]*legacyNZBHydrationJob
	arrBackoffs  map[string]legacyNZBArrBackoff
	inFlight     map[string]*legacyNZBHydrationInFlight
	wake         chan struct{}
	pauses       int
	running      bool
	scanComplete bool
	hydrated     int
	cancel       context.CancelFunc
	done         chan struct{}
}

func newLegacyNZBHydrationWorker(deps legacyNZBHydrationWorkerDeps) *legacyNZBHydrationWorker {
	if deps.listIDs == nil || deps.inspect == nil || deps.hydrate == nil || deps.store == nil {
		return nil
	}
	return &legacyNZBHydrationWorker{
		deps:         deps,
		startupDelay: legacyNZBHydrationStartupDelay,
		baseBackoff:  legacyNZBHydrationBaseBackoff,
		maxBackoff:   legacyNZBHydrationMaxBackoff,
		itemTimeout:  legacyNZBHydrationItemTimeout,
		concurrency:  legacyNZBHydrationConcurrency,
		jobs:         make(map[string]*legacyNZBHydrationJob),
		arrBackoffs:  make(map[string]legacyNZBArrBackoff),
		inFlight:     make(map[string]*legacyNZBHydrationInFlight),
		wake:         make(chan struct{}, 1),
	}
}

func (w *legacyNZBHydrationWorker) start(parent context.Context) {
	if w == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	w.running = true
	w.scanComplete = false
	w.cancel = cancel
	w.done = done
	w.mu.Unlock()

	go w.run(ctx, done)
}

func (w *legacyNZBHydrationWorker) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	cancel := w.cancel
	inFlight := w.inFlightLocked()
	done := w.done
	w.mu.Unlock()
	for _, attempt := range inFlight {
		attempt.cancel(context.Canceled)
	}
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return
	}
	timer := time.NewTimer(legacyNZBHydrationStopTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		w.deps.logger.Warn().Msg("Legacy NZB hydration worker did not stop before timeout")
	}
}

func (w *legacyNZBHydrationWorker) run(ctx context.Context, done chan struct{}) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		w.mu.Lock()
		w.running = false
		w.cancel = nil
		w.mu.Unlock()
		close(done)
	}()

	if !waitLegacyNZBDuration(ctx, w.startupDelay) || !w.waitUntilResumed(ctx) {
		return
	}
	restored := w.restorePersisted()
	if ctx.Err() != nil {
		return
	}
	w.scan(ctx, restored)

	slots := make(chan struct{}, max(1, w.concurrency))
	for ctx.Err() == nil {
		if !w.waitUntilResumed(ctx) {
			return
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		attempt, attemptCtx, cleanup, waitUntil := w.takeNext(ctx, time.Now())
		if attempt == nil {
			<-slots
			if !w.waitForWork(ctx, waitUntil) {
				return
			}
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			err := w.deps.hydrate(attemptCtx, attempt.nzbID, attempt.source)
			cause := context.Cause(attemptCtx)
			cleanup()
			w.finishAttempt(*attempt, err, cause, time.Now())
		}()
	}
}

func waitLegacyNZBDuration(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *legacyNZBHydrationWorker) waitUntilResumed(ctx context.Context) bool {
	for {
		w.mu.Lock()
		paused := w.pauses > 0
		w.mu.Unlock()
		if !paused {
			return ctx.Err() == nil
		}
		select {
		case <-ctx.Done():
			return false
		case <-w.wake:
		}
	}
}

func (w *legacyNZBHydrationWorker) restorePersisted() map[string]struct{} {
	restored := make(map[string]struct{})
	records, err := w.deps.store.ListLegacyNZBHydrations()
	if err != nil {
		w.deps.logger.Warn().Err(err).Msg("Legacy NZB hydration state could not be loaded")
		return restored
	}
	for _, record := range records {
		if record != nil && strings.TrimSpace(record.NZBID) != "" {
			restored[record.NZBID] = struct{}{}
		}
		w.restoreRecord(record)
	}
	return restored
}

func (w *legacyNZBHydrationWorker) restoreRecord(record *storage.LegacyNZBHydration) {
	if record == nil || strings.TrimSpace(record.NZBID) == "" {
		return
	}
	source := nzbHydrationSource{arrName: strings.TrimSpace(record.ArrName), mediaID: record.MediaID}
	deleteRecord := false
	w.mu.Lock()
	job := w.jobs[record.NZBID]
	if job != nil && legacyNZBSourceImproves(source, job.source) {
		deleteRecord = true
	} else {
		if job == nil {
			job = &legacyNZBHydrationJob{nzbID: record.NZBID}
			w.jobs[record.NZBID] = job
		}
		job.source, _ = mergeLegacyNZBSource(job.source, source)
		job.attempts = max(job.attempts, record.Attempts)
		job.arrBackoff = record.ArrBackoff
		job.backoffFailures = max(job.backoffFailures, record.BackoffFailures)
		job.retryAt = record.RetryAt
		if record.LastAttemptAt.After(job.lastAttemptAt) {
			job.lastAttemptAt = record.LastAttemptAt
		}
		job.lastError = record.LastError
		switch record.State {
		case storage.LegacyNZBHydrationUnavailable:
			job.state = legacyNZBHydrationJobUnavailable
		case storage.LegacyNZBHydrationRetrying:
			job.state = legacyNZBHydrationJobRetrying
			if job.arrBackoff {
				w.restoreArrBackoffLocked(job)
			}
		default:
			delete(w.jobs, record.NZBID)
			deleteRecord = true
		}
	}
	w.signalLocked()
	w.mu.Unlock()
	if deleteRecord {
		w.deleteRecord(record.NZBID)
	}
}

func (w *legacyNZBHydrationWorker) scan(ctx context.Context, restored map[string]struct{}) {
	ids, err := w.deps.listIDs()
	if err != nil {
		w.deps.logger.Warn().Err(err).Msg("Legacy NZB hydration scan failed")
		return
	}
	seen := make(map[string]struct{}, len(ids))
	for _, nzbID := range ids {
		if !w.waitUntilResumed(ctx) {
			return
		}
		seen[nzbID] = struct{}{}
		arrName, needed, err := w.deps.inspect(nzbID)
		if err != nil {
			w.deps.logger.Debug().Err(err).Str("nzb_id", nzbID).Msg("Legacy NZB hydration scan skipped an unreadable item")
		} else if needed {
			w.enqueue(nzbID, nzbHydrationSource{arrName: arrName}, false)
		} else {
			w.remove(nzbID)
		}
	}

	w.mu.Lock()
	stale := make([]string, 0)
	for nzbID := range restored {
		if _, ok := seen[nzbID]; !ok {
			stale = append(stale, nzbID)
		}
	}
	w.scanComplete = true
	w.mu.Unlock()
	for _, nzbID := range stale {
		w.remove(nzbID)
	}

	status := w.status()
	if status.Pending+status.Retrying+status.Unavailable > 0 {
		w.deps.logger.Info().
			Int("pending", status.Pending).
			Int("retrying", status.Retrying).
			Int("unavailable", status.Unavailable).
			Msg("Legacy NZB background hydration scan completed")
	}
}

func (w *legacyNZBHydrationWorker) enqueue(nzbID string, source nzbHydrationSource, priority bool) legacyNZBHydrationDisposition {
	if w == nil || strings.TrimSpace(nzbID) == "" {
		return legacyNZBHydrationUnavailable
	}
	nzbID = strings.TrimSpace(nzbID)
	source.arrName = strings.TrimSpace(source.arrName)
	entryName := strings.TrimSpace(source.entryName)
	source.entryName = ""
	deleteRecord := false
	var updateRecord *storage.LegacyNZBHydration
	disposition := legacyNZBHydrationPending

	w.mu.Lock()
	job := w.jobs[nzbID]
	if job == nil {
		job = &legacyNZBHydrationJob{
			nzbID:      nzbID,
			source:     source,
			priority:   priority,
			entryNames: make(map[string]struct{}),
		}
		w.jobs[nzbID] = job
	} else {
		var improved bool
		job.source, improved = mergeLegacyNZBSource(job.source, source)
		job.priority = job.priority || priority
		if improved {
			job.version++
			if job.state == legacyNZBHydrationJobRetrying {
				updateRecord = legacyNZBHydrationRecord(job, storage.LegacyNZBHydrationRetrying)
			}
		}
		if job.state == legacyNZBHydrationJobUnavailable {
			if improved {
				job.state = legacyNZBHydrationJobPending
				job.attempts = 0
				job.arrBackoff = false
				job.backoffFailures = 0
				job.retryAt = time.Time{}
				job.lastError = ""
				deleteRecord = true
			} else {
				disposition = legacyNZBHydrationUnavailable
			}
		}
	}
	if entryName != "" {
		if job.entryNames == nil {
			job.entryNames = make(map[string]struct{})
		}
		job.entryNames[entryName] = struct{}{}
	}
	w.signalLocked()
	w.mu.Unlock()
	if deleteRecord {
		w.deleteRecord(nzbID)
	} else if updateRecord != nil {
		w.saveRecord(updateRecord)
	}
	return disposition
}

func mergeLegacyNZBSource(current, incoming nzbHydrationSource) (nzbHydrationSource, bool) {
	improved := false
	if incoming.arrName != "" && !strings.EqualFold(current.arrName, incoming.arrName) {
		current.arrName = incoming.arrName
		current.mediaID = incoming.mediaID
		improved = true
	}
	if current.arrName == "" && incoming.arrName != "" {
		current.arrName = incoming.arrName
		improved = true
	}
	if current.mediaID <= 0 && incoming.mediaID > 0 {
		current.mediaID = incoming.mediaID
		improved = true
	}
	return current, improved
}

func legacyNZBSourceImproves(current, incoming nzbHydrationSource) bool {
	_, improved := mergeLegacyNZBSource(current, incoming)
	return improved
}

func (w *legacyNZBHydrationWorker) takeNext(ctx context.Context, now time.Time) (*legacyNZBHydrationAttempt, context.Context, func(), time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pauses > 0 {
		return nil, nil, nil, time.Time{}
	}

	var selected *legacyNZBHydrationJob
	var selectedAt time.Time
	for _, job := range w.jobs {
		if job.inFlight || job.state == legacyNZBHydrationJobUnavailable {
			continue
		}
		eligibleAt := job.retryAt
		if backoff := w.arrBackoffs[legacyNZBArrKey(job.source.arrName)]; backoff.retryAt.After(eligibleAt) {
			eligibleAt = backoff.retryAt
		}
		if selected == nil || legacyNZBJobBefore(job, eligibleAt, selected, selectedAt) {
			selected = job
			selectedAt = eligibleAt
		}
	}
	if selected == nil {
		return nil, nil, nil, time.Time{}
	}
	if selectedAt.After(now) {
		return nil, nil, nil, selectedAt
	}

	selected.inFlight = true
	selected.priority = false
	baseCtx, cancelCause := context.WithCancelCause(ctx)
	attemptCtx, timeoutCancel := context.WithTimeoutCause(baseCtx, w.itemTimeout, context.DeadlineExceeded)
	w.inFlight[selected.nzbID] = &legacyNZBHydrationInFlight{
		cancel: cancelCause,
		done:   make(chan struct{}),
	}
	attempt := &legacyNZBHydrationAttempt{
		nzbID:   selected.nzbID,
		source:  selected.source,
		version: selected.version,
	}
	cleanup := func() {
		timeoutCancel()
		cancelCause(nil)
	}
	return attempt, attemptCtx, cleanup, time.Time{}
}

func legacyNZBJobBefore(candidate *legacyNZBHydrationJob, candidateAt time.Time, current *legacyNZBHydrationJob, currentAt time.Time) bool {
	if candidateAt.Equal(currentAt) {
		if candidate.priority != current.priority {
			return candidate.priority
		}
		return candidate.nzbID < current.nzbID
	}
	return candidateAt.Before(currentAt)
}

func (w *legacyNZBHydrationWorker) finishAttempt(attempt legacyNZBHydrationAttempt, err, cause error, now time.Time) {
	var record *storage.LegacyNZBHydration
	deleteRecord := false
	var retry time.Duration
	var permanent bool
	var readyEntries []string

	w.mu.Lock()
	if tracked := w.inFlight[attempt.nzbID]; tracked != nil {
		delete(w.inFlight, attempt.nzbID)
		close(tracked.done)
	}
	job := w.jobs[attempt.nzbID]
	if job == nil {
		w.mu.Unlock()
		return
	}
	job.inFlight = false
	if err != nil && errors.Is(cause, errLegacyNZBHydrationPaused) {
		job.priority = true
		w.signalLocked()
		w.mu.Unlock()
		return
	}
	if errors.Is(cause, context.Canceled) && err != nil {
		w.signalLocked()
		w.mu.Unlock()
		return
	}

	switch {
	case err == nil:
		readyEntries = make([]string, 0, len(job.entryNames))
		for entryName := range job.entryNames {
			readyEntries = append(readyEntries, entryName)
		}
		delete(w.jobs, attempt.nzbID)
		if job.arrBackoff {
			delete(w.arrBackoffs, legacyNZBArrKey(attempt.source.arrName))
		}
		w.hydrated++
		deleteRecord = true
	case job.version != attempt.version:
		job.state = legacyNZBHydrationJobPending
		job.retryAt = time.Time{}
		job.priority = true
		job.arrBackoff = false
		job.backoffFailures = 0
		deleteRecord = true
	case legacyNZBHydrationFailurePermanent(err):
		permanent = true
		job.state = legacyNZBHydrationJobUnavailable
		job.attempts++
		job.lastAttemptAt = now
		job.retryAt = time.Time{}
		job.lastError = legacyNZBErrorString(err)
		job.arrBackoff = false
		job.backoffFailures = 0
		record = legacyNZBHydrationRecord(job, storage.LegacyNZBHydrationUnavailable)
	default:
		job.state = legacyNZBHydrationJobRetrying
		job.attempts++
		job.lastAttemptAt = now
		job.lastError = legacyNZBErrorString(err)
		job.arrBackoff = legacyNZBHydrationFailureUsesArrBackoff(err)
		if job.arrBackoff {
			key := legacyNZBArrKey(job.source.arrName)
			backoff := w.arrBackoffs[key]
			backoff.failures++
			retry = legacyNZBBackoff(w.baseBackoff, w.maxBackoff, backoff.failures)
			backoff.retryAt = now.Add(retry)
			w.arrBackoffs[key] = backoff
			job.backoffFailures = backoff.failures
			job.retryAt = backoff.retryAt
		} else {
			job.backoffFailures = 0
			retry = legacyNZBBackoff(w.baseBackoff, w.maxBackoff, job.attempts)
			job.retryAt = now.Add(retry)
		}
		record = legacyNZBHydrationRecord(job, storage.LegacyNZBHydrationRetrying)
	}
	w.signalLocked()
	w.mu.Unlock()

	if deleteRecord {
		w.deleteRecord(attempt.nzbID)
	}
	if w.deps.markReady != nil {
		for _, entryName := range readyEntries {
			w.deps.markReady(entryName)
		}
	}
	if record != nil {
		w.saveRecord(record)
	}
	if permanent {
		w.deps.logger.Debug().Err(err).
			Str("nzb_id", attempt.nzbID).
			Str("arr", attempt.source.arrName).
			Msg("Legacy NZB hydration is unavailable")
	} else if err != nil && retry > 0 {
		w.deps.logger.Warn().Err(err).
			Str("nzb_id", attempt.nzbID).
			Str("arr", attempt.source.arrName).
			Dur("retry_in", retry).
			Msg("Legacy NZB hydration will retry after transient failure")
	}
}

func legacyNZBHydrationRecord(job *legacyNZBHydrationJob, state storage.LegacyNZBHydrationState) *storage.LegacyNZBHydration {
	return &storage.LegacyNZBHydration{
		NZBID:           job.nzbID,
		ArrName:         job.source.arrName,
		MediaID:         job.source.mediaID,
		State:           state,
		Attempts:        job.attempts,
		ArrBackoff:      job.arrBackoff,
		BackoffFailures: job.backoffFailures,
		RetryAt:         job.retryAt,
		LastAttemptAt:   job.lastAttemptAt,
		LastError:       job.lastError,
	}
}

func legacyNZBBackoff(base, maximum time.Duration, failures int) time.Duration {
	if base <= 0 || failures <= 0 {
		return 0
	}
	if maximum <= 0 {
		maximum = base
	}
	delay := base
	for range failures - 1 {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

func legacyNZBArrKey(arrName string) string {
	return strings.ToLower(strings.TrimSpace(arrName))
}

func legacyNZBErrorString(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > legacyNZBHydrationErrorLimit {
		message = message[:legacyNZBHydrationErrorLimit]
	}
	return message
}

func (w *legacyNZBHydrationWorker) restoreArrBackoffLocked(job *legacyNZBHydrationJob) {
	key := legacyNZBArrKey(job.source.arrName)
	current := w.arrBackoffs[key]
	if job.retryAt.After(current.retryAt) {
		current.retryAt = job.retryAt
	}
	failures := job.backoffFailures
	if failures <= 0 {
		failures = job.attempts
	}
	current.failures = max(current.failures, failures)
	w.arrBackoffs[key] = current
}

func (w *legacyNZBHydrationWorker) waitForWork(ctx context.Context, until time.Time) bool {
	if until.IsZero() {
		select {
		case <-ctx.Done():
			return false
		case <-w.wake:
			return true
		}
	}
	delay := time.Until(until)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-w.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (w *legacyNZBHydrationWorker) pause() func() {
	if w == nil {
		return func() {}
	}
	w.mu.Lock()
	w.pauses++
	inFlight := w.inFlightLocked()
	for _, attempt := range inFlight {
		attempt.cancel(errLegacyNZBHydrationPaused)
	}
	w.signalLocked()
	w.mu.Unlock()
	if len(inFlight) > 0 {
		timer := time.NewTimer(legacyNZBHydrationStopTimeout)
		for _, attempt := range inFlight {
			select {
			case <-attempt.done:
			case <-timer.C:
				w.deps.logger.Warn().Msg("Legacy NZB hydration did not pause before repair timeout")
				timer.Stop()
				return w.resumeFunc()
			}
		}
		timer.Stop()
	}
	return w.resumeFunc()
}

// inFlightLocked snapshots the live attempts so callers can cancel or wait on
// them without holding the worker lock.
func (w *legacyNZBHydrationWorker) inFlightLocked() []*legacyNZBHydrationInFlight {
	if len(w.inFlight) == 0 {
		return nil
	}
	attempts := make([]*legacyNZBHydrationInFlight, 0, len(w.inFlight))
	for _, attempt := range w.inFlight {
		attempts = append(attempts, attempt)
	}
	return attempts
}

func (w *legacyNZBHydrationWorker) resumeFunc() func() {
	return sync.OnceFunc(func() {
		w.mu.Lock()
		if w.pauses > 0 {
			w.pauses--
		}
		w.signalLocked()
		w.mu.Unlock()
	})
}

func (r *Service) pauseLegacyNZBHydration() func() {
	if r == nil {
		return func() {}
	}
	return r.legacyNZBHydrator.pause()
}

func (r *Service) enqueueLegacyNZBHydration(nzbID string, source nzbHydrationSource) legacyNZBHydrationDisposition {
	if r == nil || r.legacyNZBHydrator == nil {
		return legacyNZBHydrationUnavailable
	}
	return r.legacyNZBHydrator.enqueue(nzbID, source, true)
}

func (w *legacyNZBHydrationWorker) remove(nzbID string) {
	w.mu.Lock()
	_, tracked := w.jobs[nzbID]
	delete(w.jobs, nzbID)
	w.signalLocked()
	w.mu.Unlock()
	if tracked {
		w.deleteRecord(nzbID)
	}
}

func (w *legacyNZBHydrationWorker) saveRecord(record *storage.LegacyNZBHydration) {
	if err := w.deps.store.SaveLegacyNZBHydration(record); err != nil {
		w.deps.logger.Warn().Err(err).Str("nzb_id", record.NZBID).Msg("Legacy NZB hydration state could not be saved")
	}
}

func (w *legacyNZBHydrationWorker) deleteRecord(nzbID string) {
	if err := w.deps.store.DeleteLegacyNZBHydration(nzbID); err != nil {
		w.deps.logger.Warn().Err(err).Str("nzb_id", nzbID).Msg("Legacy NZB hydration state could not be cleared")
	}
}

func (w *legacyNZBHydrationWorker) signalLocked() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *legacyNZBHydrationWorker) status() LegacyNZBHydrationStatus {
	if w == nil {
		return LegacyNZBHydrationStatus{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	status := LegacyNZBHydrationStatus{
		Running:      w.running,
		Paused:       w.pauses > 0,
		ScanComplete: w.scanComplete,
		Hydrated:     w.hydrated,
		InFlight:     len(w.inFlight),
	}
	for nzbID := range w.inFlight {
		if status.CurrentNZBID == "" || nzbID < status.CurrentNZBID {
			status.CurrentNZBID = nzbID
		}
	}
	var next time.Time
	for _, job := range w.jobs {
		switch job.state {
		case legacyNZBHydrationJobUnavailable:
			status.Unavailable++
			continue
		case legacyNZBHydrationJobRetrying:
			status.Retrying++
		default:
			status.Pending++
		}
		eligibleAt := job.retryAt
		if backoff := w.arrBackoffs[legacyNZBArrKey(job.source.arrName)]; backoff.retryAt.After(eligibleAt) {
			eligibleAt = backoff.retryAt
		}
		if next.IsZero() || eligibleAt.Before(next) {
			next = eligibleAt
		}
	}
	if !next.IsZero() {
		status.NextAttemptAt = new(next)
	}
	return status
}
