package arr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	reacquireMaintenanceInterval = time.Minute
	reacquireExecutionTimeout    = 2 * time.Minute
	reacquireWaitingTimeout      = 6 * time.Hour
	reacquireSuccessRetention    = 24 * time.Hour
	reacquireFailureRetention    = 7 * 24 * time.Hour
	reacquireRetryBaseDelay      = time.Second
	reacquireRetryMaxDelay       = 30 * time.Second
)

func (s *Service) Reacquire(request ReacquireRequest) (*ReacquireJob, error) {
	release, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer release()
	request.Strategy = request.Strategy.normalized()
	if err := request.validate(); err != nil {
		return nil, err
	}
	binding, ok := s.index.Lookup(request.EntryID, request.FileID)
	if !ok {
		return nil, fmt.Errorf("%w: entry %q file %q", ErrBindingNotFound, request.EntryID, request.FileID)
	}
	if !binding.AuthorizesMutation() {
		return nil, fmt.Errorf("%w: entry %q file %q", ErrBindingUnsafe, request.EntryID, request.FileID)
	}
	key := keyForBinding(binding)

	s.jobsMu.Lock()
	if id, exists := s.activeReacquisitions[key]; exists {
		job := cloneJob(s.jobs[id])
		s.jobsMu.Unlock()
		return &job, nil
	}

	bindings := s.index.ByDownloadID(binding.ArrName, binding.DownloadID)
	bindings = slices.DeleteFunc(bindings, func(binding Binding) bool {
		return !binding.AuthorizesMutation()
	})
	if len(bindings) == 0 {
		bindings = []Binding{binding}
	}
	now := s.now()
	job := ReacquireJob{
		ID:         uuid.NewString(),
		Status:     ReacquireStatusQueued,
		Cause:      request.Cause,
		Strategy:   request.Strategy,
		ArrName:    binding.ArrName,
		ArrType:    binding.ArrType,
		EntryID:    request.EntryID,
		FileID:     request.FileID,
		DownloadID: binding.DownloadID,
		Bindings:   bindings,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.jobRepository.Save(job); err != nil {
		s.jobsMu.Unlock()
		return nil, err
	}
	s.jobs[job.ID] = cloneJob(job)
	s.activeReacquisitions[key] = job.ID
	s.jobsMu.Unlock()

	s.signal()
	result := cloneJob(job)
	return &result, nil
}

func (s *Service) Jobs() []ReacquireJob {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	jobs := make([]ReacquireJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	slices.SortFunc(jobs, func(left, right ReacquireJob) int {
		if result := right.UpdatedAt.Compare(left.UpdatedAt); result != 0 {
			return result
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return jobs
}

func (s *Service) Job(id string) (ReacquireJob, bool) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()
	job, ok := s.jobs[id]
	return cloneJob(job), ok
}

func (s *Service) loadJobs(jobs []ReacquireJob) error {
	slices.SortFunc(jobs, func(left, right ReacquireJob) int {
		return left.UpdatedAt.Compare(right.UpdatedAt)
	})
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	clear(s.jobs)
	clear(s.activeReacquisitions)
	for _, job := range jobs {
		job.Strategy = job.Strategy.normalized()
		s.jobs[job.ID] = cloneJob(job)
		if job.Status.Terminal() {
			continue
		}
		key := keyForJob(job)
		if previousID, exists := s.activeReacquisitions[key]; exists {
			previous := s.jobs[previousID]
			previous.Status = ReacquireStatusCancelled
			previous.LastError = "superseded by duplicate active job"
			previous.UpdatedAt = s.now()
			previous.CompletedAt = previous.UpdatedAt
			if err := s.jobRepository.Save(previous); err != nil {
				return err
			}
			s.jobs[previousID] = previous
		}
		s.activeReacquisitions[key] = job.ID
	}
	return nil
}

func (s *Service) run(ctx context.Context) {
	ticker := time.NewTicker(reacquireMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.maintainJobs()
			s.signal()
		case <-s.wake:
			for {
				handler := s.currentHandler()
				if handler == nil {
					break
				}
				job, ok := s.nextJob()
				if !ok {
					break
				}
				if !s.runJob(ctx, handler, job) {
					break
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func (s *Service) runJob(ctx context.Context, handler ReacquireHandler, job ReacquireJob) bool {
	started, err := s.updateJob(job.ID, ReacquireStatusResolving, func(job *ReacquireJob) {
		job.RetryAt = time.Time{}
	})
	if err != nil {
		return false
	}
	progress := &serviceJobProgress{service: s, jobID: job.ID}
	jobCtx, cancel := context.WithTimeout(ctx, reacquireExecutionTimeout)
	err = handler.Reacquire(jobCtx, started, progress)
	if err == nil {
		err = jobCtx.Err()
	}
	cancel()
	if ctx.Err() != nil {
		return true
	}
	if errors.Is(err, errMutationOutcomeUnknown) {
		current, ok := s.Job(job.ID)
		if !ok {
			return false
		}
		delay := reacquireRetryDelay(current, err)
		queued, updateErr := s.updateJobDurable(job.ID, ReacquireStatusQueued, func(job *ReacquireJob) {
			job.LastError = err.Error()
			job.RetryAt = s.now().Add(delay)
		})
		if updateErr != nil {
			return false
		}
		s.scheduleRetry(queued)
		return true
	}
	if err != nil {
		_, updateErr := s.updateJob(job.ID, ReacquireStatusFailed, func(job *ReacquireJob) {
			job.LastError = err.Error()
			job.RetryAt = time.Time{}
		})
		return updateErr == nil
	}
	current, ok := s.Job(job.ID)
	if !ok || current.Status.Terminal() || progress.waiting.Load() || current.Status.waiting() {
		return true
	}
	_, err = s.updateJob(job.ID, ReacquireStatusReady, func(job *ReacquireJob) {
		job.LastError = ""
		job.RetryAt = time.Time{}
	})
	return err == nil
}

func (s *Service) nextJob() (ReacquireJob, bool) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()
	var next ReacquireJob
	found := false
	for _, id := range s.activeReacquisitions {
		job, ok := s.jobs[id]
		if !ok || !job.Status.dispatchable() || job.RetryAt.After(s.now()) {
			continue
		}
		if !found || job.UpdatedAt.Before(next.UpdatedAt) {
			next = cloneJob(job)
			found = true
		}
	}
	return next, found
}

func (s *Service) updateJob(id string, status ReacquireStatus, mutate func(*ReacquireJob)) (ReacquireJob, error) {
	return s.updateJobWithDurability(id, status, mutate, false)
}

func (s *Service) updateJobDurable(id string, status ReacquireStatus, mutate func(*ReacquireJob)) (ReacquireJob, error) {
	return s.updateJobWithDurability(id, status, mutate, true)
}

func (s *Service) updateJobWithDurability(
	id string,
	status ReacquireStatus,
	mutate func(*ReacquireJob),
	durable bool,
) (ReacquireJob, error) {
	if !status.valid() {
		return ReacquireJob{}, fmt.Errorf("invalid reacquire status %q", status)
	}
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()

	original, ok := s.jobs[id]
	if !ok {
		return ReacquireJob{}, fmt.Errorf("reacquire job %q not found", id)
	}
	if original.Status.Terminal() && status != original.Status {
		return ReacquireJob{}, fmt.Errorf("reacquire job %q is already %s", id, original.Status)
	}
	job := cloneJob(original)
	if mutate != nil {
		mutate(&job)
	}
	job.ID = original.ID
	job.Cause = original.Cause
	job.Strategy = original.Strategy
	job.ArrName = original.ArrName
	job.ArrType = original.ArrType
	job.EntryID = original.EntryID
	job.FileID = original.FileID
	job.DownloadID = original.DownloadID
	job.Bindings = cloneJob(original).Bindings
	job.CreatedAt = original.CreatedAt
	job.Status = status
	job.UpdatedAt = s.now()
	if status != ReacquireStatusQueued && job.StartedAt.IsZero() {
		job.StartedAt = job.UpdatedAt
	}
	if status.Terminal() {
		job.CompletedAt = job.UpdatedAt
		job.RetryAt = time.Time{}
	}
	var err error
	if durable {
		err = s.jobRepository.SaveDurable(job)
	} else {
		err = s.jobRepository.Save(job)
	}
	if err != nil {
		return ReacquireJob{}, err
	}
	s.jobs[id] = cloneJob(job)
	if status.Terminal() {
		key := keyForJob(job)
		if s.activeReacquisitions[key] == id {
			delete(s.activeReacquisitions, key)
		}
	}
	return cloneJob(job), nil
}

// completeWaitingJobs re-checks the waiting jobs an indexing change can satisfy.
// It takes the whole change set at once: a generation replace carries every
// binding of an Arr, and checking one binding at a time walks the job table
// once per binding.
func (s *Service) completeWaitingJobs(bindings ...Binding) error {
	downloadsByArr := make(map[string]map[string]struct{})
	for _, binding := range bindings {
		if binding.DownloadID == "" {
			continue
		}
		downloads, ok := downloadsByArr[binding.ArrName]
		if !ok {
			downloads = make(map[string]struct{})
			downloadsByArr[binding.ArrName] = downloads
		}
		downloads[binding.DownloadID] = struct{}{}
	}
	if len(downloadsByArr) == 0 {
		return nil
	}

	s.jobsMu.RLock()
	ids := make([]string, 0)
	for _, id := range s.activeReacquisitions {
		job, ok := s.jobs[id]
		if !ok || !job.Status.waiting() {
			continue
		}
		if replacesDownload(downloadsByArr[job.ArrName], job.DownloadID) {
			ids = append(ids, id)
		}
	}
	s.jobsMu.RUnlock()
	for _, id := range ids {
		job, ok := s.Job(id)
		if !ok {
			continue
		}
		if err := s.completeJobFromIndex(job); err != nil {
			return err
		}
	}
	return nil
}

type serviceJobProgress struct {
	service *Service
	jobID   string
	waiting atomic.Bool
}

func (progress *serviceJobProgress) Update(status ReacquireStatus, mutate func(*ReacquireJob)) error {
	return progress.update(status, mutate, false)
}

func (progress *serviceJobProgress) UpdateDurable(status ReacquireStatus, mutate func(*ReacquireJob)) error {
	return progress.update(status, mutate, true)
}

func (progress *serviceJobProgress) update(status ReacquireStatus, mutate func(*ReacquireJob), durable bool) error {
	if status.waiting() {
		progress.waiting.Store(true)
	}
	var (
		job ReacquireJob
		err error
	)
	if durable {
		job, err = progress.service.updateJobDurable(progress.jobID, status, mutate)
	} else {
		job, err = progress.service.updateJob(progress.jobID, status, mutate)
	}
	if err != nil || !status.waiting() {
		return err
	}
	return progress.service.completeJobFromIndex(job)
}

func reacquireRetryDelay(job ReacquireJob, err error) time.Duration {
	attempts := 0
	for _, mutation := range job.Mutations {
		attempts = max(attempts, mutation.Attempts)
	}
	delay := reacquireRetryBaseDelay
	for range max(0, min(attempts-1, 5)) {
		delay *= 2
	}
	return min(reacquireRetryMaxDelay, max(delay, mutationRetryAfter(err)))
}

func (s *Service) scheduleRetry(job ReacquireJob) {
	if job.RetryAt.IsZero() || s.ctx == nil {
		return
	}
	delay := max(time.Duration(0), job.RetryAt.Sub(s.now()))
	s.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
		case <-timer.C:
			s.signal()
		}
	})
}

func (s *Service) schedulePersistedRetries() {
	for _, job := range s.Jobs() {
		if job.Status.dispatchable() && job.RetryAt.After(s.now()) {
			s.scheduleRetry(job)
		}
	}
}

// replacesDownload reports whether any indexed download differs from the one
// the job is replacing, which is what makes the job worth re-checking.
func replacesDownload(downloads map[string]struct{}, jobDownloadID string) bool {
	if len(downloads) == 0 {
		return false
	}
	if len(downloads) > 1 {
		return true
	}
	_, sameOnly := downloads[jobDownloadID]
	return !sameOnly
}

func (status ReacquireStatus) waiting() bool {
	return status == ReacquireStatusWaitingForGrab || status == ReacquireStatusWaitingForDownload || status == ReacquireStatusWaitingForImport
}

func (status ReacquireStatus) dispatchable() bool {
	return status == ReacquireStatusQueued || status == ReacquireStatusResolving || status == ReacquireStatusInvalidating || status == ReacquireStatusBlocklisting || status == ReacquireStatusSearching
}

func keyForBinding(binding Binding) reacquireKey {
	if binding.DownloadID != "" {
		return reacquireKey{arrName: binding.ArrName, downloadID: binding.DownloadID}
	}
	return reacquireKey{arrName: binding.ArrName, entryID: binding.EntryID, fileID: binding.EntryFileID}
}

func keyForJob(job ReacquireJob) reacquireKey {
	if job.DownloadID != "" {
		return reacquireKey{arrName: job.ArrName, downloadID: job.DownloadID}
	}
	return reacquireKey{arrName: job.ArrName, entryID: job.EntryID, fileID: job.FileID}
}

func (s *Service) completeJobFromIndex(job ReacquireJob) error {
	replacementDownloadIDs, ready := s.replacementsForJob(job)
	if !ready {
		return nil
	}
	_, err := s.updateJob(job.ID, ReacquireStatusReady, func(job *ReacquireJob) {
		job.ReplacementDownloadIDs = replacementDownloadIDs
		job.ReplacementDownloadID = replacementDownloadIDs[0]
		job.LastError = ""
	})
	return err
}

func (s *Service) replacementsForJob(job ReacquireJob) ([]string, bool) {
	downloads := make(map[string]struct{})
	for _, target := range job.Bindings {
		var candidates []Binding
		switch {
		case target.MovieID > 0:
			candidates = s.index.ByMovieID(job.ArrName, target.MovieID)
		case len(target.EpisodeIDs) > 0:
			for _, episodeID := range target.EpisodeIDs {
				episodeCandidates := s.index.ByEpisodeID(job.ArrName, episodeID)
				candidate, found := replacementCandidate(episodeCandidates, job.DownloadID, func(candidate Binding) bool {
					return candidate.SeriesID == target.SeriesID
				})
				if !found {
					return nil, false
				}
				downloads[candidate.DownloadID] = struct{}{}
			}
			continue
		case target.SeriesID > 0:
			candidates = s.index.BySeriesID(job.ArrName, target.SeriesID)
		default:
			return nil, false
		}

		candidate, found := replacementCandidate(candidates, job.DownloadID, func(candidate Binding) bool {
			if target.MovieID > 0 {
				return candidate.MovieID == target.MovieID
			}
			return candidate.SeriesID == target.SeriesID && candidate.SeasonNumber == target.SeasonNumber
		})
		if !found {
			return nil, false
		}
		downloads[candidate.DownloadID] = struct{}{}
	}
	if len(downloads) == 0 {
		return nil, false
	}
	result := make([]string, 0, len(downloads))
	for downloadID := range downloads {
		result = append(result, downloadID)
	}
	slices.Sort(result)
	return result, true
}

func replacementCandidate(candidates []Binding, oldDownloadID string, matches func(Binding) bool) (Binding, bool) {
	for _, candidate := range candidates {
		if candidate.AuthorizesMutation() && candidate.DownloadID != "" && candidate.DownloadID != oldDownloadID && matches(candidate) {
			return candidate, true
		}
	}
	return Binding{}, false
}

func (s *Service) maintainJobs() {
	now := s.now()
	var expire []string
	var prune []string

	s.jobsMu.RLock()
	for id, job := range s.jobs {
		switch {
		case job.Status.waiting() && now.Sub(job.UpdatedAt) >= reacquireWaitingTimeout:
			expire = append(expire, id)
		case job.Status == ReacquireStatusFailed && terminalAge(now, job) >= reacquireFailureRetention:
			prune = append(prune, id)
		case (job.Status == ReacquireStatusReady || job.Status == ReacquireStatusCancelled) && terminalAge(now, job) >= reacquireSuccessRetention:
			prune = append(prune, id)
		}
	}
	s.jobsMu.RUnlock()

	for _, id := range expire {
		_, _ = s.updateJob(id, ReacquireStatusFailed, func(job *ReacquireJob) {
			job.LastError = "replacement was not imported before the reacquisition timeout"
		})
	}
	for _, id := range prune {
		s.pruneTerminalJob(id, now)
	}
}

func (s *Service) pruneTerminalJob(id string, now time.Time) {
	s.jobsMu.Lock()
	defer s.jobsMu.Unlock()
	job, ok := s.jobs[id]
	if !ok || !job.Status.Terminal() {
		return
	}
	retention := reacquireSuccessRetention
	if job.Status == ReacquireStatusFailed {
		retention = reacquireFailureRetention
	}
	if terminalAge(now, job) < retention {
		return
	}
	if err := s.jobRepository.Delete(id); err == nil {
		delete(s.jobs, id)
	}
}

func terminalAge(now time.Time, job ReacquireJob) time.Duration {
	at := job.CompletedAt
	if at.IsZero() {
		at = job.UpdatedAt
	}
	return now.Sub(at)
}
