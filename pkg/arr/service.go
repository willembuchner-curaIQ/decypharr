package arr

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

const (
	bindingsDatabaseName = "arr_bindings.db"
	jobsDatabaseName     = "reacquire_jobs.db"
)

type JobProgress interface {
	Update(status ReacquireStatus, mutate func(*ReacquireJob)) error
	UpdateDurable(status ReacquireStatus, mutate func(*ReacquireJob)) error
}

type ReacquireHandler interface {
	Reacquire(context.Context, ReacquireJob, JobProgress) error
}

type ServiceOptions struct {
	Directory string
	Index     *Index
	Handler   ReacquireHandler
}

type reacquireKey struct {
	arrName    string
	downloadID string
	entryID    string
	fileID     string
}

type Service struct {
	index                *Index
	bindingRepository    *BindingRepository
	jobRepository        *ReacquireJobRepository
	wake                 chan struct{}
	now                  func() time.Time
	lifecycleMu          sync.RWMutex
	started              bool
	closed               bool
	ctx                  context.Context
	cancel               context.CancelFunc
	handler              ReacquireHandler
	wg                   sync.WaitGroup
	jobsMu               sync.RWMutex
	jobs                 map[string]ReacquireJob
	activeReacquisitions map[reacquireKey]string
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Directory == "" {
		return nil, errors.New("arr service database directory is required")
	}
	bindingRepository, err := OpenBindingRepository(filepath.Join(options.Directory, bindingsDatabaseName))
	if err != nil {
		return nil, err
	}
	jobRepository, err := OpenReacquireJobRepository(filepath.Join(options.Directory, jobsDatabaseName))
	if err != nil {
		_ = bindingRepository.Close()
		return nil, err
	}
	index := options.Index
	if index == nil {
		index = NewIndex()
	}
	return &Service{
		index:                index,
		bindingRepository:    bindingRepository,
		jobRepository:        jobRepository,
		wake:                 make(chan struct{}, 1),
		now:                  func() time.Time { return time.Now().UTC() },
		handler:              options.Handler,
		jobs:                 make(map[string]ReacquireJob),
		activeReacquisitions: make(map[reacquireKey]string),
	}, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	if s.started {
		return nil
	}

	bindings, err := s.bindingRepository.LoadAll()
	if err != nil {
		return fmt.Errorf("load arr bindings: %w", err)
	}
	bindings = newestBindingRows(bindings)
	if err := s.index.replaceAll(bindings); err != nil {
		return err
	}
	jobs, err := s.jobRepository.LoadAll()
	if err != nil {
		return fmt.Errorf("load reacquire jobs: %w", err)
	}
	if err := s.loadJobs(jobs); err != nil {
		return err
	}
	for _, job := range s.Jobs() {
		if job.Status.waiting() {
			if err := s.completeJobFromIndex(job); err != nil {
				return fmt.Errorf("reconcile persisted reacquire job %q: %w", job.ID, err)
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.started = true
	s.wg.Go(func() { s.run(s.ctx) })
	s.schedulePersistedRetries()
	s.signal()
	return nil
}

func (s *Service) Close() error {
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	s.lifecycleMu.Unlock()

	s.wg.Wait()
	return errors.Join(s.bindingRepository.Close(), s.jobRepository.Close())
}

func (s *Service) SetHandler(handler ReacquireHandler) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	s.handler = handler
	s.signal()
	return nil
}

func (s *Service) Index() *Index {
	return s.index
}

func (s *Service) Lookup(entryID, fileID string) (Binding, bool) {
	return s.index.Lookup(entryID, fileID)
}

func (s *Service) Bindings() []Binding {
	return s.index.All()
}

func (s *Service) BindingsByArr(arrName string) []Binding {
	return s.index.ByArr(arrName)
}

func (s *Service) BindingsByDownload(arrName, downloadID string) []Binding {
	return s.index.ByDownloadID(arrName, downloadID)
}

func (s *Service) UpsertBinding(binding Binding) error {
	release, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	binding.UpdatedAt = s.now()
	if err := binding.validate(); err != nil {
		return fmt.Errorf("upsert arr binding: %w", err)
	}
	previous, collision := s.index.ByArrFile(binding.ArrName, binding.ArrFileID)
	if previous.EntryID == binding.EntryID && previous.EntryFileID == binding.EntryFileID {
		collision = false
	}
	if err := s.bindingRepository.Save(binding); err != nil {
		return err
	}
	if collision {
		if err := s.bindingRepository.Delete(previous.EntryID, previous.EntryFileID); err != nil {
			return err
		}
	}
	if err := s.index.Upsert(binding); err != nil {
		return err
	}
	return s.completeWaitingJobs(binding)
}

func newestBindingRows(bindings []Binding) []Binding {
	byArrFile := make(map[arrFileKey]Binding, len(bindings))
	withoutArrFile := make([]Binding, 0, len(bindings))
	for _, binding := range bindings {
		if binding.ArrFileID <= 0 {
			withoutArrFile = append(withoutArrFile, binding)
			continue
		}
		key := arrFileKey{arrName: binding.ArrName, fileID: binding.ArrFileID}
		current, exists := byArrFile[key]
		if !exists || binding.UpdatedAt.After(current.UpdatedAt) ||
			(binding.UpdatedAt.Equal(current.UpdatedAt) && binding.Generation > current.Generation) {
			byArrFile[key] = binding
		}
	}
	result := make([]Binding, 0, len(withoutArrFile)+len(byArrFile))
	result = append(result, withoutArrFile...)
	for _, binding := range byArrFile {
		result = append(result, binding)
	}
	sortBindings(result)
	return result
}

func (s *Service) ObserveBinding(binding Binding) error {
	release, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	if err := binding.validate(); err != nil {
		return fmt.Errorf("observe arr binding: %w", err)
	}
	return s.completeWaitingJobs(binding)
}

func (s *Service) ReplaceArrGeneration(arrName string, generation uint64, bindings []Binding) error {
	release, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	now := s.now()
	prepared := make([]Binding, len(bindings))
	for i, binding := range bindings {
		binding.ArrName = arrName
		binding.Generation = generation
		binding.UpdatedAt = now
		prepared[i] = binding
	}
	if err := validateUniqueArrFiles(prepared); err != nil {
		return err
	}
	if err := s.bindingRepository.ReplaceArrGeneration(arrName, generation, prepared); err != nil {
		return err
	}
	if err := s.index.ReplaceArrGeneration(arrName, generation, prepared); err != nil {
		return err
	}
	return s.completeWaitingJobs(prepared...)
}

func (s *Service) DeleteBinding(entryID, fileID string) error {
	release, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	if err := s.bindingRepository.Delete(entryID, fileID); err != nil {
		return err
	}
	s.index.DeleteEntryFile(entryID, fileID)
	return nil
}

func (s *Service) beginOperation() (func(), error) {
	s.lifecycleMu.RLock()
	if s.closed {
		s.lifecycleMu.RUnlock()
		return nil, ErrServiceClosed
	}
	if !s.started {
		s.lifecycleMu.RUnlock()
		return nil, ErrServiceNotStarted
	}
	if s.ctx != nil && s.ctx.Err() != nil {
		s.lifecycleMu.RUnlock()
		return nil, ErrServiceClosed
	}
	return s.lifecycleMu.RUnlock, nil
}

func (s *Service) currentHandler() ReacquireHandler {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	return s.handler
}

func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
