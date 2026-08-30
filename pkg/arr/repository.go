package arr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/appendstore"
)

const (
	bindingAttributeArrName  = "arrName"
	bindingSnapshotKeyPrefix = "arr-binding-snapshot:"
	bindingSnapshotVersion   = 1
	jobAttributeArrName      = "arrName"
	jobAttributeStatus       = "status"
)

type bindingRepositoryStore interface {
	ForEach(func(string, []byte) error) error
	Put(string, []byte, *appendstore.PutOptions) error
	Sync() error
	Close() error
}

type BindingRepository struct {
	mu     sync.RWMutex
	store  bindingRepositoryStore
	state  bindingRepositoryState
	loaded bool
}

type bindingSnapshot struct {
	Version    int       `json:"version"`
	ArrName    string    `json:"arrName"`
	Generation uint64    `json:"generation"`
	Bindings   []Binding `json:"bindings"`
}

type bindingRepositoryState struct {
	snapshots map[string]bindingSnapshot
	owners    map[string]string
}

func OpenBindingRepository(path string) (*BindingRepository, error) {
	store, err := openArrStore(path, []string{bindingAttributeArrName})
	if err != nil {
		return nil, fmt.Errorf("open arr binding repository: %w", err)
	}
	return &BindingRepository{store: store}, nil
}

func (r *BindingRepository) LoadAll() ([]Binding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := r.stateLocked()
	if err != nil {
		return nil, err
	}
	bindings := make([]Binding, 0, len(state.owners))
	for _, snapshot := range state.snapshots {
		bindings = append(bindings, cloneBindings(snapshot.Bindings)...)
	}
	sortBindings(bindings)
	return bindings, nil
}

func (r *BindingRepository) Save(binding Binding) error {
	if err := binding.validate(); err != nil {
		return fmt.Errorf("save arr binding: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := r.stateLocked()
	if err != nil {
		return err
	}
	key := bindingStoreKey(binding.EntryID, binding.EntryFileID)
	if owner, exists := state.owners[key]; exists && owner != binding.ArrName {
		return fmt.Errorf("save arr binding: managed file already belongs to arr %q", owner)
	}

	snapshot, exists := state.snapshots[binding.ArrName]
	if !exists {
		snapshot = bindingSnapshot{
			Version:  bindingSnapshotVersion,
			ArrName:  binding.ArrName,
			Bindings: make([]Binding, 0, 1),
		}
	} else {
		snapshot = cloneBindingSnapshot(snapshot)
	}
	index := slices.IndexFunc(snapshot.Bindings, func(current Binding) bool {
		return current.EntryID == binding.EntryID && current.EntryFileID == binding.EntryFileID
	})
	if index >= 0 {
		snapshot.Bindings[index] = cloneBinding(binding)
	} else {
		snapshot.Bindings = append(snapshot.Bindings, cloneBinding(binding))
	}
	snapshot.Generation = max(snapshot.Generation, binding.Generation)
	return r.persistSnapshotLocked(snapshot)
}

func (r *BindingRepository) Delete(entryID, fileID string) error {
	if entryID == "" || fileID == "" {
		return errors.New("entry ID and file ID are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := r.stateLocked()
	if err != nil {
		return err
	}
	owner, exists := state.owners[bindingStoreKey(entryID, fileID)]
	if !exists {
		return nil
	}
	snapshot := cloneBindingSnapshot(state.snapshots[owner])
	index := slices.IndexFunc(snapshot.Bindings, func(binding Binding) bool {
		return binding.EntryID == entryID && binding.EntryFileID == fileID
	})
	if index < 0 {
		return fmt.Errorf("delete arr binding: managed file owner %q is inconsistent", owner)
	}
	snapshot.Bindings = slices.Delete(snapshot.Bindings, index, index+1)
	return r.persistSnapshotLocked(snapshot)
}

func (r *BindingRepository) ReplaceArrGeneration(arrName string, generation uint64, bindings []Binding) error {
	if arrName == "" {
		return errors.New("arr name is required")
	}
	prepared := make([]Binding, len(bindings))
	for i, binding := range bindings {
		if binding.ArrName != "" && binding.ArrName != arrName {
			return fmt.Errorf("binding %q belongs to arr %q", binding.EntryFileID, binding.ArrName)
		}
		binding.ArrName = arrName
		binding.Generation = generation
		if err := binding.validate(); err != nil {
			return fmt.Errorf("replace arr bindings: %w", err)
		}
		prepared[i] = cloneBinding(binding)
	}
	if err := validateUniqueArrFiles(prepared); err != nil {
		return err
	}
	snapshot := bindingSnapshot{
		Version:    bindingSnapshotVersion,
		ArrName:    arrName,
		Generation: generation,
		Bindings:   prepared,
	}
	if err := validateBindingSnapshot(bindingSnapshotStoreKey(arrName), snapshot); err != nil {
		return fmt.Errorf("replace arr bindings: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := r.stateLocked()
	if err != nil {
		return err
	}
	for _, binding := range prepared {
		if owner, exists := state.owners[bindingStoreKey(binding.EntryID, binding.EntryFileID)]; exists && owner != arrName {
			return fmt.Errorf("replace arr bindings: managed file already belongs to arr %q", owner)
		}
	}
	return r.persistSnapshotLocked(snapshot)
}

func (r *BindingRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.Close()
}

func (r *BindingRepository) loadStateLocked() (bindingRepositoryState, error) {
	authoritative := make(map[string]bindingSnapshot)
	legacy := make(map[string][]Binding)
	if err := r.store.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, bindingSnapshotKeyPrefix) {
			var snapshot bindingSnapshot
			if err := json.Unmarshal(value, &snapshot); err != nil {
				return fmt.Errorf("decode arr binding snapshot %q: %w", key, err)
			}
			if err := validateBindingSnapshot(key, snapshot); err != nil {
				return fmt.Errorf("decode arr binding snapshot %q: %w", key, err)
			}
			authoritative[snapshot.ArrName] = cloneBindingSnapshot(snapshot)
			return nil
		}

		var binding Binding
		if err := json.Unmarshal(value, &binding); err != nil {
			return fmt.Errorf("decode legacy arr binding %q: %w", key, err)
		}
		if err := binding.validate(); err != nil {
			return fmt.Errorf("decode legacy arr binding %q: %w", key, err)
		}
		legacy[binding.ArrName] = append(legacy[binding.ArrName], cloneBinding(binding))
		return nil
	}); err != nil {
		return bindingRepositoryState{}, err
	}

	state := bindingRepositoryState{
		snapshots: make(map[string]bindingSnapshot, len(authoritative)+len(legacy)),
		owners:    make(map[string]string),
	}
	for arrName, snapshot := range authoritative {
		state.snapshots[arrName] = snapshot
	}
	for arrName, bindings := range legacy {
		if _, exists := authoritative[arrName]; exists {
			continue
		}
		generation := uint64(0)
		for _, binding := range bindings {
			generation = max(generation, binding.Generation)
		}
		sortBindings(bindings)
		state.snapshots[arrName] = bindingSnapshot{
			Version:    bindingSnapshotVersion,
			ArrName:    arrName,
			Generation: generation,
			Bindings:   bindings,
		}
	}
	for arrName, snapshot := range state.snapshots {
		for _, binding := range snapshot.Bindings {
			key := bindingStoreKey(binding.EntryID, binding.EntryFileID)
			if owner, exists := state.owners[key]; exists {
				return bindingRepositoryState{}, fmt.Errorf("managed file belongs to both arr %q and %q", owner, arrName)
			}
			state.owners[key] = arrName
		}
	}
	return state, nil
}

func (r *BindingRepository) stateLocked() (*bindingRepositoryState, error) {
	if r.loaded {
		return &r.state, nil
	}
	state, err := r.loadStateLocked()
	if err != nil {
		return nil, err
	}
	r.state = state
	r.loaded = true
	return &r.state, nil
}

func (r *BindingRepository) persistSnapshotLocked(snapshot bindingSnapshot) error {
	snapshot = cloneBindingSnapshot(snapshot)
	sortBindings(snapshot.Bindings)
	key := bindingSnapshotStoreKey(snapshot.ArrName)
	if err := validateBindingSnapshot(key, snapshot); err != nil {
		return fmt.Errorf("persist arr binding snapshot: %w", err)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode arr binding snapshot: %w", err)
	}
	options := &appendstore.PutOptions{Attributes: map[string]string{
		bindingAttributeArrName: snapshot.ArrName,
		"generation":            strconv.FormatUint(snapshot.Generation, 10),
	}}
	if err := r.store.Put(key, data, options); err != nil {
		return fmt.Errorf("persist arr binding snapshot: %w", err)
	}
	if err := r.store.Sync(); err != nil {
		r.state = bindingRepositoryState{}
		r.loaded = false
		return fmt.Errorf("sync arr binding snapshot: %w", err)
	}
	r.installSnapshotLocked(snapshot)
	return nil
}

func (r *BindingRepository) installSnapshotLocked(snapshot bindingSnapshot) {
	if !r.loaded {
		return
	}
	if previous, exists := r.state.snapshots[snapshot.ArrName]; exists {
		for _, binding := range previous.Bindings {
			delete(r.state.owners, bindingStoreKey(binding.EntryID, binding.EntryFileID))
		}
	}
	snapshot = cloneBindingSnapshot(snapshot)
	r.state.snapshots[snapshot.ArrName] = snapshot
	for _, binding := range snapshot.Bindings {
		r.state.owners[bindingStoreKey(binding.EntryID, binding.EntryFileID)] = snapshot.ArrName
	}
}

func validateBindingSnapshot(key string, snapshot bindingSnapshot) error {
	switch {
	case snapshot.Version != bindingSnapshotVersion:
		return fmt.Errorf("unsupported version %d", snapshot.Version)
	case snapshot.ArrName == "":
		return errors.New("arr name is required")
	case key != bindingSnapshotStoreKey(snapshot.ArrName):
		return errors.New("snapshot key does not match arr name")
	}
	seen := make(map[string]struct{}, len(snapshot.Bindings))
	for _, binding := range snapshot.Bindings {
		if err := binding.validate(); err != nil {
			return err
		}
		if binding.ArrName != snapshot.ArrName {
			return fmt.Errorf("binding belongs to arr %q", binding.ArrName)
		}
		if binding.Generation > snapshot.Generation {
			return fmt.Errorf("binding generation %d exceeds snapshot generation %d", binding.Generation, snapshot.Generation)
		}
		bindingKey := bindingStoreKey(binding.EntryID, binding.EntryFileID)
		if _, exists := seen[bindingKey]; exists {
			return fmt.Errorf("duplicate managed file %q/%q", binding.EntryID, binding.EntryFileID)
		}
		seen[bindingKey] = struct{}{}
	}
	return nil
}

func cloneBindingSnapshot(snapshot bindingSnapshot) bindingSnapshot {
	snapshot.Bindings = cloneBindings(snapshot.Bindings)
	return snapshot
}

func cloneBindings(bindings []Binding) []Binding {
	cloned := make([]Binding, len(bindings))
	for i, binding := range bindings {
		cloned[i] = cloneBinding(binding)
	}
	return cloned
}

type ReacquireJobRepository struct {
	mu    sync.RWMutex
	store *appendstore.Store
}

func OpenReacquireJobRepository(path string) (*ReacquireJobRepository, error) {
	store, err := openArrStore(path, []string{jobAttributeArrName, jobAttributeStatus})
	if err != nil {
		return nil, fmt.Errorf("open reacquire job repository: %w", err)
	}
	return &ReacquireJobRepository{store: store}, nil
}

func (r *ReacquireJobRepository) LoadAll() ([]ReacquireJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]ReacquireJob, 0, r.store.Len())
	err := r.store.ForEach(func(_ string, value []byte) error {
		var job ReacquireJob
		if err := json.Unmarshal(value, &job); err != nil {
			return fmt.Errorf("decode reacquire job: %w", err)
		}
		if err := validateJob(job); err != nil {
			return fmt.Errorf("decode reacquire job: %w", err)
		}
		jobs = append(jobs, cloneJob(job))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (r *ReacquireJobRepository) Get(id string) (ReacquireJob, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, err := r.store.Get(id)
	if errors.Is(err, appendstore.ErrKeyNotFound) {
		return ReacquireJob{}, false, nil
	}
	if err != nil {
		return ReacquireJob{}, false, fmt.Errorf("load reacquire job: %w", err)
	}
	var job ReacquireJob
	if err := json.Unmarshal(data, &job); err != nil {
		return ReacquireJob{}, false, fmt.Errorf("decode reacquire job: %w", err)
	}
	if err := validateJob(job); err != nil {
		return ReacquireJob{}, false, fmt.Errorf("decode reacquire job: %w", err)
	}
	return cloneJob(job), true, nil
}

func (r *ReacquireJobRepository) Save(job ReacquireJob) error {
	return r.save(job, false)
}

func (r *ReacquireJobRepository) SaveDurable(job ReacquireJob) error {
	return r.save(job, true)
}

func (r *ReacquireJobRepository) save(job ReacquireJob, durable bool) error {
	if err := validateJob(job); err != nil {
		return fmt.Errorf("save reacquire job: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("encode reacquire job: %w", err)
	}
	options := &appendstore.PutOptions{Attributes: map[string]string{
		jobAttributeArrName: job.ArrName,
		jobAttributeStatus:  string(job.Status),
	}}
	if err := r.store.Put(job.ID, data, options); err != nil {
		return fmt.Errorf("persist reacquire job: %w", err)
	}
	if durable {
		if err := r.store.Sync(); err != nil {
			return fmt.Errorf("sync reacquire job: %w", err)
		}
	}
	return nil
}

func (r *ReacquireJobRepository) Delete(id string) error {
	if id == "" {
		return errors.New("job ID is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return deleteStoreKey(r.store, id)
}

func (r *ReacquireJobRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.Close()
}

func openArrStore(path string, indexedFields []string) (*appendstore.Store, error) {
	if path == "" {
		return nil, errors.New("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	return appendstore.Open(path, appendstore.Options{
		CacheSize:           1000,
		SyncInterval:        time.Second,
		CompactionThreshold: 0.5,
		AutoCompact:         true,
		IndexedFields:       indexedFields,
	})
}

func bindingStoreKey(entryID, fileID string) string {
	hash := sha256.New()
	hash.Write([]byte(entryID))
	hash.Write([]byte{0})
	hash.Write([]byte(fileID))
	return hex.EncodeToString(hash.Sum(nil))
}

func bindingSnapshotStoreKey(arrName string) string {
	hash := sha256.Sum256([]byte(arrName))
	return bindingSnapshotKeyPrefix + hex.EncodeToString(hash[:])
}

func deleteStoreKey(store *appendstore.Store, key string) error {
	if err := store.Delete(key); err != nil && !errors.Is(err, appendstore.ErrKeyNotFound) {
		return err
	}
	return nil
}

func validateJob(job ReacquireJob) error {
	switch {
	case job.ID == "":
		return errors.New("job ID is required")
	case !job.Status.valid():
		return fmt.Errorf("invalid job status %q", job.Status)
	case !job.Cause.valid():
		return fmt.Errorf("invalid job cause %q", job.Cause)
	case !job.Strategy.valid():
		return fmt.Errorf("invalid job strategy %q", job.Strategy)
	case job.ArrName == "":
		return errors.New("arr name is required")
	case job.EntryID == "" || job.FileID == "":
		return errors.New("entry ID and file ID are required")
	case job.CreatedAt.IsZero() || job.UpdatedAt.IsZero():
		return errors.New("job timestamps are required")
	}
	if len(job.Bindings) == 0 {
		return errors.New("job bindings are required")
	}
	for _, binding := range job.Bindings {
		if err := binding.validate(); err != nil {
			return fmt.Errorf("invalid job binding: %w", err)
		}
	}
	mutationKeys := make(map[string]struct{}, len(job.Mutations))
	for _, mutation := range job.Mutations {
		if err := mutation.validate(); err != nil {
			return fmt.Errorf("invalid job mutation: %w", err)
		}
		if _, exists := mutationKeys[mutation.Key]; exists {
			return fmt.Errorf("duplicate job mutation key %q", mutation.Key)
		}
		mutationKeys[mutation.Key] = struct{}{}
	}
	return nil
}
