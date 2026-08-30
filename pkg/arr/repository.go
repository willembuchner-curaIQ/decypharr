package arr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/appendstore"
)

const (
	bindingAttributeArrName  = "arrName"
	bindingSnapshotKeyPrefix = "arr-binding-snapshot:"
	bindingDeltaKeyPrefix    = "arr-binding-delta:"
	bindingSnapshotVersion   = 1
	jobAttributeArrName      = "arrName"
	jobAttributeStatus       = "status"
)

type bindingRepositoryStore interface {
	ForEach(func(string, []byte) error) error
	Put(string, []byte, *appendstore.PutOptions) error
	Delete(string) error
	Sync() error
	Close() error
}

// BindingRepository persists Arr bindings as one snapshot per Arr plus a delta
// row per targeted change. A full reconciliation replaces the snapshot; single
// upserts write one row, because a library can hold hundreds of thousands of
// bindings and rewriting the snapshot per file does not scale.
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

// bindingDelta is one change made since its Arr's snapshot was written.
type bindingDelta struct {
	Version     int      `json:"version"`
	ArrName     string   `json:"arrName"`
	EntryID     string   `json:"entryId"`
	EntryFileID string   `json:"entryFileId"`
	Generation  uint64   `json:"generation,omitzero"`
	Deleted     bool     `json:"deleted,omitempty"`
	Binding     *Binding `json:"binding,omitempty"`
}

// bindingRepositoryState indexes what is on disk. It never retains binding
// payloads: the in-memory Index already holds those.
type bindingRepositoryState struct {
	owners      map[entryFileKey]string
	generations map[string]uint64
	deltas      map[entryFileKey]struct{}
	legacy      map[string]map[entryFileKey]struct{}
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

	bindings, state, err := r.scanLocked()
	if err != nil {
		return nil, err
	}
	r.state = state
	r.loaded = true
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
	key := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
	if owner, exists := state.owners[key]; exists && owner != binding.ArrName {
		return fmt.Errorf("save arr binding: managed file already belongs to arr %q", owner)
	}

	stored := cloneBinding(binding)
	delta := bindingDelta{
		Version:     bindingSnapshotVersion,
		ArrName:     binding.ArrName,
		EntryID:     binding.EntryID,
		EntryFileID: binding.EntryFileID,
		Generation:  binding.Generation,
		Binding:     &stored,
	}
	if err := r.persistDeltaLocked(delta); err != nil {
		return err
	}
	state.owners[key] = binding.ArrName
	state.deltas[key] = struct{}{}
	state.generations[binding.ArrName] = max(state.generations[binding.ArrName], binding.Generation)
	return nil
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
	key := entryFileKey{entryID: entryID, fileID: fileID}
	owner, exists := state.owners[key]
	if !exists {
		return nil
	}
	delta := bindingDelta{
		Version:     bindingSnapshotVersion,
		ArrName:     owner,
		EntryID:     entryID,
		EntryFileID: fileID,
		Generation:  state.generations[owner],
		Deleted:     true,
	}
	if err := r.persistDeltaLocked(delta); err != nil {
		return err
	}
	delete(state.owners, key)
	state.deltas[key] = struct{}{}
	return nil
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
	sortBindings(prepared)
	snapshot := bindingSnapshot{
		Version:    bindingSnapshotVersion,
		ArrName:    arrName,
		Generation: generation,
		Bindings:   prepared,
	}
	key := bindingSnapshotStoreKey(arrName)
	if err := validateBindingSnapshot(key, snapshot); err != nil {
		return fmt.Errorf("replace arr bindings: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := r.stateLocked()
	if err != nil {
		return err
	}
	for _, binding := range prepared {
		bindingKey := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
		if owner, exists := state.owners[bindingKey]; exists && owner != arrName {
			return fmt.Errorf("replace arr bindings: managed file already belongs to arr %q", owner)
		}
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode arr binding snapshot: %w", err)
	}
	options := &appendstore.PutOptions{Attributes: map[string]string{
		bindingAttributeArrName: arrName,
		"generation":            strconv.FormatUint(generation, 10),
	}}
	if err := r.store.Put(key, data, options); err != nil {
		return fmt.Errorf("persist arr binding snapshot: %w", err)
	}
	if err := r.store.Sync(); err != nil {
		r.invalidateLocked()
		return fmt.Errorf("sync arr binding snapshot: %w", err)
	}

	// The snapshot is authoritative from here on, so the rows it replaces are
	// dropped. A crash in between leaves rows the generation guard ignores.
	r.dropSupersededRowsLocked(state, arrName, prepared)
	state.generations[arrName] = generation
	return nil
}

func (r *BindingRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.Close()
}

func (r *BindingRepository) stateLocked() (*bindingRepositoryState, error) {
	if r.loaded {
		return &r.state, nil
	}
	_, state, err := r.scanLocked()
	if err != nil {
		return nil, err
	}
	r.state = state
	r.loaded = true
	return &r.state, nil
}

func (r *BindingRepository) invalidateLocked() {
	r.state = bindingRepositoryState{}
	r.loaded = false
}

// scanLocked reads the store once and merges the three row kinds: a snapshot is
// authoritative for its Arr, deltas written after it win per managed file, and
// legacy rows only apply to an Arr that has no snapshot yet.
func (r *BindingRepository) scanLocked() ([]Binding, bindingRepositoryState, error) {
	snapshots := make(map[string]bindingSnapshot)
	deltas := make(map[entryFileKey]bindingDelta)
	legacy := make(map[string][]Binding)
	if err := r.store.ForEach(func(key string, value []byte) error {
		switch {
		case strings.HasPrefix(key, bindingSnapshotKeyPrefix):
			var snapshot bindingSnapshot
			if err := json.Unmarshal(value, &snapshot); err != nil {
				return fmt.Errorf("decode arr binding snapshot %q: %w", key, err)
			}
			if err := validateBindingSnapshot(key, snapshot); err != nil {
				return fmt.Errorf("decode arr binding snapshot %q: %w", key, err)
			}
			snapshots[snapshot.ArrName] = snapshot
		case strings.HasPrefix(key, bindingDeltaKeyPrefix):
			var delta bindingDelta
			if err := json.Unmarshal(value, &delta); err != nil {
				return fmt.Errorf("decode arr binding delta %q: %w", key, err)
			}
			if err := validateBindingDelta(key, delta); err != nil {
				return fmt.Errorf("decode arr binding delta %q: %w", key, err)
			}
			deltas[entryFileKey{entryID: delta.EntryID, fileID: delta.EntryFileID}] = delta
		default:
			var binding Binding
			if err := json.Unmarshal(value, &binding); err != nil {
				return fmt.Errorf("decode legacy arr binding %q: %w", key, err)
			}
			if err := binding.validate(); err != nil {
				return fmt.Errorf("decode legacy arr binding %q: %w", key, err)
			}
			legacy[binding.ArrName] = append(legacy[binding.ArrName], binding)
		}
		return nil
	}); err != nil {
		return nil, bindingRepositoryState{}, err
	}

	state := bindingRepositoryState{
		owners:      make(map[entryFileKey]string),
		generations: make(map[string]uint64, len(snapshots)),
		deltas:      make(map[entryFileKey]struct{}, len(deltas)),
		legacy:      make(map[string]map[entryFileKey]struct{}, len(legacy)),
	}
	merged := make(map[entryFileKey]Binding, len(deltas))
	for arrName, snapshot := range snapshots {
		state.generations[arrName] = snapshot.Generation
		for _, binding := range snapshot.Bindings {
			merged[entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}] = binding
		}
	}
	for arrName, bindings := range legacy {
		keys := make(map[entryFileKey]struct{}, len(bindings))
		for _, binding := range bindings {
			keys[entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}] = struct{}{}
		}
		state.legacy[arrName] = keys
		if _, superseded := snapshots[arrName]; superseded {
			continue
		}
		for _, binding := range bindings {
			key := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
			merged[key] = binding
			state.generations[arrName] = max(state.generations[arrName], binding.Generation)
		}
	}
	for key, delta := range deltas {
		state.deltas[key] = struct{}{}
		if delta.Generation < snapshots[delta.ArrName].Generation {
			continue // superseded by a snapshot written after this delta
		}
		if delta.Deleted {
			delete(merged, key)
			continue
		}
		merged[key] = *delta.Binding
		state.generations[delta.ArrName] = max(state.generations[delta.ArrName], delta.Generation)
	}

	bindings := make([]Binding, 0, len(merged))
	for key, binding := range merged {
		if owner, exists := state.owners[key]; exists {
			return nil, bindingRepositoryState{}, fmt.Errorf("managed file belongs to both arr %q and %q", owner, binding.ArrName)
		}
		state.owners[key] = binding.ArrName
		bindings = append(bindings, cloneBinding(binding))
	}
	return bindings, state, nil
}

func (r *BindingRepository) persistDeltaLocked(delta bindingDelta) error {
	key := bindingDeltaStoreKey(delta.EntryID, delta.EntryFileID)
	if err := validateBindingDelta(key, delta); err != nil {
		return fmt.Errorf("persist arr binding delta: %w", err)
	}
	data, err := json.Marshal(delta)
	if err != nil {
		return fmt.Errorf("encode arr binding delta: %w", err)
	}
	options := &appendstore.PutOptions{Attributes: map[string]string{
		bindingAttributeArrName: delta.ArrName,
	}}
	if err := r.store.Put(key, data, options); err != nil {
		return fmt.Errorf("persist arr binding delta: %w", err)
	}
	if err := r.store.Sync(); err != nil {
		r.invalidateLocked()
		return fmt.Errorf("sync arr binding delta: %w", err)
	}
	return nil
}

// dropSupersededRowsLocked removes the delta and legacy rows a fresh snapshot
// replaces. Failures are not fatal: the loader ignores stale rows.
func (r *BindingRepository) dropSupersededRowsLocked(state *bindingRepositoryState, arrName string, bindings []Binding) {
	for key := range state.legacy[arrName] {
		if err := r.store.Delete(bindingStoreKey(key.entryID, key.fileID)); err == nil {
			delete(state.owners, key)
		}
	}
	delete(state.legacy, arrName)
	for key := range state.deltas {
		if state.owners[key] != arrName {
			continue
		}
		if err := r.store.Delete(bindingDeltaStoreKey(key.entryID, key.fileID)); err != nil {
			continue
		}
		delete(state.deltas, key)
	}
	for key, owner := range state.owners {
		if owner == arrName {
			delete(state.owners, key)
		}
	}
	for _, binding := range bindings {
		key := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
		if _, stale := state.deltas[key]; stale {
			if err := r.store.Delete(bindingDeltaStoreKey(key.entryID, key.fileID)); err == nil {
				delete(state.deltas, key)
			}
		}
		state.owners[key] = arrName
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
	seen := make(map[entryFileKey]struct{}, len(snapshot.Bindings))
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
		bindingKey := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
		if _, exists := seen[bindingKey]; exists {
			return fmt.Errorf("duplicate managed file %q/%q", binding.EntryID, binding.EntryFileID)
		}
		seen[bindingKey] = struct{}{}
	}
	return nil
}

func validateBindingDelta(key string, delta bindingDelta) error {
	switch {
	case delta.Version != bindingSnapshotVersion:
		return fmt.Errorf("unsupported version %d", delta.Version)
	case delta.ArrName == "":
		return errors.New("arr name is required")
	case key != bindingDeltaStoreKey(delta.EntryID, delta.EntryFileID):
		return errors.New("delta key does not match the managed file")
	case delta.Deleted:
		return nil
	case delta.Binding == nil:
		return errors.New("delta binding is required")
	}
	if err := delta.Binding.validate(); err != nil {
		return err
	}
	if delta.Binding.ArrName != delta.ArrName ||
		delta.Binding.EntryID != delta.EntryID ||
		delta.Binding.EntryFileID != delta.EntryFileID {
		return errors.New("delta binding identity does not match the delta")
	}
	return nil
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

func bindingDeltaStoreKey(entryID, fileID string) string {
	return bindingDeltaKeyPrefix + bindingStoreKey(entryID, fileID)
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
