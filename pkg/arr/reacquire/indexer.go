package reacquire

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
)

// ManagedFile is the stable Decypharr-side identity used by the arr.Arr index.
type ManagedFile struct {
	ArrName     string `json:"arr_name"`
	EntryID     string `json:"entry_id"`
	EntryName   string `json:"entry_name"`
	EntryFolder string `json:"entry_folder"`
	FileID      string `json:"file_id"`
	FileName    string `json:"file_name"`
	DownloadID  string `json:"download_id"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
}

// ManagedCatalog exposes managed files without coupling pkg/arr to storage.
// A non-empty entry ID must return only that entry's files: a targeted index
// runs after every completed download and must not walk the whole library.
type ManagedCatalog interface {
	ListManagedFiles(ctx context.Context, arrName, entryID string) ([]ManagedFile, error)
}

type bindingWriter interface {
	UpsertBinding(Binding) error
	ReplaceArrGeneration(string, uint64, []Binding) error
}

type bindingSnapshotReader interface {
	BindingsByArr(string) []Binding
}

type indexRequest struct {
	arrName string
	entryID string
	attempt int
}

type Indexer struct {
	arrs    *arr.Service
	catalog ManagedCatalog
	writer  bindingWriter
	logger  zerolog.Logger

	wake        chan struct{}
	queue       []indexRequest
	pending     map[string]struct{}
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	lifecycleMu sync.Mutex
	started     atomic.Bool
	closed      atomic.Bool
	sequence    atomic.Uint64
}

var targetedIndexBackoff = [...]time.Duration{
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
}

func NewIndexer(arrs *arr.Service, catalog ManagedCatalog, writer bindingWriter) *Indexer {
	return &Indexer{
		arrs:    arrs,
		catalog: catalog,
		writer:  writer,
		logger:  logger.New("arr-indexer"),
		wake:    make(chan struct{}, 1),
		pending: make(map[string]struct{}),
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	if i == nil || i.arrs == nil || i.catalog == nil || i.writer == nil {
		return fmt.Errorf("arr indexer is not configured")
	}
	i.lifecycleMu.Lock()
	defer i.lifecycleMu.Unlock()
	if i.closed.Load() {
		return fmt.Errorf("arr indexer is closed")
	}
	if !i.started.CompareAndSwap(false, true) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	i.mu.Lock()
	i.ctx = workerCtx
	i.cancel = cancel
	i.mu.Unlock()
	i.wg.Go(func() { i.run(workerCtx) })
	i.Refresh()
	return nil
}

func (i *Indexer) Close() {
	if i == nil {
		return
	}
	i.lifecycleMu.Lock()
	if !i.closed.CompareAndSwap(false, true) {
		i.lifecycleMu.Unlock()
		return
	}
	if i.cancel != nil {
		i.cancel()
	}
	i.lifecycleMu.Unlock()
	i.wg.Wait()
}

func (i *Indexer) Refresh() bool {
	return i.enqueue(indexRequest{})
}

func (i *Indexer) ReindexEntry(arrName, entryID string) bool {
	arrName = strings.TrimSpace(arrName)
	entryID = strings.TrimSpace(entryID)
	if arrName == "" || entryID == "" {
		return false
	}
	return i.enqueue(indexRequest{arrName: arrName, entryID: entryID})
}

func (i *Indexer) enqueue(request indexRequest) bool {
	if i == nil || i.closed.Load() {
		return false
	}
	key := request.arrName + "\x00" + request.entryID
	i.mu.Lock()
	if i.ctx == nil || i.ctx.Err() != nil {
		i.mu.Unlock()
		return false
	}
	if _, exists := i.pending[key]; exists {
		i.mu.Unlock()
		return true
	}
	i.pending[key] = struct{}{}
	i.queue = append(i.queue, request)
	i.mu.Unlock()

	select {
	case i.wake <- struct{}{}:
	default:
	}
	return true
}

func (i *Indexer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-i.wake:
			for {
				request, ok := i.nextRequest()
				if !ok {
					break
				}
				i.handle(ctx, request)
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func (i *Indexer) nextRequest() (indexRequest, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.queue) == 0 {
		return indexRequest{}, false
	}
	request := i.queue[0]
	i.queue[0] = indexRequest{}
	i.queue = i.queue[1:]
	delete(i.pending, request.arrName+"\x00"+request.entryID)
	return request, true
}

func (i *Indexer) handle(ctx context.Context, request indexRequest) {
	if request.arrName == "" {
		failed := false
		for _, instance := range i.arrs.All() {
			if instance.Type != arr.Sonarr && instance.Type != arr.Radarr {
				continue
			}
			if _, err := i.reconcile(ctx, instance, ""); err != nil {
				failed = true
				i.logger.Warn().Err(err).Str("arr", instance.Name).Msg("Arr index reconciliation failed")
			}
		}
		if failed && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
			i.retry(ctx, request, targetedIndexBackoff[request.attempt])
		}
		return
	}

	instance, ok := i.arrs.Get(request.arrName)
	if !ok {
		i.logger.Warn().Str("arr", request.arrName).Msg("Targeted Arr index skipped: instance not found")
		return
	}
	found, err := i.reconcile(ctx, instance, request.entryID)
	if err != nil {
		i.logger.Debug().Err(err).Str("arr", request.arrName).Str("entry_id", request.entryID).Msg("Targeted Arr index attempt failed")
	}
	if !found && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
		i.retry(ctx, request, targetedIndexBackoff[request.attempt])
	}
}

func (i *Indexer) reconcile(ctx context.Context, instance arr.Arr, entryID string) (bool, error) {
	managed, err := i.catalog.ListManagedFiles(ctx, instance.Name, entryID)
	if err != nil {
		return false, err
	}
	var history []arr.HistoryRecord
	if entryID != "" {
		history, err = i.historyForManaged(ctx, instance, managed, nil)
		if err != nil {
			return false, err
		}
	}

	var library []arr.LibraryFile
	if entryID == "" {
		library, err = i.arrs.LibraryFiles(ctx, instance.Name)
	} else {
		library, err = i.arrs.LibraryFilesForHistory(ctx, instance.Name, history)
	}
	if err != nil {
		return false, err
	}

	generation := uint64(time.Now().UnixMilli()) + i.sequence.Add(1)
	bindings := bindingsFromMatches(instance, generation, matchLibraryFiles(library, managed))
	if entryID == "" {
		if reader, ok := i.writer.(bindingSnapshotReader); ok {
			bindings = carryForwardBindings(instance, generation, bindings, reader.BindingsByArr(instance.Name), library, managed)
		}
		if unmatched := unmatchedDownloadIDs(managed, bindings); len(unmatched) > 0 {
			history, err := i.arrs.ImportHistory(ctx, instance.Name, unmatched)
			if err != nil {
				return false, err
			}
			bindings = append(bindings, bindingsFromHistoryRecords(instance, generation, library, managed, bindings, history)...)
		}
		if len(library) > 0 && len(managed) > 0 && len(bindings) == 0 {
			return false, fmt.Errorf("arr returned %d library files but none matched %d managed files", len(library), len(managed))
		}
		if err := i.writer.ReplaceArrGeneration(instance.Name, generation, bindings); err != nil {
			return false, err
		}
	} else {
		bindings = append(bindings, bindingsFromHistoryRecords(instance, generation, library, managed, bindings, history)...)
		for _, binding := range bindings {
			if err := i.writer.UpsertBinding(binding); err != nil {
				return false, err
			}
		}
	}
	return len(bindings) > 0, nil
}

func (i *Indexer) retry(ctx context.Context, request indexRequest, delay time.Duration) {
	request.attempt++
	i.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			i.enqueue(request)
		}
	})
}

func (i *Indexer) historyForManaged(
	ctx context.Context,
	instance arr.Arr,
	managed []ManagedFile,
	existing []Binding,
) ([]arr.HistoryRecord, error) {
	matched := bindingKeys(existing)
	downloadIDs := make(map[string]struct{})
	for _, file := range managed {
		if _, ok := matched[entryFileKey{entryID: file.EntryID, fileID: file.FileID}]; ok {
			continue
		}
		if file.DownloadID != "" {
			downloadIDs[file.DownloadID] = struct{}{}
		}
	}
	if len(downloadIDs) == 0 {
		return nil, nil
	}

	records := make([]arr.HistoryRecord, 0)
	for downloadID := range downloadIDs {
		downloadHistory, err := i.arrs.DownloadHistory(ctx, instance.Name, downloadID, "")
		if err != nil {
			return nil, err
		}
		records = append(records, downloadHistory...)
	}
	return records, nil
}

func matchLibraryFiles(library []arr.LibraryFile, managed []ManagedFile) []libraryMatch {
	byPath := make(map[string][]ManagedFile, len(managed))
	byIdentity := make(map[entryFileKey]ManagedFile, len(managed))
	bySize := make(map[int64][]ManagedFile)
	for _, file := range managed {
		if file.Path != "" {
			path := cleanAbsolutePath(file.Path)
			byPath[path] = append(byPath[path], file)
		}
		byIdentity[entryFileKey{entryID: file.EntryID, fileID: file.FileID}] = file
		if file.Size > 0 {
			bySize[file.Size] = append(bySize[file.Size], file)
		}
	}

	candidates := make([]libraryMatch, 0, min(len(library), len(managed)))
	for _, file := range library {
		if key, ok := strmIdentity(file.Path); ok {
			if managedFile, found := byIdentity[key]; found {
				candidates = append(candidates, libraryMatch{library: file, managed: managedFile})
			}
			continue
		}
		if target := symlinkTarget(file.Path); target != "" {
			pathMatches := byPath[target]
			if len(pathMatches) == 1 {
				candidates = append(candidates, libraryMatch{library: file, managed: pathMatches[0]})
				continue
			}
			if len(pathMatches) > 1 {
				continue
			}
		}

		candidate, ok := sameFileCandidate(file.Path, bySize[file.Size])
		if ok {
			candidates = append(candidates, libraryMatch{library: file, managed: candidate})
		}
	}

	managedUses := make(map[entryFileKey]int, len(candidates))
	arrFileUses := make(map[int]int, len(candidates))
	for _, candidate := range candidates {
		managedUses[entryFileKey{entryID: candidate.managed.EntryID, fileID: candidate.managed.FileID}]++
		arrFileUses[candidate.library.ArrFileID]++
	}
	matches := candidates[:0]
	for _, candidate := range candidates {
		managedKey := entryFileKey{entryID: candidate.managed.EntryID, fileID: candidate.managed.FileID}
		if managedUses[managedKey] == 1 && arrFileUses[candidate.library.ArrFileID] == 1 {
			matches = append(matches, candidate)
		}
	}
	return matches
}
