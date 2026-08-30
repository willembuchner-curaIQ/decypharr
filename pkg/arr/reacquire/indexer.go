package reacquire

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/strm"
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
	registry *arr.Storage
	catalog  ManagedCatalog
	writer   bindingWriter
	logger   zerolog.Logger

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

func NewIndexer(registry *arr.Storage, catalog ManagedCatalog, writer bindingWriter) *Indexer {
	return &Indexer{
		registry: registry,
		catalog:  catalog,
		writer:   writer,
		logger:   logger.New("arr-indexer"),
		wake:     make(chan struct{}, 1),
		pending:  make(map[string]struct{}),
	}
}

func (i *Indexer) Start(ctx context.Context) error {
	if i == nil || i.registry == nil || i.catalog == nil || i.writer == nil {
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
		for _, instance := range i.registry.GetAll() {
			if instance == nil || (instance.Type != arr.Sonarr && instance.Type != arr.Radarr) {
				continue
			}
			if _, err := i.reconcile(ctx, instance, ""); err != nil {
				failed = true
				i.logger.Warn().Err(err).Str("arr", instance.Name).Msg("arr.Arr index reconciliation failed")
			}
		}
		if failed && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
			i.retry(ctx, request, targetedIndexBackoff[request.attempt])
		}
		return
	}

	instance := i.registry.Get(request.arrName)
	if instance == nil {
		i.logger.Warn().Str("arr", request.arrName).Msg("Targeted arr.Arr index skipped: instance not found")
		return
	}
	found, err := i.reconcile(ctx, instance, request.entryID)
	if err != nil {
		i.logger.Debug().Err(err).Str("arr", request.arrName).Str("entry_id", request.entryID).Msg("Targeted arr.Arr index attempt failed")
	}
	if !found && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
		i.retry(ctx, request, targetedIndexBackoff[request.attempt])
	}
}

func (i *Indexer) reconcile(ctx context.Context, instance *arr.Arr, entryID string) (bool, error) {
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
		library, err = instance.ListLibraryFiles(ctx)
	} else {
		library, err = instance.ListTargetLibraryFiles(ctx, history)
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
			history, err := instance.ImportHistory(ctx, unmatched)
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

func bindingsFromMatches(instance *arr.Arr, generation uint64, matches []libraryMatch) []Binding {
	bindings := make([]Binding, 0, len(matches))
	for _, match := range matches {
		bindings = append(bindings, bindingFromFiles(instance, generation, ConfidenceExactPath, match.library, match.managed))
	}
	return bindings
}

func (i *Indexer) historyForManaged(
	ctx context.Context,
	instance *arr.Arr,
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
		downloadHistory, err := instance.HistoryByDownloadID(ctx, downloadID, "")
		if err != nil {
			return nil, err
		}
		records = append(records, downloadHistory...)
	}
	return records, nil
}

func bindingsFromHistoryRecords(
	instance *arr.Arr,
	generation uint64,
	library []arr.LibraryFile,
	managed []ManagedFile,
	existing []Binding,
	records []arr.HistoryRecord,
) []Binding {
	matched := make(map[entryFileKey]struct{}, len(existing))
	usedArrFiles := make(map[int]struct{}, len(existing))
	for _, binding := range existing {
		matched[entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}] = struct{}{}
		if binding.ArrFileID > 0 {
			usedArrFiles[binding.ArrFileID] = struct{}{}
		}
	}
	managedByPath := make(map[string][]ManagedFile)
	for _, file := range managed {
		if _, ok := matched[entryFileKey{entryID: file.EntryID, fileID: file.FileID}]; ok {
			continue
		}
		path := cleanAbsolutePath(file.Path)
		managedByPath[path] = append(managedByPath[path], file)
	}

	episodes, movies := libraryMediaIndexes(library)
	bindings := make([]Binding, 0)
	ordered := slices.Clone(records)
	slices.SortStableFunc(ordered, func(left, right arr.HistoryRecord) int {
		return right.Date.Compare(left.Date)
	})
	for _, record := range ordered {
		if !strings.EqualFold(record.EventType, "downloadFolderImported") {
			continue
		}
		droppedPath := historyValue(record.Data, "droppedPath")
		importedPath := historyValue(record.Data, "importedPath")
		if droppedPath == "" || importedPath == "" || record.DownloadID == "" {
			continue
		}
		libraryFile, ok := historyLibraryFile(instance.Type, record, episodes, movies)
		if !ok || cleanAbsolutePath(importedPath) != cleanAbsolutePath(libraryFile.Path) {
			continue
		}
		if _, used := usedArrFiles[libraryFile.ArrFileID]; used {
			continue
		}
		usedArrFiles[libraryFile.ArrFileID] = struct{}{}
		managedMatches := managedByPath[cleanAbsolutePath(droppedPath)]
		if len(managedMatches) != 1 || managedMatches[0].DownloadID != record.DownloadID {
			continue
		}
		managedFile := managedMatches[0]
		key := entryFileKey{entryID: managedFile.EntryID, fileID: managedFile.FileID}
		if _, ok := matched[key]; ok {
			continue
		}
		bindings = append(bindings, bindingFromFiles(instance, generation, ConfidenceDownloadHistory, libraryFile, managedFile))
		matched[key] = struct{}{}
	}
	return bindings
}

// unmatchedDownloadIDs returns the downloads whose managed files no exact path
// match covered, which is the only history the index still needs to look up.
func unmatchedDownloadIDs(managed []ManagedFile, bindings []Binding) map[string]struct{} {
	matched := bindingKeys(bindings)
	downloadIDs := make(map[string]struct{})
	for _, file := range managed {
		if _, ok := matched[entryFileKey{entryID: file.EntryID, fileID: file.FileID}]; ok {
			continue
		}
		if file.DownloadID != "" {
			downloadIDs[file.DownloadID] = struct{}{}
		}
	}
	return downloadIDs
}

func bindingKeys(bindings []Binding) map[entryFileKey]struct{} {
	keys := make(map[entryFileKey]struct{}, len(bindings))
	for _, binding := range bindings {
		keys[entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}] = struct{}{}
	}
	return keys
}

func carryForwardBindings(
	instance *arr.Arr,
	generation uint64,
	current []Binding,
	existing []Binding,
	library []arr.LibraryFile,
	managed []ManagedFile,
) []Binding {
	fingerprint := instance.InstanceFingerprint()
	seen := make(map[entryFileKey]struct{}, len(current))
	usedArrFiles := make(map[int]struct{}, len(current))
	for _, binding := range current {
		seen[entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}] = struct{}{}
		if binding.ArrFileID > 0 {
			usedArrFiles[binding.ArrFileID] = struct{}{}
		}
	}
	managedByID := make(map[entryFileKey]ManagedFile, len(managed))
	for _, file := range managed {
		managedByID[entryFileKey{entryID: file.EntryID, fileID: file.FileID}] = file
	}
	libraryByID := make(map[int]arr.LibraryFile, len(library))
	for _, file := range library {
		libraryByID[file.ArrFileID] = file
	}

	for _, old := range existing {
		key := entryFileKey{entryID: old.EntryID, fileID: old.EntryFileID}
		if _, ok := seen[key]; ok {
			continue
		}
		if _, used := usedArrFiles[old.ArrFileID]; used {
			continue
		}
		managedFile, managedOK := managedByID[key]
		libraryFile, libraryOK := libraryByID[old.ArrFileID]
		if !managedOK || !libraryOK ||
			fingerprint == "" || old.ArrInstanceFingerprint != fingerprint ||
			!sameLibraryPath(old.LibraryPath, libraryFile.Path) || !sameLibraryMedia(old, libraryFile) {
			continue
		}
		current = append(current, bindingFromFiles(instance, generation, old.Confidence, libraryFile, managedFile))
		seen[key] = struct{}{}
		usedArrFiles[old.ArrFileID] = struct{}{}
	}
	return current
}

func bindingFromFiles(instance *arr.Arr, generation uint64, confidence Confidence, library arr.LibraryFile, managed ManagedFile) Binding {
	return Binding{
		ArrName:                instance.Name,
		ArrType:                instance.Type,
		ArrInstanceFingerprint: instance.InstanceFingerprint(),
		EntryID:                managed.EntryID,
		EntryName:              managed.EntryName,
		EntryFileID:            managed.FileID,
		EntryFileName:          managed.FileName,
		DownloadID:             managed.DownloadID,
		ArrFileID:              library.ArrFileID,
		LibraryPath:            library.Path,
		SeriesID:               library.SeriesID,
		SeasonNumber:           library.SeasonNumber,
		EpisodeIDs:             library.EpisodeIDs,
		MovieID:                library.MovieID,
		Confidence:             confidence,
		Generation:             generation,
		UpdatedAt:              time.Now().UTC(),
	}
}

func libraryMediaIndexes(library []arr.LibraryFile) (map[int][]arr.LibraryFile, map[int][]arr.LibraryFile) {
	episodes := make(map[int][]arr.LibraryFile)
	movies := make(map[int][]arr.LibraryFile)
	for _, file := range library {
		for _, episodeID := range file.EpisodeIDs {
			episodes[episodeID] = append(episodes[episodeID], file)
		}
		if file.MovieID > 0 {
			movies[file.MovieID] = append(movies[file.MovieID], file)
		}
	}
	return episodes, movies
}

func historyLibraryFile(kind arr.Type, record arr.HistoryRecord, episodes, movies map[int][]arr.LibraryFile) (arr.LibraryFile, bool) {
	var candidates []arr.LibraryFile
	switch kind {
	case arr.Sonarr:
		candidates = episodes[record.EpisodeID]
	case arr.Radarr:
		candidates = movies[record.MovieID]
	}
	if len(candidates) != 1 {
		return arr.LibraryFile{}, false
	}
	return candidates[0], true
}

func historyValue(data map[string]string, name string) string {
	for key, value := range data {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func sameLibraryMedia(binding Binding, file arr.LibraryFile) bool {
	if binding.MovieID > 0 {
		return binding.MovieID == file.MovieID
	}
	if binding.SeriesID > 0 && binding.SeriesID != file.SeriesID {
		return false
	}
	for _, episodeID := range binding.EpisodeIDs {
		if slices.Contains(file.EpisodeIDs, episodeID) {
			return true
		}
	}
	return false
}

type libraryMatch struct {
	library arr.LibraryFile
	managed ManagedFile
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

// maxStrmRead bounds a .strm read; a canonical stream URL is far smaller.
const maxStrmRead = 8 << 10

// strmIdentity reads a .strm library file and returns the managed file it
// points at. Decypharr writes its own signed stream URLs, so the identity is
// exact; a .strm written by anything else does not parse and is skipped.
func strmIdentity(path string) (entryFileKey, bool) {
	if !strings.EqualFold(filepath.Ext(path), ".strm") {
		return entryFileKey{}, false
	}
	file, err := os.Open(path)
	if err != nil {
		return entryFileKey{}, true
	}
	defer func() { _ = file.Close() }()

	content, err := io.ReadAll(io.LimitReader(file, maxStrmRead))
	if err != nil {
		return entryFileKey{}, true
	}
	entryID, fileID, ok := strm.ParseURL(string(content))
	if !ok {
		return entryFileKey{}, true
	}
	return entryFileKey{entryID: entryID, fileID: fileID}, true
}

func symlinkTarget(path string) string {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	return cleanAbsolutePath(target)
}

func sameFileCandidate(libraryPath string, candidates []ManagedFile) (ManagedFile, bool) {
	libraryInfo, err := os.Stat(libraryPath)
	if err != nil {
		return ManagedFile{}, false
	}
	var match ManagedFile
	found := false
	for _, candidate := range candidates {
		managedInfo, err := os.Stat(candidate.Path)
		if err == nil && os.SameFile(libraryInfo, managedInfo) {
			if found {
				return ManagedFile{}, false
			}
			match = candidate
			found = true
		}
	}
	return match, found
}

func cleanAbsolutePath(path string) string {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}
