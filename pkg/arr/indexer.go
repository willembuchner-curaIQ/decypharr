package arr

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
)

// LibraryFile is the Arr-side identity of an imported media file.
type LibraryFile struct {
	ArrFileID    int    `json:"arr_file_id"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	SeriesID     int    `json:"series_id,omitempty"`
	SeasonNumber int    `json:"season_number,omitempty"`
	EpisodeIDs   []int  `json:"episode_ids,omitempty"`
	MovieID      int    `json:"movie_id,omitempty"`
}

// ManagedFile is the stable Decypharr-side identity used by the Arr index.
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
	registry *Storage
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

func NewIndexer(registry *Storage, catalog ManagedCatalog, writer bindingWriter) *Indexer {
	return &Indexer{
		registry: registry,
		catalog:  catalog,
		writer:   writer,
		logger:   logger.New("arr-indexer"),
		wake:     make(chan struct{}, 1),
		pending:  make(map[string]struct{}),
	}
}

func (indexer *Indexer) Start(ctx context.Context) error {
	if indexer == nil || indexer.registry == nil || indexer.catalog == nil || indexer.writer == nil {
		return fmt.Errorf("arr indexer is not configured")
	}
	indexer.lifecycleMu.Lock()
	defer indexer.lifecycleMu.Unlock()
	if indexer.closed.Load() {
		return fmt.Errorf("arr indexer is closed")
	}
	if !indexer.started.CompareAndSwap(false, true) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	indexer.mu.Lock()
	indexer.ctx = workerCtx
	indexer.cancel = cancel
	indexer.mu.Unlock()
	indexer.wg.Go(func() { indexer.run(workerCtx) })
	indexer.Refresh()
	return nil
}

func (indexer *Indexer) Close() {
	if indexer == nil {
		return
	}
	indexer.lifecycleMu.Lock()
	if !indexer.closed.CompareAndSwap(false, true) {
		indexer.lifecycleMu.Unlock()
		return
	}
	if indexer.cancel != nil {
		indexer.cancel()
	}
	indexer.lifecycleMu.Unlock()
	indexer.wg.Wait()
}

func (indexer *Indexer) Refresh() bool {
	return indexer.enqueue(indexRequest{})
}

func (indexer *Indexer) ReindexEntry(arrName, entryID string) bool {
	arrName = strings.TrimSpace(arrName)
	entryID = strings.TrimSpace(entryID)
	if arrName == "" || entryID == "" {
		return false
	}
	return indexer.enqueue(indexRequest{arrName: arrName, entryID: entryID})
}

func (indexer *Indexer) enqueue(request indexRequest) bool {
	if indexer == nil || indexer.closed.Load() {
		return false
	}
	key := request.arrName + "\x00" + request.entryID
	indexer.mu.Lock()
	if indexer.ctx == nil || indexer.ctx.Err() != nil {
		indexer.mu.Unlock()
		return false
	}
	if _, exists := indexer.pending[key]; exists {
		indexer.mu.Unlock()
		return true
	}
	indexer.pending[key] = struct{}{}
	indexer.queue = append(indexer.queue, request)
	indexer.mu.Unlock()

	select {
	case indexer.wake <- struct{}{}:
	default:
	}
	return true
}

func (indexer *Indexer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-indexer.wake:
			for {
				request, ok := indexer.nextRequest()
				if !ok {
					break
				}
				indexer.handle(ctx, request)
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func (indexer *Indexer) nextRequest() (indexRequest, bool) {
	indexer.mu.Lock()
	defer indexer.mu.Unlock()
	if len(indexer.queue) == 0 {
		return indexRequest{}, false
	}
	request := indexer.queue[0]
	indexer.queue[0] = indexRequest{}
	indexer.queue = indexer.queue[1:]
	delete(indexer.pending, request.arrName+"\x00"+request.entryID)
	return request, true
}

func (indexer *Indexer) handle(ctx context.Context, request indexRequest) {
	if request.arrName == "" {
		failed := false
		for _, instance := range indexer.registry.GetAll() {
			if instance == nil || (instance.Type != Sonarr && instance.Type != Radarr) {
				continue
			}
			if _, err := indexer.reconcile(ctx, instance, ""); err != nil {
				failed = true
				indexer.logger.Warn().Err(err).Str("arr", instance.Name).Msg("Arr index reconciliation failed")
			}
		}
		if failed && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
			indexer.retry(ctx, request, targetedIndexBackoff[request.attempt])
		}
		return
	}

	instance := indexer.registry.Get(request.arrName)
	if instance == nil {
		indexer.logger.Warn().Str("arr", request.arrName).Msg("Targeted Arr index skipped: instance not found")
		return
	}
	found, err := indexer.reconcile(ctx, instance, request.entryID)
	if err != nil {
		indexer.logger.Debug().Err(err).Str("arr", request.arrName).Str("entry_id", request.entryID).Msg("Targeted Arr index attempt failed")
	}
	if !found && request.attempt < len(targetedIndexBackoff) && ctx.Err() == nil {
		indexer.retry(ctx, request, targetedIndexBackoff[request.attempt])
	}
}

func (indexer *Indexer) reconcile(ctx context.Context, instance *Arr, entryID string) (bool, error) {
	managed, err := indexer.catalog.ListManagedFiles(ctx, instance.Name, entryID)
	if err != nil {
		return false, err
	}
	var history []HistoryRecord
	if entryID != "" {
		history, err = indexer.historyForManaged(ctx, instance, managed, nil)
		if err != nil {
			return false, err
		}
	}

	var library []LibraryFile
	if entryID == "" {
		library, err = instance.ListLibraryFiles(ctx)
	} else {
		library, err = instance.listTargetLibraryFiles(ctx, history)
	}
	if err != nil {
		return false, err
	}

	generation := uint64(time.Now().UnixMilli()) + indexer.sequence.Add(1)
	bindings := bindingsFromMatches(instance, generation, matchLibraryFiles(library, managed))
	if entryID == "" {
		if reader, ok := indexer.writer.(bindingSnapshotReader); ok {
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
		if err := indexer.writer.ReplaceArrGeneration(instance.Name, generation, bindings); err != nil {
			return false, err
		}
	} else {
		bindings = append(bindings, bindingsFromHistoryRecords(instance, generation, library, managed, bindings, history)...)
		for _, binding := range bindings {
			if err := indexer.writer.UpsertBinding(binding); err != nil {
				return false, err
			}
		}
	}
	return len(bindings) > 0, nil
}

func (indexer *Indexer) retry(ctx context.Context, request indexRequest, delay time.Duration) {
	request.attempt++
	indexer.wg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			indexer.enqueue(request)
		}
	})
}

func bindingsFromMatches(instance *Arr, generation uint64, matches []libraryMatch) []Binding {
	bindings := make([]Binding, 0, len(matches))
	for _, match := range matches {
		bindings = append(bindings, bindingFromFiles(instance, generation, BindingConfidenceExactPath, match.library, match.managed))
	}
	return bindings
}

func (indexer *Indexer) historyForManaged(
	ctx context.Context,
	instance *Arr,
	managed []ManagedFile,
	existing []Binding,
) ([]HistoryRecord, error) {
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

	records := make([]HistoryRecord, 0)
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
	instance *Arr,
	generation uint64,
	library []LibraryFile,
	managed []ManagedFile,
	existing []Binding,
	records []HistoryRecord,
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
	slices.SortStableFunc(ordered, func(left, right HistoryRecord) int {
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
		bindings = append(bindings, bindingFromFiles(instance, generation, BindingConfidenceDownloadHistory, libraryFile, managedFile))
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
	instance *Arr,
	generation uint64,
	current []Binding,
	existing []Binding,
	library []LibraryFile,
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
	libraryByID := make(map[int]LibraryFile, len(library))
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

func bindingFromFiles(instance *Arr, generation uint64, confidence BindingConfidence, library LibraryFile, managed ManagedFile) Binding {
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

func libraryMediaIndexes(library []LibraryFile) (map[int][]LibraryFile, map[int][]LibraryFile) {
	episodes := make(map[int][]LibraryFile)
	movies := make(map[int][]LibraryFile)
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

func historyLibraryFile(kind Type, record HistoryRecord, episodes, movies map[int][]LibraryFile) (LibraryFile, bool) {
	var candidates []LibraryFile
	switch kind {
	case Sonarr:
		candidates = episodes[record.EpisodeID]
	case Radarr:
		candidates = movies[record.MovieID]
	}
	if len(candidates) != 1 {
		return LibraryFile{}, false
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

func sameLibraryMedia(binding Binding, file LibraryFile) bool {
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
	library LibraryFile
	managed ManagedFile
}

func matchLibraryFiles(library []LibraryFile, managed []ManagedFile) []libraryMatch {
	byPath := make(map[string][]ManagedFile, len(managed))
	bySize := make(map[int64][]ManagedFile)
	for _, file := range managed {
		if file.Path != "" {
			path := cleanAbsolutePath(file.Path)
			byPath[path] = append(byPath[path], file)
		}
		if file.Size > 0 {
			bySize[file.Size] = append(bySize[file.Size], file)
		}
	}

	candidates := make([]libraryMatch, 0, min(len(library), len(managed)))
	for _, file := range library {
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
