package arr

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

type entryFileKey struct {
	entryID string
	fileID  string
}

type arrFileKey struct {
	arrName string
	fileID  int
}

type downloadKey struct {
	arrName    string
	downloadID string
}

type mediaKey struct {
	arrName string
	mediaID int
}

type Index struct {
	mu          sync.RWMutex
	bindings    map[entryFileKey]Binding
	byArr       map[string]map[entryFileKey]struct{}
	byArrFile   map[arrFileKey]entryFileKey
	byDownload  map[downloadKey]map[entryFileKey]struct{}
	byEpisode   map[mediaKey]map[entryFileKey]struct{}
	byMovie     map[mediaKey]map[entryFileKey]struct{}
	bySeries    map[mediaKey]map[entryFileKey]struct{}
	generations map[string]uint64
}

func NewIndex() *Index {
	return &Index{
		bindings:    make(map[entryFileKey]Binding),
		byArr:       make(map[string]map[entryFileKey]struct{}),
		byArrFile:   make(map[arrFileKey]entryFileKey),
		byDownload:  make(map[downloadKey]map[entryFileKey]struct{}),
		byEpisode:   make(map[mediaKey]map[entryFileKey]struct{}),
		byMovie:     make(map[mediaKey]map[entryFileKey]struct{}),
		bySeries:    make(map[mediaKey]map[entryFileKey]struct{}),
		generations: make(map[string]uint64),
	}
}

func (i *Index) Lookup(entryID, fileID string) (Binding, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	binding, ok := i.bindings[entryFileKey{entryID: entryID, fileID: fileID}]
	return cloneBinding(binding), ok
}

func (i *Index) ByArrFile(arrName string, fileID int) (Binding, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	key, ok := i.byArrFile[arrFileKey{arrName: arrName, fileID: fileID}]
	if !ok {
		return Binding{}, false
	}
	binding, ok := i.bindings[key]
	return cloneBinding(binding), ok
}

func (i *Index) ByDownloadID(arrName, downloadID string) []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.bindingsForLocked(i.byDownload[downloadKey{arrName: arrName, downloadID: downloadID}])
}

func (i *Index) ByEpisodeID(arrName string, episodeID int) []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.bindingsForLocked(i.byEpisode[mediaKey{arrName: arrName, mediaID: episodeID}])
}

func (i *Index) ByMovieID(arrName string, movieID int) []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.bindingsForLocked(i.byMovie[mediaKey{arrName: arrName, mediaID: movieID}])
}

func (i *Index) BySeriesID(arrName string, seriesID int) []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.bindingsForLocked(i.bySeries[mediaKey{arrName: arrName, mediaID: seriesID}])
}

func (i *Index) ByArr(arrName string) []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.bindingsForLocked(i.byArr[arrName])
}

func (i *Index) All() []Binding {
	i.mu.RLock()
	defer i.mu.RUnlock()

	bindings := make([]Binding, 0, len(i.bindings))
	for _, binding := range i.bindings {
		bindings = append(bindings, cloneBinding(binding))
	}
	sortBindings(bindings)
	return bindings
}

func (i *Index) Generation(arrName string) uint64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.generations[arrName]
}

func (i *Index) Upsert(binding Binding) error {
	if err := binding.validate(); err != nil {
		return fmt.Errorf("invalid arr binding: %w", err)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.upsertLocked(cloneBinding(binding))
	return nil
}

func (i *Index) DeleteEntryFile(entryID, fileID string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.removeLocked(entryFileKey{entryID: entryID, fileID: fileID})
}

func (i *Index) ReplaceArrGeneration(arrName string, generation uint64, bindings []Binding) error {
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
			return fmt.Errorf("invalid arr binding: %w", err)
		}
		prepared[i] = cloneBinding(binding)
	}
	if err := validateUniqueArrFiles(prepared); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	for key := range i.byArr[arrName] {
		i.removeLocked(key)
	}
	for _, binding := range prepared {
		i.upsertLocked(binding)
	}
	i.generations[arrName] = generation
	return nil
}

func validateUniqueArrFiles(bindings []Binding) error {
	seen := make(map[arrFileKey]entryFileKey, len(bindings))
	for _, binding := range bindings {
		if binding.ArrFileID <= 0 {
			continue
		}
		arrKey := arrFileKey{arrName: binding.ArrName, fileID: binding.ArrFileID}
		entryKey := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
		if previous, exists := seen[arrKey]; exists && previous != entryKey {
			return fmt.Errorf("arr file %d in %q maps to multiple managed files", binding.ArrFileID, binding.ArrName)
		}
		seen[arrKey] = entryKey
	}
	return nil
}

func (i *Index) replaceAll(bindings []Binding) error {
	for _, binding := range bindings {
		if err := binding.validate(); err != nil {
			return fmt.Errorf("invalid arr binding: %w", err)
		}
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	clear(i.bindings)
	clear(i.byArr)
	clear(i.byArrFile)
	clear(i.byDownload)
	clear(i.byEpisode)
	clear(i.byMovie)
	clear(i.bySeries)
	clear(i.generations)
	for _, binding := range bindings {
		i.upsertLocked(cloneBinding(binding))
	}
	return nil
}

func (i *Index) upsertLocked(binding Binding) {
	key := entryFileKey{entryID: binding.EntryID, fileID: binding.EntryFileID}
	i.removeLocked(key)
	if binding.ArrFileID != 0 {
		arrKey := arrFileKey{arrName: binding.ArrName, fileID: binding.ArrFileID}
		if previous, exists := i.byArrFile[arrKey]; exists && previous != key {
			i.removeLocked(previous)
		}
	}

	i.bindings[key] = binding
	addIndexValue(i.byArr, binding.ArrName, key)
	if binding.ArrFileID != 0 {
		i.byArrFile[arrFileKey{arrName: binding.ArrName, fileID: binding.ArrFileID}] = key
	}
	if binding.DownloadID != "" {
		addIndexValue(i.byDownload, downloadKey{arrName: binding.ArrName, downloadID: binding.DownloadID}, key)
	}
	for _, episodeID := range binding.EpisodeIDs {
		if episodeID != 0 {
			addIndexValue(i.byEpisode, mediaKey{arrName: binding.ArrName, mediaID: episodeID}, key)
		}
	}
	if binding.MovieID != 0 {
		addIndexValue(i.byMovie, mediaKey{arrName: binding.ArrName, mediaID: binding.MovieID}, key)
	}
	if binding.SeriesID != 0 {
		addIndexValue(i.bySeries, mediaKey{arrName: binding.ArrName, mediaID: binding.SeriesID}, key)
	}
	i.generations[binding.ArrName] = max(i.generations[binding.ArrName], binding.Generation)
}

func (i *Index) removeLocked(key entryFileKey) bool {
	binding, ok := i.bindings[key]
	if !ok {
		return false
	}
	delete(i.bindings, key)
	removeIndexValue(i.byArr, binding.ArrName, key)
	if binding.ArrFileID != 0 {
		delete(i.byArrFile, arrFileKey{arrName: binding.ArrName, fileID: binding.ArrFileID})
	}
	if binding.DownloadID != "" {
		removeIndexValue(i.byDownload, downloadKey{arrName: binding.ArrName, downloadID: binding.DownloadID}, key)
	}
	for _, episodeID := range binding.EpisodeIDs {
		if episodeID != 0 {
			removeIndexValue(i.byEpisode, mediaKey{arrName: binding.ArrName, mediaID: episodeID}, key)
		}
	}
	if binding.MovieID != 0 {
		removeIndexValue(i.byMovie, mediaKey{arrName: binding.ArrName, mediaID: binding.MovieID}, key)
	}
	if binding.SeriesID != 0 {
		removeIndexValue(i.bySeries, mediaKey{arrName: binding.ArrName, mediaID: binding.SeriesID}, key)
	}
	return true
}

func (i *Index) bindingsForLocked(keys map[entryFileKey]struct{}) []Binding {
	bindings := make([]Binding, 0, len(keys))
	for key := range keys {
		if binding, ok := i.bindings[key]; ok {
			bindings = append(bindings, cloneBinding(binding))
		}
	}
	sortBindings(bindings)
	return bindings
}

func addIndexValue[K comparable](values map[K]map[entryFileKey]struct{}, indexKey K, bindingKey entryFileKey) {
	keys := values[indexKey]
	if keys == nil {
		keys = make(map[entryFileKey]struct{})
		values[indexKey] = keys
	}
	keys[bindingKey] = struct{}{}
}

func removeIndexValue[K comparable](values map[K]map[entryFileKey]struct{}, indexKey K, bindingKey entryFileKey) {
	keys := values[indexKey]
	delete(keys, bindingKey)
	if len(keys) == 0 {
		delete(values, indexKey)
	}
}

func sortBindings(bindings []Binding) {
	slices.SortFunc(bindings, func(left, right Binding) int {
		if result := cmp.Compare(left.ArrName, right.ArrName); result != 0 {
			return result
		}
		if result := cmp.Compare(left.EntryID, right.EntryID); result != 0 {
			return result
		}
		return cmp.Compare(left.EntryFileID, right.EntryFileID)
	})
}
