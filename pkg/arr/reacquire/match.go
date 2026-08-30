package reacquire

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/strm"
)

// maxStrmRead bounds a .strm read; a canonical stream URL is far smaller.
const maxStrmRead = 8 << 10

func bindingsFromMatches(instance arr.Arr, generation uint64, matches []libraryMatch) []Binding {
	bindings := make([]Binding, 0, len(matches))
	for _, match := range matches {
		bindings = append(bindings, bindingFromFiles(instance, generation, ConfidenceExactPath, match.library, match.managed))
	}
	return bindings
}

func bindingsFromHistoryRecords(
	instance arr.Arr,
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
		if !strings.EqualFold(record.EventType, arr.EventImported) {
			continue
		}
		droppedPath, _ := record.DataValue("droppedPath")
		importedPath, _ := record.DataValue("importedPath")
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
	instance arr.Arr,
	generation uint64,
	current []Binding,
	existing []Binding,
	library []arr.LibraryFile,
	managed []ManagedFile,
) []Binding {
	fingerprint := instance.Fingerprint()
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

func bindingFromFiles(instance arr.Arr, generation uint64, confidence Confidence, library arr.LibraryFile, managed ManagedFile) Binding {
	return Binding{
		ArrName:                instance.Name,
		ArrType:                instance.Type,
		ArrInstanceFingerprint: instance.Fingerprint(),
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
