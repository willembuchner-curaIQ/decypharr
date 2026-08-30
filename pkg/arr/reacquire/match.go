package reacquire

import (
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

type libraryMatch struct {
	library arr.LibraryFile
	managed ManagedFile
}

type targetFileKey struct {
	folder string
	name   string
}

func matchLibraryFiles(library []arr.LibraryFile, managed []ManagedFile) []libraryMatch {
	managedByTarget := make(map[targetFileKey][]ManagedFile, len(managed))
	for _, file := range managed {
		if file.EntryFolder == "" || file.FileName == "" {
			continue
		}
		key := targetFileKey{
			folder: filepath.Clean(file.EntryFolder),
			name:   filepath.Clean(file.FileName),
		}
		managedByTarget[key] = append(managedByTarget[key], file)
	}

	candidates := make([]libraryMatch, 0, min(len(library), len(managed)))
	for _, libraryFile := range library {
		target := symlinkTarget(libraryFile.Path)
		if target == "" {
			continue
		}
		directory, name := filepath.Split(target)
		key := targetFileKey{
			folder: filepath.Base(filepath.Clean(directory)),
			name:   filepath.Clean(name),
		}
		managedFiles := managedByTarget[key]
		if len(managedFiles) == 1 {
			candidates = append(candidates, libraryMatch{library: libraryFile, managed: managedFiles[0]})
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
	return filepath.Clean(target)
}

func bindingsFromMatches(instance arr.Arr, generation uint64, matches []libraryMatch) []Binding {
	bindings := make([]Binding, 0, len(matches))
	for _, match := range matches {
		bindings = append(bindings, Binding{
			ArrName:                instance.Name,
			ArrType:                instance.Type,
			ArrInstanceFingerprint: instance.Fingerprint(),
			EntryID:                match.managed.EntryID,
			EntryName:              match.managed.EntryName,
			EntryFileID:            match.managed.FileID,
			EntryFileName:          match.managed.FileName,
			DownloadID:             match.managed.DownloadID,
			ArrFileID:              match.library.ArrFileID,
			LibraryPath:            match.library.Path,
			SeriesID:               match.library.SeriesID,
			SeasonNumber:           match.library.SeasonNumber,
			EpisodeIDs:             match.library.EpisodeIDs,
			MovieID:                match.library.MovieID,
			Confidence:             ConfidenceExactPath,
			Generation:             generation,
			UpdatedAt:              time.Now().UTC(),
		})
	}
	return bindings
}
