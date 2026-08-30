package reacquire

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

func TestMatchLibraryFilesBySymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "managed", "Movie.Release", "movie.mkv")
	libraryPath := filepath.Join(dir, "library", "Movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{{ArrFileID: 42, Path: libraryPath}},
		[]ManagedFile{{EntryID: "entry", EntryFolder: "Movie.Release", FileID: "file", FileName: "movie.mkv"}},
	)
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(matches))
	}
	if matches[0].managed.FileID != "file" {
		t.Fatalf("managed file ID = %q, want file", matches[0].managed.FileID)
	}
}

func TestMatchLibraryFilesResolvesRelativeSymlink(t *testing.T) {
	dir := t.TempDir()
	libraryPath := filepath.Join(dir, "library", "Movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join("..", "managed", "Movie.Release", "movie.mkv")
	if err := os.Symlink(target, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{{ArrFileID: 42, Path: libraryPath}},
		[]ManagedFile{{EntryID: "entry", EntryFolder: "Movie.Release", FileID: "file", FileName: "movie.mkv"}},
	)
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(matches))
	}
}

func TestMatchLibraryFilesSkipsNonSymlink(t *testing.T) {
	dir := t.TempDir()
	libraryPath := filepath.Join(dir, "Movie.mkv")
	if err := os.WriteFile(libraryPath, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{{ArrFileID: 42, Path: libraryPath}},
		[]ManagedFile{{EntryID: "entry", EntryFolder: filepath.Base(dir), FileID: "file", FileName: "Movie.mkv"}},
	)
	if len(matches) != 0 {
		t.Fatalf("matches = %d, want 0", len(matches))
	}
}

func TestMatchLibraryFilesRequiresFolderAndFilename(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "managed", "Movie.Release", "movie.mkv")
	libraryPath := filepath.Join(dir, "library", "Movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{{ArrFileID: 42, Path: libraryPath}},
		[]ManagedFile{{EntryID: "entry", EntryFolder: "Other.Release", FileID: "file", FileName: "movie.mkv"}},
	)
	if len(matches) != 0 {
		t.Fatalf("matches = %d, want 0", len(matches))
	}
}

func TestMatchLibraryFilesRejectsAmbiguousManagedFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "managed", "Movie.Release", "movie.mkv")
	libraryPath := filepath.Join(dir, "library", "Movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{{ArrFileID: 42, Path: libraryPath}},
		[]ManagedFile{
			{EntryID: "entry-1", EntryFolder: "Movie.Release", FileID: "file-1", FileName: "movie.mkv"},
			{EntryID: "entry-2", EntryFolder: "Movie.Release", FileID: "file-2", FileName: "movie.mkv"},
		},
	)
	if len(matches) != 0 {
		t.Fatalf("matches = %d, want 0", len(matches))
	}
}

func TestMatchLibraryFilesRejectsDuplicateArrTargets(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "managed", "Movie.Release", "movie.mkv")
	libraryDir := filepath.Join(dir, "library")
	if err := os.MkdirAll(libraryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	libraryPaths := []string{
		filepath.Join(libraryDir, "Movie.mkv"),
		filepath.Join(libraryDir, "Movie copy.mkv"),
	}
	for _, path := range libraryPaths {
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
	}

	matches := matchLibraryFiles(
		[]arr.LibraryFile{
			{ArrFileID: 42, Path: libraryPaths[0]},
			{ArrFileID: 43, Path: libraryPaths[1]},
		},
		[]ManagedFile{{EntryID: "entry", EntryFolder: "Movie.Release", FileID: "file", FileName: "movie.mkv"}},
	)
	if len(matches) != 0 {
		t.Fatalf("matches = %d, want 0", len(matches))
	}
}
