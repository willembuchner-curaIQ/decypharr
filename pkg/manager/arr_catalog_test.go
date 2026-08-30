package manager

import (
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Entries synced from a provider carry no category. They are still symlinked
// into an Arr library, so the catalog must return their files.
func TestListManagedFilesIgnoresEntryCategory(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	entries := []*storage.Entry{
		{
			InfoHash: "aabbccddeeff00112233445566778899aabbccdd",
			Name:     "Movie.2023",
			Category: "radarr",
			Files:    map[string]*storage.File{"movie.mkv": {Name: "movie.mkv", Size: 100}},
		},
		{
			InfoHash: "00112233445566778899aabbccddeeff00112233",
			Name:     "Synced.2023",
			Files:    map[string]*storage.File{"synced.mkv": {Name: "synced.mkv", Size: 200}},
		},
	}
	for _, entry := range entries {
		if err := store.AddOrUpdate(entry); err != nil {
			t.Fatal(err)
		}
	}

	catalog := managedArrCatalog{storage: store}
	files, err := catalog.ListManagedFiles(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2: %#v", len(files), files)
	}

	one, err := catalog.ListManagedFiles(t.Context(), entries[1].InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].FileName != "synced.mkv" || one[0].FileSize != 200 {
		t.Fatalf("targeted files = %#v", one)
	}
}
