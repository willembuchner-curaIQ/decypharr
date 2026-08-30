package arr

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/strm"
)

func TestMatchLibraryFilesRequiresExactIdentity(t *testing.T) {
	dir := t.TempDir()
	managedPath := filepath.Join(dir, "downloads", "movie.mkv")
	if err := os.MkdirAll(filepath.Dir(managedPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(managedPath, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}

	libraryPath := filepath.Join(dir, "library", "movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(managedPath, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]LibraryFile{{ArrFileID: 42, Path: libraryPath, Size: 5}},
		[]ManagedFile{{EntryID: "entry", FileID: "file", Path: managedPath, Size: 5}},
	)
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(matches))
	}
	if matches[0].managed.FileID != "file" {
		t.Fatalf("matched file ID = %q, want file", matches[0].managed.FileID)
	}
}

func TestHistoryBindingCannotReplaceExactCurrentArrFile(t *testing.T) {
	instance := &Arr{Name: "movies", Type: Radarr}
	library := []LibraryFile{{ArrFileID: 42, Path: "/library/movie.mkv", MovieID: 9}}
	managed := []ManagedFile{
		{EntryID: "current-entry", FileID: "current-file", DownloadID: "current-download", Path: "/downloads/current/movie.mkv"},
		{EntryID: "old-entry", FileID: "old-file", DownloadID: "old-download", Path: "/downloads/old/movie.mkv"},
	}
	exact := []Binding{{
		ArrName:     "movies",
		ArrType:     Radarr,
		EntryID:     "current-entry",
		EntryFileID: "current-file",
		DownloadID:  "current-download",
		ArrFileID:   42,
		MovieID:     9,
		Confidence:  BindingConfidenceExactPath,
	}}
	history := []HistoryRecord{{
		DownloadID: "old-download",
		EventType:  "downloadFolderImported",
		MovieID:    9,
		Date:       time.Now(),
		Data: map[string]string{
			"droppedPath":  "/downloads/old/movie.mkv",
			"importedPath": "/library/movie.mkv",
		},
	}}

	bindings := bindingsFromHistoryRecords(instance, 2, library, managed, exact, history)
	if len(bindings) != 0 {
		t.Fatalf("history bindings = %#v, want no collision with exact current binding", bindings)
	}
}

func TestLatestUnmanagedImportBlocksOlderHistoryBinding(t *testing.T) {
	instance := &Arr{Name: "movies", Type: Radarr}
	library := []LibraryFile{{ArrFileID: 42, Path: "/library/movie.mkv", MovieID: 9}}
	managed := []ManagedFile{{
		EntryID: "old-entry", FileID: "old-file", DownloadID: "old-download", Path: "/downloads/old/movie.mkv",
	}}
	history := []HistoryRecord{
		{
			DownloadID: "external-download",
			EventType:  "downloadFolderImported",
			MovieID:    9,
			Date:       time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC),
			Data: map[string]string{
				"droppedPath":  "/external/movie.mkv",
				"importedPath": "/library/movie.mkv",
			},
		},
		{
			DownloadID: "old-download",
			EventType:  "downloadFolderImported",
			MovieID:    9,
			Date:       time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC),
			Data: map[string]string{
				"droppedPath":  "/downloads/old/movie.mkv",
				"importedPath": "/library/movie.mkv",
			},
		},
	}

	bindings := bindingsFromHistoryRecords(instance, 2, library, managed, nil, history)
	if len(bindings) != 0 {
		t.Fatalf("history bindings = %#v, want latest current import to consume the Arr file", bindings)
	}
}

func TestMatchLibraryFilesRejectsAmbiguousManagedPath(t *testing.T) {
	dir := t.TempDir()
	managedPath := filepath.Join(dir, "downloads", "movie.mkv")
	if err := os.MkdirAll(filepath.Dir(managedPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(managedPath, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	libraryPath := filepath.Join(dir, "library", "movie.mkv")
	if err := os.MkdirAll(filepath.Dir(libraryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(managedPath, libraryPath); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]LibraryFile{{ArrFileID: 42, Path: libraryPath, Size: 5}},
		[]ManagedFile{
			{EntryID: "entry-1", FileID: "file-1", Path: managedPath, Size: 5},
			{EntryID: "entry-2", FileID: "file-2", Path: managedPath, Size: 5},
		},
	)
	if len(matches) != 0 {
		t.Fatalf("matches = %d, want none for an ambiguous managed path", len(matches))
	}
}

func TestBindingsFromHistoryUsesExactDroppedPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v3/history" || request.URL.Query().Get("downloadId") != "download-1" {
			http.NotFound(w, request)
			return
		}
		_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":1,"records":[{"id":7,"downloadId":"download-1","eventType":"downloadFolderImported","movieId":9,"data":{"droppedPath":"/downloads/movie.mkv","importedPath":"/library/Movie (2025)/Movie.mkv"}}]}`)
	}))
	defer server.Close()

	instance := &Arr{Name: "movies", Host: server.URL, Token: "secret", Type: Radarr}
	library := []LibraryFile{{ArrFileID: 42, Path: "/library/Movie (2025)/Movie.mkv", MovieID: 9}}
	managed := []ManagedFile{{EntryID: "entry", FileID: "file", DownloadID: "download-1", Path: "/downloads/movie.mkv"}}
	records, err := (&Indexer{}).historyForManaged(t.Context(), instance, managed, nil)
	if err != nil {
		t.Fatal(err)
	}
	bindings := bindingsFromHistoryRecords(instance, 3, library, managed, nil, records)
	if len(bindings) != 1 || bindings[0].Confidence != BindingConfidenceDownloadHistory {
		t.Fatalf("bindings = %#v", bindings)
	}
	if bindings[0].ArrInstanceFingerprint != instance.InstanceFingerprint() {
		t.Fatal("history binding did not capture the current Arr instance fingerprint")
	}
}

func TestCarryForwardBindingRequiresCurrentInstanceAndPath(t *testing.T) {
	instance := &Arr{Name: "movies", Host: "http://radarr.example", Type: Radarr}
	library := []LibraryFile{{ArrFileID: 42, Path: "/library/movie.mkv", MovieID: 9}}
	managed := []ManagedFile{{EntryID: "entry", FileID: "file", Path: "/downloads/movie.mkv"}}
	old := Binding{
		ArrName:     "movies",
		ArrType:     Radarr,
		EntryID:     "entry",
		EntryFileID: "file",
		ArrFileID:   42,
		LibraryPath: "/library/movie.mkv",
		MovieID:     9,
		Confidence:  BindingConfidenceExactPath,
	}

	if bindings := carryForwardBindings(instance, 2, nil, []Binding{old}, library, managed); len(bindings) != 0 {
		t.Fatal("legacy binding was carried forward without a fresh identity match")
	}
	old.ArrInstanceFingerprint = instance.InstanceFingerprint()
	old.LibraryPath = "/library/old.mkv"
	if bindings := carryForwardBindings(instance, 2, nil, []Binding{old}, library, managed); len(bindings) != 0 {
		t.Fatal("binding was carried forward after its library path changed")
	}
	old.LibraryPath = library[0].Path
	bindings := carryForwardBindings(instance, 2, nil, []Binding{old}, library, managed)
	if len(bindings) != 1 || bindings[0].ArrInstanceFingerprint != instance.InstanceFingerprint() {
		t.Fatalf("current binding was not carried forward: %#v", bindings)
	}
}

func TestMatchLibraryFilesBindsStrmByIdentity(t *testing.T) {
	dir := t.TempDir()
	const (
		entryID = "0123456789abcdef0123456789abcdef"
		fileID  = "fedcba9876543210fedcba9876543210"
	)
	libraryPath := filepath.Join(dir, "Movie (2026).strm")
	url := strm.FileURL("http://decypharr.test", "secret", entryID, fileID, "Movie.mkv")
	if err := os.WriteFile(libraryPath, []byte(url), 0o644); err != nil {
		t.Fatal(err)
	}
	foreignPath := filepath.Join(dir, "Foreign.strm")
	if err := os.WriteFile(foreignPath, []byte("http://example.com/other.mkv"), 0o644); err != nil {
		t.Fatal(err)
	}

	matches := matchLibraryFiles(
		[]LibraryFile{
			{ArrFileID: 42, Path: libraryPath, Size: 120},
			{ArrFileID: 43, Path: foreignPath, Size: 120},
		},
		[]ManagedFile{{EntryID: entryID, FileID: fileID, Path: "/downloads/Movie.mkv", Size: 1 << 30}},
	)
	if len(matches) != 1 || matches[0].library.ArrFileID != 42 {
		t.Fatalf("matches = %#v, want only the Decypharr .strm", matches)
	}
	if matches[0].managed.EntryID != entryID || matches[0].managed.FileID != fileID {
		t.Fatalf("managed identity = %#v", matches[0].managed)
	}
}
