package arr

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHistoryByDownloadIDPaginatesAndFiltersEvent(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v3/history" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("downloadId"); got != "download-1" {
			t.Errorf("downloadId = %q", got)
		}
		if got := r.Header.Get("X-Api-Key"); got != "secret" {
			t.Errorf("API key = %q", got)
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "1":
			_, _ = fmt.Fprint(w, `{"page":1,"pageSize":1,"totalRecords":2,"records":[{"id":21,"downloadId":"download-1","eventType":"grabbed"}]}`)
		case "2":
			_, _ = fmt.Fprint(w, `{"page":2,"pageSize":1,"totalRecords":2,"records":[{"id":22,"downloadId":"download-1","eventType":"downloadFailed"}]}`)
		default:
			t.Errorf("unexpected page %q", r.URL.Query().Get("page"))
		}
	}))
	defer server.Close()

	a := &Arr{Host: server.URL, Token: "secret", Type: Sonarr}
	record, found, err := a.FindGrabHistoryByDownloadID(t.Context(), "download-1")
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.ID != 21 {
		t.Fatalf("record = %#v, found = %v", record, found)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestMarkHistoryFailedCtxValidatesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v3/history/failed/42" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	a := &Arr{Host: server.URL, Token: "secret", Type: Radarr}
	if err := a.MarkHistoryFailedCtx(t.Context(), 42); err == nil {
		t.Fatal("expected non-success status error")
	}
}

func TestImportHistoryRequestsImportEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("eventType"); got != "3" {
			t.Errorf("eventType = %q, want 3", got)
		}
		_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":1,"records":[{"id":8,"downloadId":"download-1","eventType":"downloadFolderImported","data":{"droppedPath":"/downloads/movie.mkv"}}]}`)
	}))
	defer server.Close()

	a := &Arr{Host: server.URL, Token: "secret", Type: Radarr}
	records, err := a.ImportHistory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Data["droppedPath"] != "/downloads/movie.mkv" {
		t.Fatalf("records = %#v", records)
	}
}
