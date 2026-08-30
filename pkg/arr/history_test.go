package arr

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFindGrabHistoryID(t *testing.T) {
	for _, test := range []struct {
		name     string
		arrType  Type
		queryKey string
		mediaID  int
	}{
		{name: "sonarr episode", arrType: Sonarr, queryKey: "episodeId", mediaID: 41},
		{name: "radarr movie", arrType: Radarr, queryKey: "movieIds", mediaID: 73},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v3/history" {
					t.Errorf("path = %q", r.URL.Path)
				}
				if got := r.URL.Query().Get(test.queryKey); got != fmt.Sprint(test.mediaID) {
					t.Errorf("%s = %q", test.queryKey, got)
				}
				if got := r.Header.Get("X-Api-Key"); got != "arr-secret" {
					t.Errorf("API key = %q", got)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":1,"records":[{"id":99,"downloadId":"download-1","eventType":"grabbed"}]}`)
			}))
			defer server.Close()

			a := &Arr{Host: server.URL, Token: "arr-secret", Type: test.arrType}
			id, downloadID, err := a.FindGrabHistoryID(test.mediaID)
			if err != nil {
				t.Fatal(err)
			}
			if id != 99 || downloadID != "download-1" {
				t.Fatalf("history = (%d, %q), want (99, %q)", id, downloadID, "download-1")
			}
		})
	}
}

func TestFindGrabHistoryIDReturnsEmptyWhenNoGrabExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":0,"records":[]}`)
	}))
	defer server.Close()

	a := &Arr{Host: server.URL, Token: "secret", Type: Sonarr}
	id, downloadID, err := a.FindGrabHistoryID(3)
	if err != nil || id != 0 || downloadID != "" {
		t.Fatalf("history = (%d, %q, %v), want empty", id, downloadID, err)
	}
}
