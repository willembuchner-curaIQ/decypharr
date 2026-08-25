package arr

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFindGrabHistoryReturnsFullRecordAndPreservesLegacyAPI(t *testing.T) {
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
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.URL.Path != "/api/v3/history" {
					t.Errorf("path=%q", r.URL.Path)
				}
				if got := r.URL.Query().Get(test.queryKey); got != fmt.Sprint(test.mediaID) {
					t.Errorf("%s=%q", test.queryKey, got)
				}
				if got := r.Header.Get("X-Api-Key"); got != "arr-secret" {
					t.Errorf("API key=%q", got)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"page":1,"totalRecords":1,"records":[{"id":99,"downloadId":"download-1","eventType":"grabbed","sourceTitle":"Release.Title","data":{"guid":"guid-1","nzbInfoUrl":"https://indexer.invalid/details/1","indexer":"Indexer"}}]}`)
			}))
			defer server.Close()

			a := &Arr{Host: server.URL, Token: "arr-secret", Type: test.arrType}
			record, err := a.FindGrabHistoryCtx(context.Background(), test.mediaID)
			if err != nil {
				t.Fatal(err)
			}
			if record == nil || record.ID != 99 || record.SourceTitle != "Release.Title" || record.Data["guid"] != "guid-1" {
				t.Fatalf("record=%+v", record)
			}
			id, downloadID, err := a.FindGrabHistoryID(test.mediaID)
			if err != nil || id != 99 || downloadID != "download-1" {
				t.Fatalf("legacy API=(%d,%q,%v)", id, downloadID, err)
			}
			if requests != 2 {
				t.Fatalf("requests=%d, want 2", requests)
			}
		})
	}
}

func TestFindGrabHistoryEmptyIsCompatible(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"page":1,"totalRecords":0,"records":[]}`)
	}))
	defer server.Close()
	a := &Arr{Host: server.URL, Token: "secret", Type: Sonarr}
	record, err := a.FindGrabHistory(3)
	if err != nil || record != nil {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	id, downloadID, err := a.FindGrabHistoryID(3)
	if err != nil || id != 0 || downloadID != "" {
		t.Fatalf("legacy empty=(%d,%q,%v)", id, downloadID, err)
	}
}
