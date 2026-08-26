package arr

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const testNZB = `<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"></nzb>`

func TestReacquireNZBMatchesStrongIdentifiersForSonarrAndRadarr(t *testing.T) {
	for _, test := range []struct {
		name          string
		arrType       Type
		historyData   string
		releaseFields string
		queryKey      string
	}{
		{name: "sonarr guid", arrType: Sonarr, historyData: `"guid":"strong-guid"`, releaseFields: `"guid":"strong-guid","infoUrl":"https://indexer.invalid/wrong"`, queryKey: "episodeId"},
		{name: "radarr nzb info URL", arrType: Radarr, historyData: `"nzbInfoUrl":"https://indexer.invalid/details/7"`, releaseFields: `"guid":"other","infoUrl":"https://indexer.invalid/details/7"`, queryKey: "movieId"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var downloadKey string
			download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				downloadKey = r.Header.Get("X-Api-Key")
				fmt.Fprint(w, testNZB)
			}))
			defer download.Close()

			arrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("X-Api-Key"); got != "arr-secret" {
					t.Errorf("Arr API key=%q", got)
				}
				switch r.URL.Path {
				case "/api/v3/history":
					fmt.Fprintf(w, `{"page":1,"records":[{"id":5,"eventType":"grabbed","sourceTitle":"Release.Title","data":{%s,"indexer":"Index"}}]}`, test.historyData)
				case "/api/v3/release":
					if got := r.URL.Query().Get(test.queryKey); got != "55" {
						t.Errorf("%s=%q", test.queryKey, got)
					}
					fmt.Fprintf(w, `[{"title":"Release.Title","indexer":"Index","downloadProtocol":"usenet","downloadUrl":%q,%s}]`, download.URL+"/metadata?indexer_key=private", test.releaseFields)
				default:
					http.NotFound(w, r)
				}
			}))
			defer arrServer.Close()

			a := &Arr{Host: arrServer.URL, Token: "arr-secret", Type: test.arrType}
			data, err := a.ReacquireNZB(t.Context(), 55)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != testNZB {
				t.Fatalf("NZB=%q", data)
			}
			if downloadKey != "" {
				t.Fatalf("Arr API key leaked cross-origin: %q", downloadKey)
			}
		})
	}
}

func TestMatchGrabbedReleaseRequiresUniqueStrongOrTitleIndexerMatch(t *testing.T) {
	releases := []ReleaseSearchRecord{
		{Title: "Same.Title", GUID: "one", Indexer: "First", DownloadProtocol: "usenet", DownloadURL: "/one.nzb"},
		{Title: "Same.Title", GUID: "two", Indexer: "Second", DownloadProtocol: "usenet", DownloadURL: "/two.nzb"},
		{Title: "Same.Title", GUID: "torrent", Indexer: "Second", DownloadProtocol: "torrent", DownloadURL: "/torrent"},
	}
	matched, err := matchGrabbedRelease(HistoryRecord{SourceTitle: "same.title", Data: map[string]string{"indexer": "second"}}, releases)
	if err != nil || matched.GUID != "two" {
		t.Fatalf("fallback match=%+v err=%v", matched, err)
	}

	duplicate := append(releases, ReleaseSearchRecord{Title: "same.title", GUID: "three", Indexer: "SECOND", Protocol: "usenet", DownloadURL: "/three.nzb"})
	_, err = matchGrabbedRelease(HistoryRecord{SourceTitle: "Same.Title", Data: map[string]string{"indexer": "Second"}}, duplicate)
	if !errors.Is(err, ErrReacquireAmbiguous) {
		t.Fatalf("fallback ambiguity error=%v", err)
	}

	strongDuplicate := []ReleaseSearchRecord{
		{GUID: "same-guid", DownloadProtocol: "usenet"},
		{GUID: "same-guid", DownloadProtocol: "usenet"},
	}
	_, err = matchGrabbedRelease(HistoryRecord{Data: map[string]string{"guid": "same-guid"}}, strongDuplicate)
	if !errors.Is(err, ErrNZBReacquireAmbiguous) {
		t.Fatalf("strong ambiguity error=%v", err)
	}

	_, err = matchGrabbedRelease(HistoryRecord{Data: map[string]string{"guid": "missing"}}, releases)
	if !errors.Is(err, ErrNZBReacquireNoMatch) {
		t.Fatalf("strong no-match error=%v", err)
	}
}

func TestReacquireNZBMissingHistoryReturnsStableNoMatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/history" {
			fmt.Fprint(w, `{"records":[]}`)
			return
		}
		t.Errorf("unexpected request %s", r.URL.Path)
	}))
	defer server.Close()
	a := &Arr{Host: server.URL, Token: "secret", Type: Sonarr}
	_, err := a.ReacquireNZB(t.Context(), 1)
	if !errors.Is(err, ErrReacquireNoMatch) {
		t.Fatalf("error=%v", err)
	}
}

func TestReacquireNZBByDownloadIDResolvesManagedCandidate(t *testing.T) {
	tests := []struct {
		name       string
		arrType    Type
		historyKey string
		queryKey   string
	}{
		{name: "sonarr", arrType: Sonarr, historyKey: "episodeId", queryKey: "episodeId"},
		{name: "radarr", arrType: Radarr, historyKey: "movieId", queryKey: "movieId"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v3/history":
					if got := r.URL.Query().Get("downloadId"); got != "nzo-123" {
						t.Errorf("downloadId = %q", got)
					}
					fmt.Fprintf(w, `{"records":[{"downloadId":"nzo-123",%q:55,"sourceTitle":"Release","data":{"guid":"guid-55"}}]}`, test.historyKey)
				case "/api/v3/release":
					if got := r.URL.Query().Get(test.queryKey); got != "55" {
						t.Errorf("%s = %q", test.queryKey, got)
					}
					fmt.Fprintf(w, `[{"guid":"guid-55","downloadProtocol":"usenet","downloadUrl":%q}]`, server.URL+"/metadata")
				case "/metadata":
					fmt.Fprint(w, testNZB)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			a := &Arr{Host: server.URL, Token: "secret", Type: test.arrType}
			data, err := a.ReacquireNZBByDownloadID(t.Context(), "nzo-123")
			if err != nil || string(data) != testNZB {
				t.Fatalf("NZB = %q, err = %v", data, err)
			}
		})
	}
}

func TestDownloadNZBMetadataStripsKeyAndRefererOnCrossOriginRedirect(t *testing.T) {
	var crossKey, crossReferer string
	cross := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		crossKey = r.Header.Get("X-Api-Key")
		crossReferer = r.Header.Get("Referer")
		fmt.Fprint(w, testNZB)
	}))
	defer cross.Close()

	var sameOriginKey string
	arrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sameOriginKey = r.Header.Get("X-Api-Key")
		http.Redirect(w, r, cross.URL+"/nzb?indexer_key=private", http.StatusFound)
	}))
	defer arrServer.Close()

	a := &Arr{Host: arrServer.URL, Token: "arr-secret", Type: Sonarr}
	data, err := a.downloadNZBMetadata(t.Context(), "/proxy?arr_internal=private")
	if err != nil || string(data) != testNZB {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if sameOriginKey != "arr-secret" {
		t.Fatalf("same-origin key=%q", sameOriginKey)
	}
	if crossKey != "" || crossReferer != "" {
		t.Fatalf("redirect leaked headers: key=%q referer=%q", crossKey, crossReferer)
	}
}

func TestDownloadNZBMetadataEnforcesHardBoundAndValidatesRoot(t *testing.T) {
	t.Run("declared oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.FormatInt(MaxReacquiredNZBBytes+1, 10))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		a := &Arr{Host: server.URL, Token: "secret"}
		_, err := a.downloadNZBMetadata(t.Context(), server.URL+"/oversized")
		if !errors.Is(err, ErrNZBMetadataTooLarge) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("not NZB", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `<html>indexer login</html>`)
		}))
		defer server.Close()
		a := &Arr{Host: server.URL, Token: "secret"}
		_, err := a.downloadNZBMetadata(t.Context(), server.URL+"/not-nzb")
		if !errors.Is(err, ErrInvalidNZBMetadata) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestDownloadNZBMetadataErrorsNeverRenderSecretURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer server.Close()
	a := &Arr{Host: server.URL, Token: "arr-super-secret"}
	_, err := a.downloadNZBMetadata(t.Context(), server.URL+"/get?indexer_api_key=do-not-log")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, secret := range []string{"do-not-log", "arr-super-secret", "/get?"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %v", secret, err)
		}
	}
}

func TestSearchCurrentReleasesDecodesNestedReleaseShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("episodeId"); got != "8" {
			t.Errorf("episodeId=%q", got)
		}
		fmt.Fprint(w, `[{"release":{"title":"Nested","guid":"nested-guid","downloadUrl":"/nested.nzb","downloadProtocol":"usenet","indexer":"Nested Indexer"}}]`)
	}))
	defer server.Close()
	a := &Arr{Host: server.URL, Token: "secret", Type: Sonarr}
	releases, err := a.SearchCurrentReleases(t.Context(), 8)
	if err != nil || len(releases) != 1 {
		t.Fatalf("releases=%+v err=%v", releases, err)
	}
	if got := releases[0].info(); got.GUID != "nested-guid" || got.DownloadURL != "/nested.nzb" {
		t.Fatalf("normalized release=%+v", got)
	}
}
