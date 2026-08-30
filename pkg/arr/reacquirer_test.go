package arr

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
)

type recordedProgress struct {
	statuses []ReacquireStatus
}

func (progress *recordedProgress) Update(status ReacquireStatus, mutate func(*ReacquireJob)) error {
	progress.statuses = append(progress.statuses, status)
	return nil
}

func (progress *recordedProgress) UpdateDurable(status ReacquireStatus, mutate func(*ReacquireJob)) error {
	return progress.Update(status, mutate)
}

func TestReacquireHandlerFailsExactDownloadAndWaitsForArr(t *testing.T) {
	configureArrHTTPTest(t)
	var historyCalls atomic.Int64
	var failed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v3/moviefile/42":
			_, _ = fmt.Fprint(w, `{"id":42,"movieId":9,"path":"/library/movie.mkv"}`)
		case request.Method == http.MethodDelete && request.URL.Path == "/api/v3/moviefile/42":
			w.WriteHeader(http.StatusAccepted)
		case request.Method == http.MethodGet && request.URL.Path == "/api/v3/config/downloadclient":
			_, _ = fmt.Fprint(w, `{"enableCompletedDownloadHandling":true,"autoRedownloadFailed":true}`)
		case request.Method == http.MethodGet && request.URL.Path == "/api/v3/history":
			call := historyCalls.Add(1)
			if call == 1 {
				_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":0,"records":[]}`)
			} else {
				_, _ = fmt.Fprint(w, `{"page":1,"totalRecords":1,"records":[{"id":7,"downloadId":"download-1","eventType":"grabbed"}]}`)
			}
		case request.Method == http.MethodPost && request.URL.Path == "/api/v3/history/failed/7":
			failed.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	registry := newTestArrStorage()
	instance := &Arr{Name: "movies", Host: server.URL, Token: "secret", Type: Radarr}
	registry.AddOrUpdate(instance)
	handler := NewReacquireHandler(registry)
	progress := &recordedProgress{}
	job := ReacquireJob{
		ArrName:    "movies",
		ArrType:    Radarr,
		EntryID:    "entry-1",
		FileID:     "file-1",
		DownloadID: "download-1",
		Strategy:   ReacquireStrategyHistoryFailed,
		Bindings: []Binding{{
			ArrName:                "movies",
			ArrType:                Radarr,
			ArrInstanceFingerprint: instance.InstanceFingerprint(),
			EntryID:                "entry-1",
			EntryFileID:            "file-1",
			DownloadID:             "download-1",
			ArrFileID:              42,
			LibraryPath:            "/library/movie.mkv",
			MovieID:                9,
			Confidence:             BindingConfidenceExactPath,
		}},
	}
	if err := handler.Reacquire(t.Context(), job, progress); err != nil {
		t.Fatal(err)
	}
	if !failed.Load() {
		t.Fatal("history record was not marked failed")
	}
	if got := progress.statuses[len(progress.statuses)-1]; got != ReacquireStatusWaitingForGrab {
		t.Fatalf("last status = %q, want %q", got, ReacquireStatusWaitingForGrab)
	}
}

func TestReacquireHandlerRefusesStaleArrFileIdentity(t *testing.T) {
	configureArrHTTPTest(t)
	var deleted atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v3/moviefile/42":
			_, _ = fmt.Fprint(w, `{"id":42,"movieId":9,"path":"/library/other.mkv"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/api/v3/config/downloadclient":
			_, _ = fmt.Fprint(w, `{"enableCompletedDownloadHandling":true}`)
		case request.Method == http.MethodDelete && request.URL.Path == "/api/v3/moviefile/42":
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	registry := newTestArrStorage()
	instance := &Arr{Name: "movies", Host: server.URL, Token: "secret", Type: Radarr}
	registry.AddOrUpdate(instance)
	job := ReacquireJob{
		ArrName:  "movies",
		ArrType:  Radarr,
		EntryID:  "entry-1",
		FileID:   "file-1",
		Strategy: ReacquireStrategyCommandSearch,
		Bindings: []Binding{{
			ArrName:                "movies",
			ArrType:                Radarr,
			ArrInstanceFingerprint: instance.InstanceFingerprint(),
			EntryID:                "entry-1",
			EntryFileID:            "file-1",
			ArrFileID:              42,
			LibraryPath:            "/library/movie.mkv",
			MovieID:                9,
			Confidence:             BindingConfidenceExactPath,
		}},
	}
	err := NewReacquireHandler(registry).Reacquire(t.Context(), job, &recordedProgress{})
	if err == nil {
		t.Fatal("expected stale Arr identity to be rejected")
	}
	if deleted.Load() {
		t.Fatal("stale Arr file was deleted")
	}
}

func TestReacquireHandlerRefusesChangedArrInstance(t *testing.T) {
	configureArrHTTPTest(t)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		http.NotFound(w, request)
	}))
	defer server.Close()

	registry := newTestArrStorage()
	registry.AddOrUpdate(&Arr{Name: "movies", Host: server.URL, Token: "secret", Type: Radarr})
	job := ReacquireJob{
		ArrName:  "movies",
		ArrType:  Radarr,
		EntryID:  "entry-1",
		FileID:   "file-1",
		Strategy: ReacquireStrategyCommandSearch,
		Bindings: []Binding{{
			ArrName:                "movies",
			ArrType:                Radarr,
			ArrInstanceFingerprint: "v1:old-instance",
			EntryID:                "entry-1",
			EntryFileID:            "file-1",
			ArrFileID:              42,
			LibraryPath:            "/library/movie.mkv",
			MovieID:                9,
			Confidence:             BindingConfidenceExactPath,
		}},
	}
	err := NewReacquireHandler(registry).Reacquire(t.Context(), job, &recordedProgress{})
	if err == nil {
		t.Fatal("expected changed Arr instance to be rejected")
	}
	if requests.Load() != 0 {
		t.Fatalf("changed Arr instance received %d requests", requests.Load())
	}
}

func TestAutoRedownloadsFailureHonorsInteractiveSourceConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		config DownloadClientConfig
		source string
		want   bool
	}{
		{name: "master disabled", config: DownloadClientConfig{AutoRedownloadFailedFromInteractiveSearch: true}, source: "InteractiveSearch"},
		{name: "automatic source", config: DownloadClientConfig{AutoRedownloadFailed: true}, source: "Search", want: true},
		{name: "missing source", config: DownloadClientConfig{AutoRedownloadFailed: true}, want: true},
		{name: "interactive disabled", config: DownloadClientConfig{AutoRedownloadFailed: true}, source: "InteractiveSearch"},
		{name: "interactive enabled", config: DownloadClientConfig{AutoRedownloadFailed: true, AutoRedownloadFailedFromInteractiveSearch: true}, source: "InteractiveSearch", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := exactDownloadFailure{grabRecord: HistoryRecord{Data: map[string]string{"releaseSource": test.source}}}
			if got := autoRedownloadsFailure(test.config, failure); got != test.want {
				t.Fatalf("autoRedownloadsFailure() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSearchBindingsReconcilesPersistedCommandWithoutRedispatch(t *testing.T) {
	dispatched := atomic.Int64{}
	queued := time.Now().UTC().Add(-time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			_, _ = fmt.Fprintf(w, `[{"id":71,"name":"MoviesSearch","queued":%q,"body":{"name":"MoviesSearch","movieIds":[9]}}]`, queued.Format(time.RFC3339Nano))
		case http.MethodPost:
			dispatched.Add(1)
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	mutation := ReacquireMutation{
		Key:              mutationKey(ReacquireMutationMovieSearch, "9"),
		Kind:             ReacquireMutationMovieSearch,
		State:            ReacquireMutationIntent,
		CommandName:      "MoviesSearch",
		MovieIDs:         []int{9},
		IntentAt:         queued.Add(-time.Second),
		LastDispatchedAt: queued,
		Attempts:         1,
	}
	job := ReacquireJob{Mutations: []ReacquireMutation{mutation}}
	status, err := searchBindings(
		t.Context(),
		&Arr{Host: server.URL, Token: "secret", Type: Radarr},
		&job,
		[]Binding{{ArrType: Radarr, MovieID: 9}},
		&recordedProgress{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if status != ReacquireStatusWaitingForGrab {
		t.Fatalf("status = %q", status)
	}
	if dispatched.Load() != 0 {
		t.Fatalf("search was redispatched %d times", dispatched.Load())
	}
	confirmed, ok := mutationForKey(job, mutation.Key)
	if !ok || confirmed.State != ReacquireMutationConfirmed || confirmed.ReceiptID != 71 {
		t.Fatalf("confirmed mutation = %#v, found = %v", confirmed, ok)
	}
}

func TestSearchBindingsWaitsForCommandVisibilityBeforeRedispatch(t *testing.T) {
	dispatched := atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			dispatched.Add(1)
		}
		_, _ = fmt.Fprint(w, `[]`)
	}))
	defer server.Close()

	now := time.Now().UTC()
	mutation := ReacquireMutation{
		Key:              mutationKey(ReacquireMutationMovieSearch, "9"),
		Kind:             ReacquireMutationMovieSearch,
		State:            ReacquireMutationIntent,
		CommandName:      "MoviesSearch",
		MovieIDs:         []int{9},
		IntentAt:         now.Add(-time.Second),
		LastDispatchedAt: now,
		Attempts:         1,
	}
	job := ReacquireJob{Mutations: []ReacquireMutation{mutation}}
	_, err := searchBindings(
		t.Context(),
		&Arr{Host: server.URL, Token: "secret", Type: Radarr},
		&job,
		[]Binding{{ArrType: Radarr, MovieID: 9}},
		&recordedProgress{},
	)
	if !errors.Is(err, errMutationOutcomeUnknown) {
		t.Fatalf("error = %v, want unknown mutation outcome", err)
	}
	if dispatched.Load() != 0 {
		t.Fatalf("search was redispatched %d times", dispatched.Load())
	}
}

func TestPreexistingFailedHistoryUsesExplicitSearch(t *testing.T) {
	searches := atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/v3/command" {
			http.NotFound(w, request)
			return
		}
		searches.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"id":72,"name":"MoviesSearch","status":"queued"}`)
	}))
	defer server.Close()

	job := ReacquireJob{}
	status, err := (&reacquireHandler{}).failHistory(
		t.Context(),
		&Arr{Host: server.URL, Token: "secret", Type: Radarr},
		&job,
		[]Binding{{ArrType: Radarr, MovieID: 9}},
		exactDownloadFailure{found: true, alreadyFailed: true, downloadID: "download-1", failedID: 18},
		DownloadClientConfig{AutoRedownloadFailed: true},
		&recordedProgress{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if status != ReacquireStatusWaitingForGrab || searches.Load() != 1 {
		t.Fatalf("status = %q, searches = %d", status, searches.Load())
	}
}

func newTestArrStorage() *Storage {
	return &Storage{arrs: xsync.NewMap[string, *Arr]()}
}

func configureArrHTTPTest(t *testing.T) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
}
