package arr

import (
	"context"
	"testing"
	"time"
)

const testArrInstanceFingerprint = "v1:test-instance"

func TestIndexReplacesOneArrGeneration(t *testing.T) {
	index := NewIndex()
	sonarr := Binding{
		ArrName:     "sonarr",
		ArrType:     Sonarr,
		EntryID:     "old-entry",
		EntryFileID: "old-file",
		DownloadID:  "old-download",
		ArrFileID:   11,
		EpisodeIDs:  []int{101, 102},
		Generation:  1,
	}
	radarr := Binding{
		ArrName:     "radarr",
		ArrType:     Radarr,
		EntryID:     "movie-entry",
		EntryFileID: "movie-file",
		DownloadID:  "movie-download",
		ArrFileID:   22,
		MovieID:     202,
		Generation:  1,
	}
	if err := index.Upsert(sonarr); err != nil {
		t.Fatal(err)
	}
	if err := index.Upsert(radarr); err != nil {
		t.Fatal(err)
	}

	replacement := sonarr
	replacement.EntryID = "new-entry"
	replacement.EntryFileID = "new-file"
	replacement.DownloadID = "new-download"
	replacement.ArrFileID = 12
	if err := index.ReplaceArrGeneration("sonarr", 2, []Binding{replacement}); err != nil {
		t.Fatal(err)
	}

	if _, ok := index.Lookup("old-entry", "old-file"); ok {
		t.Fatal("stale Sonarr binding remained indexed")
	}
	if bindings := index.ByEpisodeID("sonarr", 102); len(bindings) != 1 || bindings[0].Generation != 2 {
		t.Fatalf("episode reverse lookup = %#v", bindings)
	}
	if _, ok := index.Lookup("movie-entry", "movie-file"); !ok {
		t.Fatal("replacing Sonarr generation removed Radarr binding")
	}
}

type waitingReacquireHandler struct {
	called chan string
}

func (handler *waitingReacquireHandler) Reacquire(_ context.Context, job ReacquireJob, progress JobProgress) error {
	if err := progress.Update(ReacquireStatusWaitingForImport, nil); err != nil {
		return err
	}
	handler.called <- job.ID
	return nil
}

type delayedWaitingHandler struct {
	started chan struct{}
	release chan struct{}
}

type unknownMutationHandler struct{}

func (unknownMutationHandler) Reacquire(_ context.Context, _ ReacquireJob, progress JobProgress) error {
	now := time.Now().UTC()
	if err := progress.UpdateDurable(ReacquireStatusSearching, func(job *ReacquireJob) {
		setMutation(job, ReacquireMutation{
			Key:              "movie_search:7",
			Kind:             ReacquireMutationMovieSearch,
			State:            ReacquireMutationIntent,
			CommandName:      "MoviesSearch",
			MovieIDs:         []int{7},
			IntentAt:         now.Add(-time.Second),
			LastDispatchedAt: now,
			Attempts:         1,
		})
	}); err != nil {
		return err
	}
	return unknownMutationOutcome(context.DeadlineExceeded, time.Hour)
}

func (handler *delayedWaitingHandler) Reacquire(_ context.Context, _ ReacquireJob, progress JobProgress) error {
	close(handler.started)
	<-handler.release
	return progress.Update(ReacquireStatusWaitingForImport, nil)
}

func TestServiceDeduplicatesAndPersistsReacquireJobs(t *testing.T) {
	directory := t.TempDir()
	handler := &waitingReacquireHandler{called: make(chan string, 1)}
	service, err := NewService(ServiceOptions{Directory: directory, Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	broken := Binding{
		ArrName:                "radarr",
		ArrType:                Radarr,
		ArrInstanceFingerprint: testArrInstanceFingerprint,
		EntryID:                "broken-entry",
		EntryFileID:            "broken-file",
		DownloadID:             "broken-download",
		ArrFileID:              40,
		LibraryPath:            "/library/movie.mkv",
		MovieID:                7,
		Confidence:             BindingConfidenceExactPath,
	}
	if err := service.UpsertBinding(broken); err != nil {
		t.Fatal(err)
	}
	request := ReacquireRequest{EntryID: broken.EntryID, FileID: broken.EntryFileID, Cause: ReacquireCauseStream}
	first, err := service.Reacquire(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Reacquire(request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("duplicate requests created jobs %q and %q", first.ID, second.ID)
	}

	select {
	case id := <-handler.called:
		if id != first.ID {
			t.Fatalf("handler received job %q, want %q", id, first.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reacquire handler was not called")
	}
	job := waitForJobStatus(t, service, first.ID, ReacquireStatusWaitingForImport)
	if !job.CompletedAt.IsZero() {
		t.Fatal("waiting job was marked complete")
	}

	replacement := broken
	replacement.EntryID = "replacement-entry"
	replacement.EntryFileID = "replacement-file"
	replacement.DownloadID = "replacement-download"
	replacement.ArrFileID = 41
	if err := service.UpsertBinding(replacement); err != nil {
		t.Fatal(err)
	}
	job = waitForJobStatus(t, service, first.ID, ReacquireStatusReady)
	if job.ReplacementDownloadID != replacement.DownloadID {
		t.Fatalf("replacement download ID = %q", job.ReplacementDownloadID)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewService(ServiceOptions{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if persisted, ok := reopened.Job(first.ID); !ok || persisted.Status != ReacquireStatusReady {
		t.Fatalf("persisted job = %#v, found = %v", persisted, ok)
	}
	if _, ok := reopened.Lookup(replacement.EntryID, replacement.EntryFileID); !ok {
		t.Fatal("replacement binding was not restored")
	}
}

func TestServicePersistsUnknownMutationForRestartReconciliation(t *testing.T) {
	directory := t.TempDir()
	service, err := NewService(ServiceOptions{Directory: directory, Handler: unknownMutationHandler{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	binding := Binding{
		ArrName:                "radarr",
		ArrType:                Radarr,
		ArrInstanceFingerprint: testArrInstanceFingerprint,
		EntryID:                "entry",
		EntryFileID:            "file",
		DownloadID:             "download",
		ArrFileID:              7,
		LibraryPath:            "/library/movie.mkv",
		MovieID:                7,
		Confidence:             BindingConfidenceExactPath,
	}
	if err := service.UpsertBinding(binding); err != nil {
		t.Fatal(err)
	}
	created, err := service.Reacquire(ReacquireRequest{
		EntryID: binding.EntryID,
		FileID:  binding.EntryFileID,
		Cause:   ReacquireCauseStream,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var queued ReacquireJob
	for time.Now().Before(deadline) {
		queued, _ = service.Job(created.ID)
		if queued.Status == ReacquireStatusQueued && !queued.RetryAt.IsZero() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if queued.Status != ReacquireStatusQueued || queued.RetryAt.IsZero() || len(queued.Mutations) != 1 {
		t.Fatalf("queued job = %#v", queued)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewService(ServiceOptions{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, ok := reopened.Job(created.ID)
	if !ok || persisted.Status != ReacquireStatusQueued || persisted.RetryAt.IsZero() || len(persisted.Mutations) != 1 {
		t.Fatalf("persisted job = %#v, found = %v", persisted, ok)
	}
}

func TestWaitingTransitionFindsExistingCompleteEpisodeReplacement(t *testing.T) {
	handler := &delayedWaitingHandler{started: make(chan struct{}), release: make(chan struct{})}
	service, err := NewService(ServiceOptions{Directory: t.TempDir(), Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	broken := Binding{
		ArrName:                "sonarr",
		ArrType:                Sonarr,
		ArrInstanceFingerprint: testArrInstanceFingerprint,
		EntryID:                "broken-entry",
		EntryFileID:            "broken-file",
		DownloadID:             "broken-download",
		ArrFileID:              40,
		LibraryPath:            "/library/episode.mkv",
		SeriesID:               7,
		EpisodeIDs:             []int{101, 102},
		Confidence:             BindingConfidenceExactPath,
	}
	if err := service.UpsertBinding(broken); err != nil {
		t.Fatal(err)
	}
	sibling := broken
	sibling.EntryID = "broken-entry-2"
	sibling.EntryFileID = "broken-file-2"
	sibling.ArrFileID = 43
	sibling.EpisodeIDs = []int{103}
	if err := service.UpsertBinding(sibling); err != nil {
		t.Fatal(err)
	}
	job, err := service.Reacquire(ReacquireRequest{
		EntryID: broken.EntryID,
		FileID:  broken.EntryFileID,
		Cause:   ReacquireCauseStream,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handler.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	first := broken
	first.EntryID = "replacement-entry-1"
	first.EntryFileID = "replacement-file-1"
	first.DownloadID = "replacement-download"
	first.ArrFileID = 41
	first.EpisodeIDs = []int{101}
	if err := service.UpsertBinding(first); err != nil {
		t.Fatal(err)
	}
	close(handler.release)
	waitForJobStatus(t, service, job.ID, ReacquireStatusWaitingForImport)

	second := first
	second.EntryID = "replacement-entry-2"
	second.EntryFileID = "replacement-file-2"
	second.ArrFileID = 42
	second.EpisodeIDs = []int{102}
	if err := service.UpsertBinding(second); err != nil {
		t.Fatal(err)
	}
	waitForJobStatus(t, service, job.ID, ReacquireStatusWaitingForImport)

	third := first
	third.EntryID = "replacement-entry-3"
	third.EntryFileID = "replacement-file-3"
	third.ArrFileID = 44
	third.EpisodeIDs = []int{103}
	if err := service.UpsertBinding(third); err != nil {
		t.Fatal(err)
	}
	ready := waitForJobStatus(t, service, job.ID, ReacquireStatusReady)
	if ready.ReplacementDownloadID != "replacement-download" {
		t.Fatalf("replacement download ID = %q", ready.ReplacementDownloadID)
	}
}

func TestWaitingJobExpiresAndReleasesDeduplicationKey(t *testing.T) {
	handler := &waitingReacquireHandler{called: make(chan string, 2)}
	service, err := NewService(ServiceOptions{Directory: t.TempDir(), Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	base := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return base }
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	binding := Binding{
		ArrName:                "radarr",
		ArrType:                Radarr,
		ArrInstanceFingerprint: testArrInstanceFingerprint,
		EntryID:                "entry",
		EntryFileID:            "file",
		DownloadID:             "download",
		ArrFileID:              7,
		LibraryPath:            "/library/movie.mkv",
		MovieID:                8,
		Confidence:             BindingConfidenceExactPath,
	}
	if err := service.UpsertBinding(binding); err != nil {
		t.Fatal(err)
	}
	first, err := service.Reacquire(ReacquireRequest{EntryID: "entry", FileID: "file", Cause: ReacquireCauseRepair})
	if err != nil {
		t.Fatal(err)
	}
	waitForJobStatus(t, service, first.ID, ReacquireStatusWaitingForImport)

	service.now = func() time.Time { return base.Add(reacquireWaitingTimeout + time.Minute) }
	service.maintainJobs()
	waitForJobStatus(t, service, first.ID, ReacquireStatusFailed)
	second, err := service.Reacquire(ReacquireRequest{EntryID: "entry", FileID: "file", Cause: ReacquireCauseRepair})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.ID {
		t.Fatal("expired job retained the active deduplication key")
	}
}

func waitForJobStatus(t *testing.T, service *Service, id string, status ReacquireStatus) ReacquireJob {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, ok := service.Job(id)
		if ok && job.Status == status {
			return job
		}
		time.Sleep(time.Millisecond)
	}
	job, _ := service.Job(id)
	t.Fatalf("job status = %q, want %q", job.Status, status)
	return ReacquireJob{}
}
