package repair

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

func TestHydrateLegacyNZBFromSourcesPrefersLocalSource(t *testing.T) {
	local := []byte("local NZB")
	reacquired := atomic.Bool{}
	hydrated := atomic.Bool{}

	err := hydrateLegacyNZBFromSources(
		t.Context(),
		"nzb-id",
		func(nzbID string) (string, []byte, error) {
			if nzbID != "nzb-id" {
				t.Fatalf("load NZB ID = %q", nzbID)
			}
			return "original.nzb", local, nil
		},
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return []byte("remote NZB"), nil
		},
		func(_ context.Context, nzbID, sourceName string, content []byte) error {
			hydrated.Store(true)
			if nzbID != "nzb-id" || sourceName != "original.nzb" || string(content) != string(local) {
				t.Fatalf("hydrate(%q, %q, %q)", nzbID, sourceName, content)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reacquired.Load() {
		t.Fatal("Arr reacquisition ran despite a usable local source")
	}
	if !hydrated.Load() {
		t.Fatal("local source was not hydrated")
	}
}

func TestHydrateLegacyNZBFromSourcesFallsBackToArr(t *testing.T) {
	remote := []byte("remote NZB")
	reacquired := atomic.Bool{}

	err := hydrateLegacyNZBFromSources(
		t.Context(),
		"nzb-id",
		func(string) (string, []byte, error) {
			return "", nil, usenetpkg.ErrLegacyNZBSourceUnavailable
		},
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return remote, nil
		},
		func(_ context.Context, nzbID, sourceName string, content []byte) error {
			if nzbID != "nzb-id" || sourceName != "nzb-id.nzb" || string(content) != string(remote) {
				t.Fatalf("hydrate(%q, %q, %q)", nzbID, sourceName, content)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reacquired.Load() {
		t.Fatal("Arr reacquisition did not run after local source unavailability")
	}
}

func TestHydrateLegacyNZBFromSourcesPreservesUnexpectedLocalError(t *testing.T) {
	loadErr := errors.New("local read failed")
	reacquired := atomic.Bool{}
	err := hydrateLegacyNZBFromSources(
		t.Context(),
		"nzb-id",
		func(string) (string, []byte, error) { return "", nil, loadErr },
		func(context.Context) ([]byte, error) {
			reacquired.Store(true)
			return nil, nil
		},
		func(context.Context, string, string, []byte) error { return nil },
	)
	if !errors.Is(err, loadErr) {
		t.Fatalf("error = %v, want wrapped local error", err)
	}
	if reacquired.Load() {
		t.Fatal("Arr reacquisition ran after an unexpected local loader error")
	}
}

func TestLegacyNZBHydrationFailureClassification(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		permanent  bool
		arrBackoff bool
	}{
		{name: "cancelled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "unknown operational error", err: errors.New("local storage unavailable")},
		{name: "upstream 502", err: &arr.NZBMetadataError{Stage: "request", Status: http.StatusBadGateway}, arrBackoff: true},
		{name: "throttled", err: &arr.NZBMetadataError{Stage: "request", Status: http.StatusTooManyRequests}, arrBackoff: true},
		{name: "release search 503", err: &arr.NZBMetadataError{Stage: "release search", Status: http.StatusServiceUnavailable}, arrBackoff: true},
		{name: "no match", err: &arr.ReleaseMatchError{Stage: "identifier"}, permanent: true},
		{name: "ambiguous", err: &arr.ReleaseMatchError{Stage: "identifier", Candidates: 2, Ambiguous: true}, permanent: true},
		{name: "metadata 404", err: &arr.NZBMetadataError{Stage: "request", Status: http.StatusNotFound}, permanent: true},
		{name: "metadata too large", err: &arr.NZBMetadataError{Stage: "response", Kind: arr.ErrNZBMetadataTooLarge}, permanent: true},
		{name: "invalid metadata", err: &arr.NZBMetadataError{Stage: "validation", Kind: arr.ErrInvalidNZBMetadata}, permanent: true},
		{name: "no arr source", err: errLegacyNZBNoArrSource, permanent: true},
		{name: "ineligible arr source", err: errLegacyNZBArrIneligible, permanent: true},
		{name: "no par2", err: usenetpkg.ErrLegacyNZBNoPAR2, permanent: true},
		{name: "identity mismatch", err: usenetpkg.ErrLegacyNZBIdentityMismatch, permanent: true},
		// Budget exhaustion is local capacity, not a property of the release,
		// so it must stay retriable or a full store retires the whole backlog.
		{name: "download budget", err: recovery.ErrBudgetExceeded, permanent: false},
		{name: "storage budget", err: recovery.ErrStorageBudget, permanent: false},
		{name: "layout unavailable", err: recovery.ErrLayoutUnavailable, permanent: true},
		{name: "no recovery set", err: recovery.ErrNoRecoverySet, permanent: true},
		{name: "article missing", err: &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}, permanent: true},
		{
			// A budget branch anywhere in the tree keeps the whole failure
			// retriable, even beside a deterministic sibling.
			name: "joined article and budget failure",
			err: errors.Join(
				&nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430},
				recovery.ErrBudgetExceeded,
			),
			permanent: false,
		},
		{
			name: "transient provider branch wins over layout fallback",
			err: errors.Join(
				&nntp.Error{Type: nntp.ErrorTypeConnection, Message: "connection unavailable"},
				recovery.ErrLayoutUnavailable,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := legacyNZBHydrationFailurePersistent(test.err); got != test.permanent {
				t.Fatalf("permanent = %t, want %t", got, test.permanent)
			}
			if got := legacyNZBHydrationFailureUsesArrBackoff(test.err); got != test.arrBackoff {
				t.Fatalf("Arr backoff = %t, want %t", got, test.arrBackoff)
			}
		})
	}
}

func TestArrMediaID(t *testing.T) {
	content := arr.ContentFile{Id: 41, EpisodeId: 82}
	if got := arrMediaID(arr.Sonarr, content); got != 82 {
		t.Fatalf("Sonarr media ID = %d, want 82", got)
	}
	if got := arrMediaID(arr.Radarr, content); got != 41 {
		t.Fatalf("Radarr media ID = %d, want 41", got)
	}
	if got := arrMediaID(arr.Lidarr, content); got != 0 {
		t.Fatalf("unsupported Arr media ID = %d, want 0", got)
	}
}

func TestNZBHydrationSourceUsesManagedEntryCategory(t *testing.T) {
	service := &Service{}
	source := service.nzbHydrationSource(
		&candidate{},
		&storage.Entry{Category: "Sonarr"},
		"movie.mkv",
	)
	if source.arrName != "Sonarr" {
		t.Fatalf("Arr name = %q, want Sonarr", source.arrName)
	}
	if source.entryName != "" {
		t.Fatalf("entry name = %q, want empty candidate name", source.entryName)
	}
}

func TestNZBHydrationSourceFallsBackToCandidateMedia(t *testing.T) {
	arrs := arr.NewStorage()
	arrs.AddOrUpdate(&arr.Arr{
		Name: "Sonarr", Host: "http://sonarr.invalid", Token: "secret", Type: arr.Sonarr,
	})
	service := &Service{arrs: arrs}
	source := service.nzbHydrationSource(
		&candidate{
			name:    "Series/Season 01",
			arrName: "Sonarr",
			contentMap: map[string]arr.ContentFile{
				"movie.mkv": {EpisodeId: 82},
			},
		},
		&storage.Entry{Category: "Sonarr"},
		"extras.mkv",
	)
	if source.mediaID != 82 {
		t.Fatalf("media ID = %d, want 82", source.mediaID)
	}
	if source.entryName != "Series/Season 01" {
		t.Fatalf("entry name = %q", source.entryName)
	}
}

func TestNZBRepairConfirmedForFile(t *testing.T) {
	if !nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{RepairedFiles: []string{"movie.mkv"}}, "movie.mkv") {
		t.Fatal("fully repaired file was not confirmed")
	}
	if nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{
		RepairedFiles: []string{"movie.mkv"},
		FailedFiles:   []string{"movie.mkv"},
	}, "movie.mkv") {
		t.Fatal("partially failed file was confirmed")
	}
	if nzbRepairConfirmedForFile(usenetpkg.NZBRepairReport{RepairedFiles: []string{"other.mkv"}}, "movie.mkv") {
		t.Fatal("unaffected file was confirmed")
	}
}
