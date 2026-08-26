package repair

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
)

const legacyNZBFailureCooldown = 24 * time.Hour

var (
	errLegacyNZBHydration = errors.New("legacy NZB hydration failed")
	errLegacyNZBCooldown  = errors.New("legacy NZB hydration is in cooldown")
)

type legacyNZBFailure struct {
	until time.Time
	err   error
}

// legacyNZBRecoveryState coalesces concurrent attempts and remembers only
// stable failures. Successful attempts and operational/transient failures are
// never negatively cached.
type legacyNZBRecoveryState struct {
	mu       sync.Mutex
	failures map[string]legacyNZBFailure
	flights  singleflight.Group
	cooldown time.Duration
	now      func() time.Time
}

func newLegacyNZBRecoveryState() *legacyNZBRecoveryState {
	return &legacyNZBRecoveryState{
		failures: make(map[string]legacyNZBFailure),
		cooldown: legacyNZBFailureCooldown,
		now:      time.Now,
	}
}

func (s *legacyNZBRecoveryState) do(key string, attempt func() error) error {
	if attempt == nil {
		return errors.New("legacy NZB recovery attempt is unavailable")
	}
	if s == nil || strings.TrimSpace(key) == "" {
		return attempt()
	}
	if err := s.cachedFailure(key); err != nil {
		return err
	}

	_, err, _ := s.flights.Do(key, func() (any, error) {
		if err := s.cachedFailure(key); err != nil {
			return nil, err
		}
		err := attempt()
		if err == nil {
			s.clearFailure(key)
			return nil, nil
		}
		if cacheLegacyNZBFailure(err) {
			s.rememberFailure(key, err)
		}
		return nil, err
	})
	return err
}

func (s *legacyNZBRecoveryState) clock() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func (s *legacyNZBRecoveryState) cachedFailure(key string) error {
	now := s.clock()
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.DeleteFunc(s.failures, func(_ string, failure legacyNZBFailure) bool {
		return !now.Before(failure.until)
	})
	failure, ok := s.failures[key]
	if !ok {
		return nil
	}
	return fmt.Errorf("%w until %s: %w", errLegacyNZBCooldown, failure.until.UTC().Format(time.RFC3339), failure.err)
}

func (s *legacyNZBRecoveryState) rememberFailure(key string, err error) {
	cooldown := s.cooldown
	if cooldown <= 0 {
		cooldown = legacyNZBFailureCooldown
	}
	until := s.clock().Add(cooldown)
	s.mu.Lock()
	s.failures[key] = legacyNZBFailure{until: until, err: err}
	s.mu.Unlock()
}

func (s *legacyNZBRecoveryState) clearFailure(key string) {
	s.mu.Lock()
	delete(s.failures, key)
	s.mu.Unlock()
}

func cacheLegacyNZBFailure(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if customerror.IsRetriableError(err) {
		return false
	}
	// Arr exposes response status on metadata-download errors. A timeout,
	// throttling response, or upstream 5xx is operational even when it has no
	// wrapped net.Error for the shared retry classifier to inspect.
	if metadataErr, ok := errors.AsType[*arr.NZBMetadataError](err); ok {
		return metadataErr.Status != http.StatusRequestTimeout &&
			metadataErr.Status != http.StatusTooManyRequests &&
			(metadataErr.Status < 500 || metadataErr.Status > 599)
	}
	return true
}

func (r *Service) legacyNZBState() *legacyNZBRecoveryState {
	r.legacyNZBMu.Lock()
	defer r.legacyNZBMu.Unlock()
	if r.legacyNZB == nil {
		r.legacyNZB = newLegacyNZBRecoveryState()
	}
	return r.legacyNZB
}

func legacyNZBCooldownKey(arrName, nzbID string) string {
	return strings.TrimSpace(arrName) + "\x00" + strings.TrimSpace(nzbID)
}

func (r *Service) hydrateLegacyNZB(ctx context.Context, nzbID string, source nzbHydrationSource) error {
	if r == nil || r.usenet == nil {
		return fmt.Errorf("%w: usenet client is unavailable", errLegacyNZBHydration)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	err := r.legacyNZBState().do(legacyNZBCooldownKey(source.arrName, nzbID), func() error {
		return hydrateLegacyNZBFromSources(
			ctx,
			nzbID,
			r.usenet.LoadLegacyNZBSource,
			func(ctx context.Context) ([]byte, error) {
				if r.arrs == nil || strings.TrimSpace(source.arrName) == "" {
					return nil, errors.New("no Arr source is associated with this NZB file")
				}
				a := r.arrs.Get(source.arrName)
				if a == nil {
					return nil, fmt.Errorf("Arr %q is unavailable", source.arrName)
				}
				if a.Host == "" || a.Token == "" || a.SkipRepair {
					return nil, fmt.Errorf("Arr %q is not eligible for repair", source.arrName)
				}
				content, err := a.ReacquireNZBByDownloadID(ctx, nzbID)
				if err == nil || source.mediaID <= 0 || !errors.Is(err, arr.ErrNZBReacquireNoMatch) {
					return content, err
				}
				return a.ReacquireNZB(ctx, source.mediaID)
			},
			r.usenet.HydrateLegacyNZB,
		)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", errLegacyNZBHydration, err)
	}
	return nil
}

func arrMediaID(kind arr.Type, content arr.ContentFile) int {
	switch kind {
	case arr.Sonarr:
		return content.EpisodeId
	case arr.Radarr:
		return content.Id
	default:
		return 0
	}
}

func (r *Service) nzbHydrationSource(candidate *candidate, entry *storage.Entry, fileName string) nzbHydrationSource {
	var source nzbHydrationSource
	if candidate != nil {
		source.arrName = candidate.arrName
	}
	if source.arrName == "" && entry != nil {
		source.arrName = entry.Category
	}
	if r == nil || r.arrs == nil || candidate == nil {
		return source
	}
	a := r.arrs.Get(source.arrName)
	if a == nil {
		return source
	}
	if content, ok := candidate.contentMap[fileName]; ok {
		source.mediaID = arrMediaID(a.Type, content)
	}
	if source.mediaID > 0 {
		return source
	}
	for _, content := range candidate.contentMap {
		if source.mediaID = arrMediaID(a.Type, content); source.mediaID > 0 {
			break
		}
	}
	return source
}

type legacyNZBLoadFunc func(string) (sourceName string, content []byte, err error)
type legacyNZBReacquireFunc func(context.Context) ([]byte, error)
type legacyNZBHydrateFunc func(context.Context, string, string, []byte) error

// hydrateLegacyNZBFromSources keeps source selection independently testable:
// a surviving local NZB always wins, while the Arr performs the metadata-only
// network reacquisition solely when Usenet reports the local source absent.
func hydrateLegacyNZBFromSources(ctx context.Context, nzbID string, load legacyNZBLoadFunc, reacquire legacyNZBReacquireFunc, hydrate legacyNZBHydrateFunc) error {
	if load == nil || hydrate == nil {
		return errors.New("legacy NZB source loader or hydrator is unavailable")
	}
	sourceName, content, err := load(nzbID)
	if err == nil {
		return hydrate(ctx, nzbID, sourceName, content)
	}
	if !errors.Is(err, usenetpkg.ErrLegacyNZBSourceUnavailable) {
		return fmt.Errorf("load local legacy NZB source: %w", err)
	}
	if reacquire == nil {
		return errors.New("Arr NZB reacquisition is unavailable")
	}
	content, err = reacquire(ctx)
	if err != nil {
		return fmt.Errorf("reacquire legacy NZB from Arr: %w", err)
	}
	return hydrate(ctx, nzbID, nzbID+".nzb", content)
}
