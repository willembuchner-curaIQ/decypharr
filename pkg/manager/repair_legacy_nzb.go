package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/arr"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
)

const legacyNZBFailureCooldown = 24 * time.Hour

var (
	errLegacyNZBHydration = errors.New("legacy NZB hydration failed")
	errLegacyNZBCooldown  = errors.New("legacy NZB hydration is in cooldown")
)

// legacyNZBProbeSource is deliberately attached to a file probe rather than
// rediscovered after the entry has been marked broken. This lets the probe
// recover the old NZB before health is finalized and before the Arr fallback
// deletes any media rows.
type legacyNZBProbeSource struct {
	arrName string
	content arr.ContentFile
}

// legacyNZBHydrationError distinguishes metadata reacquisition/hydration from
// a PAR2 audit failure. A hydration failure must retain the sampled
// usenet_segment_missing result so the existing Arr replacement fallback is
// still reached.
type legacyNZBHydrationError struct {
	err error
}

func (e *legacyNZBHydrationError) Error() string {
	if e == nil || e.err == nil {
		return errLegacyNZBHydration.Error()
	}
	return fmt.Sprintf("%v: %v", errLegacyNZBHydration, e.err)
}

func (e *legacyNZBHydrationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *legacyNZBHydrationError) Is(target error) bool {
	return target == errLegacyNZBHydration
}

type legacyNZBCooldownError struct {
	until time.Time
	cause error
}

func (e *legacyNZBCooldownError) Error() string {
	if e == nil {
		return errLegacyNZBCooldown.Error()
	}
	return fmt.Sprintf("%v until %s", errLegacyNZBCooldown, e.until.UTC().Format(time.RFC3339))
}

func (e *legacyNZBCooldownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *legacyNZBCooldownError) Is(target error) bool {
	return target == errLegacyNZBCooldown
}

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
	for candidate, failure := range s.failures {
		if !now.Before(failure.until) {
			delete(s.failures, candidate)
		}
	}
	failure, ok := s.failures[key]
	if !ok {
		return nil
	}
	return &legacyNZBCooldownError{until: failure.until, cause: failure.err}
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
	var metadataErr *arr.NZBMetadataError
	if errors.As(err, &metadataErr) {
		return metadataErr.Status != http.StatusRequestTimeout &&
			metadataErr.Status != http.StatusTooManyRequests &&
			(metadataErr.Status < 500 || metadataErr.Status > 599)
	}
	if hasTransientHTTPStatus(err.Error()) {
		return false
	}
	return true
}

func hasTransientHTTPStatus(message string) bool {
	message = strings.ToLower(message)
	for status := 408; status <= 599; status++ {
		if status != http.StatusRequestTimeout && status != http.StatusTooManyRequests && status < 500 {
			continue
		}
		code := fmt.Sprintf("%d", status)
		if strings.Contains(message, "status "+code) ||
			strings.Contains(message, ": "+code+" ") ||
			strings.HasSuffix(message, ": "+code) {
			return true
		}
	}
	return false
}

func (r *Repair) legacyNZBState() *legacyNZBRecoveryState {
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

func (r *Repair) hydrateLegacyNZB(ctx context.Context, nzbID string, source legacyNZBProbeSource) error {
	if r == nil || r.manager == nil || r.manager.usenet == nil {
		return &legacyNZBHydrationError{err: errors.New("usenet client is unavailable")}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	err := r.legacyNZBState().do(legacyNZBCooldownKey(source.arrName, nzbID), func() error {
		return hydrateLegacyNZBFromSources(
			ctx,
			nzbID,
			r.manager.usenet.LoadLegacyNZBSource,
			func(ctx context.Context) ([]byte, error) {
				if r.manager.arr == nil || strings.TrimSpace(source.arrName) == "" {
					return nil, errors.New("no Arr source is associated with this NZB file")
				}
				a := r.manager.arr.Get(source.arrName)
				if a == nil {
					return nil, fmt.Errorf("Arr %q is unavailable", source.arrName)
				}
				mediaID := legacyNZBMediaID(a.Type, source.content)
				if mediaID <= 0 {
					return nil, fmt.Errorf("Arr %q has no media ID for this NZB file", source.arrName)
				}
				return a.ReacquireNZB(ctx, mediaID)
			},
			r.manager.usenet.HydrateLegacyNZB,
		)
	})
	if err != nil {
		return &legacyNZBHydrationError{err: err}
	}
	return nil
}

func legacyNZBMediaID(kind arr.Type, content arr.ContentFile) int {
	switch kind {
	case arr.Sonarr:
		return content.EpisodeId
	case arr.Radarr:
		return content.Id
	default:
		return 0
	}
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
