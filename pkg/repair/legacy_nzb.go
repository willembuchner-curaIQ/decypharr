package repair

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
	usenetrecovery "github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

var (
	errLegacyNZBHydration     = errors.New("legacy NZB hydration failed")
	errLegacyNZBNoArrSource   = errors.New("legacy NZB has no associated Arr source")
	errLegacyNZBArrIneligible = errors.New("legacy NZB Arr source is not eligible for repair")
)

// legacyNZBHydrationFailurePersistent reports whether a hydration failure is a
// property of the release itself rather than of the moment. Only those are
// remembered on the entry health record; everything else retries on the next
// run.
//
// Budget exhaustion is deliberately excluded. A full PAR2 store or a spent
// download allowance says nothing about the NZB, and treating it as decisive
// silently retired thousands of recoverable releases the first time the store
// filled up.
func legacyNZBHydrationFailurePersistent(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Hydration errors may join the original provider failure, a PAR2 failure,
	// and a provenance fallback failure. Any genuinely transient branch wins
	// over deterministic siblings such as the fallback layout error.
	if legacyNZBErrorTreeAny(err, customerror.IsRetriableError) {
		return false
	}
	if legacyNZBHydrationFailureOperational(err) {
		return false
	}
	// Arr exposes response status on metadata-download errors. A timeout,
	// throttling response, or upstream 5xx is operational even when it has no
	// wrapped net.Error for the shared retry classifier to inspect.
	if metadataErr, ok := errors.AsType[*arr.NZBMetadataError](err); ok {
		if metadataErr.Status != 0 {
			return metadataErr.Status != http.StatusRequestTimeout &&
				metadataErr.Status != http.StatusTooManyRequests &&
				(metadataErr.Status < 500 || metadataErr.Status > 599)
		}
		return errors.Is(metadataErr, arr.ErrNZBMetadataTooLarge) ||
			errors.Is(metadataErr, arr.ErrInvalidNZBMetadata)
	}
	if _, definitive := par2RepairFailureReason(err); definitive {
		return true
	}
	return errors.Is(err, arr.ErrNZBReacquireNoMatch) ||
		errors.Is(err, arr.ErrNZBReacquireAmbiguous) ||
		errors.Is(err, errLegacyNZBNoArrSource) ||
		errors.Is(err, errLegacyNZBArrIneligible) ||
		errors.Is(err, usenetpkg.ErrLegacyNZBIdentityMismatch) ||
		errors.Is(err, usenetpkg.ErrLegacyNZBNoPAR2)
}

// legacyNZBHydrationFailureOperational reports a failure caused by exhausted
// local capacity rather than by the release. These must stay retriable.
func legacyNZBHydrationFailureOperational(err error) bool {
	return errors.Is(err, usenetrecovery.ErrStorageBudget) ||
		errors.Is(err, usenetrecovery.ErrBudgetExceeded) ||
		errors.Is(err, usenetrecovery.ErrUnboundedTraffic)
}

// legacyNZBHydrationReason labels a persistent failure for the health record.
func legacyNZBHydrationReason(err error) string {
	switch {
	case errors.Is(err, errLegacyNZBNoArrSource):
		return "hydration_no_arr_source"
	case errors.Is(err, errLegacyNZBArrIneligible):
		return "hydration_arr_ineligible"
	case errors.Is(err, arr.ErrNZBReacquireNoMatch):
		return "hydration_release_not_found"
	case errors.Is(err, arr.ErrNZBReacquireAmbiguous):
		return "hydration_release_ambiguous"
	case errors.Is(err, usenetpkg.ErrLegacyNZBNoPAR2):
		return "hydration_no_par2"
	case errors.Is(err, usenetpkg.ErrLegacyNZBIdentityMismatch):
		return "hydration_identity_mismatch"
	}
	if reason, definitive := par2RepairFailureReason(err); definitive {
		return reason
	}
	return "hydration_failed"
}

func legacyNZBHydrationFailureUsesArrBackoff(err error) bool {
	metadataErr, ok := errors.AsType[*arr.NZBMetadataError](err)
	if !ok {
		return false
	}
	if metadataErr.Status != 0 {
		return metadataErr.Status == http.StatusRequestTimeout ||
			metadataErr.Status == http.StatusTooManyRequests ||
			metadataErr.Status >= 500 && metadataErr.Status <= 599
	}
	return !errors.Is(metadataErr, arr.ErrNZBMetadataTooLarge) &&
		!errors.Is(metadataErr, arr.ErrInvalidNZBMetadata)
}

func legacyNZBErrorTreeAny(err error, predicate func(error) bool) bool {
	if err == nil || predicate == nil {
		return false
	}
	if predicate(err) {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if legacyNZBErrorTreeAny(child, predicate) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return legacyNZBErrorTreeAny(wrapped.Unwrap(), predicate)
	}
	return false
}

func (r *Service) hydrateLegacyNZB(ctx context.Context, nzbID string, source nzbHydrationSource) error {
	if r == nil || r.usenet == nil {
		return fmt.Errorf("%w: usenet client is unavailable", errLegacyNZBHydration)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	err := hydrateLegacyNZBFromSources(
		ctx,
		nzbID,
		r.usenet.LoadLegacyNZBSource,
		func(ctx context.Context) ([]byte, error) {
			if strings.TrimSpace(source.arrName) == "" {
				return nil, errLegacyNZBNoArrSource
			}
			if r.arrs == nil {
				return nil, errors.New("arr storage is unavailable")
			}
			a := r.arrs.Get(source.arrName)
			if a == nil {
				return nil, fmt.Errorf("arr %q is unavailable", source.arrName)
			}
			if a.Host == "" || a.Token == "" || a.SkipRepair {
				return nil, fmt.Errorf("%w: %q", errLegacyNZBArrIneligible, source.arrName)
			}
			content, err := a.ReacquireNZBByDownloadID(ctx, nzbID)
			if err == nil || source.mediaID <= 0 || !errors.Is(err, arr.ErrNZBReacquireNoMatch) {
				return content, err
			}
			return a.ReacquireNZB(ctx, source.mediaID)
		},
		r.usenet.HydrateLegacyNZB,
	)
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
		source.entryName = candidate.name
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
		return errors.New("arr NZB reacquisition is unavailable")
	}
	content, err = reacquire(ctx)
	if err != nil {
		return fmt.Errorf("reacquire legacy NZB from Arr: %w", err)
	}
	return hydrate(ctx, nzbID, nzbID+".nzb", content)
}
