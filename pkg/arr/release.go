package arr

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	json "github.com/bytedance/sonic"
)

type Release struct {
	GUID                string             `json:"guid"`
	Title               string             `json:"title"`
	Indexer             string             `json:"indexer"`
	IndexerID           int                `json:"indexerId"`
	Approved            bool               `json:"approved"`
	Rejected            bool               `json:"rejected"`
	TemporarilyRejected bool               `json:"temporarilyRejected"`
	DownloadAllowed     bool               `json:"downloadAllowed"`
	Rejections          []string           `json:"rejections"`
	Payload             stdjson.RawMessage `json:"-"`
}

func (a *Arr) SearchEpisodeReleases(ctx context.Context, episodeID int) ([]Release, error) {
	if err := a.requireType(Sonarr); err != nil {
		return nil, err
	}
	if episodeID <= 0 {
		return nil, fmt.Errorf("release search: invalid episode ID %d", episodeID)
	}
	query := url.Values{"episodeId": {strconv.Itoa(episodeID)}}
	return a.searchReleases(ctx, query)
}

func (a *Arr) SearchSeasonReleases(ctx context.Context, seriesID, seasonNumber int) ([]Release, error) {
	if err := a.requireType(Sonarr); err != nil {
		return nil, err
	}
	if seriesID <= 0 {
		return nil, fmt.Errorf("release search: invalid series ID %d", seriesID)
	}
	if seasonNumber < 0 {
		return nil, fmt.Errorf("release search: invalid season number %d", seasonNumber)
	}
	query := url.Values{
		"seriesId":     {strconv.Itoa(seriesID)},
		"seasonNumber": {strconv.Itoa(seasonNumber)},
	}
	return a.searchReleases(ctx, query)
}

func (a *Arr) SearchMovieReleases(ctx context.Context, movieID int) ([]Release, error) {
	if err := a.requireType(Radarr); err != nil {
		return nil, err
	}
	if movieID <= 0 {
		return nil, fmt.Errorf("release search: invalid movie ID %d", movieID)
	}
	query := url.Values{"movieId": {strconv.Itoa(movieID)}}
	return a.searchReleases(ctx, query)
}

func (a *Arr) searchReleases(ctx context.Context, query url.Values) ([]Release, error) {
	var payloads []stdjson.RawMessage
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/release?"+query.Encode(), nil, &payloads)
	if err != nil {
		return nil, fmt.Errorf("search releases: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("search releases: %w", err)
	}

	releases := make([]Release, 0, len(payloads))
	for _, payload := range payloads {
		var release Release
		if err := json.Unmarshal(payload, &release); err != nil {
			return nil, fmt.Errorf("decode release: %w", err)
		}
		release.Payload = bytes.Clone(payload)
		releases = append(releases, release)
	}
	return releases, nil
}

func (a *Arr) GrabRelease(ctx context.Context, release Release) error {
	if a == nil {
		return fmt.Errorf("arr not configured")
	}
	if len(release.Payload) == 0 {
		return fmt.Errorf("grab release: release payload is required")
	}
	if release.GUID == "" || release.IndexerID <= 0 {
		return fmt.Errorf("grab release: release identity is incomplete")
	}

	resp, err := a.requestMutationCtx(ctx, http.MethodPost, "api/v3/release", release.Payload, nil)
	if err != nil {
		err = fmt.Errorf("grab release %q: %w", release.Title, err)
		if ambiguousMutationRequest(resp, err) {
			return UnknownMutationOutcome(err, 0)
		}
		return err
	}
	if err := expectSuccess(resp); err != nil {
		return fmt.Errorf("grab release %q: %w", release.Title, err)
	}
	return nil
}
