package arr

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxReacquiredNZBBytes is a hard decoded-response limit. Reacquisition is
	// metadata-only and must never turn into an unbounded release download.
	MaxReacquiredNZBBytes int64 = 32 << 20
	nzbRequestTimeout           = 60 * time.Second
)

var (
	ErrReacquireNoMatch    = errors.New("arr NZB reacquisition found no unique release")
	ErrReacquireAmbiguous  = errors.New("arr NZB reacquisition release is ambiguous")
	ErrNZBMetadataTooLarge = errors.New("arr NZB metadata exceeds the response limit")
	ErrInvalidNZBMetadata  = errors.New("arr response is not NZB metadata")

	// Explicit aliases make the stable negative-cache sentinel discoverable to
	// callers searching by the full feature name.
	ErrNZBReacquireNoMatch   = ErrReacquireNoMatch
	ErrNZBReacquireAmbiguous = ErrReacquireAmbiguous
)

// ReleaseMatchError deliberately contains no release URLs or titles, which
// may embed indexer credentials.
type ReleaseMatchError struct {
	Stage      string
	Candidates int
	Ambiguous  bool
}

func (e *ReleaseMatchError) Error() string {
	if e.Ambiguous {
		return fmt.Sprintf("%v at %s stage (%d candidates)", ErrReacquireAmbiguous, e.Stage, e.Candidates)
	}
	return fmt.Sprintf("%v at %s stage", ErrReacquireNoMatch, e.Stage)
}

func (e *ReleaseMatchError) Is(target error) bool {
	if e.Ambiguous {
		return target == ErrReacquireAmbiguous
	}
	return target == ErrReacquireNoMatch
}

// NZBMetadataError keeps network causes available to errors.Is/As without
// rendering their credential-bearing URLs in Error().
type NZBMetadataError struct {
	Kind   error
	Stage  string
	Status int
	Cause  error
}

func (e *NZBMetadataError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("Arr NZB metadata %s failed with HTTP status %d", e.Stage, e.Status)
	}
	if e.Kind != nil {
		return fmt.Sprintf("Arr NZB metadata %s failed: %v", e.Stage, e.Kind)
	}
	return fmt.Sprintf("Arr NZB metadata %s failed", e.Stage)
}

func (e *NZBMetadataError) Unwrap() error { return e.Cause }

func (e *NZBMetadataError) Is(target error) bool { return e.Kind != nil && target == e.Kind }

// ReleaseSearchRecord models both the flat Sonarr/Radarr v3 response and the
// nested release shape used by newer APIs.
type ReleaseSearchRecord struct {
	Title            string             `json:"title"`
	GUID             string             `json:"guid"`
	DownloadURL      string             `json:"downloadUrl"`
	InfoURL          string             `json:"infoUrl"`
	NZBInfoURL       string             `json:"nzbInfoUrl"`
	Indexer          string             `json:"indexer"`
	IndexerID        int                `json:"indexerId"`
	Protocol         string             `json:"protocol"`
	DownloadProtocol string             `json:"downloadProtocol"`
	Release          *ReleaseSearchInfo `json:"release,omitempty"`
}

type ReleaseSearchInfo struct {
	Title            string `json:"title"`
	GUID             string `json:"guid"`
	DownloadURL      string `json:"downloadUrl"`
	InfoURL          string `json:"infoUrl"`
	NZBInfoURL       string `json:"nzbInfoUrl"`
	Indexer          string `json:"indexer"`
	IndexerID        int    `json:"indexerId"`
	Protocol         string `json:"protocol"`
	DownloadProtocol string `json:"downloadProtocol"`
}

func (r ReleaseSearchRecord) info() ReleaseSearchInfo {
	flat := ReleaseSearchInfo{
		Title: r.Title, GUID: r.GUID, DownloadURL: r.DownloadURL,
		InfoURL: r.InfoURL, NZBInfoURL: r.NZBInfoURL, Indexer: r.Indexer,
		IndexerID: r.IndexerID, Protocol: r.Protocol, DownloadProtocol: r.DownloadProtocol,
	}
	if r.Release == nil {
		return flat
	}
	nested := *r.Release
	fillString := func(value *string, fallback string) {
		if *value == "" {
			*value = fallback
		}
	}
	fillString(&nested.Title, flat.Title)
	fillString(&nested.GUID, flat.GUID)
	fillString(&nested.DownloadURL, flat.DownloadURL)
	fillString(&nested.InfoURL, flat.InfoURL)
	fillString(&nested.NZBInfoURL, flat.NZBInfoURL)
	fillString(&nested.Indexer, flat.Indexer)
	fillString(&nested.Protocol, flat.Protocol)
	fillString(&nested.DownloadProtocol, flat.DownloadProtocol)
	if nested.IndexerID == 0 {
		nested.IndexerID = flat.IndexerID
	}
	return nested
}

// SearchCurrentReleases performs an interactive release search scoped to the
// affected Sonarr episode or Radarr movie.
func (a *Arr) SearchCurrentReleases(ctx context.Context, mediaDBID int) ([]ReleaseSearchRecord, error) {
	if a == nil || a.Host == "" || a.Token == "" {
		return nil, fmt.Errorf("arr not configured")
	}
	if mediaDBID <= 0 {
		return nil, &ReleaseMatchError{Stage: "media"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query := url.Values{}
	switch a.Type {
	case Sonarr:
		query.Set("episodeId", strconv.Itoa(mediaDBID))
	case Radarr:
		query.Set("movieId", strconv.Itoa(mediaDBID))
	default:
		return nil, fmt.Errorf("release search is unsupported for arr type %q", a.Type)
	}
	var releases []ReleaseSearchRecord
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/release?"+query.Encode(), nil, &releases)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("release search failed: %s", resp.Status)
	}
	return releases, nil
}

// ReacquireNZB retrieves only the original NZB metadata for a legacy item.
// The current release URL must be re-issued by Arr's interactive search and
// uniquely tied back to its grab history before it is requested.
func (a *Arr) ReacquireNZB(ctx context.Context, mediaDBID int) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	history, err := a.FindGrabHistoryCtx(ctx, mediaDBID)
	if err != nil {
		return nil, err
	}
	if history == nil {
		return nil, &ReleaseMatchError{Stage: "history"}
	}
	releases, err := a.SearchCurrentReleases(ctx, mediaDBID)
	if err != nil {
		return nil, err
	}
	matched, err := matchGrabbedRelease(*history, releases)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(matched.DownloadURL) == "" {
		return nil, &ReleaseMatchError{Stage: "download URL"}
	}
	return a.downloadNZBMetadata(ctx, matched.DownloadURL)
}

func matchGrabbedRelease(history HistoryRecord, releases []ReleaseSearchRecord) (ReleaseSearchInfo, error) {
	guid := strings.TrimSpace(historyDataValue(history.Data, "guid"))
	infoURL := strings.TrimSpace(historyDataValue(history.Data, "nzbInfoUrl"))
	strong := guid != "" || infoURL != ""
	candidates := make([]ReleaseSearchInfo, 0, 1)
	for _, record := range releases {
		release := record.info()
		if !isUsenetRelease(release) {
			continue
		}
		matched := guid != "" && strings.TrimSpace(release.GUID) == guid
		matched = matched || infoURL != "" && (strings.TrimSpace(release.InfoURL) == infoURL || strings.TrimSpace(release.NZBInfoURL) == infoURL)
		if matched {
			candidates = append(candidates, release)
		}
	}
	if strong {
		return uniqueRelease("identifier", candidates)
	}

	title := strings.TrimSpace(history.SourceTitle)
	indexer := strings.TrimSpace(historyDataValue(history.Data, "indexer", "indexerName"))
	if title == "" || indexer == "" {
		return ReleaseSearchInfo{}, &ReleaseMatchError{Stage: "fallback"}
	}
	for _, record := range releases {
		release := record.info()
		if !isUsenetRelease(release) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(release.Title), title) && strings.EqualFold(strings.TrimSpace(release.Indexer), indexer) {
			candidates = append(candidates, release)
		}
	}
	return uniqueRelease("title/indexer", candidates)
}

func uniqueRelease(stage string, candidates []ReleaseSearchInfo) (ReleaseSearchInfo, error) {
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return ReleaseSearchInfo{}, &ReleaseMatchError{Stage: stage, Candidates: len(candidates), Ambiguous: len(candidates) > 1}
}

func isUsenetRelease(release ReleaseSearchInfo) bool {
	protocol := strings.ToLower(strings.TrimSpace(release.DownloadProtocol))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(release.Protocol))
	}
	// Older APIs omitted the protocol for NZB-only indexers. Known non-Usenet
	// values are rejected; an omitted value remains compatible.
	return protocol == "" || protocol == "usenet" || protocol == "nzb"
}

func historyDataValue(data map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := data[key]; value != "" {
			return value
		}
		for actual, value := range data {
			if strings.EqualFold(actual, key) && value != "" {
				return value
			}
		}
	}
	return ""
}

func (a *Arr) downloadNZBMetadata(ctx context.Context, address string) ([]byte, error) {
	base, err := url.Parse(a.Host)
	if err != nil || base.Scheme == "" || base.Host == "" || !isHTTPScheme(base.Scheme) {
		return nil, &NZBMetadataError{Stage: "Arr origin", Cause: err}
	}
	reference, err := url.Parse(strings.TrimSpace(address))
	if err != nil {
		return nil, &NZBMetadataError{Stage: "address", Cause: err}
	}
	resolver := *base
	resolver.RawQuery = ""
	resolver.Fragment = ""
	if !reference.IsAbs() && !strings.HasPrefix(reference.Path, "/") && !strings.HasSuffix(resolver.Path, "/") {
		resolver.Path += "/"
	}
	target := resolver.ResolveReference(reference)
	if target.Host == "" || !isHTTPScheme(target.Scheme) {
		return nil, &NZBMetadataError{Stage: "address"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, &NZBMetadataError{Stage: "request", Cause: err}
	}
	req.Header.Set("Accept", "application/x-nzb, application/xml, text/xml")
	if sameOrigin(base, target) {
		req.Header.Set("X-Api-Key", a.Token)
	}

	client := &http.Client{
		Timeout: nzbRequestTimeout,
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("redirect limit exceeded")
			}
			if !isHTTPScheme(next.URL.Scheme) {
				return errors.New("redirect scheme is not HTTP")
			}
			// Go copies custom headers across redirects. Explicitly remove the
			// Arr credential whenever the destination origin changes.
			if !sameOrigin(base, next.URL) {
				next.Header.Del("X-Api-Key")
			}
			next.Header.Del("Referer")
			return nil
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, &NZBMetadataError{Stage: "request", Cause: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, &NZBMetadataError{Stage: "request", Status: resp.StatusCode}
	}
	if resp.ContentLength > MaxReacquiredNZBBytes {
		return nil, &NZBMetadataError{Kind: ErrNZBMetadataTooLarge, Stage: "response size"}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxReacquiredNZBBytes+1))
	if err != nil {
		return nil, &NZBMetadataError{Stage: "response read", Cause: err}
	}
	if int64(len(data)) > MaxReacquiredNZBBytes {
		return nil, &NZBMetadataError{Kind: ErrNZBMetadataTooLarge, Stage: "response size"}
	}
	if !hasNZBRoot(data) {
		return nil, &NZBMetadataError{Kind: ErrInvalidNZBMetadata, Stage: "validation"}
	}
	return data, nil
}

func hasNZBRoot(data []byte) bool {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		if start, ok := token.(xml.StartElement); ok {
			return strings.EqualFold(start.Name.Local, "nzb")
		}
	}
}

func isHTTPScheme(scheme string) bool {
	return strings.EqualFold(scheme, "http") || strings.EqualFold(scheme, "https")
}

func sameOrigin(left, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) || !strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return effectivePort(left) == effectivePort(right)
}

func effectivePort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	return "80"
}
