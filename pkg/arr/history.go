package arr

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	gourl "net/url"
	"strconv"
	"strings"
	"time"
)

const (
	QueueActionNone              QueueAction = ""                   // leave the item in the queue
	QueueActionImport            QueueAction = "import"             // force a manual import
	QueueActionBlocklist         QueueAction = "blacklist"          // blocklist + remove, do NOT re-search
	QueueActionBlocklistResearch QueueAction = "blacklist_research" // blocklist + remove + re-search
)

type HistorySchema struct {
	Page          int             `json:"page"`
	PageSize      int             `json:"pageSize"`
	SortKey       string          `json:"sortKey"`
	SortDirection string          `json:"sortDirection"`
	TotalRecords  int             `json:"totalRecords"`
	Records       []HistoryRecord `json:"records"`
}

type HistoryRecord struct {
	ID          int               `json:"id"`
	DownloadID  string            `json:"downloadId"`
	EventType   string            `json:"eventType"`
	EpisodeID   int               `json:"episodeId,omitempty"`
	SeriesID    int               `json:"seriesId,omitempty"`
	MovieID     int               `json:"movieId,omitempty"`
	SourceTitle string            `json:"sourceTitle,omitempty"`
	Data        map[string]string `json:"data,omitempty"`
	Date        time.Time         `json:"date,omitzero"`
}

func (a *Arr) GetHistory(downloadId, eventType string) *HistorySchema {
	query := gourl.Values{}
	if downloadId != "" {
		query.Add("downloadId", downloadId)
	}
	query.Add("eventType", eventType)
	query.Add("pageSize", "100")
	url := "api/v3/history" + "?" + query.Encode()
	var data *HistorySchema
	resp, err := a.Request(http.MethodGet, url, nil, &data)
	if err != nil {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	return data
}

// FindGrabHistoryID returns the ID and downloadId of the most recent "grabbed"
// history record for the given episode (Sonarr) or movie (Radarr). Returns
// (0, "", nil) when no grab record is found (e.g. history trimmed, manual import).
func (a *Arr) FindGrabHistoryID(mediaDBID int) (int, string, error) {
	if a == nil {
		return 0, "", fmt.Errorf("arr not configured")
	}
	if mediaDBID <= 0 {
		return 0, "", nil
	}

	query := gourl.Values{}
	query.Add("page", "1")
	query.Add("pageSize", "50")
	query.Add("sortKey", "date")
	query.Add("sortDirection", "descending")
	query.Add("eventType", "1") // 1 = grabbed

	switch a.Type {
	case Sonarr:
		query.Add("episodeId", strconv.Itoa(mediaDBID))
	case Radarr:
		query.Add("movieIds", strconv.Itoa(mediaDBID))
	default:
		return 0, "", nil
	}

	var data HistorySchema
	url := "api/v3/history?" + query.Encode()
	resp, err := a.Request(http.MethodGet, url, nil, &data)
	if err != nil {
		return 0, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("history lookup failed: %s", resp.Status)
	}
	if len(data.Records) == 0 {
		return 0, "", nil
	}
	record := data.Records[0]
	return record.ID, record.DownloadID, nil
}

// MarkHistoryFailed marks a grab history record as failed. This blocklists
// the release in the arr and, if redownload is enabled, triggers a re-search
// for whatever is currently missing from that grab's scope.
func (a *Arr) MarkHistoryFailed(historyID int) error {
	if historyID <= 0 {
		return nil
	}
	return a.MarkHistoryFailedCtx(context.Background(), historyID)
}

// DataValue reads one field of a history record's data map. Arr casing is not
// stable across versions, so the lookup is case-insensitive.
func (record HistoryRecord) DataValue(key string) (string, bool) {
	for currentKey, value := range record.Data {
		if strings.EqualFold(currentKey, key) {
			return value, true
		}
	}
	return "", false
}

const (
	HistoryEventGrabbed        = "grabbed"
	HistoryEventDownloadFailed = "downloadFailed"
	historyPageSize            = 100
	importHistoryPageSize      = 1000
	importHistoryMaxPages      = 20
	importHistoryEventType     = 3
	grabbedHistoryEventType    = 1
)

// ImportHistory returns the import records for the given download IDs, newest
// first. The scan is bounded: a large library has far more history than the
// index needs, so it keeps only wanted records and stops once every download
// is found or the page budget runs out.
func (a *Arr) ImportHistory(ctx context.Context, downloadIDs map[string]struct{}) ([]HistoryRecord, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}
	if len(downloadIDs) == 0 {
		return nil, nil
	}

	records := make([]HistoryRecord, 0)
	found := make(map[string]struct{}, len(downloadIDs))
	fetched := 0
	for page := 1; page <= importHistoryMaxPages; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("pageSize", strconv.Itoa(importHistoryPageSize))
		query.Set("sortKey", "date")
		query.Set("sortDirection", "descending")
		query.Set("eventType", strconv.Itoa(importHistoryEventType))

		var history HistorySchema
		resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/history?"+query.Encode(), nil, &history)
		if err != nil {
			return nil, fmt.Errorf("import history lookup: %w", err)
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			return nil, fmt.Errorf("import history lookup: %w", err)
		}

		fetched += len(history.Records)
		for _, record := range history.Records {
			if _, wanted := downloadIDs[record.DownloadID]; !wanted {
				continue
			}
			if !strings.EqualFold(record.EventType, "downloadFolderImported") {
				continue
			}
			records = append(records, record)
			found[record.DownloadID] = struct{}{}
		}
		if len(found) == len(downloadIDs) || len(history.Records) == 0 || fetched >= history.TotalRecords {
			break
		}
	}
	return records, nil
}

func (a *Arr) HistoryByDownloadID(ctx context.Context, downloadID, eventType string) ([]HistoryRecord, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}
	downloadID = strings.TrimSpace(downloadID)
	if downloadID == "" {
		return nil, fmt.Errorf("history lookup: download ID is required")
	}

	records := make([]HistoryRecord, 0)
	fetched := 0
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("pageSize", strconv.Itoa(historyPageSize))
		query.Set("sortKey", "date")
		query.Set("sortDirection", "descending")
		query.Set("downloadId", downloadID)

		var history HistorySchema
		resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/history?"+query.Encode(), nil, &history)
		if err != nil {
			return nil, fmt.Errorf("history lookup for download %q: %w", downloadID, err)
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			return nil, fmt.Errorf("history lookup for download %q: %w", downloadID, err)
		}

		fetched += len(history.Records)
		for _, record := range history.Records {
			if record.DownloadID != downloadID {
				continue
			}
			if eventType == "" || strings.EqualFold(record.EventType, eventType) {
				records = append(records, record)
			}
		}

		if len(history.Records) == 0 || fetched >= history.TotalRecords {
			break
		}
	}
	return records, nil
}

func (a *Arr) FindGrabHistoryByDownloadID(ctx context.Context, downloadID string) (HistoryRecord, bool, error) {
	return a.findHistoryByDownloadID(ctx, downloadID, HistoryEventGrabbed)
}

func (a *Arr) FindDownloadFailedHistoryByDownloadID(ctx context.Context, downloadID string) (HistoryRecord, bool, error) {
	return a.findHistoryByDownloadID(ctx, downloadID, HistoryEventDownloadFailed)
}

func (a *Arr) GrabHistorySince(ctx context.Context, since time.Time) ([]HistoryRecord, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}
	records := make([]HistoryRecord, 0)
	fetched := 0
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("pageSize", strconv.Itoa(historyPageSize))
		query.Set("sortKey", "date")
		query.Set("sortDirection", "descending")
		query.Set("eventType", strconv.Itoa(grabbedHistoryEventType))

		var history HistorySchema
		resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/history?"+query.Encode(), nil, &history)
		if err != nil {
			return nil, fmt.Errorf("grab history lookup: %w", err)
		}
		if err := expectStatus(resp, http.StatusOK); err != nil {
			return nil, fmt.Errorf("grab history lookup: %w", err)
		}

		fetched += len(history.Records)
		pageReachedOlderRecord := false
		for _, record := range history.Records {
			if !strings.EqualFold(record.EventType, HistoryEventGrabbed) {
				continue
			}
			if !record.Date.IsZero() && record.Date.Before(since) {
				pageReachedOlderRecord = true
				continue
			}
			records = append(records, record)
		}
		if pageReachedOlderRecord || len(history.Records) == 0 || fetched >= history.TotalRecords {
			break
		}
	}
	return records, nil
}

func (a *Arr) HasDownloadFailedHistory(ctx context.Context, downloadID string) (bool, error) {
	_, found, err := a.FindDownloadFailedHistoryByDownloadID(ctx, downloadID)
	return found, err
}

func (a *Arr) findHistoryByDownloadID(ctx context.Context, downloadID, eventType string) (HistoryRecord, bool, error) {
	records, err := a.HistoryByDownloadID(ctx, downloadID, eventType)
	if err != nil {
		return HistoryRecord{}, false, err
	}
	if len(records) == 0 {
		return HistoryRecord{}, false, nil
	}
	return records[0], true, nil
}

func (a *Arr) MarkHistoryFailedCtx(ctx context.Context, historyID int) error {
	if a == nil {
		return fmt.Errorf("arr not configured")
	}
	if historyID <= 0 {
		return fmt.Errorf("mark history failed: invalid history ID %d", historyID)
	}

	endpoint := fmt.Sprintf("api/v3/history/failed/%d", historyID)
	resp, err := a.requestMutationCtx(ctx, http.MethodPost, endpoint, nil, nil)
	if err != nil {
		err = fmt.Errorf("mark history %d failed: %w", historyID, err)
		if ambiguousMutationRequest(resp, err) {
			return UnknownMutationOutcome(err, 0)
		}
		return err
	}
	if err := expectSuccess(resp); err != nil {
		return fmt.Errorf("mark history %d failed: %w", historyID, err)
	}
	return nil
}
