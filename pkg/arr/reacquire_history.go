package arr

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	HistoryEventGrabbed        = "grabbed"
	HistoryEventDownloadFailed = "downloadFailed"
	historyPageSize            = 100
	importHistoryPageSize      = 1000
	importHistoryEventType     = 3
	grabbedHistoryEventType    = 1
)

func (a *Arr) ImportHistory(ctx context.Context) ([]HistoryRecord, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}

	records := make([]HistoryRecord, 0)
	fetched := 0
	for page := 1; ; page++ {
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
			if strings.EqualFold(record.EventType, "downloadFolderImported") {
				records = append(records, record)
			}
		}
		if len(history.Records) == 0 || fetched >= history.TotalRecords {
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
			return unknownMutationOutcome(err, 0)
		}
		return err
	}
	if err := expectSuccess(resp); err != nil {
		return fmt.Errorf("mark history %d failed: %w", historyID, err)
	}
	return nil
}
