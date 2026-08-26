package storage

import (
	"errors"
	"fmt"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/appendstore"
)

// LegacyNZBHydrationState distinguishes operational retries from releases
// that could not be reacquired safely. Successful hydration is represented by
// the upgraded NZB metadata itself, so completed records are deleted.
type LegacyNZBHydrationState string

const (
	LegacyNZBHydrationRetrying    LegacyNZBHydrationState = "retrying"
	LegacyNZBHydrationUnavailable LegacyNZBHydrationState = "unavailable"
)

// LegacyNZBHydration records enough migration state to avoid hammering an Arr
// again after a restart. It intentionally stores no release title, URL, or NZB
// contents.
type LegacyNZBHydration struct {
	NZBID         string                  `json:"nzb_id"`
	ArrName       string                  `json:"arr_name,omitempty"`
	MediaID       int                     `json:"media_id,omitzero"`
	State         LegacyNZBHydrationState `json:"state"`
	Attempts      int                     `json:"attempts,omitzero"`
	RetryAt       time.Time               `json:"retry_at,omitzero"`
	LastAttemptAt time.Time               `json:"last_attempt_at,omitzero"`
	UpdatedAt     time.Time               `json:"updated_at"`
	LastError     string                  `json:"last_error,omitempty"`
}

func (s *Storage) SaveLegacyNZBHydration(record *LegacyNZBHydration) error {
	if s == nil || s.legacyNZBHydration == nil {
		return fmt.Errorf("legacy NZB hydration storage is unavailable")
	}
	if record == nil || record.NZBID == "" {
		return fmt.Errorf("legacy NZB hydration record is missing NZB id")
	}
	record.UpdatedAt = time.Now()
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.legacyNZBHydration.Put(record.NZBID, data, nil)
}

func (s *Storage) DeleteLegacyNZBHydration(nzbID string) error {
	if s == nil || s.legacyNZBHydration == nil || nzbID == "" {
		return nil
	}
	err := s.legacyNZBHydration.Delete(nzbID)
	if errors.Is(err, appendstore.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (s *Storage) ListLegacyNZBHydrations() ([]*LegacyNZBHydration, error) {
	if s == nil || s.legacyNZBHydration == nil {
		return nil, fmt.Errorf("legacy NZB hydration storage is unavailable")
	}
	records := make([]*LegacyNZBHydration, 0)
	err := s.legacyNZBHydration.ForEach(func(key string, value []byte) error {
		var record LegacyNZBHydration
		if err := json.Unmarshal(value, &record); err != nil {
			return nil
		}
		if record.NZBID == "" {
			record.NZBID = key
		}
		records = append(records, &record)
		return nil
	})
	return records, err
}
