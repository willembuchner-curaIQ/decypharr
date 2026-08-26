package storage

import (
	"testing"
	"time"
)

func TestLegacyNZBHydrationStatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC)
	record := &LegacyNZBHydration{
		NZBID:     "legacy-id",
		ArrName:   "Sonarr",
		MediaID:   42,
		State:     LegacyNZBHydrationRetrying,
		Attempts:  3,
		RetryAt:   retryAt,
		LastError: "connection reset by peer",
	}
	if err := store.SaveLegacyNZBHydration(record); err != nil {
		t.Fatal(err)
	}
	if record.UpdatedAt.IsZero() {
		t.Fatal("save did not stamp UpdatedAt")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records, err := store.ListLegacyNZBHydrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1", len(records))
	}
	got := records[0]
	if got.NZBID != record.NZBID || got.ArrName != record.ArrName || got.MediaID != record.MediaID ||
		got.State != record.State || got.Attempts != record.Attempts || !got.RetryAt.Equal(retryAt) || got.LastError != record.LastError {
		t.Fatalf("persisted record = %+v, want %+v", got, record)
	}
	if err := store.DeleteLegacyNZBHydration(record.NZBID); err != nil {
		t.Fatal(err)
	}
	records, err = store.ListLegacyNZBHydrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("record count after delete = %d, want 0", len(records))
	}
	if err := store.DeleteLegacyNZBHydration(record.NZBID); err != nil {
		t.Fatalf("repeated delete = %v", err)
	}
}
