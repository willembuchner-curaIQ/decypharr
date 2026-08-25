package arr

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestSyncFromConfigAppliesValidHost(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	arrs := NewStorage()
	arrs.AddOrUpdate(New("whisparr", "http://old.example", "old-token", false, nil, "", string(SourceAuto)))
	arrs.SyncFromConfig([]config.Arr{{
		Name:   "whisparr",
		Host:   "http://new.example",
		Token:  "new-token",
		Source: string(SourceAuto),
	}})

	got := arrs.Get("whisparr")
	if got == nil || got.Host != "http://new.example" || got.Token != "new-token" {
		t.Fatalf("synced Arr = %#v", got)
	}
}

func TestSyncFromConfigPreservesResolvedHostForInvalidUpdate(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	arrs := NewStorage()
	arrs.AddOrUpdate(New("whisparr", "http://resolved.example", "old-token", false, nil, "", string(SourceAuto)))
	arrs.SyncFromConfig([]config.Arr{{
		Name:   "whisparr",
		Host:   "not-a-url",
		Token:  "new-token",
		Source: string(SourceAuto),
	}})

	got := arrs.Get("whisparr")
	if got == nil || got.Host != "http://resolved.example" || got.Token != "new-token" {
		t.Fatalf("synced Arr = %#v", got)
	}
}
