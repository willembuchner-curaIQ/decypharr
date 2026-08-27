package config

import "testing"

func TestHearsayIsZero(t *testing.T) {
	if !(Hearsay{}).IsZero() {
		t.Fatal("empty Hearsay config should be zero")
	}
	if (Hearsay{Participate: true}).IsZero() {
		t.Fatal("explicit participation should not be zero")
	}
}

func TestHearsayEnvironment(t *testing.T) {
	t.Setenv("DECYPHARR_HEARSAY__PARTICIPATE", "true")
	t.Setenv("DECYPHARR_HEARSAY__PUBLISH", "true")
	t.Setenv("DECYPHARR_HEARSAY__ADVICE_MODE", "ACTIVE")
	t.Setenv("DECYPHARR_HEARSAY__MIN_SUPPORT", "0.7")
	t.Setenv("DECYPHARR_HEARSAY__MIN_EVIDENCE", "0.4")
	t.Setenv("DECYPHARR_HEARSAY__MIN_SOURCES", "3")
	t.Setenv("DECYPHARR_HEARSAY__MAX_STORAGE_BYTES", "2048")
	t.Setenv("DECYPHARR_HEARSAY__MAX_FEEDS_PER_NAMESPACE", "32")
	t.Setenv("DECYPHARR_HEARSAY__FOLLOW", "ed25519:aa, ed25519:bb")

	var cfg Config
	cfg.applyHearsayEnvVars()
	h := cfg.Hearsay
	if !h.Participate || !h.Publish || h.AdviceMode != "active" {
		t.Fatalf("consent and mode = %+v", h)
	}
	if h.MinSupport != 0.7 || h.MinEvidence != 0.4 || h.MinSources != 3 {
		t.Fatalf("policy = %+v", h)
	}
	if h.MaxStorageBytes != 2048 || h.MaxFeedsPerNamespace != 32 {
		t.Fatalf("limits = %+v", h)
	}
	if len(h.Follow) != 2 || h.Follow[1] != "ed25519:bb" {
		t.Fatalf("follow = %v", h.Follow)
	}
}
