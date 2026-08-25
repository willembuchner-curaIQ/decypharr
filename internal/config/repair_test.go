package config

import "testing"

func TestRepairDefaultsIncludePeriodicDeepNZBAudit(t *testing.T) {
	var cfg Config
	cfg.applyRepairDefaults()
	if cfg.Repair.DeepNZBInterval != "720h" {
		t.Fatalf("deep NZB interval=%q, want 720h", cfg.Repair.DeepNZBInterval)
	}
	if !(RepairConfig{}).IsZero() {
		t.Fatal("empty repair config should be zero")
	}
	if (RepairConfig{DeepNZBInterval: "0"}).IsZero() {
		t.Fatal("configured deep NZB interval should not be zero")
	}
}
