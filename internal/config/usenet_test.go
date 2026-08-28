package config

import (
	"math"
	"testing"
)

func boolPointer(v bool) *bool { return &v }

func TestUsenetProviderID(t *testing.T) {
	a := UsenetProvider{Host: "news.example.com", Port: 563, Username: "alice"}
	b := UsenetProvider{Host: "news.example.com", Port: 563, Username: "bob"}
	c := UsenetProvider{Host: "news.example.com", Port: 119, Username: "alice"}

	if a.ID() == b.ID() {
		t.Fatal("same host with different accounts must have distinct IDs")
	}
	if a.ID() == c.ID() {
		t.Fatal("same host with different ports must have distinct IDs")
	}
	if a.ID() != (UsenetProvider{Host: "news.example.com", Port: 563, Username: "alice"}).ID() {
		t.Fatal("ID must be deterministic")
	}
}

func TestValidateUsenetRejectsDuplicateProvider(t *testing.T) {
	dup := UsenetProvider{Host: "news.example.com", Port: 563, Username: "alice", Password: "pw"}
	if err := validateUsenet([]UsenetProvider{dup, dup}); err == nil {
		t.Fatal("expected duplicate provider to be rejected")
	}

	// Dual accounts on the same host are legitimate and must pass.
	second := dup
	second.Username = "bob"
	if err := validateUsenet([]UsenetProvider{dup, second}); err != nil {
		t.Fatalf("dual-account setup rejected: %v", err)
	}
}

func TestUsenetDiskPathSelectsBufferStorage(t *testing.T) {
	var c Config
	c.updateUsenetConfig()
	if c.Usenet.DiskPath != "" {
		t.Fatalf("default disk path = %q, want empty", c.Usenet.DiskPath)
	}
	if c.Usenet.UsesDiskBuffer() {
		t.Fatal("empty disk path should use memory buffering")
	}

	c.Usenet.DiskPath = "  "
	if c.Usenet.UsesDiskBuffer() {
		t.Fatal("whitespace disk path should use memory buffering")
	}

	c.Usenet.DiskPath = "/cache/usenet"
	if !c.Usenet.UsesDiskBuffer() {
		t.Fatal("non-empty disk path should use disk buffering")
	}
}

func TestPAR2RepairDefaultsAreEnabledAndBounded(t *testing.T) {
	p := PAR2Repair{}
	if !p.IsEnabled() {
		t.Fatal("omitted PAR2 config should enable bounded on-demand repair")
	}
	if got, want := p.ExtraDownloadBudget(20<<30), int64(512<<20); got != want {
		t.Fatalf("absolute budget = %d, want %d", got, want)
	}
	if got, want := p.ExtraDownloadBudget(1<<30), int64((1<<30)/10); got != want {
		t.Fatalf("percentage budget = %d, want %d", got, want)
	}
	if got, want := p.StorageBytes(), int64(8<<30); got != want {
		t.Fatalf("storage budget = %d, want %d", got, want)
	}
}

func TestPAR2RepairCanBeDisabledAndCannotRequestFullNZB(t *testing.T) {
	disabled := PAR2Repair{Enabled: boolPointer(false)}
	if got := disabled.ExtraDownloadBudget(10 << 30); got != 0 {
		t.Fatalf("disabled repair budget = %d, want 0", got)
	}

	p := PAR2Repair{MaxDownloadPercent: 100, MaxDownloadBytes: "100GB"}
	if got, want := p.ExtraDownloadBudget(4<<30), int64(1<<30); got != want {
		t.Fatalf("hard-capped budget = %d, want %d", got, want)
	}
	if got, want := p.ExtraDownloadBudget(math.MaxInt64), int64(100<<30); got != want {
		t.Fatalf("overflow-safe absolute budget = %d, want %d", got, want)
	}
}

func TestApplyUsenetEnvVarsDiskPathAndPAR2(t *testing.T) {
	t.Setenv("DECYPHARR_USENET__DISK_PATH", "/cache/usenet")
	t.Setenv("DECYPHARR_USENET__PAR2__ENABLED", "false")
	t.Setenv("DECYPHARR_USENET__PAR2__MAX_DOWNLOAD_PERCENT", "7")
	t.Setenv("DECYPHARR_USENET__PAR2__MAX_DOWNLOAD_BYTES", "64MB")
	t.Setenv("DECYPHARR_USENET__PAR2__MAX_STORAGE", "2GB")

	var c Config
	c.applyUsenetEnvVars()
	if c.Usenet.DiskPath != "/cache/usenet" {
		t.Fatalf("environment disk path = %q, want /cache/usenet", c.Usenet.DiskPath)
	}
	if c.Usenet.PAR2.IsEnabled() {
		t.Fatal("environment override did not disable PAR2 repair")
	}
	if got := c.Usenet.PAR2.DownloadPercent(); got != 7 {
		t.Fatalf("download percent = %d, want 7", got)
	}
	if got, want := c.Usenet.PAR2.DownloadBytes(), int64(64<<20); got != want {
		t.Fatalf("download bytes = %d, want %d", got, want)
	}
	if got, want := c.Usenet.PAR2.StorageBytes(), int64(2<<30); got != want {
		t.Fatalf("storage bytes = %d, want %d", got, want)
	}
}
