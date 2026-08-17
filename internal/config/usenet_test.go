package config

import "testing"

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
