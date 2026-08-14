package hearsay

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"
	"time"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"

	hearsaylib "github.com/sirrobot01/hearsay"
	hsdebrid "github.com/sirrobot01/hearsay/debrid"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

func testService(t *testing.T) *Service {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	cfg := &config.Config{
		Debrids: []config.Debrid{
			{Provider: "realdebrid", Name: "rd-main"},
			{Provider: "torbox", Name: "tb"},
		},
	}
	cfg.Usenet.Providers = []config.UsenetProvider{{Host: "news.example", Backbone: "omicron"}}
	s, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("service unexpectedly disabled")
	}
	t.Cleanup(s.Close)
	return s
}

func TestDisabledIsInert(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	cfg := &config.Config{Debrids: []config.Debrid{{Provider: "realdebrid"}}}
	cfg.Hearsay.Disabled = true
	s, err := New(cfg, zerolog.Nop())
	if err != nil || s != nil {
		t.Fatalf("want nil service, got %v, %v", s, err)
	}
	s.ObserveTorrent("realdebrid", "abc", true)
	s.ReportAdd("realdebrid", "abc", true)
	s.ReportNZB("abc", true)
	if s.NZBClaimedIncomplete("abc") {
		t.Fatal("nil service should never claim incomplete")
	}
	if s.KnownUncached("realdebrid", "abc") {
		t.Fatal("nil service should never gate a submit")
	}
	s.Close()
}

// TestNZBClaimedIncompleteExpires pins that a local miss gates the
// retry storm but not forever: a post marked missing while it was
// still propagating must get another stat once the window passes, or
// the gate prevents the check that would correct it.
func TestNZBClaimedIncompleteExpires(t *testing.T) {
	s := testService(t)
	const subject = "2c6b6858d61da9543d4231a71db4b1c9264b06852c6b6858d61da9543d4231a7"

	if s.NZBClaimedIncomplete(subject) {
		t.Fatal("unknown subject must not gate")
	}
	s.ReportNZB(subject, false)
	if !s.NZBClaimedIncomplete(subject) {
		t.Fatal("fresh local miss must gate")
	}
	s.ReportNZB(subject, true)
	if s.NZBClaimedIncomplete(subject) {
		t.Fatal("a complete import must clear the gate")
	}

	// The store keeps the newest observation, so age a subject by
	// recording it stale outright rather than overwriting a fresh one.
	const stale = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := s.engine.Observe("usenet.omicron.complete", stale, 0, time.Now().Add(-25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if s.NZBClaimedIncomplete(stale) {
		t.Fatal("stale local miss must not gate; one stat should re-verify")
	}
}

func TestKnownUncached(t *testing.T) {
	s := testService(t)
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	if s.KnownUncached("realdebrid", ih) {
		t.Fatal("unknown subject must not gate a submit")
	}
	s.ObserveTorrent("realdebrid", ih, false)
	if !s.KnownUncached("realdebrid", ih) {
		t.Fatal("fresh local negative must gate")
	}
	if s.KnownUncached("torbox", ih) {
		t.Fatal("another vendor's truth must not gate")
	}
	s.ObserveTorrent("realdebrid", ih, true)
	if s.KnownUncached("realdebrid", ih) {
		t.Fatal("local positive must not gate")
	}
}

// TestKnownUncachedNetworkDenial checks the network path of the
// submit gate: corroborated fresh denials gate a subject the operator
// never touched, and any positive cached claim overrides them.
func TestKnownUncachedNetworkDenial(t *testing.T) {
	s := testService(t)
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	ingest := func(ns string, key hearsaylib.Key) {
		t.Helper()
		_, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		g, err := hearsaylib.BuildGeneration(priv, ns, hearsaylib.Bool, 1, time.Now(), 1, boolPayload(key))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := g.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.engine.Ingest(raw); err != nil {
			t.Fatal(err)
		}
	}
	denialKey, err := hsdebrid.NewUncached("realdebrid").Encode(ih)
	if err != nil {
		t.Fatal(err)
	}

	ingest("debrid.realdebrid.uncached", denialKey)
	if s.KnownUncached("realdebrid", ih) {
		t.Fatal("a single denying source must not gate a submit")
	}
	ingest("debrid.realdebrid.uncached", denialKey)
	if !s.KnownUncached("realdebrid", ih) {
		t.Fatal("two corroborating fresh denials must gate")
	}

	cachedKey, err := hsdebrid.New("realdebrid").Encode(ih)
	if err != nil {
		t.Fatal(err)
	}
	ingest("debrid.realdebrid.cached", cachedKey)
	if s.KnownUncached("realdebrid", ih) {
		t.Fatal("a positive cached claim must override denials")
	}
}

func TestObserveAndReport(t *testing.T) {
	s := testService(t)
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	s.ObserveTorrent("realdebrid", ih, true)
	s.ObserveTorrent("unknown-vendor", ih, true)
	s.ReportAdd("realdebrid", ih, false)
	s.ReportNZB(NZBSubjectFromGroups(map[string]*parser.FileGroup{
		"a": {
			Type:  storage.NZBFileTypeMedia,
			Files: []nzbparser.NzbFile{{Segments: nzbparser.NzbSegments{{Id: "m@x"}}}},
		},
	}), true)

	a, err := s.engine.Query("debrid.realdebrid.cached", ih)
	if err != nil || !a.Local {
		t.Fatalf("truth not recorded: %+v, %v", a, err)
	}
	if a.Value != 0 {
		t.Fatal("report outcome should have overwritten the observation")
	}
}

// A follow list is an allowlist: feeds a previous unrestricted run
// discovered must stop answering queries once the operator narrows to
// an explicit set.
func TestFollowDropsUnlistedFeeds(t *testing.T) {
	dir := t.TempDir()
	config.SetConfigPath(dir)
	cfg := &config.Config{Debrids: []config.Debrid{{Provider: "realdebrid", Name: "rd"}}}

	// First run: no follow list, so a discovered feed is retained.
	open, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	const ns = "debrid.realdebrid.cached"
	const infohash = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := hsdebrid.New("realdebrid").Encode(infohash)
	if err != nil {
		t.Fatal(err)
	}
	g, err := hearsaylib.BuildGeneration(priv, ns, hearsaylib.Bool, 1, time.Now(), 1, boolPayload(key))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := g.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open.engine.Ingest(raw); err != nil {
		t.Fatal(err)
	}
	if a, _ := open.engine.Query(ns, infohash); a.Sources != 1 {
		t.Fatalf("setup: sources = %d", a.Sources)
	}
	open.Close()

	// Second run: follow a different publisher.
	other, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Hearsay.Follow = []string{"ed25519:" + hex.EncodeToString(other)}
	s, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if a, _ := s.engine.Query(ns, infohash); a.Sources != 0 {
		t.Fatalf("feed outside the follow list still answers: %+v", a)
	}
	if got := s.engine.Feeds(ns); len(got) != 0 {
		t.Fatalf("feeds = %v", got)
	}
}

// boolPayload mirrors the library's bloom encoding for one key.
func boolPayload(key hearsaylib.Key) []byte {
	m := uint64(math.Ceil(-math.Log(0.01) / (math.Ln2 * math.Ln2)))
	m = (m + 7) &^ 7
	k := max(uint32(math.Round(float64(m)*math.Ln2)), 1)
	bits := make([]byte, m/8)
	h1 := binary.BigEndian.Uint64(key[0:8])
	h2 := binary.BigEndian.Uint64(key[8:16])
	for i := uint64(0); i < uint64(k); i++ {
		p := (h1 + i*h2) % m
		bits[p/8] |= 1 << (p % 8)
	}
	out := make([]byte, 4, 4+len(bits))
	binary.BigEndian.PutUint32(out, k)
	return append(out, bits...)
}
