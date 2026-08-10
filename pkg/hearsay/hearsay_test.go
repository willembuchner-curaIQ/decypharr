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
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
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
	s.ObserveNZB("abc", true)
	s.Close()
	if got := s.RankClients("abc", nil); got != nil {
		t.Fatal("nil service should pass through")
	}
}

type fakeClient struct {
	common.Client
	cfg config.Debrid
}

func (f fakeClient) Config() config.Debrid { return f.cfg }

func TestRankClients(t *testing.T) {
	s := testService(t)
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	rd := fakeClient{cfg: config.Debrid{Provider: "realdebrid", Name: "rd-main"}}
	tb := fakeClient{cfg: config.Debrid{Provider: "torbox", Name: "tb"}}
	clients := []common.Client{rd, tb}

	if got := s.RankClients("ffffffffffffffffffffffffffffffffffffffff", clients); got[0].Config().Name != "rd-main" {
		t.Fatal("unknown subject should keep configured order")
	}

	s.ObserveTorrent("torbox", ih, true)
	if got := s.RankClients(ih, clients); got[0].Config().Name != "tb" {
		t.Fatal("cached provider should rank first")
	}

	s.ObserveTorrent("realdebrid", ih, false)
	got := s.RankClients(ih, clients)
	if got[0].Config().Name != "tb" || got[1].Config().Name != "rd-main" {
		t.Fatal("known-uncached provider should rank last")
	}
}

// TestRankClientsPromotesSingleSource pins the case the daemon's 0.5
// collapse got wrong. Two equally weighted publishers cover the
// namespace and only the older one claims the subject, so confidence
// lands just under the threshold. That provider must still outrank one
// with no claims at all, rather than being pushed below it.
func TestRankClientsPromotesSingleSource(t *testing.T) {
	s := testService(t)
	const ns = "debrid.torbox.cached"
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"

	claimed, err := hsdebrid.New("torbox").Encode(ih)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := hsdebrid.New("torbox").Encode("ffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	ingest := func(key hearsaylib.Key, age time.Duration) {
		t.Helper()
		_, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		g, err := hearsaylib.BuildGeneration(priv, ns, hearsaylib.Bool, 1, time.Now().Add(-age), 1, boolPayload(key))
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
	ingest(claimed, time.Hour)
	ingest(unrelated, 0)

	a, err := s.engine.Query(ns, ih)
	if err != nil || a.Local || a.Sources != 1 {
		t.Fatalf("setup: %+v, %v", a, err)
	}
	if a.Confidence >= 0.5 {
		t.Fatalf("setup: confidence %.4f must fall under the threshold", a.Confidence)
	}

	rd := fakeClient{cfg: config.Debrid{Provider: "realdebrid", Name: "rd-main"}}
	tb := fakeClient{cfg: config.Debrid{Provider: "torbox", Name: "tb"}}
	got := s.RankClients(ih, []common.Client{rd, tb})
	if got[0].Config().Name != "tb" {
		t.Errorf("single-source claim not promoted: order = %s, %s",
			got[0].Config().Name, got[1].Config().Name)
	}
}

func TestObserveAndReport(t *testing.T) {
	s := testService(t)
	const ih = "2c6b6858d61da9543d4231a71db4b1c9264b0685"
	s.ObserveTorrent("realdebrid", ih, true)
	s.ObserveTorrent("unknown-vendor", ih, true)
	s.ReportAdd("realdebrid", ih, false)
	s.ObserveNZB(NZBSubjectFromGroups(map[string]*parser.FileGroup{
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
