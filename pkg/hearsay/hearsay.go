// Package hearsay wires decypharr into the Hearsay network
// (github.com/sirrobot01/hearsay): observations that fall out of work
// decypharr already does — repair probes, add outcomes, import
// availability checks — are recorded, published, and used to rank
// debrid candidates. Every hook is a nil-safe no-op when the service
// is disabled, so call sites never guard.
package hearsay

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	hearsaylib "github.com/sirrobot01/hearsay"
	hsdebrid "github.com/sirrobot01/hearsay/debrid"
	"github.com/sirrobot01/hearsay/transport"
	hsusenet "github.com/sirrobot01/hearsay/usenet"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

type Service struct {
	engine   *hearsaylib.Hearsay
	log      zerolog.Logger
	debrids  map[string]string // vendor → namespace
	usenets  map[string]string // backbone → namespace
	publish  bool
	interval time.Duration
	port     int
	gossip   int
	follow   []string

	mu        sync.Mutex
	node      *transport.Node
	closeOnce sync.Once
}

// New builds the service from configuration. It returns (nil, nil)
// when hearsay is disabled or no observable domain is configured; a
// nil *Service is safe to use everywhere.
func New(cfg *config.Config, log zerolog.Logger) (*Service, error) {
	if cfg.Hearsay.Disabled {
		return nil, nil
	}
	s := &Service{
		log:     log.With().Str("component", "hearsay").Logger(),
		debrids: map[string]string{},
		usenets: map[string]string{},
		publish: !cfg.Hearsay.NoPublish,
		port:    cfg.Hearsay.Port,
		gossip:  cfg.Hearsay.GossipPort,
	}
	var domains []hearsaylib.Domain
	for _, d := range cfg.Debrids {
		vendor := strings.ToLower(strings.TrimSpace(d.Provider))
		if vendor == "" {
			continue
		}
		if _, dup := s.debrids[vendor]; dup {
			continue
		}
		dom := hsdebrid.New(vendor)
		s.debrids[vendor] = dom.Namespace()
		domains = append(domains, dom)
	}
	for _, p := range cfg.Usenet.Providers {
		// The explicit failover backbone wins; otherwise infer it from
		// the host so operators do not need to know their backbone.
		backbone := strings.ToLower(strings.TrimSpace(p.Backbone))
		if backbone == "" {
			backbone = hsusenet.InferBackbone(p.Host)
		}
		if backbone == "" {
			continue
		}
		if _, dup := s.usenets[backbone]; dup {
			continue
		}
		dom := hsusenet.New(backbone)
		s.usenets[backbone] = dom.Namespace()
		domains = append(domains, dom)
	}
	if len(domains) == 0 {
		return nil, nil
	}
	if cfg.Hearsay.Interval != "" {
		interval, err := time.ParseDuration(cfg.Hearsay.Interval)
		if err != nil {
			return nil, fmt.Errorf("hearsay interval: %w", err)
		}
		s.interval = interval
	}
	for _, f := range cfg.Hearsay.Follow {
		s.follow = append(s.follow, strings.ToLower(strings.TrimPrefix(strings.TrimSpace(f), "ed25519:")))
	}
	engine, err := hearsaylib.New(filepath.Join(config.GetMainPath(), "hearsay"), domains...)
	if err != nil {
		return nil, err
	}
	s.engine = engine
	// A non-empty follow list is an allowlist, not a set of extra seeds.
	// Retained generations answer queries whether or not the network is
	// running, so drop what an earlier, unrestricted run discovered.
	// Failing here disables participation, which is the safe outcome:
	// better no hints than hints from a publisher the operator dropped.
	if len(s.follow) > 0 {
		self := hex.EncodeToString(engine.Identity())
		for _, ns := range s.namespaces() {
			for _, feed := range engine.Feeds(ns) {
				if feed == self || slices.Contains(s.follow, feed) {
					continue
				}
				if err := engine.Forget(ns, feed); err != nil {
					engine.Close()
					return nil, fmt.Errorf("hearsay: dropping feed outside the follow list: %w", err)
				}
				s.log.Debug().Str("ns", ns).Str("feed", feed[:8]).Msg("dropped feed outside the follow list")
			}
		}
	}
	return s, nil
}

// Start joins the network: seed and publish own generations, discover
// and fetch others'. Runs until ctx is done.
func (s *Service) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}
	node, err := transport.Listen(filepath.Join(config.GetMainPath(), "hearsay", "swarm"), s.port, s.gossip)
	if err != nil {
		return fmt.Errorf("hearsay transport: %w", err)
	}
	s.mu.Lock()
	s.node = node
	s.mu.Unlock()
	// With a follow list the node accepts nothing else: no DHT
	// announce, and no keys from the gossip handshake. Own key included,
	// or the node cannot advertise its own feed.
	if len(s.follow) > 0 {
		node.Allow(append(slices.Clone(s.follow), hex.EncodeToString(s.engine.Identity()))...)
	}
	for _, ns := range s.namespaces() {
		node.Track(ns)
		for _, key := range s.follow {
			node.AddFeed(ns, key)
		}
	}
	syncer := &transport.Syncer{
		Engine:   s.engine,
		Node:     node,
		Publish:  s.publish,
		Interval: s.interval,
		Log:      slog.New(zerologHandler{log: s.log}),
	}
	go syncer.Run(ctx)
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	s.log.Info().
		Str("identity", "ed25519:"+hex.EncodeToString(s.engine.Identity())).
		Int("namespaces", len(s.debrids)+len(s.usenets)).
		Bool("publish", s.publish).
		Msg("Participating in hearsay network")
	return nil
}

func (s *Service) namespaces() []string {
	var out []string
	for _, ns := range s.debrids {
		out = append(out, ns)
	}
	for _, ns := range s.usenets {
		out = append(out, ns)
	}
	return out
}

func (s *Service) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		node := s.node
		s.mu.Unlock()
		if node != nil {
			node.Close()
		}
		if err := s.engine.Close(); err != nil {
			s.log.Debug().Err(err).Msg("hearsay close")
		}
	})
}

// Status reports participation state for the API and UI.
type Status struct {
	Enabled   bool             `json:"enabled"`
	Identity  string           `json:"identity,omitempty"`
	Publish   bool             `json:"publish,omitempty"`
	Feeds     map[string]int   `json:"feeds,omitempty"`
	Transport *transport.Stats `json:"transport,omitempty"`
}

func (s *Service) Status() Status {
	if s == nil {
		return Status{}
	}
	st := Status{
		Enabled:  true,
		Identity: "ed25519:" + hex.EncodeToString(s.engine.Identity()),
		Publish:  s.publish,
		Feeds:    map[string]int{},
	}
	for _, ns := range s.namespaces() {
		st.Feeds[ns] = len(s.engine.Feeds(ns))
	}
	s.mu.Lock()
	node := s.node
	s.mu.Unlock()
	if node != nil {
		stats := node.Stats()
		st.Transport = &stats
	}
	return st
}

// ObserveTorrent records ground truth learned from a repair probe:
// the file is (or is not) present on the provider.
func (s *Service) ObserveTorrent(vendor, infohash string, present bool) {
	if s == nil || infohash == "" {
		return
	}
	ns, ok := s.debrids[strings.ToLower(vendor)]
	if !ok {
		return
	}
	value := 0.0
	if present {
		value = 1
	}
	if err := s.engine.Observe(ns, infohash, value, time.Now()); err != nil {
		s.log.Debug().Err(err).Str("ns", ns).Msg("observe failed")
	}
}

// ReportAdd feeds back the outcome of an actual add: instantly served
// from cache or not. This also scores every followed source that
// claimed on the subject.
func (s *Service) ReportAdd(vendor, infohash string, instant bool) {
	if s == nil || infohash == "" {
		return
	}
	ns, ok := s.debrids[strings.ToLower(vendor)]
	if !ok {
		return
	}
	if err := s.engine.Report(ns, infohash, instant); err != nil {
		s.log.Debug().Err(err).Str("ns", ns).Msg("report failed")
	}
}

// ObserveNZB records an import-time completeness observation. A
// definitive miss is attributed to every configured backbone — the
// availability check established the segments are missing on all
// providers. A success is attributed only when a single backbone is
// configured, because the check does not reveal which provider
// answered.
func (s *Service) ObserveNZB(subject string, complete bool) {
	if s == nil || subject == "" {
		return
	}
	if complete && len(s.usenets) != 1 {
		return
	}
	value := 0.0
	if complete {
		value = 1
	}
	for _, ns := range s.usenets {
		if err := s.engine.Observe(ns, subject, value, time.Now()); err != nil {
			s.log.Debug().Err(err).Str("ns", ns).Msg("observe failed")
		}
	}
}

// RankClients orders debrid candidates by cache confidence for the
// subject, most-likely-cached first. The sort is stable, so subjects
// nobody has claims on keep the configured order. Queries are local
// and cost microseconds.
func (s *Service) RankClients(infohash string, clients []common.Client) []common.Client {
	if s == nil || infohash == "" || len(clients) < 2 {
		return clients
	}
	scores := make([]float64, len(clients))
	scored := false
	for i, c := range clients {
		ns, ok := s.debrids[strings.ToLower(c.Config().Provider)]
		if !ok {
			continue
		}
		a, err := s.engine.Query(ns, infohash)
		if err != nil || (a.Sources == 0 && !a.Local) {
			continue
		}
		// Local truth records both outcomes, so its value is a real
		// boolean and a negative score is meaningful. A network answer
		// is a bloom hit: evidence of cache, never a denial, which the
		// Sources check above already settled. Scoring it through the
		// daemon's 0.5 collapse would rank a lone claiming source
		// (0.49 against two equally fresh feeds) below a provider that
		// has no claims on the subject at all.
		if a.Local && a.Value < 0.5 {
			scores[i] = -a.Confidence
		} else {
			scores[i] = a.Confidence
		}
		scored = true
	}
	if !scored {
		return clients
	}
	order := make([]int, len(clients))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(x, y int) int {
		return cmp.Compare(scores[y], scores[x])
	})
	ranked := make([]common.Client, len(clients))
	for i, idx := range order {
		ranked[i] = clients[idx]
	}
	return ranked
}

// zerologHandler maps slog records from the hearsay syncer onto
// decypharr's zerolog logger, levels and attributes intact.
type zerologHandler struct {
	log   zerolog.Logger
	attrs []slog.Attr
}

func (h zerologHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h zerologHandler) Handle(_ context.Context, r slog.Record) error {
	var e *zerolog.Event
	switch {
	case r.Level >= slog.LevelError:
		e = h.log.Error()
	case r.Level >= slog.LevelWarn:
		e = h.log.Warn()
	case r.Level >= slog.LevelInfo:
		e = h.log.Info()
	default:
		e = h.log.Debug()
	}
	for _, a := range h.attrs {
		e = e.Any(a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		e = e.Any(a.Key, a.Value.Any())
		return true
	})
	e.Msg(r.Message)
	return nil
}

func (h zerologHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.attrs = append(slices.Clip(h.attrs), attrs...)
	return h
}

func (h zerologHandler) WithGroup(string) slog.Handler { return h }

// NZBSubjectFromGroups derives the subject from the parsed file groups,
// which is the only form available before Process runs. Process is what
// fills in NZB.Files, so deriving the subject from the NZB any earlier
// hashes an empty list and yields "".
func NZBSubjectFromGroups(groups map[string]*parser.FileGroup) string {
	var ids []string
	for _, g := range groups {
		// Par2 only: the library spec excludes those and nothing else,
		// because "incidental" file classes like nfo or txt are local
		// policy that another implementation cannot reproduce, and any
		// disagreement yields a different subject for the same post.
		if g == nil || g.Type == storage.NZBFileTypePar2 {
			continue
		}
		for i := range g.Files {
			for _, seg := range g.Files[i].Segments {
				if seg.Id != "" {
					ids = append(ids, seg.Id)
				}
			}
		}
	}
	return subjectFromIDs(ids)
}

func subjectFromIDs(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
