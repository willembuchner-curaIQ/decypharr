// Package hearsay wires decypharr into the Hearsay network
// (github.com/sirrobot01/hearsay): observations that fall out of work
// decypharr already does — repair probes, add outcomes, import
// availability checks — are recorded, published, and used to fail
// doomed adds fast. Every hook is a nil-safe no-op when the service
// is disabled, so call sites never guard.
package hearsay

import (
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
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

type Service struct {
	engine   *hearsaylib.Hearsay
	log      zerolog.Logger
	debrids  map[string]string // vendor → cached namespace
	uncached map[string]string // vendor → paired denial namespace
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
		log:      log.With().Str("component", "hearsay").Logger(),
		debrids:  map[string]string{},
		uncached: map[string]string{},
		usenets:  map[string]string{},
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
		undom := hsdebrid.NewUncached(vendor)
		s.debrids[vendor] = dom.Namespace()
		s.uncached[vendor] = undom.Namespace()
		domains = append(domains, dom, undom)
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
		Int("namespaces", len(s.namespaces())).
		Bool("publish", s.publish).
		Msg("Participating in hearsay network")
	return nil
}

func (s *Service) namespaces() []string {
	var out []string
	for _, ns := range s.debrids {
		out = append(out, ns)
	}
	for _, ns := range s.uncached {
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
// the file is (or is not) present on the provider. Both paired
// namespaces record it — an absence is a positive claim in the
// uncached namespace, the only form a bloom can publish.
func (s *Service) ObserveTorrent(vendor, infohash string, present bool) {
	if s == nil || infohash == "" {
		return
	}
	vendor = strings.ToLower(vendor)
	ns, ok := s.debrids[vendor]
	if !ok {
		return
	}
	value := 0.0
	if present {
		value = 1
	}
	now := time.Now()
	if err := s.engine.Observe(ns, infohash, value, now); err != nil {
		s.log.Debug().Err(err).Str("ns", ns).Msg("observe failed")
	}
	if err := s.engine.Observe(s.uncached[vendor], infohash, 1-value, now); err != nil {
		s.log.Debug().Err(err).Str("ns", s.uncached[vendor]).Msg("observe failed")
	}
}

// ReportAdd feeds back the outcome of an actual add: instantly served
// from cache or not. Both paired namespaces score their sources from
// the one outcome — an instant add confirms cached claims and refutes
// uncached ones, and each domain's Verify knows its own polarity.
func (s *Service) ReportAdd(vendor, infohash string, instant bool) {
	if s == nil || infohash == "" {
		return
	}
	vendor = strings.ToLower(vendor)
	if _, ok := s.debrids[vendor]; !ok {
		return
	}
	for _, ns := range []string{s.debrids[vendor], s.uncached[vendor]} {
		if err := s.engine.Report(ns, infohash, instant); err != nil {
			s.log.Debug().Err(err).Str("ns", ns).Msg("report failed")
		}
	}
}

// ReportNZB feeds back an import-time completeness check: it records
// local truth and scores every followed source that claimed on the
// subject. A definitive miss is attributed to every configured
// backbone — the availability check established the segments are
// missing on all providers. A success is attributed only when a
// single backbone is configured, because the check does not reveal
// which provider answered.
func (s *Service) ReportNZB(subject string, complete bool) {
	if s == nil || subject == "" {
		return
	}
	if complete && len(s.usenets) != 1 {
		return
	}
	for _, ns := range s.usenets {
		if err := s.engine.Report(ns, subject, complete); err != nil {
			s.log.Debug().Err(err).Str("ns", ns).Msg("report failed")
		}
	}
}

// NZBClaimedIncomplete reports whether local truth or a strong network
// consensus says the post is incomplete on every configured backbone.
// The thresholds are conservative on purpose: a wrong rejection loses
// a repairable release and, because the availability check never runs,
// the mistake is never observed or corrected. Own truth is trusted
// outright; the network needs corroboration, a low completeness mean,
// and fresh confident sources.
func (s *Service) NZBClaimedIncomplete(subject string) bool {
	if s == nil || subject == "" || len(s.usenets) == 0 {
		return false
	}
	for _, ns := range s.usenets {
		a, err := s.engine.Query(ns, subject)
		if err != nil {
			return false
		}
		if a.Local {
			// Trust own negatives for a day: long enough to absorb
			// the *arr's retry storm, short enough that a post marked
			// missing while still propagating gets re-checked. One
			// stat re-verifies after expiry, and a genuinely dead
			// post re-records instantly. ReportNZB writes a miss to
			// every backbone, so one namespace answers for all.
			return a.Value < 0.5 && a.Age <= 24*time.Hour
		}
		if a.Sources < 2 || a.Value > 0.2 || a.Confidence < 0.6 {
			return false
		}
	}
	return true
}

// KnownUncached reports whether a submit to vendor is a known waste:
// the operator's own truth from the last six hours says not cached,
// or the network corroborates a denial in the paired uncached
// namespace while nobody claims cached. A positive claim always wins
// — positives are the high-precision signal — and absence alone never
// gates: it cannot distinguish a dead torrent from a fresh release
// nobody has tried yet. The six hour bound matches the uncached
// domain's half-life, because anyone's uncached download can flip the
// state at any moment. For unknown subjects the submit itself stays
// the oracle.
func (s *Service) KnownUncached(vendor, infohash string) bool {
	if s == nil || infohash == "" {
		return false
	}
	vendor = strings.ToLower(vendor)
	ns, ok := s.debrids[vendor]
	if !ok {
		return false
	}
	a, err := s.engine.Query(ns, infohash)
	if err != nil {
		return false
	}
	if a.Local {
		return a.Value < 0.5 && a.Age <= 6*time.Hour
	}
	if a.Sources > 0 {
		return false
	}
	u, err := s.engine.Query(s.uncached[vendor], infohash)
	// Local truth is written to both namespaces together, so a local
	// answer here means the cached branch above already ruled.
	if err != nil || u.Local {
		return false
	}
	return u.Sources >= 2 && u.Confidence >= 0.6
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
	// zerolog serializes opaque error structs to "{}"; keep the message.
	add := func(key string, value any) {
		if err, ok := value.(error); ok {
			e = e.Str(key, err.Error())
			return
		}
		e = e.Any(key, value)
	}
	for _, a := range h.attrs {
		add(a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		add(a.Key, a.Value.Any())
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
