package manager

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	usenetpkg "github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

func TestNZBRepairCacheMemoizesCompletedRepair(t *testing.T) {
	cache := newNZBRepairCache()
	calls := 0
	repair := func() (usenetpkg.NZBRepairReport, error) {
		calls++
		return usenetpkg.NZBRepairReport{Articles: 10, MissingArticles: 1, RepairedRanges: 2}, recovery.ErrBudgetExceeded
	}
	first := cache.do("nzb", repair)
	second := cache.do("nzb", repair)
	if first != second || calls != 1 {
		t.Fatalf("outcomes identical=%t calls=%d", first == second, calls)
	}
	if first.report.Articles != 10 || !errors.Is(first.err, recovery.ErrBudgetExceeded) {
		t.Fatalf("unexpected outcome: %+v", first)
	}
	if !first.counted.CompareAndSwap(false, true) || first.counted.CompareAndSwap(false, true) {
		t.Fatal("repair stats should be claimable exactly once")
	}
}

func TestNZBRepairCacheCoalescesConcurrentRepairs(t *testing.T) {
	const callers = 32
	cache := newNZBRepairCache()
	var calls atomic.Int64
	ready := sync.WaitGroup{}
	done := sync.WaitGroup{}
	ready.Add(callers)
	done.Add(callers)
	begin := make(chan struct{})
	release := make(chan struct{})
	started := make(chan struct{})
	outcomes := make([]*nzbRepairOutcome, callers)

	for i := range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-begin
			outcomes[i] = cache.do("nzb", func() (usenetpkg.NZBRepairReport, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return usenetpkg.NZBRepairReport{Articles: 4}, nil
			})
		}()
	}
	ready.Wait()
	close(begin)
	<-started
	close(release)
	done.Wait()

	if calls.Load() != 1 {
		t.Fatalf("repair calls=%d, want 1", calls.Load())
	}
	for i := 1; i < len(outcomes); i++ {
		if outcomes[i] != outcomes[0] {
			t.Fatalf("outcome %d was not shared", i)
		}
	}
}

func TestShouldDeepNZB(t *testing.T) {
	cfg := config.Get()
	previous := cfg.Repair.DeepNZBInterval
	t.Cleanup(func() { cfg.Repair.DeepNZBInterval = previous })
	repair := &Repair{}
	now := time.Now()

	cfg.Repair.DeepNZBInterval = "720h"
	if repair.shouldDeepNZB(time.Time{}, true, false, now) {
		t.Fatal("detect-only run should not deep-repair")
	}
	if !repair.shouldDeepNZB(now, true, true, now) {
		t.Fatal("manual deep audit should override freshness")
	}
	if !repair.shouldDeepNZB(time.Time{}, false, true, now) {
		t.Fatal("never-audited NZB should be due")
	}
	if repair.shouldDeepNZB(now.Add(-719*time.Hour), false, true, now) {
		t.Fatal("fresh PAR2 audit should not be due")
	}
	if !repair.shouldDeepNZB(now.Add(-721*time.Hour), false, true, now) {
		t.Fatal("stale PAR2 audit should be due")
	}
	cfg.Repair.DeepNZBInterval = "0"
	if repair.shouldDeepNZB(time.Time{}, false, true, now) {
		t.Fatal("zero interval should disable periodic deep audits")
	}
}

func TestFilterDueCandidatesHonorsForcedDeepNZBAudit(t *testing.T) {
	store, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	repair := &Repair{manager: &Manager{storage: store}}
	cfg := config.Get()
	previous := cfg.Repair
	t.Cleanup(func() { cfg.Repair = previous })
	cfg.Repair.RecheckInterval = "168h"
	cfg.Repair.DeepNZBInterval = "24h"
	now := time.Now()

	health := []*storage.EntryHealth{
		{EntryName: "nzb-stale", Protocol: config.ProtocolNZB, Status: storage.HealthHealthy, LastCheckedAt: now, LastPAR2AuditAt: now.Add(-25 * time.Hour)},
		{EntryName: "nzb-fresh", Protocol: config.ProtocolNZB, Status: storage.HealthHealthy, LastCheckedAt: now, LastPAR2AuditAt: now.Add(-time.Hour)},
		{EntryName: "torrent", Protocol: config.ProtocolTorrent, Status: storage.HealthHealthy, LastCheckedAt: now, LastPAR2AuditAt: now.Add(-25 * time.Hour)},
	}
	candidates := make(map[string]*candidate, len(health))
	for _, state := range health {
		if err := store.SaveEntryHealth(state); err != nil {
			t.Fatal(err)
		}
		candidates[state.EntryName] = &candidate{name: state.EntryName}
	}

	due, skipped := repair.filterDueCandidates(candidates, RepairRunOptions{}, true)
	if len(due) != 0 || skipped != 3 {
		t.Fatalf("ordinary due=%v skipped=%d", due, skipped)
	}
	due, skipped = repair.filterDueCandidates(candidates, RepairRunOptions{DeepNZB: true}, true)
	if len(due) != 2 || due["nzb-stale"] == nil || due["nzb-fresh"] == nil || skipped != 1 {
		t.Fatalf("forced due=%v skipped=%d", due, skipped)
	}
	due, skipped = repair.filterDueCandidates(candidates, RepairRunOptions{DeepNZB: true}, false)
	if len(due) != 0 || skipped != 3 {
		t.Fatalf("detect-only due=%v skipped=%d", due, skipped)
	}
}

func TestPAR2RepairFailureReason(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		reason     string
		definitive bool
	}{
		{name: "budget", err: recovery.ErrBudgetExceeded, reason: "usenet_par2_budget_exceeded", definitive: true},
		{name: "no parity", err: recovery.ErrNoRecoverySet, reason: "usenet_par2_insufficient", definitive: true},
		{name: "storage", err: recovery.ErrStorageBudget, reason: "usenet_par2_storage_exceeded", definitive: true},
		{name: "layout", err: recovery.ErrLayoutUnavailable, reason: "usenet_par2_unsupported", definitive: true},
		{name: "corrupt", err: recovery.ErrCorrupt, reason: "usenet_par2_corrupt", definitive: true},
		{name: "missing parity article", err: &nntp.Error{Type: nntp.ErrorTypeArticleNotFound}, reason: "usenet_par2_insufficient", definitive: true},
		{name: "connection", err: nntp.NewConnectionError(errors.New("offline"))},
		{name: "cancelled", err: context.Canceled},
		{name: "internal", err: errors.New("unexpected")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reason, definitive := par2RepairFailureReason(test.err)
			if reason != test.reason || definitive != test.definitive {
				t.Fatalf("got (%q, %t), want (%q, %t)", reason, definitive, test.reason, test.definitive)
			}
		})
	}
}
