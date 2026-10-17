package daemon

// Tests for the two bounds a cache-filling scan owes its CALLERS, as opposed to the
// bounds it owes itself (scanBudget, scanFailTTL - see scanoutcome_test.go):
//
//   - waiting for another call's in-flight scan must be abandonable at the caller's
//     budget, or the degrade path buildStatus exists for is unreachable while any pass
//     is running;
//   - a refill pass's remembered failures must expire together with the pass, not with
//     the individual scans inside it, or the negative cache cannot stop the restarts it
//     was added to stop on a multi-view store.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
)

// TestCountsFlightWaitIsBoundedByCallerBudget pins the abandonable single-flight wait.
//
// The per-view flight used to be a sync.Mutex, so a `status` call that arrived while
// another call's committed pass was scanning blocked for the whole scan - up to
// scanBudget (2 minutes) against an 8s IPC handler budget. The IPC layer abandoned the
// handler and the operator got the generic "handler did not finish", never the stale
// counts beside a live Paused/DiskFull/Stuck that the cohort stamp made the whole point of the
// verb. On exactly the stores the cache exists for a pass is in flight most of the
// time, so most calls still failed.
func TestCountsFlightWaitIsBoundedByCallerBudget(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	d.viewScanHook = func(uint64) {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	// Whatever happens below, the held scan must be let go so the daemon's own
	// goroutine can finish before the test does.
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo()

	holder := make(chan struct{})
	go func() {
		defer close(holder)
		if _, err := d.buildStatus(context.Background()); err != nil {
			t.Errorf("buildStatus (the call holding the flight): %v", err)
		}
	}()
	<-entered

	// The second caller's client is gone long before the held scan can finish.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waiter := make(chan ipc.StatusReport, 1)
	go func() {
		rep, err := d.buildStatus(ctx)
		if err != nil {
			t.Errorf("buildStatus (the waiter): %v (the counts degrade, they do not fail the verb)", err)
		}
		waiter <- rep
	}()

	select {
	case rep := <-waiter:
		if len(rep.Views) != 1 || !rep.Views[0].CountsStale {
			t.Fatalf("waiter's report = %+v, want one view marked CountsStale", rep.Views)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a `status` call whose budget expired is still blocked on another call's " +
			"in-flight scan; the IPC layer will abandon it and the degrade path is never reached")
	}

	letGo()
	<-holder
}

// TestRefillFailureCohortOutlivesItsPass pins the failure half of the cohort stamp.
//
// scanFailTTL is a multiple of ONE view's scanBudget, but a refill pass over N views
// runs for up to N × scanBudget. Stamped where they happened, the early views' failures
// expired before the pass that produced them had even finished, so the very next call
// restarted their doomed walks - the near-continuous doomed scanning (and its
// full-length bbolt read transactions against the sync data plane) that the negative
// cache exists to stop, back again on exactly the multi-view stores that need it.
func TestRefillFailureCohortOutlivesItsPass(t *testing.T) {
	const views = 3
	st := openTestStore(t)
	seedCountedViews(t, st, views)

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	now := time.Unix(1_750_000_000, 0)
	d.now = func() time.Time { return now }

	// A budget no walk can fit, so every view's scan fails in the budget class and is
	// remembered; each one is modelled as consuming two thirds of scanFailTTL, so a pass
	// over three views far outlasts a per-scan memory.
	restore := scanBudget
	scanBudget = -time.Second
	t.Cleanup(func() { scanBudget = restore })

	scans := 0
	d.viewScanHook = func(uint64) {
		scans++
		now = now.Add(2 * scanFailTTL / 3)
	}

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (the failing pass): %v", err)
	}
	if scans != views {
		t.Fatalf("the pass ran %d scans, want %d", scans, views)
	}
	for _, v := range rep.Views {
		if !v.CountsStale {
			t.Errorf("view %d is not marked CountsStale after a failed scan", v.ID)
		}
	}

	// The operator's retry, landing the instant the pass finished: every view's failure
	// is still remembered, so nothing is re-walked.
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (retry right after the pass): %v", err)
	}
	if scans != views {
		t.Errorf("the retry restarted %d doomed scans; want 0 - the pass's failures must expire "+
			"with the PASS, not with the individual scans inside it", scans-views)
	}
	for _, v := range rep.Views {
		if !v.CountsStale {
			t.Errorf("view %d is not marked CountsStale on the retry", v.ID)
		}
	}

	// Past the negative TTL - measured from the pass's completion - the scans are
	// retried, and now they fit.
	scanBudget = restore
	now = now.Add(scanFailTTL)
	if _, err = d.buildStatus(context.Background()); err != nil {
		t.Fatalf("buildStatus past the negative TTL: %v", err)
	}
	if scans != 2*views {
		t.Errorf("scans past the negative TTL = %d, want %d (every view must be retried)", scans, 2*views)
	}
}
