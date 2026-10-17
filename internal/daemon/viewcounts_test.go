package daemon

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
)

// seedCountedViews gives n views one StateConflict baseline and one durable
// keep-both conflict record each, so every view's `status` counts are 1/1 and both
// per-view walks have something to decode.
func seedCountedViews(t *testing.T, st *store.Store, n int) {
	t.Helper()
	for id := uint64(1); id <= uint64(n); id++ {
		if err := st.PutView(id, store.View{Name: fmt.Sprintf("v%d", id)}); err != nil {
			t.Fatalf("PutView %d: %v", id, err)
		}
		b := store.Baseline{FileID: fmt.Sprintf("f%d", id), FileType: "file", State: store.StateConflict}
		if err := st.PutBaseline(id, "a.txt", b); err != nil {
			t.Fatalf("PutBaseline %d: %v", id, err)
		}
		c := store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt"}
		if err := st.PutConflict(id, c); err != nil {
			t.Fatalf("PutConflict %d: %v", id, err)
		}
	}
}

// TestBuildStatusCountsCacheConvergesAcrossViews pins the convergence fix: a
// refill pass the daemon has entered fills EVERY stale view and stamps them as one
// cohort, so `status` converges on a multi-view store whose per-view scan outruns
// the IPC handler budget.
//
// Abandoning the assembly at the next view boundary advanced exactly ONE view's
// stamp per call, and the stamps landed sequentially (S, 2S, … N·S) while the report
// needs all N fresh at once. With a per-view walk slower than viewCountsCacheTTL
// (millions of baselines on a cold disk) the earliest stamps expired before the last
// view was ever reached: with three views the third one never got scanned at all and
// every single `synosync status` returned "handler did not finish" forever -
// destroying Stuck/Disk-full/Paused observability on exactly the stores the cache
// was built for.
//
// The clock is virtual: each per-view scan is modelled as taking TTL+5s, and the
// handler context is cancelled the moment the first scan of a call begins (the 8s
// budget against a 35s walk).
func TestBuildStatusCountsCacheConvergesAcrossViews(t *testing.T) {
	const views = 3
	st := openTestStore(t)
	seedCountedViews(t, st, views)

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	now := time.Unix(1_750_000_000, 0)
	d.now = func() time.Time { return now }

	scans := 0
	var abandon context.CancelFunc
	d.viewScanHook = func(uint64) {
		scans++
		if abandon != nil {
			// The IPC layer abandons the handler while this scan is running.
			abandon()
		}
		now = now.Add(viewCountsCacheTTL + 5*time.Second)
	}

	// First call: entered with a live budget, abandoned during the first view's scan.
	ctx, cancel := context.WithCancel(context.Background())
	abandon = cancel
	rep, err := d.buildStatus(ctx)
	cancel()
	abandon = nil
	if err != nil {
		t.Fatalf("buildStatus (abandoned pass): %v", err)
	}
	if scans != views {
		t.Fatalf("the abandoned pass scanned %d of %d views; a pass that stops at the next view "+
			"boundary can never make all %d stamps fresh at once", scans, views, views)
	}

	// Every view now carries a fresh entry stamped with the PASS's completion, so the
	// windows expire together instead of in the staggered order the scans finished in.
	for id := uint64(1); id <= views; id++ {
		c, ok := d.cachedViewCounts(id)
		if !ok {
			t.Fatalf("view %d has no fresh cached counts after the refill pass; `status` can never converge", id)
		}
		if c.unresolved != 1 || c.conflicts != 1 {
			t.Errorf("view %d cached counts: got %+v, want unresolved=1 conflicts=1", id, c)
		}
	}

	// The next call - the operator's retry - is served entirely from that cache: no
	// new scan, a complete report.
	before := scans
	ctx2, cancel2 := context.WithCancel(context.Background())
	abandon = cancel2
	rep, err = d.buildStatus(ctx2)
	cancel2()
	abandon = nil
	if err != nil {
		t.Fatalf("buildStatus (retry after the refill): %v", err)
	}
	if scans != before {
		t.Errorf("the retry ran %d more scans, want 0 (whole report served from the cohort-stamped cache)", scans-before)
	}
	if len(rep.Views) != views {
		t.Fatalf("report has %d views, want %d", len(rep.Views), views)
	}
	for _, v := range rep.Views {
		if v.Unresolved != 1 || v.Conflicts != 1 {
			t.Errorf("view %d in the report: got Unresolved=%d Conflicts=%d, want 1/1", v.ID, v.Unresolved, v.Conflicts)
		}
	}
}

// TestRefillStopsOnDaemonShutdown is the other half of that contract: a committed
// refill pass outlives its CALLER's budget, never the daemon's own lifetime. Once
// Run's context is done the pass must stop instead of walking the remaining views.
func TestRefillStopsOnDaemonShutdown(t *testing.T) {
	const views = 3
	st := openTestStore(t)
	seedCountedViews(t, st, views)

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	runCtx, cancelRun := context.WithCancel(context.Background())
	d.setBaseCtx(runCtx)

	scans := 0
	d.viewScanHook = func(uint64) {
		scans++
		cancelRun()
	}

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus during shutdown: %v (the counts degrade, they do not fail the verb)", err)
	}
	for _, v := range rep.Views {
		if !v.CountsStale {
			t.Errorf("view %d reports fresh counts after an unwinding pass; want CountsStale", v.ID)
		}
	}
	if scans != 1 {
		t.Errorf("the pass ran %d scans after the daemon's context was cancelled, want 1 (it must unwind with Run)", scans)
	}
	for id := uint64(1); id <= views; id++ {
		if _, ok := d.cachedViewCounts(id); ok {
			t.Errorf("view %d was stamped by an unwinding pass", id)
		}
	}
}

// TestBuildStatusConflictCountIsCached pins the convergence fix's mirror: the
// per-view keep-both conflict count is cached like the unresolved one instead of
// re-walking the conflicts bucket on EVERY `status` call. Uncached it was the third
// O(view) walk of the verb with no TTL cache, no single-flight, no scan budget and
// no cancellation check inside it at all - on a mass-divergence store (800k records)
// one count outran the 8s handler budget, and because nothing stamped a completion
// every later call restarted and re-aborted it, so `status` broke forever while
// `conflicts` (which does cache) kept working.
func TestBuildStatusConflictCountIsCached(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	base := time.Unix(1_750_000_000, 0)
	now := base
	d.now = func() time.Time { return now }
	scans := 0
	d.viewScanHook = func(uint64) { scans++ }

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Views[0].Conflicts != 1 || scans != 1 {
		t.Fatalf("first call: Conflicts=%d after %d scans, want 1 after 1", rep.Views[0].Conflicts, scans)
	}

	// A second keep-both conflict lands; inside the TTL the cached count is served, so
	// the report intentionally lags and no second walk happens.
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "b.txt", CopyPath: "b_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (cached): %v", err)
	}
	if rep.Views[0].Conflicts != 1 {
		t.Errorf("Conflicts inside the TTL: got %d, want 1 (cached - an uncached count re-walks the bucket every call)", rep.Views[0].Conflicts)
	}
	if scans != 1 {
		t.Errorf("scans inside the TTL: got %d, want 1", scans)
	}

	// Past the TTL the counts are refreshed together and pick up the new record.
	now = base.Add(viewCountsCacheTTL)
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (expired): %v", err)
	}
	if rep.Views[0].Conflicts != 2 || rep.Views[0].Unresolved != 1 {
		t.Errorf("after the TTL: got Conflicts=%d Unresolved=%d, want 2/1", rep.Views[0].Conflicts, rep.Views[0].Unresolved)
	}
	if scans != 2 {
		t.Errorf("scans after the TTL: got %d, want 2", scans)
	}
}

// TestBuildStatusCountsSingleFlight confirms concurrent `status` calls share ONE
// per-view count scan - the conflicts count included, which used to run per caller
// with no single-flight at all - so a polling client cannot stack duplicate O(view)
// walks against the sync data plane.
func TestBuildStatusCountsSingleFlight(t *testing.T) {
	const views = 2
	st := openTestStore(t)
	seedCountedViews(t, st, views)
	// Enough records that a scan is not instantaneous, widening the window the
	// concurrent misses overlap in.
	for i := 0; i < 400; i++ {
		p := fmt.Sprintf("f%04d.txt", i)
		b := store.Baseline{FileID: "id" + p, FileType: "file", State: store.StateConflict}
		if err := st.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %s: %v", p, err)
		}
		if err := st.PutConflict(1, store.Conflict{CanonicalPath: p, CopyPath: p + "_c"}); err != nil {
			t.Fatalf("PutConflict %s: %v", p, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	var scans atomic.Int64
	d.viewScanHook = func(uint64) { scans.Add(1) }

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	unresolved := make([]int, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rep, err := d.buildStatus(context.Background())
			errs[i] = err
			if err == nil && len(rep.Views) > 0 {
				unresolved[i] = rep.Views[0].Unresolved
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("buildStatus[%d]: %v", i, errs[i])
		}
		if unresolved[i] != 401 {
			t.Errorf("caller %d saw Unresolved=%d, want 401", i, unresolved[i])
		}
	}
	if got := scans.Load(); got != views {
		t.Errorf("%d concurrent `status` calls ran %d count scans, want %d (one per view: single-flight + cache)",
			callers, got, views)
	}
}

// TestViewCountsCacheTTLExceedsHandlerBudget pins the d12ba85 doctrine for the
// per-view counts cache: a TTL shorter than the IPC handler budget makes the stamps
// a refill lands expire before any retry can reach them, so the verb never succeeds
// at all (see viewCountsCacheTTL).
func TestViewCountsCacheTTLExceedsHandlerBudget(t *testing.T) {
	if viewCountsCacheTTL <= handlerBudgetFloor {
		t.Fatalf("viewCountsCacheTTL = %v, want more than the IPC handler budget (%v)", viewCountsCacheTTL, handlerBudgetFloor)
	}
}
