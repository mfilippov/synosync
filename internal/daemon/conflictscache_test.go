package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// TestBuildConflictsSingleFlight pins the single-flight fix: concurrent
// `conflicts` calls share ONE scan of the conflicts bucket. Without it, every
// concurrent caller (up to the IPC in-flight cap) re-decoded every conflict record
// of every view against the sync data plane - the duplication the unresolved count
// already avoided. The scan count is observed through the skip-Warn a corrupt
// conflict record triggers exactly once per scan.
func TestBuildConflictsSingleFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Enough records that a scan is not instantaneous, widening the window the
	// concurrent misses overlap in. "bad.txt" sorts first so the corrupt record is
	// hit early in every scan.
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "bad.txt", CopyPath: "bad_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict bad: %v", err)
	}
	const records = 400
	for i := 0; i < records; i++ {
		c := store.Conflict{
			CanonicalPath: fmt.Sprintf("f%04d.txt", i),
			CopyPath:      fmt.Sprintf("f%04d_Conflict.txt", i),
			DetectedAt:    int64(i),
		}
		if err := st.PutConflict(1, c); err != nil {
			t.Fatalf("PutConflict %d: %v", i, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	corruptConflictRecord(t, path, 1, "bad.txt")

	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	var logBuf bytes.Buffer
	d, err := New(Deps{
		Store:     st2,
		Config:    config.Config{Server: config.Server{Host: "h", Port: 6690}},
		SockPath:  tempSockPath(t),
		LockPath:  filepath.Join(t.TempDir(), "daemon.lock"),
		Connector: func(context.Context) (*transport.Session, error) { return nil, nil },
		Logger:    slog.New(slog.NewTextHandler(&logBuf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	reps := make([]ipc.ConflictReport, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			reps[i], errs[i] = d.buildConflicts(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("buildConflicts[%d]: %v", i, errs[i])
		}
		if reps[i].Total != records { // the corrupt record is skipped
			t.Errorf("report[%d].Total: got %d, want %d", i, reps[i].Total, records)
		}
		if len(reps[i].Conflicts) != records {
			t.Errorf("report[%d]: got %d entries, want %d", i, len(reps[i].Conflicts), records)
		}
	}
	if warns := bytes.Count(logBuf.Bytes(), []byte("corrupt conflict record")); warns != 1 {
		t.Errorf("scan ran %d times for %d concurrent calls, want exactly 1 (single-flight + cache)", warns, callers)
	}
}

// TestBuildConflictsTTLCache confirms a successful report is served from the TTL
// cache and refreshed once the TTL expires, mirroring the unresolved count's
// caching contract: inside the window the report intentionally lags the store, and
// nothing is cached without a completed scan.
func TestBuildConflictsTTLCache(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt", DetectedAt: 100}); err != nil {
		t.Fatalf("PutConflict a: %v", err)
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	base := time.Unix(1_750_000_000, 0)
	now := base
	d.now = func() time.Time { return now }

	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != 1 {
		t.Fatalf("Total: got %d, want 1", rep.Total)
	}

	// A second conflict appears; inside the TTL the cached report is served, so it
	// intentionally lags.
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "b.txt", CopyPath: "b_Conflict.txt", DetectedAt: 200}); err != nil {
		t.Fatalf("PutConflict b: %v", err)
	}
	now = base.Add(conflictsCacheTTL - time.Nanosecond)
	rep, err = d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts (cached): %v", err)
	}
	if rep.Total != 1 || len(rep.Conflicts) != 1 {
		t.Errorf("inside TTL: got Total=%d len=%d, want 1/1 (cached)", rep.Total, len(rep.Conflicts))
	}

	// Past the TTL the report is rebuilt and picks up the new record.
	now = base.Add(conflictsCacheTTL)
	rep, err = d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts (expired): %v", err)
	}
	if rep.Total != 2 || len(rep.Conflicts) != 2 {
		t.Fatalf("after TTL: got Total=%d len=%d, want 2/2 (fresh)", rep.Total, len(rep.Conflicts))
	}
	// Newest first, as always.
	if rep.Conflicts[0].CanonicalPath != "b.txt" {
		t.Errorf("newest-first order broken after refresh: %+v", rep.Conflicts)
	}
}

// TestBuildConflictsCachesOnlySuccess confirms a failed scan is never cached: the
// next call re-reads the store rather than serving a phantom report (and the
// failure keeps surfacing while the store stays broken).
func TestBuildConflictsCachesOnlySuccess(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := d.buildConflicts(context.Background()); err == nil {
			t.Fatalf("buildConflicts #%d on a closed store: got nil error, want failure", i)
		}
	}
	d.conflictsMu.Lock()
	cached := d.conflictsCache.ok
	d.conflictsMu.Unlock()
	if cached {
		t.Error("a failed scan populated the report cache")
	}
}

// TestBuildConflictsHonoursCancelledContext confirms the handler's context cuts
// the scan short instead of draining the whole conflicts bucket for a client that
// is already gone (the IPC layer abandons the handler at its budget), and that the
// abandoned attempt caches nothing.
func TestBuildConflictsHonoursCancelledContext(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for i := 0; i < 8; i++ {
		c := store.Conflict{CanonicalPath: fmt.Sprintf("f%d.txt", i), CopyPath: fmt.Sprintf("f%d_Conflict.txt", i)}
		if err := st.PutConflict(1, c); err != nil {
			t.Fatalf("PutConflict %d: %v", i, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := d.buildConflicts(ctx); err == nil {
		t.Fatal("buildConflicts with a cancelled context: got nil error, want the cancellation surfaced")
	}
	d.conflictsMu.Lock()
	cached := d.conflictsCache.ok
	d.conflictsMu.Unlock()
	if cached {
		t.Error("an abandoned scan populated the report cache")
	}

	// A fresh call still works: the abandoned one left no state behind.
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts after cancellation: %v", err)
	}
	if rep.Total != 8 {
		t.Errorf("Total: got %d, want 8", rep.Total)
	}
}

// TestCacheFillingScanDropsAnExpiredWaiter pins the OTHER half of that gate: the
// handler budget must still be checked AFTER the single-flight wait. A caller that
// missed the cache, queued behind another view's scan and expired while waiting used
// to acquire the lock and start a full O(tree) scan anyway for a client that was
// already gone - so during a run of failing scans (errors are never cached) every
// abandoned handler in turn serialised another one. The cache re-check under the lock
// still comes first, so nothing that a completed scan produced is thrown away; only a
// caller that would have to start its own scan is dropped.
//
// The expiry is injected at the exact instant the waiter is released: d.now is called
// by the cache re-check under the flight lock, so cancelling on that call is the
// deterministic stand-in for "the budget ran out while queued".
func TestCacheFillingScanDropsAnExpiredWaiter(t *testing.T) {
	// expireOnFlightRecheck cancels ctx on the THIRD d.now() call of a build: the two
	// pre-lock reads (the cache lookup and the remembered-failure lookup) come
	// first, and the third is the cache re-check under the flight lock.
	// Cancelling any earlier would return at the PRE-lock gate instead - a path
	// TestBuildStatusHonoursCancelledContext already covers - and this test would
	// silently stop exercising the post-lock guards it exists for.
	const flightRecheckNowCall = 3
	expireOnFlightRecheck := func(d *Daemon, cancel context.CancelFunc) *int {
		calls := 0
		base := time.Unix(1_750_000_000, 0)
		d.now = func() time.Time {
			calls++
			if calls == flightRecheckNowCall {
				cancel()
			}
			return base
		}
		return &calls
	}

	t.Run("conflicts", func(t *testing.T) {
		st := openTestStore(t)
		if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt"}); err != nil {
			t.Fatalf("PutConflict: %v", err)
		}
		d, _ := newStatusDaemon(t, st, config.Config{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := expireOnFlightRecheck(d, cancel)

		rep, err := d.buildConflicts(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("buildConflicts = (%+v, %v), want context.Canceled - no whole-bucket scan for a caller whose budget expired while queued", rep, err)
		}
		if *calls < flightRecheckNowCall {
			t.Fatalf("the cache re-check under the flight lock never ran (now called %d times, want at least %d)",
				*calls, flightRecheckNowCall)
		}
		d.conflictsMu.Lock()
		cached := d.conflictsCache.ok
		d.conflictsMu.Unlock()
		if cached {
			t.Error("the expired caller ran a scan and stamped the report cache")
		}
	})

	t.Run("unresolved", func(t *testing.T) {
		st := openTestStore(t)
		seedStatusStore(t, st)
		d, _ := newStatusDaemon(t, st, config.Config{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := expireOnFlightRecheck(d, cancel)

		a := d.viewCountsFor(ctx, 1)
		if !errors.Is(a.err, context.Canceled) {
			t.Fatalf("viewCountsFor = (%+v, %v), want context.Canceled - no baseline walk for a caller whose budget expired while queued", a.counts, a.err)
		}
		if *calls < flightRecheckNowCall {
			t.Fatalf("the cache re-check under the flight lock never ran (now called %d times, want at least %d)",
				*calls, flightRecheckNowCall)
		}
		d.countsMu.Lock()
		_, cached := d.countsCache[1]
		d.countsMu.Unlock()
		if cached {
			t.Error("the expired caller ran a scan and stamped the view-counts cache")
		}
	})
}

// TestBuildStatusHonoursCancelledContext is the same contract for `status`: a call
// whose handler context is already done starts no per-view O(tree) work at all, and
// caches no partial count. (Once a refill pass HAS started it deliberately runs to
// completion - see TestBuildStatusCountsCacheConvergesAcrossViews.)
func TestBuildStatusHonoursCancelledContext(t *testing.T) {
	st := openTestStore(t)
	seedStatusStore(t, st)
	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The report is still assembled - its scan-free fields cost nothing - but every view's
	// counts are marked stale, because no O(tree) walk may start for a client that is gone.
	rep0, err := d.buildStatus(ctx)
	if err != nil {
		t.Fatalf("buildStatus with a cancelled context: %v", err)
	}
	for _, v := range rep0.Views {
		if !v.CountsStale {
			t.Errorf("view %d reports fresh counts for an abandoned caller; want CountsStale", v.ID)
		}
	}

	if a := d.viewCountsFor(ctx, 1); a.err == nil {
		t.Fatal("viewCountsFor with a cancelled context: got nil error, want the cancellation surfaced")
	} else if a.scanned {
		t.Fatal("viewCountsFor ran a scan for a caller whose budget was already gone")
	}
	d.countsMu.Lock()
	_, cached := d.countsCache[1]
	d.countsMu.Unlock()
	if cached {
		t.Error("an abandoned baseline scan populated the unresolved-count cache")
	}

	// The uncancelled path still assembles the full report.
	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus after cancellation: %v", err)
	}
	if len(rep.Views) != 2 || rep.Views[0].Unresolved != 2 {
		t.Errorf("status after cancellation: got %+v", rep.Views)
	}
}

// TestScanContextIsIndependentOfTheHandlerBudget pins a follow-up fix: the
// cache-filling store scans must NOT run on the IPC handler's budget. Threading the
// handler context all the way into the scan made a scan slower than that budget
// self-defeating - it was cancelled mid-walk, cached nothing, and every later
// `status`/`conflicts` call restarted and re-aborted it, so the verb stayed broken
// forever on exactly the large trees the completion-stamped cache exists for. The scan
// context must therefore carry its own (longer) budget and hang off the daemon's
// lifetime, not off any caller.
func TestScanContextIsIndependentOfTheHandlerBudget(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	runCtx, cancelRun := context.WithCancel(context.Background())
	d.setBaseCtx(runCtx)

	// A caller whose own context is already dead must not poison the scan context.
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()

	scanCtx, cancelScan := d.scanContext()
	defer cancelScan()
	if callerCtx.Err() == nil {
		t.Fatal("test setup: the caller context should be cancelled")
	}
	if err := scanCtx.Err(); err != nil {
		t.Fatalf("scan context is already done (%v); it must not inherit a caller's cancellation", err)
	}
	dl, ok := scanCtx.Deadline()
	if !ok {
		t.Fatal("scan context carries no deadline; the scan must stay bounded")
	}
	if left := time.Until(dl); left <= handlerBudgetFloor {
		t.Errorf("scan budget has %v left, want more than the IPC handler budget (%v); "+
			"a scan on the handler's budget can never fill the cache", left, handlerBudgetFloor)
	}

	// The daemon's own lifetime still stops it.
	cancelRun()
	if err := scanCtx.Err(); err == nil {
		t.Error("scan context survived the daemon's shutdown; an abandoned scan must unwind with Run")
	}
}

// handlerBudgetFloor mirrors ipc's unexported handlerTimeout: the scan budget must
// exceed it, or the scan is back on the handler's clock.
const handlerBudgetFloor = 8 * time.Second

// TestScansStillCacheWhenTheirCallerIsAbandoned is the behavioural half: a scan the
// daemon entered runs to completion and stamps the cache even though the IPC layer has
// long abandoned the handler that triggered it, so the NEXT call is served from the
// cache instead of restarting the same walk.
func TestScansStillCacheWhenTheirCallerIsAbandoned(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for i := 0; i < 8; i++ {
		c := store.Conflict{CanonicalPath: fmt.Sprintf("f%d.txt", i), CopyPath: fmt.Sprintf("f%d_Conflict.txt", i)}
		if err := st.PutConflict(1, c); err != nil {
			t.Fatalf("PutConflict %d: %v", i, err)
		}
	}
	d, _ := newStatusDaemon(t, st, config.Config{})

	// A caller context that expires the instant the scan begins - the IPC layer's
	// "handler outran its budget" state. The scan is already entered, so it completes.
	callerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep, err := d.buildConflicts(callerCtx)
	cancel()
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != 8 {
		t.Fatalf("Total = %d, want 8", rep.Total)
	}
	d.conflictsMu.Lock()
	cached := d.conflictsCache.ok
	d.conflictsMu.Unlock()
	if !cached {
		t.Fatal("a completed scan did not stamp the cache; every later call would rerun it")
	}

	// The abandoned caller's dead context must not stop the NEXT call from being served
	// from that cache - the cache hit is checked before the caller's budget.
	if _, err := d.buildConflicts(callerCtx); err != nil {
		t.Errorf("cached report not served to a caller with a dead context: %v", err)
	}
}

// TestFitEntriesTrimIsMaximal pins what the switch from halve-and-remarshal
// to exact incremental accounting actually bought, which TestBuildConflictsByteBudgetTrims
// cannot distinguish: the retained prefix is the LONGEST one that fits, not a power-of-two
// fraction of it. The old halving discarded half the list per round, so a report one byte
// over budget came back with 500 of its 1000 records; the assertion here is that appending
// the very next candidate would blow the budget - i.e. nothing that fits was dropped.
func TestFitEntriesTrimIsMaximal(t *testing.T) {
	const total = 40
	cands := make([]conflictCandidate, total)
	for i := range cands {
		cands[i] = conflictCandidate{
			entry: ipc.ConflictEntry{
				ViewID:        1,
				ViewName:      "alpha",
				CanonicalPath: fmt.Sprintf("%s-%02d", strings.Repeat("c", 900), i),
				CopyPath:      strings.Repeat("k", 900),
				DetectedAt:    int64(total - i), // already newest-first
			},
			seq: i,
		}
	}

	oldBudget := maxConflictReportBytes
	maxConflictReportBytes = 24 << 10 // forces a trim well inside the 40 candidates
	t.Cleanup(func() { maxConflictReportBytes = oldBudget })

	entries, err := fitEntries(cands, total)
	if err != nil {
		t.Fatalf("fitEntries: %v", err)
	}
	if len(entries) == 0 || len(entries) >= total {
		t.Fatalf("kept %d entries, want a genuine trim (0 < kept < %d)", len(entries), total)
	}

	// encodedLen is the true wire size of the report holding the first n candidates -
	// the quantity fitEntries estimates incrementally.
	encodedLen := func(n int) int {
		t.Helper()
		rep := ipc.ConflictReport{Total: total, Truncated: n < total}
		for i := 0; i < n; i++ {
			rep.Conflicts = append(rep.Conflicts, cands[i].entry)
		}
		enc, merr := json.Marshal(rep)
		if merr != nil {
			t.Fatalf("Marshal: %v", merr)
		}
		return len(enc)
	}

	if got := encodedLen(len(entries)); got > maxConflictReportBytes {
		t.Errorf("kept report is %d bytes, over the %d budget", got, maxConflictReportBytes)
	}
	// Maximality: one more candidate must NOT fit. A halving trim would leave a large gap
	// here and this assertion would fail.
	if got := encodedLen(len(entries) + 1); got <= maxConflictReportBytes {
		t.Errorf("one more entry would still fit (%d <= %d): %d of %d kept, so the trim discarded "+
			"records that fit - the accounting is not exact", got, maxConflictReportBytes, len(entries), total)
	}
}

// TestFitEntriesSingleOversizeEntryYieldsEmptyReport covers the boundary the trim loop must
// not mishandle: one record whose own encoding exceeds the whole byte budget. The report
// must come back with no entries (rather than one that blows the CLI's reply cap) while
// still reporting the true Total, so `conflicts` degrades to "there are N, none shown"
// instead of failing the read.
func TestFitEntriesSingleOversizeEntryYieldsEmptyReport(t *testing.T) {
	oldBudget := maxConflictReportBytes
	maxConflictReportBytes = 512
	t.Cleanup(func() { maxConflictReportBytes = oldBudget })

	cands := []conflictCandidate{{
		entry: ipc.ConflictEntry{
			ViewID: 1, ViewName: "alpha",
			CanonicalPath: strings.Repeat("c", 4096),
			CopyPath:      strings.Repeat("k", 4096),
		},
	}}
	entries, err := fitEntries(cands, 3)
	if err != nil {
		t.Fatalf("fitEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("kept %d entries, want 0 - a single record over the whole budget must not be emitted", len(entries))
	}
	rep := ipc.ConflictReport{Conflicts: entries, Total: 3, Truncated: len(entries) < 3}
	enc, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(enc) > maxConflictReportBytes {
		t.Errorf("empty-list report is %d bytes, over the %d budget", len(enc), maxConflictReportBytes)
	}
	if !rep.Truncated {
		t.Error("Truncated = false; an empty list over a non-zero Total must be marked truncated")
	}
}
