package engine

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// infoLogger returns a logger that captures Info and above into buf, so a test can
// assert both the Warn (going absent) and Info (reappearing) transition lines
// reconcileAbsentViews emits.
func infoLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// TestReconcileAbsentViewsMarksAndClears confirms the absent-view lifecycle: a stored view
// that drops out of the query_user_info enumeration is marked absent (Warn, once), its
// data is left in the store untouched, and a later enumeration that lists it again
// clears the mark (Info) and resumes syncing.
func TestReconcileAbsentViewsMarksAndClears(t *testing.T) {
	st := newTestStore(t)
	fc := &fakeClient{views: []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r7"},
		{ViewID: 9, Name: "Docs", Privilege: "rw", NodeID: "r9"},
	}}
	var buf bytes.Buffer
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", infoLogger(&buf))

	// First enumeration lists both views: neither is absent.
	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews (both present): %v", err)
	}
	if e.absentViews[7] || e.absentViews[9] {
		t.Fatalf("absentViews after both present = %v, want none marked", e.absentViews)
	}

	// Second enumeration drops view 9: it must be marked absent and Warned once, while
	// its store record survives (we do not delete server-owned views).
	buf.Reset()
	fc.views = []actions.View{{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r7"}}
	// The omitted view's root probe returns the path-error sentinel: positive evidence the
	// view really is gone, which the mark now requires.
	// One path-error is not corroboration (the sentinel conflates a real absence with a probe
	// timeout), so the mark lands only once absentRootConfirmations refreshes agree.
	fc.nodeErr = actions.ErrPathError
	for i := 0; i < absentRootConfirmations; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews (9 dropped, confirmation %d): %v", i+1, err)
		}
	}
	if !e.absentViews[9] {
		t.Fatal("view 9 dropped from the enumeration but not marked absent")
	}
	if e.absentViews[7] {
		t.Fatal("view 7 still enumerated but marked absent")
	}
	if _, err := st.GetView(9); err != nil {
		t.Fatalf("absent view 9's store record was disturbed: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "no longer present") || !strings.Contains(got, "view=9") {
		t.Fatalf("absent transition log = %q, want a Warn naming view 9", got)
	}
	// A second enumeration still omitting 9 must NOT re-Warn (transition-once).
	buf.Reset()
	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews (9 still dropped): %v", err)
	}
	if strings.Contains(buf.String(), "no longer present") {
		t.Fatalf("re-Warned an already-absent view: %q", buf.String())
	}

	// Third enumeration lists view 9 again: the mark clears and an Info line fires.
	buf.Reset()
	fc.views = []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r7"},
		{ViewID: 9, Name: "Docs", Privilege: "rw", NodeID: "r9"},
	}
	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews (9 reappeared): %v", err)
	}
	if e.absentViews[9] {
		t.Fatal("view 9 reappeared in the enumeration but is still marked absent")
	}
	if got := buf.String(); !strings.Contains(got, "reappeared") || !strings.Contains(got, "view=9") {
		t.Fatalf("reappearance log = %q, want an Info naming view 9", got)
	}
}

// TestRefreshViewsRejectsBareEmptyEnumeration pins the bare-enumeration guard at the
// engine layer: a contentless query_user_info reply must never mark the stored views
// absent. Two shapes reach refreshViews after the decode-layer fix: the bare {} now
// surfaces as an error (QueryUserInfo rejects it - modelled by viewsErr), which
// refreshViews propagates BEFORE any absent-marking; and an errno-evidenced reply
// without a view_list still decodes tolerantly to a NIL slice, which the
// belt-and-braces consumer guard skips. Before the fix a nil enumeration marked
// EVERY stored view absent - pollOnce, localScan and coldStartPending then skipped
// them all, a daemon "without errors, without sync" until restart.
func TestRefreshViewsRejectsBareEmptyEnumeration(t *testing.T) {
	t.Run("decode rejection propagates before marking", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.PutView(7, store.View{Name: "Photos", NodeID: "r7", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		fc := &fakeClient{viewsErr: fmt.Errorf(
			"actions: query_user_info reply carries neither errno nor view_list: %w", actions.ErrUnexpectedReply)}
		e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
		if err := e.refreshViews(context.Background()); err == nil {
			t.Fatal("refreshViews = nil, want the decode-layer rejection to propagate")
		}
		if len(e.absentViews) != 0 {
			t.Fatalf("absentViews = %v, want none marked on a rejected enumeration", e.absentViews)
		}
	})
	t.Run("nil enumeration marks nothing", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.PutView(7, store.View{Name: "Photos", NodeID: "r7", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		fc := &fakeClient{views: nil} // errno-evidenced but list-less: no evidence either way
		e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews: %v", err)
		}
		if len(e.absentViews) != 0 {
			t.Fatalf("absentViews = %v, want none marked on a nil (unevidenced) enumeration", e.absentViews)
		}
	})
}

// TestReconcileAbsentViewsEmptyEnumerationStillMarks pins the legitimate all-revoked
// case the nil-guard must NOT suppress: an errno:0 reply with a PRESENT but
// empty view_list decodes to a non-nil empty slice - positive evidence the account
// holds zero views - and keeps marking every stored view absent, exactly as before.
func TestReconcileAbsentViewsEmptyEnumerationStillMarks(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "r7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// The stored view's root probe confirms it is gone (path error), the positive evidence
	// the absent mark requires.
	fc := &fakeClient{views: []actions.View{}, nodeErr: actions.ErrPathError}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
	for i := 0; i < absentRootConfirmations; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews (confirmation %d): %v", i+1, err)
		}
	}
	if !e.absentViews[7] {
		t.Fatal("view 7 not marked absent on a positively-evidenced empty enumeration")
	}
}

// TestAbsentViewMarkClearsInProcess drives the steady-state
// re-enumeration end-to-end through pollOnce: a view revoked mid-run is marked absent
// by the budgeted periodic refreshViews re-run (before the fix refreshViews never ran
// again once every stored view had a root id, so both the marking and its clearing
// required a daemon restart), the re-run is budgeted (an immediate next cycle does not
// re-issue the turn), the tightened absent cadence applies once a mark exists, and a
// restored view clears the mark and rejoins the long_poll subscription in-process.
func TestAbsentViewMarkClearsInProcess(t *testing.T) {
	st := newTestStore(t)
	for _, v := range []struct {
		id   uint64
		name string
		node string
	}{{7, "Photos", "r7"}, {9, "Docs", "r9"}} {
		if err := st.PutView(v.id, store.View{Name: v.name, NodeID: v.node, Privilege: "rw", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	both := []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r7"},
		{ViewID: 9, Name: "Docs", Privilege: "rw", NodeID: "r9"},
	}
	fc := &fakeClient{views: both}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
	cur := time.Unix(1700000000, 0)
	e.now = func() time.Time { return cur }

	// Cycle 1: the budget is unset, so the first poll enumerates and subscribes both.
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce 1: %v", err)
	}
	if fc.calls != 1 {
		t.Fatalf("QueryUserInfo calls = %d after cycle 1, want 1", fc.calls)
	}
	if got := fc.longPollViews; len(got) != 2 {
		t.Fatalf("cycle 1 subscription = %v, want both views", got)
	}

	// Cycle 2, immediately: within the steady-state budget - no re-enumeration.
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce 2: %v", err)
	}
	if fc.calls != 1 {
		t.Fatalf("QueryUserInfo calls = %d after cycle 2, want 1 (budgeted)", fc.calls)
	}

	// The server revokes view 9; past the steady-state interval the periodic re-run
	// discovers it without a restart and drops it from the subscription this cycle.
	fc.views = both[:1]
	// Its root probe answers with the path-error sentinel - the positive evidence the mark
	// now requires, so a paged enumeration cannot suppress a healthy view.
	fc.nodeErr = actions.ErrPathError
	// The path-error verdict is corroborated across absentRootConfirmations re-enumerations,
	// each one a cycle past the (tightened, once a probe is pending) refresh interval.
	for i := 0; i < absentRootConfirmations; i++ {
		cur = cur.Add(viewRefreshInterval + time.Second)
		if err := e.pollOnce(context.Background()); err != nil {
			t.Fatalf("pollOnce 3 (confirmation %d): %v", i+1, err)
		}
	}
	if fc.calls != 1+absentRootConfirmations {
		t.Fatalf("QueryUserInfo calls = %d after cycle 3, want %d", fc.calls, 1+absentRootConfirmations)
	}
	if !e.absentViews[9] {
		t.Fatal("view 9 revoked mid-run but not marked absent by the periodic re-enumeration")
	}
	if got := fc.longPollViews; len(got) != 1 || got[0] != 7 {
		t.Fatalf("cycle 3 subscription = %v, want [7]", got)
	}

	// The server restores view 9; the tightened absent cadence (well under the
	// steady-state interval) clears the mark and resumes its sync in-process.
	fc.views = both
	cur = cur.Add(viewRefreshAbsentInterval + time.Second)
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce 4: %v", err)
	}
	if want := 2 + absentRootConfirmations; fc.calls != want {
		t.Fatalf("QueryUserInfo calls = %d after cycle 4, want %d (tightened absent cadence)", fc.calls, want)
	}
	if e.absentViews[9] {
		t.Fatal("view 9 restored but its absent mark did not clear in-process")
	}
	if got := fc.longPollViews; len(got) != 2 {
		t.Fatalf("cycle 4 subscription = %v, want both views resumed", got)
	}
}

// TestPollOnceSkipsAbsentViewSubscription confirms an absent view is excluded from the
// shared long_poll subscription, so a dead view id can no longer fail the whole poll
// and spin the inbound loop every cycle.
func TestPollOnceSkipsAbsentViewSubscription(t *testing.T) {
	st := newTestStore(t)
	for _, v := range []struct {
		id   uint64
		name string
	}{{7, "Photos"}, {9, "Docs"}} {
		if err := st.PutView(v.id, store.View{Name: v.name, NodeID: "r", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	fc := &fakeClient{}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
	e.absentViews[9] = true // view 9 removed server-side

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if want := []int64{7}; len(fc.longPollViews) != 1 || fc.longPollViews[0] != want[0] {
		t.Fatalf("long_poll subscription = %v, want %v (view 9 must be excluded)", fc.longPollViews, want)
	}
}

// TestLocalScanSkipsAbsentView confirms localScan does not scan an absent view for
// outbound changes: a genuine on-disk modification in the absent view produces no op,
// while the same modification in a present view still does.
func TestLocalScanSkipsAbsentView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Alpha", NodeID: "a1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 7: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "Beta", NodeID: "b1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 8: %v", err)
	}
	root := t.TempDir()
	alpha := filepath.Join(root, "Alpha")
	beta := filepath.Join(root, "Beta")
	for _, d := range []string{alpha, beta} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed view dir %s: %v", d, err)
		}
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	e.scanInterval = 0
	e.limits.FullRehashInterval = 1 // every pass re-hashes, so the swapped bytes are seen
	e.absentViews[8] = true         // view 8 removed server-side → must not be scanned

	seedSwappedSameSizeMtime(t, st, 7, alpha, "a.txt")
	seedSwappedSameSizeMtime(t, st, 8, beta, "b.txt")

	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("localScan enqueued %d ops, want 1 (only the present view 7)", len(ops))
	}
	if ops[0].ViewID != 7 {
		t.Fatalf("enqueued op for view %d, want 7 (view 8 is absent and must be skipped)", ops[0].ViewID)
	}
}

// TestDrainSkipsAbsentViewOpsAndResumes pins the queue side of the
// absent-view suppression: ops enqueued before the server revoked their view are
// skipped by the drain - not run, not counted as failures (no stuck escalation), not
// deleted - with a transition-once Warn, and drain normally again once the view
// reappears in the enumeration. Before the fix they retried (and spuriously failed)
// every drain forever, and a head window saturated with them starved the whole queue.
func TestDrainSkipsAbsentViewOpsAndResumes(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView 7: %v", err)
	}
	if err := st.PutView(9, store.View{Name: "Docs"}); err != nil {
		t.Fatalf("PutView 9: %v", err)
	}
	root := t.TempDir()
	var buf bytes.Buffer
	// The revoked view's root probe answers with the path-error sentinel, the positive
	// evidence reconcileAbsentViews requires before the absent mark becomes sticky.
	e := New(st, &concTransport{client: &dlClient{queryErr: actions.ErrPathError}}, root, "sess", infoLogger(&buf))

	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 9, Path: "d9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "d7",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Revoke view 9: an enumeration listing only view 7, with absentRootConfirmations agreeing
	// root probes behind it, marks it absent.
	for i := 0; i < absentRootConfirmations; i++ {
		if err := e.reconcileAbsentViews(context.Background(), []actions.View{{ViewID: 7, Name: "Photos"}}); err != nil {
			t.Fatalf("reconcileAbsentViews (9 revoked, confirmation %d): %v", i+1, err)
		}
	}

	buf.Reset()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue = %v, want nil (a suppressed op must not read as a failure)", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "d7")); err != nil {
		t.Fatalf("present view's op did not apply: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Docs", "d9")); err == nil {
		t.Fatal("absent view's op was applied, want it suppressed")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("pending = %d (err %v), want the absent view's op to stay queued (1)", n, err)
	}
	if got := e.StuckOps(); got != 0 {
		t.Errorf("StuckOps = %d, want 0 (a suppressed op is not a failure)", got)
	}
	if got := buf.String(); !strings.Contains(got, "suppressing queued ops") || !strings.Contains(got, "view=9") {
		t.Fatalf("suppression log = %q, want a Warn naming view 9", got)
	}

	// A second drain must not re-Warn (transition-once).
	buf.Reset()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (second): %v", err)
	}
	if strings.Contains(buf.String(), "suppressing queued ops") {
		t.Fatalf("re-Warned an already-suppressed view: %q", buf.String())
	}

	// The view reappears: the mark clears and the suppressed op drains normally.
	if err := e.reconcileAbsentViews(context.Background(), []actions.View{
		{ViewID: 7, Name: "Photos"}, {ViewID: 9, Name: "Docs"},
	}); err != nil {
		t.Fatalf("reconcileAbsentViews (9 returned): %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (after reappearance): %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Docs", "d9")); err != nil {
		t.Fatalf("returned view's op did not drain: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending = %d (err %v), want 0 after the returned view drained", n, err)
	}

	// A later re-revocation Warns again: the once-guard re-arms on reappearance.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 9, Path: "d9b",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	for i := 0; i < absentRootConfirmations; i++ {
		if err := e.reconcileAbsentViews(context.Background(), []actions.View{{ViewID: 7, Name: "Photos"}}); err != nil {
			t.Fatalf("reconcileAbsentViews (9 re-revoked, confirmation %d): %v", i+1, err)
		}
	}
	buf.Reset()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (after re-revocation): %v", err)
	}
	if !strings.Contains(buf.String(), "suppressing queued ops") {
		t.Fatalf("re-revocation did not Warn again: %q", buf.String())
	}
}
