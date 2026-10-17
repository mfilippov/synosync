package engine

// This file pins two stale-event hardening gates:
//
//   - Reconcile skips an event positioned STRICTLY BELOW the view's durable cursor: the
//     cursor records the highest sync id consumed, so such an event was already folded into
//     the store, and replaying it rewrites baselines against a world that has moved past it
//     (a stale delete replayed after a recreate, a stale move replayed after the node moved
//     on). An event exactly AT the cursor is still processed (the inclusive/exclusive
//     pull_event window is not a confirmed contract, and the stalled-page shape is the
//     syncView spin guard's business), as is an UNPOSITIONED one (no sync id, no position).
//   - A self-echo create/modify does not overlay a record that identifies a DIFFERENT node,
//     mirroring applySelfEchoDelete / dropSelfEchoMoveSource. Overlaying there would rewrite
//     the successor's record under our node's id and, through the byid index, re-point our id
//     at the successor's path. Finding H3 made that gate POSITIONAL rather than absolute - an
//     echo stamped above the record's ServerSyncID is the later statement about the path and is
//     adopted - so the tests here pin the stale direction and the disarm; the adopting direction
//     lives in inbound_remove_successor_test.go.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDropConsumedEventsWarnsOnce pins the summary reporting of the consumed-event filter. The
// shapes that trigger it - a history-replaying server, or a page replayed after a crash between the
// enqueue and the per-page cursor advance - are server-sized, so a per-event Warn floods the
// size-capped rotating log in exactly the scenario an operator needs it to diagnose, evicting the
// surrounding history. This mirrors the cold start's summary reporting:
// ONE Warn carrying the count and the dropped range, with the per-event detail at Debug.
func TestDropConsumedEventsWarnsOnce(t *testing.T) {
	var buf bytes.Buffer
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", "self",
		slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	events := make([]actions.Event, 0, 500)
	for i := 1; i <= 500; i++ {
		events = append(events, actions.Event{
			SyncID: int64(i), Action: "delete", FileID: "f1", Path: "a.txt",
		})
	}
	kept := e.dropConsumedEvents(1, events, 900)
	if len(kept) != 0 {
		t.Fatalf("kept %d events, want 0 (all sit below the cursor)", len(kept))
	}

	warns := strings.Count(buf.String(), "level=WARN")
	if warns != 1 {
		t.Fatalf("emitted %d WARN records for 500 dropped events, want exactly 1:\n%s", warns, buf.String())
	}
	out := buf.String()
	for _, want := range []string{"count=500", "cursor=900", "lowest_sync_id=1", "highest_sync_id=500"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary Warn missing %q:\n%s", want, out)
		}
	}
}

// TestReconcileSkipsEventBelowCursor drives a re-delivered page: a delete whose sync id sits
// below the durable cursor must not touch the tracked node, and the cursor must not move.
func TestReconcileSkipsEventBelowCursor(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "h1", LocalHash: "h1",
		ServerSyncID: 20, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	ops, cursor, err := e.reconcile(1, []actions.Event{
		{SyncID: 12, Action: "delete", FileID: "f1", Path: "a.txt"}, // strictly below the cursor
		{SyncID: 19, Action: "delete", FileID: "f1", Path: "a.txt"}, // still below the cursor
	}, 20)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("already-consumed events produced %d ops, want 0: %+v", len(ops), ops)
	}
	if cursor != 20 {
		t.Errorf("cursor = %d, want 20 (unchanged)", cursor)
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced (a consumed event must not flip it)", b.State)
	}
}

// TestReconcileProcessesEventAtCursor pins the first deliberate exemption: an event whose
// sync id equals the cursor is still folded, since the pull_event window's inclusivity is not
// a confirmed contract (the stalled-page shape is the syncView spin guard's business).
func TestReconcileProcessesEventAtCursor(t *testing.T) {
	e, _ := newReconcileEngine(t, "self")
	ops, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 20, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 2},
	}, 20)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want one download (an event AT the cursor must still be folded)", ops)
	}
}

// TestReconcileProcessesUnpositionedEvent pins the second deliberate exemption: an event with no sync
// id has no position in the log, so it cannot be judged consumed and is folded as before -
// even when the view already carries a cursor.
func TestReconcileProcessesUnpositionedEvent(t *testing.T) {
	e, _ := newReconcileEngine(t, "self")
	ops, cursor, err := e.reconcile(1, []actions.Event{
		{Action: "create", FileID: "f9", Path: "sparse.txt", Hash: "h9", Size: 3},
	}, 20)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 20 {
		t.Errorf("cursor = %d, want 20 (an unpositioned event cannot advance it)", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Path != "sparse.txt" {
		t.Fatalf("ops = %+v, want one download of sparse.txt", ops)
	}
}

// TestReconcileEventAboveCursorStillApplies is the control: the ordinary forward event is
// untouched by the gate.
func TestReconcileEventAboveCursorStillApplies(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, cursor, err := e.reconcile(1, []actions.Event{
		{SyncID: 21, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 2},
	}, 20)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 21 || len(ops) != 1 {
		t.Fatalf("cursor = %d, ops = %+v; want 21 and one download", cursor, ops)
	}
	if _, err := st.GetBaseline(1, "a.txt"); err != nil {
		t.Errorf("GetBaseline: %v", err)
	}
}

// TestSelfEchoCreateSkipsForeignNodeAtPath pins the identity gate: our own echoed create for
// node F must not rewrite the record of a DIFFERENT node N that has since taken the path.
//
// "Since" is what the positions say (finding H3 made the gate directional): N's record is
// stamped at 60 and our echo sits at 31, so the server's latest word about a.txt is N's, and the
// echo is the stale one. The mirror - a record stamped BELOW the echo, i.e. one naming the node
// our own push replaced - is TestSelfEchoCreateAdoptsSuccessorNode, where skipping used to leave
// the record naming a corpse and cost a whole replica.
func TestSelfEchoCreateSkipsForeignNodeAtPath(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "N", FileType: "file", ServerHash: "hN", LocalHash: "hN",
		LocalSize: 4, ServerSyncID: 60, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed successor baseline: %v", err)
	}

	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 31, Action: "create", FileID: "F", Path: "a.txt", Hash: "hF", Size: 9,
			CommitterSessionID: "self"},
	}, 30); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "N" || b.LocalHash != "hN" || b.ServerHash != "hN" {
		t.Errorf("successor record rewritten by a stale self-echo: %+v, want node N with hN intact", b)
	}
	if p, perr := st.PathByID(1, "N"); perr != nil || p != "a.txt" {
		t.Errorf("PathByID(N) = %q, %v; want a.txt still indexed", p, perr)
	}
	if _, perr := st.PathByID(1, "F"); perr == nil {
		t.Errorf("the stale echo bound our own id F to the successor's path")
	}
}

// TestSelfEchoCreateOverlaysOwnNode is the control: an echo naming the SAME node still
// overlays its facts (the sparse-echo behaviour selfEchoCreateBaseline exists for).
func TestSelfEchoCreateOverlaysOwnNode(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: "h1", LocalHash: "h1",
		LocalSize: 4, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 31, Action: "modify", FileID: "F", Path: "a.txt", Hash: "h2", Size: 7,
			CommitterSessionID: "self"},
	}, 30); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.ServerHash != "h2" || b.LocalHash != "h2" || b.LocalSize != 7 {
		t.Errorf("self-echo of our own node did not overlay its facts: %+v", b)
	}
}

// TestSelfEchoCreateUnpositionedDoesNotSupersede pins the disarm of the positional arm: an echo
// with no sync id has no place in the log, so it cannot claim to be the later statement about
// the path and the foreign record is left alone (the mirror of reconcile's own unpositioned-event
// exemption above, which folds such an event without letting it move the cursor).
func TestSelfEchoCreateUnpositionedDoesNotSupersede(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "N", FileType: "file", ServerHash: "hN", LocalHash: "hN",
		LocalSize: 4, State: store.StateSynced, // no position of its own either
	}); err != nil {
		t.Fatalf("seed successor baseline: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{Action: "create", FileID: "F", Path: "a.txt", Hash: "hF", Size: 9, CommitterSessionID: "self"},
	}, 30); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "N" || b.ServerHash != "hN" {
		t.Errorf("unpositioned echo rewrote the foreign record: %+v, want node N intact", b)
	}
}

// TestSelfEchoCreateOnIDLessRecordStillOverlays pins the sparse exemption shared with
// applySelfEchoDelete: with either id empty there is no identity to compare, so the
// historical path-only behaviour stands.
func TestSelfEchoCreateOnIDLessRecordStillOverlays(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 4, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed id-less baseline: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 31, Action: "create", FileID: "F", Path: "a.txt", Hash: "h2", Size: 7,
			CommitterSessionID: "self"},
	}, 30); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "F" || b.ServerHash != "h2" {
		t.Errorf("id-less record was not adopted by the echo: %+v", b)
	}
}

// TestReconcileBelowCursorDeleteSparesQueuedDownload is the regression pin for the gate's
// placement: the filter runs over the WHOLE batch before any pass reads it, not per-event
// inside the reconcile loop.
//
// dropSupersededQueuedDownloads is destructive - it acks queued ops and drops their
// baselines - and deleteSupersedes matches an ancestor-directory delete by path prefix
// regardless of file id. So a re-delivered, already-consumed `delete d` reaching that pass
// would ack the legitimately queued OpDownload for `d/x` and drop its pending baseline,
// while the loop skipped the very same delete and enqueued no OpRemove to replace it: the
// server node would never materialise locally, and neither the queue nor the next scan
// (which drives only StateSynced records) would re-detect it - a silent permanent divergence
// in exactly the re-delivery shape the gate was added for.
func TestReconcileBelowCursorDeleteSparesQueuedDownload(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 1,
		Path: "d/x.txt", FileID: "G", LocalHash: "hx", LocalSize: 4,
	}); err != nil {
		t.Fatalf("seed queued download: %v", err)
	}
	if err := st.PutBaseline(1, "d/x.txt", store.Baseline{
		FileID: "G", FileType: "file", ServerHash: "hx", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
	}

	ops, cursor, err := e.reconcile(1, []actions.Event{
		{SyncID: 50, Action: "delete", FileID: "D", Path: "d", IsDir: true}, // below the cursor
	}, 100)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("consumed delete produced %d ops, want 0: %+v", len(ops), ops)
	}
	if cursor != 100 {
		t.Errorf("cursor = %d, want 100 (unchanged)", cursor)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Path != "d/x.txt" {
		t.Fatalf("queued download was dropped by a consumed delete: %+v", pending)
	}
	if _, err := st.GetBaseline(1, "d/x.txt"); err != nil {
		t.Fatalf("pending baseline was dropped by a consumed delete: %v", err)
	}
}
