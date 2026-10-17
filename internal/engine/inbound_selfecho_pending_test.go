package engine

// These tests pin the self-echo pending fix: a self-echo create/modify (the echo of our own
// already-pushed upload) must NOT advance an existing StatePending baseline to StateSynced
// during reconcile - only the apply path may do that. A foreign [move F a->b] relocates F's
// baseline to b as StatePending (relocatePending); our own upload's echo (modify F@b, committed
// before the move but delivered after it) used to overlay that relocated record and stamp it
// synced still in reconcile, forging exactly the proof supersededReplay (inbound_rename.go)
// trusts as "a prior apply already ran". The queued rename then skipped the on-disk move, a
// same-batch recreate's download landed over the un-moved file at the source, and the next
// local scan diffed the synced-but-fileless destination into an outbound OpRemove deleting the
// live server node - a foreign rename destroyed plus a permanent silent replica divergence.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestReconcileSelfEchoModifyPreservesRelocatedPending is the store-level unit: a foreign move
// relocates F's baseline to the destination as StatePending, and the same-batch self-echo
// modify at that destination must overlay the content facts (ServerSyncID, hash) while
// PRESERVING the pending state the queued OpRename still owns.
func TestReconcileSelfEchoModifyPreservesRelocatedPending(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "F", FileType: "file",
		ServerHash: "h2", LocalHash: "h2", LocalSize: 22, LocalMTime: 100,
		ServerSyncID: 50, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	ops, cursor, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 101, Action: "modify", FileID: "F", Path: "b.txt", Hash: "h2", Size: 22, MTime: 555, CommitterSessionID: "self"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 101 {
		t.Errorf("cursor = %d, want 101", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("ops = %+v, want exactly the queued OpRename", ops)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("relocated baseline state = %q, want pending (the queued rename still owes the apply)", b.State)
	}
	if b.FileID != "F" || b.ServerSyncID != 101 || b.LocalHash != "h2" || b.LocalMTime != 555 {
		t.Errorf("echo did not overlay content facts onto the pending record: %+v", b)
	}
}

// TestReconcileSelfEchoModifyPreservesOutboundPending pins the normal-cycle safety of the
// state-preserve guard: a StatePending record owned by a queued OUTBOUND upload (the user
// edited again before our earlier upload's echo arrived) stays pending - the queued upload's
// (idempotent) apply advances it - rather than being flipped synced by the stale echo.
func TestReconcileSelfEchoModifyPreservesOutboundPending(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "old", LocalHash: "edit2", LocalSize: 9, LocalMTime: 200,
		ServerSyncID: 40, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 60, Action: "modify", FileID: "f9", Path: "doc.txt", Hash: "old", Size: 9, MTime: 150, CommitterSessionID: "self"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(7, "doc.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("outbound-pending baseline state = %q, want pending preserved", b.State)
	}
	if b.ServerSyncID != 60 {
		t.Errorf("ServerSyncID = %d, want 60 (content facts still overlaid)", b.ServerSyncID)
	}
}

// TestReconcileSelfEchoModifyPreservesConflictState: a StateConflict record is owned by the
// keep-both resolver; a late self-echo must not bury the surfaced divergence by flipping it
// synced.
func TestReconcileSelfEchoModifyPreservesConflictState(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "srv", LocalHash: "loc", LocalSize: 9,
		ServerSyncID: 40, State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 60, Action: "modify", FileID: "f9", Path: "doc.txt", CommitterSessionID: "self"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(7, "doc.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("conflict baseline state = %q, want conflict preserved", b.State)
	}
}

// seedSelfEchoPendingPreState wires the self-echo pending scenario pre-state: node F is synced at
// a.txt holding origBytes (content we pushed earlier - the echo of that upload is still in the
// server's event stream).
func seedSelfEchoPendingPreState(t *testing.T, c *pushClient, origBytes []byte) (*Engine, *store.Store, string) {
	t.Helper()
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: md4.ContentHash(origBytes),
		LocalHash: md4.ContentHash(origBytes), LocalSize: int64(len(origBytes)),
		ServerSyncID: 50, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	return e, st, viewDir
}

// assertSelfEchoPendingConverged asserts the converged self-echo pending outcome: F's bytes moved
// to b.txt, G's bytes landed at a.txt, both tracked synced, and - the crux - the next
// local scan emits NO ops (in particular no outbound OpRemove{b.txt} deleting the live server
// node).
func assertSelfEchoPendingConverged(t *testing.T, e *Engine, st *store.Store, viewDir string, origBytes, gBytes []byte) {
	t.Helper()
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(origBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q (the rename must APPLY on disk)", got, err, origBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(gBytes) {
		t.Fatalf("a.txt = %q (err %v), want the recreated node's content %q", got, err, gBytes)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "F" || b.State != store.StateSynced {
		t.Errorf("b.txt baseline = %+v, want synced F", b)
	}
	a, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline(a.txt): %v", err)
	}
	if a.FileID != "G" || a.State != store.StateSynced {
		t.Errorf("a.txt baseline = %+v, want synced G", a)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0", n)
	}
	recs, _ := st.ListConflicts(7)
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
	// The replica converged, so the next scan must find nothing to push - before the fix the
	// synced-but-fileless b.txt baseline diffed into an outbound OpRemove of the live server node.
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("next scan emitted %+v, want none (especially no OpRemove of the moved node)", ops)
	}
}

// TestInboundSelfEchoPendingModifyMoveRecreateConverges is the primary self-echo pending scenario
// in one batch: [move F a->b (foreign), modify F@b (self-echo of our earlier upload), create G@a
// (foreign move-then-recreate)]. The self-echo lands on the relocated StatePending record; with
// the state preserved, supersededReplay stays false, the rename applies on disk (b = F's
// bytes), the download materialises G at a, and the replica converges. Before the fix the echo
// stamped the record synced, the rename was skipped as a superseded replay, G's download
// clobbered the un-moved file at a, and the next scan pushed OpRemove{b} - deleting the live
// server node.
func TestInboundSelfEchoPendingModifyMoveRecreateConverges(t *testing.T) {
	origBytes := []byte("our-pushed-content-h2")
	origHash := md4.ContentHash(origBytes)
	gBytes := []byte("recreated-node-content-g")
	gHash := md4.ContentHash(gBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"G": gBytes},
		downloadNode:    map[string]actions.Node{"G": {FileID: "G", Hash: gHash, MTime: 777, Size: int64(len(gBytes))}},
	}
	e, st, viewDir := seedSelfEchoPendingPreState(t, c, origBytes)

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 101, Action: "modify", FileID: "F", Path: "b.txt", Hash: origHash, Size: int64(len(origBytes)), MTime: 555, CommitterSessionID: "sess"},
		{SyncID: 102, Action: "create", FileID: "G", Path: "a.txt", Hash: gHash, Size: int64(len(gBytes)), MTime: 777},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertSelfEchoPendingConverged(t, e, st, viewDir, origBytes, gBytes)
}

// TestInboundSelfEchoPendingModifyCrossBatchConverges is the cross-batch variant: the foreign
// move arrives in batch 1 but the drain is deferred (a transient failure between reconciles),
// and the self-echo modify plus the recreate arrive in batch 2 while the rename is still
// queued. The relocated record is live-read by the echo, so the state-preserve guard must hold
// across batches exactly like the same-batch case.
func TestInboundSelfEchoPendingModifyCrossBatchConverges(t *testing.T) {
	origBytes := []byte("our-pushed-content-h2")
	origHash := md4.ContentHash(origBytes)
	gBytes := []byte("recreated-node-content-g")
	gHash := md4.ContentHash(gBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"G": gBytes},
		downloadNode:    map[string]actions.Node{"G": {FileID: "G", Hash: gHash, MTime: 777, Size: int64(len(gBytes))}},
	}
	e, st, viewDir := seedSelfEchoPendingPreState(t, c, origBytes)

	// Batch 1: the foreign move alone. The OpRename is enqueued and the baseline relocated to
	// b.txt as StatePending, but the drain never runs (transient failure).
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile batch 1: %v", err)
	}
	// Batch 2: our upload's echo at the relocated path, then the foreign recreate at the source.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "modify", FileID: "F", Path: "b.txt", Hash: origHash, Size: int64(len(origBytes)), MTime: 555, CommitterSessionID: "sess"},
		{SyncID: 102, Action: "create", FileID: "G", Path: "a.txt", Hash: gHash, Size: int64(len(gBytes)), MTime: 777},
	}, 100); err != nil {
		t.Fatalf("reconcile batch 2: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertSelfEchoPendingConverged(t, e, st, viewDir, origBytes, gBytes)
}
