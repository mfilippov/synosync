package engine

// These tests cover the edit-delete race: the inbound remove pipeline used to identify its target
// by PATH alone, so a delete of node F applied to whatever node happened to hold F's old path.
// The motivating interleaving is the laptop-sleep race, which the cycle order (localScan ->
// drainQueue -> pollOnce, loop.go) makes the DEFAULT rather than a narrow window:
//
//	machine A sleeps holding a local edit of p (node F); machine B deletes p, so the server
//	emits [delete F@p]. A wakes: its scan+drain push the edit BEFORE the poll, and since p is
//	free server-side the server assigns a FRESH node N - markSynced records {N, synced,
//	LocalHash: the edit} at p and drops byid[F].
//
// The delete of F then found N's record at p: its last-synced hash IS the current on-disk
// content, so the divergence guard read "in sync -> server authority" by construction and
// fsutil.Remove destroyed the just-pushed edit with no _Conflict copy. The vanished-but-synced
// record then made the next scan emit an outbound OpRemove deleting N server-side too, so the
// edit was lost from BOTH replicas (only the server-side recycle bin held it).
//
// Three gates close it, all keyed on node identity rather than path: the reconcile-time capture
// (baseIdentifiesNode), flipPending, and applyRemove's own success path (the live mirror of the
// dead-letter twin dropRemoveBaselineAt).

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedSupersededNodeRace runs the shared prefix of the laptop-sleep interleaving: a tracked node
// F synced at its original content, edited offline, then pushed by the scan+drain that precede
// the poll - so the path is tracked as a FRESH server node N carrying the edit. It returns the
// engine, store, view dir and the edited bytes, with the delete of F not yet reconciled.
func seedSupersededNodeRace(t *testing.T) (*Engine, *store.Store, *pushClient, string, []byte) {
	t.Helper()
	ts := time.Date(2026, 7, 24, 9, 15, 0, 0, time.UTC)

	origBytes := []byte("the-original-synced-content")
	editBytes := []byte("the-offline-edit-the-server-never-saw")
	editHash := md4.ContentHash(editBytes)

	c := &pushClient{uploadNode: map[string]actions.Node{
		"doc.txt": {FileID: "N", Hash: editHash, MTime: 999},
	}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", ts)

	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: md4.ContentHash(origBytes),
		LocalHash: md4.ContentHash(origBytes), LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The offline edit, made while the daemon could not reach the server.
	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), editBytes, 0o644); err != nil {
		t.Fatalf("seed offline edit: %v", err)
	}

	// The cycle's scan and drain run BEFORE the poll, so the edit is pushed first. The old node
	// was deleted server-side, so the upload lands as a brand-new node N at the freed path.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (push): %v", err)
	}
	b, err := st.GetBaseline(7, "doc.txt")
	if err != nil || b.FileID != "N" || b.State != store.StateSynced {
		t.Fatalf("post-push baseline = %+v (err %v), want synced node N", b, err)
	}
	if _, err := st.PathByID(7, "F"); err == nil {
		t.Fatalf("byid still tracks the superseded node F after the push")
	}
	return e, st, c, viewDir, editBytes
}

// TestInboundDeleteOfSupersededNodePreservesPushedEdit is the edit-delete race regression: the
// server's delete of the OLD node F must not destroy the freshly pushed node N that now holds the
// path. Before the fix the capture read N's record as F's divergence base (a guaranteed match),
// fsutil.Remove destroyed the edit with no _Conflict copy, and DeleteBaseline dropped N's record.
func TestInboundDeleteOfSupersededNodePreservesPushedEdit(t *testing.T) {
	e, st, c, viewDir, editBytes := seedSupersededNodeRace(t)

	// Only now does the poll deliver the other client's delete of the old node F.
	events := []actions.Event{{SyncID: 9, Action: "delete", FileID: "F", Path: "doc.txt"}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want a single OpRemove", ops)
	}
	// Capture gate: the successor's record identifies a DIFFERENT node, so it is no divergence
	// base for this delete (before the fix BaseWasSynced/BaseLocalHash carried the edit's own
	// hash, making the apply-time hash-vs-base check pass by construction).
	if ops[0].BaseWasSynced || ops[0].BaseLocalHash != "" {
		t.Errorf("remove op captured the successor node as its base: %+v", ops[0])
	}
	// flipPending gate: a foreign record is left alone rather than marked as owing remote work.
	if b, err := st.GetBaseline(7, "doc.txt"); err != nil || b.FileID != "N" || b.State != store.StateSynced {
		t.Fatalf("post-reconcile baseline = %+v (err %v), want node N still synced", b, err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (delete): %v", err)
	}

	// The pushed edit survives on disk untouched - not even set aside as a _Conflict copy, since
	// node N holds exactly this content server-side and nothing diverges.
	got, err := os.ReadFile(filepath.Join(viewDir, "doc.txt"))
	if err != nil || string(got) != string(editBytes) {
		t.Fatalf("doc.txt = %q (err %v), want the pushed edit %q", got, err, editBytes)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("read view dir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("view dir = %v, want only doc.txt (no _Conflict copy needed)", entries)
	}
	if b, err := st.GetBaseline(7, "doc.txt"); err != nil || b.FileID != "N" || b.State != store.StateSynced {
		t.Fatalf("post-delete baseline = %+v (err %v), want node N still synced", b, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d op(s) remain", n)
	}

	// The tail: the next scan must not diff a vanished-but-synced record into an outbound
	// OpRemove that would delete N - the only remaining server copy of the edit.
	tailOps, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("post-delete localReconcile: %v", err)
	}
	if len(tailOps) != 0 {
		t.Fatalf("post-delete scan emitted %d op(s) %+v, want none", len(tailOps), tailOps)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (tail): %v", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("server removes = %v, want none", c.removes)
	}
}

// TestInboundDeleteSupersededSelfEchoTailKeepsServerNode covers the second half of the same
// interleaving: the self-echo create of our own node N arrives on a later page, AFTER the delete
// of F was applied. Before the fix the delete had emptied both the path and the baseline, so
// selfEchoCreateBaseline planted a phantom synced record (sparse facts, nothing on disk) that the
// next scan diffed as vanished and pushed as an outbound OpRemove - deleting the live server node
// N. With the delete correctly skipped, the echo merely confirms the record it already holds.
func TestInboundDeleteSupersededSelfEchoTailKeepsServerNode(t *testing.T) {
	e, st, _, viewDir, editBytes := seedSupersededNodeRace(t)

	// Page 1: the foreign delete of the superseded node F.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 9, Action: "delete", FileID: "F", Path: "doc.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile (delete): %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (delete): %v", err)
	}
	// Page 2: the self-echo of our own upload - sparse, as a committing client's echo commonly is.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 10, Action: "create", FileID: "N", Path: "doc.txt", CommitterSessionID: "sess"},
	}, 9); err != nil {
		t.Fatalf("reconcile (self-echo): %v", err)
	}

	b, err := st.GetBaseline(7, "doc.txt")
	if err != nil || b.FileID != "N" || b.State != store.StateSynced {
		t.Fatalf("post-echo baseline = %+v (err %v), want node N synced", b, err)
	}
	if b.LocalHash != md4.ContentHash(editBytes) {
		t.Errorf("post-echo LocalHash = %q, want the pushed edit's hash", b.LocalHash)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "doc.txt")); rerr != nil || string(got) != string(editBytes) {
		t.Fatalf("doc.txt = %q (err %v), want the pushed edit", got, rerr)
	}

	tailOps, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("post-echo localReconcile: %v", err)
	}
	if len(tailOps) != 0 {
		t.Fatalf("post-echo scan emitted %d op(s) %+v, want none", len(tailOps), tailOps)
	}
}

// TestInboundDeleteSameNodeStillApplies is the pin: the identity gates must not blunt an ordinary
// delete. When the path still tracks the node the server deleted, the file and its baseline go.
func TestInboundDeleteSameNodeStillApplies(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 24, 9, 15, 0, 0, time.UTC))

	body := []byte("in-sync-content")
	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: md4.ContentHash(body),
		LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	ops, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "delete", FileID: "F", Path: "doc.txt"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The capture still trusts the node's OWN synced record as the divergence base...
	if len(ops) != 1 || !ops[0].BaseWasSynced || ops[0].BaseLocalHash != md4.ContentHash(body) {
		t.Fatalf("ops = %+v, want the node's own synced base captured", ops)
	}
	// ...and flipPending still flips its own record.
	if b, gerr := st.GetBaseline(7, "doc.txt"); gerr != nil || b.State != store.StatePending {
		t.Fatalf("baseline = %+v (err %v), want StatePending", b, gerr)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "doc.txt")); !os.IsNotExist(err) {
		t.Errorf("file survived an authoritative delete of its own node: %v", err)
	}
	if _, err := st.GetBaseline(7, "doc.txt"); err == nil {
		t.Errorf("baseline survived an authoritative delete of its own node")
	}
}

// TestInboundDeleteIDLessKeepsPathOnlyBehaviour pins the sparse-event contract: an id-less delete
// has no identity to compare, so the historical path-only behaviour stands (the gates are gated on
// a non-empty event/op file id, mirroring applySelfEchoDelete and dropRemoveBaselineAt).
func TestInboundDeleteIDLessKeepsPathOnlyBehaviour(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 24, 9, 15, 0, 0, time.UTC))

	body := []byte("in-sync-content")
	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "F", FileType: "file", LocalHash: md4.ContentHash(body),
		LocalSize: int64(len(body)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "delete", Path: "doc.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "doc.txt")); !os.IsNotExist(err) {
		t.Errorf("id-less delete no longer removes the file: %v", err)
	}
	if _, err := st.GetBaseline(7, "doc.txt"); err == nil {
		t.Errorf("id-less delete no longer drops the baseline")
	}
}
