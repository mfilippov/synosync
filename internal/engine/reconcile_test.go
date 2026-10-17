package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// newReconcileEngine builds an Engine wired to a fresh in-memory store and a fake
// transport, with the given self session id, for the pure reconcile scenarios.
func newReconcileEngine(t *testing.T, selfSession string) (*Engine, *store.Store) {
	t.Helper()
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", selfSession, nil)
	return e, st
}

// TestReconcileCreateModifyEnqueuesDownload checks a file create and a file modify
// both become an inbound OpDownload carrying the server facts, the baseline is left
// pending, and the cursor advances to the highest sync_id. The
// trailing modify's enqueue additionally supersedes the create's still-queued download
// of the same file id (a download always fetches the node's CURRENT content by id, so
// the older op is pure liability), leaving exactly ONE durable op - the newest.
func TestReconcileCreateModifyEnqueuesDownload(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 10, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 3, MTime: 100},
		{SyncID: 12, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "h2", Size: 5, MTime: 200},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 12 {
		t.Errorf("cursor = %d, want 12", cursor)
	}
	if len(ops) != 2 {
		t.Fatalf("enqueued %d ops, want 2: %+v", len(ops), ops)
	}
	for _, op := range ops {
		if op.Type != store.OpDownload || op.Direction != store.DirInbound {
			t.Errorf("op = %+v, want inbound download", op)
		}
		if op.Path != "a.txt" || op.FileID != "f1" {
			t.Errorf("op path/id = %q/%q, want a.txt/f1", op.Path, op.FileID)
		}
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %q, want pending", b.State)
	}
	if b.ServerHash != "h2" || b.ServerSyncID != 12 {
		t.Errorf("baseline server facts = %+v, want hash h2 sync 12", b)
	}
	// The download op carries the server-reported content facts (Phase-G): applyDownload
	// uses LocalSize to pick the chunked resume_download path for a large file and LocalHash
	// to verify the whole-file MD4 of a chunk-assembled result. The modify event (sync 12)
	// reported hash h2, size 5, mtime 200.
	if ops[1].LocalHash != "h2" || ops[1].LocalSize != 5 || ops[1].LocalMTime != 200 {
		t.Errorf("download op should carry the server content facts, got %+v", ops[1])
	}
	// Only the newest download survives durably: the modify's enqueue dropped the
	// create's queued download as superseded.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("queue has %d ops, want 1 (older same-id download superseded)", len(pending))
	}
	if pending[0].Type != store.OpDownload || pending[0].ServerSyncID != 12 {
		t.Errorf("pending = %+v, want the sync-12 download", pending[0])
	}
}

// TestReconcileDirCreateEnqueuesMkdir checks a directory create maps to OpMkdir and
// records a dir baseline.
func TestReconcileDirCreateEnqueuesMkdir(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 4, Action: "create", FileID: "d1", Path: "dir", IsDir: true, MTime: 50},
	}
	ops, cursor, err := e.reconcile(2, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 4 {
		t.Errorf("cursor = %d, want 4", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpMkdir || ops[0].Path != "dir" {
		t.Fatalf("ops = %+v, want one mkdir for dir", ops)
	}
	b, err := st.GetBaseline(2, "dir")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileType != "dir" || b.State != store.StatePending {
		t.Errorf("baseline = %+v, want pending dir", b)
	}
}

// TestReconcileMoveEnqueuesRename checks a move maps to OpRename from the source to
// the destination path and relocates the source baseline (with its facts) to the
// destination path, left pending.
func TestReconcileMoveEnqueuesRename(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(3, "old.txt", store.Baseline{FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 7, Action: "move", FileID: "f9", FromPath: "old.txt", Path: "new.txt"},
	}
	ops, _, err := e.reconcile(3, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("ops = %+v, want one rename", ops)
	}
	op := ops[0]
	if op.Type != store.OpRename || op.Path != "old.txt" || op.NewPath != "new.txt" {
		t.Errorf("rename op = %+v, want old.txt -> new.txt", op)
	}
	// The move's own position rides on the op, exactly as the create/modify and delete arms
	// carry theirs. dropRenameCompensated stamps it onto the compensating download a dropped
	// rename leaves behind (H2), and every positional guard that later reads the rebuilt
	// baseline - removeSupersededByLaterObservation (H3), downloadRelocatedBySelfEcho -
	// disarms itself at 0, so a rename that carried none left the repaired node unguarded.
	if op.ServerSyncID != 7 {
		t.Errorf("rename op ServerSyncID = %d, want the move event's 7", op.ServerSyncID)
	}
	// The source baseline is carried to the destination, taking its facts with it.
	if _, err := st.GetBaseline(3, "old.txt"); !isNotFound(err) {
		t.Errorf("source baseline survived the move: err=%v", err)
	}
	b, err := st.GetBaseline(3, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline(new): %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("destination baseline state = %q, want pending", b.State)
	}
	if b.FileID != "f9" || b.ServerHash != "h9" {
		t.Errorf("destination baseline = %+v, want f9/h9 carried from the source", b)
	}
}

// TestReconcileDeleteEnqueuesRemove checks a delete maps to OpRemove and flips an
// existing baseline to pending (the executor performs the actual deletion later).
func TestReconcileDeleteEnqueuesRemove(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(4, "gone.txt", store.Baseline{FileID: "f3", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 9, Action: "delete", FileID: "f3", Path: "gone.txt"},
	}
	ops, cursor, err := e.reconcile(4, events, 5)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 9 {
		t.Errorf("cursor = %d, want 9", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "gone.txt" {
		t.Fatalf("ops = %+v, want one remove", ops)
	}
	b, err := st.GetBaseline(4, "gone.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %q, want pending", b.State)
	}
}

// TestReconcileSelfEchoAdvancesBaselineNoOp confirms an event committed by this
// client's own session enqueues nothing but writes a synced baseline and advances
// the returned cursor.
func TestReconcileSelfEchoAdvancesBaselineNoOp(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 20, Action: "create", FileID: "f5", Path: "echo.txt", Hash: "hh", Size: 8, MTime: 300, CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(5, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("self-echo enqueued %d ops, want 0", len(ops))
	}
	if cursor != 20 {
		t.Errorf("cursor = %d, want 20", cursor)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue has %d ops, want 0", n)
	}
	b, err := st.GetBaseline(5, "echo.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced", b.State)
	}
	if b.LocalHash != "hh" || b.ServerHash != "hh" {
		t.Errorf("baseline hashes = local %q server %q, want both hh", b.LocalHash, b.ServerHash)
	}
}

// TestReconcileSelfEchoCreatePreservesFacts confirms a sparse self-echo create/modify
// (paths and id only, content facts omitted) preserves the existing baseline's local
// facts rather than zeroing them. The outbound apply that produced this change recorded
// the richest baseline at this path moments ago; rebuilding from the bare event would
// blank LocalHash/LocalSize/LocalMTime, so the next local scan would see a mismatch and
// re-upload the unchanged file (the create/modify analogue of the move fix).
func TestReconcileSelfEchoCreatePreservesFacts(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "echo.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "abc", ServerMTime: 111, ServerSyncID: 9,
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// A bare modify echo: id and path only, no content facts.
	events := []actions.Event{
		{SyncID: 50, Action: "modify", FileID: "f9", Path: "echo.txt", CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 || cursor != 50 {
		t.Fatalf("ops=%+v cursor=%d, want 0 ops cursor 50", ops, cursor)
	}
	b, err := st.GetBaseline(7, "echo.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f9" || b.FileType != "file" {
		t.Errorf("baseline = %+v, want synced file f9", b)
	}
	if b.LocalHash != "abc" || b.ServerHash != "abc" || b.LocalMTime != 111 || b.LocalSize != 42 {
		t.Errorf("self-echo create/modify zeroed preserved facts: %+v", b)
	}
	if b.ServerSyncID != 50 {
		t.Errorf("baseline ServerSyncID = %d, want 50 (refreshed from the event)", b.ServerSyncID)
	}
}

// TestReconcileSelfEchoCreateUpdatesFacts confirms a self-echo create/modify that DOES
// report new content facts adopts them (the bytes changed, the echo carries the new hash).
func TestReconcileSelfEchoCreateUpdatesFacts(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "echo.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "old", ServerMTime: 111, ServerSyncID: 9,
		LocalHash: "old", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 60, Action: "modify", FileID: "f9", Path: "echo.txt", Hash: "new", Size: 99, MTime: 222, CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(7, "echo.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalHash != "new" || b.ServerHash != "new" || b.LocalSize != 99 || b.LocalMTime != 222 {
		t.Errorf("self-echo modify did not adopt reported facts: %+v", b)
	}
}

// TestReconcileSelfEchoUnknownActionSkipped confirms a self-echo event with an
// unrecognised action plants NO baseline. Mirroring opForEvent (which
// yields no op for an unknown action), applySelfEcho must skip it: a non-conformant or
// updated server emitting a new action type with our committer id on an in-view path with
// no local file would otherwise write a phantom StateSynced record (empty facts) that the
// next scan diffs as vanished, pushing a spurious outbound OpRemove of a live server node.
func TestReconcileSelfEchoUnknownActionSkipped(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Seed an unrelated healthy baseline to prove the unknown-action echo neither plants a
	// phantom nor disturbs an existing record.
	if err := st.PutBaseline(3, "keep.txt", store.Baseline{
		FileID: "fk", FileType: "file", ServerHash: "k", LocalHash: "k", LocalSize: 7,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 70, Action: "link", FileID: "f7", Path: "new.txt", Hash: "hh", Size: 5, MTime: 100, CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(3, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("unknown-action self-echo enqueued %d ops, want 0", len(ops))
	}
	if cursor != 70 {
		t.Errorf("cursor = %d, want 70 (advanced past the skipped event)", cursor)
	}
	if _, err := st.GetBaseline(3, "new.txt"); !isNotFound(err) {
		t.Errorf("phantom baseline planted for unknown-action self-echo: err=%v", err)
	}
	keep, err := st.GetBaseline(3, "keep.txt")
	if err != nil {
		t.Fatalf("GetBaseline keep: %v", err)
	}
	if keep.LocalSize != 7 || keep.LocalHash != "k" {
		t.Errorf("unrelated baseline disturbed: %+v", keep)
	}
}

// TestReconcileSelfEchoTruncateToZeroAdoptsZeroSize confirms a self-echo of our own
// truncate-to-zero (a content-bearing modify: fresh hash and mtime, Size 0) adopts the
// genuine zero LocalSize rather than keeping the stale one. The stale
// LocalSize would otherwise never match the on-disk (size,mtime) again and force a per-scan
// re-hash forever. Before the fix the "ev.Size != 0" guard kept LocalSize at 42.
func TestReconcileSelfEchoTruncateToZeroAdoptsZeroSize(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "echo.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "abc", ServerMTime: 111, ServerSyncID: 9,
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The empty-content MD4 a truncate-to-zero carries, plus a fresh mtime; Size 0.
	empty := "31d6cfe0d16ae931b73c59d7e0c089c0"
	events := []actions.Event{
		{SyncID: 80, Action: "modify", FileID: "f9", Path: "echo.txt", Hash: empty, Size: 0, MTime: 222, CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(7, "echo.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalSize != 0 {
		t.Errorf("LocalSize = %d, want 0 (truncate-to-zero adopted)", b.LocalSize)
	}
	if b.LocalMTime != 222 || b.LocalHash != empty {
		t.Errorf("baseline did not adopt truncate facts: %+v", b)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced", b.State)
	}
}

// TestReconcileSelfEchoContentModifyOmittedSizeKeepsLocalSize confirms a content-bearing
// self-echo modify that omits Size (a REAL non-empty hash and a fresh mtime, but Size 0 -
// the server did not restate the unchanged size) keeps the recorded LocalSize rather than
// zeroing it (the mirror of the truncate-to-zero case). Zeroing a non-empty file's LocalSize would
// make the cheap-scan (size,mtime) hint never match the on-disk size again, forcing a full re-hash
// of that file on every scan forever - the same failure the truncate-to-zero fix set out to kill,
// inverted onto any size-omitting content echo. Only the empty-content hash proves a true zero.
func TestReconcileSelfEchoContentModifyOmittedSizeKeepsLocalSize(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(7, "echo.txt", store.Baseline{
		FileID: "f9", FileType: "file",
		ServerHash: "abc", ServerMTime: 111, ServerSyncID: 9,
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// A content-bearing modify echo: a real new hash and a fresh mtime, but Size omitted (0).
	events := []actions.Event{
		{SyncID: 80, Action: "modify", FileID: "f9", Path: "echo.txt", Hash: "def", MTime: 222, CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(7, "echo.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalSize != 42 {
		t.Errorf("LocalSize = %d, want 42 (size-omitting content echo must keep recorded size)", b.LocalSize)
	}
	// The reported facts are still adopted.
	if b.LocalHash != "def" || b.LocalMTime != 222 {
		t.Errorf("baseline did not adopt reported facts: %+v", b)
	}
}

// TestReconcileSelfEchoDeleteDropsBaseline confirms a self-echo delete removes the
// baseline rather than writing a synced one.
func TestReconcileSelfEchoDeleteDropsBaseline(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(6, "x.txt", store.Baseline{FileID: "f8", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 30, Action: "delete", FileID: "f8", Path: "x.txt", CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(6, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 || cursor != 30 {
		t.Fatalf("ops=%+v cursor=%d, want 0 ops cursor 30", ops, cursor)
	}
	if _, err := st.GetBaseline(6, "x.txt"); !isNotFound(err) {
		t.Errorf("baseline still present after self-echo delete: err=%v", err)
	}
}

// TestReconcileSelfEchoDeleteSkipsRecreatedNode confirms the self-echo delete drop is
// identity-gated (mirroring dropSelfEchoMoveSource): when the path is
// already tracked by a DIFFERENT node - our delete of the old node (f8) was followed by a
// create of a new one (f9) at the same path, reconciled before the delete's late echo
// arrived - the echo must not drop the recreated node's baseline, which would churn a
// spurious full re-upload of an already-synced file on the next scan.
func TestReconcileSelfEchoDeleteSkipsRecreatedNode(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(6, "x.txt", store.Baseline{
		FileID: "f9", FileType: "file", LocalHash: "new", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed recreated baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 31, Action: "delete", FileID: "f8", Path: "x.txt", CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(6, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 || cursor != 31 {
		t.Fatalf("ops=%+v cursor=%d, want 0 ops cursor 31", ops, cursor)
	}
	b, err := st.GetBaseline(6, "x.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v (recreated node's baseline must survive the stale echo)", err)
	}
	if b.FileID != "f9" || b.State != store.StateSynced {
		t.Errorf("baseline = %+v, want the recreated f9 node untouched", b)
	}
}

// TestReconcileSelfEchoDeleteSparseEventStillDrops pins the ungated branches of the
// identity check: with no event file id - or a tracked baseline that never got one -
// there is no identity to compare, so the path snapshot keeps the unconditional drop.
func TestReconcileSelfEchoDeleteSparseEventStillDrops(t *testing.T) {
	t.Run("sparse event id", func(t *testing.T) {
		e, st := newReconcileEngine(t, "self")
		if err := st.PutBaseline(6, "x.txt", store.Baseline{FileID: "f8", FileType: "file", State: store.StateSynced}); err != nil {
			t.Fatalf("seed baseline: %v", err)
		}
		events := []actions.Event{
			{SyncID: 32, Action: "delete", Path: "x.txt", CommitterSessionID: "self"},
		}
		if _, _, err := e.reconcile(6, events, 0); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if _, err := st.GetBaseline(6, "x.txt"); !isNotFound(err) {
			t.Errorf("baseline still present after sparse self-echo delete: err=%v", err)
		}
	})
	t.Run("id-less baseline", func(t *testing.T) {
		e, st := newReconcileEngine(t, "self")
		if err := st.PutBaseline(6, "x.txt", store.Baseline{FileType: "file", State: store.StateSynced}); err != nil {
			t.Fatalf("seed baseline: %v", err)
		}
		events := []actions.Event{
			{SyncID: 33, Action: "delete", FileID: "f8", Path: "x.txt", CommitterSessionID: "self"},
		}
		if _, _, err := e.reconcile(6, events, 0); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if _, err := st.GetBaseline(6, "x.txt"); !isNotFound(err) {
			t.Errorf("baseline still present after self-echo delete of id-less baseline: err=%v", err)
		}
	})
}

// TestReconcileSelfEchoMovePreservesFacts confirms a self-echo move relocates and merges
// the existing baseline rather than rebuilding a bare one from the sparse move event. A move
// event commonly omits hash/size/mtime, so building straight from it would zero those facts
// while marking the record synced; the moved node's content did not change, so
// they must travel with it to the destination.
func TestReconcileSelfEchoMovePreservesFacts(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(6, "src.txt", store.Baseline{
		FileID: "f1", FileType: "file",
		ServerHash: "abc", ServerMTime: 111, ServerSyncID: 9,
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// A bare move event: paths and id only, no content facts.
	events := []actions.Event{
		{SyncID: 40, Action: "move", FileID: "f1", FromPath: "src.txt", Path: "dst.txt", CommitterSessionID: "self"},
	}
	ops, cursor, err := e.reconcile(6, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 || cursor != 40 {
		t.Fatalf("ops=%+v cursor=%d, want 0 ops cursor 40", ops, cursor)
	}
	if _, err := st.GetBaseline(6, "src.txt"); !isNotFound(err) {
		t.Errorf("source baseline survived the self-echo move: err=%v", err)
	}
	b, err := st.GetBaseline(6, "dst.txt")
	if err != nil {
		t.Fatalf("GetBaseline(dst): %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f1" || b.FileType != "file" {
		t.Errorf("moved baseline = %+v, want synced file f1", b)
	}
	if b.ServerHash != "abc" || b.LocalHash != "abc" || b.LocalMTime != 111 || b.LocalSize != 42 {
		t.Errorf("moved baseline lost preserved facts: %+v", b)
	}
	if b.ServerSyncID != 40 {
		t.Errorf("moved baseline ServerSyncID = %d, want 40 (refreshed from the event)", b.ServerSyncID)
	}
	if p, err := st.PathByID(6, "f1"); err != nil || p != "dst.txt" {
		t.Errorf("PathByID(f1) = %q, %v, want dst.txt", p, err)
	}
}

// TestReconcileSelfEchoMoveEmptyIDDropsSource confirms a self-echo move of a local-only
// node (empty file id, so never byid-indexed) still removes the source baseline. A plain
// PutBaseline at the destination would relocate an indexed node through the byid index but
// leave an unindexed one stranded at the source; the destination write goes through
// MoveBaseline so the source is dropped in the same transaction.
func TestReconcileSelfEchoMoveEmptyIDDropsSource(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(6, "src.txt", store.Baseline{
		FileType:  "file",
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 40, Action: "move", FromPath: "src.txt", Path: "dst.txt", CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(6, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := st.GetBaseline(6, "src.txt"); !isNotFound(err) {
		t.Errorf("source baseline survived the empty-id self-echo move: err=%v", err)
	}
	b, err := st.GetBaseline(6, "dst.txt")
	if err != nil {
		t.Fatalf("GetBaseline(dst): %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != "abc" || b.LocalMTime != 111 || b.LocalSize != 42 {
		t.Errorf("moved baseline = %+v, want synced with preserved facts", b)
	}
}

// TestReconcileSelfEchoMoveReplayPrefersDestBaseline confirms that replaying a self-echo
// move after the source baseline is already gone (a prior apply relocated it; a crash
// before the cursor persisted re-runs the event) keeps the rich destination baseline
// rather than overwriting it with the sparse move event, which omits the content facts.
func TestReconcileSelfEchoMoveReplayPrefersDestBaseline(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Simulate the post-first-apply state: the source is gone and the destination already
	// holds the rich, synced baseline the first apply carried across.
	if err := st.PutBaseline(6, "dst.txt", store.Baseline{
		FileID: "f1", FileType: "file",
		ServerHash: "abc", ServerMTime: 111, ServerSyncID: 40,
		LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dst baseline: %v", err)
	}
	// A bare move event re-pulled on replay: paths and id only, no content facts.
	events := []actions.Event{
		{SyncID: 40, Action: "move", FileID: "f1", FromPath: "src.txt", Path: "dst.txt", CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(6, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(6, "dst.txt")
	if err != nil {
		t.Fatalf("GetBaseline(dst): %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f1" || b.FileType != "file" {
		t.Errorf("replayed baseline = %+v, want synced file f1", b)
	}
	if b.ServerHash != "abc" || b.LocalHash != "abc" || b.LocalMTime != 111 || b.LocalSize != 42 {
		t.Errorf("replay zeroed preserved facts: %+v", b)
	}
}

// TestReconcileSelfEchoMoveReplayIgnoresRecreatedSource confirms that replaying a self-echo
// move whose source path has since been taken by a recreated, different node resolves the
// moved node by file id - carrying its rich facts to the destination - instead of copying the
// recreated node's facts off the source path.
func TestReconcileSelfEchoMoveReplayIgnoresRecreatedSource(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Post-first-apply state: the moved node f1 already lives (rich) at dst.txt, and the
	// source path src.txt has been recreated as a different node f2 with different facts.
	if err := st.PutBaseline(6, "dst.txt", store.Baseline{
		FileID: "f1", FileType: "file",
		ServerHash: "abc", LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dst: %v", err)
	}
	if err := st.PutBaseline(6, "src.txt", store.Baseline{
		FileID: "f2", FileType: "file",
		ServerHash: "zzz", LocalHash: "zzz", LocalMTime: 999, LocalSize: 7,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed recreated src: %v", err)
	}
	events := []actions.Event{
		{SyncID: 40, Action: "move", FileID: "f1", FromPath: "src.txt", Path: "dst.txt", CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(6, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(6, "dst.txt")
	if err != nil {
		t.Fatalf("GetBaseline(dst): %v", err)
	}
	if b.FileID != "f1" || b.ServerHash != "abc" || b.LocalHash != "abc" || b.LocalMTime != 111 || b.LocalSize != 42 {
		t.Errorf("replay copied facts from the recreated source: dst = %+v, want f1's rich facts", b)
	}
	src, err := st.GetBaseline(6, "src.txt")
	if err != nil || src.FileID != "f2" || src.ServerHash != "zzz" {
		t.Errorf("recreated source baseline = %+v, err=%v; want f2 untouched", src, err)
	}
}

// TestReconcileSelfEchoMultiHopMoveReplayKeepsRichFacts confirms a multi-hop self-echo move
// replay (a->b->c after the node already reached c) resolves the node by file id at each hop,
// so it does not write a sparse baseline at the first hop and evict the rich record at c
// through the byid index.
func TestReconcileSelfEchoMultiHopMoveReplayKeepsRichFacts(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Post-first-apply state of a->b->c: node f1 already lives (rich) at c.txt.
	if err := st.PutBaseline(6, "c.txt", store.Baseline{
		FileID: "f1", FileType: "file",
		ServerHash: "abc", LocalHash: "abc", LocalMTime: 111, LocalSize: 42,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed c: %v", err)
	}
	events := []actions.Event{
		{SyncID: 40, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "self"},
		{SyncID: 41, Action: "move", FileID: "f1", FromPath: "b.txt", Path: "c.txt", CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(6, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(6, "c.txt")
	if err != nil {
		t.Fatalf("GetBaseline(c): %v", err)
	}
	if b.FileID != "f1" || b.ServerHash != "abc" || b.LocalSize != 42 {
		t.Errorf("multi-hop replay zeroed facts: c = %+v, want f1 rich", b)
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if _, err := st.GetBaseline(6, p); !isNotFound(err) {
			t.Errorf("stale baseline left at %q: err=%v", p, err)
		}
	}
	if p, err := st.PathByID(6, "f1"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f1) = %q, %v; want c.txt", p, err)
	}
}

// TestReconcileSelfEchoDirMoveRelocatesDescendants confirms a self-echo directory move
// re-homes the descendant baselines under the new prefix. No executor op is queued for a
// self-echo, so reconcile is the only place that can carry the subtree's records across
// after the local commit already moved them on disk.
func TestReconcileSelfEchoDirMoveRelocatesDescendants(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	for path, b := range map[string]store.Baseline{
		"d":         {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"d/x.txt":   {FileID: "fx", FileType: "file", State: store.StateSynced},
		"d/sub":     {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"d/sub/y.t": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(6, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "fd", IsDir: true, FromPath: "d", Path: "e", CommitterSessionID: "self"},
	}
	if _, _, err := e.reconcile(6, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, old := range []string{"d", "d/x.txt", "d/sub", "d/sub/y.t"} {
		if _, err := st.GetBaseline(6, old); !isNotFound(err) {
			t.Errorf("baseline survived under old prefix %q: err=%v", old, err)
		}
	}
	for path, id := range map[string]string{"e": "fd", "e/x.txt": "fx", "e/sub": "fs", "e/sub/y.t": "fy"} {
		b, err := st.GetBaseline(6, path)
		if err != nil {
			t.Errorf("GetBaseline(%q): %v", path, err)
			continue
		}
		if b.FileID != id || b.State != store.StateSynced {
			t.Errorf("baseline %q = %+v, want synced id %q", path, b, id)
		}
		if p, err := st.PathByID(6, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}
}

// TestReconcileMixedBatchDirOrdering confirms that an out-of-sync_id batch is
// re-sorted strictly by ascending sync_id and enqueued in that order (no reshuffle by
// op type or path): the parent dir (sync 1), its child dir (sync 2) and the file
// (sync 3) precede the two deletes (sync 4, 5) purely because their sync_ids do.
func TestReconcileMixedBatchDirOrdering(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Intentionally shuffled: reconcile must re-sort by sync_id and keep that order.
	events := []actions.Event{
		{SyncID: 3, Action: "create", FileID: "fc", Path: "a/b/c.txt", Hash: "h"},
		{SyncID: 1, Action: "create", FileID: "da", Path: "a", IsDir: true},
		{SyncID: 2, Action: "create", FileID: "db", Path: "a/b", IsDir: true},
		{SyncID: 4, Action: "delete", FileID: "fo", Path: "a/old/leaf.txt"},
		{SyncID: 5, Action: "delete", FileID: "do", Path: "a/old"},
	}
	ops, cursor, err := e.reconcile(8, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 5 {
		t.Errorf("cursor = %d, want 5", cursor)
	}
	gotPaths := make([]string, len(ops))
	for i, op := range ops {
		gotPaths[i] = op.Path
	}
	want := []string{"a", "a/b", "a/b/c.txt", "a/old/leaf.txt", "a/old"}
	if len(gotPaths) != len(want) {
		t.Fatalf("op paths = %v, want %v", gotPaths, want)
	}
	for i := range want {
		if gotPaths[i] != want[i] {
			t.Fatalf("op order = %v, want %v", gotPaths, want)
		}
	}
	// The durable queue must hold them in the same FIFO order.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	for i := range want {
		if pending[i].Path != want[i] {
			t.Fatalf("queued order = %v, want %v", pending, want)
		}
	}
}

// TestReconcileSamePathKeepsSyncIDOrder confirms a delete (sync 1) followed by a
// create (sync 2) of the same path is enqueued delete-then-download - strictly by
// sync_id - and is never reordered to download-then-remove (which would leave the
// final create absent). Both ops target the same path, so the executor applies them
// in this enqueue order.
func TestReconcileSamePathKeepsSyncIDOrder(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "x.txt", store.Baseline{FileID: "f1", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 1, Action: "delete", FileID: "f1", Path: "x.txt"},
		{SyncID: 2, Action: "create", FileID: "f2", Path: "x.txt", Hash: "h", Size: 3, MTime: 100},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("ops = %+v, want delete then download", ops)
	}
	if ops[0].Type != store.OpRemove || ops[1].Type != store.OpDownload {
		t.Fatalf("op order = [%s %s], want [remove download]", ops[0].Type, ops[1].Type)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if pending[0].Type != store.OpRemove || pending[1].Type != store.OpDownload {
		t.Fatalf("queued order = [%s %s], want [remove download]", pending[0].Type, pending[1].Type)
	}
}

// TestReconcileEmptySessionTreatsAllAsRemote confirms that with no self session id,
// an event carrying a committer id is still applied (no echo suppression).
func TestReconcileEmptySessionTreatsAllAsRemote(t *testing.T) {
	e, _ := newReconcileEngine(t, "")
	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "a.txt", CommitterSessionID: "someone"},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Errorf("enqueued %d ops, want 1 (no self id => everything remote)", len(ops))
	}
}

// TestReconcileUnknownActionSkipped confirms an unrecognised action produces no op
// and does not advance into the queue, while still advancing the cursor.
func TestReconcileUnknownActionSkipped(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 11, Action: "frobnicate", FileID: "f1", Path: "a.txt"},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none for unknown action", ops)
	}
	if cursor != 11 {
		t.Errorf("cursor = %d, want 11 (advances past unknown action)", cursor)
	}
	n, _ := st.PendingLen()
	if n != 0 {
		t.Errorf("queue has %d ops, want 0", n)
	}
}

// TestReconcileSkipsNonCanonicalPath covers a server path that is lexically
// containable but not canonical ("d/." collapses to "d") must be skipped rather than used as a
// baseline key. The store keys baselines by the raw path while the executor's abs() cleans, so
// keying at "d/." while disk sits at "d" would leave a later event naming "d" unable to find the
// root baseline. The event is skipped (no op, no baseline at the non-canonical key) while the
// cursor still advances so it is not re-pulled forever.
func TestReconcileSkipsNonCanonicalPath(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Seed the directory baseline at its canonical key, as a prior synced apply would.
	if err := st.PutBaseline(1, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// A degenerate no-op move whose endpoints are canonically equal but lexically differ.
	events := []actions.Event{
		{SyncID: 9, Action: "move", FileID: "fd", IsDir: true, FromPath: "d", Path: "d/."},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none for non-canonical path", ops)
	}
	if cursor != 9 {
		t.Errorf("cursor = %d, want 9 (advances past skipped event)", cursor)
	}
	// The root baseline must remain at its canonical key, untouched - never re-keyed to "d/.".
	if _, err := st.GetBaseline(1, "d/."); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline keyed at non-canonical path: err=%v", err)
	}
	b, err := st.GetBaseline(1, "d")
	if err != nil {
		t.Fatalf("canonical baseline lost: %v", err)
	}
	if b.FileID != "fd" || b.State != store.StateSynced {
		t.Errorf("canonical baseline = %+v, want synced fd untouched", b)
	}
}

// TestReconcileSkipsSeparatorAliasedPath covers the Windows separator-aliasing variant of
// TestReconcileSkipsNonCanonicalPath: a server path with a literal backslash ("a\b") resolves
// to the same on-disk file
// as the slash form ("a/b") via abs()'s filepath.Join, yet the store would key the baseline
// under the distinct raw path. containable rejects it so the event is skipped (no op, no
// baseline) while the cursor still advances. The aliasing only exists on Windows, where
// filepath treats '\' as a separator; on Unix '\' is an ordinary filename byte and the path is
// legitimately containable, so the test only asserts the rejection there.
func TestReconcileSkipsSeparatorAliasedPath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("backslash aliases a separator only on Windows; on Unix it is a valid filename byte")
	}
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 7, Action: "create", FileID: "f1", Path: `a\b`},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none for separator-aliased path", ops)
	}
	if cursor != 7 {
		t.Errorf("cursor = %d, want 7 (advances past skipped event)", cursor)
	}
	if _, err := st.GetBaseline(1, `a\b`); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline keyed at separator-aliased path: err=%v", err)
	}
}

// TestReconcileDefersDownloadWhenUploadQueued covers the Phase-6d inbound conflict funnel:
// a remote modify for a path that already has a queued outbound upload must NOT enqueue an
// inbound download or rewrite the baseline's server facts - the queued upload is left intact
// so it fires against the now-stale base, draws 24579, and funnels into the keep-both
// resolver. The cursor still advances. A non-conflicting modify in the same batch downloads
// normally, proving the deferral is path-scoped.
func TestReconcileDefersDownloadWhenUploadQueued(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// A local content change for "a.txt" is already queued for upload, with its pending
	// baseline carrying the local facts and the prior server base.
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "base", ServerSyncID: 5,
		LocalHash: "local", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}
	events := []actions.Event{
		{SyncID: 20, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "server", Size: 9, MTime: 200},
		{SyncID: 21, Action: "create", FileID: "f2", Path: "b.txt", Hash: "hb", Size: 3, MTime: 210},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 21 {
		t.Errorf("cursor = %d, want 21 (advances past deferred event)", cursor)
	}
	// Only the non-conflicting "b.txt" download is enqueued; "a.txt" is deferred.
	if len(ops) != 1 {
		t.Fatalf("ops = %+v, want one (b.txt download only)", ops)
	}
	if ops[0].Type != store.OpDownload || ops[0].Path != "b.txt" {
		t.Errorf("op = %+v, want download for b.txt", ops[0])
	}
	// The conflicting baseline keeps its stale server base and local facts untouched.
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending || b.ServerHash != "base" || b.ServerSyncID != 5 {
		t.Errorf("conflicting baseline server facts changed: %+v, want stale base/sync 5 pending", b)
	}
	if b.LocalHash != "local" {
		t.Errorf("conflicting baseline local facts changed: %+v, want local hash preserved", b)
	}
	// The queued outbound upload survives, plus the new inbound download for b.txt - and the
	// keep-both resolve the PROVEN divergence enqueues (finding H1's cheap addition): the event
	// reports "server" where the pre-batch base agreed on "base", so the deferral no longer rests
	// on the 24579 a live DSM almost never sends.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("queue has %d ops, want 3 (upload + b.txt download + keep-both resolve)", len(pending))
	}
	var sawUpload, sawResolve bool
	for _, qo := range pending {
		if qo.Type == store.OpUpload && qo.Path == "a.txt" && qo.Direction == store.DirOutbound {
			sawUpload = true
		}
		if qo.Type == store.OpResolveConflict && qo.Path == "a.txt" {
			sawResolve = true
			if qo.FileID != "f1" {
				t.Errorf("resolve op FileID = %q, want f1 (the node the resolver must key on)", qo.FileID)
			}
			if qo.ServerSyncID != 20 {
				t.Errorf("resolve op ServerSyncID = %d, want 20 (the diverging event's)", qo.ServerSyncID)
			}
		}
	}
	if !sawUpload {
		t.Errorf("queued outbound upload for a.txt was dropped: %+v", pending)
	}
	if !sawResolve {
		t.Errorf("no keep-both resolve enqueued for the provably diverged deferral: %+v", pending)
	}
	// The resolve is queued BEHIND the upload, which still gets its own turn at the server.
	if pending[len(pending)-1].Type != store.OpResolveConflict {
		t.Errorf("resolve is not last in the queue: %+v", pending)
	}
}

// TestReconcileDeferredDownloadNoProofNoResolve is the negative half of the deferral funnel: the
// divergence must be PROVEN before a keep-both resolve is enqueued. An event carrying no content
// hash, and a base carrying none either, prove nothing - a resolve there would set a perfectly
// clean local file aside as a spurious _Conflict copy on every deferral.
func TestReconcileDeferredDownloadNoProofNoResolve(t *testing.T) {
	for _, tc := range []struct {
		name       string
		baseHash   string
		eventHash  string
		wantQueued int
	}{
		{name: "event reports no hash", baseHash: "base", eventHash: "", wantQueued: 1},
		{name: "no agreed base hash", baseHash: "", eventHash: "server", wantQueued: 1},
		{name: "hashes agree", baseHash: "same", eventHash: "same", wantQueued: 1},
		{name: "proven divergence", baseHash: "base", eventHash: "server", wantQueued: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, st := newReconcileEngine(t, "self")
			if err := st.PutBaseline(1, "a.txt", store.Baseline{
				FileID: "f1", FileType: "file", ServerHash: tc.baseHash, ServerSyncID: 5,
				LocalHash: "local", LocalSize: 4, State: store.StatePending,
			}); err != nil {
				t.Fatalf("seed baseline: %v", err)
			}
			if _, err := st.Enqueue(store.Op{
				Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "a.txt", FileID: "f1",
			}); err != nil {
				t.Fatalf("seed upload op: %v", err)
			}
			if _, _, err := e.reconcile(1, []actions.Event{
				{SyncID: 20, Action: "modify", FileID: "f1", Path: "a.txt", Hash: tc.eventHash, Size: 9, MTime: 200},
			}, 0); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			pending, err := st.PendingOps()
			if err != nil {
				t.Fatalf("PendingOps: %v", err)
			}
			if len(pending) != tc.wantQueued {
				t.Fatalf("queue has %d ops, want %d: %+v", len(pending), tc.wantQueued, pending)
			}
		})
	}
}

// TestReconcileDeferredDownloadResolveNotDuplicated pins that two modifies of the SAME node in one
// batch enqueue one keep-both resolve, not one per event: the registration is keyed by node, so a
// churning file cannot flood the queue with resolves of the same path.
func TestReconcileDeferredDownloadResolveNotDuplicated(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "base", ServerSyncID: 5,
		LocalHash: "local", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 20, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "server1", Size: 9, MTime: 200},
		{SyncID: 21, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "server2", Size: 9, MTime: 210},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	resolves := 0
	for _, qo := range pending {
		if qo.Type == store.OpResolveConflict {
			resolves++
			if qo.ServerSyncID != 21 {
				t.Errorf("resolve ServerSyncID = %d, want 21 (the latest diverging event)", qo.ServerSyncID)
			}
		}
	}
	if resolves != 1 {
		t.Fatalf("enqueued %d resolves for one node, want 1: %+v", resolves, pending)
	}
}

// TestReconcileDeferredDownloadResolveNotDuplicatedAcrossBatches is the cross-batch half of that
// rule, and the one the within-batch map cannot cover. The deferral re-proves itself on every
// poll batch for as long as the upload sits queued - the pre-batch snapshot cannot move until
// something applies, and each new peer edit brings a fresh hash - so the queue grew by one
// resolve per batch for the whole life of a wedged upload, every one of them serialised behind
// it on the same path key. The queue itself is the cross-batch memory.
func TestReconcileDeferredDownloadResolveNotDuplicatedAcrossBatches(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "base", ServerSyncID: 5,
		LocalHash: "local", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}
	for i, hash := range []string{"server1", "server2", "server3"} {
		if _, _, err := e.reconcile(1, []actions.Event{{
			SyncID: int64(20 + i), Action: "modify", FileID: "f1", Path: "a.txt",
			Hash: hash, Size: 9, MTime: int64(200 + i),
		}}, 0); err != nil {
			t.Fatalf("reconcile batch %d: %v", i, err)
		}
	}

	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	resolves := 0
	for _, qo := range pending {
		if qo.Type == store.OpResolveConflict {
			resolves++
		}
	}
	if resolves != 1 {
		t.Fatalf("enqueued %d resolves across three batches, want 1: %+v", resolves, pending)
	}
}

// TestReconcileDeferredDownloadDeletedLaterNoResolve pins the other guard on the post-batch
// enqueue: a deferral whose node the SAME batch goes on to delete gets no resolve. The delete's
// own OpRemove is the authoritative outcome, and a resolve queued for a path the server no longer
// holds could only dead-letter as "conflict server node has vanished".
func TestReconcileDeferredDownloadDeletedLaterNoResolve(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "base", ServerSyncID: 5,
		LocalHash: "local", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 20, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "server", Size: 9, MTime: 200},
		{SyncID: 21, Action: "delete", FileID: "f1", Path: "a.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	for _, qo := range pending {
		if qo.Type == store.OpResolveConflict {
			t.Fatalf("a resolve was enqueued for a node the batch deletes: %+v", pending)
		}
	}
}

// TestReconcileDeleteMoveBatchSkipsUploadScan checks the pendingOutboundUploads guard: a
// batch with no file create/modify never scans the queue for pending uploads (a delete/move
// cannot collide with an upload), so a queued outbound upload does not interfere with the
// inbound delete/move applying normally.
func TestReconcileDeleteMoveBatchSkipsUploadScan(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "gone.txt", store.Baseline{
		FileID: "fg", FileType: "file", ServerHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "other.txt", FileID: "fo",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}
	events := []actions.Event{
		{SyncID: 8, Action: "delete", FileID: "fg", Path: "gone.txt"},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 8 {
		t.Errorf("cursor = %d, want 8", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "gone.txt" {
		t.Fatalf("ops = %+v, want one remove for gone.txt", ops)
	}
}

// TestReconcileStaleDeleteKeepsMovedDownload is a regression test: a
// download queued for node f1 at "a.txt" (batch 1) whose node a later batch moves
// a.txt->b.txt (batch 2), followed by a batch that creates AND deletes an unrelated
// node f4 at the download's now-stale enqueue-time path "a.txt" (batch 3). The old
// code matched the delete against the queued op's recorded path and dropped the
// still-valid download: f1 lives on server-side at b.txt, the local replica silently
// never materialises it, and the cursor has moved past its create - the file would
// not appear until its next server-side modification. The queued download must
// survive (matched via its node's CURRENT tracked path and file id) and the drain
// must materialise b.txt.
func TestReconcileStaleDeleteKeepsMovedDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("payload")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", MTime: 1700000000, Size: 7}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Batch 1: create a.txt (f1) - the download is queued but not yet applied.
	b1 := []actions.Event{{SyncID: 1, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 7, MTime: 1700000000}}
	if _, _, err := e.reconcile(3, b1, 0); err != nil {
		t.Fatalf("reconcile batch 1: %v", err)
	}

	// Batch 2: the node moves a.txt -> b.txt before the download applies. The baseline
	// relocates with it; the queued download still records the pre-move path.
	b2 := []actions.Event{{SyncID: 2, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(3, b2, 1); err != nil {
		t.Fatalf("reconcile batch 2: %v", err)
	}

	// Batch 3: an unrelated node f4 is created and deleted at the download's stale path.
	b3 := []actions.Event{
		{SyncID: 3, Action: "create", FileID: "f4", Path: "a.txt", Hash: "h4", Size: 2},
		{SyncID: 4, Action: "delete", FileID: "f4", Path: "a.txt"},
	}
	if _, _, err := e.reconcile(3, b3, 2); err != nil {
		t.Fatalf("reconcile batch 3: %v", err)
	}

	// The still-valid download of f1 must have survived batch 3's delete of f4.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	sawDownload := false
	for _, qo := range pending {
		if qo.Type == store.OpDownload && qo.FileID == "f1" {
			sawDownload = true
		}
	}
	if !sawDownload {
		t.Fatalf("queue = %+v, the moved node's still-valid download was dropped", pending)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt (the moved node never materialised): %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("b.txt = %q, want %q", got, "payload")
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt present after the move and f4's delete: err=%v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	b, err := st.GetBaseline(3, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "f1" || b.State != store.StateSynced {
		t.Errorf("b.txt baseline = %+v, want synced f1", b)
	}
}

// TestReconcileCaseFoldDeleteKeepsOtherNodeDownload is the case-fold
// variant of the test above: on a case-insensitive filesystem a same-batch delete of "A.txt" (node
// f4) folds to the same key as the live create of "a.txt" (node f1). The old path-only
// match elided f1's download - a still-live node silently missing from the replica.
// With both ids present the identity comparison keeps the download; the delete of the
// genuinely distinct node still enqueues its remove.
func TestReconcileCaseFoldDeleteKeepsOtherNodeDownload(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 3},
		{SyncID: 2, Action: "delete", FileID: "f4", Path: "A.txt"},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 2 {
		t.Errorf("cursor = %d, want 2", cursor)
	}
	if len(ops) != 2 || ops[0].Type != store.OpDownload || ops[1].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [download a.txt (f1), remove A.txt (f4)]", ops)
	}
	if ops[0].FileID != "f1" || ops[0].Path != "a.txt" {
		t.Errorf("download op = %+v, want f1 at a.txt", ops[0])
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("queue = %+v, want the download and the remove both queued", pending)
	}
}

// TestReconcileSparseDeleteStillDropsQueuedDownload pins the id-less fallback behind
// the identity gate: supersession must still match by (folded) path when
// either side lacks a file id - a sparse delete event, or a download queued without an
// id - so an id-poor server still converges instead of wedging a doomed download.
func TestReconcileSparseDeleteStillDropsQueuedDownload(t *testing.T) {
	cases := []struct {
		name       string
		downloadID string
		deleteID   string
	}{
		{"sparse delete event", "f1", ""},
		{"id-less queued download", "", "f4"},
		{"both id-less", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
				t.Fatalf("PutView: %v", err)
			}
			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound,
				ViewID: 3, Path: "ghost.txt", FileID: tc.downloadID,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, t.TempDir(), "sess", nil)
			events := []actions.Event{{SyncID: 9, Action: "delete", FileID: tc.deleteID, Path: "ghost.txt"}}
			if _, _, err := e.reconcile(3, events, 4); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			pending, err := st.PendingOps()
			if err != nil {
				t.Fatalf("PendingOps: %v", err)
			}
			if len(pending) != 1 || pending[0].Type != store.OpRemove {
				t.Fatalf("queue = %+v, want only the remove (doomed download dropped)", pending)
			}
		})
	}
}

// TestDeleteSupersedesIdentityMatrix pins deleteSupersedes' matching rules:
// an ancestor-directory delete matches by path regardless of ids, a same-node
// match is id-authoritative when both ids are present (even across a stale path), and
// an id-less side falls back to folded path equality.
func TestDeleteSupersedesIdentityMatrix(t *testing.T) {
	cases := []struct {
		name   string
		del    actions.Event
		cur    string
		fileID string
		want   bool
	}{
		{"same node by id, same path", actions.Event{Action: "delete", Path: "a.txt", FileID: "f1"}, "a.txt", "f1", true},
		{"same node by id, stale path", actions.Event{Action: "delete", Path: "b.txt", FileID: "f1"}, "a.txt", "f1", true},
		{"different node at same path", actions.Event{Action: "delete", Path: "a.txt", FileID: "f4"}, "a.txt", "f1", false},
		{"ancestor dir, different ids", actions.Event{Action: "delete", Path: "dir", FileID: "fd"}, "dir/a.txt", "f1", true},
		{"sparse delete, same path", actions.Event{Action: "delete", Path: "a.txt"}, "a.txt", "f1", true},
		{"id-less download, same path", actions.Event{Action: "delete", Path: "a.txt", FileID: "f4"}, "a.txt", "", true},
		{"both id-less, different path", actions.Event{Action: "delete", Path: "b.txt"}, "a.txt", "", false},
	}
	for _, tc := range cases {
		if got := deleteSupersedes(tc.del, tc.cur, tc.fileID); got != tc.want {
			t.Errorf("%s: deleteSupersedes = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestPreBatchDeletePathResolvesThroughMoves pins the reverse path-mapping a sparse
// id-less delete's remove-base capture relies on: earlier batch
// moves re-base the deleted path back to its pre-batch source (directly, through a
// chain, or by ancestor-directory prefix), an in-batch create ends the walk (the
// deleted node was created there, so no pre-batch record describes it), a modify does
// not (content only), and a non-containable event is ignored exactly as the reconcile
// loop discards it.
func TestPreBatchDeletePathResolvesThroughMoves(t *testing.T) {
	cases := []struct {
		name   string
		events []actions.Event
		path   string
		want   string
	}{
		{
			name:   "no earlier moves keeps the path",
			events: []actions.Event{{Action: "modify", Path: "a.txt"}},
			path:   "a.txt",
			want:   "a.txt",
		},
		{
			name:   "single move maps back to the source",
			events: []actions.Event{{Action: "move", FromPath: "a.txt", Path: "b.txt"}},
			path:   "b.txt",
			want:   "a.txt",
		},
		{
			name: "move chain maps through every hop",
			events: []actions.Event{
				{Action: "move", FromPath: "a.txt", Path: "b.txt"},
				{Action: "move", FromPath: "b.txt", Path: "c.txt"},
			},
			path: "c.txt",
			want: "a.txt",
		},
		{
			name:   "ancestor directory move re-bases by prefix",
			events: []actions.Event{{Action: "move", FromPath: "d", Path: "e"}},
			path:   "e/x.txt",
			want:   "d/x.txt",
		},
		{
			name: "in-batch create ends the walk",
			events: []actions.Event{
				{Action: "move", FromPath: "a.txt", Path: "b.txt"},
				{Action: "move", FromPath: "b.txt", Path: "c.txt"},
				{Action: "create", Path: "b.txt"},
			},
			path: "b.txt",
			want: "b.txt",
		},
		{
			name: "modify does not end the walk",
			events: []actions.Event{
				{Action: "move", FromPath: "a.txt", Path: "b.txt"},
				{Action: "modify", Path: "b.txt"},
			},
			path: "b.txt",
			want: "a.txt",
		},
		{
			name:   "non-containable move is ignored",
			events: []actions.Event{{Action: "move", FromPath: "../evil.txt", Path: "b.txt"}},
			path:   "b.txt",
			want:   "b.txt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The delete under test sits after every listed event.
			if got := preBatchDeletePath(tc.events, len(tc.events), tc.path); got != tc.want {
				t.Errorf("preBatchDeletePath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

// TestReconcileEnqueueFailureLeavesBaselineIntact pins the crash-window
// ordering: reconcile enqueues each op BEFORE markPending rewrites the baseline, so a
// failure (or a crash) between the two leaves the pre-event synced record intact and the
// replay converges cleanly. Under the former order (every baseline mutated in the loop,
// the whole batch enqueued after it) the same failure left a pending baseline with NO
// queued op: the replay's snapshot saw no synced record, captured no divergence base, and
// the re-enqueued download set the untouched local file aside as a spurious _Conflict
// copy plus a re-upload.
func TestReconcileEnqueueFailureLeavesBaselineIntact(t *testing.T) {
	origBytes := []byte("clean-synced-content")
	origHash := md4.ContentHash(origBytes)
	serverBytes := []byte("server-modify-v2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// The artificial failure is the crash-window probe: the event's op never reaches the
	// durable queue, so its baseline must not have been touched either. A create/modify
	// commits both writes in ONE transaction now (enqueueAndMarkPending), so the probe fails
	// that transaction rather than the bare enqueue it used to poison.
	boom := errors.New("enqueue failed")
	e.batchStore = func(func(*store.Batch) error) error { return boom }
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); !errors.Is(err, boom) {
		t.Fatalf("reconcile err = %v, want the injected enqueue failure", err)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != origHash {
		t.Fatalf("baseline after failed enqueue = {State:%q LocalHash:%q}, want the untouched synced record (markPending must not precede the enqueue)",
			b.State, b.LocalHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue holds %d ops after the failed enqueue, want 0", n)
	}

	// Replay (the cursor was never advanced): the snapshot still sees the synced record,
	// so the re-pulled download carries the divergence base and the clean local file is
	// overwritten directly - no spurious keep-both detour.
	e.batchStore = st.Batch
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("replay reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].BaseLocalHash != origHash {
		t.Fatalf("replay ops = %+v, want one download carrying base %q", ops, origHash)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("a.txt = %q (err %v), want the server bytes %q", got, err, serverBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("conflict records = %+v, want none (a clean replay must not funnel)", recs)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		names := make([]string, len(entries))
		for i, en := range entries {
			names[i] = en.Name()
		}
		t.Fatalf("view dir = %v, want only a.txt (no spurious _Conflict copy)", names)
	}
}

// TestReconcileDeferredDownloadRescuedByMove is a regression test at the
// reconcile level. The keep-both deferral (a remote create/modify for a path that already has
// a queued outbound upload is skipped so the upload draws 24579 and the resolver arbitrates)
// rests on the deferred node STAYING on the disputed path. A later same-batch move breaks that:
// [delete F@p, create G@p, move G p->q] leaves G materialised nowhere - its rename can only
// dead-letter, our node having never been tracked at p - while the create event is already
// consumed (the cursor advanced past it), so the server node is permanently absent from the
// replica. The move must therefore re-emit the deferred download at the node's new path, and
// must NOT enqueue the rename (whose source holds the user's still-unpushed local file).
func TestReconcileDeferredDownloadRescuedByMove(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// A local content change for "note.txt" is queued for upload (node F, the pre-existing one).
	if err := st.PutBaseline(1, "note.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: "base", ServerSyncID: 4,
		LocalHash: "local-edit", LocalSize: 9, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "note.txt", FileID: "F",
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "delete", FileID: "F", Path: "note.txt"},
		{SyncID: 6, Action: "create", FileID: "G", Path: "note.txt", Hash: "hg", Size: 12, MTime: 600},
		{SyncID: 7, Action: "move", FileID: "G", FromPath: "note.txt", Path: "moved.txt"},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 7 {
		t.Errorf("cursor = %d, want 7", cursor)
	}
	if len(ops) != 2 {
		t.Fatalf("ops = %+v, want two (the remove and the rescued download)", ops)
	}
	if ops[0].Type != store.OpRemove || ops[0].Path != "note.txt" {
		t.Errorf("first op = %+v, want the remove for note.txt", ops[0])
	}
	rescued := ops[1]
	if rescued.Type != store.OpDownload || rescued.Direction != store.DirInbound {
		t.Fatalf("second op = %+v, want an inbound download (the rename can only dead-letter)", rescued)
	}
	if rescued.Path != "moved.txt" || rescued.FileID != "G" {
		t.Errorf("rescued op path/id = %q/%q, want moved.txt/G", rescued.Path, rescued.FileID)
	}
	// The deferred create's content facts travel with the node; the sync id is the move's (the
	// later fact - ServerSyncID must never rewind).
	if rescued.LocalHash != "hg" || rescued.LocalSize != 12 || rescued.LocalMTime != 600 {
		t.Errorf("rescued op facts = %q/%d/%d, want hg/12/600 from the deferred create",
			rescued.LocalHash, rescued.LocalSize, rescued.LocalMTime)
	}
	if rescued.ServerSyncID != 7 {
		t.Errorf("rescued op ServerSyncID = %d, want 7 (the move's)", rescued.ServerSyncID)
	}
	// The node is tracked pending at its new path, ready for the drain.
	b, err := st.GetBaseline(1, "moved.txt")
	if err != nil {
		t.Fatalf("GetBaseline(moved.txt): %v", err)
	}
	if b.State != store.StatePending || b.FileID != "G" {
		t.Errorf("baseline at moved.txt = %+v, want pending G", b)
	}
	// The queued outbound upload is untouched: the deferral for the ORIGINAL path still stands.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	sawUpload := false
	for _, qo := range pending {
		if qo.Direction == store.DirOutbound && qo.Type == store.OpUpload && qo.Path == "note.txt" {
			sawUpload = true
		}
		if qo.Type == store.OpRename {
			t.Errorf("a rename was enqueued for the never-materialised node: %+v", qo)
		}
		// The batch DID register a proven deferral for note.txt (base "base" vs the create's
		// "hg"), and the move rescue must have removed it: a resolve queued for a path the node
		// has left resolves against nothing, then re-funnels to the new path and wedges. Without
		// this assertion the guard could be deleted with the suite still green.
		if qo.Type == store.OpResolveConflict {
			t.Errorf("a keep-both resolve was enqueued for a path the node has left: %+v", qo)
		}
	}
	if !sawUpload {
		t.Errorf("queued outbound upload for note.txt was dropped: %+v", pending)
	}
}

// TestSelfEchoOverlaysNeverRewindTheSyncStamp pins the monotonic stamp guard on both self-echo
// overlays. Event.SyncID is tolerantly decoded, so an echo that omits sync_id arrives as 0 - and
// the unconditional write erased the record's position with it. Both of finding H3's positional
// gates disarm at a zero stamp (removeSupersededByLaterObservation returns early, and
// downloadRelocatedBySelfEcho's "strictly newer" test can never hold), so one sparse echo
// silently removed the only defence those paths have left. selfEchoSupersedesRecord already
// refuses to let an unpositioned echo DECIDE anything; the write must be as careful.
func TestSelfEchoOverlaysNeverRewindTheSyncStamp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlay func(*store.Baseline, actions.Event)
	}{
		{name: "create/modify echo", overlay: overlaySelfEchoFacts},
		{name: "move echo", overlay: overlaySelfEchoMoveFacts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := store.Baseline{FileID: "f1", FileType: "file", ServerSyncID: 300, State: store.StateSynced}
			tc.overlay(&b, actions.Event{FileID: "f1", Hash: "h", Size: 4, MTime: 9}) // SyncID omitted
			if b.ServerSyncID != 300 {
				t.Fatalf("ServerSyncID = %d, want 300: a sparse echo must not rewind the stamp", b.ServerSyncID)
			}
			tc.overlay(&b, actions.Event{FileID: "f1", SyncID: 400, Hash: "h2", Size: 4, MTime: 10})
			if b.ServerSyncID != 400 {
				t.Errorf("ServerSyncID = %d, want 400: a positioned echo must still advance the stamp", b.ServerSyncID)
			}
		})
	}
}

// TestReconcileDeferredDownloadMovedAwayMaterialises is the end-to-end half of
// TestReconcileDeferredDownloadRescuedByMove: the same [delete F@p, create G@p, move G p->q]
// batch, drained against a
// fake server. The local edit is pushed (the server assigns node N at the freed path, so the
// delete of F is skipped by the identity gate) and the server's node G must appear at q. Before
// the fix reconcile enqueued a rename whose source held N - applyRename dead-lettered it
// ("source holds a different node while its own node is absent"), leaving G nowhere on disk and
// its create event already consumed, with only a generic Dropped trace.
func TestReconcileDeferredDownloadMovedAwayMaterialises(t *testing.T) {
	userBytes := []byte("the-offline-local-edit")
	serverBytes := []byte("the-server-node-that-moved-away")
	userHash := md4.ContentHash(userBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		uploadNode:      map[string]actions.Node{"note.txt": {FileID: "N", Hash: userHash, MTime: 777}},
		downloadContent: map[string][]byte{"G": serverBytes},
		downloadNode: map[string]actions.Node{
			"G": {FileID: "G", Hash: serverHash, MTime: 600, Size: int64(len(serverBytes))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, "note.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "note.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: "base", ServerSyncID: 4,
		LocalHash: userHash, LocalSize: int64(len(userBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Direction: store.DirOutbound, ViewID: 7, Type: store.OpUpload, Path: "note.txt", FileID: "F",
		LocalHash: userHash, LocalSize: int64(len(userBytes)), LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("seed upload op: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "delete", FileID: "F", Path: "note.txt"},
		{SyncID: 6, Action: "create", FileID: "G", Path: "note.txt", Hash: serverHash,
			Size: int64(len(serverBytes)), MTime: 600},
		{SyncID: 7, Action: "move", FileID: "G", FromPath: "note.txt", Path: "moved.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The server node materialises at the path it moved to.
	if got, err := os.ReadFile(filepath.Join(viewDir, "moved.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("moved.txt = %q (err %v), want the server node %q", got, err, serverBytes)
	}
	b, err := st.GetBaseline(7, "moved.txt")
	if err != nil {
		t.Fatalf("GetBaseline(moved.txt): %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "G" {
		t.Errorf("baseline at moved.txt = %+v, want synced G", b)
	}
	// The local edit is untouched: it was pushed as the successor node N, and the delete of the
	// superseded node F never authorised destroying it.
	if got, err := os.ReadFile(filepath.Join(viewDir, "note.txt")); err != nil || string(got) != string(userBytes) {
		t.Fatalf("note.txt = %q (err %v), want the local edit %q", got, err, userBytes)
	}
	dl, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dl) != 0 {
		t.Errorf("dead letters = %+v, want none (the rescued download replaces the doomed rename)", dl)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestRescueDeferredMovePrefersMoveContentFacts pins rescueDeferredMove's override arms. A move
// event is usually sparse (path facts only), which is why the rescued download inherits the
// deferred create's hash/size/mtime - but a server that DOES report content facts on the move is
// reporting the node's state AFTER it, so those facts must win. Dropping them would leave the
// rescued download verifying against the older create's hash and failing ErrHashMismatch every
// drain; preferring the create's over a present move fact would record a wrong baseline.
func TestRescueDeferredMovePrefersMoveContentFacts(t *testing.T) {
	deferred := actions.Event{
		SyncID: 6, Action: "create", FileID: "G", Path: "note.txt",
		Hash: "hg", Size: 12, MTime: 600,
	}
	tests := []struct {
		name                string
		move                actions.Event
		wantHash            string
		wantSize, wantMTime int64
	}{
		{
			name:     "sparse move keeps the deferred create's facts",
			move:     actions.Event{SyncID: 7, Action: "move", FileID: "G", FromPath: "note.txt", Path: "moved.txt"},
			wantHash: "hg", wantSize: 12, wantMTime: 600,
		},
		{
			name: "content-bearing move overrides every fact",
			move: actions.Event{
				SyncID: 7, Action: "move", FileID: "G", FromPath: "note.txt", Path: "moved.txt",
				Hash: "hmv", Size: 40, MTime: 900,
			},
			wantHash: "hmv", wantSize: 40, wantMTime: 900,
		},
		{
			name: "partially sparse move overrides only what it reports",
			move: actions.Event{
				SyncID: 7, Action: "move", FileID: "G", FromPath: "note.txt", Path: "moved.txt",
				Size: 40,
			},
			wantHash: "hg", wantSize: 40, wantMTime: 600,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rescueDeferredMove(deferred, tc.move)
			if got.Action != "create" || got.FileID != "G" {
				t.Errorf("rescued event = %+v, want the deferred create's action and id", got)
			}
			if got.Path != "moved.txt" || got.FromPath != "" || got.FromFileID != "" {
				t.Errorf("rescued event addressing = %+v, want a plain create at the move's destination", got)
			}
			if got.SyncID != 7 {
				t.Errorf("rescued SyncID = %d, want the move's 7 (the later fact)", got.SyncID)
			}
			if got.Hash != tc.wantHash || got.Size != tc.wantSize || got.MTime != tc.wantMTime {
				t.Errorf("rescued facts = %q/%d/%d, want %q/%d/%d",
					got.Hash, got.Size, got.MTime, tc.wantHash, tc.wantSize, tc.wantMTime)
			}
		})
	}
}
