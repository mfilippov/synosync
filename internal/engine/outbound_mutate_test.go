package engine

// These tests cover revertRelocatedRemove's baseline-state discipline: an
// outbound OpRemove whose target node an inbound rename relocated server-side reverts the
// relocated path's baseline to StateSynced so the next scan re-emits the delete on the new path.
// The revert must preserve a StateConflict record - the divergence is owed to keep-both and must
// stay surfaced (mirroring revertRelocatedOutbound); flipping it synced would let the re-emitted
// OpRemove delete the diverged server copy the resolver has not reconciled yet.
//
// They also cover the outbound remove's pre-turn divergence precondition: a
// queued path-addressed delete must not destroy a server-side modify that landed after the local
// delete was scanned, nor a foreign node that took the path after a delete+recreate.
//
// And the positive exits from a "present" probe: a parent listing that still
// shows the node BY ID under a DIFFERENT leaf name means another client renamed it in place, so
// the op's path can never resolve again. Before the fix every consumer read that as plain
// "present" and retried forever - a permanent wedge (visible only as Stuck, curable only by
// Reset) that also starved the authoritative inbound OpRename serialized behind it on the shared
// path key.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestRevertRelocatedRemovePreservesConflict pins the fix: a StateConflict record at the
// relocated path is left untouched. Before the fix it was flipped to StateSynced, burying the
// divergence and letting the scan's re-emitted OpRemove delete the diverged server copy.
func TestRevertRelocatedRemovePreservesConflict(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, nil, root, "sess", nil)

	// The node's baseline was relocated to c/x and left StateConflict by a racing keep-both.
	seed := store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "local-diverged", LocalSize: 3,
		ServerHash: "server-diverged", State: store.StateConflict,
	}
	if err := st.PutBaseline(7, "c/x", seed); err != nil {
		t.Fatalf("seed conflict baseline: %v", err)
	}

	op := store.Op{Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx"}
	if err := e.revertRelocatedRemove(op, "c/x"); err != nil {
		t.Fatalf("revertRelocatedRemove: %v", err)
	}

	got, err := st.GetBaseline(7, "c/x")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if got.State != store.StateConflict {
		t.Fatalf("relocated baseline state = %q, want StateConflict (divergence buried before the fix)", got.State)
	}
	// The divergence facts are untouched, so keep-both can still reconcile it.
	if got.LocalHash != seed.LocalHash || got.ServerHash != seed.ServerHash {
		t.Errorf("conflict facts mutated: %+v, want %+v", got, seed)
	}
}

// TestRevertRelocatedRemoveRevertsPending is the regression pin: the ordinary StatePending
// relocated record is still flipped to StateSynced so detectRemoves re-emits the OpRemove.
func TestRevertRelocatedRemoveRevertsPending(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, nil, root, "sess", nil)

	if err := st.PutBaseline(7, "c/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "h", LocalSize: 3, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
	}

	op := store.Op{Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx"}
	if err := e.revertRelocatedRemove(op, "c/x"); err != nil {
		t.Fatalf("revertRelocatedRemove: %v", err)
	}

	got, err := st.GetBaseline(7, "c/x")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if got.State != store.StateSynced {
		t.Fatalf("relocated baseline state = %q, want StateSynced", got.State)
	}
}

// TestRevertRelocatedRemoveSyncedNoOp pins the existing StateSynced short-circuit: already
// synced is a no-op (idempotent replay).
func TestRevertRelocatedRemoveSyncedNoOp(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, nil, root, "sess", nil)

	seed := store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "h", LocalSize: 3, State: store.StateSynced,
	}
	if err := st.PutBaseline(7, "c/x", seed); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}

	op := store.Op{Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx"}
	if err := e.revertRelocatedRemove(op, "c/x"); err != nil {
		t.Fatalf("revertRelocatedRemove: %v", err)
	}

	got, err := st.GetBaseline(7, "c/x")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if got.State != store.StateSynced || got.LocalHash != seed.LocalHash {
		t.Errorf("synced baseline mutated: %+v, want %+v", got, seed)
	}
}

// seedRemoveDivergenceRace stages the interleaving the remove-divergence tests start from: a view
// holding a tracked, synced file gone.txt (last agreed with the server at hash h1) plus a keep.txt
// that keeps the view non-empty (so the empty-view debounce does not swallow the scan), with
// gone.txt deleted on disk in the client's offline window. The local scan then emits the OpRemove
// whose destructive turn the pre-turn probe must gate.
func seedRemoveDivergenceRace(t *testing.T, e *Engine, st *store.Store, viewDir string) {
	t.Helper()
	keep := []byte("keep")
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keep, 0o644); err != nil {
		t.Fatalf("seed keep file: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileID: "fk", FileType: "file", LocalHash: md4.ContentHash(keep), LocalSize: int64(len(keep)),
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keep baseline: %v", err)
	}
	// gone.txt: last synced against server hash h1, then deleted locally while offline. No file is
	// written for it, so the scan diffs it as a vanished baseline.
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{
		FileID: "f2", FileType: "file", LocalHash: "h1local", LocalSize: 4, ServerHash: "h1",
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed gone baseline: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "gone.txt" || ops[0].FileID != "f2" {
		t.Fatalf("scan ops = %+v, want a single OpRemove(gone.txt, f2)", ops)
	}
}

// TestOutboundRemoveOfServerModifiedNodeRecoversServerVersion pins the identity precondition: a
// local delete that sat in the queue (offline, a transiently failing drain) must not destroy
// another client's modify of the same node. DESIGN §6 makes "local change + remote change" a
// conflict, and the server is the authority for divergence - so the pre-turn probe sees the
// server's content moved off the base this client last synced, skips the destructive Remove and
// recovers the server version by enqueueing an inbound download onto the now-empty path.
//
// Before the fix the remove verb carried NO identity/divergence precondition on its clean-success
// path (the file id was consulted only in the ErrPathError branch), so the path-addressed delete
// removed the modified node outright: the remote edit was destroyed with no _Conflict copy and
// only the server-side recycle bin to recover it from.
func TestOutboundRemoveOfServerModifiedNodeRecoversServerVersion(t *testing.T) {
	body := []byte("another client edit")
	c := &pushClient{
		// The server holds the SAME node (f2) at the path, but its content has moved to h2.
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "f2", Hash: "h2", Size: int64(len(body)), MTime: 555},
		},
		downloadContent: map[string][]byte{"f2": body},
		downloadNode: map[string]actions.Node{
			"f2": {FileID: "f2", Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: 555},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The destructive turn never ran: the diverged server node is intact.
	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - the server modify must not be deleted", c.removes)
	}
	// The recovery download materialised the server version on the canonical path.
	got, err := os.ReadFile(filepath.Join(viewDir, "gone.txt"))
	if err != nil {
		t.Fatalf("read recovered file: %v (the server version was not downloaded)", err)
	}
	if string(got) != string(body) {
		t.Errorf("recovered content = %q, want %q", got, body)
	}
	b, err := st.GetBaseline(7, "gone.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	wantHash := md4.ContentHash(body)
	if b.State != store.StateSynced || b.FileID != "f2" || b.LocalHash != wantHash || b.ServerHash != wantHash {
		t.Errorf("baseline = %+v, want synced f2 at hash %s", b, wantHash)
	}
	// Both the stale delete and its recovery download left the queue, with no durable drop trace
	// (a race resolution, not a failure).
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter trace written for a race resolution: %d", dl)
	}
}

// TestOutboundRemoveOfForeignNodeAtPathDropsStaleDelete covers the second shape the
// identity precondition guards: another client deleted our node and created its OWN node at
// the same path (delete+recreate). The queued
// path-addressed delete would remove that stranger's node - content this user never touched and
// never saw. The probe reads a different file id at the path, drops the stale delete (our node is
// already gone server-side) and clears the baseline, without ever calling Remove.
func TestOutboundRemoveOfForeignNodeAtPathDropsStaleDelete(t *testing.T) {
	c := &pushClient{
		// A DIFFERENT node now occupies the path.
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "foreign", Hash: "hz", Size: 9, MTime: 777},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - a foreign node must never be deleted", c.removes)
	}
	// Our node's tracking is dropped (it is gone server-side) and the op is acked idempotently.
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the resolved stale delete")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter trace written for an idempotent resolution: %d", dl)
	}
}

// TestOutboundRemoveForeignOccupantKeepsMovedNodeQueued closes the gap the foreign-occupant
// outcome left open: "a different node holds op.Path" proves the PATH is no longer ours, never
// that OUR node is gone - and the ack + baseline drop asserts the latter. The rotate pattern
// (mv notes.txt notes.old.txt; touch notes.txt) produces exactly this shape with our node alive
// under a new leaf name in the SAME directory.
//
// Before the fix the op was acked and the baseline dropped: the user's delete was silently lost
// AND the surviving server node fell out of the replica entirely (the later rename event finds
// nodeTracked false and dead-letters). The id-addressed second probe now recognises the node as
// MOVED, so the op stays queued - a bounded wait for the rename event, after which the PathByID
// cross-check re-emits the delete on the node's current path.
func TestOutboundRemoveForeignOccupantKeepsMovedNodeQueued(t *testing.T) {
	c := &pushClient{
		// A stranger's node took the path...
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "foreign", Hash: "hz", Size: 9, MTime: 777},
		},
		// ...while OUR node (f2) is still alive in the same directory under a new leaf name.
		listResult: map[string][]actions.Node{
			viewRootPath: {
				{FileID: "foreign", Name: "gone.txt"},
				{FileID: "f2", Name: "gone.old.txt"},
			},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue: want a transient error keeping the delete queued (it was acked before the fix)")
	}
	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - a foreign node must never be deleted", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("pending ops = %d, want the delete still queued for the rename event", n)
	}
	if _, berr := st.GetBaseline(7, "gone.txt"); berr != nil {
		t.Errorf("baseline dropped for a node that is still alive server-side: %v", berr)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter trace written for a transient wait: %d", dl)
	}
}

// TestOutboundRemoveUnchangedNodeStillDeletes is the regression pin for the added probe: when the
// server still holds OUR node at OUR last-synced hash, the delete is a genuine local delete and
// must reach the server exactly as before the fix.
func TestOutboundRemoveUnchangedNodeStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "f2", Hash: "h1", Size: 4, MTime: 111},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the remove")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundRemoveForeignOccupantReEmitsForRelocatedNode pins the identity cross-check on the
// foreign-occupant branch: when the store already knows where our node went (an inbound rename of
// an ancestor relocated it), the delete belongs on THAT path - it is re-emitted through the
// existing removeNodeMovedError resolution rather than acked away, which would silently lose the
// user's deletion and strand the relocated baseline StatePending forever.
func TestOutboundRemoveForeignOccupantReEmitsForRelocatedNode(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "foreign", Hash: "hz", Size: 9, MTime: 777},
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// byid points f2 at moved/gone.txt (the inbound rename already relocated it), while the stale
	// op still addresses the old top-level path.
	if err := st.PutBaseline(7, "moved/gone.txt", store.Baseline{
		FileID: "f2", FileType: "file", LocalHash: "h1local", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - the foreign occupant must not be deleted", c.removes)
	}
	b, err := st.GetBaseline(7, "moved/gone.txt")
	if err != nil {
		t.Fatalf("GetBaseline moved/gone.txt: %v (the relocated record must survive)", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("relocated baseline state = %q, want StateSynced so the next scan re-emits the delete", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundRenameSourceRenamedInPlaceResolves is renamed-in-place shape (a): the user moved
// a/x to b/y locally while another client renamed the SAME node in place server-side (a/x ->
// a/z). pc.Rename fails ErrPathError (a/x resolves to nothing), the destination postcondition
// probe finds nothing, and both parents read alive - so the only remaining evidence is the source
// parent's listing, which shows the node under the leaf "z".
//
// Before the fix that id-match reported "present" and the op stayed transient FOREVER: every
// drain re-ran the same probe for the same answer, and the authoritative inbound OpRename
// (a/x -> a/z) serialized behind it on the shared source key never ran. The listing now
// classifies "same id, different leaf" as MOVED - equally unaddressable from op.Path - so the
// stale move resolves as a race in ONE drain: the destination's pending baseline is dropped (no
// durable trace), the moved file stays untouched on disk, the inbound rename drains and the next
// scan re-uploads the still-present file as a fresh create.
func TestOutboundRenameSourceRenamedInPlaceResolves(t *testing.T) {
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr:          actions.ErrPathError,
		queryNodeErrByPath: map[string]error{"b/y": actions.ErrPathError},
		listResult: map[string][]actions.Node{
			// Both parents alive; the source parent still holds OUR node (fx) - under its new
			// leaf name "z", which is what the pre-fix id-match read as "present".
			"":  {{Name: "a", FileID: "fa"}, {Name: "b", FileID: "fb"}},
			"a": {{Name: "z", FileID: "fx"}},
		},
		uploadNode: map[string]actions.Node{"y": {FileID: "fnew", Hash: "h-y", MTime: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	for _, d := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(viewDir, d), 0o755); err != nil {
			t.Fatalf("seed dir %s: %v", d, err)
		}
	}
	// The local move already happened on disk: b/y holds the file, a/x is gone.
	if err := os.WriteFile(filepath.Join(viewDir, "b", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b", store.Baseline{FileID: "fb", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed b baseline: %v", err)
	}
	// localReconcile already relocated the moved node's baseline to the destination.
	if err := st.PutBaseline(7, "b/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed b/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "b/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// The server-side in-place rename reconciles into an inbound OpRename serialized BEHIND the
	// outbound one (shared source key a/x); relocatePending no-ops (our record sits at b/y).
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a/x", Path: "a/z"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to leave the queue in one drain; permanent wedge before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (the rename retried forever before the fix)", n)
	}
	// The stale outbound rename resolved without a trace; the inbound rename found neither a
	// tracked node nor an on-disk source and dropped as a cold-start gap (one durable trace).
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the gap-dropped inbound rename", dl)
	}
	// Never-lose: the moved file is untouched on disk, merely untracked.
	got, err := os.ReadFile(filepath.Join(viewDir, "b", "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("moved file at b/y = %q (err %v), want %q", got, err, moved)
	}
	if b, gerr := st.GetBaseline(7, "b/y"); !isNotFound(gerr) {
		t.Fatalf("b/y baseline not dropped: %+v (err %v)", b, gerr)
	}

	// Functional convergence: the next scan re-uploads the surviving file fresh under b/.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (re-emitted create): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push (nothing converged before the fix)", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fb" || u.name != "y" || string(u.content) != string(moved) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fb/y with the moved content", u.parentID, u.name, u.content)
	}
}

// seedVanishedUploadRace stages renamed-in-place shape (b): a tracked, modified file whose local
// source the user deleted in the scan->apply window, so the queued OpUpload hits
// errUploadSourceGone and routes into handleVanishedUpload's server probe.
func seedVanishedUploadRace(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// markLocalPending's shape: the local edit's facts plus the preserved last-synced ServerHash.
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "edit-hash", LocalSize: 7, ServerHash: "h1",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
		LocalHash: "edit-hash", LocalSize: 7,
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
}

// TestVanishedUploadServerRenamedAwayResolves is renamed-in-place shape (b): the upload's
// source vanished locally (a genuine user delete) while another client renamed the node in place
// server-side (a/x -> a/y). QueryNode(a/x) comes back ErrPathError, and the parent listing shows
// the node alive under the leaf "y".
//
// Before the fix confirmAbsentByParentList read that as "present", so the probe reported an
// inconclusive error and handleVanishedUpload requeued the op - forever, since the answer could
// never change, holding the authoritative inbound OpRename behind it on the shared path key.
// There is genuinely nothing to recover AT op.Path (the node left it) and no local content to
// lose, so the moved classification resolves the vanish as a local delete: the pending baseline
// is reverted for re-emission and the op leaves the queue in one drain.
func TestVanishedUploadServerRenamedAwayResolves(t *testing.T) {
	c := &pushClient{
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError},
		listResult: map[string][]actions.Node{
			"a": {{Name: "y", FileID: "fx"}},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir a: %v", err)
	}
	seedVanishedUploadRace(t, st)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want the vanished upload to resolve; endless requeue before the fix)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (the probe requeued forever before the fix)", n)
	}
	// No keep-both funnel: there is no server version sitting at op.Path to recover, and no
	// local content left to preserve.
	if conflicts, _ := st.ListConflicts(7); len(conflicts) != 0 {
		t.Errorf("spurious conflicts recorded: %+v", conflicts)
	}
	// The baseline is reverted to StateSynced with the vanished edit's local facts cleared, so
	// the next scan re-emits the deletion (resolveVanishedUpload's modify shape).
	b, err := st.GetBaseline(7, "a/x")
	if err != nil {
		t.Fatalf("GetBaseline a/x: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != "" || b.LocalSize != 0 {
		t.Errorf("baseline = %+v, want StateSynced with cleared local facts", b)
	}
}

// TestVanishedUploadInconclusiveProbeStaysQueued is the conservative pin for shape (b): a node
// still occupying the LEAF (the ErrPathError was a probe timeout, not a rename) is not a
// positive outcome, so the op stays queued with its pending baseline intact rather than being
// resolved away as a local delete.
func TestVanishedUploadInconclusiveProbeStaysQueued(t *testing.T) {
	c := &pushClient{
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError},
		listResult: map[string][]actions.Node{
			"a": {{Name: "x", FileID: "fx"}},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir a: %v", err)
	}
	seedVanishedUploadRace(t, st)

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want a transient error keeping the op queued, got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("queued ops = %d, want 1", n)
	}
	b, err := st.GetBaseline(7, "a/x")
	if err != nil || b.State != store.StatePending || b.LocalHash != "edit-hash" {
		t.Fatalf("baseline = %+v (err %v), want the intact StatePending record", b, err)
	}
}

// TestOutboundRemoveTargetRenamedAwayReEmitsOnNewPath covers the third consumer of the same
// "present" probe: a local delete of a/x racing a server-side in-place
// rename to a/y. The path-addressed Remove fails ErrPathError and the postcondition listing
// shows the node alive under the new leaf - read as "present" before the fix, so the op retried
// forever behind an unchanging answer.
//
// A moved node is NOT an absence: it must never reach the idempotent ack + baseline drop (that
// would lose the user's deletion and strand the node server-side). It only unlocks the identity
// cross-check, which - once reconcile has folded the rename event and byid names the new path -
// re-emits the delete there through the existing removeNodeMovedError resolution.
func TestOutboundRemoveTargetRenamedAwayReEmitsOnNewPath(t *testing.T) {
	c := &pushClient{
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError},
		removeErr:          actions.ErrPathError,
		listResult: map[string][]actions.Node{
			"a": {{Name: "y", FileID: "fx"}},
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// The inbound rename event was already reconciled: relocatePending re-homed the record to
	// a/y, so byid names the node's current path while the queued delete still addresses a/x.
	if err := st.PutBaseline(7, "a/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "h", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want the stale delete re-emitted; permanent wedge before the fix)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (the remove retried forever before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("race resolution wrote a durable dead-letter trace: %d, want 0", dl)
	}
	// The relocated record survives, reverted to StateSynced so the next scan re-emits the
	// delete against the node's current path.
	b, err := st.GetBaseline(7, "a/y")
	if err != nil {
		t.Fatalf("GetBaseline a/y: %v (the relocated record must survive)", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("relocated baseline state = %q, want StateSynced so the next scan re-emits the delete", b.State)
	}
}

// TestOutboundRemoveTargetRenamedAwaySurvivesTheCarryingRename pins the interaction the
// re-emission actually runs inside. The reconcile pass that relocated the record to a/y also
// enqueued the inbound OpRename(a/x -> a/y) that carries the server's move, and that rename
// conflicts with the remove on the shared a/x key - so it is serialized behind it and runs in
// the SAME drain batch. Its source is gone from disk (the user's delete is what started this)
// and its destination never materialised, so it lands in dropColdStartGapRename, whose
// destination-baseline cleanup used to delete the very StateSynced marker revertRelocatedRemove
// had just planted: the user's deletion was silently discarded AND the live server node fell out
// of the replica entirely (no baseline, no file), recoverable only by a cold start.
func TestOutboundRemoveTargetRenamedAwaySurvivesTheCarryingRename(t *testing.T) {
	c := &pushClient{
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError},
		removeErr:          actions.ErrPathError,
		listResult: map[string][]actions.Node{
			"a": {{Name: "y", FileID: "fx"}},
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "a/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "h", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	// The inbound rename reconcile enqueued alongside the relocation, serialized behind the
	// remove on the shared a/x key.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		Path: "a/x", NewPath: "a/y", FileID: "fx",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	b, err := st.GetBaseline(7, "a/y")
	if err != nil {
		t.Fatalf("GetBaseline a/y: %v - the carrying rename deleted the re-emission marker, "+
			"losing the local delete and dropping the live server node out of the replica", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("relocated baseline state = %q, want StateSynced so the next scan re-emits the delete", b.State)
	}
	if b.FileID != "fx" {
		t.Errorf("relocated baseline file id = %q, want fx", b.FileID)
	}
}

// TestOutboundRemoveTargetRenamedAwayNotYetIndexedStaysQueued pins the guarded half: while the
// store still indexes the node at the op's own path (the inbound rename event has not been
// reconciled yet), the server-confirmed rename must NOT ack the delete away - that would drop
// the baseline and lose the deletion while the node lives on server-side. The op stays queued
// for the bounded wait until the event arrives.
func TestOutboundRemoveTargetRenamedAwayNotYetIndexedStaysQueued(t *testing.T) {
	c := &pushClient{
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError},
		removeErr:          actions.ErrPathError,
		listResult: map[string][]actions.Node{
			"a": {{Name: "y", FileID: "fx"}},
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// planRemove's shape: the record still sits at the op's own path (byid points there too).
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "h", LocalSize: 4, ServerHash: "h1",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue: want a transient error keeping the op queued, got nil")
	}
	if !errors.Is(err, actions.ErrPathError) {
		t.Errorf("drainQueue err = %v, want it to wrap actions.ErrPathError", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("queued ops = %d, want 1", n)
	}
	if _, gerr := st.GetBaseline(7, "a/x"); gerr != nil {
		t.Fatalf("baseline dropped on a moved (not absent) node: %v - the delete would be lost", gerr)
	}
}

// seedRemoveDivergenceRaceBase is seedRemoveDivergenceRace with the gone node's synced server
// and local hashes chosen by the caller, so a test can drive the base-selection rules directly.
func seedRemoveDivergenceRaceBase(t *testing.T, e *Engine, st *store.Store, viewDir, fileID, serverHash, localHash string) {
	t.Helper()
	keep := []byte("keep")
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keep, 0o644); err != nil {
		t.Fatalf("seed keep file: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileID: "fk", FileType: "file", LocalHash: md4.ContentHash(keep), LocalSize: int64(len(keep)),
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keep baseline: %v", err)
	}
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{
		FileID: fileID, FileType: "file", LocalHash: localHash, LocalSize: 4, ServerHash: serverHash,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed gone baseline: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "gone.txt" {
		t.Fatalf("scan ops = %+v, want a single OpRemove(gone.txt)", ops)
	}
}

// TestOutboundRemoveIDLessSkipsProbe pins the documented short-circuit: an OpRemove carrying no
// file id has nothing to compare identity against, so it runs no QueryNode at all and deletes
// exactly as it would without the identity precondition. Without the short-circuit every id-less
// delete would pay an extra server turn and decide from a reply it cannot interpret.
func TestOutboundRemoveIDLessSkipsProbe(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRaceBase(t, e, st, viewDir, "", "h1", "h1local")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	for _, p := range c.queryNodeCalls {
		if p == "gone.txt" {
			t.Fatalf("an id-less remove probed the path: queryNodeCalls = %v", c.queryNodeCalls)
		}
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
}

// TestOutboundRemoveHashLessServerReplyStillDeletes pins the "with either hash missing, node
// identity decides" rule. A directory (and any DSM that omits the hash) reports no content hash,
// so the comparison cannot run; the same file id at the path is the unchanged node and the delete
// must proceed. Reading a missing hash as divergence would stall every such delete behind a
// pointless download recovery.
func TestOutboundRemoveHashLessServerReplyStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"gone.txt": {FileID: "f2", Size: 4, MTime: 111}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRaceBase(t, e, st, viewDir, "f2", "h1", "h1local")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
}

// TestOutboundRemoveFallsBackToLocalHashBase pins removeTargetFacts's ServerHash->LocalHash
// fallback. A DSM that omits the hash from its upload reply leaves ServerHash empty on every
// locally created or modified file (applyUploadOut assigns node.Hash verbatim) while markSynced
// always derives a LocalHash - so without the fallback the identity precondition would be inert
// for the majority of a push-heavy view and a queued delete would destroy a concurrent server
// edit again.
func TestOutboundRemoveFallsBackToLocalHashBase(t *testing.T) {
	body := []byte("another client edit")
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "f2", Hash: "h2", Size: int64(len(body)), MTime: 555},
		},
		downloadContent: map[string][]byte{"f2": body},
		downloadNode: map[string]actions.Node{
			"f2": {FileID: "f2", Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: 555},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// No ServerHash recorded; the local hash of the last agreed content is the only base.
	seedRemoveDivergenceRaceBase(t, e, st, viewDir, "f2", "", "h1local")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - the server modify must not be deleted", c.removes)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "gone.txt"))
	if err != nil {
		t.Fatalf("read recovered file: %v (the server version was not downloaded)", err)
	}
	if string(got) != string(body) {
		t.Errorf("recovered content = %q, want %q", got, body)
	}
}

// TestOutboundRemoveLocalHashBaseMatchStillDeletes is the companion regression pin: with the
// fallback in force, a server whose content still matches the recorded local hash is the
// unchanged node, so a genuine local delete still reaches the server.
func TestOutboundRemoveLocalHashBaseMatchStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"gone.txt": {FileID: "f2", Hash: "h1local", Size: 4}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRaceBase(t, e, st, viewDir, "f2", "", "h1local")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
}
