package engine

// These tests cover an outbound upload/mkdir/rename whose parent baseline was
// RELOCATED by a server-side rename of an ancestor (relocatePending re-homes the subtree's
// baselines at reconcile time) died on errParentUnknown BEFORE any server turn - all of the
// parent-gone terminal exits hang off the server turn's ErrPathError - while the authoritative
// inbound OpRename that would resolve the situation was serialized BEHIND the wedged op on
// the ancestor conflict key: a permanent cyclic wait in both directions (scenario A), and the
// dead-letter cleanup keyed on the op's stale path silently no-op'ed on the relocated
// baseline, stranding it StatePending forever (scenario C).

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundUploadUnderRenamedParentConverges reproduces scenario A and pins the fix: a
// local edit inside a/ produced a queued OpUpload(a/x) (a modify, carrying the node's id);
// before it drained, another client renamed a -> c server-side, so reconcile eagerly
// relocated the baselines (a -> c, a/x -> c/x) and queued the inbound OpRename(a->c) BEHIND
// the upload. The upload's resolveParentID("a/x") then found no parent baseline
// (errParentUnknown) and its only pre-turn exit - the source-gone probe - read the
// still-on-disk file as "keep waiting": a transient failure every drain, with the rename
// skipping behind the bad op forever. With the fix the upload resolves the relocation via
// the byid index (a race resolution: baseline reverted for re-emission, acked, NO durable
// trace), the rename applies in the same drain, and the next scan re-pushes the edit at c/x.
func TestOutboundUploadUnderRenamedParentConverges(t *testing.T) {
	edited := []byte("locally-edited-content-not-yet-on-server")

	c := &pushClient{
		uploadNode: map[string]actions.Node{"x": {FileID: "fx", Hash: "h-serv", MTime: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	// a is a synced directory; a/x is a modify pending its upload (the scan already ran).
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(edited),
		LocalSize: int64(len(edited)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	// The outbound upload is enqueued FIRST, so it drains ahead of the inbound rename.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
		LocalHash: md4.ContentHash(edited), LocalSize: int64(len(edited)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	// The server rename a -> c reconciles into a serialized inbound OpRename(a->c), eagerly
	// relocating the stored baselines a -> c and a/x -> c/x (relocatePending).
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "c"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge in one drain; cyclic wedge before the fix)", err)
	}

	// Both ops left the queue: the upload resolved as relocated, the rename applied.
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (cyclic wedge before the fix)", n)
	}
	// A race resolution, not a drop: no durable dead-letter trace.
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("race resolution wrote a durable dead-letter trace: %d, want 0", dl)
	}
	// The inbound rename applied: the directory (and the edit) moved to c/.
	got, err := os.ReadFile(filepath.Join(viewDir, "c", "x"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("moved edit at c/x = %q (err %v), want %q", got, err, edited)
	}
	// The relocated baseline was reverted for re-emission: synced with cleared local facts.
	b, err := st.GetBaseline(7, "c/x")
	if err != nil {
		t.Fatalf("GetBaseline c/x: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != "" || b.LocalSize != 0 {
		t.Fatalf("relocated baseline = %+v, want StateSynced with cleared local facts", b)
	}

	// The next scan re-diffs the edit at the new path and pushes it under the live parent.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (re-emitted upload): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fa" || u.name != "x" || string(u.content) != string(edited) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fa/x with the edit", u.parentID, u.name, u.content)
	}
	b, err = st.GetBaseline(7, "c/x")
	if err != nil || b.State != store.StateSynced || b.ServerHash != "h-serv" {
		t.Errorf("final c/x baseline = %+v (err %v), want synced with server facts", b, err)
	}
	// The edit converged without any keep-both fallout.
	if conflicts, _ := st.ListConflicts(7); len(conflicts) != 0 {
		t.Errorf("spurious conflicts recorded: %+v", conflicts)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained after re-emission: %d", n)
	}
}

// TestOutboundCreateUploadUnderRenamedParentReemits is the id-less variant of scenario A: a
// FRESH local create under a/ (no file id, so no byid entry to resolve the relocation
// through) whose parent the server renamed before the upload drained. The pre-turn ancestor
// walk reads the renamed parent as gone (leaf-blind, accepted as data-safe), the op
// dead-letters, and the relocation-aware cleanup follows the QUEUED inbound rename to delete
// the re-based pending baseline (a path-keyed delete alone would strand it) - so after the
// rename applies, the next scan re-detects the file as untracked and re-uploads it fresh
// under the live parent.
func TestOutboundCreateUploadUnderRenamedParentReemits(t *testing.T) {
	fresh := []byte("fresh-create-content-the-server-never-saw")

	c := &pushClient{
		// The ancestor walk lists the view root: it shows the parent only under its NEW name,
		// so "a" reads gone - and the OLD path resolves to nothing, so the corroborating probe
		// agrees (listing evidence is hardened, but it cannot see through a rename).
		listResult:         map[string][]actions.Node{"": {{Name: "c", FileID: "fa"}}},
		queryNodeErrByPath: map[string]error{"a": actions.ErrPathError},
		uploadNode:         map[string]actions.Node{"nf.txt": {FileID: "fnew", Hash: "h-new"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "nf.txt"), fresh, 0o644); err != nil {
		t.Fatalf("seed fresh file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// planUpload's pending baseline for the fresh create: no file id yet.
	if err := st.PutBaseline(7, "a/nf.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash(fresh),
		LocalSize: int64(len(fresh)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/nf.txt baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/nf.txt",
		LocalHash: md4.ContentHash(fresh), LocalSize: int64(len(fresh)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "c"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes; cyclic wedge before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (cyclic wedge before the fix)", n)
	}
	// The id-less create was dropped through the errParentGone path: one durable trace.
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the dropped create", dl)
	}
	// The relocation-aware cleanup followed the queued inbound rename: no stranded pending
	// baseline at the re-based path.
	if b, gerr := st.GetBaseline(7, "c/nf.txt"); !isNotFound(gerr) {
		t.Fatalf("re-based pending baseline not cleaned: %+v (err %v)", b, gerr)
	}
	// The inbound rename applied: the file rode along to c/.
	got, err := os.ReadFile(filepath.Join(viewDir, "c", "nf.txt"))
	if err != nil || string(got) != string(fresh) {
		t.Fatalf("moved fresh file at c/nf.txt = %q (err %v), want %q", got, err, fresh)
	}

	// The next scan re-detects the untracked file and uploads it under the live parent.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue (re-emitted create): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fa" || u.name != "nf.txt" || string(u.content) != string(fresh) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fa/nf.txt with the fresh content", u.parentID, u.name, u.content)
	}
	b, err := st.GetBaseline(7, "c/nf.txt")
	if err != nil || b.State != store.StateSynced || b.FileID != "fnew" {
		t.Errorf("final c/nf.txt baseline = %+v (err %v), want synced with the assigned id", b, err)
	}
}

// TestOutboundRenameGoneParentCleanupResolvesRelocatedBaseline reproduces scenario C: an
// intra-directory local move a/x -> a/y (baseline relocated to a/y pending) raced a server
// rename of the parent a -> c, which reconcile re-based to c/y before the outbound rename
// drained. The rename's server turn fails ErrPathError and the leaf-blind ancestor walk
// reads the renamed parent as gone (accepted), dead-lettering the op - but the pre-fix
// cleanup deleted only at op.NewPath ("a/y"), a silent no-op on the relocated record, so
// baseline c/y sat StatePending with no queued op, inert forever (the scan only re-drives
// StateSynced records). The relocation-aware cleanup now reverts the record at its ACTUAL
// tracked path for re-emission, so the next scan pushes the surviving file at c/y.
func TestOutboundRenameGoneParentCleanupResolvesRelocatedBaseline(t *testing.T) {
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr: actions.ErrPathError,
		// The old parent path resolves to nothing after the rename, so the corroborating probe
		// agrees with the listing.
		queryNodeErrByPath: map[string]error{"a/y": actions.ErrPathError, "a": actions.ErrPathError},
		// The ancestor walk lists the view root: the parent shows only under its NEW name,
		// so "a" reads gone (leaf-blind, accepted as data-safe).
		listResult: map[string][]actions.Node{"": {{Name: "c", FileID: "fa"}}},
		uploadNode: map[string]actions.Node{"y": {FileID: "fy", Hash: "h-y"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// The local move already happened on disk: a/y holds the file, a/x is gone.
	if err := os.WriteFile(filepath.Join(viewDir, "a", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// localReconcile already relocated the baseline to the destination (StatePending).
	if err := st.PutBaseline(7, "a/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "a/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// The server rename a -> c re-bases the pending baseline a/y -> c/y and queues the
	// inbound OpRename(a->c) behind the outbound rename.
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "c"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	// The doomed outbound rename dead-lettered (errParentGone keeps its durable trace).
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the dropped rename", dl)
	}
	// The inbound rename applied: the file lives at c/y on disk.
	got, err := os.ReadFile(filepath.Join(viewDir, "c", "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("moved file at c/y = %q (err %v), want %q", got, err, moved)
	}
	// No stranded StatePending: the relocated record was reverted for re-emission.
	b, err := st.GetBaseline(7, "c/y")
	if err != nil {
		t.Fatalf("GetBaseline c/y: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != "" {
		t.Fatalf("relocated baseline = %+v, want StateSynced with cleared local facts (stranded StatePending before the fix)", b)
	}

	// The next scan re-diffs c/y and pushes the surviving content under the live parent.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue (re-emitted push): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push (nothing re-emitted before the fix)", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fa" || u.name != "y" || string(u.content) != string(moved) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fa/y with the moved content", u.parentID, u.name, u.content)
	}
	if b, err := st.GetBaseline(7, "c/y"); err != nil || b.State != store.StateSynced {
		t.Errorf("final c/y baseline = %+v (err %v), want synced", b, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained after re-emission: %d", n)
	}
}
