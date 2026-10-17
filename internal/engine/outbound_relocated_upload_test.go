package engine

// These tests cover an outbound modify-upload whose OWN node was relocated by a
// server-side rename before the op drained. The only pre-existing relocation check in
// applyUploadOut hung off the errParentBaselineAbsent branch, which a
// SAME-DIRECTORY rename (a/x -> a/y) never reaches - the parent baseline stays perfectly
// alive, so resolveParentID succeeds and the upload was sent against the FREED old path. The
// server then minted a second node there carrying the edit, and the trailing inbound rename
// plus the next local scan deleted it again: the edit survived on neither replica while the
// baseline claimed synced (a silent permanent divergence). The fix is an unconditional
// pre-turn byid redirect, the outbound mirror of the inbound byid redirect.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedRelocatedUploadRace builds the shared interleaving prefix: a synced directory a/ holding
// the synced node fx at a/x, whose local content was edited but not yet pushed (localReconcile
// queues the modify), after which the server renames the node within the same directory
// (a/x -> a/y) so reconcile relocates its baseline and queues the inbound OpRename BEHIND the
// upload on the shared source key. It returns the engine, the store and the view directory.
func seedRelocatedUploadRace(t *testing.T, c *pushClient, edited []byte) (*Engine, *store.Store, string) {
	t.Helper()
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{
		FileID: "fa", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// The last synced state describes the SERVER's content; the on-disk bytes are the edit.
	old := []byte("server-content-before-the-edit")
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", ServerHash: md4.ContentHash(old),
		LocalHash: md4.ContentHash(old), LocalSize: int64(len(old)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}

	// The local scan diffs the edit into an outbound OpUpload(a/x) carrying the node's id.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "a/x" || ops[0].FileID != "fx" {
		t.Fatalf("queued ops = %+v, want one OpUpload(a/x, fx)", ops)
	}

	// The drain fails transiently (or simply has not run yet) and the poll brings the server's
	// same-directory rename of the very node the upload addresses: relocatePending re-homes the
	// baseline a/x -> a/y and the inbound OpRename queues behind the upload.
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a/x", Path: "a/y"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cur, err := st.PathByID(7, "fx"); err != nil || cur != "a/y" {
		t.Fatalf("byid[fx] = %q (err %v), want the relocated path a/y", cur, err)
	}

	return e, st, viewDir
}

// TestOutboundUploadOfRelocatedNodeReEmitsAtNewPath pins the full relocated-upload scenario: the
// stale upload must NOT be sent against the freed old path. Before the fix it was, the server
// minted a second node (fx2) there, and the next scan diffed the synced-but-fileless old path
// into an outbound OpRemove that deleted that second node - the only server copy of the edit.
func TestOutboundUploadOfRelocatedNodeReEmitsAtNewPath(t *testing.T) {
	edited := []byte("locally-edited-content-not-yet-on-server")
	c := &pushClient{uploadNode: map[string]actions.Node{
		// Pre-fix the stale push lands here and the server mints a SECOND node.
		"x": {FileID: "fx2", Hash: "h-second-node", MTime: 9},
		// The re-emitted push at the node's current path updates the node itself.
		"y": {FileID: "fx", Hash: md4.ContentHash(edited), MTime: 11},
	}}
	e, st, viewDir := seedRelocatedUploadRace(t, c, edited)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The stale upload was resolved as a relocation race - never sent, no durable trace.
	if len(c.uploads) != 0 {
		t.Fatalf("uploads during the racing drain = %+v, want none (a second server node before the fix)", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("race resolution wrote a durable dead-letter trace: %d, want 0", dl)
	}
	// The inbound rename applied: the edited file now sits at the node's new path.
	got, err := os.ReadFile(filepath.Join(viewDir, "a", "y"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("moved edit at a/y = %q (err %v), want %q", got, err, edited)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a", "x")); !os.IsNotExist(err) {
		t.Errorf("old path a/x still on disk (err %v), want it moved away", err)
	}
	// No phantom synced record at the freed old path (the producer of the fatal OpRemove).
	if _, err := st.GetBaseline(7, "a/x"); !isNotFound(err) {
		t.Errorf("baseline a/x = %v, want absent (a synced-but-fileless record before the fix)", err)
	}
	// The relocated record was reverted for re-emission: synced with cleared local facts.
	b, err := st.GetBaseline(7, "a/y")
	if err != nil {
		t.Fatalf("GetBaseline a/y: %v", err)
	}
	if b.FileID != "fx" || b.State != store.StateSynced || b.LocalHash != "" {
		t.Fatalf("relocated baseline = %+v, want fx StateSynced with cleared local facts", b)
	}

	// The next scan re-diffs the still-unpushed edit at the new path and pushes it there.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile (re-emission): %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (re-emitted upload): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %+v, want exactly one re-emitted push", c.uploads)
	}
	u := c.uploads[0]
	if u.parentID != "fa" || u.name != "y" || string(u.content) != string(edited) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fa/y with the edit",
			u.parentID, u.name, u.content)
	}
	// Nothing was ever deleted server-side: pre-fix the second node fx2 was removed here.
	if len(c.removes) != 0 {
		t.Fatalf("server removes = %v, want none (the edit's only server copy was deleted before the fix)", c.removes)
	}
	// The node is tracked at its current path with the pushed content.
	if cur, err := st.PathByID(7, "fx"); err != nil || cur != "a/y" {
		t.Errorf("byid[fx] = %q (err %v), want a/y", cur, err)
	}
	fb, err := st.GetBaseline(7, "a/y")
	if err != nil || fb.State != store.StateSynced || fb.LocalHash != md4.ContentHash(edited) {
		t.Errorf("baseline a/y after the push = %+v (err %v), want synced with the edited hash", fb, err)
	}
}

// TestOutboundUploadOfRelocatedNodeIDLessReplyKeepsRecord covers the sub-shape where the
// server's upload reply omits a file id: applyUploadOut falls back to op.FileID, so the stale
// push wrote a record carrying fx at the OLD path, and putBaselineTx's drop-moved-orphan
// (byid holds one path per file id) EVICTED the relocated record at the new path - the node's
// facts collapsed onto a path the rename was about to empty. The redirect must fire before any
// of that, leaving the relocated record intact.
func TestOutboundUploadOfRelocatedNodeIDLessReplyKeepsRecord(t *testing.T) {
	edited := []byte("edited-bytes-awaiting-their-push")
	// No scripted node for "x": the reply carries no file id, so the pre-fix path fell back
	// to op.FileID and evicted the relocated record.
	c := &pushClient{uploadNode: map[string]actions.Node{
		"y": {FileID: "fx", Hash: md4.ContentHash(edited), MTime: 12},
	}}
	e, st, viewDir := seedRelocatedUploadRace(t, c, edited)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 0 {
		t.Fatalf("uploads during the racing drain = %+v, want none", c.uploads)
	}
	// The relocated record survived with the node's identity (evicted before the fix).
	b, err := st.GetBaseline(7, "a/y")
	if err != nil {
		t.Fatalf("GetBaseline a/y: %v (the relocated record was evicted before the fix)", err)
	}
	if b.FileID != "fx" || b.State != store.StateSynced {
		t.Fatalf("relocated baseline = %+v, want fx StateSynced", b)
	}
	if cur, err := st.PathByID(7, "fx"); err != nil || cur != "a/y" {
		t.Fatalf("byid[fx] = %q (err %v), want a/y", cur, err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "a", "y"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("moved edit at a/y = %q (err %v), want %q", got, err, edited)
	}

	// The edit is re-emitted at the node's current path, and nothing is deleted server-side.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile (re-emission): %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (re-emitted upload): %v", err)
	}
	if len(c.uploads) != 1 || c.uploads[0].name != "y" || string(c.uploads[0].content) != string(edited) {
		t.Fatalf("uploads = %+v, want exactly one re-emitted push of the edit at y", c.uploads)
	}
	if len(c.removes) != 0 {
		t.Fatalf("server removes = %v, want none", c.removes)
	}
}

// TestOutboundUploadNotRelocatedKeepsDirectPush is the regression pin: an ordinary modify whose
// node is still tracked at the op's own path costs one extra byid read and takes the unchanged
// direct-push flow.
func TestOutboundUploadNotRelocatedKeepsDirectPush(t *testing.T) {
	edited := []byte("an-ordinary-local-edit")
	c := &pushClient{uploadNode: map[string]actions.Node{
		"x": {FileID: "fx", Hash: md4.ContentHash(edited), MTime: 7},
	}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{
		FileID: "fa", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	old := []byte("old")
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", ServerHash: md4.ContentHash(old),
		LocalHash: md4.ContentHash(old), LocalSize: int64(len(old)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %+v, want exactly one direct push", c.uploads)
	}
	u := c.uploads[0]
	if u.parentID != "fa" || u.name != "x" || string(u.content) != string(edited) {
		t.Errorf("upload = parent %q name %q content %q, want fa/x with the edit", u.parentID, u.name, u.content)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter traces = %d, want 0", dl)
	}
	b, err := st.GetBaseline(7, "a/x")
	if err != nil {
		t.Fatalf("GetBaseline a/x: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "fx" || b.LocalHash != md4.ContentHash(edited) {
		t.Fatalf("baseline = %+v, want fx synced with the edited hash", b)
	}
}
