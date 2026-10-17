package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// This file is the Windows arm of the containment guards that no test had ever executed:
// every symlink fixture in this package needs a privilege a stock Windows
// session does not hold, so abs/noSymlinkEscape's Windows behaviour was covered by nothing at
// all - on any developer host or CI runner.
//
// Running it turned up the gap the round predicted. The guards test fi.Mode()&os.ModeSymlink,
// but the redirection an unprivileged Windows user can actually create is a JUNCTION, and Go
// reports one as ModeIrregular, never ModeSymlink (pinned by makeJunction on the live
// filesystem). So a junction planted inside the sync root by any local process passed every
// containment gate. What each arc then did was MEASURED on this host by running these fixtures
// against the pre-fix classifier, not assumed:
//
//   - mkdir: a real containment ESCAPE. os.MkdirAll created the server's directory OUTSIDE the
//     sync root, through the junction, and the op acked clean - no error, no dead letter, and
//     the whole inbound subtree beneath it would follow.
//   - download and remove: no escape, but no apply either. Both route their divergence sweep
//     through openUploadSource, whose os.Root refuses the traversal ("path escapes from parent")
//     - a hard error the executor classifies transient, so the op re-failed every drain forever
//     and every same-path op serialised behind it. Fail-safe on data, fatal to liveness.
//
// pathRedirects (reparse.go) closes both by classifying name-surrogate reparse points alongside
// symlinks: the op now dead-letters with an observable trace at the containment gate, before any
// apply is attempted.

// junctionEngine seeds a store, a sync root with a view directory, and a junction inside that
// view directory pointing at a tree OUTSIDE the sync root. It returns the engine, the store, the
// outside directory (which every test asserts stays untouched) and the view directory.
func junctionEngine(t *testing.T, dl *dlClient) (*Engine, *store.Store, string, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	makeJunction(t, filepath.Join(viewDir, "link"), outside)
	return New(st, &concTransport{client: dl}, root, "sess", nil), st, outside, viewDir
}

// assertRejected confirms the drain dead-lettered the op instead of wedging it: the queue is
// empty and exactly one dead-letter trace was recorded, so the refusal is observable to the
// operator rather than a silent drop.
func assertRejected(t *testing.T, e *Engine, st *store.Store) {
	t.Helper()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a junction path, not wedge: %v", err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("rejected op left queued (%d): it would re-fail every drain forever", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1 (the refusal must leave an observable trace)", len(dls))
	}
}

// TestInboundDownloadThroughJunctionRefused is the write arc. Pre-fix it did not escape - the
// os.Root in openUploadSource refused the traversal - but it did not dead-letter either: the
// refusal surfaced as a plain error the executor reads as transient, so the op re-failed every
// drain forever. The assertion is both halves at once: nothing outside the root, and the op
// dropped with a trace.
func TestInboundDownloadThroughJunctionRefused(t *testing.T) {
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e, st, outside, _ := junctionEngine(t, dl)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "link/escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertRejected(t, e, st)
	if _, err := os.Stat(filepath.Join(outside, "escape.txt")); !os.IsNotExist(err) {
		t.Errorf("download escaped through a junction: err=%v", err)
	}
}

// TestInboundMkdirThroughJunctionRefused is the arc that genuinely escaped: measured pre-fix,
// the inbound mkdir created the server's directory OUTSIDE the sync root through the junction
// and acked the op clean, so every child of that server directory would have followed it there.
func TestInboundMkdirThroughJunctionRefused(t *testing.T) {
	e, st, outside, _ := junctionEngine(t, &dlClient{})
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "link/graft", FileID: "d1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertRejected(t, e, st)
	if _, err := os.Stat(filepath.Join(outside, "graft")); !os.IsNotExist(err) {
		t.Errorf("mkdir grafted a server subtree outside the sync root through a junction: err=%v", err)
	}
}

// TestInboundRemoveThroughJunctionRefused is the destructive arc: a server-side delete replayed
// through a junction would be an os.RemoveAll of a path outside the sync root - user data the
// client holds no authority over whatsoever. Pre-fix the os.Root guard stopped the deletion and
// wedged the op instead (same eternal-transient shape as the download); the gate now refuses it
// at the containment check, with the victim provably intact either way.
func TestInboundRemoveThroughJunctionRefused(t *testing.T) {
	e, st, outside, _ := junctionEngine(t, &dlClient{})
	victim := filepath.Join(outside, "victim.txt")
	if err := os.WriteFile(victim, []byte("the user's own file"), 0o644); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	if err := st.PutBaseline(7, "link/victim.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "link/victim.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertRejected(t, e, st)
	if b, err := os.ReadFile(victim); err != nil || string(b) != "the user's own file" {
		t.Errorf("remove deleted a file outside the sync root through a junction (content %q, err %v)", b, err)
	}
}

// TestScanSkipsJunctionWithoutPushingADelete covers the outbound half. Unlike the apply arcs
// this one was already GREEN pre-fix and is a regression guard, not a red test: a junction is
// neither a directory (name surrogates carry no ModeDir) nor regular, so it fell through to the
// walk's default arm, which protects a tracked path exactly like the symlink arm does. The
// protection is what keeps the omission from becoming a remote deletion - an omitted path that
// still carries a baseline is read by the merge-join as "the file vanished" - so the behaviour
// is worth pinning now that the classification moved: the junction must still be protected, must
// still not surface content from the far side, and now reports as "junction" rather than an
// opaque mode string.
func TestScanSkipsJunctionWithoutPushingADelete(t *testing.T) {
	e, st, outside, _ := junctionEngine(t, &dlClient{})
	if err := os.WriteFile(filepath.Join(outside, "outside.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed outside file: %v", err)
	}
	// The tracked slot the junction shadows: a server directory this client had synced, which
	// the user then replaced locally with a junction. Only a TRACKED path can be diffed as
	// vanished, so this is the state in which an unprotected omission turns into a remote
	// delete - an untracked junction is ignored either way.
	if err := st.PutBaseline(7, "link", store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	entries, unreadable, err := e.scanView(context.Background(), 7, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	for _, ent := range entries {
		if ent.relPath == "link" || filepath.ToSlash(ent.relPath) == "link/outside.txt" {
			t.Errorf("scan surfaced content through a junction: %+v", ent)
		}
	}
	if !unreadable["link"] {
		t.Errorf("junction not recorded as protected: unreadable=%v - an unprotected omission "+
			"lets the merge-join read it as a deletion and push a remote remove", unreadable)
	}
}
