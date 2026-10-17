package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// seedTrackedSubtree records a directory and one child under oldPath, as a rejected move would
// leave them: tracked at a path the server no longer names.
func seedTrackedSubtree(t *testing.T, st *store.Store, oldPath string) {
	t.Helper()
	if err := st.PutBaseline(7, oldPath, store.Baseline{
		FileID: "dir1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, oldPath+"/child.txt", store.Baseline{
		FileID: "child1", FileType: "file", LocalHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
}

// TestMkdirReplayAdoptsOnlyItsOwnSubtree pins the evidence the replay branch now requires.
//
// "The old path is gone and a directory stands at the destination" is the shape of a crashed
// prior apply - and equally the shape of a user who deleted the old tree and made a fresh
// directory at the same name. Adopting the second case relocates the old subtree's synced
// descendant records under a directory that does not hold those files, after which the next
// scan reads every one as vanished and pushes a delete of the server's live descendants.
func TestMkdirReplayAdoptsOnlyItsOwnSubtree(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The destination holds an UNRELATED directory: no child.txt inside it.
	if err := os.MkdirAll(filepath.Join(viewDir, "new"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "new", "something-else.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	seedTrackedSubtree(t, st, "old")

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", discardLogger())
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "new", FileID: "dir1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The child's record must NOT have been carried under the unrelated directory: doing so
	// makes the next scan delete the server's copy.
	if _, err := st.GetBaseline(7, "new/child.txt"); err == nil {
		t.Error("the old subtree's descendant record was adopted under an unrelated directory")
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "new", "something-else.txt")); err != nil {
		t.Errorf("the unrelated directory's own content was disturbed: %v", err)
	}
}

// TestMkdirReplayAdoptsAGenuinelyMovedSubtree is the other side: when the destination really
// does hold the recorded contents, the replay finishes the baseline relocation. Refusing here
// would strand every descendant record under a prefix no later op reaches.
func TestMkdirReplayAdoptsAGenuinelyMovedSubtree(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The destination holds OUR subtree: the prior apply's rename landed before it crashed.
	if err := os.MkdirAll(filepath.Join(viewDir, "new"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "new", "child.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	seedTrackedSubtree(t, st, "old")

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", discardLogger())
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "new", FileID: "dir1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := st.GetBaseline(7, "new/child.txt"); err != nil {
		t.Errorf("the moved subtree's descendant record was not carried across: %v", err)
	}
	if _, err := st.GetBaseline(7, "old/child.txt"); err == nil {
		t.Error("the descendant record was left behind at the old prefix as well")
	}
}

// TestMkdirReplayAdoptsAnEmptyRecordedSubtree pins that a directory recording no descendants is
// adopted: there is nothing that could be stranded and nothing to check against, and that is
// also what a freshly created empty directory looks like on both sides.
func TestMkdirReplayAdoptsAnEmptyRecordedSubtree(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "new"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := st.PutBaseline(7, "old", store.Baseline{
		FileID: "dir1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", discardLogger())
	adopt, err := e.destHoldsMovedSubtree(7, "old", filepath.Join(viewDir, "new"))
	if err != nil {
		t.Fatalf("destHoldsMovedSubtree: %v", err)
	}
	if !adopt {
		t.Error("a directory with no recorded descendants was refused; nothing could be stranded")
	}
}
