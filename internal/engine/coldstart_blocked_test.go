package engine

// Tests for the file↔dir collision: the server holds a DIRECTORY where disk holds a regular
// file (or the mirror image). It is the one shape where a cold start cannot simply plan what
// it sees, because something is in the way of the path it needs.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// blockedChildBody is the server-only content living under the collided root.
var blockedChildBody = []byte("server child content")

// collidedColdStart runs a cold start over a server tree whose directory `docs` holds
// `docs/f.txt`, against a local regular FILE occupying `docs`, and drains the queue.
func collidedColdStart(t *testing.T, privilege string) (*Engine, *store.Store, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: privilege, NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "docs"), []byte("local occupant"), 0o644); err != nil {
		t.Fatalf("write occupant: %v", err)
	}
	c := &catalogClient{
		logHead: 4,
		tree: map[string][]actions.Node{
			"":     {{Name: "docs", FileID: "d1", IsDir: true, MTime: 50}},
			"docs": {{Name: "f.txt", FileID: "f1", Hash: md4.ContentHash(blockedChildBody), Size: int64(len(blockedChildBody)), MTime: 50}},
		},
		content: map[string][]byte{"f1": blockedChildBody},
	}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	return e, st, viewDir
}

// TestColdStartFileDirCollisionConvergesInOnePass is the property the deferral never had.
//
// The old cold start surfaced the collision root as an OpResolveConflict, which cannot settle
// a server DIRECTORY - keep-both resolves file content - so it dead-lettered, and the subtree
// beneath it was rescued only by an incremental replay that a cursor clamp arranged. Nothing
// about that survived the move to a walk, and re-walking could not stand in for it: the
// dead-lettered op is no longer pending, so nothing suppresses a re-plan and every pass
// re-enqueues the same unresolvable op, forever, with the whole view frozen out of sync
// meanwhile.
//
// The mkdir was always what actually converged it, so it is planned directly. Its occupant
// guard is the never-lose gate: the local file is set aside WHOLE as a keep-both copy, never
// deleted, and the freed path is what the descendants' downloads needed.
func TestColdStartFileDirCollisionConvergesInOnePass(t *testing.T) {
	_, st, viewDir := collidedColdStart(t, "rw")

	fi, err := os.Lstat(filepath.Join(viewDir, "docs"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("docs Lstat = %v (err %v), want a directory", fi, err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "docs", "f.txt"))
	if err != nil {
		t.Fatalf("read docs/f.txt: %v", err)
	}
	if string(got) != string(blockedChildBody) {
		t.Errorf("docs/f.txt = %q, want %q", got, blockedChildBody)
	}
	if b, err := st.GetBaseline(7, "docs/f.txt"); err != nil {
		t.Fatalf("GetBaseline docs/f.txt: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("docs/f.txt baseline = %q, want synced", b.State)
	}
	// The occupant survives, whole, under a keep-both name - never deleted.
	var copies []store.Conflict
	if err := st.ScanConflicts(7, func(c store.Conflict) error {
		copies = append(copies, c)
		return nil
	}); err != nil {
		t.Fatalf("ScanConflicts: %v", err)
	}
	if len(copies) != 1 {
		t.Fatalf("conflict copies = %+v, want exactly one for the displaced occupant", copies)
	}
	body, err := os.ReadFile(filepath.Join(viewDir, copies[0].CopyPath))
	if err != nil {
		t.Fatalf("read the keep-both copy %q: %v", copies[0].CopyPath, err)
	}
	if string(body) != "local occupant" {
		t.Errorf("keep-both copy = %q, want the original local bytes", body)
	}
	// One pass, and the view is done: no queue left, nothing to retry.
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0", n, err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = false, want true (the collision must not hold the view back)")
	}
}

// TestColdStartFileDirCollisionConvergesOnAReadOnlyView pins the arm that is easiest to get
// wrong: an inbound mkdir is NOT a push, so it must be planned on a read-only view too.
//
// The read-only early-continue skips the keep-both PUSH, which is right - the local side can
// never be sent. Letting it skip the mkdir as well left the view with a collision that nothing
// would resolve and nothing an operator could see.
func TestColdStartFileDirCollisionConvergesOnAReadOnlyView(t *testing.T) {
	_, st, viewDir := collidedColdStart(t, "ro")

	fi, err := os.Lstat(filepath.Join(viewDir, "docs"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("docs Lstat = %v (err %v), want a directory even on a read-only view", fi, err)
	}
	if _, err := os.ReadFile(filepath.Join(viewDir, "docs", "f.txt")); err != nil {
		t.Errorf("read docs/f.txt on a read-only view: %v", err)
	}
	// The occupant is preserved locally; nothing is pushed from a read-only view.
	var copies []store.Conflict
	if err := st.ScanConflicts(7, func(c store.Conflict) error {
		copies = append(copies, c)
		return nil
	}); err != nil {
		t.Fatalf("ScanConflicts: %v", err)
	}
	if len(copies) != 1 {
		t.Fatalf("conflict copies = %+v, want the occupant preserved", copies)
	}
	if !strings.Contains(copies[0].CopyPath, "_Conflict") {
		t.Errorf("copy path = %q, want a keep-both name", copies[0].CopyPath)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = false, want true")
	}
}

// TestColdStartWithoutTypeMismatchKeepsTheHead is the contrast case: a cold start with no
// file↔dir type mismatch defers nothing, so it marks the view done and keeps the cursor it read
// before the walk. A plain CONTENT divergence - a keep-both the resolver can actually settle -
// must not hold the view back; only a blocked SUBTREE does.
func TestColdStartWithoutTypeMismatchKeepsTheHead(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(filepath.Join(viewDir, "docs"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "clash.txt"), []byte("local clash"), 0o644); err != nil {
		t.Fatalf("write clash.txt: %v", err)
	}
	c := &catalogClient{logHead: 4, tree: map[string][]actions.Node{
		"": {
			{Name: "docs", FileID: "d1", IsDir: true, MTime: 50},
			{Name: "clash.txt", FileID: "c1", Hash: md4.ContentHash([]byte("server clash")), Size: 12, MTime: 50},
		},
		"docs": {},
	}}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	v := listViewOf(t, st, 7).View
	if !v.InitialSyncDone {
		t.Errorf("InitialSyncDone = false, want true")
	}
	if v.LastSeenSyncID != 4 {
		t.Errorf("cursor = %d, want 4 (the pre-walk log head - nothing was deferred)", v.LastSeenSyncID)
	}
}
