package engine

// Tests for the dir-over-dir terminal exit: a DIR-placing inbound op -
// a directory rename, or the mkdir rejected-move subtree relocate - whose destination is
// held by an untracked local directory. Unix dead-letters it through the fs.ErrExist arm
// (os.Rename's userspace guard synthesises EEXIST for any existing-directory destination);
// Windows reports ERROR_ACCESS_DENIED → fs.ErrPermission instead, which used to read as
// transient and retry forever with blockedKeys starving both path prefixes. The classifier
// arm makes both hosts reach the same terminal outcome, so the drain assertions below are
// deliberately platform-independent.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// terminalDirTargetFailure mirrors applyOp's two dir-target arms: the platform errno the
// failing apply produced is either fs.ErrExist (Unix's rename guard) or a permission error
// the classifier recognises as dir-over-dir (Windows).
func terminalDirTargetFailure(e *Engine, op store.Op, err error) bool {
	return errors.Is(err, fs.ErrExist) ||
		(errors.Is(err, fs.ErrPermission) && e.permissionBlockedByDirTarget(op))
}

// TestPermissionBlockedByDirTargetDirPlacingOps pins the widened classifier: a directory
// rename and the mkdir subtree relocate now resolve their permission failures through the
// same Lstat probe as the file-placing verbs, while every shape that is not a replacing
// directory move stays transient - most importantly a PLAIN mkdir, whose target existing as
// a directory is its success shape.
func TestPermissionBlockedByDirTargetDirPlacingOps(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// dirblock is the untracked occupant every dir-placing op below collides with; tracked
	// is the subtree a rejected move left at its old path (file id fd).
	for _, dir := range []string{"dirblock", "tracked"} {
		if err := os.MkdirAll(filepath.Join(viewDir, dir), 0o755); err != nil {
			t.Fatalf("seed %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(viewDir, "fileblock.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file occupant: %v", err)
	}
	for path, b := range map[string]store.Baseline{
		// The relocate shape: a dir node tracked at `tracked`, still on disk there.
		"tracked": {FileID: "fd", FileType: "dir", State: store.StateSynced},
		// Tracked at the mkdir's own path - nothing to relocate.
		"dirblock": {FileID: "fb", FileType: "dir", State: store.StateSynced},
		// Tracked elsewhere but no longer on disk - the relocate falls back to a plain mkdir.
		"goneold": {FileID: "fg", FileType: "dir", State: store.StateSynced},
		// Tracked elsewhere as a FILE - not the subtree-relocate shape.
		"otherfile.txt": {FileID: "ff", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	cases := []struct {
		name string
		op   store.Op
		want bool
	}{
		{"dir rename onto dir occupant", store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "tracked", NewPath: "dirblock"}, true},
		{"dir rename onto file occupant stays transient", store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "tracked", NewPath: "fileblock.txt"}, false},
		{"dir rename onto absent target stays transient", store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "tracked", NewPath: "missing"}, false},
		{"mkdir relocate onto dir occupant", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "fd"}, true},
		{"plain mkdir stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock"}, false},
		{"mkdir tracked at its own path stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "fb"}, false},
		{"mkdir with unknown file id stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "nope"}, false},
		{"mkdir whose old path is gone stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "fg"}, false},
		{"mkdir whose tracked node is a file stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "ff"}, false},
		{"mkdir relocate onto absent target stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "missing", FileID: "fd"}, false},
		// Regressions from the original table: the file-placing verbs are unchanged.
		{"download onto dir occupant", store.Op{Type: store.OpDownload, ViewID: 7, Path: "dirblock"}, true},
		{"remove stays transient", store.Op{Type: store.OpRemove, IsDir: true, ViewID: 7, Path: "dirblock"}, false},
	}
	for _, tc := range cases {
		if got := e.permissionBlockedByDirTarget(tc.op); got != tc.want {
			t.Errorf("%s: permissionBlockedByDirTarget = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestPermissionBlockedByDirTargetSelfAddressedMove pins the same-node exit: a directory move
// whose destination is a SECOND SPELLING of its own source - a case-only rename on a
// case-insensitive filesystem, an NFD↔NFC-only one on a normalising filesystem - has no
// occupant at all, so its permission failure is ordinary transient noise (an open handle, a
// process holding the directory as its CWD) and must keep retrying. Without the os.SameFile
// guard the destination Lstat finds the tracked SOURCE subtree, reads it as the untracked
// never-vanishing occupant the terminal classification assumes, and dead-letters an inbound
// rename forever - leaving the subtree permanently diverged from the server.
func TestPermissionBlockedByDirTargetSelfAddressedMove(t *testing.T) {
	root := t.TempDir()
	insensitive, err := probeSyncRootCaseFold(root)
	if err != nil {
		t.Skipf("host case-fold probe failed: %v", err)
	}
	if !insensitive {
		t.Skip("host filesystem distinguishes case; a case-only rename addresses two distinct nodes")
	}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	viewDir := filepath.Join(root, "Photos")
	// Docs/ is the tracked subtree; "docs" is the same directory under a second spelling.
	if err := os.MkdirAll(filepath.Join(viewDir, "Docs"), 0o755); err != nil {
		t.Fatalf("seed tracked subtree: %v", err)
	}
	if err := st.PutBaseline(7, "Docs", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	for _, tc := range []struct {
		name string
		op   store.Op
	}{
		{"case-only dir rename", store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "Docs", NewPath: "docs"}},
		{"case-only mkdir relocate", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "docs", FileID: "fd"}},
	} {
		if e.permissionBlockedByDirTarget(tc.op) {
			t.Errorf("%s: permissionBlockedByDirTarget = true; the destination resolves to the op's own "+
				"source, so a permission failure there is transient, not a dir-over-dir replace", tc.name)
		}
	}
}

// TestMkdirRelocateClassifierMatchesTheRelocateOnUnresolvableOldPaths pins the one gate the
// classifier used to disagree with relocateTrackedDirForMkdir on. The relocate returns "not the
// relocate shape" - a PLAIN mkdir - when the tracked old path cannot be resolved without crossing
// a symlink; the classifier used to swallow that failure, keep the literal path and answer "this
// was a dir-over-dir replace", so a plain mkdir's ordinary permission failure dead-lettered the
// op instead of retrying. Every read failure must answer false.
func TestMkdirRelocateClassifierMatchesTheRelocateOnUnresolvableOldPaths(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// real/ holds the tracked subtree; link/ is a symlink to it, so the tracked path
	// "link/tracked" Lstats fine literally but cannot be resolved without crossing the symlink.
	if err := os.MkdirAll(filepath.Join(viewDir, "real", "tracked"), 0o755); err != nil {
		t.Fatalf("seed real subtree: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "dirblock"), 0o755); err != nil {
		t.Fatalf("seed occupant: %v", err)
	}
	if err := os.Symlink(filepath.Join(viewDir, "real"), filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("host refuses to create the fixture symlink: %v", err)
	}
	if err := st.PutBaseline(7, "link/tracked", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	op := store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock", FileID: "fd"}
	// The relocate itself declines (it falls back to a plain mkdir), so the classifier must too.
	relocated, err := e.relocateTrackedDirForMkdir(op, filepath.Join(viewDir, "dirblock"))
	if err != nil || relocated {
		t.Fatalf("relocateTrackedDirForMkdir = (%v, %v), want (false, nil) for an unresolvable old path", relocated, err)
	}
	if e.permissionBlockedByDirTarget(op) {
		t.Error("permissionBlockedByDirTarget = true for an op the relocate declines; a plain mkdir's " +
			"permission failure would be dead-lettered instead of retried")
	}
}

// TestDirOverDirRenameIsHostTerminal is the empirical half: it performs the very rename the
// apply paths perform - a directory onto an existing directory, empty and non-empty - and
// asserts the error the HOST produces is one applyOp treats as terminal. It is the pin that
// caught the Windows gap: ERROR_ACCESS_DENIED is not fs.ErrExist, so before the classifier
// arm the non-empty and the empty occupant alike read as a transient failure there.
func TestDirOverDirRenameIsHostTerminal(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	for _, tc := range []struct {
		name     string
		occupant string
		child    bool
	}{
		{"empty occupant", "emptydst", false},
		{"non-empty occupant", "fulldst", true},
	} {
		src := filepath.Join(viewDir, "src-"+tc.occupant)
		if err := os.MkdirAll(src, 0o755); err != nil {
			t.Fatalf("%s: seed source: %v", tc.name, err)
		}
		dst := filepath.Join(viewDir, tc.occupant)
		if err := os.MkdirAll(dst, 0o755); err != nil {
			t.Fatalf("%s: seed occupant: %v", tc.name, err)
		}
		if tc.child {
			if err := os.WriteFile(filepath.Join(dst, "child.txt"), []byte("x"), 0o644); err != nil {
				t.Fatalf("%s: seed occupant child: %v", tc.name, err)
			}
		}
		err := os.Rename(src, dst)
		if err == nil {
			// A host that replaces a directory outright would break the never-lose direction
			// long before the classifier does; neither Unix nor Windows does this.
			t.Fatalf("%s: os.Rename replaced a directory occupant", tc.name)
		}
		op := store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "src-" + tc.occupant, NewPath: tc.occupant}
		if !terminalDirTargetFailure(e, op, err) {
			t.Errorf("%s: host dir-over-dir rename error %v is classified transient; the op would retry forever", tc.name, err)
		}
	}
}

// TestDrainQueueDeadLettersDirRenameOverLocalDir is the wedge half for the rename verb: the
// server moved a tracked directory onto a path the user holds as their own untracked
// directory. The occupant never vanishes under the never-delete policy, so the op must leave
// the queue in ONE drain - with both prefixes' chains (source and destination) flowing right
// behind it - instead of starving them forever.
func TestDrainQueueDeadLettersDirRenameOverLocalDir(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The tracked directory sits at Reports with a child; the user's own untracked Archive
	// occupies the destination.
	if err := os.MkdirAll(filepath.Join(viewDir, "Reports"), 0o755); err != nil {
		t.Fatalf("seed source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "Reports", "r.txt"), []byte("rr"), 0o644); err != nil {
		t.Fatalf("seed source child: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "Archive", "mine"), 0o755); err != nil {
		t.Fatalf("seed dest occupant: %v", err)
	}
	if err := st.PutBaseline(7, "Reports", store.Baseline{FileID: "fd", FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, IsDir: true,
		ViewID: 7, Path: "Reports", NewPath: "Archive", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// Two successors, one overlapping each prefix of the doomed rename: both are serialised
	// behind it by the scheduler and must drain in the same pass once it dead-letters.
	for _, path := range []string{"Reports/sub", "Archive/sub"} {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: path,
		}); err != nil {
			t.Fatalf("Enqueue mkdir %q: %v", path, err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a dir rename onto a local dir, not wedge: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the dir rename and its two chains are wedged", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly the dropped rename's trace", n, err)
	}
	// Nothing on disk was destroyed: the occupant, its child and the source subtree all stand.
	for _, rel := range []string{"Archive", "Archive/mine", "Reports"} {
		if fi, err := os.Stat(filepath.Join(viewDir, filepath.FromSlash(rel))); err != nil || !fi.IsDir() {
			t.Errorf("%s changed: isdir=%v err=%v", rel, fi != nil && fi.IsDir(), err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "Reports", "r.txt")); err != nil || string(got) != "rr" {
		t.Errorf("source child = %q (err %v), want rr", got, err)
	}
	// The unblocked successors really applied.
	for _, rel := range []string{"Reports/sub", "Archive/sub"} {
		if fi, err := os.Stat(filepath.Join(viewDir, filepath.FromSlash(rel))); err != nil || !fi.IsDir() {
			t.Errorf("successor %s did not apply: isdir=%v err=%v", rel, fi != nil && fi.IsDir(), err)
		}
	}
}

// TestDrainQueueDeadLettersMkdirRelocateOverLocalDir is the mirror: the rejected-move
// recovery (relocateTrackedDirForMkdir) performs the very same replacing directory rename
// one verb later, and OpMkdir was not even in the classifier's switch. Same terminal outcome
// on both platforms, same untouched disk.
func TestDrainQueueDeadLettersMkdirRelocateOverLocalDir(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The directory node (file id fd) is tracked - and materialised - at old/, while the
	// server reports it at new/, which the user's own untracked directory occupies.
	if err := os.MkdirAll(filepath.Join(viewDir, "old", "sub"), 0o755); err != nil {
		t.Fatalf("seed tracked subtree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "old", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed tracked child: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "new", "mine"), 0o755); err != nil {
		t.Fatalf("seed dest occupant: %v", err)
	}
	for path, b := range map[string]store.Baseline{
		"old":           {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"old/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"old/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7,
		Path: "new", FileID: "fd", ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "new/later",
	}); err != nil {
		t.Fatalf("Enqueue successor: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a mkdir relocate onto a local dir, not wedge: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the relocate and its chain are wedged", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly the dropped mkdir's trace", n, err)
	}
	// The occupant is untouched and the tracked subtree stays whole at its old path (the
	// relocate never ran), so the next scan/cold start can still converge it.
	if fi, err := os.Stat(filepath.Join(viewDir, "new", "mine")); err != nil || !fi.IsDir() {
		t.Errorf("occupant child changed: isdir=%v err=%v", fi != nil && fi.IsDir(), err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "old", "sub", "y.txt")); err != nil || string(got) != "yy" {
		t.Errorf("tracked child = %q (err %v), want yy", got, err)
	}
	// A synced record is never dropped by the dead-letter cleanup, so the subtree stays tracked.
	if b, err := st.GetBaseline(7, "old"); err != nil || b.FileID != "fd" {
		t.Errorf("tracked dir baseline = %+v (err %v), want fd still at old", b, err)
	}
	if fi, err := os.Stat(filepath.Join(viewDir, "new", "later")); err != nil || !fi.IsDir() {
		t.Errorf("successor new/later did not apply: isdir=%v err=%v", fi != nil && fi.IsDir(), err)
	}
}

// TestDirRenamePermissionWithoutOccupantStaysTransient pins the conservatism of the widened
// arm: a permission failure with NO directory at the destination (a read-only parent here, an
// antivirus lock or transient ACL noise in the field) is not proof the op can never apply, so
// the dir rename keeps its transient outcome - queued, untraced, retried on the next drain.
func TestDirRenamePermissionWithoutOccupantStaysTransient(t *testing.T) {
	requireReadOnlyDir(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "Reports"), 0o755); err != nil {
		t.Fatalf("seed source dir: %v", err)
	}
	// The destination parent denies writes and holds no occupant at all.
	roParent := filepath.Join(viewDir, "ro")
	if err := os.Mkdir(roParent, 0o500); err != nil {
		t.Fatalf("seed read-only parent: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roParent, 0o700) })
	if err := st.PutBaseline(7, "Reports", store.Baseline{FileID: "fd", FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, IsDir: true,
		ViewID: 7, Path: "Reports", NewPath: "ro/Archive", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the transient permission error surfaced, got nil")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Errorf("PendingLen = %d (err %v), want 1: a transient permission failure must stay queued", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0: the op was dropped on a transient failure", n, err)
	}
}
