package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// TestNestedUnmountDoesNotDeleteTheSubtreeOnTheServer is the scenario the guard exists for.
//
// A mount point one level down - `<view>/Photos` on a USB disk, an NFS export, a bind mount -
// reads after an unmount as an ordinary empty directory. Every gate downstream then behaves
// correctly and the outcome is still a disaster: the scan diffs each baseline beneath it as
// vanished, each remove's precondition passes (the server holds exactly what the baseline
// recorded, which is what an untouched subtree looks like), and `remove` is recursive.
//
// The empty-scan debounce protected only the view ROOT. This asserts the server's copy survives
// a nested one.
func TestNestedUnmountDoesNotDeleteTheSubtreeOnTheServer(t *testing.T) {
	ctx := context.Background()
	d := newFakeDSM()
	d.addView(7, 70, "Photos")

	n := newDSMEngine(t, d, "sess-a", false)
	n.write(t, "Photos", "mount/a.txt", "first")
	n.write(t, "Photos", "mount/b.txt", "second")
	n.write(t, "Photos", "keep.txt", "outside the mount")
	n.settle(t, 2)

	if got := d.paths(7); len(got) != 4 {
		t.Fatalf("server holds %v, want the directory, its two files and keep.txt", got)
	}

	// THE UNMOUNT: the path still names a directory, but a different, empty one - the
	// directory the mount covered. Moving the mounted one out of the view (rather than
	// deleting its files) is what keeps the simulation honest: an unmount never empties the
	// mounted directory in place, it uncovers another object, and that difference is exactly
	// what the guard keys on. The moved directory stays alive, so its inode cannot be reused.
	uncoverMount(t, filepath.Join(n.viewDir("Photos"), "mount"))

	n.e.lastScan = time.Time{}
	if err := n.e.localScan(ctx); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if err := n.e.drainQueue(ctx); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, ok := d.content(7, "mount/a.txt"); !ok {
		t.Errorf("the server's mount/a.txt was deleted on the first empty scan; server holds %v", d.paths(7))
	}
	if _, ok := d.content(7, "mount/b.txt"); !ok {
		t.Errorf("the server's mount/b.txt was deleted on the first empty scan; server holds %v", d.paths(7))
	}
}

// TestEmptiedDirectoryConvergesOnceConfirmed is the other half: a directory emptied while the
// engine had no identity evidence for it - first seen after a restart - still gets the deletion,
// once the emptiness has been observed often enough and over enough wall clock. The guard
// delays that case; it must not block it.
func TestEmptiedDirectoryConvergesOnceConfirmed(t *testing.T) {
	ctx := context.Background()
	d := newFakeDSM()
	d.addView(7, 70, "Photos")

	n := newDSMEngine(t, d, "sess-a", false)
	n.write(t, "Photos", "dir/a.txt", "content")
	n.settle(t, 2)
	if _, ok := d.content(7, "dir/a.txt"); !ok {
		t.Fatalf("the file never reached the server; server holds %v", d.paths(7))
	}

	if err := os.Remove(filepath.Join(n.viewDir("Photos"), "dir", "a.txt")); err != nil {
		t.Fatalf("empty the directory: %v", err)
	}
	// A restart: the run's memory of the directory as populated is gone, so its emptiness
	// carries no identity evidence and must be corroborated the slow way.
	clear(n.e.dirSeenNonEmpty)

	// Advance the clock past the debounce floor between scans, the way real time would.
	base := time.Now()
	for i := 0; i < n.e.limits.EmptyScanThreshold+1; i++ {
		at := base.Add(time.Duration(i) * (emptyScanConfirmMinSpacing + time.Second))
		n.e.now = func() time.Time { return at }
		n.e.lastScan = time.Time{}
		if err := n.e.localScan(ctx); err != nil {
			t.Fatalf("localScan %d: %v", i, err)
		}
		if err := n.e.drainQueue(ctx); err != nil {
			t.Fatalf("drainQueue %d: %v", i, err)
		}
	}

	if _, ok := d.content(7, "dir/a.txt"); ok {
		t.Errorf("the deletion never propagated; server still holds %v", d.paths(7))
	}
}

// TestEmptiedDirsIgnoresPopulatedAncestors pins the classifier: only a directory with nothing
// beneath it on disk is a candidate, and the view root is never one (that is the view-root
// guard's own subject).
func TestEmptiedDirsIgnoresPopulatedAncestors(t *testing.T) {
	rows := []joinRow{
		{scan: &scanEntry{relPath: "a", isDir: true}},
		{scan: &scanEntry{relPath: "a/b", isDir: true}},
		{scan: &scanEntry{relPath: "a/b/f.txt"}},
		{scan: &scanEntry{relPath: "c", isDir: true}},
	}
	got := emptiedDirs(rows)
	if !got["c"] {
		t.Error("c holds nothing on disk and was not reported empty")
	}
	if got["a"] || got["a/b"] {
		t.Errorf("a populated ancestor was reported empty: %v", got)
	}
	if got[""] {
		t.Error("the view root was reported; its emptiness belongs to the view-root guard")
	}
}

// TestSuspectEmptiedDirsSkipsDirectoriesWithNothingRecorded pins that an empty directory the
// baselines record no contents under keeps no streak: there is nothing beneath it to delete, so
// there is nothing to protect.
func TestSuspectEmptiedDirsSkipsDirectoriesWithNothingRecorded(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())

	rows := []joinRow{
		{scan: &scanEntry{relPath: "empty", isDir: true}},
		{scan: &scanEntry{relPath: "withkids", isDir: true},
			base: &store.PathBaseline{Path: "withkids", Baseline: store.Baseline{State: store.StateSynced}}},
		{base: &store.PathBaseline{Path: "withkids/gone.txt", Baseline: store.Baseline{State: store.StateSynced}}},
	}
	suspect := e.suspectEmptiedDirs(7, rows, 3)

	if suspect["empty"] {
		t.Error("a directory with nothing recorded under it was treated as suspicious")
	}
	if !suspect["withkids"] {
		t.Error("a directory whose recorded contents vanished was not deferred")
	}
	if len(e.emptiedDirScans) != 1 {
		t.Errorf("streaks = %d, want one (only the directory that needed protecting)", len(e.emptiedDirScans))
	}
}

// uncoverMount stands in for an unmount at dir: the populated directory leaves the view and a
// different, empty directory takes its path.
func uncoverMount(t *testing.T, dir string) {
	t.Helper()
	covered := filepath.Join(t.TempDir(), "mounted")
	if err := os.Rename(dir, covered); err != nil {
		t.Skipf("cannot move the mounted directory aside on this host (%v)", err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("recreate the covered directory: %v", err)
	}
}

// TestEmptiedInPlaceDirectoryPropagatesAtOnce is the common case the guard used to tax: a user
// deletes the last file in a folder. The folder is the same object it was when it held the
// file, so this is a deletion, not an uncovered mount point, and it reaches the server on the
// very next scan instead of after the debounce - two minutes with the default threshold.
func TestEmptiedInPlaceDirectoryPropagatesAtOnce(t *testing.T) {
	ctx := context.Background()
	d := newFakeDSM()
	d.addView(7, 70, "Photos")

	n := newDSMEngine(t, d, "sess-a", false)
	n.write(t, "Photos", "dir/a.txt", "content")
	n.settle(t, 2)
	if _, ok := d.content(7, "dir/a.txt"); !ok {
		t.Fatalf("the file never reached the server; server holds %v", d.paths(7))
	}

	if err := os.Remove(filepath.Join(n.viewDir("Photos"), "dir", "a.txt")); err != nil {
		t.Fatalf("delete the last file: %v", err)
	}
	n.e.lastScan = time.Time{}
	if err := n.e.localScan(ctx); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if err := n.e.drainQueue(ctx); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, ok := d.content(7, "dir/a.txt"); ok {
		t.Errorf("deleting the last file in a folder was deferred; server still holds %v", d.paths(7))
	}
	if _, ok := d.content(7, "dir"); !ok && !containsPath(d.paths(7), "dir") {
		t.Errorf("the emptied folder itself was removed; server holds %v", d.paths(7))
	}
}

// TestSuspectEmptiedDirsTrustsOnlyAMatchingIdentity pins the discriminator at the row level:
// an emptied directory is a deletion only when it is the very object this run saw holding
// content. A different object at the path, or one never seen populated, keeps the debounce.
func TestSuspectEmptiedDirsTrustsOnlyAMatchingIdentity(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())
	child := func(dir string) joinRow {
		return joinRow{base: &store.PathBaseline{Path: dir + "/f.txt", Baseline: store.Baseline{State: store.StateSynced}}}
	}
	dirRow := func(dir string, id fileIdent, withFile bool) []joinRow {
		rows := []joinRow{{scan: &scanEntry{relPath: dir, isDir: true, dirID: id, hasDirID: true},
			base: &store.PathBaseline{Path: dir, Baseline: store.Baseline{State: store.StateSynced}}}}
		if withFile {
			rows = append(rows, joinRow{scan: &scanEntry{relPath: dir + "/f.txt"}, base: child(dir).base})
		} else {
			rows = append(rows, child(dir))
		}
		return rows
	}
	same, swapped, unseen := fileIdent{dev: 1, ino: 10}, fileIdent{dev: 1, ino: 20}, fileIdent{dev: 1, ino: 30}

	// Pass 1: "same" and "swapped" are seen populated; "unseen" is first seen already empty.
	var pass1 []joinRow
	pass1 = append(pass1, dirRow("same", same, true)...)
	pass1 = append(pass1, dirRow("swapped", swapped, true)...)
	pass1 = append(pass1, dirRow("unseen", unseen, false)...)
	if s := e.suspectEmptiedDirs(7, pass1, 3); !s["unseen"] || len(s) != 1 {
		t.Fatalf("pass 1 suspect = %v, want only the directory never seen populated", s)
	}

	// Pass 2: all three are empty; "swapped" now has a different identity.
	var pass2 []joinRow
	pass2 = append(pass2, dirRow("same", same, false)...)
	pass2 = append(pass2, dirRow("swapped", fileIdent{dev: 2, ino: 1}, false)...)
	pass2 = append(pass2, dirRow("unseen", unseen, false)...)
	s := e.suspectEmptiedDirs(7, pass2, 3)
	if s["same"] {
		t.Error("a directory emptied in place was deferred")
	}
	if !s["swapped"] {
		t.Error("a different object at a populated directory's path was trusted as a deletion")
	}
	if !s["unseen"] {
		t.Error("a directory never seen populated was trusted as a deletion")
	}
}

// containsPath reports whether paths holds p.
func containsPath(paths []string, p string) bool {
	for _, x := range paths {
		if x == p {
			return true
		}
	}
	return false
}
