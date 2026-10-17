//go:build !windows

package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// This file carries the symlink-component regressions: the two scanView skip branches that
// used to drop a path WITHOUT recording it in the unsurfaced (protect) set - the
// non-containable check and the noSymlinkEscape check. Every other skip branch of the walk
// protects its path; an unprotected skip lets the merge-join read a still-tracked baseline as
// "the file vanished" and push an OpRemove that deletes the live remote node.

// assertBaselinesStillSynced fails when any of paths lost its StateSynced baseline - the
// symptom a spurious OpRemove leaves behind once it is applied.
func assertBaselinesStillSynced(t *testing.T, e *Engine, viewID uint64, paths ...string) {
	t.Helper()
	for _, p := range paths {
		b, err := e.store.GetBaseline(viewID, p)
		if err != nil {
			t.Fatalf("GetBaseline %q: %v", p, err)
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline state for %q = %q, want synced (untouched)", p, b.State)
		}
	}
}

// TestScanSymlinkedViewNameSegmentProtectsEntries is variant (a): an INTERMEDIATE
// segment of a multi-segment view name ("a/b") is a symlink to the real directory (the user
// moved the tree to a bigger disk and linked it back). Both WalkDir and the empty-scan
// availability probe traverse an intermediate link - os.Lstat only refuses to follow the LAST
// component - so the walk enumerates a perfectly healthy tree while noSymlinkEscape refuses
// every single entry. Before the fix those refusals were silent: the scan came back empty with
// an empty protect set, viewRootUnavailable said "present and healthy", the device guard had
// nothing to compare against, and the empty-scan debounce therefore confirmed a whole-view
// delete and erased every remote baseline. With the branch protected, the paths are treated as
// still-present and no op is ever emitted, however many times the scan repeats.
func TestScanSymlinkedViewNameSegmentProtectsEntries(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	elsewhere := t.TempDir() // the "bigger disk" the user moved the tree to
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "a/b", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	var buf bytes.Buffer
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))

	// The real tree lives outside the sync root; root/a is a symlink to it, so the view's
	// on-disk path root/a/b resolves to a healthy directory with live content.
	realView := filepath.Join(elsewhere, "a", "b")
	if err := os.MkdirAll(filepath.Join(realView, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir real view: %v", err)
	}
	top := []byte("top content")
	nested := []byte("nested content")
	if err := os.WriteFile(filepath.Join(realView, "x.txt"), top, 0o644); err != nil {
		t.Fatalf("write x.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(realView, "sub", "y.txt"), nested, 0o644); err != nil {
		t.Fatalf("write y.txt: %v", err)
	}
	if err := os.Symlink(filepath.Join(elsewhere, "a"), filepath.Join(root, "a")); err != nil {
		t.Fatalf("symlink view segment: %v", err)
	}

	putSynced(t, e, viewID, "x.txt", "fx", "file", md4.ContentHash(top), int64(len(top)))
	putSynced(t, e, viewID, "sub", "fs", "dir", "", 0)
	putSynced(t, e, viewID, "sub/y.txt", "fy", "file", md4.ContentHash(nested), int64(len(nested)))

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("scanView surfaced %d entries from behind a symlinked view segment, want 0: %+v", len(entries), entries)
	}
	for _, p := range []string{"x.txt", "sub"} {
		if !unreadable[p] {
			t.Errorf("path %q skipped behind a symlinked component was not recorded in the unsurfaced set; its baseline diffs as a local deletion", p)
		}
	}

	// Run past the empty-scan debounce threshold: the run of empty scans must never turn into
	// a whole-view delete, because every baseline's path is protected.
	for i := 0; i < e.limits.EmptyScanThreshold+1; i++ {
		ops, rerr := e.localReconcile(context.Background(), viewID)
		if rerr != nil {
			t.Fatalf("localReconcile pass %d: %v", i+1, rerr)
		}
		if len(ops) != 0 {
			t.Fatalf("localReconcile pass %d emitted %d ops for a view behind a symlinked name segment, want 0: %+v", i+1, len(ops), ops)
		}
	}
	assertBaselinesStillSynced(t, e, viewID, "x.txt", "sub", "sub/y.txt")
	if !strings.Contains(buf.String(), "symlinked path component") {
		t.Errorf("no Warn logged for the symlinked view segment; log: %s", buf.String())
	}
}

// TestScanSymlinkedComponentWarnsOncePerPass pins the Warn's per-pass throttle: the symlinked
// component is shared by every path beneath it, so a per-entry Warn would be a log wall over
// the whole view on every scan.
func TestScanSymlinkedComponentWarnsOncePerPass(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	elsewhere := t.TempDir()
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "a/b", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	var buf bytes.Buffer
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))

	realView := filepath.Join(elsewhere, "a", "b")
	if err := os.MkdirAll(realView, 0o755); err != nil {
		t.Fatalf("mkdir real view: %v", err)
	}
	for _, n := range []string{"f1.txt", "f2.txt", "f3.txt"} {
		if err := os.WriteFile(filepath.Join(realView, n), []byte(n), 0o644); err != nil {
			t.Fatalf("write %s: %v", n, err)
		}
	}
	if err := os.Symlink(filepath.Join(elsewhere, "a"), filepath.Join(root, "a")); err != nil {
		t.Fatalf("symlink view segment: %v", err)
	}

	if _, _, err := e.scanView(context.Background(), viewID, nil); err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if n := strings.Count(buf.String(), "symlinked path component"); n != 1 {
		t.Fatalf("logged the symlinked-component Warn %d times in one pass, want exactly 1; log: %s", n, buf.String())
	}
}

// TestScanTrackedDirSwappedToSymlinkMidWalkProtectsSubtree is variant (b): a tracked
// subdirectory is swapped for a directory symlink DURING the walk - after the parent's readdir
// (so the cached DirEntry still says "directory" and the entry-level symlink branch never
// fires) but before the entry's own callback. noSymlinkEscape then refuses the directory, and
// before the fix that refusal was silent: neither the directory nor anything beneath it entered
// the protect set, so the same non-empty scan (no debounce involved) diffed the whole subtree
// as vanished and pushed an OpRemove per node.
//
// The mid-walk swap is made deterministic through the scanBaselines seam: an untracked stray
// symlink at "aaa" (lexically ahead of "big") drives protectReplacedTracked's fold-key
// fallback, which is the only baseline stream that runs INSIDE the walk. The first stream of
// the pass is localReconcileWith's own collectBaselines, so the swap is performed on the
// second call. The "symlinked path component" Warn assertion pins that the walk really took
// the noSymlinkEscape branch rather than the (already protected) symlink-entry branch, so the
// test cannot pass vacuously if that ordering ever shifts.
func TestScanTrackedDirSwappedToSymlinkMidWalkProtectsSubtree(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	elsewhere := t.TempDir()
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	var buf bytes.Buffer
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "big"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}

	keep := []byte("keep")
	child := []byte("child content")
	writeFile(t, viewDir, "keep.txt", keep)
	writeFile(t, viewDir, "big/x.txt", child)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	putSynced(t, e, viewID, "big", "fb", "dir", "", 0)
	putSynced(t, e, viewID, "big/x.txt", "fx", "file", md4.ContentHash(child), int64(len(child)))

	// An untracked stray symlink, walked before "big", gives us a hook inside the walk.
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "aaa")); err != nil {
		t.Fatalf("symlink stray: %v", err)
	}

	swapped := false
	calls := 0
	inner := e.scanBaselines
	e.scanBaselines = func(v uint64, fn func(store.PathBaseline) error) ([]string, error) {
		calls++
		if calls == 2 && !swapped {
			swapped = true
			// mv Photos/big <elsewhere>/big && ln -s <elsewhere>/big Photos/big
			moved := filepath.Join(elsewhere, "big")
			if err := os.Rename(filepath.Join(viewDir, "big"), moved); err != nil {
				t.Errorf("mid-walk move: %v", err)
				return inner(v, fn)
			}
			if err := os.Symlink(moved, filepath.Join(viewDir, "big")); err != nil {
				t.Errorf("mid-walk symlink: %v", err)
			}
		}
		return inner(v, fn)
	}

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if !swapped {
		t.Fatal("the mid-walk swap never ran; the scanBaselines call ordering changed and the scenario was not exercised")
	}
	if !strings.Contains(buf.String(), "symlinked path component") {
		t.Fatalf("the walk did not take the noSymlinkEscape branch, so the scenario was not exercised; log: %s", buf.String())
	}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted OpRemove %d/%q for a subtree behind a mid-walk symlink swap; this deletes the still-present remote node", op.ViewID, op.Path)
		}
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
	assertBaselinesStillSynced(t, e, viewID, "keep.txt", "big", "big/x.txt")
}
