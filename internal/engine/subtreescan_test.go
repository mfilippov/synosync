package engine

// Tests for the three hot-path narrowings: the subtree-bounded baseline
// sweeps (relocate/drop), the per-scan symlink-prefix memo, and the protected-path prefix
// index. Each pins BOTH that the cheaper shape produces the same answers as the O(view) /
// O(entries × depth) / O(candidates × protected) one it replaced, and that the work is
// actually bounded (a decode counter, the memo's contents).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// countDecodedBaselines wraps e.scanBaselinePrefix with a counter of the records the store
// actually decoded and handed to the engine, returning a pointer to the running count.
func countDecodedBaselines(e *Engine) *int {
	n := 0
	inner := e.scanBaselinePrefix
	e.scanBaselinePrefix = func(viewID uint64, dir string, fn func(store.PathBaseline) error) ([]string, error) {
		return inner(viewID, dir, func(pb store.PathBaseline) error {
			n++
			return fn(pb)
		})
	}
	return &n
}

// putBaseline writes one synced file baseline with a distinct file id (a repeated id would
// read as a move and delete the earlier record).
func putBaseline(t *testing.T, e *Engine, viewID uint64, path string) {
	t.Helper()
	b := store.Baseline{
		FileID:    "fid-" + path,
		FileType:  "file",
		LocalHash: "aabb",
		State:     store.StateSynced,
	}
	if err := e.store.PutBaseline(viewID, path, b); err != nil {
		t.Fatalf("PutBaseline %q: %v", path, err)
	}
}

// TestRelocateDescendantBaselinesDecodesOnlySubtree pins the subtree-sweep narrowing:
// a directory move relocates its descendants while decoding ONLY the moved subtree's
// records. The old shape materialised every baseline of the view (BaselinesForView) and
// filtered in Go, so a single move in a 50k-file view paid 50k JSON decodes and a 50k-record
// snapshot - on the inbound apply path, per moved directory.
func TestRelocateDescendantBaselinesDecodesOnlySubtree(t *testing.T) {
	e, viewID, _ := newScanEngine(t)

	// Three records under the moved directory, and four the sweep must never touch: two
	// unrelated trees, one sibling whose name merely shares the prefix's letters, and the
	// moved directory's own record (its baseline is the apply's, not the sweep's).
	inside := []string{"d/x.txt", "d/y.txt", "d/sub/z.txt"}
	outside := []string{"a.txt", "d", "d2/keep.txt", "other/deep/file.txt"}
	for _, p := range append(append([]string{}, inside...), outside...) {
		putBaseline(t, e, viewID, p)
	}

	decoded := countDecodedBaselines(e)
	if err := e.relocateDescendantBaselines(viewID, "d", "moved"); err != nil {
		t.Fatalf("relocateDescendantBaselines: %v", err)
	}
	if *decoded != len(inside) {
		t.Errorf("relocate decoded %d baselines, want %d (only the moved subtree)", *decoded, len(inside))
	}

	// Equivalence: every descendant moved to the new prefix, nothing else did.
	for _, p := range inside {
		moved := "moved" + strings.TrimPrefix(p, "d")
		if _, err := e.store.GetBaseline(viewID, moved); err != nil {
			t.Errorf("descendant %q not relocated to %q: %v", p, moved, err)
		}
		if _, err := e.store.GetBaseline(viewID, p); err == nil {
			t.Errorf("descendant %q survived at the old prefix", p)
		}
		if got, err := e.store.PathByID(viewID, "fid-"+p); err != nil || got != moved {
			t.Errorf("byid for %q = %q, %v, want %q", p, got, err, moved)
		}
	}
	for _, p := range outside {
		if _, err := e.store.GetBaseline(viewID, p); err != nil {
			t.Errorf("out-of-subtree baseline %q was disturbed: %v", p, err)
		}
	}
}

// TestDropDescendantBaselinesDecodesOnlySubtree is the same narrowing for the dead-letter
// sweep: it deletes exactly the subtree's records and decodes nothing outside it.
func TestDropDescendantBaselinesDecodesOnlySubtree(t *testing.T) {
	e, viewID, _ := newScanEngine(t)

	inside := []string{"d/x.txt", "d/sub/z.txt"}
	outside := []string{"d", "d2/keep.txt", "z.txt"}
	for _, p := range append(append([]string{}, inside...), outside...) {
		putBaseline(t, e, viewID, p)
	}

	decoded := countDecodedBaselines(e)
	if err := e.dropDescendantBaselines(viewID, "d"); err != nil {
		t.Fatalf("dropDescendantBaselines: %v", err)
	}
	if *decoded != len(inside) {
		t.Errorf("drop decoded %d baselines, want %d (only the subtree)", *decoded, len(inside))
	}
	for _, p := range inside {
		if _, err := e.store.GetBaseline(viewID, p); err == nil {
			t.Errorf("descendant %q survived the drop", p)
		}
	}
	for _, p := range outside {
		if _, err := e.store.GetBaseline(viewID, p); err != nil {
			t.Errorf("out-of-subtree baseline %q was dropped: %v", p, err)
		}
	}
}

// cleanMemoTree lays out a view "Photos" with one subdirectory and two files under root,
// returning the absolute view directory.
func cleanMemoTree(t *testing.T, root string) string {
	t.Helper()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, p := range []string{"top.txt", filepath.Join("sub", "leaf.txt")} {
		if err := os.WriteFile(filepath.Join(viewDir, p), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %q: %v", p, err)
		}
	}
	return viewDir
}

// TestNoSymlinkEscapeMemoCachesDirectoriesOnly pins WHAT the per-scan memo remembers, which
// is exactly what a later entry's walk skips re-Lstat'ing: the existing, proven non-symlink
// DIRECTORY prefixes and nothing else. Leaf files are not cached (no later path can traverse
// one, so the entry would be dead weight - the memo stays O(dirs), not O(entries)), and
// neither is a component that does not exist (it ends the walk and must be re-observed by a
// later entry, since it may have appeared since - possibly as a symlink).
func TestNoSymlinkEscapeMemoCachesDirectoriesOnly(t *testing.T) {
	root := t.TempDir()
	e := &Engine{syncRoot: root}
	viewDir := cleanMemoTree(t, root)

	memo := make(symlinkMemo)
	for _, rel := range []string{
		"top.txt",
		"sub",
		filepath.Join("sub", "leaf.txt"),
		filepath.Join("sub", "does-not-exist.txt"),
		"missing",
		filepath.Join("missing", "child.txt"),
	} {
		if err := e.noSymlinkEscapeMemo("Photos", rel, memo); err != nil {
			t.Errorf("noSymlinkEscapeMemo(%q) = %v, want nil (nothing here is a symlink)", rel, err)
		}
	}

	want := map[string]bool{
		viewDir:                       true,
		filepath.Join(viewDir, "sub"): true,
	}
	if len(memo) != len(want) {
		t.Errorf("memo = %v, want exactly the directory prefixes %v", memo, want)
	}
	for k := range want {
		if !memo[k] {
			t.Errorf("memo missing proven directory %q", k)
		}
	}
	for _, bad := range []string{
		filepath.Join(viewDir, "top.txt"),
		filepath.Join(viewDir, "sub", "leaf.txt"),
		filepath.Join(viewDir, "missing"),
	} {
		if memo[bad] {
			t.Errorf("memo recorded %q; only directories proven non-symlink may be cached", bad)
		}
	}
}

// TestNoSymlinkEscapeMemoMatchesUnmemoized pins that the per-scan memo changes only the
// number of Lstat calls, never the verdict: every path the un-memoized walk accepts or
// rejects gets the same answer from a walk carrying a memo primed by all the paths before
// it. The symlink itself must never enter the memo - a cached symlink would be exactly the
// escape hatch this check exists to close.
func TestNoSymlinkEscapeMemoMatchesUnmemoized(t *testing.T) {
	requireSymlinks(t)
	root := t.TempDir()
	e := &Engine{syncRoot: root}
	viewDir := cleanMemoTree(t, root)

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	rels := []string{
		"top.txt",
		"sub",
		filepath.Join("sub", "leaf.txt"),
		filepath.Join("sub", "does-not-exist.txt"),
		"link",
		filepath.Join("link", "escaped.txt"),
		filepath.Join("link", "deeper", "escaped.txt"),
		"missing",
	}
	memo := make(symlinkMemo)
	for _, rel := range rels {
		want := e.noSymlinkEscape("Photos", rel)
		got := e.noSymlinkEscapeMemo("Photos", rel, memo)
		switch {
		case want == nil && got != nil:
			t.Errorf("memoized walk rejected %q the plain walk accepted: %v", rel, got)
		case want != nil && got == nil:
			t.Errorf("memoized walk accepted %q the plain walk rejected: %v", rel, want)
		case want != nil && got != nil && want.Error() != got.Error():
			t.Errorf("memoized walk error for %q = %v, want %v", rel, got, want)
		}
	}
	if memo[filepath.Join(viewDir, "link")] {
		t.Error("the symlink was memoized as a proven-clean prefix")
	}
}

// swapToSymlink returns an os.Lstat replacement that reports target as a symlink from the
// call after its first observation onwards, leaving every other path (and the first look at
// target itself) untouched. It models the exact window the memo has to survive: WalkDir
// proves a directory in its own callback, and the directory is swapped for a symlink before
// the ReadDir that lists its children. Unlike a real symlink fixture it needs no host
// privilege, so the guard is exercised on every platform.
func swapToSymlink(target string) func(string) (os.FileInfo, error) {
	seen := false
	return func(name string) (os.FileInfo, error) {
		fi, err := os.Lstat(name)
		if name != target || err != nil {
			return fi, err
		}
		if !seen {
			seen = true // the entry's own callback still sees the real directory
			return fi, nil
		}
		return symlinkInfo{FileInfo: fi}, nil
	}
}

// symlinkInfo re-reports a real FileInfo with the symlink bit set.
type symlinkInfo struct{ os.FileInfo }

func (s symlinkInfo) Mode() os.FileMode { return (s.FileInfo.Mode() &^ os.ModeDir) | os.ModeSymlink }

// TestNoSymlinkEscapeMemoRechecksSwappedParentSeam is the host-independent half of
// TestNoSymlinkEscapeMemoRechecksSwappedParent below: the swap is injected through the lstat
// seam instead of the filesystem, so the guard is verified on hosts where symlinks cannot be
// created at all. A cached proof must never cover the checked path's immediate parent.
func TestNoSymlinkEscapeMemoRechecksSwappedParentSeam(t *testing.T) {
	root := t.TempDir()
	viewDir := cleanMemoTree(t, root)
	subDir := filepath.Join(viewDir, "sub")
	e := &Engine{syncRoot: root, lstat: swapToSymlink(subDir)}

	memo := make(symlinkMemo)
	if err := e.noSymlinkEscapeMemo("Photos", "sub", memo); err != nil {
		t.Fatalf("noSymlinkEscapeMemo(sub) = %v, want nil (the directory is still real here)", err)
	}
	if !memo[subDir] {
		t.Fatal("the directory was not memoized; this fixture no longer exercises the memo")
	}

	child := filepath.Join("sub", "leaf.txt")
	if got := e.noSymlinkEscapeMemo("Photos", child, memo); !errors.Is(got, errRejectedPath) {
		t.Errorf("memoized walk for %q = %v, want errRejectedPath (the parent is a symlink now)", child, got)
	}
	if memo[subDir] {
		t.Error("the swapped-in symlink stayed memoized as a proven-clean prefix")
	}
}

// TestScanViewRechecksParentSwappedAfterDescent is the end-to-end statement of the same rule:
// a directory that turns into a symlink after scanView's walk proved it must stop protecting
// nothing - its children must land in the unsurfaced set instead of being scanned as local
// content. Without the per-entry parent re-check the children rode the memo's cached proof,
// and the entries the walk read through the link (which ReadDir follows) would have been
// diffed as local creates and pushed to the server as remote directories.
func TestScanViewRechecksParentSwappedAfterDescent(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	for _, p := range []string{"top.txt", filepath.Join("sub", "leaf.txt")} {
		if err := os.WriteFile(filepath.Join(viewDir, p), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %q: %v", p, err)
		}
	}
	e.lstat = swapToSymlink(filepath.Join(viewDir, "sub"))

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, "sub/leaf.txt"); ok {
		t.Error("scan surfaced a child of a directory swapped for a symlink after it was proven")
	}
	if !unreadable["sub/leaf.txt"] {
		t.Error("the child behind the swapped-in symlink was not recorded in the protected set")
	}
	if _, ok := findEntry(entries, "top.txt"); !ok {
		t.Error("the untouched sibling stopped scanning")
	}
}

// TestNoSymlinkEscapeMemoRechecksSwappedParent pins the one prefix a cached proof may NEVER
// stand in for: the checked path's immediate parent. WalkDir proves a directory in its own
// callback and only THEN calls ReadDir on it, so a directory swapped for a symlink in that
// window is enumerated through the link - and if its children could ride the cached proof,
// the walk would surface entries of the link TARGET as local content (foreign directory names
// pushed to the server as remote mkdirs). Re-Lstat'ing the parent per entry keeps the
// memoized walk's verdict identical to the un-memoized one here.
func TestNoSymlinkEscapeMemoRechecksSwappedParent(t *testing.T) {
	requireSymlinks(t)
	root := t.TempDir()
	e := &Engine{syncRoot: root}
	viewDir := cleanMemoTree(t, root)

	// The parents-first walk proves "sub" in its own callback, which memoizes it.
	memo := make(symlinkMemo)
	if err := e.noSymlinkEscapeMemo("Photos", "sub", memo); err != nil {
		t.Fatalf("noSymlinkEscapeMemo(sub) = %v, want nil", err)
	}
	if !memo[filepath.Join(viewDir, "sub")] {
		t.Fatal("the directory was not memoized; this fixture no longer exercises the memo")
	}

	// Swap it for a symlink out of the tree before the descent that lists its children.
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "leaf.txt"), []byte("s"), 0o644); err != nil {
		t.Fatalf("write outside leaf: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "sub")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	child := filepath.Join("sub", "leaf.txt")
	got := e.noSymlinkEscapeMemo("Photos", child, memo)
	if !errors.Is(got, errRejectedPath) {
		t.Errorf("memoized walk for %q = %v, want errRejectedPath (the parent is a symlink now)", child, got)
	}
	if want := e.noSymlinkEscape("Photos", child); !errors.Is(want, errRejectedPath) {
		t.Errorf("un-memoized walk for %q = %v, want errRejectedPath", child, want)
	}
	if memo[filepath.Join(viewDir, "sub")] {
		t.Error("the swapped-in symlink stayed memoized as a proven-clean prefix")
	}
}

// TestScanViewSymlinkMemoStillProtectsSwappedSubtree guards the memo against the failure it
// could plausibly introduce: a directory proven clean early in the pass must not let a
// SYMLINK deeper on the same path through. The memo only ever caches the ancestors, so the
// symlinked component itself is still Lstat'ed for every entry beneath it.
func TestScanViewSymlinkMemoStillProtectsSwappedSubtree(t *testing.T) {
	requireSymlinks(t)
	e, viewID, viewDir := newScanEngine(t)

	if err := os.MkdirAll(filepath.Join(viewDir, "keep"), 0o755); err != nil {
		t.Fatalf("mkdir keep: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "keep", "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("s"), 0o644); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, "keep/a.txt"); !ok {
		t.Error("the clean subtree was not scanned")
	}
	for _, en := range entries {
		if strings.HasPrefix(en.relPath, "link") {
			t.Errorf("scan surfaced %q from behind a symlink", en.relPath)
		}
	}
	// An UNTRACKED symlink is deliberately NOT protected (protectReplacedTracked): with no
	// baseline there is no base-only row for the merge-join to read as vanished, so there is
	// nothing to mis-diff - and protecting every stray symlink would warn on trees full of
	// them. The assertion here used to demand the opposite, which no run ever executed:
	// requireSymlinks skips this test on a host without symlinks, so a Windows-only test run
	// never reached it and the linux build context is where it surfaces (single-GOOS
	// enforcement misses exactly this).
	if unreadable["link"] {
		t.Error("an untracked symlink joined the protected set; only a TRACKED path replaced " +
			"by a non-syncable node is protected")
	}

	// The protection that DOES matter: the same symlink sitting on a tracked path. Without
	// it the merge-join reads the vanished baseline as a local delete and pushes an OpRemove
	// that destroys the still-present remote file.
	if err := e.store.PutBaseline(viewID, "link", store.Baseline{
		FileType: "file", State: store.StateSynced, LocalHash: "h", ServerHash: "h",
	}); err != nil {
		t.Fatalf("seed baseline for the replaced path: %v", err)
	}
	if _, unreadable, err = e.scanView(context.Background(), viewID, nil); err != nil {
		t.Fatalf("rescan: %v", err)
	}
	if !unreadable["link"] {
		t.Error("a TRACKED path replaced by a symlink was not protected - the diff would read " +
			"it as a local delete and push an OpRemove for the live remote file")
	}
}

// TestProtectedIndexMatchesLinearScan pins the prefix index against the linear predicate it
// replaced: the same verdict for an exact hit, a subtree hit, a sibling that merely shares a
// name prefix, and the case/NFC folding normpath.Key applies.
func TestProtectedIndexMatchesLinearScan(t *testing.T) {
	// The original linear predicate, kept here as the oracle.
	linear := func(path string, unreadable map[string]bool) bool {
		fp := normpath.Key(path)
		for k := range unreadable {
			fk := normpath.Key(k)
			if fp == fk || strings.HasPrefix(fp, fk+"/") {
				return true
			}
		}
		return false
	}

	unreadable := map[string]bool{
		"locked":      true,
		"racing.txt":  true,
		"a/b/deep.md": true,
	}
	idx := newProtectedIndex(unreadable)

	for _, path := range []string{
		"locked",
		"locked/child.txt",
		"locked/deep/child.txt",
		"lockedsibling",       // shares the letters, not the subtree
		"locked.txt",          // ditto
		"racing.txt",          // exact
		"racing.txt/child",    // a file key still protects by prefix (harmless)
		"a/b/deep.md",         // exact, nested
		"a/b",                 // the ancestor itself is NOT protected
		"a/b/deeper/file.txt", // sibling of the protected leaf
		"unrelated/file.txt",
		"LOCKED/child.txt", // fold-sensitive: matches only on a case-insensitive FS
	} {
		if got, want := idx.has(path), linear(path, unreadable); got != want {
			t.Errorf("protectedIndex.has(%q) = %v, want %v (linear oracle)", path, got, want)
		}
	}

	// An empty set yields a nil index that answers false without touching anything.
	if empty := newProtectedIndex(nil); empty != nil || empty.has("anything") {
		t.Errorf("newProtectedIndex(nil) = %v, has = %v, want nil/false", empty, empty.has("anything"))
	}
}

// TestSubtreeSweepsCommitInBatchedTransactions pins the write granularity of the two subtree
// sweeps a directory move or delete runs.
//
// Narrowing the SCAN to the subtree (the two tests above) fixed how many records they decode; it
// left how many transactions they commit. Each descendant cost its own fsync'd write - a read plus
// a write for the relocation - and both sweeps run EAGERLY inside reconcile, on the goroutine that
// also owns the poll and the drain, so a server-side rename or delete of a large tree stalled the
// whole sync cycle for the length of the subtree. bolt rewrites a b-tree path plus the freelist
// per transaction: the same amplification cold start measured at 40 ops/s.
//
// What the sweeps DECIDE is unchanged and is covered by the sibling tests; this one covers only
// the count, and asserts the records really moved so a batch cannot silently drop the work.
func TestSubtreeSweepsCommitInBatchedTransactions(t *testing.T) {
	const kids = subtreeBaselineBatch + 7
	wantBatches := (kids + subtreeBaselineBatch - 1) / subtreeBaselineBatch

	t.Run("relocate", func(t *testing.T) {
		e, viewID, _ := newScanEngine(t)
		for i := range kids {
			putBaseline(t, e, viewID, fmt.Sprintf("d/f%04d.txt", i))
		}
		batches := countBatches(e)
		if err := e.relocateDescendantBaselines(viewID, "d", "moved"); err != nil {
			t.Fatalf("relocateDescendantBaselines: %v", err)
		}
		if *batches != wantBatches {
			t.Errorf("relocation committed %d transactions for %d descendants, want %d (one per "+
				"chunk of %d)", *batches, kids, wantBatches, subtreeBaselineBatch)
		}
		for i := range kids {
			if _, err := e.store.GetBaseline(viewID, fmt.Sprintf("moved/f%04d.txt", i)); err != nil {
				t.Fatalf("descendant %d not relocated: %v", i, err)
			}
		}
	})

	t.Run("drop", func(t *testing.T) {
		e, viewID, _ := newScanEngine(t)
		for i := range kids {
			putBaseline(t, e, viewID, fmt.Sprintf("d/f%04d.txt", i))
		}
		batches := countBatches(e)
		if err := e.dropDescendantBaselines(viewID, "d"); err != nil {
			t.Fatalf("dropDescendantBaselines: %v", err)
		}
		if *batches != wantBatches {
			t.Errorf("drop committed %d transactions for %d descendants, want %d (one per chunk "+
				"of %d)", *batches, kids, wantBatches, subtreeBaselineBatch)
		}
		for i := range kids {
			if _, err := e.store.GetBaseline(viewID, fmt.Sprintf("d/f%04d.txt", i)); !isNotFound(err) {
				t.Fatalf("descendant %d survived the drop: %v", i, err)
			}
		}
	})
}
