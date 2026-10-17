package engine

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// newScanEngine builds an Engine rooted at a fresh temp dir with a single view named
// "Photos", returning the engine, the view id, and the absolute view directory.
func newScanEngine(t *testing.T) (*Engine, uint64, string) {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	return e, viewID, viewDir
}

// byRelPath sorts entries so a test asserts independently of walk order.
func byRelPath(entries []scanEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].relPath < entries[j].relPath })
}

func findEntry(entries []scanEntry, rel string) (scanEntry, bool) {
	for _, en := range entries {
		if en.relPath == rel {
			return en, true
		}
	}
	return scanEntry{}, false
}

func TestScanViewNestedDirsAndHashes(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	top := []byte("top level content")
	nested := []byte("nested content")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), top, 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "sub", "b.txt"), nested, 0o600); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	byRelPath(entries)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(entries), entries)
	}

	a, ok := findEntry(entries, "a.txt")
	if !ok || a.isDir {
		t.Fatalf("a.txt missing or marked dir: %+v", a)
	}
	if a.size != int64(len(top)) {
		t.Errorf("a.txt size = %d, want %d", a.size, len(top))
	}
	if a.md4 != md4.ContentHash(top) {
		t.Errorf("a.txt md4 = %q, want %q", a.md4, md4.ContentHash(top))
	}

	b, ok := findEntry(entries, "sub/b.txt")
	if !ok || b.isDir {
		t.Fatalf("sub/b.txt missing or marked dir: %+v", b)
	}
	if b.md4 != md4.ContentHash(nested) {
		t.Errorf("sub/b.txt md4 = %q, want %q", b.md4, md4.ContentHash(nested))
	}

	sub, ok := findEntry(entries, "sub")
	if !ok || !sub.isDir {
		t.Fatalf("sub missing or not marked dir: %+v", sub)
	}
	if sub.md4 != "" || sub.size != 0 {
		t.Errorf("dir entry carried md4=%q size=%d, want empty", sub.md4, sub.size)
	}
}

func TestScanViewPermAndMtime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	e, viewID, viewDir := newScanEngine(t)

	p := filepath.Join(viewDir, "exec.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"), 0o750); err != nil {
		t.Fatalf("write exec.sh: %v", err)
	}
	if err := os.Chmod(p, 0o750); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	const mtime = 1_600_000_000
	if err := os.Chtimes(p, time.Unix(mtime, 0), time.Unix(mtime, 0)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, ok := findEntry(entries, "exec.sh")
	if !ok {
		t.Fatalf("exec.sh not scanned")
	}
	if en.unixPerm != 750 {
		t.Errorf("unixPerm = %d, want 750 (wire-decimal of 0o750)", en.unixPerm)
	}
	if en.mtime != mtime {
		t.Errorf("mtime = %d, want %d", en.mtime, mtime)
	}
}

func TestScanViewSkipsSymlink(t *testing.T) {
	requireSymlinks(t)
	e, viewID, viewDir := newScanEngine(t)

	if err := os.WriteFile(filepath.Join(viewDir, "real.txt"), []byte("real"), 0o644); err != nil {
		t.Fatalf("write real.txt: %v", err)
	}
	// A symlink to a file inside the view and a symlink to a directory outside it.
	if err := os.Symlink(filepath.Join(viewDir, "real.txt"), filepath.Join(viewDir, "link.txt")); err != nil {
		t.Fatalf("symlink file: %v", err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "escape")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, "link.txt"); ok {
		t.Error("symlinked file was scanned")
	}
	if _, ok := findEntry(entries, "escape"); ok {
		t.Error("symlinked directory was scanned")
	}
	if _, ok := findEntry(entries, "escape/secret"); ok {
		t.Error("descended into symlinked directory (escaped the sync root)")
	}
	if _, ok := findEntry(entries, "real.txt"); !ok {
		t.Error("real file was not scanned")
	}
}

func TestScanViewSkipsStaging(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	files := map[string][]byte{
		"keep.txt": []byte("keep"),
		// A real staging temp (os.CreateTemp's ".<base>.dl-<digits>") is an engine artefact
		// and must be filtered out.
		".keep.txt.dl-123456": []byte("staging"),
		// A hidden file containing ".dl-" but not the numeric suffix is real content and must
		// still be scanned, not silently dropped.
		".notes.dl-draft": []byte("user draft"),
		// Keep-both conflict-shaped names are NOT name-filtered: a copy the Phase-6d resolver
		// pushes is an ordinary server file, so a name match could only misfire on a legitimate
		// user file - every "_Conflict" shape below, including the full "_<host>_<ts>_Conflict"
		// form, must still be scanned and synced.
		"doc_Conflict.txt":                           []byte("user content"),
		"Report_Conflict_draft.docx":                 []byte("user content"),
		"report_Jun-11-143052-2026_Conflict.txt":     []byte("user content"),
		"doc_myhost_Jun-11-143052-2026_Conflict.txt": []byte("user content"),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(viewDir, name), data, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	// A directory whose name fits the staging shape is NOT an engine artefact - os.CreateTemp
	// only ever creates regular files - so it and its subtree must be scanned normally, not
	// excluded.
	if err := os.MkdirAll(filepath.Join(viewDir, ".cache.dl-123", "inner"), 0o755); err != nil {
		t.Fatalf("mkdir staging-shaped dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, ".cache.dl-123", "inner", "data.bin"), []byte("user data"), 0o644); err != nil {
		t.Fatalf("write inside staging-shaped dir: %v", err)
	}

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	got := make(map[string]bool, len(entries))
	for _, en := range entries {
		got[en.relPath] = true
	}
	want := map[string]bool{
		"keep.txt":                                   true,
		".notes.dl-draft":                            true,
		"doc_Conflict.txt":                           true,
		"Report_Conflict_draft.docx":                 true,
		"report_Jun-11-143052-2026_Conflict.txt":     true,
		"doc_myhost_Jun-11-143052-2026_Conflict.txt": true,
		".cache.dl-123":                              true,
		".cache.dl-123/inner":                        true,
		".cache.dl-123/inner/data.bin":               true,
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d (%v): %+v", len(entries), len(want), want, entries)
	}
	for name := range want {
		if !got[name] {
			t.Errorf("expected %q to be scanned, missing from %v", name, got)
		}
	}
}

// TestScanViewFilteredNamesReportedUnreadable confirms a filtered internal-artifact name
// (a staging temp) is not surfaced as an entry but IS reported in the unreadable set, so
// reconcile treats a present user file sharing that shape as still-present rather than
// deleted.
func TestScanViewFilteredNamesReportedUnreadable(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	for _, name := range []string{"keep.txt", ".keep.txt.dl-99"} {
		if err := os.WriteFile(filepath.Join(viewDir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	_, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if !unreadable[".keep.txt.dl-99"] {
		t.Error("staging temp not reported unreadable")
	}
}

// TestScanViewUnreadableSubtreeDoesNotAbort confirms an EACCES on one subdirectory skips just
// that subtree (recording it in the unreadable set, prefix-protecting its baselines) and lets
// the rest of the view scan succeed, rather than failing the whole view scan.
func TestScanViewUnreadableSubtreeDoesNotAbort(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix directory permission bits are not meaningful on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	e, viewID, viewDir := newScanEngine(t)
	if err := os.WriteFile(filepath.Join(viewDir, "top.txt"), []byte("top"), 0o644); err != nil {
		t.Fatalf("write top.txt: %v", err)
	}
	locked := filepath.Join(viewDir, "locked")
	if err := os.MkdirAll(locked, 0o755); err != nil {
		t.Fatalf("mkdir locked: %v", err)
	}
	if err := os.WriteFile(filepath.Join(locked, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write secret.txt: %v", err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatalf("chmod locked: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) }) // let TempDir cleanup remove it

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView aborted on an unreadable subtree: %v", err)
	}
	if _, ok := findEntry(entries, "top.txt"); !ok {
		t.Error("file outside the locked subtree was not scanned")
	}
	if !unreadable["locked"] {
		t.Error("unreadable subtree not recorded; its baselines could be read as deletions")
	}
}

func TestScanViewEmptyAndMissing(t *testing.T) {
	e, viewID, _ := newScanEngine(t)

	// Empty (but existing) view directory.
	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView empty: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("empty view scanned %d entries, want 0", len(entries))
	}

	// A view whose on-disk directory was never created yields an empty scan, not an error.
	st := e.store
	const missingView = 42
	if err := st.PutView(missingView, store.View{Name: "Never", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	entries, _, err = e.scanView(context.Background(), missingView, nil)
	if err != nil {
		t.Fatalf("scanView missing dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("missing view dir scanned %d entries, want 0", len(entries))
	}
}

func TestScanViewNumericFallbackDir(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	const viewID = 99
	// A view with no name falls back to its numeric id as the on-disk directory.
	if err := st.PutView(viewID, store.View{Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	dir := filepath.Join(root, "99")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if len(entries) != 1 || entries[0].relPath != "f.txt" {
		t.Fatalf("got %+v, want one entry f.txt", entries)
	}
}
