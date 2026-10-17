package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameNoReplaceMoves(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src")
	to := filepath.Join(dir, "sub", "dst") // parent does not exist yet
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := RenameNoReplace(from, to); err != nil {
		t.Fatalf("RenameNoReplace: %v", err)
	}
	// Source is gone, destination holds the content (parent created).
	if _, err := os.Lstat(from); !os.IsNotExist(err) {
		t.Errorf("source still present, err = %v", err)
	}
	got, err := os.ReadFile(to)
	if err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q (err %v), want payload", got, err)
	}
}

// TestRenameNoReplaceSyncsEveryCreatedAncestor confirms RenameNoReplace creates
// the destination parent chain durably (mkdirAllDurable): the parent of every
// intermediate directory it creates is fsynced, plus the destination's immediate
// parent after the move - matching Rename/Mkdir so a crash cannot lose a freshly
// created intermediate directory entry.
func TestRenameNoReplaceSyncsEveryCreatedAncestor(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	// dir exists; "a" and "a/b" are created by this one RenameNoReplace, and the
	// move lands at "a/b/dst".
	to := filepath.Join(dir, "a", "b", "dst")
	if err := RenameNoReplace(from, to); err != nil {
		t.Fatalf("RenameNoReplace: %v", err)
	}
	// Each created directory's parent must have been fsynced through the seam:
	// dir (parent of a) and dir/a (parent of b). The destination's own parent
	// (dir/a/b) and the source directory are flushed after the move too
	// (syncRenameDirs), but this test pins only the created-ancestor flushes.
	for _, want := range []string{dir, filepath.Join(dir, "a")} {
		found := false
		for _, c := range *calls {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("SyncDir not called for %q; calls = %v", want, *calls)
		}
	}
}

// TestRenameNoReplaceCrossDirSyncsBothParents confirms a cross-directory
// RenameNoReplace fsyncs BOTH affected directories through the seam - the
// destination's parent (new entry) first, then the source's parent (removed
// entry) - mirroring Rename.
func TestRenameNoReplaceCrossDirSyncsBothParents(t *testing.T) {
	dir := t.TempDir()
	fromDir := filepath.Join(dir, "a")
	toDir := filepath.Join(dir, "b")
	for _, d := range []string{fromDir, toDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed dir: %v", err)
		}
	}
	from := filepath.Join(fromDir, "f.txt")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	if err := RenameNoReplace(from, filepath.Join(toDir, "f.txt")); err != nil {
		t.Fatalf("RenameNoReplace: %v", err)
	}
	// The destination parent pre-exists, so mkdirAllDurable creates nothing but still
	// re-flushes ITS parent; then the post-rename pair follows,
	// destination first, then source.
	if len(*calls) != 3 || (*calls)[0] != dir || (*calls)[1] != toDir || (*calls)[2] != fromDir {
		t.Fatalf("SyncDir calls = %v, want [%q %q %q]", *calls, dir, toDir, fromDir)
	}
}

// TestRenameNoReplaceSameDirSyncsOnce confirms a same-directory RenameNoReplace
// flushes the shared parent exactly once post-rename (the destination parent's own
// parent is flushed first by mkdirAllDurable's unconditional
// re-flush).
func TestRenameNoReplaceSameDirSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "old.txt")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	if err := RenameNoReplace(from, filepath.Join(dir, "new.txt")); err != nil {
		t.Fatalf("RenameNoReplace: %v", err)
	}
	if len(*calls) != 2 || (*calls)[0] != filepath.Dir(dir) || (*calls)[1] != dir {
		t.Fatalf("SyncDir calls = %v, want exactly [%q %q]", *calls, filepath.Dir(dir), dir)
	}
}

func TestRenameNoReplaceRefusesExisting(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src")
	to := filepath.Join(dir, "dst")
	if err := os.WriteFile(from, []byte("new"), 0o644); err != nil {
		t.Fatalf("seed src: %v", err)
	}
	if err := os.WriteFile(to, []byte("keep"), 0o644); err != nil {
		t.Fatalf("seed dst: %v", err)
	}

	err := RenameNoReplace(from, to)
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("RenameNoReplace err = %v, want fs.ErrExist", err)
	}
	// The existing destination is untouched and the source is left in place.
	if got, _ := os.ReadFile(to); string(got) != "keep" {
		t.Errorf("destination clobbered = %q, want keep", got)
	}
	if got, _ := os.ReadFile(from); string(got) != "new" {
		t.Errorf("source = %q, want untouched new", got)
	}
}

// TestRenameNoReplaceGenericFallback exercises the portable Lstat-guarded path directly
// (used when the platform/filesystem lacks an atomic no-replace rename primitive).
func TestRenameNoReplaceGenericFallback(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src")
	to := filepath.Join(dir, "dst")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Free destination: the move succeeds.
	if err := renameNoReplaceGeneric(from, to); err != nil {
		t.Fatalf("renameNoReplaceGeneric move: %v", err)
	}
	if got, err := os.ReadFile(to); err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q (err %v), want payload", got, err)
	}

	// Existing destination: refused with fs.ErrExist, leaving it intact.
	if err := os.WriteFile(from, []byte("again"), 0o644); err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if err := renameNoReplaceGeneric(from, to); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("renameNoReplaceGeneric existing dst err = %v, want fs.ErrExist", err)
	}
	if got, _ := os.ReadFile(to); string(got) != "payload" {
		t.Errorf("destination clobbered = %q, want payload", got)
	}
}

func TestRenameNoReplaceMissingSource(t *testing.T) {
	dir := t.TempDir()
	err := RenameNoReplace(filepath.Join(dir, "nope"), filepath.Join(dir, "dst"))
	if err == nil {
		t.Fatal("RenameNoReplace of missing source: want error, got nil")
	}
	if errors.Is(err, fs.ErrExist) {
		t.Errorf("err = %v, want a not-exist style error, not fs.ErrExist", err)
	}
}

func TestRenameNoReplaceParentUncreatable(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A regular file stands where the destination's parent directory would need
	// to be created, so MkdirAll fails and RenameNoReplace reports the parent-prep
	// error rather than attempting (or succeeding at) the move.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("y"), 0o644); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	err := RenameNoReplace(from, filepath.Join(blocker, "sub", "dst"))
	if err == nil {
		t.Fatal("RenameNoReplace under a file parent: want error, got nil")
	}
	if errors.Is(err, fs.ErrExist) {
		t.Errorf("err = %v, want a parent-prep error, not fs.ErrExist", err)
	}
	if _, serr := os.Stat(from); serr != nil {
		t.Errorf("source should be untouched after a failed move, stat err = %v", serr)
	}
}
