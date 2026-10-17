package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// writable reports whether path's owner-write bit is set. On Windows that bit is
// exactly FILE_ATTRIBUTE_READONLY's inverse (os.Chmod/os.Lstat map the attribute to
// mode 0200), so the same helper pins the attribute there and the mode bits on Unix.
func writable(t *testing.T, path string) bool {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat %q: %v", path, err)
	}
	return fi.Mode().Perm()&0o200 != 0
}

// TestRenameReplaceOverReadOnlyTarget is the red test for the read-only destination wedge: the
// replacing promote of an inbound download whose destination is marked read-only.
// On Windows MoveFileEx(MOVEFILE_REPLACE_EXISTING) fails that move with
// ERROR_ACCESS_DENIED and the attribute never clears itself, so before the fix the
// op re-downloaded the whole body and failed at the rename on every drain forever.
// On Unix a mode-0444 destination is replaced by any rename (the permission comes
// from the parent directory), so this arm pins that the behaviour is unchanged
// there.
func TestRenameReplaceOverReadOnlyTarget(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, ".dl-tmp")
	to := filepath.Join(dir, "tracked.txt")
	if err := os.WriteFile(from, []byte("server bytes"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(to, []byte("base bytes"), 0o644); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	if err := os.Chmod(to, 0o444); err != nil {
		t.Fatalf("mark destination read-only: %v", err)
	}
	if writable(t, to) {
		t.Skip("host does not honour a read-only destination; nothing to pin")
	}

	if err := RenameReplace(from, to); err != nil {
		t.Fatalf("RenameReplace onto a read-only destination: %v", err)
	}
	got, err := os.ReadFile(to)
	if err != nil || string(got) != "server bytes" {
		t.Fatalf("destination = %q (err %v), want the promoted content", got, err)
	}
	if _, err := os.Lstat(from); !os.IsNotExist(err) {
		t.Errorf("source still present after the move, err = %v", err)
	}
	// The promoted file carries the SOURCE's attributes: the engine applies the wire
	// permission afterwards through SetMeta, exactly as for a writable destination.
	if !writable(t, to) {
		t.Errorf("promoted file is still read-only; the staging temp's attributes must survive the move")
	}
}

// TestRenameOverReadOnlyTarget is the same arc for the inbound rename verb, which
// reaches the primitive through fsutil.Rename (destination parent created, both
// directories flushed).
func TestRenameOverReadOnlyTarget(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "moved.txt")
	to := filepath.Join(dir, "sub", "dst.txt")
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatalf("seed destination dir: %v", err)
	}
	if err := os.WriteFile(from, []byte("moved bytes"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(to, []byte("base bytes"), 0o644); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	if err := os.Chmod(to, 0o444); err != nil {
		t.Fatalf("mark destination read-only: %v", err)
	}
	if writable(t, to) {
		t.Skip("host does not honour a read-only destination; nothing to pin")
	}

	if err := Rename(from, to); err != nil {
		t.Fatalf("Rename onto a read-only destination: %v", err)
	}
	got, err := os.ReadFile(to)
	if err != nil || string(got) != "moved bytes" {
		t.Fatalf("destination = %q (err %v), want the moved content", got, err)
	}
}

// TestRenameReplaceMissingSourceUnchanged pins that a failure shape that is NOT the
// read-only one keeps its error verbatim on every platform: an absent source fails
// before any destination attribute is looked at, so no retry (and no attribute
// rewrite) happens.
func TestRenameReplaceMissingSourceUnchanged(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "absent")
	to := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(to, []byte("base bytes"), 0o644); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	if err := os.Chmod(to, 0o444); err != nil {
		t.Fatalf("mark destination read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(to, 0o644) })

	err := RenameReplace(from, to)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("RenameReplace with an absent source = %v, want fs.ErrNotExist", err)
	}
	if runtime.GOOS == "windows" && writable(t, to) {
		t.Errorf("the destination's read-only attribute was cleared for a failure that never reached it")
	}
	got, err := os.ReadFile(to)
	if err != nil || string(got) != "base bytes" {
		t.Fatalf("destination = %q (err %v), want it untouched", got, err)
	}
}
