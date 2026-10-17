//go:build windows

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

// swapRenameFn installs a move primitive for the duration of the test.
func swapRenameFn(t *testing.T, fn func(from, to string) error) {
	t.Helper()
	orig := renameFn
	renameFn = fn
	t.Cleanup(func() { renameFn = orig })
}

// TestRenameReplaceIsWriteThrough is the write-through pin for the promote's
// other half: the replacing move must be a MoveFileEx carrying
// MOVEFILE_WRITE_THROUGH, not the stdlib's plain MOVEFILE_REPLACE_EXISTING. Without
// it a power cut in the window right after the promote returned could leave the OLD
// file in place while the store already recorded the downloaded content as agreed,
// and the next scan pushed those stale bytes over the server's newer version. The
// seam delegates to the real MoveFileEx, so the move still has to land.
func TestRenameReplaceIsWriteThrough(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src.txt")
	to := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(to, []byte("base"), 0o644); err != nil {
		t.Fatalf("seed destination: %v", err)
	}

	var seen []uint32
	orig := moveFileEx
	moveFileEx = func(f, tp *uint16, flags uint32) error {
		seen = append(seen, flags)
		return orig(f, tp, flags)
	}
	t.Cleanup(func() { moveFileEx = orig })

	if err := RenameReplace(from, to); err != nil {
		t.Fatalf("RenameReplace: %v", err)
	}
	want := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if len(seen) != 1 || seen[0] != want {
		t.Fatalf("MoveFileEx flags = %#x, want exactly one call with %#x", seen, want)
	}
	got, err := os.ReadFile(to)
	if err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q (err %v), want the promoted payload", got, err)
	}
	if _, err := os.Lstat(from); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("source survived the move: err = %v", err)
	}
}

// TestRenameReplaceMapsFailureToLinkError pins that the direct Win32 call keeps the
// *os.LinkError shape os.Rename produced, so every existing classification of a
// rename failure (permissionBlockedByDirTarget, the read-only retry's own
// fs.ErrPermission gate) keeps matching verbatim.
func TestRenameReplaceMapsFailureToLinkError(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "absent.txt")
	to := filepath.Join(dir, "dst.txt")

	err := RenameReplace(from, to)
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) {
		t.Fatalf("RenameReplace error = %T (%v), want *os.LinkError", err, err)
	}
	if linkErr.Old != from || linkErr.New != to {
		t.Errorf("LinkError = %+v, want the operand paths preserved", linkErr)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("RenameReplace with an absent source = %v, want fs.ErrNotExist", err)
	}
}

// TestRenameReplaceKeepsGenuinePermissionError pins that the retry does not widen the
// classifier: a destination held by a DIRECTORY fails MoveFileEx with
// ERROR_ACCESS_DENIED too, but carries no read-only attribute, so the original
// permission error reaches the executor and permissionBlockedByDirTarget still
// dead-letters the op instead of it silently becoming something else.
func TestRenameReplaceKeepsGenuinePermissionError(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src.txt")
	to := filepath.Join(dir, "occupant")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.Mkdir(to, 0o755); err != nil {
		t.Fatalf("seed directory occupant: %v", err)
	}

	err := RenameReplace(from, to)
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("RenameReplace onto a directory occupant = %v, want fs.ErrPermission", err)
	}
	if fi, serr := os.Lstat(to); serr != nil || !fi.IsDir() {
		t.Errorf("occupant = %v (err %v), want the directory untouched", fi, serr)
	}
	if _, serr := os.Lstat(from); serr != nil {
		t.Errorf("source must survive a failed move: %v", serr)
	}
}

// TestRenameReplaceRestoresAttrsWhenRetryFails covers the arc where the attribute was
// the wrong diagnosis: the retry fails anyway, so the destination's attributes go back
// and the caller sees the FIRST error - the one the destination actually produced.
func TestRenameReplaceRestoresAttrsWhenRetryFails(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "src.txt")
	to := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(to, []byte("base"), 0o644); err != nil {
		t.Fatalf("seed destination: %v", err)
	}
	if err := os.Chmod(to, 0o444); err != nil {
		t.Fatalf("mark destination read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(to, 0o644) })

	first := &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.ERROR_ACCESS_DENIED}
	second := &os.LinkError{Op: "rename", Old: from, New: to, Err: windows.ERROR_SHARING_VIOLATION}
	calls := 0
	swapRenameFn(t, func(string, string) error {
		calls++
		if calls == 1 {
			return first
		}
		return second
	})

	err := RenameReplace(from, to)
	if !errors.Is(err, first) {
		t.Fatalf("RenameReplace = %v, want the first (destination-produced) error %v", err, first)
	}
	if calls != 2 {
		t.Fatalf("move attempts = %d, want exactly 2 (one retry after clearing the attribute)", calls)
	}
	if writable(t, to) {
		t.Errorf("destination is writable; a retry that failed must put the read-only attribute back")
	}
}

// TestClearReadOnlyAttrArms pins the narrow shapes the clear applies to: only a
// regular file that actually carries the attribute. A writable file and a directory
// (the permanently-blocked shape) are left alone and report no clear, so their errors
// keep their existing classification.
func TestClearReadOnlyAttrArms(t *testing.T) {
	dir := t.TempDir()
	readOnly := filepath.Join(dir, "ro.txt")
	writableFile := filepath.Join(dir, "rw.txt")
	subdir := filepath.Join(dir, "sub")
	for _, p := range []string{readOnly, writableFile} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %q: %v", p, err)
		}
	}
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("seed subdir: %v", err)
	}
	if err := os.Chmod(readOnly, 0o444); err != nil {
		t.Fatalf("mark read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o644) })

	attrs, cleared := clearReadOnlyAttr(readOnly)
	if !cleared {
		t.Fatalf("clearReadOnlyAttr(read-only file) reported no clear")
	}
	if attrs&windows.FILE_ATTRIBUTE_READONLY == 0 {
		t.Errorf("reported attributes %#x do not carry FILE_ATTRIBUTE_READONLY", attrs)
	}
	if !writable(t, readOnly) {
		t.Errorf("file is still read-only after the clear")
	}
	// The reported attributes are exactly what restores the original state.
	restoreFileAttrs(readOnly, attrs)
	if writable(t, readOnly) {
		t.Errorf("restoreFileAttrs did not put the read-only attribute back")
	}

	if _, cleared := clearReadOnlyAttr(writableFile); cleared {
		t.Errorf("clearReadOnlyAttr(writable file) reported a clear")
	}
	if _, cleared := clearReadOnlyAttr(subdir); cleared {
		t.Errorf("clearReadOnlyAttr(directory) reported a clear; that shape is dead-lettered, not retried")
	}
	if _, cleared := clearReadOnlyAttr(filepath.Join(dir, "absent")); cleared {
		t.Errorf("clearReadOnlyAttr(absent path) reported a clear")
	}
}
