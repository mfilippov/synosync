//go:build !windows

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestFsyncUnsupportedClassifier pins the exact errno set that degrades a
// directory fsync to non-durable (EINVAL/ENOTSUP/EOPNOTSUPP/ENOSYS - a
// deliberate decision): a genuine I/O failure must never be swallowed.
func TestFsyncUnsupportedClassifier(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"EINVAL", syscall.EINVAL, true},
		{"ENOTSUP", syscall.ENOTSUP, true},
		{"EOPNOTSUPP", syscall.EOPNOTSUPP, true},
		{"ENOSYS", syscall.ENOSYS, true},
		{"wrapped EINVAL", &fs.PathError{Op: "sync", Path: "d", Err: syscall.EINVAL}, true},
		{"wrapped ENOTSUP", &fs.PathError{Op: "sync", Path: "d", Err: syscall.ENOTSUP}, true},
		{"wrapped EOPNOTSUPP", &fs.PathError{Op: "sync", Path: "d", Err: syscall.EOPNOTSUPP}, true},
		{"EIO", syscall.EIO, false},
		{"EBADF", syscall.EBADF, false},
		{"ENOSPC", syscall.ENOSPC, false},
		{"generic error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fsyncUnsupported(tt.err); got != tt.want {
				t.Fatalf("fsyncUnsupported(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// swapDirFsync injects a directory-handle fsync result and captures the
// degraded-durability warning, restoring both seams and the once-guard on
// cleanup. It returns a counter of warnings fired.
func swapDirFsync(t *testing.T, syncErr error) *int {
	t.Helper()
	origSync := syncFileHandle
	t.Cleanup(func() { syncFileHandle = origSync })
	syncFileHandle = func(*os.File) error { return syncErr }
	return captureFsyncWarns(t)
}

// TestSyncDirDegradesOnUnsupportedFsync reproduces the defect: on a
// filesystem whose directory fsync fails EINVAL (CIFS/SMB, several FUSE),
// SyncDir must degrade to non-durable success with exactly one Warn per
// process - before the fix every apply failed forever as a transient retry.
func TestSyncDirDegradesOnUnsupportedFsync(t *testing.T) {
	dir := t.TempDir()
	warns := swapDirFsync(t, &fs.PathError{Op: "sync", Path: dir, Err: syscall.EINVAL})

	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir under EINVAL dir-fsync: %v, want degrade-to-nil", err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir (second call): %v", err)
	}
	if *warns != 1 {
		t.Fatalf("degradation warnings = %d, want exactly 1 per process", *warns)
	}
}

// TestSyncDirRealFsyncErrorStillFails pins the other half of the classifier:
// an errno outside the unsupported set (EIO) propagates as before and fires no
// degradation warning.
func TestSyncDirRealFsyncErrorStillFails(t *testing.T) {
	dir := t.TempDir()
	warns := swapDirFsync(t, &fs.PathError{Op: "sync", Path: dir, Err: syscall.EIO})

	err := SyncDir(dir)
	if err == nil {
		t.Fatal("SyncDir under EIO dir-fsync: nil, want error")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("SyncDir error = %v, want EIO preserved", err)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 for a real I/O failure", *warns)
	}
}

// TestSyncDirDegradesOnUnreadableDir reproduces the unreadable-directory defect
// with no seam on the failure itself: POSIX permits a rename INTO a
// write+search-only (0300) directory, but the read-only open a directory fsync
// requires fails EACCES there, so before the fix the disk mutation succeeded
// while SyncDir failed - every apply into such a directory retried forever as a
// transient. It must now degrade to non-durable success with exactly one Warn
// per process, and the real inbound-mutation helper (Rename into the 0300
// directory) must complete end to end.
func TestSyncDirDegradesOnUnreadableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "writeonly")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) }) // let TempDir cleanup remove it

	warns := captureFsyncWarns(t)

	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatalf("Chmod 0300: %v", err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir on 0300 dir: %v, want degrade-to-nil", err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir (second call): %v", err)
	}

	src := filepath.Join(root, "f.txt")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := Rename(src, filepath.Join(dir, "f.txt")); err != nil {
		t.Fatalf("Rename into 0300 dir: %v, want degraded success", err)
	}

	if *warns != 1 {
		t.Fatalf("degradation warnings = %d, want exactly 1 per process", *warns)
	}
}

// TestSyncDirOpenRealErrorStillFails pins the non-permission half of the open
// stage: a genuine open failure (here ENOENT on a missing directory) still
// propagates and fires no degradation warning.
func TestSyncDirOpenRealErrorStillFails(t *testing.T) {
	warns := captureFsyncWarns(t)

	err := SyncDir(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("SyncDir on missing dir: nil, want error")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncDir error = %v, want not-exist preserved", err)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 for a real open failure", *warns)
	}
}

// TestApplyHelpersSucceedUnderUnsupportedFsync drives the real apply helpers
// (Mkdir/Rename/Remove) with the injected EINVAL directory fsync: each must
// complete its on-disk mutation and return nil, since the flush is the only
// degraded part.
func TestApplyHelpersSucceedUnderUnsupportedFsync(t *testing.T) {
	root := t.TempDir()
	swapDirFsync(t, &fs.PathError{Op: "sync", Path: root, Err: syscall.ENOTSUP})

	nested := filepath.Join(root, "a", "b")
	if err := Mkdir(nested); err != nil {
		t.Fatalf("Mkdir under ENOTSUP dir-fsync: %v", err)
	}

	src := filepath.Join(nested, "f.txt")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	dst := filepath.Join(root, "moved.txt")
	if err := Rename(src, dst); err != nil {
		t.Fatalf("Rename under ENOTSUP dir-fsync: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("Stat renamed destination: %v", err)
	}

	if err := Remove(dst); err != nil {
		t.Fatalf("Remove under ENOTSUP dir-fsync: %v", err)
	}
	if _, err := os.Lstat(dst); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Lstat removed path: err = %v, want not-exist", err)
	}
}
