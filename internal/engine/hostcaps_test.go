package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// Host-capability probes for fixtures whose premise is a filesystem behaviour the
// host may not provide. Each probes the real behaviour instead of branching on
// runtime.GOOS, so the test runs wherever the behaviour actually holds (Windows
// with the symlink privilege granted, a non-root Unix user) and skips with a
// concrete reason where it does not - rather than failing on the release
// platform for a reason that says nothing about the code under test.

// requireSymlinks skips unless the host can create a symlink. On Windows that
// needs the SeCreateSymbolicLinkPrivilege (developer mode or an elevated token),
// which an ordinary session does not hold.
func requireSymlinks(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "target"), filepath.Join(dir, "link")); err != nil {
		t.Skipf("symlinks unavailable on this host: %v", err)
	}
}

// requireUnreadableDir skips unless the host denies reads of a chmod-000
// directory. Windows ignores the POSIX mode bits (ACLs are the real mechanism)
// and root bypasses the check on Unix, so the fixture would otherwise stop
// exercising the walk-error branch it targets.
func requireUnreadableDir(t *testing.T) {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(probe, 0o700); err != nil {
		t.Fatalf("probe mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(probe, 0o700) })
	if err := os.Chmod(probe, 0o000); err != nil {
		t.Skipf("chmod-based read denial unavailable here: %v", err)
	}
	if _, err := os.ReadDir(probe); err == nil {
		t.Skip("host does not enforce chmod-based directory read denial (Windows, or running as root)")
	}
}

// requireUnreadableFile is requireUnreadableDir's file counterpart: it skips
// unless opening a chmod-000 regular file fails.
func requireUnreadableFile(t *testing.T) {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "locked.txt")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatalf("probe write: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(probe, 0o600) })
	if err := os.Chmod(probe, 0o000); err != nil {
		t.Skipf("chmod-based read denial unavailable here: %v", err)
	}
	f, err := os.Open(probe)
	if err == nil {
		_ = f.Close()
		t.Skip("host does not enforce chmod-based file read denial (Windows, or running as root)")
	}
}

// requireReadOnlyDir skips unless the host refuses a file creation inside a
// chmod-0500 directory - the fault injection the "transient apply failure"
// fixtures rely on. Windows does not honour the write bit for a directory.
func requireReadOnlyDir(t *testing.T) {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(probe, 0o500); err != nil {
		t.Fatalf("probe mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(probe, 0o700) })
	f, err := os.Create(filepath.Join(probe, "probe.txt"))
	if err == nil {
		_ = f.Close()
		t.Skip("host does not enforce a read-only directory mode (Windows, or running as root)")
	}
}
