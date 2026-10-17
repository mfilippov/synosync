package watcher

import (
	"os"
	"path/filepath"
	"testing"
)

// requireSymlinks skips unless the host can create a symlink, mirroring the engine package's
// probe (internal/engine/hostcaps_test.go).
//
// The gate it replaces was an Lstat of "/proc/self" - a LINUX-only artefact, so the no-follow
// watcher guard was skipped on darwin too, leaving it exercised on exactly one of the three
// fsnotify backends. Probing the behaviour the fixture actually needs runs
// it on every host that provides it (darwin unconditionally, Windows with the symlink privilege)
// and skips with a concrete reason where it does not.
func requireSymlinks(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "target"), filepath.Join(dir, "link")); err != nil {
		t.Skipf("symlinks unavailable on this host: %v", err)
	}
}
