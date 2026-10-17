package watcher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newRootWatcher returns a Watcher over a fresh root plus a fake backend that already
// holds the root watch, the state Watch leaves behind.
func newRootWatcher(t *testing.T, buf *bytes.Buffer) (*Watcher, *fakeFSWatcher, string) {
	t.Helper()
	root := t.TempDir()
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(root, log)
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("prime root watch: %v", err)
	}
	return w, fake, root
}

// TestRecheckRootStillWatchedIsSilent: the common case must touch nothing - no Add, no
// wake, no log.
func TestRecheckRootStillWatchedIsSilent(t *testing.T) {
	var buf bytes.Buffer
	w, fake, root := newRootWatcher(t, &buf)
	before := len(fake.addedPaths())

	if w.recheckRoot(fake) {
		t.Fatal("recheckRoot forced a wake while the root was still watched")
	}
	if got := fake.addedPaths(); len(got) != before {
		t.Fatalf("Add calls = %v, want no new registration for %q", got, root)
	}
	if buf.Len() != 0 {
		t.Fatalf("unexpected log output: %q", buf.String())
	}
}

// TestRecheckRootReregistersDroppedRoot is the regression this mechanism exists for.
// Deleting the sync root drops its watch, and the replacement is watched by nobody: the
// root's parent lies outside the tree and is never watched, so no Create event for the
// new root can arrive. Measured on Windows, fsnotify emits a Remove for the root and
// empties WatchList, and a file written under the recreated root produces no event at
// all. Before this the watcher stayed silent for the rest of the daemon's life.
func TestRecheckRootReregistersDroppedRoot(t *testing.T) {
	var buf bytes.Buffer
	w, fake, root := newRootWatcher(t, &buf)
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatalf("mkdir child: %v", err)
	}
	fake.drop(root)

	if !w.recheckRoot(fake) {
		t.Fatal("recheckRoot did not force a wake after the root watch was dropped")
	}
	got := fake.addedPaths()
	if len(got) < 3 || got[len(got)-2] != root || got[len(got)-1] != child {
		t.Fatalf("Add calls = %v, want the root re-added followed by its subtree", got)
	}
	if !strings.Contains(buf.String(), "sync root's watch was dropped") {
		t.Fatalf("expected a re-registration Warn, got log: %q", buf.String())
	}
}

// TestRecheckRootAbsentRootWarnsOnceAndRecovers: while the root does not exist Add
// fails every tick, so the Warn must fire once per outage and no wake may be forced -
// there is nothing under the root to rescan. When the root returns, the next tick
// re-registers it and wakes the engine.
func TestRecheckRootAbsentRootWarnsOnceAndRecovers(t *testing.T) {
	var buf bytes.Buffer
	w, fake, root := newRootWatcher(t, &buf)
	fake.drop(root)
	fake.mu.Lock()
	fake.addErr = map[string]error{root: errors.New("no such directory")}
	fake.mu.Unlock()

	if w.recheckRoot(fake) {
		t.Fatal("recheckRoot forced a wake while the root could not be re-added")
	}
	if !strings.Contains(buf.String(), "cannot be re-registered") {
		t.Fatalf("expected an outage Warn, got log: %q", buf.String())
	}
	buf.Reset()
	if w.recheckRoot(fake) {
		t.Fatal("recheckRoot forced a wake on the second failing tick")
	}
	if buf.Len() != 0 {
		t.Fatalf("outage Warn repeated: %q", buf.String())
	}

	fake.mu.Lock()
	fake.addErr = nil
	fake.mu.Unlock()
	if !w.recheckRoot(fake) {
		t.Fatal("recheckRoot did not re-register the returned root")
	}
	if !strings.Contains(buf.String(), "sync root's watch was dropped") {
		t.Fatalf("expected a re-registration Warn once the root returned, got log: %q", buf.String())
	}
}

// TestWatchRecoversFromReplacedRoot drives the whole loop over a real fsnotify watcher:
// after the root is deleted and recreated, a file created under it must still produce a
// wake. Without the recheck this times out, because the only watch died with the
// original directory.
func TestWatchRecoversFromReplacedRoot(t *testing.T) {
	root := t.TempDir()
	w := New(root, nil, WithDebounce(testDebounce), WithRootRecheck(20*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := os.Remove(root); err != nil {
		t.Skipf("host will not let a watched root be removed: %v", err)
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("recreate root: %v", err)
	}
	// The re-registration itself forces a wake, since the tree changed while the
	// watcher was blind; drain that before testing the restored watch.
	waitWake(t, wake, 3*time.Second)

	if err := os.WriteFile(filepath.Join(root, "after.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitWake(t, wake, 3*time.Second)
}
