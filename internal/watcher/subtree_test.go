package watcher

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// hostWatchesSubtrees probes whether this host's backend covers a whole tree with one watch.
func hostWatchesSubtrees(t *testing.T) bool {
	t.Helper()
	fsw, recursive, err := newFSWatcher()
	if err != nil {
		t.Fatalf("newFSWatcher: %v", err)
	}
	_ = fsw.Close()
	return recursive
}

// skipOnSubtreeBackend skips a test about per-directory registration (the descriptor cap and
// its bookkeeping) on a host whose backend registers the root alone.
func skipOnSubtreeBackend(t *testing.T) {
	t.Helper()
	if hostWatchesSubtrees(t) {
		t.Skip("this host's backend watches the whole tree with one handle; there is no per-directory registration to cap")
	}
}

// TestSubtreeBackendWatchesTheTreeWithOneWatch: where the backend can watch a subtree, the
// whole tree costs ONE registration - a per-directory watch cost ~64 KiB of buffer each on
// Windows, 338 MB on a 5.5k-folder view - and a change deep in a directory that existed
// before the watch started still wakes the loop.
func TestSubtreeBackendWatchesTheTreeWithOneWatch(t *testing.T) {
	if !hostWatchesSubtrees(t) {
		t.Skip("this host's backend watches one directory per registration")
	}
	root := t.TempDir()
	deep := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	fsw, _, err := newFSWatcher()
	if err != nil {
		t.Fatal(err)
	}
	w := New(root, nil, WithDebounce(testDebounce))
	w.recursive = true
	if err := w.addWatch(fsw, root); err != nil {
		t.Fatalf("add root: %v", err)
	}
	if err := w.addSubtree(fsw, root); err != nil {
		t.Fatalf("addSubtree: %v", err)
	}
	if got := fsw.WatchList(); !slices.Equal(got, []string{root}) {
		t.Fatalf("WatchList = %q, want just the root", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	go w.run(ctx, fsw, wake)
	if err := os.WriteFile(filepath.Join(deep, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("no wake for a write three directories below the root")
	}
}
