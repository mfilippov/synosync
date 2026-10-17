package watcher

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestWatchBudgetChargesPerEntryNotPerDirectory pins what the cap actually measures.
//
// The cap exists to keep the process clear of kern.maxfilesperproc on macOS, where fsnotify's
// kqueue backend opens one descriptor per watched directory AND one per entry inside it
// (backend_kqueue.go: watchDirectoryFiles registers every os.ReadDir entry, files included).
// Counting directories measured the wrong resource by the tree's fan-out: ONE directory holding
// thousands of files passed a 4096-directory cap with a single registration while opening
// thousands of descriptors. The degradation the cap exists to trigger - errWatchCap, which
// sends the engine to its periodic scan - was unreachable, because Add fails with EMFILE long
// before any directory count approaches the limit.
//
// A flat directory of files is therefore the shape to assert: under the old accounting its cost
// was 1.
func TestWatchBudgetChargesPerEntryNotPerDirectory(t *testing.T) {
	root := t.TempDir()
	const files = 40
	for i := 0; i < files; i++ {
		p := filepath.Join(root, "f"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if got, want := watchCost(root), files+1; got != want {
		t.Fatalf("watchCost(a directory of %d files) = %d, want %d (the directory plus every entry)",
			files, got, want)
	}

	// And the budget refuses it: one flat directory alone overruns a budget that would have
	// admitted 4095 more directories under the old unit.
	w := New(root, nil, WithDebounce(testDebounce), WithMaxWatches(files/2))
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err == nil {
		t.Fatalf("addWatch charged %d against a budget of %d without refusing it",
			w.watchCount, files/2)
	}
	if len(fake.addedPaths()) != 0 {
		t.Errorf("the refused directory was registered anyway: %v", fake.addedPaths())
	}
}

// TestWatchCostOfUnreadableDirectoryIsOne pins the fallback: a directory whose entries cannot
// be listed is charged the minimum rather than refused, because addSubtree deliberately logs
// and skips an unreadable subtree instead of failing the whole watch.
func TestWatchCostOfUnreadableDirectoryIsOne(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if got := watchCost(missing); got != 1 {
		t.Fatalf("watchCost(unreadable) = %d, want 1", got)
	}
}
