package watcher

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// logBuffer is a mutex-guarded writer so the run goroutine's slog output can be
// read from the test goroutine without a race.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWatchCapExceededAtStartupErrors confirms that a tree too large for the
// watch cap fails Watch outright - the engine's SetWatcher failure path then
// falls back to the periodic scan - rather than silently watching an arbitrary
// subset of the tree.
func TestWatchCapExceededAtStartupErrors(t *testing.T) {
	skipOnSubtreeBackend(t)
	root := t.TempDir()
	for _, d := range []string{"d1", "d2", "d3", "d4", "d5"} {
		if err := os.Mkdir(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Budget 8: the root costs 1 + its 5 entries = 6, then each empty subdir costs 1,
	// so two of them fit and the third exceeds.
	w := New(root, nil, WithDebounce(testDebounce), WithMaxWatches(8))
	if _, err := w.Watch(ctx); err == nil {
		t.Fatal("expected Watch to fail when the startup tree exceeds the watch cap")
	}
}

// TestWatchCapSufficientStartupSucceeds pins the negative side: a tree that
// fits the cap exactly starts and functions normally.
func TestWatchCapSufficientStartupSucceeds(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "d1")
	if err := os.MkdirAll(filepath.Join(root, "d2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Budget 5 = the root (1 + its 2 entries) + the two empty subdirectories, exactly.
	w := New(root, nil, WithDebounce(testDebounce), WithMaxWatches(5))
	wake, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

// TestRunCapExceededRuntimeWarnsOnceAndKeepsWaking confirms the runtime policy
// on the plan's 3-cap/5-dir tree: directories created past the cap are not
// registered, the saturation is warned exactly once, existing watches keep
// working, and no wake is lost.
func TestRunCapExceededRuntimeWarnsOnceAndKeepsWaking(t *testing.T) {
	root := t.TempDir()
	var dirs []string
	for _, d := range []string{"d1", "d2", "d3", "d4", "d5"} {
		p := filepath.Join(root, d)
		if err := os.Mkdir(p, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, p)
	}
	lb := &logBuffer{}
	// Budget 8 = the root (1 + its 5 entries) + two empty subdirectories.
	w := New(root, slog.New(slog.NewTextHandler(lb, nil)),
		WithDebounce(testDebounce), WithMaxWatches(8))
	fake := newFakeFSWatcher()
	// Seed the root watch as Watch would, leaving room for two more.
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("addWatch(root): %v", err)
	}
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.run(ctx, fake, wake)

	// d1, d2 fit under the cap; d3, d4, d5 exceed it.
	for _, p := range dirs {
		fake.events <- fsnotify.Event{Name: p, Op: fsnotify.Create}
	}
	waitWake(t, wake, time.Second)

	want := []string{root, dirs[0], dirs[1]}
	if got := fake.addedPaths(); !slices.Equal(got, want) {
		t.Fatalf("registered watches = %v, want %v", got, want)
	}
	if n := strings.Count(lb.String(), "watch cap exceeded"); n != 1 {
		t.Fatalf("cap Warn logged %d times, want exactly 1\nlog:\n%s", n, lb.String())
	}

	// The loop must stay functional after saturation: a plain file event on a
	// still-watched path wakes again.
	fake.events <- fsnotify.Event{Name: filepath.Join(dirs[0], "x.txt"), Op: fsnotify.Write}
	waitWake(t, wake, time.Second)
}

// TestRunCapExceededMidSubtreeKeepsParentWatch confirms that when the cap bites
// while registering a created directory's descendants, the already-added parent
// watch is kept, the saturation is warned, and the wake still fires.
func TestRunCapExceededMidSubtreeKeepsParentWatch(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	child := filepath.Join(parent, "sub")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	lb := &logBuffer{}
	// Budget 4 = the root (1 + its one entry) + parent (1 + its one entry).
	w := New(root, slog.New(slog.NewTextHandler(lb, nil)),
		WithDebounce(testDebounce), WithMaxWatches(4))
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("addWatch(root): %v", err)
	}
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.run(ctx, fake, wake)

	// parent fits (root 1+1, parent 1+1 = 4/4); its pre-populated child exceeds mid-walk.
	fake.events <- fsnotify.Event{Name: parent, Op: fsnotify.Create}
	waitWake(t, wake, time.Second)

	want := []string{root, parent}
	if got := fake.addedPaths(); !slices.Equal(got, want) {
		t.Fatalf("registered watches = %v, want %v", got, want)
	}
	if n := strings.Count(lb.String(), "watch cap exceeded"); n != 1 {
		t.Fatalf("cap Warn logged %d times, want exactly 1\nlog:\n%s", n, lb.String())
	}
}

// TestNestedCreateBurstCountsDistinctDirs pins the fix for the ~D²/2 counter
// inflation: registerCreatedDir re-walks a created directory's subtree, so a
// mkdir -p burst delivered as a Create-chain (a, a/b, a/b/c after all three
// exist) revisits descendants an earlier Create already registered. The watch
// counter must equal the number of DISTINCT directories registered (one fd
// each), not the number of Add calls - otherwise a deep tree hits the darwin
// cap far below its real fd use. The Create-chain is driven synchronously (no
// run goroutine) so w.watchCount can be read without a race.
func TestNestedCreateBurstCountsDistinctDirs(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	ab := filepath.Join(a, "b")
	abc := filepath.Join(ab, "c")
	if err := os.MkdirAll(abc, 0o755); err != nil {
		t.Fatal(err)
	}

	// Budget exactly at the chain's real cost: root(1+1) + a(1+1) + a/b(1+1) + a/b/c(1+0)
	// = 7. The re-walk revisits a/b and a/b/c, which must consume no additional budget
	// (idempotent Add, no new fd) - under the double-counting bug the re-visits push the
	// charge past the budget and registerCreatedDir returns errWatchCap.
	w := New(root, nil, WithDebounce(testDebounce), WithMaxWatches(7))
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("addWatch(root): %v", err)
	}
	for _, dir := range []string{a, ab, abc} {
		if err := w.registerCreatedDir(fake, dir); err != nil {
			t.Fatalf("registerCreatedDir(%s): %v - re-registering a watched dir must not exceed the cap", dir, err)
		}
	}

	if w.watchCount != 7 {
		t.Fatalf("watchCount = %d, want 7: each of root,a,a/b,a/b/c charged exactly once", w.watchCount)
	}
	if len(w.watched) != 4 {
		t.Fatalf("len(watched) = %d, want 4 distinct paths", len(w.watched))
	}
	for _, p := range []string{root, a, ab, abc} {
		if _, ok := w.watched[p]; !ok {
			t.Fatalf("watched set missing %q", p)
		}
	}
}

// TestUncappedPlatformDoesNotRetainWatchedSet pins the fix for the
// unbounded growth of w.watched on uncapped platforms (Linux/Windows,
// maxWatches == 0): the distinct-path set exists only to serve the cap, so when
// the cap is disabled the set must NOT be maintained at all - otherwise it
// accumulates the union of every directory ever created under the root over the
// daemon's lifetime (fsnotify drops the kernel watch on delete, but the map key
// would survive; the run loop only handles Create, so nothing prunes it), a slow
// memory leak. Registrations still reach the underlying watcher; only the
// bookkeeping map stays empty. Driven synchronously (no run goroutine) so the
// unlocked fields can be read without a race.
func TestUncappedPlatformDoesNotRetainWatchedSet(t *testing.T) {
	root := t.TempDir()
	// maxWatches == 0 => uncapped, mirroring the non-darwin defaultMaxWatches.
	w := New(root, nil, WithMaxWatches(0))
	if w.maxWatches != 0 {
		t.Fatalf("maxWatches = %d, want 0 for this test", w.maxWatches)
	}
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("addWatch(root): %v", err)
	}
	// Simulate a churn of many uniquely-named created directories. Under the
	// pre-fix code each unique path inserted a surviving map key.
	const churn = 100
	for i := range churn {
		dir := filepath.Join(root, fmt.Sprintf("churn-%d", i))
		if err := w.addWatch(fake, dir); err != nil {
			t.Fatalf("addWatch(%s): %v", dir, err)
		}
	}
	if len(w.watched) != 0 {
		t.Fatalf("len(watched) = %d on an uncapped platform, want 0 (the set must not be retained without a cap)", len(w.watched))
	}
	if w.watchCount != 0 {
		t.Fatalf("watchCount = %d on an uncapped platform, want 0", w.watchCount)
	}
	// Every registration still reached the underlying watcher (root + churn).
	if got := len(fake.addedPaths()); got != churn+1 {
		t.Fatalf("Add calls = %d, want %d (root + churn dirs still registered)", got, churn+1)
	}
}

// TestCappedPlatformRetainsWatchedSetForDedup is the positive counterpart: when
// the cap IS active the distinct-path set is still maintained so a re-Add of an
// already-watched directory dedups, and the counter
// tracks distinct registrations. Re-adding a watched path must not grow either.
func TestCappedPlatformRetainsWatchedSetForDedup(t *testing.T) {
	root := t.TempDir()
	w := New(root, nil, WithMaxWatches(4))
	fake := newFakeFSWatcher()
	if err := w.addWatch(fake, root); err != nil {
		t.Fatalf("addWatch(root): %v", err)
	}
	a := filepath.Join(root, "a")
	if err := w.addWatch(fake, a); err != nil {
		t.Fatalf("addWatch(a): %v", err)
	}
	// Re-adding an already-watched path re-issues the idempotent Add but must not
	// grow the set or the counter.
	if err := w.addWatch(fake, a); err != nil {
		t.Fatalf("addWatch(a) re-add: %v", err)
	}
	if len(w.watched) != 2 {
		t.Fatalf("len(watched) = %d, want 2 (root, a) - a re-Add must not grow the set", len(w.watched))
	}
	if w.watchCount != 2 {
		t.Fatalf("watchCount = %d, want 2 distinct registrations", w.watchCount)
	}
	// The re-Add still reached the underlying watcher (root, a, a again).
	if got := fake.addedPaths(); !slices.Equal(got, []string{root, a, a}) {
		t.Fatalf("Add calls = %v, want [root a a] (idempotent refresh re-issued)", got)
	}
}

// TestDefaultMaxWatchesPerGOOS pins the decided policy: a per-GOOS constant
// (darwin capped for kqueue fd use, everything else unlimited), overridable via
// the WithMaxWatches Option - no config key.
func TestDefaultMaxWatchesPerGOOS(t *testing.T) {
	w := New(t.TempDir(), nil)
	if w.maxWatches != defaultMaxWatches() {
		t.Fatalf("maxWatches = %d, want per-GOOS default %d", w.maxWatches, defaultMaxWatches())
	}
	// The override applies, and a negative value is ignored.
	w = New(t.TempDir(), nil, WithMaxWatches(7))
	if w.maxWatches != 7 {
		t.Fatalf("maxWatches = %d, want 7", w.maxWatches)
	}
	w = New(t.TempDir(), nil, WithMaxWatches(-1))
	if w.maxWatches != defaultMaxWatches() {
		t.Fatalf("maxWatches = %d after negative override, want default %d", w.maxWatches, defaultMaxWatches())
	}
}
