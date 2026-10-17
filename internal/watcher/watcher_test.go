package watcher

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

const testDebounce = 40 * time.Millisecond

// waitWake blocks until a wake is received or the timeout elapses, failing the
// test on timeout.
func waitWake(t *testing.T, wake <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-wake:
		if !ok {
			t.Fatal("wake channel closed while waiting for a wake")
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a wake")
	}
}

// assertNoWake fails if a wake arrives within the window.
func assertNoWake(t *testing.T, wake <-chan struct{}, window time.Duration) {
	t.Helper()
	select {
	case _, ok := <-wake:
		if ok {
			t.Fatal("unexpected wake")
		}
	case <-time.After(window):
	}
}

func startWatcher(t *testing.T, root string) (<-chan struct{}, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := New(root, nil, WithDebounce(testDebounce))
	wake, err := w.Watch(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Watch: %v", err)
	}
	return wake, cancel
}

func TestWatchFileCreateWakes(t *testing.T) {
	root := t.TempDir()
	wake, cancel := startWatcher(t, root)
	defer cancel()

	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

func TestWatchBurstCoalescesToOneWake(t *testing.T) {
	root := t.TempDir()
	wake, cancel := startWatcher(t, root)
	defer cancel()

	// A rapid burst within the debounce window must collapse to one wake.
	for i := range 10 {
		p := filepath.Join(root, "f"+string(rune('0'+i)))
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitWake(t, wake, time.Second)
	// No further wake should follow from the same burst.
	assertNoWake(t, wake, 5*testDebounce)
}

func TestWatchModifyAndDeleteWake(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "a.txt")
	if err := os.WriteFile(p, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	wake, cancel := startWatcher(t, root)
	defer cancel()

	if err := os.WriteFile(p, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)

	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

func TestWatchMkdirAndNestedFileWatched(t *testing.T) {
	root := t.TempDir()
	wake, cancel := startWatcher(t, root)
	defer cancel()

	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)

	// A newly-created subdirectory must be watched: a file inside it wakes.
	if err := os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

func TestWatchExistingSubdirWatched(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "pre", "deep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	wake, cancel := startWatcher(t, root)
	defer cancel()

	if err := os.WriteFile(filepath.Join(sub, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

// TestSymlinkedDirNotRegistered pins the no-follow guard on BOTH registration
// paths: the startup walk (addSubtree, whose WalkDir entries are Lstat-based)
// and the runtime Create branch in run (which Lstats rather than Stats). A
// symlink to an out-of-tree directory must never be handed to fsWatcher.Add -
// registering it would watch a subtree outside the sync root, inconsistent with
// the engine's strict symlink refusal along sync paths.
//
// It asserts at the fsWatcher seam rather than end-to-end ("a write behind the
// link does not wake") because that end-to-end property is not this package's
// to keep. fsnotify's kqueue backend (macOS, BSD) opens an fd per entry of a
// watched directory with O_EVTONLY, and open(2) follows a symlink, so it
// registers the symlink's TARGET itself for entries it discovered by listing -
// it only skips that resolution for paths passed to Add ("Follow symlinks, but
// only for paths added with Add()", backend_kqueue.go). A write behind the link
// therefore wakes a macOS watcher no matter what this code does, which is what
// reddened the whole macOS CI job once the host-capability probe replaced the old
// /proc/self gate and let the assertion run on darwin. The spurious wake is
// harmless - the watcher is a latency optimization and the engine re-derives
// the truth by scanning - whereas Add()ing the target would genuinely watch
// out-of-tree. The seam is where the guarantee lives, and asserting there is
// deterministic on every backend instead of true on exactly one.
func TestSymlinkedDirNotRegistered(t *testing.T) {
	requireSymlinks(t)
	// An out-of-tree directory: the target the watcher must never register.
	outside := t.TempDir()

	t.Run("startup walk", func(t *testing.T) {
		root := t.TempDir()
		link := filepath.Join(root, "link")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}
		realDir := filepath.Join(root, "real")
		if err := os.Mkdir(realDir, 0o755); err != nil {
			t.Fatal(err)
		}

		fake := newFakeFSWatcher()
		w := New(root, nil, WithDebounce(testDebounce))
		if err := w.addSubtree(fake, root); err != nil {
			t.Fatalf("addSubtree: %v", err)
		}
		added := fake.addedPaths()
		if slices.Contains(added, link) {
			t.Errorf("addSubtree registered the symlink %q; Add calls = %v", link, added)
		}
		if !slices.Contains(added, realDir) {
			t.Errorf("addSubtree skipped the real subdirectory %q; Add calls = %v", realDir, added)
		}
	})

	t.Run("runtime create", func(t *testing.T) {
		root := t.TempDir()
		link := filepath.Join(root, "link")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}

		fake := newFakeFSWatcher()
		w := New(root, nil, WithDebounce(testDebounce))
		wake := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.run(ctx, fake, wake)

		fake.events <- fsnotify.Event{Name: link, Op: fsnotify.Create}
		// Creating the link IS a change inside the root, so it legitimately wakes;
		// waiting for that wake also orders the assertion after run handled the event.
		waitWake(t, wake, time.Second)
		if added := fake.addedPaths(); slices.Contains(added, link) {
			t.Errorf("run registered the created symlink %q; Add calls = %v", link, added)
		}
	})
}

func TestWatchUnreadableSubtreeSkipped(t *testing.T) {
	if os.Geteuid() == 0 {
		// root bypasses the 0o000 permission, so the walk would not error.
		t.Skip("test relies on permission enforcement; skipped as root")
	}
	root := t.TempDir()
	// A pre-existing subdirectory the walk cannot read (its children are
	// inaccessible). addSubtree must SkipDir on the walk error rather than abort,
	// leaving the rest of the tree watched.
	bad := filepath.Join(root, "bad")
	if err := os.Mkdir(bad, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o755) }) // restore so t.TempDir cleanup can remove it
	good := filepath.Join(root, "good")
	if err := os.Mkdir(good, 0o755); err != nil {
		t.Fatal(err)
	}
	wake, cancel := startWatcher(t, root)
	defer cancel()

	// The watch must still function for a readable sibling subtree.
	if err := os.WriteFile(filepath.Join(good, "x.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitWake(t, wake, time.Second)
}

func TestWatchAllFilteredBurstNoWake(t *testing.T) {
	root := t.TempDir()
	wake, cancel := startWatcher(t, root)
	defer cancel()

	// A burst consisting solely of noise (staging temps, conflict copies, temp
	// suffixes) must not produce a wake.
	for _, name := range []string{
		".report.docx.dl-123456789",
		"report_host_Jun-12-150405-2006_Conflict.docx",
		"draft.tmp",
		"notes.txt~",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	assertNoWake(t, wake, 5*testDebounce)
}

func TestWatchMixedBurstWakesOnce(t *testing.T) {
	root := t.TempDir()
	wake, cancel := startWatcher(t, root)
	defer cancel()

	// A mix of noise and a real user file must wake exactly once.
	for _, name := range []string{
		".data.dl-42",
		"draft.tmp",
		"real.txt", // the one event that should survive the filter
		"movie.part",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitWake(t, wake, time.Second)
	assertNoWake(t, wake, 5*testDebounce)
}

func TestWatchContextCancelClosesChannel(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	w := New(root, nil, WithDebounce(testDebounce))
	wake, err := w.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	cancel()
	select {
	case _, ok := <-wake:
		if ok {
			// A pending wake may drain first; the channel must then close.
			select {
			case _, ok := <-wake:
				if ok {
					t.Fatal("expected channel close after cancel")
				}
			case <-time.After(time.Second):
				t.Fatal("channel not closed after cancel")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}
}

func TestWatchMissingRootErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := New(filepath.Join(t.TempDir(), "does-not-exist"), nil, WithDebounce(testDebounce))
	if _, err := w.Watch(ctx); err == nil {
		t.Fatal("expected error watching a missing root")
	}
}

func TestNewNilLoggerSafe(t *testing.T) {
	// New with a nil logger must not panic when events are logged.
	w := New(t.TempDir(), nil)
	if w.log == nil {
		t.Fatal("expected a non-nil discarding logger")
	}
	if w.debounce != defaultDebounce {
		t.Fatalf("expected default debounce, got %v", w.debounce)
	}
}
