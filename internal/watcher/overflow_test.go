package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fakeFSWatcher implements fsWatcher with caller-controlled channels so run can be
// driven with synthetic events and errors (notably fsnotify.ErrEventOverflow)
// without a real kernel watch. Add calls are recorded so the watch-cap tests
// can assert exactly which paths were registered.
type fakeFSWatcher struct {
	events chan fsnotify.Event
	errs   chan error
	closed chan struct{}

	mu      sync.Mutex
	added   []string
	dropped map[string]bool
	addErr  map[string]error
}

func newFakeFSWatcher() *fakeFSWatcher {
	return &fakeFSWatcher{
		events: make(chan fsnotify.Event),
		errs:   make(chan error),
		closed: make(chan struct{}),
	}
}

func (f *fakeFSWatcher) Events() <-chan fsnotify.Event { return f.events }
func (f *fakeFSWatcher) Errors() <-chan error          { return f.errs }
func (f *fakeFSWatcher) Add(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, path)
	return f.addErr[path]
}
func (f *fakeFSWatcher) Close() error {
	close(f.closed)
	return nil
}

// addedPaths returns a snapshot of the paths registered via Add.
func (f *fakeFSWatcher) addedPaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.added...)
}

// TestRegisterCreatedDirWalksChildrenAfterParentAddFailure pins the runtime
// recovery path: a transient failure for the newly-created parent must not
// prevent independently watchable descendants from being registered.
func TestRegisterCreatedDirWalksChildrenAfterParentAddFailure(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatal(err)
	}
	fake := newFakeFSWatcher()
	fake.addErr = map[string]error{root: errors.New("transient parent add failure")}
	w := New(root, nil)

	if err := w.registerCreatedDir(fake, root); err != nil {
		t.Fatalf("registerCreatedDir: %v", err)
	}
	if got := fake.addedPaths(); len(got) != 2 || got[0] != root || got[1] != child {
		t.Fatalf("Add calls = %v, want parent attempt followed by child", got)
	}
}

// TestRunOverflowForcesWake confirms that an fsnotify.ErrEventOverflow - events
// dropped, changed path unknown - forces a debounced wake so the engine runs a
// prompt localScan rather than waiting for the periodic catch-up.
func TestRunOverflowForcesWake(t *testing.T) {
	w := New("unused", nil, WithDebounce(testDebounce))
	fake := newFakeFSWatcher()
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.run(ctx, fake, wake)

	fake.errs <- fsnotify.ErrEventOverflow
	waitWake(t, wake, time.Second)
}

// TestRunNonOverflowErrorNoWake confirms a non-overflow fsnotify error is logged
// but does not force a wake (the changed path is unknown only on overflow).
func TestRunNonOverflowErrorNoWake(t *testing.T) {
	w := New("unused", nil, WithDebounce(testDebounce))
	fake := newFakeFSWatcher()
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.run(ctx, fake, wake)

	fake.errs <- errors.New("some transient fsnotify error")
	assertNoWake(t, wake, 4*testDebounce)
}

// TestRunErrorsChannelCloseStops confirms that closing the Errors channel stops
// the loop and closes the wake channel.
func TestRunErrorsChannelCloseStops(t *testing.T) {
	w := New("unused", nil, WithDebounce(testDebounce))
	fake := newFakeFSWatcher()
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.run(ctx, fake, wake)

	close(fake.errs)
	select {
	case _, ok := <-wake:
		if ok {
			t.Fatal("expected wake channel to close, got a wake")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run to stop on closed Errors channel")
	}
	select {
	case <-fake.closed:
	case <-time.After(time.Second):
		t.Fatal("run did not close the fsWatcher on exit")
	}
}

// WatchList reports the paths Add registered and Drop removed, mirroring fsnotify's
// registry: a backend drops a path from it when the directory behind the watch is
// gone, which is the signal recheckRoot reads.
func (f *fakeFSWatcher) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.added))
	for _, p := range f.added {
		if f.dropped[p] || slices.Contains(out, p) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// drop simulates the backend discarding a watch because its directory went away.
func (f *fakeFSWatcher) drop(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropped == nil {
		f.dropped = make(map[string]bool)
	}
	f.dropped[path] = true
}
