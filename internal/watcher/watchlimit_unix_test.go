//go:build !windows

package watcher

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestIsWatchLimitUnix(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("add: %w", syscall.ENOSPC), true},
		{&os.PathError{Op: "open", Path: "/x", Err: syscall.EMFILE}, true},
		{syscall.ENFILE, true},
		{&os.PathError{Op: "open", Path: "/x", Err: syscall.EACCES}, false},
		{os.ErrNotExist, false},
	} {
		if got := isWatchLimit(tc.err); got != tc.want {
			t.Errorf("isWatchLimit(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// TestSystemWatchLimitStopsTheWalkOnce: when the kernel runs out of watches partway through a
// tree, the walk stops at the first refusal and reports the cap - one degradation to the
// periodic scan - instead of trying, failing and logging every remaining directory.
func TestSystemWatchLimitStopsTheWalkOnce(t *testing.T) {
	root := t.TempDir()
	fake := newFakeFSWatcher()
	fake.addErr = map[string]error{}
	for _, d := range []string{"d1", "d2", "d3", "d4", "d5"} {
		p := filepath.Join(root, d)
		if err := os.Mkdir(p, 0o755); err != nil {
			t.Fatal(err)
		}
		fake.addErr[p] = fmt.Errorf("inotify_add_watch: %w", syscall.ENOSPC)
	}
	var logs logBuffer
	w := New(root, slog.New(slog.NewTextHandler(&logs, nil)), WithMaxWatches(0))
	err := w.addSubtree(fake, root)
	if !errors.Is(err, errWatchCap) {
		t.Fatalf("addSubtree = %v, want errWatchCap", err)
	}
	if n := len(fake.addedPaths()); n != 1 {
		t.Fatalf("Add called %d times, want 1: the walk must stop at the first refusal", n)
	}
	if strings.Contains(logs.String(), "add subdir") {
		t.Fatalf("a per-directory Warn was logged for a system limit: %q", logs.String())
	}
}
