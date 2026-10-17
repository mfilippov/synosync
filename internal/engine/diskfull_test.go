package engine

// Tests for the per-OS disk-full classifier. The `Disk: full` latch used
// to test errors.Is(err, syscall.ENOSPC) directly, which made it dead code on Windows (where
// syscall.ENOSPC is a synthetic value no Win32 call returns) and blind to an exhausted quota
// on Unix (EDQUOT). Both are silent losses of operability: the ops correctly stay queued and
// retry, but `status` never names the reason, so the operator debugs a phantom stall.
//
// The host-native code tables live in diskfull_{windows,other}_test.go next to the
// production lists they pin, so each is compiled on its own platform.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// hostErrCase is one named host errno for the table-driven classifier tests.
type hostErrCase struct {
	name string
	err  error
}

// TestHostDiskFullCodesAreClassified is the classifier half of the red test: every
// host-native out-of-space code must be recognised bare, wrapped with %w, and inside the
// *fs.PathError the os package actually hands back from a failed write.
func TestHostDiskFullCodesAreClassified(t *testing.T) {
	for _, tc := range hostDiskFullErrs {
		t.Run(tc.name, func(t *testing.T) {
			forms := []struct {
				shape string
				err   error
			}{
				{"bare", tc.err},
				{"wrapped", fmt.Errorf("engine: apply download: %w", tc.err)},
				{"PathError", &fs.PathError{Op: "write", Path: "a.txt", Err: tc.err}},
				{"wrapped PathError", fmt.Errorf("write body: %w",
					&fs.PathError{Op: "write", Path: "a.txt", Err: tc.err})},
			}
			for _, f := range forms {
				if !isDiskFull(f.err) {
					t.Errorf("isDiskFull(%s: %v) = false, want the disk-full latch to fire", f.shape, f.err)
				}
			}
		})
	}
}

// TestNonDiskFullErrorsAreNotClassified is the conservatism guard: the latch must stay
// silent for neighbouring host errnos and for ordinary errors, or `Disk: full` becomes noise
// that tells the operator nothing.
func TestNonDiskFullErrorsAreNotClassified(t *testing.T) {
	cases := append([]hostErrCase{
		{"nil", nil},
		{"plain", errors.New("engine: apply: server rejected the push")},
		{"wrapped plain", fmt.Errorf("outer: %w", errors.New("inner"))},
		{"context canceled", context.Canceled},
	}, hostNotDiskFullErrs...)
	for _, tc := range cases {
		if isDiskFull(tc.err) {
			t.Errorf("isDiskFull(%s: %v) = true, want it NOT classified as disk-full", tc.name, tc.err)
		}
	}
}

// TestHostDiskFullCodesDoNotMatchENOSPCAlone pins the reason the fix is needed at all: on
// every platform there is at least one genuine out-of-space code that the old
// errors.Is(err, syscall.ENOSPC) predicate does not match (all of them on Windows, EDQUOT on
// Unix). If this ever starts failing, the host errno bridge changed and the extra per-OS
// lists can be revisited.
func TestHostDiskFullCodesDoNotMatchENOSPCAlone(t *testing.T) {
	if errors.Is(hostNativeDiskFullErr, syscall.ENOSPC) {
		t.Fatalf("hostNativeDiskFullErr (%v) matches syscall.ENOSPC: pick a code the old predicate missed", hostNativeDiskFullErr)
	}
	// ...and it is nonetheless the same operability condition.
	if !isDiskFull(hostNativeDiskFullErr) {
		t.Fatalf("isDiskFull(%v) = false, want the host-native disk-full code classified", hostNativeDiskFullErr)
	}
}

// TestDiskFullLatchesOnHostNativeCode is the engine half of the red test: an apply that
// fails with the HOST's own out-of-space code (not the injected synthetic ENOSPC the older
// tests use) must latch DiskFull(), log the Warn, leave the op queued rather than
// dead-lettered - and clear the latch once the apply succeeds. Pre-fix the flag never lit on
// Windows at all, and never lit for a quota-full Unix volume.
func TestDiskFullLatchesOnHostNativeCode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("hello")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 5}},
		dlErr: fmt.Errorf("write body: %w",
			&fs.PathError{Op: "write", Path: "a.txt", Err: hostNativeDiskFullErr}),
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	e := New(st, &concTransport{client: dl}, root, "sess", log)
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the host disk-full apply error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the disk-full op still queued for retry (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want a disk-full op NOT dead-lettered (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatalf("DiskFull() false after an apply failing with %v, want it latched", hostNativeDiskFullErr)
	}
	if logs := logbuf.String(); !strings.Contains(logs, "disk full") {
		t.Errorf("no disk-full Warn logged:\n%s", logs)
	}

	// Free the "disk": the next drain applies the op and clears the latch, so the operator
	// is not left staring at a stale `Disk: full`.
	dl.mu.Lock()
	dl.dlErr = nil
	dl.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "a.txt")); err != nil {
		t.Errorf("file not materialised after retry: %v", err)
	}
}
