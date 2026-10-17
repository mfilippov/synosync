//go:build windows

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// fakeDirHandle is the handle value the seam-injected opens hand out. It is never
// passed to a real Win32 call: every test that produces it also swaps the flush
// and close seams.
const fakeDirHandle = windows.Handle(0x5A5A)

// swapDirHandleSeams installs open/flush/close seams for the duration of the test.
// A nil argument leaves that seam alone, so a test can inject only the stage it
// cares about and let the real one run for the rest.
func swapDirHandleSeams(t *testing.T,
	open func(string, uint32) (windows.Handle, error),
	flush func(windows.Handle) error,
	closeH func(windows.Handle) error,
) {
	t.Helper()
	origOpen, origFlush, origClose := createDirHandle, flushDirHandle, closeDirHandle
	t.Cleanup(func() {
		createDirHandle, flushDirHandle, closeDirHandle = origOpen, origFlush, origClose
	})
	if open != nil {
		createDirHandle = open
	}
	if flush != nil {
		flushDirHandle = flush
	}
	if closeH != nil {
		closeDirHandle = closeH
	}
}

// TestSyncDirFlushesRealDirectory is the red test for the Windows directory flush: fsyncDir
// on Windows was a no-op justified by the claim that FlushFileBuffers rejects a
// directory handle. It does not - this drives the real CreateFile +
// FlushFileBuffers against a real NTFS directory and pins that the flush both
// fires and succeeds, with no degradation warning. Before the fix the call count
// was zero and the whole directory-flush discipline of DESIGN §7 bought nothing on Windows.
func TestSyncDirFlushesRealDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "entry.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	warns := captureFsyncWarns(t)
	flushes := 0
	swapDirHandleSeams(t, nil, func(h windows.Handle) error {
		flushes++
		return windows.FlushFileBuffers(h)
	}, nil)

	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir on a real directory: %v, want success", err)
	}
	if flushes != 1 {
		t.Fatalf("FlushFileBuffers calls = %d, want exactly 1 (a no-op would be 0)", flushes)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 - the host flushed the handle fine", *warns)
	}
}

// TestApplyHelpersFlushRealDirectories drives the real apply primitives end to
// end with the real flush in place: each directory mutation must succeed AND
// actually reach FlushFileBuffers, which is the guarantee the store's
// StateSynced baseline is written against.
func TestApplyHelpersFlushRealDirectories(t *testing.T) {
	root := t.TempDir()
	flushes := 0
	swapDirHandleSeams(t, nil, func(h windows.Handle) error {
		flushes++
		return windows.FlushFileBuffers(h)
	}, nil)

	nested := filepath.Join(root, "a", "b")
	if err := Mkdir(nested); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	src := filepath.Join(nested, "f.txt")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	dst := filepath.Join(root, "moved.txt")
	if err := Rename(src, dst); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if err := Remove(dst); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	// Two created ancestors, both rename parents, one remove parent - the exact
	// count is the helpers' business; what this pins is that it is not zero.
	if flushes < 5 {
		t.Fatalf("FlushFileBuffers calls = %d, want one per mutated directory entry", flushes)
	}
}

// TestSyncDirFallsBackToReadHandle pins the access-mask fallback: a directory
// whose ACL refuses a GENERIC_WRITE open is retried GENERIC_READ rather than
// failing, because the mkdir/rename this flush follows was accepted by that same
// directory and giving up would leave every apply into it retrying forever.
func TestSyncDirFallsBackToReadHandle(t *testing.T) {
	warns := captureFsyncWarns(t)
	var masks []uint32
	swapDirHandleSeams(t,
		func(_ string, access uint32) (windows.Handle, error) {
			masks = append(masks, access)
			if access == windows.GENERIC_WRITE {
				return windows.InvalidHandle, windows.ERROR_ACCESS_DENIED
			}
			return fakeDirHandle, nil
		},
		func(windows.Handle) error { return nil },
		func(windows.Handle) error { return nil },
	)

	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatalf("SyncDir with a write-denied directory: %v, want the read-handle fallback to succeed", err)
	}
	want := []uint32{windows.GENERIC_WRITE, windows.GENERIC_READ}
	if len(masks) != len(want) || masks[0] != want[0] || masks[1] != want[1] {
		t.Fatalf("open access masks = %#x, want %#x", masks, want)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 - the fallback flushed", *warns)
	}
}

// TestSyncDirDegradesOnDeniedOpen covers the capability gap with no handle at
// all: both opens are refused on permissions, so the flush degrades to
// non-durable success with exactly one Warn per process - the Windows twin of
// the Unix 0300-directory arc.
func TestSyncDirDegradesOnDeniedOpen(t *testing.T) {
	warns := captureFsyncWarns(t)
	opens := 0
	swapDirHandleSeams(t,
		func(string, uint32) (windows.Handle, error) {
			opens++
			return windows.InvalidHandle, windows.ERROR_ACCESS_DENIED
		}, nil, nil)

	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir under a denied open: %v, want degrade-to-nil", err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir (second call): %v", err)
	}
	if opens != 4 {
		t.Fatalf("open attempts = %d, want 4 (write+read fallback per call)", opens)
	}
	if *warns != 1 {
		t.Fatalf("degradation warnings = %d, want exactly 1 per process", *warns)
	}
}

// TestSyncDirDegradesOnUnsupportedFlush covers the other capability gap: the
// handle opens but the filesystem cannot flush a directory (what a redirector or
// a user-mode filesystem answers). One Warn, non-durable success - never a
// forever-transient apply.
func TestSyncDirDegradesOnUnsupportedFlush(t *testing.T) {
	warns := captureFsyncWarns(t)
	closes := 0
	swapDirHandleSeams(t,
		func(string, uint32) (windows.Handle, error) { return fakeDirHandle, nil },
		func(windows.Handle) error { return windows.ERROR_INVALID_FUNCTION },
		func(windows.Handle) error { closes++; return nil },
	)

	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir under an unsupported flush: %v, want degrade-to-nil", err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir (second call): %v", err)
	}
	if closes != 2 {
		t.Fatalf("handle closes = %d, want 2 - a degraded flush must not leak the handle", closes)
	}
	if *warns != 1 {
		t.Fatalf("degradation warnings = %d, want exactly 1 per process", *warns)
	}
}

// TestSyncDirFallsBackToReadHandleOnUnsupportedWriteOpen is the red test for the
// open stage's capability gap: a redirector that will not hand out a WRITABLE
// directory handle answers ERROR_INVALID_PARAMETER, not ERROR_ACCESS_DENIED. Before
// the fix that skipped the GENERIC_READ fallback entirely and failed SyncDir, so
// every fsutil.Mkdir/Rename/Remove on such a mount retried forever. The read handle
// flushes fine here, so the call must succeed outright with no degradation.
func TestSyncDirFallsBackToReadHandleOnUnsupportedWriteOpen(t *testing.T) {
	warns := captureFsyncWarns(t)
	var masks []uint32
	swapDirHandleSeams(t,
		func(_ string, access uint32) (windows.Handle, error) {
			masks = append(masks, access)
			if access == windows.GENERIC_WRITE {
				return windows.InvalidHandle, windows.ERROR_INVALID_PARAMETER
			}
			return fakeDirHandle, nil
		},
		func(windows.Handle) error { return nil },
		func(windows.Handle) error { return nil },
	)

	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatalf("SyncDir with a write open refused as unsupported: %v, want the read-handle fallback to succeed", err)
	}
	want := []uint32{windows.GENERIC_WRITE, windows.GENERIC_READ}
	if len(masks) != len(want) || masks[0] != want[0] || masks[1] != want[1] {
		t.Fatalf("open access masks = %#x, want %#x", masks, want)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 - the fallback flushed", *warns)
	}
}

// TestSyncDirDegradesOnUnsupportedOpen covers the same gap with no handle at all:
// a user-mode filesystem that refuses BOTH access masks with a capability-gap code.
// The mkdir/rename this flush follows already landed, so the only non-wedging
// answer is the same degrade-to-nil the flush stage gives - one Warn per process,
// never a propagated error the executor would retry every drain.
func TestSyncDirDegradesOnUnsupportedOpen(t *testing.T) {
	for _, code := range []windows.Errno{
		windows.ERROR_INVALID_PARAMETER,
		windows.ERROR_NOT_SUPPORTED,
		windows.ERROR_INVALID_FUNCTION,
		windows.ERROR_CALL_NOT_IMPLEMENTED,
	} {
		t.Run(code.Error(), func(t *testing.T) {
			warns := captureFsyncWarns(t)
			opens := 0
			swapDirHandleSeams(t,
				func(string, uint32) (windows.Handle, error) {
					opens++
					return windows.InvalidHandle, code
				}, nil, nil)

			if err := SyncDir(t.TempDir()); err != nil {
				t.Fatalf("SyncDir under an unsupported open (%v): %v, want degrade-to-nil", code, err)
			}
			if opens != 2 {
				t.Fatalf("open attempts = %d, want 2 (write + read fallback)", opens)
			}
			if *warns != 1 {
				t.Fatalf("degradation warnings = %d, want exactly 1", *warns)
			}
		})
	}
}

// TestSyncDirRealOpenErrorStillFails pins the other half of the open classifier: a
// genuine device failure is not a capability gap, so it propagates and the caller's
// op retries rather than recording a durability it never got.
func TestSyncDirRealOpenErrorStillFails(t *testing.T) {
	warns := captureFsyncWarns(t)
	opens := 0
	swapDirHandleSeams(t,
		func(string, uint32) (windows.Handle, error) {
			opens++
			return windows.InvalidHandle, windows.ERROR_IO_DEVICE
		}, nil, nil)

	err := SyncDir(t.TempDir())
	if !errors.Is(err, windows.ERROR_IO_DEVICE) {
		t.Fatalf("SyncDir under an I/O open failure = %v, want ERROR_IO_DEVICE preserved", err)
	}
	if opens != 1 {
		t.Fatalf("open attempts = %d, want 1 - an I/O failure must not retry the fallback", opens)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 for a real I/O failure", *warns)
	}
}

// TestSyncDirRealFlushErrorStillFails pins the other half of the classifier: a
// genuine I/O failure propagates as before and fires no degradation warning, so
// the caller's op retries instead of silently recording a durability it did not
// get.
func TestSyncDirRealFlushErrorStillFails(t *testing.T) {
	warns := captureFsyncWarns(t)
	closes := 0
	swapDirHandleSeams(t,
		func(string, uint32) (windows.Handle, error) { return fakeDirHandle, nil },
		func(windows.Handle) error { return windows.ERROR_IO_DEVICE },
		func(windows.Handle) error { closes++; return nil },
	)

	err := SyncDir(t.TempDir())
	if !errors.Is(err, windows.ERROR_IO_DEVICE) {
		t.Fatalf("SyncDir under an I/O failure = %v, want ERROR_IO_DEVICE preserved", err)
	}
	if closes != 1 {
		t.Fatalf("handle closes = %d, want 1", closes)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 for a real I/O failure", *warns)
	}
}

// TestSyncDirMissingDirStillFails pins the open stage's non-permission half
// against the real filesystem: a missing directory must surface as a not-exist
// error, which is what fsutil.Remove keys its idempotent "parent already gone"
// tolerance on.
func TestSyncDirMissingDirStillFails(t *testing.T) {
	warns := captureFsyncWarns(t)

	err := SyncDir(filepath.Join(t.TempDir(), "missing"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncDir on a missing dir = %v, want not-exist preserved", err)
	}
	if *warns != 0 {
		t.Fatalf("degradation warnings = %d, want 0 for a real open failure", *warns)
	}
}

// TestRemoveToleratesMissingParentOnWindows is the end-to-end consequence of the
// test above: an idempotent remove whose whole subtree is already gone must not
// fail on the parent flush.
func TestRemoveToleratesMissingParentOnWindows(t *testing.T) {
	gone := filepath.Join(t.TempDir(), "gone", "child.txt")
	if err := Remove(gone); err != nil {
		t.Fatalf("Remove of an already-absent path under an absent parent: %v", err)
	}
}

// TestFsyncUnsupportedClassifierWindows pins the exact error set that degrades a
// directory flush to non-durable: the refusals a filesystem answers with, never a
// genuine I/O failure.
func TestFsyncUnsupportedClassifierWindows(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ERROR_INVALID_FUNCTION", windows.ERROR_INVALID_FUNCTION, true},
		{"ERROR_NOT_SUPPORTED", windows.ERROR_NOT_SUPPORTED, true},
		{"ERROR_INVALID_PARAMETER", windows.ERROR_INVALID_PARAMETER, true},
		{"ERROR_ACCESS_DENIED", windows.ERROR_ACCESS_DENIED, true},
		{"ERROR_CALL_NOT_IMPLEMENTED", windows.ERROR_CALL_NOT_IMPLEMENTED, true},
		{"wrapped ERROR_NOT_SUPPORTED", &fs.PathError{Op: "sync", Path: "d", Err: windows.ERROR_NOT_SUPPORTED}, true},
		{"ERROR_IO_DEVICE", windows.ERROR_IO_DEVICE, false},
		{"ERROR_DISK_FULL", windows.ERROR_DISK_FULL, false},
		{"ERROR_WRITE_FAULT", windows.ERROR_WRITE_FAULT, false},
		{"ERROR_INVALID_HANDLE", windows.ERROR_INVALID_HANDLE, false},
		{"generic error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fsyncUnsupported(tt.err); got != tt.want {
				t.Fatalf("fsyncUnsupported(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
