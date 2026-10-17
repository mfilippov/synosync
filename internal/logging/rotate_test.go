package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRotatingWriterDefaults(t *testing.T) {
	dir := t.TempDir()
	w, err := NewRotatingWriter(filepath.Join(dir, "synosync.log"), RotateOptions{})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()
	if w.maxBytes != DefaultMaxBytes {
		t.Errorf("maxBytes = %d, want %d", w.maxBytes, DefaultMaxBytes)
	}
	if w.maxBackups != DefaultMaxBackups {
		t.Errorf("maxBackups = %d, want %d", w.maxBackups, DefaultMaxBackups)
	}
}

func TestRotatingWriterCreatesDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "logs", "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("log file not created: %v", err)
	}
}

func TestRotatingWriterNoRotateBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 100, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	for range 5 {
		if _, err := w.Write([]byte("0123456789")); err != nil { // 10 bytes each, 50 total < 100
			t.Fatalf("Write: %v", err)
		}
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("unexpected rotation: .1 exists (err=%v)", err)
	}
}

func TestRotatingWriterRotatesOnceAboveThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 20, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	// First write fills to threshold; second write triggers exactly one rotation.
	if _, err := w.Write([]byte("01234567890123456789AB")); err != nil { // 22 bytes >= 20
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected .1 backup after rotation: %v", err)
	}
	if _, err := os.Stat(path + ".2"); !os.IsNotExist(err) {
		t.Fatalf("unexpected .2 backup (only one rotation expected): %v", err)
	}
	active, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	if string(active) != "second" {
		t.Errorf("active file = %q, want %q", active, "second")
	}
}

func TestRotatingWriterShiftsAndDropsOldest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 5, MaxBackups: 2})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	// Each write is >= threshold so the *next* write rotates. Write distinct
	// markers so we can track where each lands.
	markers := []string{"AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"}
	for _, m := range markers {
		if _, err := w.Write([]byte(m)); err != nil {
			t.Fatalf("Write %s: %v", m, err)
		}
	}

	// After 4 writes with MaxBackups=2:
	// rotations happen before writes 2,3,4.
	// active = DDDDDD, .1 = CCCCCC, .2 = BBBBBB, AAAAAA dropped.
	checks := map[string]string{
		path:        "DDDDDD",
		path + ".1": "CCCCCC",
		path + ".2": "BBBBBB",
	}
	for p, want := range checks {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", p, got, want)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf(".3 should not exist (MaxBackups=2): %v", err)
	}
}

func TestRotatingWriterReopenAppends(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")

	w1, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 1000})
	if err != nil {
		t.Fatalf("NewRotatingWriter 1: %v", err)
	}
	if _, err := w1.Write([]byte("first\n")); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	w2, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 1000})
	if err != nil {
		t.Fatalf("NewRotatingWriter 2: %v", err)
	}
	defer w2.Close()
	if _, err := w2.Write([]byte("second\n")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "first\nsecond\n" {
		t.Errorf("content = %q, want %q", got, "first\nsecond\n")
	}
}

func TestRotatingWriterWriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := NewRotatingWriter(filepath.Join(dir, "synosync.log"), RotateOptions{})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("expected error writing to closed writer")
	}
	// Close again is a no-op.
	if err := w.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestRotatingWriterReopensAfterRotateFailure forces the active-file rename in
// rotate() to fail and asserts the writer recovers: it best-effort reopens the
// active file so a subsequent Write still lands rather than bricking the sink
// forever with a nil handle. The record still lands, but because the size bound
// was not honoured (the active file was not shifted aside) the failure is now
// surfaced rather than swallowed - Write returns the rotation error joined with
// the write result.
func TestRotatingWriterReopensAfterRotateFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 8, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	// Fill the active file past the threshold so the next write triggers a rotate.
	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatalf("initial Write: %v", err)
	}

	// Force the active-file rename to fail; backup shifts (different names) still
	// succeed so only the active->.1 move is broken.
	restoreRename := osRename
	osRename = func(oldpath, newpath string) error {
		if newpath == path+".1" {
			return fmt.Errorf("injected rename failure")
		}
		return os.Rename(oldpath, newpath)
	}
	defer func() { osRename = restoreRename }()

	// This write triggers a rotation that fails on the active-file rename. The
	// writer must reopen the active file and still append the record, but the
	// degraded rotation must now be surfaced as a non-nil error (previously it
	// was swallowed, so an unbounded active file grew with zero observability).
	n, err := w.Write([]byte("recovered"))
	if err == nil {
		t.Fatal("Write after failed rotate returned nil error; degraded rotation must be surfaced")
	}
	if n != len("recovered") {
		t.Fatalf("short write: n = %d, want %d", n, len("recovered"))
	}
	if w.f == nil {
		t.Fatal("writer left bricked with nil file handle after recoverable rotate failure")
	}

	// The rename failed, so the active file was never moved aside; "recovered"
	// must have been appended to it rather than dropped.
	if data, err := os.ReadFile(path); err != nil {
		t.Fatalf("ReadFile after failed rotate: %v", err)
	} else if !strings.Contains(string(data), "recovered") {
		t.Errorf("active file missing %q after failed rotate; got %q", "recovered", data)
	}

	// Restore rename so subsequent writes behave normally, and confirm the writer
	// keeps working (here the now-oversized active file rotates cleanly).
	osRename = restoreRename
	if _, err := w.Write([]byte("again")); err != nil {
		t.Fatalf("Write after recovery: %v", err)
	}
}

// TestRotatingWriterCloseFailureDoesNotBrick closes the OS handle out from
// under the writer so rotate()'s pre-shift Close fails, and asserts the
// rotation still proceeds. Before the fix rotate() early-returned on the Close
// error with the dead handle still installed, so every subsequent write
// repeated rotate→Close(ErrClosed)→write-to-closed-handle forever.
func TestRotatingWriterCloseFailureDoesNotBrick(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 8, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	// Fill past the threshold so the next write triggers a rotation.
	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatalf("initial Write: %v", err)
	}

	// Sabotage the handle: rotate()'s Close will now fail with ErrClosed.
	if err := w.f.Close(); err != nil {
		t.Fatalf("pre-close underlying handle: %v", err)
	}

	// The rotation's Close fails, but the shift+reopen must still run and the
	// record must land in the fresh active file.
	if _, err := w.Write([]byte("after")); err != nil {
		t.Fatalf("Write after Close failure: %v", err)
	}
	if backup, err := os.ReadFile(path + ".1"); err != nil {
		t.Fatalf("read .1 backup (rotation should have completed): %v", err)
	} else if string(backup) != "0123456789" {
		t.Errorf(".1 backup = %q, want %q", backup, "0123456789")
	}
	if active, err := os.ReadFile(path); err != nil {
		t.Fatalf("read active: %v", err)
	} else if string(active) != "after" {
		t.Errorf("active file = %q, want %q", active, "after")
	}

	// And the writer keeps working (this write rotates again, cleanly).
	if _, err := w.Write([]byte("more")); err != nil {
		t.Fatalf("Write after recovery: %v", err)
	}
}

// TestRotatingWriterWriteHealsAfterTotalRotateFailure drives the writer into
// the worst state - both the backup shift and the best-effort reopen fail, so
// no active file is open - then repairs the failure and asserts the next Write
// self-heals by reopening. Before the fix Write returned "write to closed
// RotatingWriter" forever once w.f was nil.
func TestRotatingWriterWriteHealsAfterTotalRotateFailure(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	path := filepath.Join(logDir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 8, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatalf("initial Write: %v", err)
	}

	// Replace the log directory with a regular file: the backup shift then fails
	// (ENOTDIR on Unix, path-not-found on Windows) and so does the reopen's
	// MkdirAll - the transient total failure (an NFS blip, EMFILE, ENOSPC) in a
	// reproducible form. The sabotage is injected through the rename seam rather
	// than performed up front because rotate() closes the active handle before it
	// shifts the chain: on Windows a still-open handle blocks the removal of its
	// parent directory (a sharing violation), so an up-front RemoveAll would fail
	// the test on the release platform.
	restoreRename := osRename
	squatted := false
	osRename = func(oldpath, newpath string) error {
		if !squatted {
			if err := os.RemoveAll(logDir); err != nil {
				return err
			}
			if err := os.WriteFile(logDir, []byte("squatter"), 0o600); err != nil {
				return err
			}
			squatted = true
		}
		return os.Rename(oldpath, newpath)
	}
	defer func() { osRename = restoreRename }()

	if _, err := w.Write([]byte("lost")); err == nil {
		t.Fatal("expected error while the log directory is unavailable")
	}
	if w.f != nil {
		t.Fatal("w.f != nil after total rotate failure; test setup did not reach the broken state")
	}

	// The broken state persists while the failure does.
	if _, err := w.Write([]byte("still lost")); err == nil {
		t.Fatal("expected error on subsequent write while still broken")
	}

	// Repair the directory: the next write must reopen and land.
	osRename = restoreRename
	if !squatted {
		t.Fatal("rename seam never fired; the squatter was never installed")
	}
	if err := os.Remove(logDir); err != nil {
		t.Fatalf("remove squatter: %v", err)
	}
	if _, err := w.Write([]byte("healed")); err != nil {
		t.Fatalf("Write after repair: %v", err)
	}
	if active, err := os.ReadFile(path); err != nil {
		t.Fatalf("read active after heal: %v", err)
	} else if string(active) != "healed" {
		t.Errorf("active file = %q, want %q", active, "healed")
	}
}

// TestRotatingWriterSurfacesPermanentRotateFailure drives a permanently failing
// backup shift and asserts the fix: each over-threshold record still lands (the
// active file grows), but the degraded rotation is observable - Write returns a
// non-nil error and warnRotateDegraded fires exactly once per failure streak
// (not on every over-threshold write). When the obstruction clears the writer
// rotates cleanly and a later failure warns again. Before the fix Write
// swallowed the rotation error, so the active file grew unbounded at zero
// observability.
func TestRotatingWriterSurfacesPermanentRotateFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")

	var warns int
	restoreWarn := warnRotateDegraded
	warnRotateDegraded = func(string, error) { warns++ }
	defer func() { warnRotateDegraded = restoreWarn }()

	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 8, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	// Permanently fail the active-file rename so every rotation leaves the active
	// file un-shifted (still growing), the reproducible form of a permanent cause
	// (a backup replaced by a directory, revoked log-dir rename permissions).
	restoreRename := osRename
	fail := true
	osRename = func(oldpath, newpath string) error {
		if fail && newpath == path+".1" {
			return fmt.Errorf("injected permanent rename failure")
		}
		return os.Rename(oldpath, newpath)
	}
	defer func() { osRename = restoreRename }()

	// Fill past the threshold, then two over-threshold writes. Both records must
	// land AND surface the rotation error, but the streak warns only once.
	if _, err := w.Write([]byte("0123456789")); err != nil {
		t.Fatalf("initial Write: %v", err)
	}
	for _, rec := range []string{"first", "second"} {
		n, err := w.Write([]byte(rec))
		if err == nil {
			t.Fatalf("Write %q: expected surfaced rotation error, got nil", rec)
		}
		if n != len(rec) {
			t.Fatalf("Write %q: short write n=%d want %d", rec, n, len(rec))
		}
	}
	if warns != 1 {
		t.Fatalf("warnRotateDegraded fired %d times during one streak, want 1", warns)
	}
	// The records landed in the (unrotated, growing) active file, and no backup
	// was produced while the shift kept failing.
	if data, err := os.ReadFile(path); err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if !strings.Contains(string(data), "first") || !strings.Contains(string(data), "second") {
		t.Fatalf("active file missing records; got %q", data)
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatalf("expected no .1 backup while rotation is failing (err=%v)", err)
	}

	// Clear the obstruction: the next over-threshold write rotates cleanly and
	// does not warn again.
	fail = false
	if _, err := w.Write([]byte("healed")); err != nil {
		t.Fatalf("Write after clearing obstruction: %v", err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected .1 backup after rotation resumed: %v", err)
	}
	if warns != 1 {
		t.Fatalf("warn count changed after clean rotation: %d, want 1", warns)
	}

	// A fresh failure streak warns again (the clean rotation reset the streak).
	fail = true
	n, err := w.Write([]byte("padding"))
	if err == nil {
		t.Fatal("expected surfaced rotation error on the second streak, got nil")
	}
	if n != len("padding") {
		t.Fatalf("short write on second streak: n=%d want %d", n, len("padding"))
	}
	if warns != 2 {
		t.Fatalf("second failure streak fired %d warns total, want 2", warns)
	}
}

func TestRotatingWriterConcurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "synosync.log")
	w, err := NewRotatingWriter(path, RotateOptions{MaxBytes: 64, MaxBackups: 4})
	if err != nil {
		t.Fatalf("NewRotatingWriter: %v", err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 50 {
				if _, err := fmt.Fprintf(w, "g%d-i%d\n", g, i); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
