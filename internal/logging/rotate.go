package logging

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultMaxBytes is the size threshold at which the active log file is
	// rotated (~10 MiB).
	DefaultMaxBytes int64 = 10 << 20
	// DefaultMaxBackups is the number of rotated backups retained (the active
	// file plus .1 … .MaxBackups).
	DefaultMaxBackups = 5
)

// osRename and osRemove indirect the filesystem mutations rotate() performs so a
// test can force a rename/remove failure and assert the writer recovers rather
// than bricking.
var (
	osRename = os.Rename
	osRemove = os.Remove
)

// warnRotateDegraded emits the once-per-streak degraded-rotation warning Write
// fires when a rotation cannot shift the active file aside (the backup chain
// failed) yet the active file was reopened, so it keeps growing past the size
// threshold. A package var so tests can capture it without swapping the global
// slog default, mirroring fsutil.warnFsyncDegraded.
var warnRotateDegraded = func(path string, err error) {
	slog.Warn("logging: log rotation failing; active file exceeding size threshold",
		"path", path, "err", err)
}

// RotateOptions configures a RotatingWriter. A zero value selects the defaults
// (DefaultMaxBytes, DefaultMaxBackups).
type RotateOptions struct {
	// MaxBytes is the size threshold (in bytes) past which the active file is
	// rotated before a write. A non-positive value selects DefaultMaxBytes.
	MaxBytes int64
	// MaxBackups is the number of rotated backups to retain. A non-positive
	// value selects DefaultMaxBackups.
	MaxBackups int
}

// RotatingWriter is a size-based, rotating io.WriteCloser. It appends to a
// single active file and, once that file would exceed the configured size
// threshold, renames it to "<name>.1", shifting any existing "<name>.N" to
// "<name>.N+1" and discarding the oldest backup. Writes are serialised with a
// mutex, so a RotatingWriter is safe for concurrent use. No compression is
// performed.
type RotatingWriter struct {
	path       string
	maxBytes   int64
	maxBackups int

	mu   sync.Mutex
	f    *os.File
	size int64
	// closed records an explicit Close(). It is distinct from f == nil, which
	// also occurs when a rotation failed so hard that even the best-effort
	// reopen did not produce an active file - that state self-heals on the next
	// Write, whereas a closed writer must stay closed.
	closed bool
	// rotateFailing guards the once-per-streak degraded-rotation warning: a
	// permanent cause (a backup replaced by a directory, revoked log-dir rename
	// permissions) would otherwise repeat the Warn on every over-threshold
	// write. It is set when a rotation leaves the active file un-shifted (still
	// growing) and cleared by the next clean rotation, so a later streak warns
	// again. Guarded by mu.
	rotateFailing bool
}

// NewRotatingWriter opens (creating the parent directory and file as needed)
// the log file at path for appending. Existing content is preserved and
// further writes are appended.
func NewRotatingWriter(path string, opts RotateOptions) (*RotatingWriter, error) {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	maxBackups := opts.MaxBackups
	if maxBackups <= 0 {
		maxBackups = DefaultMaxBackups
	}

	w := &RotatingWriter{
		path:       path,
		maxBytes:   maxBytes,
		maxBackups: maxBackups,
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// open creates the parent directory and opens the active file for appending,
// recording its current size.
func (w *RotatingWriter) open() error {
	if err := os.MkdirAll(filepath.Dir(w.path), 0o700); err != nil {
		return fmt.Errorf("logging: create log dir: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("logging: open log file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("logging: stat log file: %w", err)
	}
	w.f = f
	w.size = info.Size()
	return nil
}

// Write appends p to the active file, rotating first when appending p would push
// the file past the size threshold. The whole record is written to a single file
// (no record is split across a rotation boundary), so the active file may
// overshoot the threshold by at most one record. A non-empty active file is never
// rotated to make room for an empty write, and an empty active file is never
// rotated (a single record larger than the threshold still lands in one file).
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, fmt.Errorf("logging: write to closed RotatingWriter")
	}
	if w.f == nil {
		// A prior rotation failed so completely (shift AND reopen, e.g. a
		// transient ENOSPC/EMFILE/NFS blip) that no active file is open. Retry
		// the open here so the sink heals on the next record instead of staying
		// bricked until the process restarts.
		if err := w.open(); err != nil {
			return 0, err
		}
	}
	var rotErr error
	boundViolated := false
	if w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		bounded, err := w.rotate()
		rotErr = err
		if rotErr != nil && w.f == nil {
			// rotate() failed and could not even reopen the active file; the
			// writer is unusable for this record. The error is returned, so this
			// total failure is already observable to the caller.
			return 0, rotErr
		}
		// bounded reports whether the active file was actually shifted aside. A
		// clean shift (bounded, whatever incidental close/reopen noise rotErr
		// carries) means the size bound is honoured, so the record is appended to
		// a fresh file and any rotErr is benign - swallowed as before. When the
		// shift failed (!bounded) rotate() has best-effort reopened the SAME,
		// still-growing active file: the record is not lost, but the threshold is
		// no longer enforced. That must not be swallowed silently - warn once per
		// streak and surface rotErr to the caller.
		boundViolated = !bounded && rotErr != nil
		w.noteRotate(boundViolated, rotErr)
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	if boundViolated {
		// The record landed (n bytes) but rotation is degraded; return the
		// rotation error joined with any write error so a consumer inspecting it
		// (the daemon's bestEffortWriter) surfaces the fault rather than reading
		// the append as an unqualified success.
		return n, errors.Join(rotErr, err)
	}
	return n, err
}

// noteRotate maintains the once-per-streak degraded-rotation warning. A rotation
// that leaves the active file un-shifted (boundViolated) warns the first time in
// a streak; any rotation that honours the size bound clears the streak so a later
// failure warns again.
func (w *RotatingWriter) noteRotate(boundViolated bool, rotErr error) {
	if boundViolated {
		if !w.rotateFailing {
			w.rotateFailing = true
			warnRotateDegraded(w.path, rotErr)
		}
		return
	}
	w.rotateFailing = false
}

// rotate closes the active file, shifts the backup chain, and reopens a fresh
// active file. The caller must hold w.mu. It returns bounded=true when the
// active file was shifted aside (the size bound is honoured, whatever incidental
// close/reopen errors err aggregates) and bounded=false when the backup shift
// failed and the still-growing active file was reopened in place - the condition
// Write surfaces.
//
// rotate never lets a one-shot failure brick the writer: a Close error does not
// abort the rotation - the handle is unusable either way, so it is dropped and
// the shift+reopen still run (retrying the Close on the next write would only
// re-fail on the same dead handle forever); if shifting the backup chain fails
// partway (a permission error, a backup replaced by a directory, ENOSPC), the
// active file is best-effort reopened so subsequent writes still land
// somewhere. Only when even that reopen fails is w.f left nil - and Write then
// retries the open on the next record. All failures are returned aggregated.
func (w *RotatingWriter) rotate() (bounded bool, err error) {
	var errs []error
	if err := w.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("logging: close before rotate: %w", err))
	}
	w.f = nil

	if err := w.shiftBackups(); err != nil {
		errs = append(errs, err)
		if reopenErr := w.open(); reopenErr != nil {
			errs = append(errs, fmt.Errorf("logging: reopen after failed rotate: %w", reopenErr))
		}
		return false, errors.Join(errs...)
	}

	if err := w.open(); err != nil {
		errs = append(errs, err)
	}
	return true, errors.Join(errs...)
}

// shiftBackups discards the oldest backup, shifts .N → .N+1 down to .1, and
// finally moves the active file to .1. The caller must hold w.mu and must have
// already closed and cleared the active file handle.
func (w *RotatingWriter) shiftBackups() error {
	oldest := fmt.Sprintf("%s.%d", w.path, w.maxBackups)
	if err := osRemove(oldest); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logging: remove oldest backup: %w", err)
	}
	for i := w.maxBackups - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", w.path, i)
		dst := fmt.Sprintf("%s.%d", w.path, i+1)
		if err := osRename(src, dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("logging: shift backup %d: %w", i, err)
		}
	}
	if err := osRename(w.path, w.path+".1"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("logging: rotate active file: %w", err)
	}
	return nil
}

// Close closes the active file. The writer must not be used afterwards.
func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

var _ io.WriteCloser = (*RotatingWriter)(nil)
