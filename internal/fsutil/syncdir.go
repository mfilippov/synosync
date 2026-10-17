package fsutil

import (
	"errors"
	"io/fs"
	"log/slog"
	"sync/atomic"
)

// fsyncDegradedWarned guards the once-per-process degraded-durability warning: a
// filesystem that refuses to flush a directory wholesale would otherwise repeat
// the same Warn on every apply. Tests reset it.
var fsyncDegradedWarned atomic.Bool

// warnFsyncDegraded emits the one-per-process degraded-durability warning; a
// package var so tests can capture it without swapping the global slog default.
var warnFsyncDegraded = func(dir string, err error) {
	slog.Warn("fsutil: directory fsync unavailable (unsupported by the "+
		"filesystem, or the directory refuses the open the flush requires); "+
		"directory-entry durability degraded to non-durable",
		"dir", dir, "err", err)
}

// degradeFsync records a directory-flush capability gap: the caller's on-disk
// mutation already landed, so the flush degrades to non-durable success and the
// process warns exactly once. Both per-OS fsyncDir implementations route their
// capability-gap arms through it so the once-per-process budget is shared.
func degradeFsync(dir string, err error) {
	if fsyncDegradedWarned.CompareAndSwap(false, true) {
		warnFsyncDegraded(dir, err)
	}
}

// fsyncOpenDenied reports whether the open a directory flush requires was refused
// on permissions - EACCES/EPERM on Unix and ERROR_ACCESS_DENIED on Windows,
// exactly the errnos syscall.Errno maps onto fs.ErrPermission. POSIX permits
// creating and renaming entries inside a write+search-only (0300) directory, but
// the fsync needs an O_RDONLY open, which such a directory refuses; on Windows a
// directory whose ACL grants traverse/add-file but not read/write refuses the
// backup-semantics open the same way. The on-disk mutation already succeeded in
// both cases, so propagating the open failure would leave every apply into that
// directory retrying forever as a transient - the same
// forever-transient class the flush-stage degradation closes for CIFS/FUSE mounts
// - while tolerating it only loses the crash-durability flush for a directory the
// user deliberately made unreadable. A genuine open failure (EIO, ENOENT, EMFILE)
// still propagates.
func fsyncOpenDenied(err error) bool {
	return errors.Is(err, fs.ErrPermission)
}
