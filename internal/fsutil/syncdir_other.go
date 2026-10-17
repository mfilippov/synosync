//go:build !windows

package fsutil

import (
	"errors"
	"os"
	"syscall"
)

// syncFileHandle is the directory-handle fsync seam fsyncDir routes through, so
// tests can inject a filesystem that rejects directory fsync (EINVAL/ENOTSUP/
// ENOSYS) or a genuine I/O failure without needing such a filesystem mounted.
var syncFileHandle = (*os.File).Sync

// fsyncUnsupported reports whether a directory-fsync failure means the
// filesystem cannot flush a directory handle at all - EINVAL/ENOTSUP/EOPNOTSUPP/
// ENOSYS, the errnos CIFS/SMB and several FUSE filesystems return unconditionally
// for fsync on a directory - as opposed to a genuine I/O failure (EIO, ENOSPC),
// which must still fail the caller. Decision: EBADF is
// deliberately excluded - POSIX sanctions fsyncing a directory opened read-only,
// so an EBADF here signals a real bug, not a capability gap. EOPNOTSUPP is
// included alongside ENOTSUP because on Linux they alias (both 95) but on
// darwin/BSD they are distinct errnos (ENOTSUP 45, EOPNOTSUPP 102) and either
// can surface from a network-mount directory fsync - both mean "not supported",
// a capability gap, never a transient I/O error.
func fsyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.ENOSYS)
}

// fsyncDir fsyncs the directory at dir so a rename into it is durable across a
// crash. POSIX permits fsync on a directory opened read-only; that flushes the
// directory's entry changes (the new/removed names) to stable storage.
//
// A filesystem that cannot fsync a directory at all (CIFS/SMB and several FUSE
// filesystems return EINVAL/ENOTSUP/ENOSYS for it unconditionally) degrades to
// non-durable instead of failing: propagating the error would make every
// inbound mkdir/rename/remove, every download promote and every keep-both
// set-aside fail forever as a transient retry - sync entirely inoperable on
// the very NAS mounts this client targets - while tolerating it only loses a
// crash-durability guarantee that filesystem never provided in the first place
// (PostgreSQL ignores EINVAL on the same call for the same reason). One Warn
// per process records the degradation.
//
// The open stage degrades the same way on a permission refusal:
// a write+search-only (0300) directory accepts the rename/mkdir but
// refuses the read-only open the fsync needs, so an EACCES/EPERM there is a
// capability gap of the same shape, not an I/O failure - see fsyncOpenDenied.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if fsyncOpenDenied(err) {
			degradeFsync(dir, err)
			return nil
		}
		return err
	}
	if err := syncFileHandle(d); err != nil {
		_ = d.Close()
		if fsyncUnsupported(err) {
			degradeFsync(dir, err)
			return nil
		}
		return err
	}
	return d.Close()
}
