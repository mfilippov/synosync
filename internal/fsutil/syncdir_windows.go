//go:build windows

package fsutil

import (
	"errors"

	"golang.org/x/sys/windows"
)

// createDirHandle opens dir for the flush, a package var so tests can inject an
// open failure (a denied ACL, a network filesystem's refusal) without needing
// such a directory or mount on the host. The access mask is a parameter so the
// GENERIC_WRITE → GENERIC_READ fallback is exercised through the same seam.
var createDirHandle = createDirHandleWin32

// flushDirHandle is the FlushFileBuffers seam, so tests can inject a filesystem
// that refuses to flush a directory handle at all, or a genuine I/O failure.
var flushDirHandle = windows.FlushFileBuffers

// closeDirHandle is the CloseHandle seam, kept alongside the other two so a test
// that injects a fake handle never closes a real one.
var closeDirHandle = windows.CloseHandle

// createDirHandleWin32 opens the directory at dir with the given access mask.
// FILE_FLAG_BACKUP_SEMANTICS is what makes CreateFile accept a directory at all;
// the share mode is fully permissive because holding this handle must not block
// a concurrent reader, writer or delete of the directory. The path goes through
// ExtendedPath so a tree deeper than MAX_PATH - which the os package happily
// creates - can still be flushed.
func createDirHandleWin32(dir string, access uint32) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(ExtendedPath(dir))
	if err != nil {
		return windows.InvalidHandle, err
	}
	return windows.CreateFile(p, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
}

// openDirForFlush opens dir for FlushFileBuffers, preferring a GENERIC_WRITE
// handle - the access FlushFileBuffers is documented to require - and falling
// back to GENERIC_READ when the write open is refused on permissions. The
// fallback exists because a directory whose ACL grants add-file/traverse but not
// write still accepts the mkdir/rename this flush follows; if the read handle
// then fails the flush with ERROR_ACCESS_DENIED that is a capability gap
// fsyncUnsupported degrades, which is strictly better than never trying. When the
// fallback open fails too, the WRITE open's error is the one reported: it
// describes the access this call actually wanted.
//
// The fallback is taken for a capability-gap-shaped refusal too, not just a
// permission one: a redirector or user-mode filesystem that will not hand out a
// WRITABLE directory handle answers ERROR_INVALID_PARAMETER / ERROR_NOT_SUPPORTED
// rather than ERROR_ACCESS_DENIED, and a GENERIC_READ handle is exactly what such
// a mount does accept - the same shape the flush stage already degrades, refused
// one syscall earlier.
func openDirForFlush(dir string) (windows.Handle, error) {
	h, err := createDirHandle(dir, windows.GENERIC_WRITE)
	if err == nil {
		return h, nil
	}
	if !fsyncOpenDegradable(err) {
		return windows.InvalidHandle, err
	}
	h, rerr := createDirHandle(dir, windows.GENERIC_READ)
	if rerr != nil {
		return windows.InvalidHandle, err
	}
	return h, nil
}

// fsyncOpenDegradable reports whether an open failure is a capability gap rather
// than a genuine I/O failure: a permission refusal (fsyncOpenDenied) or one of the
// "this filesystem does not do that" codes fsyncUnsupported already names at the
// flush stage. Both stages share the classifier because a driver is free to refuse
// the directory handle at either - a Dokan/WinFsp mount or an SMB redirector that
// stubs directory flushing out may reject the CreateFile with
// ERROR_INVALID_PARAMETER instead of accepting the handle and failing
// FlushFileBuffers. Keying only on fs.ErrPermission there left that mount failing
// every fsutil.Mkdir/Rename/Remove - and store.Open's own dir-entry fsync -
// forever, the exact forever-transient class the degradation exists to
// prevent, and one this change's own real syscall newly exposed. A genuine open
// failure (ERROR_IO_DEVICE, ERROR_NOT_READY, a missing directory) still propagates.
func fsyncOpenDegradable(err error) bool {
	return fsyncOpenDenied(err) || fsyncUnsupported(err)
}

// fsyncUnsupported reports whether a directory-flush failure means the filesystem
// cannot flush a directory handle at all, as opposed to a genuine I/O failure
// (ERROR_IO_DEVICE, ERROR_DISK_FULL, ERROR_WRITE_FAULT) which must still fail the
// caller - the Windows twin of the Unix EINVAL/ENOTSUP/ENOSYS set.
// ERROR_INVALID_FUNCTION and ERROR_NOT_SUPPORTED are what a redirector or a
// user-mode filesystem (Dokan/WinFsp-backed mounts, several SMB servers) returns
// for FlushFileBuffers on a directory; ERROR_INVALID_PARAMETER is the same
// refusal from a driver that validates the handle type instead; ERROR_ACCESS_DENIED
// is the documented answer when the handle lacks GENERIC_WRITE, which is exactly
// the fallback handle openDirForFlush may have had to settle for.
// ERROR_CALL_NOT_IMPLEMENTED covers a redirector that stubs the operation out.
func fsyncUnsupported(err error) bool {
	return errors.Is(err, windows.ERROR_INVALID_FUNCTION) ||
		errors.Is(err, windows.ERROR_NOT_SUPPORTED) ||
		errors.Is(err, windows.ERROR_INVALID_PARAMETER) ||
		errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED)
}

// fsyncDir flushes the directory entry changes for dir to durable storage on
// Windows: it opens the directory with FILE_FLAG_BACKUP_SEMANTICS and calls
// FlushFileBuffers on the handle.
//
// This used to be a no-op, justified by two claims that do not hold
// up. The first - "a directory handle cannot be flushed, FlushFileBuffers
// rejects a directory" - is empirically false: the call succeeds on an NTFS
// directory handle opened GENERIC_WRITE + FILE_FLAG_BACKUP_SEMANTICS. The second
// - "NTFS does not require it for rename durability" - contradicts this package's
// own rename_noreplace_windows.go, which passes MOVEFILE_WRITE_THROUGH precisely
// so a set-aside conflict copy survives a crash mid-rename. Between them the directory-flush
// discipline DESIGN §7 describes (every directory mutation's entry made durable
// before the store records agreement) was reduced to nothing on Windows: a power
// cut after a promote could leave the OLD file in place while the baseline already
// recorded the new content as agreed, and the next scan pushed the stale bytes
// back over the server's newer version.
//
// The capability-gap degradation mirrors the Unix arm exactly: an open the
// filesystem refuses on permissions or cannot serve at all (fsyncOpenDegradable)
// and a flush it cannot perform (fsyncUnsupported) degrade to non-durable success
// with one Warn per process, because the caller's on-disk mutation already landed
// and failing here would retry every apply into that directory forever. A genuine
// I/O failure propagates.
func fsyncDir(dir string) error {
	h, err := openDirForFlush(dir)
	if err != nil {
		if fsyncOpenDegradable(err) {
			degradeFsync(dir, err)
			return nil
		}
		return err
	}
	ferr := flushDirHandle(h)
	cerr := closeDirHandle(h)
	if ferr != nil {
		if fsyncUnsupported(ferr) {
			degradeFsync(dir, ferr)
			return nil
		}
		return ferr
	}
	return cerr
}
