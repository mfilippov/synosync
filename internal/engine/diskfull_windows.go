//go:build windows

package engine

import "syscall"

// Windows never reports the POSIX ENOSPC: syscall.ENOSPC there is a synthetic
// APPLICATION_ERROR-range value (0x20000054) no Win32 call returns, and windows'
// Errno.Is bridge maps only ErrPermission/ErrExist/ErrNotExist/ErrUnsupported - verified on
// this host, errors.Is(&fs.PathError{Err: syscall.Errno(112)}, syscall.ENOSPC) is false.
// A write that runs out of room surfaces as one of the Win32 codes below instead. Which one
// a given volume/redirector picks was not reproduced (filling a disk is not a unit test), so
// all documented no-space forms are listed: the arm only decides whether `status` says
// `Disk: full`, never whether the op retries.
var extraDiskFullErrors = []error{
	syscall.Errno(112),  // ERROR_DISK_FULL - "There is not enough space on the disk."
	syscall.Errno(39),   // ERROR_HANDLE_DISK_FULL - the handle-scoped form of the same condition
	syscall.Errno(1816), // ERROR_NOT_ENOUGH_QUOTA - the quota mirror of EDQUOT
	syscall.Errno(314),  // ERROR_DISK_RESOURCES_EXHAUSTED - thin-provisioned volume out of backing store
}
