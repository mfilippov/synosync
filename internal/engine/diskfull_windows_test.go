//go:build windows

package engine

import "syscall"

// hostDiskFullErrs are the Win32 codes a write with nowhere to go surfaces as. None of them
// matches the synthetic syscall.ENOSPC under errors.Is (hostDiskFullIsNotENOSPC pins that),
// which is exactly why the old latch was dead code on Windows.
var hostDiskFullErrs = []hostErrCase{
	{"ERROR_DISK_FULL", syscall.Errno(112)},
	{"ERROR_HANDLE_DISK_FULL", syscall.Errno(39)},
	{"ERROR_NOT_ENOUGH_QUOTA", syscall.Errno(1816)},
	{"ERROR_DISK_RESOURCES_EXHAUSTED", syscall.Errno(314)},
}

// hostNotDiskFullErrs are neighbouring Win32 codes the classifier must NOT claim: an
// over-broad latch would report `Disk: full` for an antivirus lock or a missing path.
var hostNotDiskFullErrs = []hostErrCase{
	{"ERROR_ACCESS_DENIED", syscall.Errno(5)},
	{"ERROR_FILE_NOT_FOUND", syscall.Errno(2)},
	{"ERROR_SHARING_VIOLATION", syscall.Errno(32)},
	{"ERROR_INVALID_NAME", syscall.Errno(123)},
}

// hostNativeDiskFullErr is the host-native code the engine-level latch test injects - the
// real ERROR_DISK_FULL an NTFS volume out of room reports, which the pre-fix predicate
// silently ignored.
var hostNativeDiskFullErr error = syscall.Errno(112)
