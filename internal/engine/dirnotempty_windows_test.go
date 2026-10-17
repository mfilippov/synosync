//go:build windows

package engine

import "syscall"

// hostDirNotEmptyErrs are the Win32 codes a directory removal blocked by a surviving entry
// surfaces as. ERROR_DIR_NOT_EMPTY does NOT match the synthetic syscall.ENOTEMPTY under
// errors.Is (hostDirNotEmptyIsNotENOTEMPTY pins that), which is exactly why the shared check
// alone would leave the Windows arm of the directory-not-empty check dead.
var hostDirNotEmptyErrs = []hostErrCase{
	{"ERROR_DIR_NOT_EMPTY", syscall.Errno(145)},
}

// hostNotDirNotEmptyErrs are neighbouring Win32 codes the classifier must NOT claim. Both
// occupant codes are in the list deliberately: they reach applyOp through the very same
// fs.ErrExist sentinel, and claiming them would turn the permanent "an untracked local node
// occupies the target" shape into an endless retry.
var hostNotDirNotEmptyErrs = []hostErrCase{
	{"ERROR_ALREADY_EXISTS", syscall.Errno(183)},
	{"ERROR_FILE_EXISTS", syscall.Errno(80)},
	{"ERROR_ACCESS_DENIED", syscall.Errno(5)},
	{"ERROR_SHARING_VIOLATION", syscall.Errno(32)},
}

// hostNativeDirNotEmptyErr is the host-native code the classifier and verb-gate tests inject.
// It is deliberately NOT syscall.ENOTEMPTY: only a code the shared check misses proves the
// per-OS list is load-bearing (see hostDirNotEmptyIsNotENOTEMPTY).
var hostNativeDirNotEmptyErr error = syscall.Errno(145)
