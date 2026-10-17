//go:build !windows

package engine

import "syscall"

// hostDirNotEmptyErrs are the errnos a Unix-like host returns when a directory removal is
// blocked by a surviving entry. Here the shared syscall.ENOTEMPTY check is the whole
// classifier, so the per-OS list (extraDirNotEmptyErrors) is legitimately empty.
var hostDirNotEmptyErrs = []hostErrCase{
	{"ENOTEMPTY", syscall.ENOTEMPTY},
}

// hostNotDirNotEmptyErrs are neighbouring errnos the classifier must NOT claim: EEXIST is the
// permanent-occupant shape reaching applyOp through the same fs.ErrExist sentinel, and
// claiming it would turn a dead-letter into an endless retry.
var hostNotDirNotEmptyErrs = []hostErrCase{
	{"EEXIST", syscall.EEXIST},
	{"EACCES", syscall.EACCES},
	{"ENOTDIR", syscall.ENOTDIR},
	{"EBUSY", syscall.EBUSY},
}

// hostNativeDirNotEmptyErr is the host-native code the classifier and verb-gate tests inject.
var hostNativeDirNotEmptyErr error = syscall.ENOTEMPTY
