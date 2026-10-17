//go:build !windows

package engine

import "syscall"

// hostDiskFullErrs are the errnos a Unix-like host returns when a write has nowhere to go.
// EDQUOT is the quota counterpart: a distinct errno from ENOSPC, invisible to the old
// errors.Is(err, syscall.ENOSPC) latch, yet the same "no apply can write" condition.
var hostDiskFullErrs = []hostErrCase{
	{"ENOSPC", syscall.ENOSPC},
	{"EDQUOT", syscall.EDQUOT},
}

// hostNotDiskFullErrs are neighbouring errnos the classifier must NOT claim: an over-broad
// latch would report `Disk: full` for every permission or missing-path failure.
var hostNotDiskFullErrs = []hostErrCase{
	{"EACCES", syscall.EACCES},
	{"ENOENT", syscall.ENOENT},
	{"ENOTDIR", syscall.ENOTDIR},
	{"ENAMETOOLONG", syscall.ENAMETOOLONG},
}

// hostNativeDiskFullErr is the host-native code the engine-level latch test injects. It is
// deliberately NOT ENOSPC: the pre-fix predicate already handled that one, so only a
// non-ENOSPC host code proves the classifier (see hostDiskFullIsNotENOSPC).
var hostNativeDiskFullErr error = syscall.EDQUOT
