package engine

// isDiskFull classifies the host's "there is no room for this write" failure so
// noteApplyResult can latch the `Disk: full` operability flag.
//
// The latch used to test errors.Is(err, syscall.ENOSPC) directly, which made it dead code on
// Windows: syscall.ENOSPC there is a synthetic APPLICATION_ERROR-range value no Win32 call
// produces, and the windows Errno.Is bridge maps only ErrPermission/ErrExist/ErrNotExist/
// ErrUnsupported - so a real ERROR_DISK_FULL never matched and an operator filling the disk
// saw a healthy daemon with silently retrying ops. The mirror gap on Unix is the quota:
// an exhausted user quota (ext4/XFS/ZFS/NFS) surfaces as EDQUOT, a distinct errno that does
// not match ENOSPC either, so a quota-full volume was equally invisible.
//
// Like nameTooLong the per-OS codes live in build-tagged files rather
// than in a GOOS branch here, so each list is compiled - and vetted - on its own platform.
// syscall.ENOSPC stays in the shared check: it is the genuine code on Unix and the value
// tests inject through the apply seams on every platform.
//
// Misclassifying here is cosmetic in both directions - the disk-full arm changes no control
// flow (the op stays queued and retries either way), it only decides whether `status` names
// the condition - so the lists err toward covering every documented no-space code.

import (
	"errors"
	"syscall"
)

// isDiskFull reports whether err is the host's out-of-space (or out-of-quota) rejection.
func isDiskFull(err error) bool {
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}
	for _, target := range extraDiskFullErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
