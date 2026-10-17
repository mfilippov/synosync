//go:build !windows

package engine

import "syscall"

// Unix-like hosts report a full filesystem as the POSIX ENOSPC that isDiskFull already
// checks. An exhausted per-user/per-group QUOTA is a separate errno (EDQUOT) that does not
// match ENOSPC under errors.Is, yet it is the same operability condition for the engine -
// no apply can write - so it belongs in the same latch: otherwise an operator on a
// quota-full ext4/XFS/ZFS/NFS volume debugs a phantom stall instead of freeing quota.
var extraDiskFullErrors = []error{syscall.EDQUOT}
