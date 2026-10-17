//go:build !windows

package watcher

import (
	"errors"
	"syscall"
)

// isWatchLimit reports whether a failed registration means the SYSTEM has no more watches to
// give, rather than that this one directory could not be watched: ENOSPC is inotify's
// fs.inotify.max_user_watches (8192 on older kernels, so a large tree reaches it), and
// EMFILE/ENFILE are the descriptor tables kqueue spends an entry per watched file on.
//
// The difference decides what the walk does next. A per-directory failure is logged and
// skipped; a system limit fails every directory after it, and logging each one put tens of
// thousands of identical Warns in the log of a large tree.
func isWatchLimit(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE)
}
