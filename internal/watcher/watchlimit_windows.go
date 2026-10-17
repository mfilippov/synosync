//go:build windows

package watcher

// isWatchLimit is never true here: the Windows backend watches the whole tree through one
// directory handle, so there is no per-directory allowance to run out of.
func isWatchLimit(error) bool { return false }
