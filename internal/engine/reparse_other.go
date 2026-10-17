//go:build !windows

package engine

// isNameSurrogate reports false on a Unix-like host: the only directory entry whose name
// resolves to another named entity is a symlink, and pathRedirects has already classified that
// from the mode bits before reaching here. An irregular entry on such a host is a device node,
// FIFO or socket - content the scanner and the no-follow open refuse on their own merits, not a
// redirection of the path.
func isNameSurrogate(string) bool { return false }
