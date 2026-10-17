//go:build !windows

package engine

// componentHostRepresentable reports whether the host materialises a path component
// verbatim. Unix-like hosts store a filename as the byte string they were given - a
// trailing dot or space is an ordinary byte there and a legitimate name on ext4/btrfs/APFS -
// so every component is representable and this gate is a no-op (see namemangle.go).
func componentHostRepresentable(string) bool { return true }
