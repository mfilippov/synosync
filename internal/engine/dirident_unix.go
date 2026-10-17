//go:build !windows

package engine

import (
	"fmt"
	"io/fs"
	"syscall"

	"golang.org/x/sys/unix"
)

// dirIdentity returns the (device, inode) of a directory from the lstat the scanner already
// made, so on Unix every scanned directory carries its identity for free. emptieddir.go
// compares it across scans: an unmount always changes the object at the path - the covered
// directory is a different inode, and usually a different device - while a user emptying a
// directory leaves the same object in place.
func dirIdentity(fi fs.FileInfo) (fileIdent, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdent{}, false
	}
	// Dev and Ino widths differ per platform (Dev is int32 on Darwin); only the bit pattern
	// matters, as an opaque key compared against a remembered value - see osDeviceID.
	return fileIdent{dev: uint64(st.Dev), ino: uint64(st.Ino)}, true //nolint:unconvert,gosec // platform-dependent widths
}

// osDirIdentity reads a directory's identity by path, for a scan entry that carried none.
// dirIdentity always succeeds on Unix, so this is only the fallback the shared code expects.
func osDirIdentity(path string) (fileIdent, error) {
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return fileIdent{}, fmt.Errorf("engine: lstat identity %q: %w", path, err)
	}
	return fileIdent{dev: uint64(st.Dev), ino: uint64(st.Ino)}, nil //nolint:unconvert,gosec // platform-dependent widths
}
