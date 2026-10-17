//go:build linux

package fsutil

import (
	"errors"
	"io/fs"

	"golang.org/x/sys/unix"
)

// renameNoReplace uses renameat2(RENAME_NOREPLACE): an atomic rename that fails with
// EEXIST when the destination already exists, with no separate unlink and no hard-link
// requirement. A filesystem or kernel that does not support the flag reports EINVAL or
// ENOSYS, in which case we fall back to the portable Lstat-guarded rename.
func renameNoReplace(from, to string) error {
	err := unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EEXIST):
		return fs.ErrExist
	case errors.Is(err, unix.EINVAL), errors.Is(err, unix.ENOSYS):
		// RENAME_NOREPLACE is unsupported on this kernel/filesystem; degrade gracefully.
		return renameNoReplaceGeneric(from, to)
	default:
		return err
	}
}
