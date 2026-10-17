//go:build darwin

package fsutil

import (
	"errors"
	"io/fs"

	"golang.org/x/sys/unix"
)

// renameNoReplace uses renamex_np(RENAME_EXCL): the macOS atomic no-replace rename,
// failing with EEXIST when the destination already exists. A filesystem that does not
// support the flag reports ENOTSUP/EINVAL, in which case we fall back to the portable
// Lstat-guarded rename.
func renameNoReplace(from, to string) error {
	err := unix.RenamexNp(from, to, unix.RENAME_EXCL)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EEXIST):
		return fs.ErrExist
	case errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EINVAL):
		return renameNoReplaceGeneric(from, to)
	default:
		return err
	}
}
