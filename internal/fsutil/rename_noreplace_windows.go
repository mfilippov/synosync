//go:build windows

package fsutil

import (
	"errors"
	"io/fs"

	"golang.org/x/sys/windows"
)

// renameNoReplace uses MoveFileEx without MOVEFILE_REPLACE_EXISTING: Windows fails the
// move when the destination already exists, which is exactly the no-replace semantics we
// want (Go's os.Rename passes MOVEFILE_REPLACE_EXISTING and so would clobber). An
// already-existing destination surfaces as ERROR_ALREADY_EXISTS/ERROR_FILE_EXISTS, mapped
// to fs.ErrExist. MOVEFILE_WRITE_THROUGH flushes the move to disk before returning so a
// conflict copy set aside by the keep-both resolver survives a crash mid-rename.
//
// Both operands go through ExtendedPath. This is a direct Win32 call, so it does NOT get
// the stdlib's own MAX_PATH fixup (os.fixLongPath), while os.MkdirAll and os.Rename do -
// which means the client happily materialises a DSM tree deeper than 260 characters on a
// default-registry Windows and then failed every no-replace primitive on it, taking the
// whole keep-both core (preserveConflictCopy → RenameNoReplace) out on exactly those trees
// The call is routed through the moveFileEx seam it shares with
// moveReplaceWriteThrough so a test can pin the operands the syscall is handed.
func renameNoReplace(from, to string) error {
	fromp, err := windows.UTF16PtrFromString(ExtendedPath(from))
	if err != nil {
		return err
	}
	top, err := windows.UTF16PtrFromString(ExtendedPath(to))
	if err != nil {
		return err
	}
	if err := moveFileEx(fromp, top, windows.MOVEFILE_WRITE_THROUGH); err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
			return fs.ErrExist
		}
		return err
	}
	return nil
}
