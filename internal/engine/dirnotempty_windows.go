//go:build windows

package engine

import "syscall"

// Windows never reports the POSIX ENOTEMPTY: syscall.ENOTEMPTY there is a synthetic
// APPLICATION_ERROR-range value no Win32 call returns. RemoveDirectory on a directory that
// still holds entries reports ERROR_DIR_NOT_EMPTY (145) instead - "The directory is not
// empty" - and windows' Errno.Is bridge maps exactly that code (alongside
// ERROR_ALREADY_EXISTS and ERROR_FILE_EXISTS) onto fs.ErrExist, which is what let the
// transient shape reach applyOp's permanent-occupant arm indistinguishably.
//
// ERROR_DIR_NOT_EMPTY also covers the delete-pending form: on volumes without POSIX delete
// semantics (FAT32/exFAT, some SMB redirectors) an unlinked child keeps its name until the
// last handle closes, so a directory an indexer or AV scanner is holding reads as non-empty
// for as long as that handle lives - transient in exactly the same way.
var extraDirNotEmptyErrors = []error{
	syscall.Errno(145), // ERROR_DIR_NOT_EMPTY
}
