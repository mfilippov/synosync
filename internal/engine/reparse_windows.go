//go:build windows

package engine

import (
	"golang.org/x/sys/windows"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// reparseTagNameSurrogate is the IO_REPARSE_TAG_NAME_SURROGATE bit (Win32 does not export it
// through x/sys/windows). A tag carrying it declares that the entry "is a surrogate for another
// named entity" - the reparse points that REDIRECT path resolution: IO_REPARSE_TAG_SYMLINK
// (0xA000000C) and IO_REPARSE_TAG_MOUNT_POINT (0xA0000003, a junction) both set it, while the
// reparse points that merely virtualise a file's storage - IO_REPARSE_TAG_CLOUD (OneDrive
// placeholders, 0x9000001A) and IO_REPARSE_TAG_DEDUP (0x80000013) - do not. Keying the guard on
// this bit is what lets it refuse an escape vector without dead-lettering ordinary syncing on a
// cloud-backed or deduplicated volume.
const reparseTagNameSurrogate = 0x2000_0000

// isNameSurrogate reports whether the reparse point at path redirects to another named entity
// (a junction or any other name-surrogate tag). Only entries Go already classified as irregular
// reach here, so the query runs on the rare entry, never on a regular file or directory.
//
// A tag the host will not report is treated as a redirection - fail closed. The containment
// guards are a security check, and their whole subject is a path component that is already an
// unrecognised reparse point: refusing it dead-letters one op with an observable trace, whereas
// admitting it would let an apply write through a redirection nobody could characterise. This is
// the same fail-closed call verifyNoSymlinkRedirect makes for the unverifiable upload open.
func isNameSurrogate(path string) bool {
	tag, err := reparseTag(path)
	if err != nil {
		return true
	}
	return tagIsNameSurrogate(tag)
}

// tagIsNameSurrogate reports whether a reparse tag declares its entry a surrogate for another
// named entity. Split out from isNameSurrogate so the classification - which decides between
// "refuse this path" and "sync this file normally" for every reparse tag Windows defines - is
// table-testable on a host that has no cloud-files or dedup volume to produce one.
//
// The zero tag reparseTag returns for a non-reparse-point entry is correctly not a surrogate.
func tagIsNameSurrogate(tag uint32) bool { return tag&reparseTagNameSurrogate != 0 }

// reparseTag returns the reparse tag of the directory entry at path, or 0 when the entry is not
// a reparse point at all.
//
// FindFirstFileW answers from the PARENT directory's entry, so it never traverses the reparse
// point it is describing - the no-follow property the guards need - and it is a single call
// rather than the open-with-FILE_FLAG_OPEN_REPARSE_POINT + GetFileInformationByHandleEx pair.
// The path goes through fsutil.ExtendedPath because a direct Win32 call gets none of the
// stdlib's MAX_PATH fixup: without it the query would fail on exactly the
// deep trees the os package materialises happily, and the fail-closed arm above would then
// reject every irregular component found there.
func reparseTag(path string) (uint32, error) {
	p, err := windows.UTF16PtrFromString(fsutil.ExtendedPath(path))
	if err != nil {
		return 0, err
	}
	var fd windows.Win32finddata
	h, err := windows.FindFirstFile(p, &fd)
	if err != nil {
		return 0, err
	}
	// The search handle holds no state worth reporting on and nothing to retry; close it
	// explicitly rather than leaving an unchecked return.
	defer func() { _ = windows.FindClose(h) }()
	if fd.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return 0, nil
	}
	// Reserved0 carries the reparse tag when the entry is a reparse point (documented
	// WIN32_FIND_DATAW behaviour); it is meaningless otherwise, hence the attribute check.
	return fd.Reserved0, nil
}
