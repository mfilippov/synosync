package engine

import "io/fs"

// This file carries the engine's "does this path component redirect elsewhere?" classifier -
// the portable core of the no-follow containment guards (noSymlinkEscape,
// pathHasSymlinkComponent, the scan walk).
//
// Those guards were written against a POSIX model in which exactly one kind of directory entry
// resolves to another named entity: a symlink, spelled os.ModeSymlink. Windows has a second one
// the mode bits do NOT spell that way. A junction (an NTFS mount point, IO_REPARSE_TAG_MOUNT_POINT)
// redirects a path exactly like a directory symlink does, and - unlike a symlink - an ORDINARY,
// unprivileged user can create one (`mklink /J`, no SeCreateSymbolicLinkPrivilege). Empirically
// pinned on this host:
//
//	mklink /J junc outside            // succeeds with no privilege at all
//	os.Lstat("junc").Mode()           // ?rw-rw-rw-  - ModeIrregular, NOT ModeSymlink
//	os.Stat("junc").Mode()            // drwxrwxrwx  - resolution goes through it
//
// So a `fi.Mode()&os.ModeSymlink != 0` gate is blind to it: a junction planted inside the sync
// root by any local process turns every inbound apply beneath it into a write OUTSIDE the sync
// root, which is the escape class the guards exist to close. The Windows arm of those guards has
// never been executed by any test (the fixtures skipped on GOOS), so the gap long went
// unnoticed.
//
// The classification is deliberately NOT "any reparse point". Windows uses reparse points for
// things that are not redirections at all - a OneDrive/cloud-files placeholder, a Data
// Deduplication chunk store entry - and refusing those would dead-letter ordinary syncing on a
// perfectly healthy tree. Win32 already draws exactly the line we need: the name-surrogate bit
// (0x20000000) of the reparse tag marks a tag whose entry "is a surrogate for another named
// entity" - symlinks and mount points have it, cloud and dedup tags do not. That bit is the gate.

// pathRedirects reports whether the on-disk entry at path - whose no-follow mode bits are mode
// (an os.Lstat Mode, or a WalkDir DirEntry Type) - resolves the remainder of a path somewhere
// else: a symlink on every host, and on Windows also a junction or any other name-surrogate
// reparse point.
//
// The mode is consulted first so the common case costs nothing: a regular file or directory
// carries neither ModeSymlink nor ModeIrregular, and only the rare irregular entry pays the
// per-OS tag query. Go maps every reparse point it does not recognise as a symlink, socket or
// dedup chunk to ModeIrregular, so the surrogate class is a strict subset of what that bit
// admits - the per-OS arm narrows it, it never has to widen it.
func pathRedirects(path string, mode fs.FileMode) bool {
	if mode&fs.ModeSymlink != 0 {
		return true
	}
	if mode&fs.ModeIrregular == 0 {
		return false
	}
	return isNameSurrogate(path)
}
