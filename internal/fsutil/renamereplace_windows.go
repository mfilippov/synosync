//go:build windows

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

// renameFn is the move primitive renameReplace drives, a package var so a test can
// fail the SECOND attempt (the one that follows the attribute clear) and pin that
// the first error is returned and the destination's attributes are put back. The
// first attempt goes through it too, so a test that fails every call still reaches
// the ordinary no-retry path.
var renameFn = moveReplaceWriteThrough

// moveFileEx is the MoveFileEx seam shared by both move primitives, so a test can
// pin the flags the replacing move is issued with without having to observe the
// crash behaviour they buy, and the extended-length operands the no-replace one
// hands the syscall without needing a host whose registry rejects long paths.
var moveFileEx = windows.MoveFileEx

// moveReplaceWriteThrough is os.Rename plus MOVEFILE_WRITE_THROUGH: the same
// replacing MoveFileEx the stdlib issues (MOVEFILE_REPLACE_EXISTING, no
// MOVEFILE_COPY_ALLOWED, so a cross-volume move still fails rather than silently
// becoming a copy), with the move flushed to disk before the call returns.
//
// The write-through is what makes the staging-temp promote crash-durable, and it
// is the same guarantee renameNoReplace already asks for: without it, a power cut
// in the window after the promote returned could leave the OLD file in place while
// the store already recorded the downloaded content as the agreed baseline, and
// the next scan diffed the stale bytes into an outbound upload OVER the server's
// newer version. Both promote primitives now carry the same
// discipline, so which of them a given apply happens to take does not change the
// durability the engine gets.
//
// The path goes through ExtendedPath because this is a direct Win32 call and so
// does not get the stdlib's own MAX_PATH fixup, and the failure is wrapped in the
// *os.LinkError shape os.Rename produces so every existing classification of a
// rename failure keeps working verbatim.
func moveReplaceWriteThrough(from, to string) error {
	fromp, err := windows.UTF16PtrFromString(ExtendedPath(from))
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	top, err := windows.UTF16PtrFromString(ExtendedPath(to))
	if err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if err := moveFileEx(fromp, top, flags); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	return nil
}

// renameReplace performs a replacing move, retrying once when the destination's
// FILE_ATTRIBUTE_READONLY is what blocked it (see RenameReplace for why that retry
// is both necessary and safe).
func renameReplace(from, to string) error {
	err := renameFn(from, to)
	if err == nil || !errors.Is(err, fs.ErrPermission) {
		return err
	}
	attrs, cleared := clearReadOnlyAttr(to)
	if !cleared {
		return err
	}
	if rerr := renameFn(from, to); rerr != nil {
		// The destination survived the failed move, so put its attributes back:
		// clearing them is only ever justified by a move that actually lands.
		restoreFileAttrs(to, attrs)
		return err
	}
	return nil
}

// clearReadOnlyAttr clears FILE_ATTRIBUTE_READONLY on path and reports the
// attributes it held, so a caller whose retry fails can restore them. It reports
// false - leaving path untouched - unless path is a REGULAR file that actually
// carries the attribute: a directory destination is the permanently-blocked shape
// engine/executor.go dead-letters, and a reparse point must never have its
// attributes rewritten through a path that may resolve outside the sync tree. The
// attributes are read from the Lstat that classifies it, so the decision and the
// value written back describe the same observation.
func clearReadOnlyAttr(path string) (uint32, bool) {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return 0, false
	}
	data, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok || data.FileAttributes&windows.FILE_ATTRIBUTE_READONLY == 0 {
		return 0, false
	}
	if err := setFileAttrs(path, data.FileAttributes&^windows.FILE_ATTRIBUTE_READONLY); err != nil {
		return 0, false
	}
	return data.FileAttributes, true
}

// restoreFileAttrs re-applies attrs to path, best-effort: it runs on the failed-retry
// path where an error is already being returned, and a destination that vanished
// under us has nothing to restore.
func restoreFileAttrs(path string, attrs uint32) {
	_ = setFileAttrs(path, attrs)
}

// setFileAttrs writes attrs to path. The path goes through ExtendedPath for the same
// reason the move does: this is a direct Win32 call, so a destination deeper than
// MAX_PATH would otherwise fail the clear and turn the read-only retry back into the
// forever-transient it exists to remove.
func setFileAttrs(path string, attrs uint32) error {
	p, err := windows.UTF16PtrFromString(ExtendedPath(path))
	if err != nil {
		return err
	}
	return windows.SetFileAttributes(p, attrs)
}
