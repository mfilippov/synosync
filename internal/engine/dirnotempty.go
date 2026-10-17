package engine

// dirNotEmpty classifies the host's "that directory still holds entries" refusal so applyOp's
// fs.ErrExist arm can tell it apart from the permanent occupant shape that arm dead-letters.
//
// Go folds BOTH shapes into fs.ErrExist: ENOTEMPTY (unix) and ERROR_DIR_NOT_EMPTY (windows)
// satisfy errors.Is(err, fs.ErrExist) exactly as EEXIST/ERROR_ALREADY_EXISTS do. They say
// opposite things about permanence, though. An occupant blocking a file- or dir-PLACING apply
// is permanent - under the never-delete policy the untracked local node never vanishes on its
// own, which is the whole rationale for dead-lettering it. "Directory not empty" is reported by
// a directory REMOVAL, and the entry that blocks it is by construction one os.RemoveAll's single
// readdir pass did not see: a file a user application (or the indexer/AV on a volume without
// POSIX delete semantics) dropped into the tree while the delete was walking it. Seconds later
// the blocker is gone and the very same op applies - the definition of transient.
//
// Getting that wrong is not cosmetic: the fs.ErrExist arm drops the op AND its baselines
// (dropRejectedRemoveBaseline), so a half-deleted directory is left untracked on disk and the
// next scan re-uploads it as a fresh local create - resurrecting a server-deleted subtree on
// every replica. The classifier therefore keeps the ENOTEMPTY shape queued for the verbs whose
// apply performs a directory removal (see existsBlocksForever) and leaves the placing verbs'
// rationale untouched.
//
// Like nameTooLong and isDiskFull the per-OS codes live
// in build-tagged files rather than a GOOS branch here, so each list is compiled and vetted on
// its own platform. syscall.ENOTEMPTY stays in the shared check: it is the genuine code on Unix
// and the value tests inject through the apply seams on every platform.

import (
	"errors"
	"syscall"
)

// dirNotEmpty reports whether err is the host's "directory not empty" refusal.
func dirNotEmpty(err error) bool {
	if errors.Is(err, syscall.ENOTEMPTY) {
		return true
	}
	for _, target := range extraDirNotEmptyErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
