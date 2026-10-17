package fsutil

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// RenameNoReplace atomically moves the entry at from to to, failing with fs.ErrExist
// when an entry already exists at to rather than silently replacing it (unlike Rename
// and os.Rename, which clobber an existing destination on POSIX). It creates to's
// parent directory if it does not yet exist.
//
// The move is a single filesystem operation, so - unlike a hard-link-then-unlink - a
// concurrent replacement of from cannot leave a different inode deleted while the copy
// keeps the old one, and the source is never removed by a second path resolution that a
// swapped parent symlink could redirect. This is the primitive the keep-both conflict
// resolver needs to set a divergent local file aside without clobbering an existing
// copy and without a source race (engine/conflict.go).
//
// Where the platform offers an atomic no-replace rename - Linux renameat2
// RENAME_NOREPLACE, macOS renamex_np RENAME_EXCL, Windows MoveFileEx without
// MOVEFILE_REPLACE_EXISTING - it is used directly. On a filesystem that does not
// support the flag it degrades to a Lstat-guarded os.Rename (renameNoReplaceGeneric),
// which still performs the move in one syscall (no source race) and works on
// filesystems without hard links (FAT/exFAT, some network shares), at the cost of a
// narrow window where a file created at to between the check and the rename could be
// replaced.
func RenameNoReplace(from, to string) error {
	// Create the destination's parent chain durably (mkdirAllDurable fsyncs the
	// parent of every directory it creates), matching Rename/Mkdir - a bare
	// os.MkdirAll would leave a freshly created intermediate directory entry
	// non-durable across a crash that immediately follows the move.
	if err := mkdirAllDurable(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	if err := renameNoReplace(from, to); err != nil {
		return fmt.Errorf("fsutil: rename %s -> %s: %w", from, to, err)
	}
	// Persist the destination directory's new entry - and, on a cross-directory
	// move, the source directory's removed entry - so the move survives a crash
	// that immediately follows it (see syncRenameDirs).
	return syncRenameDirs(from, to)
}

// renameNoReplaceGeneric is the portable fallback used when no atomic no-replace rename
// primitive is available (or the filesystem rejects the flag): it refuses an existing
// destination via Lstat (returning fs.ErrExist), then performs a single-syscall
// os.Rename of the source. The move itself is atomic, so the source is never stranded and
// no concurrent edit is deleted by a separate unlink.
//
// The Lstat→rename window is NOT as narrow as a local-filesystem reading suggests
// (an honest re-assessment): the filesystems that reject the atomic flag
// and therefore reach this fallback are exactly the network/user-mode ones - NFS before
// 4.2, CIFS/SMB, several FUSE mounts - where each of the two operations is a network
// round-trip, so the window is milliseconds, not nanoseconds, and it is precisely the NAS
// client's expected deployment. A file created at `to` inside that window is replaced
// without a keep-both copy (the engine's fresh-create download promote and the resolver's
// set-aside are the two callers). No fix is applied: local filesystems - where the engine
// runs its own sync root in the supported configuration - support the flag and never reach
// this path, and closing the window portably would need a link-then-unlink (which
// reintroduces the source race this primitive exists to avoid) or an O_EXCL staging dance
// per platform. The residual is recorded in DESIGN §13.
func renameNoReplaceGeneric(from, to string) error {
	if _, err := os.Lstat(to); err == nil {
		return fs.ErrExist
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(from, to)
}
