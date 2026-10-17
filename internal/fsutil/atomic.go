// Package fsutil provides the on-disk apply primitives the sync engine uses to
// materialise remote changes locally: directory creation, rename, idempotent
// remove, post-rename directory fsync and metadata (mtime/perm) application.
//
// File contents are not written through this package: the engine streams a
// download into a staging temp in the destination directory, fsyncs it, and
// renames it into place itself (see internal/engine/inbound_download.go), then
// applies metadata via SetMeta. Permissions arrive from the wire as a
// decimal-encoded Unix mode (DESIGN §7); SetMeta converts that to an octal
// os.FileMode before chmod, and skips chmod entirely on Windows.
package fsutil

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

// syncDir is the directory-fsync seam Mkdir/Rename/Remove route through so a
// crash immediately after a directory mutation cannot lose the new/removed entry.
// It is a package var (defaulting to SyncDir) so tests can record that the
// durability flush actually fires for a created/removed directory.
var syncDir = SyncDir

// SyncDir flushes the directory entry changes for dir to durable storage, so a
// rename into dir survives a crash that immediately follows it. os.Rename is
// atomic with respect to a concurrent reader, but the parent directory's updated
// entry is not guaranteed persistent until the directory itself is fsynced -
// without this, a crash right after a rename can lose the new entry, leaving the
// store's StateSynced baseline recording a file the directory no longer lists,
// which the next local scan would diff as a deletion and push to the server
// (DESIGN §7).
//
// The platform-specific flush lives in fsyncDir: a POSIX directory fsync on Unix,
// and on Windows a FlushFileBuffers over a FILE_FLAG_BACKUP_SEMANTICS directory
// handle. The Windows arm used to be a no-op on the claim that a directory handle
// cannot be flushed at all; that claim is empirically false and the omission left
// every Windows apply with no durability guarantee whatsoever.
// Both arms degrade a capability gap (a filesystem that cannot flush a directory,
// or a directory that refuses the open the flush needs) to non-durable success
// with one Warn per process, and propagate a genuine I/O failure.
func SyncDir(dir string) error {
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsutil: sync dir %s: %w", dir, err)
	}
	return nil
}

// Mkdir creates the directory at path, including any missing parents
// (MkdirAll semantics). It is a no-op if the directory already exists.
//
// After creating the leaf it fsyncs the parent of EVERY directory it created so
// each new directory entry is durable across a crash that immediately follows -
// without it, a crash right after an inbound OpMkdir can lose an entry while the
// store already records a StateSynced baseline for the directory, which the next
// local scan would diff as a deletion and push to the server (DESIGN §7).
// os.MkdirAll may create several intermediate directories at once;
// fsyncing only the leaf's parent would leave those intermediate entries
// non-durable, so mkdirAllDurable flushes the parent of each.
// When the directory already exists the leaf's parent is flushed anyway, so a
// retry after a failed flush repeats it (see mkdirAllDurable).
func Mkdir(path string) error {
	return mkdirAllDurable(path, 0o755)
}

// MkdirAllDurable creates path (with MkdirAll semantics, applying perm to every
// directory it creates) and fsyncs the parent directory of each, so a freshly
// created directory entry survives a crash that immediately follows. It exists
// for callers outside the engine's apply path that need the same durable-create
// discipline with their own permission bits - the store creates the 0700 state
// dir chain through it - where Mkdir is fixed at the
// sync-tree 0755.
func MkdirAllDurable(path string, perm os.FileMode) error {
	return mkdirAllDurable(path, perm)
}

// mkdirAllDurable creates path (with MkdirAll semantics) and fsyncs the parent
// directory of every directory it actually created, so a crash immediately after
// cannot lose a freshly created directory entry - including an intermediate one
// when MkdirAll fills in more than one missing ancestor.
//
// When this call created nothing (path already exists) it still flushes the leaf's
// parent. That flush is what makes the durability RETRYABLE (the
// same unconditional-refsync recipe store.Open uses): the created-set
// is computed before the MkdirAll, so a genuine flush failure - SyncDir degrades a
// capability gap (ENOTSUP/EACCES) but deliberately propagates a real EIO/ENOSPC -
// leaves the directory on disk with a non-durable entry AND returns an error, and the
// op's retry then finds the directory existing, computes an empty created-set and, in
// the one-shot form, skipped the flush forever: applyMkdir wrote a StateSynced
// baseline and acked, so a power loss before writeback lost the entry and the next
// scan diffed the synced-baseline-without-directory into an outbound OpRemove of the
// server directory and its whole subtree. Re-flushing an already-durable entry is
// idempotent and costs one directory fsync (on a clean directory inode there is
// nothing to write back), which is also paid by
// Rename's destination-parent chain - the same retry guarantee for a parent
// directory a previous Rename created but could not flush.
func mkdirAllDurable(path string, perm os.FileMode) error {
	created := missingAncestors(path)
	if err := os.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("fsutil: mkdir %s: %w", path, err)
	}
	if len(created) == 0 {
		return syncDir(filepath.Dir(path))
	}
	// Attempt EVERY created directory's parent and aggregate the failures rather than
	// returning on the first: missingAncestors reports the chain deepest-first, so an early
	// return would leave the shallower ancestors' entries unflushed - and the op's retry then
	// finds the whole chain existing, computes an empty created-set and re-flushes only the
	// leaf's parent (above), so those shallower entries would never become durable. That is
	// the same one-shot skip this function's unconditional re-flush exists to close, just one
	// level up.
	//
	// The aggregation guarantees one ATTEMPT per created ancestor, not a retry of a failed
	// one: if a multi-level create sees a SELECTIVE failure (the flush of an intermediate
	// ancestor's parent fails while the leaf's succeeds), the retry's empty created-set
	// re-flushes only the leaf's parent, so that intermediate entry can stay non-durable
	// until some later call happens to flush it. Closing that too would need the failed
	// directories remembered across calls; it is recorded as a residual in DESIGN §13.
	var errs []error
	for _, dir := range created {
		if err := syncDir(filepath.Dir(dir)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// missingAncestors returns path and each ancestor directory that does not yet
// exist, stopping at the first existing ancestor - i.e. exactly the directories
// os.MkdirAll(path) will create. Fsyncing each returned directory's parent after
// the MkdirAll makes every new directory entry durable, not just the leaf's.
func missingAncestors(path string) []string {
	var missing []string
	for p := path; ; {
		if _, err := os.Lstat(p); err == nil {
			break // p exists, so every ancestor above it exists too
		}
		missing = append(missing, p)
		parent := filepath.Dir(p)
		if parent == p {
			break // reached the filesystem root
		}
		p = parent
	}
	return missing
}

// Rename moves the entry at from to to, creating the destination's parent
// directory if it does not yet exist. The parent chain is created durably
// (mkdirAllDurable) so a crash cannot lose a freshly created intermediate
// directory, and after the rename both affected directories are fsynced - the
// destination's parent so the new entry is durable, and (when the move crossed
// directories) the source's parent so the old entry's removal is durable too
// (syncRenameDirs).
//
// The move itself goes through RenameReplace, so a Windows destination whose only
// obstacle is FILE_ATTRIBUTE_READONLY does not wedge the inbound rename in a
// forever-transient retry; the caller has already proved the
// destination may be replaced (guardRenameDest).
func Rename(from, to string) error {
	if err := mkdirAllDurable(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	if err := renameReplace(from, to); err != nil {
		return fmt.Errorf("fsutil: rename %s -> %s: %w", from, to, err)
	}
	return syncRenameDirs(from, to)
}

// syncRenameDirs makes a completed rename durable on both sides: the destination
// directory's new entry first, then - when the move crossed directories - the
// source directory's removed entry. POSIX guarantees neither until the directory
// itself is fsynced; flushing only the destination leaves a crash window where
// the entry survives at BOTH paths, and the next local scan would diff the
// resurrected source as a brand-new create and push a duplicate to the server
// (the safe direction, duplication not loss, but the same
// directory-flush durability class as the destination flush). A same-directory rename needs
// only the one flush.
func syncRenameDirs(from, to string) error {
	toDir := filepath.Dir(to)
	if err := syncDir(toDir); err != nil {
		return err
	}
	if fromDir := filepath.Dir(from); fromDir != toDir {
		if err := syncDir(fromDir); err != nil {
			return err
		}
	}
	return nil
}

// Remove deletes the entry at path. It is idempotent: a missing path is not an
// error. Directories are removed recursively.
//
// After the removal it fsyncs the parent directory so the entry's disappearance
// is durable across a crash that immediately follows - without it, a crash right
// after an inbound OpRemove can resurrect the entry while the store has already
// dropped its baseline, which the next local scan would diff as a brand-new
// create and push back to the server (DESIGN §7). The parent
// may itself be absent (an idempotent remove of an already-gone subtree), in
// which case there is nothing to flush and the missing parent is not an error.
func Remove(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("fsutil: remove %s: %w", path, err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// warnMalformedPerm emits the malformed-wire-permission warning SetMeta fires
// when it skips the chmod; a package var so tests can capture it without
// swapping the global slog default (mirroring warnFsyncDegraded).
var warnMalformedPerm = func(path string, perm uint32, err error) {
	slog.Warn("fsutil: malformed wire permission; skipping chmod",
		"path", path, "perm", perm, "err", err)
}

// maxMtimeSeconds bounds an accepted Unix-seconds mtime. Converting a seconds
// mtime to nanoseconds - time.Unix(mtime,0).UnixNano(), on BOTH the Unix
// (unix.NsecToTimeval) and Windows (os.Chtimes) paths - int64-overflows
// silently for mtime greater than ~9.2e9 s, exactly what a non-conformant DSM
// reporting a millisecond-scale mtime lands past, and the resulting timestamp is
// either kernel-clamped to a wrong value (Linux: a never-matching cheap-scan
// (size,mtime) hint, so the file re-hashes on every scan forever) or rejected
// outright by the filesystem (Windows/macOS FILETIME/APFS) - and that rejection
// happens AFTER the download promote-rename, so the op transient-retries and the
// file re-downloads on every drain forever (the
// forever-transient-after-promote class, there closed for the perm axis). ~4e9 s
// reaches year ~2096, past any legitimate file mtime, with headroom below the
// overflow point. A negative mtime is rejected the same way.
const maxMtimeSeconds int64 = 4_000_000_000

// warnMalformedMtime emits the out-of-range-mtime warning SetMeta fires when it
// skips the mtime stamp; a package var so tests can capture it without swapping
// the global slog default (mirroring warnMalformedPerm).
var warnMalformedMtime = func(path string, mtime int64) {
	slog.Warn("fsutil: out-of-range mtime; skipping mtime stamp",
		"path", path, "mtime", mtime)
}

// MTimeOutOfRange reports whether a Unix-seconds mtime falls outside the range
// SetMeta can stamp to disk - negative, or past maxMtimeSeconds where the
// nanosecond conversion int64-overflows. SetMeta warns and SKIPS the mtime stamp
// for such a value, leaving the file's ~now promote timestamp untouched, so a
// caller that records the raw wire mtime as a baseline's LocalMTime would build a
// cheap-scan (size, mtime) hint that never matches the on-disk mtime and re-hash
// the file on every scan forever; it must derive the recorded mtime from the
// staged file on disk instead (read back
// from the sealed staging temp - the engine's stagedOnDiskMTime). A
// zero mtime (the "unset"/unknown wire form) is NOT out of range - SetMeta simply
// leaves the modification time untouched - so it reports false, matching SetMeta's
// own `mtime != 0 && MTimeOutOfRange(mtime)` skip gate.
func MTimeOutOfRange(mtime int64) bool {
	return mtime < 0 || mtime > maxMtimeSeconds
}

// SetMeta applies file metadata to path: it sets the modification time from the
// Unix-seconds mtime, and sets permissions from unixPermDecimal - the wire form
// of a Unix mode, the octal mode written as base-10 digits (e.g. 755 == 0o755,
// 2775 == 0o2775, decoded by decimalToMode). On
// Windows the chmod is skipped, since Unix permission bits are not meaningful
// there. A zero mtime leaves the modification time untouched; a zero
// unixPermDecimal (the wire's "unset"/unknown form) leaves permissions untouched
// rather than chmodding the file to 0000. A malformed unixPermDecimal (a digit
// 8/9, not valid wire-decimal) is warned about and the chmod skipped rather
// than returned as an error: SetMeta runs AFTER the download promote/rename, so
// failing here would leave the op retried (and the file re-downloaded) forever
// over metadata the server itself sent malformed. An
// out-of-range mtime (negative, or past maxMtimeSeconds where the nanosecond
// conversion int64-overflows) is warned about and the mtime stamp skipped for
// the same reason - the perm axis's mirror on the mtime axis;
// the perm still applies.
//
// On Unix both mutations run over a single O_NOFOLLOW descriptor of the target
// (setMetaNoFollow): SetMeta is the one engine apply step
// that runs AFTER the promote-rename, so a name-based os.Chmod/os.Chtimes
// re-resolved the path in that window and FOLLOWED a symlink swapped onto the
// freshly promoted file - with the setuid/setgid mapping, a hostile
// server perm 4755 would have landed ModeSetuid on whatever the link pointed
// at, anywhere outside the sync tree. A symlink occupying the target now fails
// the open (ELOOP), surfacing as an ordinary transient error the next drain's
// pre-apply symlink discipline handles. On Windows the mtime is still applied
// by name - a documented acceptance, see setmeta_windows.go.
func SetMeta(path string, mtime int64, unixPermDecimal uint32) error {
	applyPerm := runtime.GOOS != "windows" && unixPermDecimal != 0
	var mode os.FileMode
	if applyPerm {
		m, err := decimalToMode(unixPermDecimal)
		if err != nil {
			warnMalformedPerm(path, unixPermDecimal, err)
			applyPerm = false
		} else {
			mode = m
		}
	}
	if mtime != 0 && MTimeOutOfRange(mtime) {
		warnMalformedMtime(path, mtime)
		mtime = 0
	}
	if mtime == 0 && !applyPerm {
		return nil
	}
	return setMetaNoFollow(path, mtime, mode, applyPerm)
}

// decimalToMode converts a decimal-encoded Unix permission (the wire form: the
// octal mode read as base-10 digits, e.g. 755 or 2775) into an os.FileMode.
// The lower nine permission bits map directly, but the setuid/setgid/sticky
// special bits (octal 0o4000/0o2000/0o1000) must be translated to their
// os.FileMode positions (os.ModeSetuid/ModeSetgid/ModeSticky, bits 23/22/20):
// os.Chmod only honours the FileMode forms, so returning the raw octal value
// silently applied a wire 2775 as 0775 (decision - map them
// on Unix rather than document the drop; Windows never reaches the chmod). Any
// higher bits (a full st_mode's file-type bits) are dropped - only permission
// is ever applied.
func decimalToMode(unixPermDecimal uint32) (os.FileMode, error) {
	octal, err := strconv.ParseUint(strconv.FormatUint(uint64(unixPermDecimal), 10), 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid decimal perm %d: %w", unixPermDecimal, err)
	}
	mode := os.FileMode(octal & 0o777)
	if octal&0o4000 != 0 {
		mode |= os.ModeSetuid
	}
	if octal&0o2000 != 0 {
		mode |= os.ModeSetgid
	}
	if octal&0o1000 != 0 {
		mode |= os.ModeSticky
	}
	return mode, nil
}
