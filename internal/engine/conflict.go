package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// conflictTimeLayout is the timestamp layout embedded in a keep-both conflict copy
// name (DESIGN §2): month-day-HHMMSS-year, e.g. "Jun-11-153004-2026".
const conflictTimeLayout = "Jan-02-150405-2006"

// errConflictServerDir marks a resolve-conflict op whose diverged server node is a
// directory rather than a file. Keep-both is scoped to file-content divergence (DESIGN
// §6), so the file-shaped resolution cannot run; like errParentConflicted this is
// terminal - the op is marked StateConflict and dead-lettered rather than retried
// forever (which would also leave the local file renamed aside but unrecorded, wedging
// the queue).
var errConflictServerDir = errors.New("engine: conflict server node is a directory")

// errConflictServerGone marks a resolve-conflict op whose diverged server node has since
// vanished (QueryNode reports ErrPathError - the node was deleted or moved server-side
// after the 24579 rejection). Keep-both has nothing to resolve against, and the server's
// authoritative delete/move event is funnelled through its own inbound op (reconcile.go),
// so this is terminal: the op is marked StateConflict and dead-lettered rather than
// retried forever, which would wedge the queue and block the authoritative inbound
// remove/rename that chains behind it on the same path.
var errConflictServerGone = errors.New("engine: conflict server node has vanished")

// errConflictNodeMoved marks a resolve-conflict op whose diverged node is no longer tracked at
// op.Path because a same-batch inbound rename relocated it - and the local file with it - to a
// DIFFERENT path between the funnel that enqueued this op and the time it drains (the rename
// serialises ahead of the resolve on op.Path). The node's divergence (if any) follows it to that
// new path, so resolveConflict re-funnels a fresh keep-both resolve there (refunnelResolveAtPath)
// and returns this sentinel to drop the now-stale op. Unlike the other terminal cases this does
// NOT mark the baseline StateConflict: the inbound rename already relocated the baseline off
// op.Path, so markOutboundConflict would synthesise a phantom record whose file id collides with
// (and deletes, via the byid index) the real moved baseline. The op is simply Acked and dropped;
// the re-funnelled resolve at the new path keeps both versions.
var errConflictNodeMoved = errors.New("engine: conflict server node moved to another path")

// errResolveNoServerID marks a resolve-conflict turn whose server node answered without a
// file id. The resolver needs the id to download the canonical version and to stamp the
// baseline, so the turn cannot proceed - but a single id-less reply is no proof of anything
// permanent (Node.FileID is tolerantly decoded, DESIGN §13), so it stays TRANSIENT and the op
// retries.
//
// It is typed so it can carry a deterministic-rejection streak (resolveNoServerIDCode,
// servererr.go) instead of a plain fmt.Errorf. This reply shape is also
// reachable from a second direction: serverDivergedForVanishedUpload now reads a hash-carrying,
// id-less reply as divergence and funnels it here, and on a DSM that systematically omits
// file_id the resolver then fails this way on every drain forever - a plain error carries no
// server result code, so the escalation never counted it, and blockedKeys serialised the
// authoritative inbound remove/rename of the same path behind it indefinitely. An unbroken run
// of the identical non-answer is the same determinism evidence an unbroken run of one unmapped
// errno is, so it rides the same machinery to the same observable exit (StateConflict + a
// durable trace).
var errResolveNoServerID = errors.New("engine: conflict server node has no file id")

// maxCopyNameComponentBytes bounds the on-disk basename of a conflict copy to the
// per-component limit common filesystems enforce (NAME_MAX is 255 bytes on ext4/APFS and
// the byte/UTF-16 length cap on Windows). Appending the host/timestamp/_Conflict marker to
// an already-long valid filename can otherwise push the component past that limit, where
// RenameNoReplace fails forever with ENAMETOOLONG and - since that is neither fs.ErrExist
// nor a recognised terminal error - wedges the resolver retrying the op every drain.
// Holding the basename within the limit keeps the move applyable.
const maxCopyNameComponentBytes = 255

// conflictCopyName derives the keep-both conflict-copy path for a divergent local
// file. relPath is the slash-separated, view-relative canonical path; the returned
// path keeps the same directory and inserts a
// "_<hostname>_<Jan-02-150405-2006>_Conflict" marker before the final extension
// (DESIGN §2). The split is on the final dot of the base name only, so a nested
// directory portion is preserved untouched, a name with no extension gets the marker
// appended whole, and a dotfile whose only dot leads the name (".bashrc") is treated
// as extensionless rather than splitting on its leading dot.
//
// The marker carries the copy's identity (host + timestamp), so the base name is truncated
// first - deterministically, on a UTF-8 rune boundary so a multibyte rune is never split - to
// keep the resulting on-disk component within maxCopyNameComponentBytes. A
// pathologically long extension can on its own exceed the budget the marker leaves, so the
// extension is then trimmed too, guaranteeing name+marker+ext stays within the limit for any
// realistic (bounded) hostname rather than only when the base name overflows.
func conflictCopyName(relPath, hostname string, t time.Time) string {
	return conflictCopyNameN(relPath, hostname, t, 0)
}

// conflictCopyNameN is conflictCopyName with preserveConflictCopy's collision counter: n > 0
// appends "_<n>" to the marker, so the counter is budgeted with the marker rather than eating
// into it. Trimming the already-assembled basename instead would cut the
// marker's tail - the "_Conflict" suffix DESIGN §2 defines and watcher/filter.go matches to
// recognise a keep-both copy as self-induced - turning an at-limit name into a copy the
// watcher re-reports as a user file. The whole basename (name + marker + counter + ext) stays
// within maxCopyNameComponentBytes, so RenameNoReplace can never fail with ENAMETOOLONG.
func conflictCopyNameN(relPath, hostname string, t time.Time, n int) string {
	dir, name, ext := splitBaseExt(relPath)
	stamp := t.Format(conflictTimeLayout)
	marker := "_" + hostname + "_" + stamp + "_Conflict"
	if n > 0 {
		marker += "_" + strconv.Itoa(n)
	}
	// Trim the extension if the marker alone leaves no room for it, then trim the base name
	// with whatever budget remains, so the full basename never exceeds the limit.
	if len(marker)+len(ext) > maxCopyNameComponentBytes {
		ext = truncateUTF8(ext, max(maxCopyNameComponentBytes-len(marker), 0))
	}
	if budget := maxCopyNameComponentBytes - len(marker) - len(ext); budget < len(name) {
		name = truncateUTF8(name, max(budget, 0))
	}
	return dir + name + marker + ext
}

// truncateUTF8 returns the longest prefix of s that is at most n bytes and ends on a UTF-8
// rune boundary, so a multibyte rune is never cut in half.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// splitBaseExt splits a slash-separated path into its directory portion, the base name
// without its final extension, and that extension (including the dot). The split is on
// the final dot of the base name only, never the leading dot at index 0, so a nested
// directory portion is preserved untouched, a name with no extension yields an empty
// ext, and a dotfile whose only dot leads the name (".bashrc") is treated as
// extensionless rather than splitting on its leading dot.
func splitBaseExt(relPath string) (dir, name, ext string) {
	dir, base := path.Split(relPath)
	name = base
	for i := len(base) - 1; i > 0; i-- {
		if base[i] == '.' {
			name = base[:i]
			ext = base[i:]
			break
		}
	}
	return dir, name, ext
}

// maxConflictCopyAttempts bounds the search for a free conflict-copy name before giving
// up; a directory with this many same-second collisions for one file is pathological.
const maxConflictCopyAttempts = 10000

// preserveConflictCopy atomically moves the divergent local file at abs aside to a free
// keep-both conflict-copy path, returning the chosen view-relative path and its resolved
// absolute path. Starting from conflictCopyName it claims each candidate with a no-replace
// rename (fsutil.RenameNoReplace fails with fs.ErrExist when the destination already
// exists): a single atomic move, so - unlike a hard-link-then-unlink - a concurrent
// replacement of the source cannot delete a fresh edit while the copy keeps the old inode,
// and the source is never removed by a second path resolution a swapped parent symlink
// could redirect. Unlike a stat-then-rename it also never silently clobbers a destination
// that appeared concurrently. Each fs.ErrExist advances the "_<n>" counter - covering a
// real user file that happens to match the name or a prior same-second conflict copy -
// until a free name is claimed. The move both preserves the divergent content and clears
// the canonical path, which is about to be overwritten with the server version.
// The no-replace rename runs through the engine's fsRenameNoReplace seam so a test can
// inject the host rejections this client cannot provoke on a NAME_MAX=255 filesystem - an
// ENAMETOOLONG from a mount whose per-component limit is lower than the copy-name budget.
func (e *Engine) preserveConflictCopy(viewID uint64, canonicalRel, abs string, t time.Time) (string, string, error) {
	// On a byte-preserving filesystem the divergent local file may sit at its original
	// (e.g. NFD) name rather than the NFC canonical path the op carries, so resolve the real
	// on-disk source before the move; the canonical NFC path is still what the server version
	// will be downloaded onto afterwards (Phase 9b).
	if _, lerr := os.Lstat(abs); lerr != nil && os.IsNotExist(lerr) {
		abs = e.onDiskAbs(abs)
	}
	for n := 0; n < maxConflictCopyAttempts; n++ {
		// Re-derive the whole name per attempt rather than splicing the counter into the
		// previous candidate: at the length limit the counter has to be budgeted alongside
		// the marker, not carved out of it (conflictCopyNameN).
		cand := conflictCopyNameN(canonicalRel, e.hostname, t, n)
		copyAbs, err := e.abs(viewID, cand)
		if err != nil {
			return "", "", err
		}
		// Resolve the copy destination's real on-disk parent. The conflict copy lands in the same
		// directory as the canonical path, which on a byte-preserving filesystem may be spelled NFD
		// while cand carries the canonical NFC path (Phase 9b). A literal-NFC RenameNoReplace would
		// park the copy under a parallel NFC twin directory next to the NFD original - the next scan
		// folds both spellings into one normpath.Key and protects the subtree, stopping its sync
		// (the same fold-collision the inbound promotes guard against). The prefix
		// variant reuses the existing NFD parent while the freshly-chosen copy leaf stays literal NFC.
		copyAbs, err = e.onDiskAbsPrefixNoSymlink(copyAbs)
		if err != nil {
			return "", "", err
		}
		rerr := e.fsRenameNoReplace(abs, copyAbs)
		if rerr == nil {
			return cand, copyAbs, nil
		}
		if errors.Is(rerr, fs.ErrExist) {
			// The candidate is taken; advance the counter rather than overwrite it.
			continue
		}
		// The failing candidate is returned ALONGSIDE the error: fsutil.RenameNoReplace can
		// fail after the move has already landed - its post-rename durability flush
		// (syncRenameDirs → fsyncDir) propagates a genuine I/O failure (EIO, ENOSPC, EMFILE),
		// degrading only a capability gap - so this error is not proof that no copy exists.
		// preserveAndRecord probes this path before rolling its speculative provenance row
		// back. The paths are meaningless to a caller that
		// only reads them on success, so returning them costs nothing there.
		return cand, copyAbs, rerr
	}
	return "", "", fmt.Errorf("engine: no free conflict-copy name for %q in view %d after %d attempts",
		canonicalRel, viewID, maxConflictCopyAttempts)
}

// preserveDivergentLocal sets the on-disk file at abs aside to a keep-both _Conflict copy and
// records a durable store.Conflict, returning preserved=true, when the file's content diverges
// from base (or base is empty, meaning the content is untracked - never synced - and is preserved
// whole). It is the shared keep-both funnel for the inbound apply paths that destroy local content
// without a server node to reconcile against - applyRemove (the server deleted the node) and
// applyRename's destination overwrite (the server moves a DIFFERENT node onto the path) - closing
// the cluster-A data-loss gap symmetrically with the inbound download guard (DESIGN §6, Law #1).
//
// It mirrors funnelDownloadConflict's durable-before-destroy, idempotent-on-replay contract: the
// Conflict record and the no-replace rename-aside both run BEFORE the caller's destructive fsutil
// op, so a crash in between leaves the file either still in place (the caller's op is un-Acked and
// re-detects the divergence on the next drain) or already preserved (the caller's destructive op
// then becomes a no-op - fsutil.Remove of a missing path is not an error, and a rename whose
// destination was vacated proceeds onto the now-free path). The preserved copy is an ordinary local
// file the next localScan re-detects and uploads, so both versions survive.
//
// A source that vanished or became non-regular between detection and here (errUploadSourceGone) has
// no content to lose, so it is not a divergence. A content match (hash == base) is the common
// in-sync case and is also not a divergence: the caller proceeds with its server-authoritative op.
func (e *Engine) preserveDivergentLocal(viewID uint64, canonicalRel, abs, base string) (bool, error) {
	sum, err := e.hashLocalFileMD4(abs)
	if err != nil {
		if errors.Is(err, errUploadSourceGone) {
			return false, nil
		}
		return false, err
	}
	if base != "" && sum == base {
		return false, nil // matches the last sync; the server op is authoritative
	}
	if err := e.preserveAside(viewID, canonicalRel, abs); err != nil {
		return false, err
	}
	return true, nil
}

// preserveAside moves the on-disk node at abs aside to a free keep-both _Conflict copy and records
// a durable store.Conflict, without any divergence check - the caller has already decided the node
// must be kept (preserveDivergentLocal after a file hash mismatch, or guardRemoveDir after a subtree
// scan). It works for a file or a whole directory (preserveConflictCopy's no-replace rename moves
// either). The Conflict record is written BEFORE the rename and corrected after, mirroring
// resolveConflict, so a crash between the bbolt write and the filesystem move never leaves content
// set aside with no provenance.
func (e *Engine) preserveAside(viewID uint64, canonicalRel, abs string) error {
	copyRel, _, err := e.preserveAndRecord(viewID, canonicalRel, abs, e.now())
	if err != nil {
		return err
	}
	e.log.Info("engine: inbound apply would destroy locally diverged content; preserved as keep-both copy",
		"view", viewID, "path", canonicalRel, "copy", copyRel)
	return nil
}

// applyResolveConflict performs DESIGN §6 keep-both resolution for one diverged file:
// the server version wins on the canonical path and the divergent local content is
// preserved as a _Conflict copy pushed back to the server as a new file, recorded in
// the durable conflicts list. The op is enqueued by the outbound stale-base (24579)
// funnel (outbound.go) and routed here from applyOp ahead of the Direction split, since
// the resolution touches both disk (download/rename) and the server (query/upload).
//
// On success the queue record is Acked. A structurally invalid canonical path is
// dead-lettered (errDeadLettered) like the inbound/outbound rejected-path cases, as are
// the terminal cases where the canonical path's parent is itself stuck in conflict
// (errParentConflicted) or the server replaced the path with a directory
// (errConflictServerDir) - both mark the baseline StateConflict and drop the op so it
// cannot wedge the queue. A name the local filesystem cannot hold (nameTooLong, from the
// copy rename or the download staging temp) is terminal for the same reason and on the
// same path. A parent whose server id is not yet known leaves the op queued
// (errParentUnknown), and a broken connection is re-dialed by WithWorker - both surface
// as a plain error that leaves the record for the next drain.
func (e *Engine) applyResolveConflict(ctx context.Context, qo store.QueuedOp) error {
	op := qo.Op
	// A view the account now holds read-only cannot accept the copy-upload half of
	// keep-both: the server would reject it as unauthorized, but only AFTER the resolver
	// has already renamed the local file aside and overwritten the canonical path with the
	// server version on disk - and a retry would then take the source-gone fast path and
	// ack without ever pushing or recording the preserved copy. Keep-both is out of scope
	// for read-only views (the plan: localScan never pushes them, so an inbound content
	// conflict is unreachable; an op enqueued while writable can still drain here after a
	// privilege downgrade). Guard before any disk mutation: mark the baseline StateConflict
	// and dead-letter, mirroring applyOutbound's read-only branch.
	if e.viewIsReadOnly(op.ViewID) {
		// A node relocated server-side must be handled even under a read-only view, BEFORE
		// markOutboundConflict runs: an inbound rename already moved the baseline (and the local
		// bytes) off op.Path, so markOutboundConflict(op.Path) would synthesise a phantom conflict
		// baseline at the now-absent op.Path whose file id collides with - and so deletes, via the
		// byid index - the real moved baseline. Re-funnel a fresh resolve at the node's new path
		// (it drains and dead-letters again under read-only, but without the corruption) and drop
		// this op without touching any baseline. The read-only early return otherwise bypasses
		// resolveConflict's identical moved-node guard.
		if newPath, moved, mErr := e.refunnelIfMoved(op); mErr != nil {
			return mErr
		} else if moved {
			e.log.Info("engine: dropping stale read-only resolve-conflict op; node moved server-side, re-funnelled at its new path",
				"view", op.ViewID, "path", op.Path, "to", newPath)
			if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
				return fmt.Errorf("engine: ack moved read-only resolve-conflict %d: %w", qo.Seq, ackErr)
			}
			return errDeadLettered
		}
		e.log.Warn("engine: resolve-conflict op targets a read-only view; marking conflict and dropping",
			"view", op.ViewID, "path", op.Path)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		// Record the trace and ack atomically (after the conflict-marking cleanup) so a retry
		// cannot double-count Dropped.
		if dlErr := e.deadLetterAndAck(op, qo.Seq, "resolve-conflict op targets a read-only view"); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if err := e.resolveConflict(ctx, op); err != nil {
		if errors.Is(err, errRejectedPath) {
			e.log.Warn("engine: dropping resolve-conflict op with rejected path",
				"view", op.ViewID, "path", op.Path, "err", err)
			// Record the trace and ack atomically so a retry cannot double-count Dropped.
			if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("resolve-conflict rejected path: %v", err)); dlErr != nil {
				return dlErr
			}
			return errDeadLettered
		}
		if errors.Is(err, errConflictNodeMoved) {
			// The node was relocated server-side and the local file moved with it; resolveConflict
			// already re-funnelled a fresh keep-both resolve at the node's new path. Drop this stale
			// op WITHOUT marking a conflict baseline - the inbound rename already relocated the
			// baseline off op.Path, so synthesising one here (markOutboundConflict) would strand a
			// phantom record whose file id collides with the real moved baseline in the byid index.
			e.log.Info("engine: dropping stale resolve-conflict op; node moved server-side, re-funnelled at its new path",
				"view", op.ViewID, "path", op.Path)
			if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
				return fmt.Errorf("engine: ack moved resolve-conflict %d: %w", qo.Seq, ackErr)
			}
			return errDeadLettered
		}
		if nameTooLong(err) {
			// The host filesystem cannot hold a name this resolution needs: the keep-both copy's
			// basename (preserveConflictCopy's no-replace rename) or the download staging temp
			// (promoteStagedDownload's CreateTemp). Both are budgeted to maxCopyNameComponentBytes
			// (255), which holds on ext4/APFS/NTFS but NOT on a mount whose per-component limit is
			// lower - eCryptfs, some FUSE/network mounts - where a perfectly valid source name still
			// produces an unappliable copy name. No retry ever shortens it, so left transient the
			// resolve retried every drain, escalated to Stuck and, via blockedKeys, serialised every
			// overlapping op (including the authoritative inbound modify/remove of the same path)
			// behind it forever. applyOp's shared classifier already treats this shape as terminal,
			// but it is routed past this verb's early return, so keep-both needs its own backstop -
			// the mirror of the inbound verbs' arm.
			//
			// Data-safe: whichever half failed, no local content is destroyed. A failed
			// aside-rename leaves the divergent file exactly where it was; a failed staging temp
			// leaves it already preserved as a _Conflict copy (with its durable provenance record)
			// that the next localScan re-detects and uploads. Only the server version is not
			// materialised, and the baseline is marked StateConflict so the path is not re-detected
			// as a fresh local create.
			e.log.Warn("engine: dropping resolve-conflict op whose name the filesystem cannot hold",
				"view", op.ViewID, "path", op.Path, "err", err)
			if cerr := e.markOutboundConflict(op); cerr != nil {
				return cerr
			}
			// Record the trace and ack atomically (after the conflict-marking cleanup) so a retry
			// cannot double-count Dropped.
			if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("resolve-conflict name unappliable: %v", err)); dlErr != nil {
				return dlErr
			}
			return errDeadLettered
		}
		if errors.Is(err, errParentConflicted) || errors.Is(err, errParentCorrupt) ||
			errors.Is(err, errConflictServerDir) || errors.Is(err, errConflictServerGone) ||
			errors.Is(err, errParentGone) {
			// Terminal: the canonical path's parent is itself stuck in conflict with no server id,
			// the parent baseline record is corrupt (unrecoverable), the server
			// replaced the path with a directory (out of keep-both's file-content scope), the
			// server node has vanished so there is nothing to keep-both against, or an id-less
			// resolve's parent directory was removed server-side (errParentGone, confirmed by the
			// ancestor walk - the applyUploadOut/applyMkdirOut mirror).
			// None can ever resolve, so mark the baseline StateConflict and dead-letter
			// the op rather than retrying forever and wedging the queue (mirrors applyOutbound's
			// errParentConflicted handling). For the vanished/gone cases the server's authoritative
			// delete/move funnels through its own inbound op, which can now apply once this
			// resolver stops blocking the shared path.
			e.log.Warn("engine: dropping unresolvable resolve-conflict op",
				"view", op.ViewID, "path", op.Path, "err", err)
			if cerr := e.markOutboundConflict(op); cerr != nil {
				return cerr
			}
			// Record the trace and ack atomically (after the conflict-marking cleanup) so a
			// retry cannot double-count Dropped.
			if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("resolve-conflict unresolvable: %v", err)); dlErr != nil {
				return dlErr
			}
			return errDeadLettered
		}
		// errParentUnknown and any transient transport/server error leave the op queued:
		// the parent's own op records its id (or the connection recovers) and the next
		// drain retries.
		return fmt.Errorf("engine: apply resolve-conflict %d/%q: %w", op.ViewID, op.Path, err)
	}
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack resolve-conflict %d: %w", qo.Seq, err)
	}
	return nil
}

// refunnelResolveAtPath re-enqueues a keep-both resolve op at newPath, where a same-batch inbound
// rename relocated the diverged node (and its local bytes) after this op was funnelled at the now-
// stale op.Path. It mirrors funnelUploadConflict/funnelDownloadConflict: the new op carries the
// node's file id (so the resolver's server-vanished disambiguation keys on the stable id), its
// perms, and the latest server sync id (so the resolved baseline records the right event). The
// original op is dropped by the caller (errConflictNodeMoved), so a crash between this enqueue and
// that drop re-runs the original, re-detects the relocation and re-enqueues - a harmless duplicate,
// since once the first new-path resolve runs the canonical path matches the server and the
// duplicate takes the false-conflict fast path.
func (e *Engine) refunnelResolveAtPath(op store.Op, newPath string) error {
	e.log.Info("engine: diverged node moved server-side; re-funnelling keep-both resolver to its new path",
		"view", op.ViewID, "from", op.Path, "to", newPath)
	if _, err := e.store.Enqueue(store.Op{
		Type:         store.OpResolveConflict,
		Direction:    op.Direction,
		ViewID:       op.ViewID,
		Path:         newPath,
		FileID:       op.FileID,
		UnixPerm:     op.UnixPerm,
		ServerSyncID: op.ServerSyncID,
	}); err != nil {
		return fmt.Errorf("engine: re-enqueue resolve-conflict %d/%q: %w", op.ViewID, newPath, err)
	}
	return nil
}

// refunnelIfMoved checks whether op's diverged node (by file id) has been relocated server-side
// off op.Path - an inbound rename serialised ahead of this resolve already moved the baseline (and
// the local bytes) to the node's new path. When so it re-funnels a fresh keep-both resolve at that
// new path and reports moved=true (with the new path) so the caller drops this stale op WITHOUT
// synthesising a phantom baseline at op.Path: that record's file id would collide with - and so
// delete, via the store byid index - the real moved baseline. The check is keyed on the fold of
// both paths so a case-only difference on a case-insensitive filesystem is not read as a relocation.
// A node with no file id, or still tracked at op.Path, reports moved=false. It is store-only (no
// network), so both the normal drain (resolveConflict) and the read-only early branch
// (applyResolveConflict) call it.
func (e *Engine) refunnelIfMoved(op store.Op) (newPath string, moved bool, err error) {
	if op.FileID == "" {
		return "", false, nil
	}
	cur, perr := e.store.PathByID(op.ViewID, op.FileID)
	if perr != nil && !isNotFound(perr) {
		return "", false, fmt.Errorf("engine: resolve relocated conflict node %d/%q: %w", op.ViewID, op.FileID, perr)
	}
	if perr == nil && normpath.Key(cur) != normpath.Key(op.Path) {
		if rerr := e.refunnelResolveAtPath(op, cur); rerr != nil {
			return "", false, rerr
		}
		return cur, true, nil
	}
	return "", false, nil
}

// trackedSpelling returns the path the keep-both resolver must address for op: op.Path, unless
// the store's byid index tracks op.FileID at a path that folds to the SAME key but is spelled
// differently - a case-only server rename applied by an inbound op after this resolve was
// funnelled. That is not a relocation (refunnelIfMoved's fold comparison is deliberate: a
// case-only difference on a case-insensitive filesystem must not read as a move), so the node
// keeps its baseline, its local bytes and its identity; only the spelling is stale. The byid
// index is the authoritative witness of the current one, exactly as moveSourceByFoldedKey
// (reconcile.go) ADOPTS the stored spelling on a fold match rather than re-deriving it.
//
// A node with no file id, an id the index does not track (not-found), or an already-current
// spelling reports op.Path unchanged. A store failure is surfaced so the op stays queued rather
// than resolving against a possibly stale path.
func (e *Engine) trackedSpelling(op store.Op) (string, error) {
	if op.FileID == "" {
		return op.Path, nil
	}
	cur, err := e.store.PathByID(op.ViewID, op.FileID)
	if err != nil {
		if isNotFound(err) {
			return op.Path, nil
		}
		return "", fmt.Errorf("engine: resolve conflict node spelling %d/%q: %w", op.ViewID, op.FileID, err)
	}
	// An empty or fold-different path is not a spelling of op.Path: the former is no witness at
	// all, the latter is a genuine relocation, which refunnelIfMoved (not this helper) owns.
	if cur == "" || normpath.Key(cur) != normpath.Key(op.Path) {
		return op.Path, nil
	}
	return cur, nil
}

// adoptResolveSpelling rewrites a queued keep-both resolve to the spelling the byid index
// currently tracks for its node, before ANY consumer reads its path. A
// case-only server rename is deliberately NOT a relocation (refunnelIfMoved folds both paths so a
// case-insensitive filesystem does not read one as a move), but it does leave op.Path - the
// enqueue-time spelling - byte-different from the authoritative one the inbound rename recorded.
// Every consumer is path-addressed: the QueryNode/download turns (a DSM that resolves its path
// parameter case-sensitively answers ErrPathError for the stale spelling forever, and
// resolveServerNodeAbsence then classifies the node "moved", whose exit leaves the op queued - a
// permanent transient that also starves every fold-equal op serialised behind it), and
// markOutboundConflict, which would otherwise write a phantom baseline at the stale key whose file
// id collides in the byid index with the real one.
//
// It runs in applyOp, OUTSIDE applyResolveConflict, because the deterministic-rejection escalation
// wraps that call and must name the same path: escalateDeterministicServerFailure marks the
// baseline (markOutboundConflict) and writes the dead-letter trace from the QueuedOp it was handed,
// so an adoption confined to the resolver's own copy would leave exactly the phantom-baseline
// collision above to the one exit that drops the op for good.
//
// A store failure is surfaced so the op stays queued rather than resolving against a stale path.
func (e *Engine) adoptResolveSpelling(qo store.QueuedOp) (store.QueuedOp, error) {
	cur, err := e.trackedSpelling(qo.Op)
	if err != nil {
		return qo, err
	}
	if cur == qo.Path {
		return qo, nil
	}
	e.log.Info("engine: adopting the tracked spelling of the diverged node's path for the keep-both resolve",
		"view", qo.ViewID, "from", qo.Path, "to", cur)
	qo.Path = cur
	return qo, nil
}

// resolveConflict carries the keep-both work for applyResolveConflict. It queries the
// server's current node for the canonical path; when the local file already hashes to
// the server hash it is a false conflict (the diverged push was redundant), so it just
// adopts the server facts onto the baseline and finishes. Otherwise the server version
// wins: the divergent local content is renamed to its conflict-copy name on disk, the
// server node is downloaded onto the canonical path (MD4-verified by Download), the
// canonical baseline is advanced to synced with the server facts, the preserved copy is
// uploaded back as a new file under the same parent, its baseline is recorded synced
// with the assigned file id, and a durable Conflict record is written.
//
// The whole resolution runs over a single leased worker connection so the query,
// download and copy upload share one turn. The copy name and the conflict record's
// detected-at timestamp are taken from a single e.now() reading so they stay consistent.
//
// The multi-step resolution is not transactional: if it fails after renaming the
// divergent local file aside but before recording the copy baseline, the op is retried
// with the local source now gone (sourceGone), so the retry only re-applies the
// server-wins download onto the canonical path. The preserved copy is not lost - it
// remains on disk and the next localScan re-detects and uploads it as an ordinary file.
// The durable Conflict provenance record is written immediately after the local file is
// set aside (before the download/upload) precisely so a later transient failure cannot
// drop it: once the server version lands on the canonical path a retry takes the
// false-conflict fast path and acks, so a record written only at the very end would be
// lost forever. The end-of-function write enriches that record with the copy's file id.
func (e *Engine) resolveConflict(ctx context.Context, op store.Op) error {
	if !containable(op.Path) {
		return fmt.Errorf("%w: resolve-conflict path %q in view %d", errRejectedPath, op.Path, op.ViewID)
	}

	// A same-batch inbound rename serialises ahead of this resolve on op.Path, so by the time
	// the resolver drains it has already relocated the diverged node - and the local bytes with
	// it - to the node's new path. The store's byid index, updated by that authoritative server
	// move, is the reliable witness: if op.FileID is now tracked at a DIFFERENT path, op.Path is
	// stale and the keep-both belongs at the new path. Re-funnel a resolve there and drop this op.
	// This handles a same-directory AND a cross-directory move uniformly. The old-parent listing
	// (resolveServerNodeAbsence) alone cannot: a cross-dir move leaves the old parent without the
	// node, so it is mis-read as a vanish, dead-lettered as "server gone", and markOutboundConflict
	// synthesises a phantom old-path conflict baseline whose file id collides with - and so deletes,
	// via the byid index - the real moved baseline. A same-dir move with no
	// downstream resolver would otherwise silently drop the divergence, losing the server modify on
	// the next scan with no keep-both copy. The relocation check (refunnelIfMoved)
	// is shared with applyResolveConflict's read-only early branch so both paths handle a moved node.
	if newPath, moved, mErr := e.refunnelIfMoved(op); mErr != nil {
		return mErr
	} else if moved {
		return fmt.Errorf("%w: resolve-conflict %d/%q -> %q", errConflictNodeMoved, op.ViewID, op.Path, newPath)
	}

	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	// Resolve the canonical path to its real on-disk spelling before hashing the local file,
	// setting it aside, and downloading the server version onto it. On a byte-preserving
	// filesystem (Linux, Windows) the divergent local file - and its parent directory - may sit
	// under a non-NFC (NFD) name while op.Path is the canonical NFC path (Phase 9b). A literal-NFC
	// download promote would materialise a parallel NFC twin next to the NFD original, which the
	// next scan folds into one normpath.Key and protects, silently stopping the subtree's sync -
	// the exact failure the inbound download/mkdir promotes guard against. The prefix
	// variant reuses the existing NFD parent (and the NFD file itself when it still exists) while a
	// vanished leaf stays literal NFC. preserveConflictCopy resolves the copy DESTINATION parent
	// the same way, so both halves of keep-both land inside the NFD directory.
	abs, err = e.onDiskAbsPrefixNoSymlink(abs)
	if err != nil {
		return err
	}

	ts := e.now()

	// Resolve the parent id before leasing so an unknown parent leaves the op queued
	// (errParentUnknown) without a wasted server turn. The conflict copy is created in the
	// same directory as the canonical path, so resolving the parent from op.Path is
	// equivalent to resolving it from a specific copy name - and it lets the copy name be
	// claimed atomically at preserve time (preserveConflictCopy) rather than chosen up front.
	parentID, err := e.resolveParentID(op.ViewID, op.Path)
	if err != nil {
		// Relocation/gone handling for the keep-both resolver - the mirror
		// applyUploadOut/applyMkdirOut got but resolveConflict did not. The parent
		// baseline is ABSENT (errParentBaselineAbsent), not merely an id-less pending parent
		// awaiting its own op: either a server-side rename of an ancestor relocated the whole
		// subtree's baselines (relocatePending) before this resolve drained, or an ancestor was
		// deleted. Waiting is a cyclic wedge - the authoritative inbound OpRename/OpRemove is
		// serialized BEHIND this resolve on the ancestor conflict key, so a plain transient
		// retry never converges (visible only as Stuck).
		if errors.Is(err, errParentBaselineAbsent) {
			// The RENAME case: re-base op.Path onto the node's new path and re-funnel a fresh
			// resolve there, dropping this stale op WITHOUT marking a conflict baseline
			// (errConflictNodeMoved) because relocatePending already moved that baseline off
			// op.Path. An id-carrying resolve's relocation is witnessed authoritatively by the
			// byid index in refunnelIfMoved ABOVE (it would have returned moved=true and we would
			// not be here), so this path is id-less only - an id-less op has no byid entry, so it
			// re-bases through the QUEUED inbound renames instead (the relocation's own producers,
			// still serialized behind this op), mirroring dropGoneParentBaseline's id-less arm.
			if op.FileID == "" {
				if reb := e.relocatedByQueuedInboundRenames(op.ViewID, op.Path); reb != op.Path {
					if rerr := e.refunnelResolveAtPath(op, reb); rerr != nil {
						return rerr
					}
					return fmt.Errorf("%w: resolve-conflict %d/%q -> %q", errConflictNodeMoved, op.ViewID, op.Path, reb)
				}
			}
			// The DELETE case, for BOTH forms: no relocation covers the path, so the parent is
			// genuinely gone. An id-carrying resolve reaches here only when refunnelIfMoved found
			// no byid relocation - i.e. the node's baseline was DROPPED by an ancestor's applied
			// delete (dropDescendantBaselines), not moved, so the byid entry gives no positive
			// proof and the server walk is the only witness left (the id-carrying
			// half, previously wedged as transient forever behind this resolve). Confirm
			// the parent gone with the same ancestor walk the outbound create verbs use and
			// dead-letter terminally (errParentGone) so the inbound remove serialized behind this
			// resolve can finally apply; a parent that is actually live reports false and the op
			// stays the plain transient wait below.
			if e.parentGoneConfirmed(ctx, op.ViewID, op.Path) {
				return fmt.Errorf("%w: resolve-conflict %d/%q", errParentGone, op.ViewID, op.Path)
			}
		}
		return err
	}

	// Hash the divergent local content up front (disk only), capturing the on-disk (size,
	// mtime) from the same descriptor so a false-conflict adoption records the local facts
	// rather than the server's mtime. A source that vanished in the
	// scan→resolve window has no content left to preserve, so the resolution degrades to a
	// plain "server wins" download of the canonical path with no copy.
	localHash, localFI, herr := e.hashLocalFileMD4WithInfo(abs)
	sourceGone := errors.Is(herr, errUploadSourceGone)
	if herr != nil && !sourceGone {
		return herr
	}

	return e.withWorker(ctx, func(c syncClient) error {
		viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
		server, qerr := c.QueryNode(ctx, viewID, op.Path)
		if qerr != nil {
			if errors.Is(qerr, actions.ErrPathError) {
				// ErrPathError (28672) means EITHER the node is gone (deleted/moved server-side
				// after the 24579 rejection) OR a server-side operation timed out (errors.go,
				// DESIGN §3) - indistinguishable from this reply alone. Dead-lettering
				// unconditionally would permanently ack a still-unresolved conflict on a mere
				// transient timeout. Disambiguate exactly as applyRemoveOut does:
				// only a clean parent listing that omits the node is positive proof it vanished, in
				// which case there is nothing left to keep-both against - dead-letter it
				// (errConflictServerGone) so it cannot wedge the authoritative inbound remove/rename
				// queued behind it on the same path. An inconclusive listing (a
				// timeout, so the listing itself errors) or a node still present leaves the op queued
				// to retry. No disk mutation has happened yet.
				pc, ok := c.(outboundClient)
				if !ok {
					return errNoOutbound
				}
				gone, moved, cerr := resolveServerNodeAbsence(ctx, pc, e, viewID, op.Path, op.FileID)
				if cerr == nil && gone {
					return fmt.Errorf("%w: resolve-conflict %d/%q", errConflictServerGone, op.ViewID, op.Path)
				}
				if cerr == nil && moved && sourceGone {
					// The node was renamed/moved server-side away from op.Path (still present under
					// another name) AND the local file is gone from op.Path too - the same-batch
					// inbound rename that moved the node server-side also carried the local bytes to
					// the new path, where a separate resolve op handles any divergence. Nothing here
					// to keep-both against, so drop this stale op (errConflictNodeMoved) instead of
					// retrying QueryNode(op.Path) - which will ErrPathError forever - and wedging the
					// queue. Gated on sourceGone so a path that still holds divergent
					// local content is never dropped without preserving it.
					return fmt.Errorf("%w: resolve-conflict %d/%q", errConflictNodeMoved, op.ViewID, op.Path)
				}
				// Still present at the leaf, moved-but-local-content-present, or inconclusive:
				// surface the original path error so the op stays queued (a broken listing
				// connection is re-dialed by WithWorker via Broken()).
				return qerr
			}
			return qerr
		}
		if server.FileID == "" {
			// A 24579 rejection means the server holds a diverged version, so its node must
			// exist; an empty id is an unexpected reply. Leave the op queued to retry rather
			// than recording a baseline with no id - but as the TYPED errResolveNoServerID, so
			// an id-less reply that repeats identically across drains escalates to the same
			// observable terminal exit an unmapped server rejection gets instead of wedging the
			// path forever (this shape is also reachable from the vanished-upload funnel).
			return fmt.Errorf("%w: resolve-conflict %d/%q", errResolveNoServerID, op.ViewID, op.Path)
		}
		if server.IsDir {
			// The server replaced this path with a directory. Keep-both resolves file-content
			// divergence only (DESIGN §6); downloading a directory node as a file would corrupt
			// the path and the dead-letter happens before any disk mutation (the rename-aside
			// below), so no half-applied state is left behind.
			return fmt.Errorf("%w: resolve-conflict %d/%q", errConflictServerDir, op.ViewID, op.Path)
		}

		// False conflict: the local file already matches the server content (the push that hit
		// 24579 was redundant). Adopt the server facts and finish - no copy, no record.
		if !sourceGone && server.Hash != "" && localHash == server.Hash {
			return e.adoptFalseConflict(op, server, server.Hash, localFI.Size(), localFI.ModTime().Unix())
		}
		// The fast path above proves equality in exactly one form - a non-empty server hash. A
		// query_node reply that omits it (an unconfirmed reply shape, DESIGN §13) leaves the
		// equality UNPROVABLE, and an earlier version degraded that to size+identity evidence to
		// spare the deliberately harmless duplicate replays (funnelUploadConflict,
		// refunnelResolveAtPath) a _Conflict copy plus two transfers of content already identical
		// on both sides. That degrade is withdrawn: identity is no evidence of equality HERE. A
		// modify's resolve addresses its own node, so `op.FileID == server.FileID` holds by
		// construction and the guard collapses to a bare size comparison - on a path the server
		// has just PROVED diverged by rejecting the push (24579). Another client's same-sized edit
		// then satisfies it, and adopting records StateSynced with the LOCAL hash as the agreed
		// base; the next server event's divergence gate finds nothing diverged and applyDownload
		// overwrites the unpushed local edit with no _Conflict copy. The storm the degrade avoided
		// is a cost; this is a silent loss, and the never-lose doctrine settles the trade (DESIGN
		// §6). A hash-less reply therefore falls through to the real keep-both below - one copy
		// plus two transfers, and nothing lost - with a Warn naming the evidence that was missing.
		if !sourceGone && server.Hash == "" {
			e.log.Warn("engine: resolve-conflict server node carried no content hash; equality is unprovable, resolving as a real keep-both",
				"view", op.ViewID, "path", op.Path, "size", server.Size, "file_id", server.FileID)
		}

		// Real conflict: preserve the divergent local content as a copy before the server
		// version overwrites the canonical path, then download the server version onto the
		// canonical path and push the preserved copy back as a new file (DESIGN §6 keep-both).
		copyPreserved := false
		var copyRel, copyAbs string
		if !sourceGone {
			r, a, perr := e.preserveAndRecord(op.ViewID, op.Path, abs, ts)
			if perr != nil {
				return perr
			}
			copyRel, copyAbs = r, a
			copyPreserved = true
		}

		if err := e.downloadCanonical(ctx, c, op, server, abs, ts); err != nil {
			return err
		}

		if !copyPreserved {
			// Nothing to preserve; the canonical path is resolved.
			return nil
		}
		return e.pushConflictCopy(ctx, c, op, parentID, copyRel, copyAbs, ts)
	})
}

// adoptFalseConflict resolves the keep-both false-conflict case: the local file already
// hashes to the server content (the diverged push that hit 24579 was redundant), so the
// baseline simply adopts the server facts as StateSynced - no copy is preserved and no
// Conflict record is written.
//
// LocalSize/LocalMTime record the ACTUAL on-disk facts (localSize/localMTime, read from the
// same descriptor that hashed the content) rather than the server's - the file is not touched
// here (no SetMeta), so its on-disk mtime keeps whatever the user's edit left. Recording the
// server mtime instead would leave the baseline's (size, mtime) permanently mismatched against
// the file, so the Phase-9d cheap-scan hint never matches and every later scan re-opens and
// re-hashes this file forever.
//
// localHash is what the LOCAL bytes hash to, which on this path is the SHARED hash - the
// equality it proved is the only thing that gets here (an unprovable one now falls through to
// the real keep-both, see the caller). It is passed explicitly rather than read off the server
// node so LocalHash always describes the file on disk: a baseline whose LocalHash is empty would
// read as diverged from its own file on every later scan.
func (e *Engine) adoptFalseConflict(op store.Op, server actions.Node, localHash string, localSize, localMTime int64) error {
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = server.FileID
		b.FileType = fileType(false)
		b.ServerHash = server.Hash
		b.ServerMTime = server.MTime
		b.LocalHash = localHash
		b.LocalSize = localSize
		b.LocalMTime = localMTime
		b.UnixPerm = op.UnixPerm
		if op.ServerSyncID > b.ServerSyncID {
			// Stamp the funnelling download's sync id so the resolved baseline records the
			// latest observed server event - but NEVER rewind it (mirroring
			// the monotonic markSynced guard in applyDownload): a funneled/refunneled resolve
			// carries the OLD triggering event's sync id, and lowering the recorded value below
			// a self-echo move's id would destroy downloadRelocatedBySelfEcho's "cur is strictly
			// newer" proof in the crash-replay window. The strict inequality subsumes the former
			// non-zero guard - a resolve op that carries none (e.g. an outbound or cold-start
			// funnel) never exceeds a stored value, so it still never zeroes the existing one.
			b.ServerSyncID = op.ServerSyncID
		}
	})
}

// preserveAndRecord sets the divergent local file at abs aside to a free keep-both
// conflict-copy path and writes a durable Conflict provenance record, returning the
// chosen copy's view-relative and absolute paths. The copy name is claimed atomically
// with a no-replace rename so a concurrent write cannot be silently clobbered and a
// concurrent source replacement cannot be deleted out from under us.
//
// The Conflict record is written BEFORE the file is moved aside (keyed by the canonical
// path with the prospective copy name) and corrected after: the rename and the provenance
// write cannot be one atomic step (one is a filesystem op, the other a bbolt commit), so a
// crash between preserveConflictCopy's rename and the post-claim write would otherwise
// leave the file set aside with no durable record - and the retry, seeing the source gone,
// takes the false-conflict fast path and never writes one. Pre-writing the
// first-candidate name closes the common (no-collision) window; the post-claim write
// corrects the name if the no-replace claim had to advance its counter, and the
// end-of-resolution write (pushConflictCopy) enriches it with the copy's assigned file id.
//
// The pre-write covers the CRASH window only; a failure it can observe is rolled back, because
// the caller may drop the op terminally (nameTooLong), which would otherwise
// strand the speculative row as a permanent claim of a _Conflict copy that was never created.
// The rollback restores the record the pre-write displaced rather than deleting blindly: the same
// canonical-path key also carries an EARLIER resolution's provenance and, in downloadCanonical's
// intruder retry, the original divergent copy's - deleting those would lose the provenance of
// content that really is parked on disk. It restores that prior on
// evidence, not on trust: a prior stranded by a crash in this very window names no copy at all and
// must not be resurrected (priorConflictCopyProvablyAbsent).
//
// A preserveConflictCopy error is NOT proof that no copy exists: fsutil.RenameNoReplace fsyncs
// the destination directory after the move and propagates a genuine I/O failure from it, so the
// aside can have landed under the returned error. rollbackSpeculativeConflict therefore rolls back
// only when the copy is provably absent - the host rejecting the name outright, or an on-disk
// probe of the failing candidate saying so.
func (e *Engine) preserveAndRecord(viewID uint64, canonicalRel, abs string, ts time.Time) (copyRel, copyAbs string, err error) {
	prior, hadPrior, gerr := e.store.GetConflict(viewID, canonicalRel)
	if gerr != nil {
		return "", "", gerr
	}
	if perr := e.store.PutConflict(viewID, store.Conflict{
		CanonicalPath: canonicalRel,
		CopyPath:      conflictCopyName(canonicalRel, e.hostname, ts),
		DetectedAt:    ts.Unix(),
	}); perr != nil {
		return "", "", perr
	}

	r, a, perr := e.preserveConflictCopy(viewID, canonicalRel, abs, ts)
	if perr != nil {
		e.rollbackSpeculativeConflict(viewID, canonicalRel, r, a, ts, prior, hadPrior, perr)
		return "", "", perr
	}

	if perr := e.store.PutConflict(viewID, store.Conflict{
		CanonicalPath: canonicalRel,
		CopyPath:      r,
		DetectedAt:    ts.Unix(),
	}); perr != nil {
		return "", "", perr
	}
	return r, a, nil
}

// rollbackSpeculativeConflict undoes preserveAndRecord's pre-write after the aside-rename it
// speculated on failed: it restores the record the pre-write displaced, or removes the row
// entirely when the canonical path had none. It is best-effort - the caller's error is the one
// that must reach applyResolveConflict's terminal classifier (nameTooLong et al.), so a store
// failure here is logged, not returned; the stale row is then no worse than before this rollback
// existed and the next resolution of the same path overwrites it.
//
// It rolls back only when the copy is PROVABLY absent (copyRel/copyAbs name the candidate
// preserveConflictCopy failed on; both are empty when it failed before attempting any rename).
// fsutil.RenameNoReplace's post-rename directory fsync can fail on real I/O after the move has
// landed, so a bare error is no proof - and a rollback there would delete the provenance of a
// _Conflict copy that now exists on disk, recreating the very "content parked without provenance"
// class the pre-write closes. A name the host rejects outright IS proof (see
// conflictCopyProvablyAbsent). When the candidate is present, or its absence is unprovable, the row
// is instead CORRECTED to name it: the retry cannot write one (it sees the source gone and takes
// the false-conflict fast path), and the next localScan uploads that copy as an ordinary file.
//
// The prior row is restored on the SAME evidence rule rather than trusted blindly.
// A prior can itself be speculative: a crash between an earlier attempt's pre-write and
// its aside-rename leaves a row naming a copy that was never created, and restoring that here -
// on a path the caller is about to drop terminally (nameTooLong), so no later
// resolution overwrites it - would make the phantom permanent, recreating exactly the artefact the
// rollback exists to prevent. So the prior is restored only when its copy is NOT provably absent;
// an inconclusive probe restores (fail toward protected, DESIGN §6), a definitive absence deletes.
func (e *Engine) rollbackSpeculativeConflict(viewID uint64, canonicalRel, copyRel, copyAbs string,
	ts time.Time, prior store.Conflict, hadPrior bool, renameErr error) {
	var rerr error
	switch {
	case copyAbs != "" && !e.conflictCopyProvablyAbsent(copyAbs, renameErr):
		rerr = e.store.PutConflict(viewID, store.Conflict{
			CanonicalPath: canonicalRel,
			CopyPath:      copyRel,
			DetectedAt:    ts.Unix(),
		})
		if rerr == nil {
			e.log.Warn("engine: keep-both aside-rename failed after the copy landed; provenance "+
				"record kept for the parked copy",
				"view", viewID, "path", canonicalRel, "copy", copyRel)
			return
		}
	case hadPrior && !e.priorConflictCopyProvablyAbsent(viewID, prior):
		rerr = e.store.PutConflict(viewID, prior)
	case hadPrior:
		e.log.Warn("engine: dropping a keep-both provenance record whose copy is not on disk",
			"view", viewID, "path", canonicalRel, "copy", prior.CopyPath)
		rerr = e.store.DeleteConflict(viewID, canonicalRel)
	default:
		rerr = e.store.DeleteConflict(viewID, canonicalRel)
	}
	if rerr != nil {
		e.log.Warn("engine: could not roll back speculative keep-both conflict record",
			"view", viewID, "path", canonicalRel, "error", rerr)
	}
}

// conflictCopyProvablyAbsent reports whether nothing exists at the conflict-copy candidate a
// failed aside-rename targeted.
//
// nameTooLong(renameErr) is proof on its own, ahead of any probe: a name this filesystem cannot
// represent cannot name an entry that exists, so the move provably did not land - and the probe
// CANNOT establish that, because Lstat of the same unrepresentable name fails the same way and
// would be read as "unprovable". Left to the probe, the very case
// the terminal name-too-long drop exists for - a mount whose per-component limit is below the
// copy-name budget - would end its terminal drop by writing a provenance row for a _Conflict copy
// that can never exist, making `conflicts` permanently misleading about content that is really
// still at its canonical path. The classifier is the same one applyOp uses to call such a name
// terminal.
//
// Otherwise only a definitive IsNotExist counts (or the same name-rejection surfacing from the
// probe itself): an inconclusive Lstat (EIO on the failing mount, a permission-denied parent) must
// NOT be read as permission to drop the provenance of possibly-parked content - fail toward
// protected, never toward loss (DESIGN §6). The candidate was free when the rename ran (an
// occupied one returns fs.ErrExist and advances the counter instead), so a present entry is the
// aside itself in every case but a concurrent creation inside the failure window - and pointing
// the row at that is still the safe direction: a stale row the next resolution overwrites.
func (e *Engine) conflictCopyProvablyAbsent(copyAbs string, renameErr error) bool {
	if nameTooLong(renameErr) {
		return true
	}
	_, err := e.fsConflictCopyLstat(copyAbs)
	return err != nil && (os.IsNotExist(err) || nameTooLong(err))
}

// priorConflictCopyProvablyAbsent reports whether the _Conflict copy a PRE-EXISTING provenance row
// claims is provably not on disk, i.e. whether the row is itself speculative - written by an
// earlier attempt's pre-write whose aside-rename never landed (a crash in that window, the one the
// pre-write exists to survive). There is no rename error to reason from here, so the on-disk probe
// is the only evidence: a definitive IsNotExist (or the host rejecting the recorded name, which
// nothing can ever create) proves the copy was never parked, while anything else - an unreadable
// mount, a containment failure resolving the recorded path - is inconclusive and keeps the row,
// because a row for content that really is parked is the one thing that must never be lost.
//
// An empty CopyPath claims no copy at all, so there is nothing its removal can strand.
//
// The probe repeats under onDiskAbs when the literal (NFC) path is missing: on a byte-preserving
// filesystem the parked copy can sit under an NFD-spelled parent while the record carries the
// canonical spelling (Phase 9b, as in preserveConflictCopy), and reading that mismatch as absence
// would delete the provenance of a real copy.
func (e *Engine) priorConflictCopyProvablyAbsent(viewID uint64, prior store.Conflict) bool {
	if prior.CopyPath == "" {
		return true
	}
	abs, err := e.abs(viewID, prior.CopyPath)
	if err != nil {
		return false
	}
	if !e.conflictCopyProvablyAbsent(abs, nil) {
		return false
	}
	if real := e.onDiskAbs(abs); real != abs {
		return e.conflictCopyProvablyAbsent(real, nil)
	}
	return true
}

// downloadCanonical lets the server version win on the canonical path: it downloads the
// server node (MD4-verified by Download) onto abs and advances the canonical baseline to
// StateSynced with the server facts. downloadNodeToPath promotes with a no-replace rename,
// so a fresh local file that landed at the canonical path during the resolution is not
// clobbered: it fails with fs.ErrExist, the intruder is set aside as its own conflict copy
// with a durable Conflict record (preserveAndRecord - preserved on disk for the next
// localScan to upload, never parked without provenance) and the promotion retries onto the
// now-free path.
//
// The download reply is decoded tolerantly (decodeNode), so its node id, hash and size may
// each be omitted. The baseline must describe the bytes ACTUALLY on disk, not the QueryNode
// metadata: a file id is stable across modifications, so the server file could have changed
// content between QueryNode and Download (Download skips MD4 verification when the reply
// carries no hash), in which case falling back to server.Hash/server.Size would record a
// baseline that disagrees with disk and make the next scan re-detect a phantom change. So
// the hash and size are derived from the STREAMED BYTES when the reply omits them -
// authoritative regardless of any concurrent server-side change - inside the staging temp
// before the promote (deriveStagedDownloadFacts), never by re-reading the promoted path
// afterwards: that read would swallow a concurrent local edit into the baseline as
// server-agreed content, permanently. When the reply DOES carry a hash,
// Download already verified it against the streamed bytes, so no re-hash is needed.
// The file id is not content, so the stable server.FileID remains a safe fallback
// for indexing.
func (e *Engine) downloadCanonical(ctx context.Context, c syncClient, op store.Op, server actions.Node, abs string, ts time.Time) error {
	// downloadNodeToPath returns both mtime axes: the wire value it wrote (the reply's mtime, or
	// server.MTime when the reply omitted one) and the value the filesystem actually stored.
	var node actions.Node
	var canonicalMTime, localMTime int64
	for attempt := 0; ; attempt++ {
		n, mt, onDisk, derr := e.downloadNodeToPath(ctx, c, op, server, abs)
		if derr == nil {
			node, canonicalMTime, localMTime = n, mt, onDisk
			break
		}
		if errors.Is(derr, fs.ErrExist) && attempt < maxConflictCopyAttempts {
			// preserveAndRecord, not bare preserveConflictCopy: the intruder is content being
			// set aside, so it gets a durable Conflict record BEFORE the rename like every
			// other set-aside (never park content without provenance). The record shares the
			// canonical-path key, so the end-of-resolution write (pushConflictCopy) points it
			// back at the original divergent copy on success; until then - including a crash
			// right here - it names the most recently parked copy.
			if _, _, perr := e.preserveAndRecord(op.ViewID, op.Path, abs, ts); perr != nil {
				return perr
			}
			continue
		}
		return derr
	}
	canonicalID := node.FileID
	if canonicalID == "" {
		canonicalID = server.FileID
	}
	// node.Hash/node.Size are complete: downloadNodeToPath derived whatever the reply omitted
	// from the sealed staging temp (deriveStagedDownloadFacts), so they describe the bytes that
	// streamed. They are NOT re-derived from abs - this resolution just promised to protect
	// whatever lands on the canonical path, and re-hashing it would swallow an edit that landed
	// in the promote→record window into the baseline as server-agreed content
	// (the window here is the wider one, spanning SetMeta/SyncDir and the intruder
	// retries above).
	canonicalHash := node.Hash
	canonicalSize := node.Size
	// Mirror applyDownload: LocalMTime is the mtime the
	// promote observed the filesystem store (an out-of-range canonicalMTime was skipped by
	// SetMeta, a coarse mount rounded it), or the cheap-scan hint never matches and the resolved
	// file re-hashes on every scan forever. ServerMTime keeps the raw wire value.
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = canonicalID
		b.FileType = fileType(false)
		b.ServerHash = canonicalHash
		b.ServerMTime = canonicalMTime
		b.LocalHash = canonicalHash
		b.LocalSize = canonicalSize
		b.LocalMTime = localMTime
		b.UnixPerm = op.UnixPerm
		if op.ServerSyncID > b.ServerSyncID {
			// Record the funnelling download's sync id on the resolved canonical baseline, but
			// NEVER rewind it (mirroring adoptFalseConflict): a funneled
			// resolve carries the OLD event's sync id, and rewinding below a self-echo move's id
			// would destroy the redirect gate's "strictly newer" proof on crash-replay. The
			// strict inequality subsumes the former non-zero guard (a funnel that carries none
			// never exceeds a stored value).
			b.ServerSyncID = op.ServerSyncID
		}
	})
}

// pushConflictCopy uploads the preserved _Conflict copy at copyAbs back to the server as a
// new file under the canonical path's parent, records its baseline StateSynced, and writes
// the durable Conflict record enriched with the copy's assigned file id. It is the final
// keep-both phase: the canonical path is already resolved by downloadCanonical, so a copy
// that cannot be indexed (the upload reply, possibly via a recovery QueryNode, yields no
// file id) is deferred to the next local scan rather than recorded as an unindexed baseline.
func (e *Engine) pushConflictCopy(ctx context.Context, c syncClient, op store.Op, parentID, copyRel, copyAbs string, ts time.Time) error {
	viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	pc, ok := c.(outboundClient)
	if !ok {
		return errNoOutbound
	}
	copyName := path.Base(copyRel)
	copyNode, copyHash, copySize, copyMTime, uerr := e.uploadConflictCopy(ctx, pc, op.ViewID, parentID, copyName, copyAbs)
	if uerr != nil {
		return uerr
	}
	// The upload reply is decoded tolerantly, so its hash and mtime may be absent; seed the
	// copy's server facts from it and enrich them from any recovery QueryNode below.
	copyFileID := copyNode.FileID
	copyServerHash := copyNode.Hash
	copyServerMTime := copyNode.MTime
	if copyFileID == "" {
		// The upload reply omitted a file id (the exact key is unconfirmed against a live
		// DSM, DESIGN §13); recover it by querying the copy path so the baseline is indexed.
		// A query that BREAKS the connection must propagate so WithWorker re-dials the
		// poisoned worker rather than the error being swallowed and a dead connection released
		// back to the pool (matching recoverUploadFileID). A clean lookup
		// failure leaves the id empty and falls through to the deferred-baseline branch below.
		// When it succeeds it also yields the complete server hash/mtime the upload reply
		// lacked, so adopt those too rather than recording a sparse synced baseline.
		existing, eqerr := c.QueryNode(ctx, viewID, copyRel)
		if eqerr != nil {
			if br, ok := c.(brokenReporter); ok && br.Broken() {
				return eqerr
			}
		} else if !existing.IsDir {
			copyFileID = existing.FileID
			if existing.Hash != "" {
				copyServerHash = existing.Hash
			}
			if existing.MTime != 0 {
				copyServerMTime = existing.MTime
			}
		}
	}
	// The copy content is exactly what we just uploaded, so its server hash equals the local
	// content hash we computed; fall back to it (and the local mtime) when the server replies
	// omitted them, so the synced baseline never carries an empty hash that the next scan would
	// re-detect as a change.
	if copyServerHash == "" {
		copyServerHash = copyHash
	}
	if copyServerMTime == 0 {
		copyServerMTime = copyMTime
	}
	if copyFileID == "" {
		// Still no id: the copy did reach the server (the upload returned a node) but we
		// cannot index it. Recording a StateSynced baseline with an empty FileID would leave
		// an unindexed record that localScan, seeing the on-disk copy match its baseline,
		// never re-pushes - so it would stay unindexed forever and a later server move/delete
		// (keyed by file id) could misclassify it. Instead, skip the copy baseline entirely so
		// the next localScan re-detects the on-disk copy as a fresh file and uploads it under
		// the same path (a no-op modify server-side) with a readable id. The canonical path is
		// already resolved and recorded above, so the resolution still succeeds; record the
		// durable Conflict for provenance (its CopyFileID is informational only).
		e.log.Warn("engine: conflict copy upload returned no file id; deferring its baseline to the next local scan",
			"view", op.ViewID, "canonical", op.Path, "copy", copyRel)
		return e.store.PutConflict(op.ViewID, store.Conflict{
			CanonicalPath: op.Path,
			CopyPath:      copyRel,
			DetectedAt:    ts.Unix(),
		})
	}
	if err := e.markSynced(op.ViewID, copyRel, func(b *store.Baseline) {
		b.FileID = copyFileID
		b.FileType = fileType(false)
		b.ServerHash = copyServerHash
		b.ServerMTime = copyServerMTime
		b.LocalHash = copyHash
		b.LocalSize = copySize
		b.LocalMTime = copyMTime
		b.UnixPerm = op.UnixPerm
	}); err != nil {
		return err
	}
	return e.store.PutConflict(op.ViewID, store.Conflict{
		CanonicalPath: op.Path,
		CopyPath:      copyRel,
		CopyFileID:    copyFileID,
		DetectedAt:    ts.Unix(),
	})
}

// hashLocalFileMD4 streams the MD4 of the local file at abs through the same no-follow,
// regular-file-only descriptor the upload path uses (openUploadSource), so a swapped-in
// symlink or non-regular file is refused rather than hashed. A vanished or swapped source
// surfaces as errUploadSourceGone.
func (e *Engine) hashLocalFileMD4(abs string) (string, error) {
	sum, _, err := e.hashLocalFileMD4WithInfo(abs)
	return sum, err
}

// hashLocalFileMD4WithInfo is hashLocalFileMD4 that also returns the on-disk FileInfo read
// from the SAME no-follow descriptor used to hash (openUploadSource's fstat), so a caller
// recording a baseline gets the size/mtime of the exact bytes it hashed with no re-stat
// TOCTOU. A vanished or swapped source surfaces as errUploadSourceGone.
func (e *Engine) hashLocalFileMD4WithInfo(abs string) (string, os.FileInfo, error) {
	f, fi, err := e.openUploadSource(abs)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()
	sum, herr := hashReaderMD4(f)
	if herr != nil {
		return "", nil, herr
	}
	return sum, fi, nil
}

// downloadNodeToPath streams the server node (server.FileID) into a staging temp in the
// destination directory, fsyncs and renames it onto abs, and applies its metadata -
// the shared promoteStagedDownload sequence without applyDownload's
// inbound baseline bookkeeping (the caller records the baseline). A node larger than
// limits.UploadResumeThreshold is fetched in offset-based resume_download chunks so a
// network drop resumes from the last offset instead of restarting at 0 (mirroring the
// inbound resumeDownloadToTemp path, DESIGN §4); a smaller node keeps the single-frame
// Download. Either way the content MD4 is verified (single-frame by Download, chunked by
// resumeDownloadInto against server.Hash) and partial bytes may be left in the temp on
// error, so the temp is promoted only on a clean download and always removed afterwards.
//
// The resume branch reuses the connection c already leased by resolveConflict's WithWorker
// rather than leasing again (resumeDownloadInto, not resumeDownloadToTemp), since a nested
// lease can deadlock at numWorkers=2.
//
// The promotion is a NO-REPLACE rename (fsutil.RenameNoReplace): the caller has just
// emptied the canonical path (preserveConflictCopy moved the divergent local file aside,
// or the source had already vanished), so a file present at abs now is a fresh local edit
// that landed during the resolution. Clobbering it with a replacing rename would silently
// destroy that edit and then mark the baseline synced, so the next scan never re-detects
// the loss - defeating the very keep-both guarantee. Instead the promotion fails with
// fs.ErrExist and the caller (resolveConflict) sets the intruder aside as its own conflict
// copy before retrying, so no concurrent write is lost.
//
// The download reply is decoded tolerantly, so node.MTime may be absent (zero). Applying
// a zero mtime would stamp the file with the Unix epoch and leave the on-disk mtime
// disagreeing with the baseline mtime the caller resolves via the server.MTime fallback.
// So the mtime WRITTEN to disk is node.MTime when present, else server.MTime, and that
// resolved wire value is returned for the caller's ServerMTime axis -
// alongside the mtime the filesystem actually STORED for it (promoteStagedDownload /
// stagedOnDiskMTime), which is what the caller must record as LocalMTime: on a coarse mount the
// two differ, and a baseline holding the unrounded value re-hashes the resolved file on every
// scan forever.
func (e *Engine) downloadNodeToPath(ctx context.Context, c syncClient, op store.Op, server actions.Node, abs string) (actions.Node, int64, int64, error) {
	var node actions.Node
	var wireMTime int64
	fill := func(tmp *os.File) (int64, error) {
		if server.Size > e.limits.UploadResumeThreshold {
			// Large diverged server node: stream it in offset-based resume_download chunks over the
			// connection resolveConflict already leased, so a mid-transfer network drop resumes from
			// the last offset instead of re-fetching from 0 every retry (mirroring the inbound path).
			mt, rerr := e.resumeDownloadInto(ctx, c, op.ViewID, op.Path, server.FileID, server.Hash, server.Size, tmp)
			if rerr != nil {
				return 0, rerr
			}
			node = actions.Node{FileID: server.FileID, Hash: server.Hash, Size: server.Size, MTime: mt}
		} else {
			n, derr := c.Download(ctx, server.FileID, e.viewRootNodeID(op.ViewID), op.ServerSyncID, tmp)
			if derr != nil {
				return 0, derr
			}
			node = n
		}
		// Fill in whatever the tolerantly-decoded reply omitted from the staging temp, before
		// the promote makes the bytes reachable by other writers.
		if err := deriveStagedDownloadFacts(tmp, &node); err != nil {
			return 0, err
		}
		wireMTime = node.MTime
		if wireMTime == 0 {
			wireMTime = server.MTime
		}
		return wireMTime, nil
	}
	onDiskMTime, err := e.promoteStagedDownload(abs, false, op.UnixPerm, fill, nil)
	if err != nil {
		return actions.Node{}, 0, 0, err
	}
	return node, wireMTime, onDiskMTime, nil
}

// uploadConflictCopy pushes the preserved _Conflict copy at abs to the server under
// parentID/name, picking the single-frame Upload for a small file or the chunked
// resume_upload for a large one (uploadResumeThreshold), mirroring applyUploadOut. It
// returns the server-assigned node plus the local content hash/size/mtime so the caller
// can record the copy's baseline. The source is opened once through the no-follow,
// regular-file-only descriptor so a swap cannot redirect the bytes.
func (e *Engine) uploadConflictCopy(ctx context.Context, pc outboundClient, viewID uint64, parentID, name, abs string) (actions.Node, string, int64, int64, error) {
	f, fi, oerr := e.openUploadSource(abs)
	if oerr != nil {
		return actions.Node{}, "", 0, 0, oerr
	}
	defer f.Close()
	size := fi.Size()
	mtime := fi.ModTime().Unix()

	if size <= e.limits.UploadResumeThreshold {
		content := make([]byte, size)
		if _, rerr := io.ReadFull(f, content); rerr != nil {
			return actions.Node{}, "", 0, 0, fmt.Errorf("read conflict copy %q: %w", abs, rerr)
		}
		hash := md4.ContentHash(content)
		n, uerr := pc.Upload(ctx, parentID, name, mtime, e.viewRootNodeID(viewID), content)
		if uerr != nil {
			return actions.Node{}, "", 0, 0, uerr
		}
		return n, hash, size, mtime, nil
	}
	// Hash exactly size bytes (io.LimitReader), NOT the descriptor to EOF: resumeUploadFile
	// streams precisely size bytes (for offset < size), so should the renamed-aside copy grow
	// in the fstat→hash window the advertised hash must still cover only the streamed bytes or
	// the server completion check never converges and the op wedges on every drain - mirroring
	// applyUploadOut's large branch and the small branch above, which reads only size bytes.
	hash, herr := hashReaderMD4(io.LimitReader(f, size))
	if herr != nil {
		return actions.Node{}, "", 0, 0, fmt.Errorf("hash conflict copy %q: %w", abs, herr)
	}
	if _, serr := f.Seek(0, io.SeekStart); serr != nil {
		return actions.Node{}, "", 0, 0, fmt.Errorf("seek conflict copy %q: %w", abs, serr)
	}
	n, uerr := e.resumeUploadFile(ctx, pc, viewID, f, parentID, name, size, hash, mtime)
	if uerr != nil {
		return actions.Node{}, "", 0, 0, uerr
	}
	return n, hash, size, mtime, nil
}
