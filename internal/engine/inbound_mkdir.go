package engine

// This file carries the inbound OpMkdir apply path: it creates the directory on disk
// (idempotently, with parents) and advances its baseline to synced, plus the
// rejected-move-recovery case (relocateTrackedDirForMkdir) where a create/modify
// mkdir's directory node is already tracked at a different on-disk path.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/store"
)

// applyMkdir creates the directory (idempotently, with parents) and advances its
// baseline to synced.
func (e *Engine) applyMkdir(op store.Op) error {
	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	// On a byte-preserving filesystem (Linux, Windows) the directory - or a parent along its
	// path - may live under a non-NFC (NFD) spelling while the op carries the canonical NFC
	// path (Phase 9b). Resolve the real on-disk spelling first, mirroring applyDownload:
	// a literal-NFC MkdirAll would otherwise create a parallel NFC twin
	// directory next to the NFD one (and materialise every future descendant under it), which
	// the next scan folds into one normpath.Key and protects as a collision - the subtree
	// stops syncing until manual repair. The prefix resolution keeps not-yet-existing
	// components literal, so a genuinely new directory still materialises under NFC while an
	// existing NFD-spelled directory (target or parent) is reused.
	abs, err = e.onDiskAbsPrefixNoSymlink(abs)
	if err != nil {
		return err
	}
	// A regular FILE occupying the mkdir's target is a file<->dir collision the server-
	// authoritative dir-create is about to land on: without this gate
	// MkdirAll fails ENOTDIR, the op dead-letters, the cleanup drops the (pending-dir)
	// baseline, and the next scan re-uploads the now-untracked file into the same collision
	// - a silent re-upload cycle the cold start funnels via planColdConflict but the
	// incremental path never resolved. Dispatch on the reconcile-captured pre-batch base
	// (BaseWasSynced/BaseLocalHash/BaseIsDir, the OpRemove capture precedent): a
	// last-synced file whose on-disk content still matches is cleared under server
	// authority, anything diverged/untracked/unverifiable is preserved as a _Conflict copy
	// - never lost - and either way the path is freed so the mkdir below lands. A
	// directory occupant keeps the plain idempotent mkdir; a symlink/non-regular occupant
	// keeps the existing dead-letter path (mirroring protectReplacedTracked's refusal to
	// treat it as user content).
	if fi, lerr := os.Lstat(abs); lerr == nil && fi.Mode().IsRegular() {
		if gerr := e.guardMkdirOccupant(op, abs); gerr != nil {
			return gerr
		}
	}
	// A directory node already tracked (by file id) at a *different* on-disk path is one a
	// rejected move re-tracked at its still-reachable source while the server now reports it
	// (and this create/modify) at the destination. The directory already exists there with its
	// whole subtree, so a plain mkdir would create an empty directory at op.Path and markSynced
	// below would reindex only the root through the byid index - stranding the subtree and its
	// descendant baselines at the old prefix, where a later parent delete (which only reaches
	// op.Path) can never remove them. Move the whole subtree across instead so root and
	// descendants stay together. Only the destination being appliable reaches here at all (abs
	// above rejects a still-present symlink), so this runs precisely once the rejected move
	// becomes possible.
	if op.FileID != "" {
		relocated, rerr := e.relocateTrackedDirForMkdir(op, abs)
		if rerr != nil {
			return rerr
		}
		if relocated {
			return nil
		}
	}
	if err := fsutil.Mkdir(abs); err != nil {
		return err
	}
	// Apply the server mtime to the new directory so its baseline's recorded LocalMTime
	// matches what is on disk at creation time. Without this the baseline claimed
	// op.LocalMTime while the directory carried its creation time, so a later local scan
	// would read the gap as a local modification. The mkdir op carries no
	// permission bits (UnixPerm is unset/0), so SetMeta sets only the mtime and skips
	// chmod.
	//
	// This is necessarily best-effort: a directory's mtime is a function of its entries,
	// so any later child create/download/remove - in this batch or a future cycle - bumps
	// the parent's mtime again while its baseline still records the server value.
	// No ordering within the executor can keep a directory's mtime equal to the
	// server's while the directory is actively syncing children, so directory mtime is not
	// a usable content-change signal. The Phase-6c local scan (the only consumer of the
	// baseline mtime, and a separate phase per DESIGN §6) must therefore compare directory
	// mtime advisorily, if at all - never as proof of a local modification.
	if err := fsutil.SetMeta(abs, op.LocalMTime, op.UnixPerm); err != nil {
		return err
	}
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = op.FileID
		b.FileType = fileType(true)
		b.LocalMTime = op.LocalMTime
		// Record the server mtime too. Normally markSynced loads the pending baseline
		// reconcile pre-wrote (which carried ServerMTime via baselineFromEvent) and this
		// preserves it; but if a dead-lettered sibling op dropped that pending record,
		// markSynced starts from an empty baseline and ServerMTime would stay 0.
		// The mkdir op's LocalMTime carries the event mtime, which for a freshly-
		// created directory is also its server mtime, so stamp it here unconditionally -
		// matching the value baselineFromEvent would have written.
		b.ServerMTime = op.LocalMTime
		if op.ServerSyncID != 0 {
			// As in applyDownload: keep the event's sync id even if a dead-lettered sibling
			// op dropped the pending baseline that carried it.
			b.ServerSyncID = op.ServerSyncID
		}
	})
}

// guardMkdirOccupant resolves a regular file occupying an inbound mkdir's target path,
// the cluster-A never-lose matrix over the reconcile-captured
// pre-batch base (removeBaseFor, shared with the inbound remove gate):
//
//   - a last-synced FILE base with a recorded hash → divergence base: on-disk content that
//     still matches is cleared under server authority (the server replaced the file with a
//     directory), a cheap-scan-missed edit is preserved as a _Conflict copy first.
//   - anything else (untracked, non-synced, a DIR base a regular file locally replaced, or the
//     anomalous synced-file-without-hash inbound-rename write) → removeBaseFor returns
//     an empty base and the occupant is preserved whole as a _Conflict copy; there is no
//     trustworthy base to compare against, so nothing is ever destroyed on a guess.
//
// Preservation moves the occupant aside (preserveDivergentLocal/preserveAside write the
// durable Conflict record before the no-replace rename), so on return the path is free for
// the caller's MkdirAll either way. Idempotent on replay: a re-drain finds the path free
// (or a directory) and never reaches this gate again.
func (e *Engine) guardMkdirOccupant(op store.Op, abs string) error {
	base := removeBaseFor(op)
	preserved, err := e.preserveDivergentLocal(op.ViewID, op.Path, abs, base)
	if err != nil {
		return err
	}
	if !preserved {
		// The on-disk content matches the last-synced base (or vanished mid-check): the
		// server-authoritative replacement proceeds - clear the occupant so the mkdir lands.
		//
		// Through the removeNode seam like every other subtree delete an apply performs. The
		// occupant can be a DIRECTORY, so this call can lose the same readdir/rmdir race the
		// seam exists to make drivable, and the classifier's verb gate has to be exercisable
		// here too. Calling fsutil.Remove directly left this one site outside the only test
		// that can reach that errno.
		return removeNode(abs)
	}
	return nil
}

// relocateTrackedDirForMkdir handles a create/modify mkdir whose directory node is already
// tracked at a different on-disk path - the rejected-move-recovery case described in
// applyMkdir. When that old path actually holds the directory on disk it renames the whole
// subtree to op.Path, carries the descendant baselines to the new prefix, and writes the
// synced root baseline at op.Path (whose byid re-point drops the old root record), so root
// and descendants move together rather than the subtree stranding at the old prefix.
//
// It returns (true, nil) when it handled the op, and (false, nil) - leaving applyMkdir's
// plain mkdir path to run - whenever this is an ordinary mkdir: the node is untracked, is
// already tracked at op.Path, is not a directory, or has no subtree reachable on disk at the
// old path (so there is nothing to move and a plain mkdir is correct). The old-path lookups
// are guarded by file id, so a foreign node sitting where this one used to be is never moved.
//
// The relocation is not atomic - the disk rename precedes the baseline moves - so it is made
// replay-safe the same way applyRename is: if a crash (or a SetMeta failure) interrupts it
// after the disk rename, a replay finds the old path gone but the destination present and
// still finishes the descendant-baseline relocation rather than falling back to a plain mkdir
// that would strand them.
//
// Serialisation note (serialize.go "byid-resolved side effects"): only op.Path (the rename
// destination) is in this op's serializeKeys; the byid-resolved old path (the rename source) is
// NOT, so the scheduler does not order this subtree move against a concurrent op that owns the
// old path. The interleaving is direction-safe because the move is gated on byid identity (the
// old path's baseline carries op.FileID) and, on a race, the source is at worst re-scanned as
// vanished and re-tracked by file id - self-healing, never a silent loss.
func (e *Engine) relocateTrackedDirForMkdir(op store.Op, abs string) (bool, error) {
	oldPath, err := e.store.PathByID(op.ViewID, op.FileID)
	if isNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve tracked dir %d/%q: %w", op.ViewID, op.FileID, err)
	}
	if oldPath == op.Path {
		return false, nil
	}
	ob, err := e.store.GetBaseline(op.ViewID, oldPath)
	switch {
	case isNotFound(err):
		return false, nil
	case errors.Is(err, store.ErrCorruptRecord):
		// The by-id-resolved old baseline no longer decodes (bbolt checksums only its meta
		// pages, so a latent value corruption surfaces here at read time). Per the
		// apply-side rule (warnCorruptBaseline): an ErrCorruptRecord read is NEVER surfaced as
		// a plain error - the executor classifies that transient and retries every drain
		// forever (blocking overlapping successors), and the healing write that would clear the
		// key can never run. Treat it as absent (the isNotFound direction) and fall back to
		// applyMkdir's plain mkdir: its markSynced Put at op.Path drops the orphaned old record
		// through the byid re-point (putBaselineTx's drop-moved-orphan), clearing the corrupt
		// key, and nothing on disk is touched - the non-destructive direction for this
		// classification read.
		//
		// Accepted degradation: the plain-mkdir fallback re-points only
		// the ROOT through the byid index. If the corrupt directory has DESCENDANT baselines,
		// those keep their own (intact) records at the OLD prefix - relocateDescendantBaselines
		// never runs - so the subtree stays tracked at the old path while the server node
		// materialises at op.Path. The next local scan then re-uploads the stranded old subtree
		// as a fresh directory in parallel with the server subtree (a server-side subtree
		// DUPLICATION). This is non-destructive (nothing on disk is deleted, no local edit
		// lost) and bounded - it needs a directory-baseline bit-flip, a rare event - and clears
		// once the corruption is repaired or the install is re-paired. The descendants case is
		// pinned by TestApplyMkdirCorruptRelocateWithDescendantsDuplicates.
		e.warnCorruptBaseline("mkdir-relocate-old", op.ViewID, oldPath, err)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("load tracked dir baseline %d/%q: %w", op.ViewID, oldPath, err)
	}
	if ob.FileID != op.FileID || ob.FileType != "dir" {
		return false, nil
	}
	oldAbs, err := e.abs(op.ViewID, oldPath)
	if err != nil {
		// The old path is no longer safe to touch (escapes, or a symlink crept along it);
		// leave it for the Phase-6c local scan and fall back to a plain mkdir at op.Path.
		return false, nil //nolint:nilerr // intentional: unsafe old path falls back to mkdir
	}
	// Resolve the old path's real on-disk spelling before probing it: on a byte-preserving
	// filesystem the tracked directory may sit under a non-NFC (NFD) name while oldPath is the
	// canonical NFC key, so a bare NFC Lstat would miss the subtree, fall through to a plain mkdir
	// at op.Path, and strand the descendant baselines at the old prefix - re-uploaded as a
	// duplicate next scan (mirrors applyRemove). A resolution landing on a
	// symlink component is unsafe to move, so fall back to a plain mkdir like the abs failure above.
	oldAbs, err = e.onDiskAbsNoSymlink(oldAbs)
	if err != nil {
		return false, nil //nolint:nilerr // intentional: unsafe resolved path falls back to mkdir
	}
	switch _, statErr := os.Lstat(oldAbs); {
	case statErr == nil:
		// The subtree is on disk at the old path: carry it across to op.Path.
		if err := fsutil.Rename(oldAbs, abs); err != nil {
			return false, err
		}
	case os.IsNotExist(statErr) && destExists(abs):
		// The old path is gone but the destination already holds the subtree: a prior apply
		// renamed it on disk and then crashed (or SetMeta failed) before the descendant
		// baselines and root baseline were rewritten. The disk move is already done, so skip
		// it and finish the (idempotent) baseline relocation below - falling back to a plain
		// mkdir here would reindex only the root and strand every descendant baseline under
		// the old prefix forever, since a later replay keeps finding the old path gone.
		// Mirrors applyRename's gone-source/present-destination replay handling.
		//
		// Including its durability re-flush: the swallowed rename's own
		// flush lived in the fsutil.Rename call that already returned, so a crash (or a real
		// EIO) between os.Rename and syncRenameDirs would otherwise leave this replay acking
		// a StateSynced subtree over non-durable entries at both ends.
		//
		// "A directory stands at the destination" is not by itself proof that it is OUR moved
		// subtree. A user who deleted the old tree and made a fresh directory at op.Path
		// presents exactly this shape, and adopting it relocates the old subtree's synced
		// descendant records under a directory that does not contain those files - after which
		// the next scan reads every one of them as vanished and pushes a delete of the server's
		// still-live descendants. So require one piece of positive evidence: a descendant the
		// baselines record must actually be there.
		//
		// Falling back to a plain mkdir instead strands the descendant records under the old
		// prefix, which the comment above rightly calls bad - but the two costs are not
		// comparable. Stranded records are stale bookkeeping the scan and the repair sweep can
		// still reach; a wrong adoption deletes the user's files on the server.
		adopt, aerr := e.destHoldsMovedSubtree(op.ViewID, oldPath, abs)
		if aerr != nil {
			return false, aerr
		}
		if !adopt {
			e.log.Warn("engine: a directory stands where a tracked subtree should have moved, but holds none of its recorded contents; treating it as unrelated",
				"view", op.ViewID, "from", oldPath, "to", op.Path)
			return false, nil
		}
		e.bestEffortSyncDirs(filepath.Dir(abs), filepath.Dir(oldAbs))
	default:
		// Nothing on disk at either path - no subtree to carry across; a plain mkdir is correct.
		return false, nil
	}
	// Re-stamp the destination mtime: idempotent on the first apply, and on a crash-replay
	// whose prior apply died between the rename and SetMeta this is where the mtime finally
	// lands. The op carries no permission bits (UnixPerm unset), so only mtime is set.
	if err := fsutil.SetMeta(abs, op.LocalMTime, op.UnixPerm); err != nil {
		return false, err
	}
	// Carry the descendant baselines to the new prefix before re-homing the root, mirroring
	// relocateDescendantBaselines' use in applyRename/relocatePending: the root Put below drops
	// the old root through the byid index, so doing descendants first keeps the relocation
	// crash-replay safe.
	if err := e.relocateDescendantBaselines(op.ViewID, oldPath, op.Path); err != nil {
		return false, err
	}
	if err := e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = op.FileID
		b.FileType = fileType(true)
		b.LocalMTime = op.LocalMTime
		b.ServerMTime = op.LocalMTime
		if op.ServerSyncID != 0 {
			b.ServerSyncID = op.ServerSyncID
		}
	}); err != nil {
		return false, err
	}
	return true, nil
}

// destSubtreeSampleCap bounds how many recorded descendants the adoption probe looks at. One
// hit is the whole answer, so the cap only matters when none of them is there: a handful of
// Lstats is enough to distinguish a moved subtree from an unrelated directory, and a subtree of
// a hundred thousand files must not cost a hundred thousand stats to decline.
const destSubtreeSampleCap = 8

// destHoldsMovedSubtree reports whether the directory now standing at absDest actually holds
// the contents the baselines record under oldPath.
//
// It answers the replay branch's question - "is this MY subtree, already moved?" - with
// evidence rather than with the mere presence of a directory. A record with no descendants at
// all is adopted: there is nothing that could be stranded and nothing to check, which is also
// the shape a freshly created empty directory takes on both sides.
//
// Errors resolve toward adopting, matching the branch's own prior behaviour: a store failure or
// an unreadable path is not evidence AGAINST the subtree being ours, and refusing on one would
// turn a transient read failure into a permanently stranded prefix.
func (e *Engine) destHoldsMovedSubtree(viewID uint64, oldPath, absDest string) (bool, error) {
	var sample []string
	skipped, err := e.scanBaselinePrefix(viewID, oldPath, func(pb store.PathBaseline) error {
		if len(sample) < destSubtreeSampleCap {
			sample = append(sample, pb.Path)
		}
		return nil
	})
	if err != nil {
		return true, nil //nolint:nilerr // a store failure is not evidence against the subtree
	}
	for _, p := range skipped {
		e.warnCorruptBaseline("mkdir-adopt-probe", viewID, p, store.ErrCorruptRecord)
	}
	if len(sample) == 0 {
		return true, nil
	}
	prefix := oldPath + "/"
	for _, p := range sample {
		rel := strings.TrimPrefix(p, prefix)
		if rel == p {
			continue
		}
		if _, err := os.Lstat(filepath.Join(absDest, filepath.FromSlash(rel))); err == nil {
			return true, nil
		}
	}
	return false, nil
}
