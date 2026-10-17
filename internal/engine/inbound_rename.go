package engine

// This file carries the inbound OpRename apply path: it moves the node on disk from
// its source to its destination, re-points its baseline (and, for a directory, its
// descendant baselines) at the new path, and handles the move-then-recreate,
// superseded-replay, multi-move-chain and cold-start-gap cases. The shared
// descendant-relocation helper (relocateDescendantBaselines) and the two on-disk
// probes (renameHasNoSource, destExists) live here too.

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// applyRename moves the node on disk from its source to its destination path and
// re-points its baseline at the new path.
//
// The moved node is identified by its file id, not merely by the source path. The
// reconcile pass relocates the moved node's baseline to the destination before the
// drain runs, so at apply time the source path normally holds either nothing (a clean
// move), a different recreated node (a same-batch move-then-recreate), or - only for a
// directly-seeded op - the moved node itself. Three facts must be kept straight:
//
//   - Disk move: the on-disk rename runs in every case except a superseded replay -
//     a *different* node has taken the source path AND the destination baseline already
//     records our node as synced (proof a prior apply completed the move). Re-running it
//     then would drag the recreated node over the correct destination. In particular, a
//     genuine overwrite move (the source still holds, or was relocated to, our node, and
//     the destination merely already exists on disk) must still run, replacing the
//     destination - its destination baseline is still StatePending, not synced.
//   - Destination baseline: the richest facts win. An existing destination baseline
//     already identifying this node is authoritative - reconcile relocated the moved
//     facts there (the final hop of a multi-move chain, or a single move), or a prior
//     apply wrote them - so it is kept rather than clobbered with a bare file-id record.
//   - Source baseline: dropped only when the source genuinely still holds the node we
//     moved (a directly-seeded op), never when a new node has taken its place or it was
//     already relocated away.
func (e *Engine) applyRename(op store.Op) error {
	from, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	to, err := e.abs(op.ViewID, op.NewPath)
	if err != nil {
		return err
	}
	// Resolve both endpoints to their real on-disk spelling. A byte-preserving filesystem (Linux,
	// Windows) may hold the source - and an existing destination occupant - under a non-NFC (e.g.
	// NFD) name while the op carries the canonical NFC path, so a literal-NFC os.Rename would miss
	// the file, find no source, and treat the move as a no-op (mirrors the outbound resolveNFCPath
	// and keep-both onDiskAbs resolution). The store keys below stay op.Path / op.NewPath (the NFC
	// baseline keys); only the disk paths resolve. When the fallback resolves to a different
	// spelling, the No-Symlink variants re-check that real path for a symlink component (e.abs's
	// noSymlinkEscape only walked the absent literal NFC path) so os.Rename can never follow a
	// symlinked component outside the sync root.
	//
	// The SOURCE must fully resolve (onDiskAbsNoSymlink): it names an existing node, so an all-or-
	// nothing resolution is correct - a source that resolves nowhere stays literal NFC and the
	// gone-source path below handles it. The DESTINATION uses the prefix variant
	// (onDiskAbsPrefixNoSymlink): a genuine new-leaf move addresses a
	// name that does not exist yet, so an all-or-nothing resolution would leave the WHOLE path -
	// including an NFD-spelled parent directory - literal NFC, and fsutil.Rename would then create
	// a parallel NFC twin directory next to the NFD original and drop the file inside it (the next
	// scan folds both spellings into one normpath.Key and protects the subtree, stopping its sync).
	// The prefix resolution reuses the existing NFD parent while keeping the not-yet-existing leaf
	// literal NFC.
	from, err = e.onDiskAbsNoSymlink(from)
	if err != nil {
		return err
	}
	to, err = e.onDiskAbsPrefixNoSymlink(to)
	if err != nil {
		return err
	}

	rs, err := e.classifyRenameSource(op)
	if err != nil {
		return err
	}
	src, srcIsMoved, srcRecreated := rs.baseline, rs.isMoved, rs.recreated

	// Read the destination baseline once, up front: it drives both the superseded-replay
	// decision below and the baseline choice further down.
	dst, dstErr := e.store.GetBaseline(op.ViewID, op.NewPath)
	switch {
	case errors.Is(dstErr, store.ErrCorruptRecord):
		// An undecodable destination record is treated as absent: a plain
		// error here retried the op transiently forever while the PutBaseline below - the very
		// write that heals the key - never ran. With no decodable facts the record cannot prove
		// a superseded replay or supply a richer baseline, so the absent classification is the
		// conservative one, and the destination Put overwrites (heals) the key.
		e.warnCorruptBaseline("rename-dest", op.ViewID, op.NewPath, dstErr)
		dst, dstErr = store.Baseline{}, store.ErrNotFound
	case dstErr != nil && !isNotFound(dstErr):
		return fmt.Errorf("load destination baseline: %w", dstErr)
	}

	// Resolve where (if anywhere) the moved node is currently tracked through the byid
	// index. A hit proves we have materialised this node - at the source, the
	// destination, or a transient hop of a multi-move chain - so the on-disk source
	// genuinely holds (or transiently holds) our bytes and the rename should move them.
	// A miss means the node was never tracked: its create predates our event-log window
	// (the cold-start gap, Phase 6c), so it is not on disk under any path. The lookup is
	// read here, up front, so it can both gate the foreign-source guard below and serve
	// the transient-hop early-return further down.
	idPath, idErr := e.store.PathByID(op.ViewID, op.FileID)
	if idErr != nil && !isNotFound(idErr) {
		return fmt.Errorf("resolve moved id %d/%q: %w", op.ViewID, op.FileID, idErr)
	}
	nodeTracked := idErr == nil

	// The moved node's kind, read only off baselines that IDENTIFY it (every arm of
	// resolveMovedIsDir is gated on FileID == op.FileID). It is resolved HERE, ahead of the drop
	// arms below, rather than at its historical site further down: the drops' compensations must
	// be typed from this witness and never from the event alone or from a record that merely sits
	// at the source path - see compensatedKind.
	movedIsDir, err := e.resolveMovedIsDir(op, src, srcIsMoved, dst, dstErr, idPath, nodeTracked)
	if err != nil {
		return err
	}

	// A foreign node's baseline sits at the source path while our moved node is tracked
	// nowhere (nodeTracked is false): the source path holds someone else's node, not ours
	// - ours was never materialised (the cold-start gap). Running the on-disk rename would
	// drag that foreign node onto the destination and then mislabel it as op.FileID, and
	// recording a destination baseline for a node we do not have would later read as a
	// local delete and push a spurious removal. Drop the op rather than corrupt the
	// foreign node. A move-then-recreate, where the source baseline is also foreign but
	// our node IS tracked (reconcile relocated it to the destination while the recreate
	// download is still queued behind us), keeps nodeTracked true and falls through to the
	// rename, so a genuine transient hop still moves our bytes.
	if srcRecreated && !nodeTracked {
		e.log.Warn("engine: dropping rename whose source holds a different node while its own node is absent",
			"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID)
		// The move placed nothing at the destination (our node was never materialised), so a
		// same-id rename fed by this one must be doomed, not trust a false feeder.
		// There is no baseline to clean up: nodeTracked is false, so the moved id is untracked.
		// The node itself still lives on the server at the destination, so the drop carries the
		// compensating inbound op that re-requests it there (finding H2). Its KIND is corroborated
		// by an id-matched baseline, never taken from the event alone and never from the record at
		// the source path - which in this very arm belongs to the FOREIGN node squatting it. See
		// compensatedKind.
		return dropRenameCompensated(op, compensatedKind(op, movedIsDir))
	}

	// The moved node was never tracked by us (byid has no record) yet an entity of the WRONG KIND
	// sits at the on-disk source. A node we materialised always carries a baseline (OpDownload/
	// OpMkdir record one, indexed in byid), so an entity here that byid does not resolve to
	// op.FileID is the user's local content. When its on-disk kind disagrees with the moved
	// node's own kind (op.IsDir, threaded from the move event), it cannot be our node at all, so
	// the rename would drag unrelated content across the tree and mislabel it as op.FileID - a
	// server node of the OTHER kind. Two symmetric cases reach here: a file download that
	// dead-lettered because an untracked DIRECTORY blocked its target path (op.IsDir false, source
	// a dir), and a directory mkdir that dead-lettered because an untracked FILE occupied its path
	// (op.IsDir true, source a file) - executor.go's file-over-dir / dir-over-file routes, after
	// dropStrandedBaseline drops the phantom over the type-mismatched occupant.
	// Drop the op; a later modify event (or the 6c full-catalog pull) materialises our node
	// properly. There is no baseline to clean up: nodeTracked is false, so relocatePending was a
	// no-op and the moved id is untracked. A SAME-kind untracked source is left to the existing
	// overwrite-move path (it may genuinely hold a not-yet-tracked node of our kind), and an absent
	// source falls through to the cold-start-gap path below (dropColdStartGapRename).
	if !nodeTracked {
		if fi, statErr := os.Lstat(from); statErr == nil && fi.IsDir() != op.IsDir {
			e.log.Warn("engine: dropping rename whose source holds an untracked entity of the wrong kind while its own node is absent",
				"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID, "node_is_dir", op.IsDir)
			// Our node is absent locally but present server-side at the destination, so the drop
			// re-requests it there rather than relying on a later event that never comes (H2).
			// The GATE above reads op.IsDir deliberately - a kind the event did not state is
			// false, which makes the mismatch test fire and the op drop, the never-lose direction.
			// The COMPENSATION must not inherit that pessimism: an OpDownload aimed at a directory
			// node is the permanent wedge compensatedKind exists to avoid, so it takes the
			// id-matched kind. The on-disk entity at the source cannot supply it either - the gate
			// above just proved that entity is NOT our node.
			return dropRenameCompensated(op, compensatedKind(op, movedIsDir))
		}
	}

	// The move has already been applied to disk exactly when the destination baseline
	// records THIS node (file id matches) as StateSynced - only an apply, never the
	// reconcile pass, advances a baseline to synced. Re-running the on-disk rename then,
	// while a different (recreated) node sits at the source, would drag that recreated
	// node over the correct destination. Probing only whether the destination exists on
	// disk (the previous test) cannot tell a completed move from a first-time genuine
	// overwrite of a pre-existing node - both find the destination present - so an
	// overwrite move combined with a same-batch source recreate was wrongly skipped,
	// leaving stale content at the destination. The synced-baseline signal distinguishes
	// them: after reconcile the relocated destination baseline is still StatePending, so
	// a first overwrite runs; only the post-apply replay finds it StateSynced and skips.
	supersededReplay := srcRecreated && dstErr == nil && dst.FileID == op.FileID && dst.State == store.StateSynced

	// movedToDest records whether op.FileID now physically sits at the destination because
	// THIS apply moved it there (a successful on-disk rename). It stays false for a swallowed
	// no-op (the source was already gone) and for a superseded replay, where the bytes were
	// placed by an earlier apply, not this one - distinguishing those is what keeps a
	// pre-existing foreign destination baseline from being wrongly clobbered (or wrongly
	// kept) by the baseline choice below.
	movedToDest := false
	// dirAtDest records whether the moved node now sits at the destination on disk - because
	// this apply renamed it there, or a prior apply already did and a crash before the
	// descendant relocation / baseline writes is replaying the op. Either way, for a directory
	// move the descendant baselines must be re-homed under the new prefix; doing it on the
	// swallowed-no-op replay path too (not only when this apply performs the rename) is what
	// makes the relocation crash-replay safe. relocateDescendantBaselines is
	// idempotent, so a replay whose relocation already finished is a no-op (empty old-prefix
	// scan).
	dirAtDest := false
	if !supersededReplay {
		// Cluster-A keep-both guard: a server move onto a path where the user holds a
		// locally-diverged or untracked file must not silently clobber that file. Set it aside
		// as a _Conflict copy before the destructive rename (the inbound download path already
		// guards its target this way; this brings the move path to the same symmetry). The live
		// destination baseline read up front is passed so the guard can supersede a stale
		// pre-batch capture with a download that applied since.
		if err := e.guardRenameDest(op, from, to, dst, dstErr == nil); err != nil {
			return err
		}
		if err := fsutil.Rename(from, to); err != nil {
			if !renameHasNoSource(from) {
				return err
			}
			// The source is gone. Normally that means the move already applied (the node is
			// at the destination) or a same-batch chain emptied the source, so the on-disk
			// move is a harmless no-op. Two shapes must NOT be swallowed that way, because in
			// both of them our node is absent from disk and falling through would record a
			// destination baseline (a bare {FileID: op.FileID} at worst) forced to StateSynced
			// - a synced claim for a node we do not have:
			//
			//   - The destination is ALSO absent: the move never took effect and we never had
			//     the node on disk - a create that predates our event-log window (the
			//     cold-start gap, Phase 6c). A later local scan would see a tracked baseline
			//     with no file and push a spurious delete to the server.
			//   - The moved node is not tracked at all (byid miss) while SOMETHING occupies the
			//     destination. The invariant established above still holds - a node we
			//     materialised always carries a byid-indexed baseline - so that occupant is
			//     provably NOT our node: it is the user's local content (or another node's).
			//     Swallowing here would stamp a false synced claim over a foreign entity. For a
			//     DIRECTORY occupant the damage compounds: the bare record carries no FileType,
			//     so the next scan reads a file->dir type replacement, detectRemoves emits an
			//     outbound OpRemove of op.FileID and the LIVE server node - with its whole
			//     subtree - is deleted, the local directory then re-uploaded as a fresh tree
			//     (the server subtree replaced by unrelated data). Dropping the op instead
			//     leaves the foreign occupant untouched and the server node intact; a
			//     subsequent modify event (or the 6c full-catalog pull) materialises our node
			//     properly, and the keep-both funnels then arbitrate the occupied path.
			//
			// Drop the op with a warning instead of falsely acking it, cleaning up any baseline
			// reconcile relocated to the destination (or a directly-seeded source baseline) so
			// the drop does not strand a tracked node with no file behind it.
			if !destExists(to) || !nodeTracked {
				return e.dropColdStartGapRename(op, srcIsMoved, movedIsDir, dst, dstErr)
			}
			// Source gone, destination present: the on-disk move already happened (this apply
			// or a prior one). The descendant relocation below still runs so a crash between
			// the rename and the relocation is repaired on replay.
			//
			// The move's own durability flush lived in the fsutil.Rename call that already
			// returned - so if THAT call died between os.Rename and syncRenameDirs (or its
			// flush failed with a real EIO), this replay would swallow the move and ack a
			// StateSynced baseline over a pair of entries neither of which is durable: a power
			// loss then resurrects the file at the source, or loses it at the destination, and
			// the next scan pushes a duplicate create (or a spurious delete) to the server.
			// Re-flush both affected parents before falling through
			// (mirroring mkdirAllDurable's unconditional re-flush on the create axis).
			e.bestEffortSyncDirs(filepath.Dir(to), filepath.Dir(from))
			dirAtDest = true
		} else {
			movedToDest = true
			dirAtDest = true
		}
	}
	// The whole subtree now sits at the destination on disk; bring the descendant baselines
	// (and their byid entries) across to the new prefix to match.
	if movedIsDir && dirAtDest {
		if err := e.relocateDescendantBaselines(op.ViewID, op.Path, op.NewPath); err != nil {
			return err
		}
	}

	// If reconcile already relocated the moved node's baseline past this hop - to a
	// later destination in a same-batch move chain (a->b->c: the a->b op finds the
	// node's baseline already sitting at c) - this rename is only a transient disk
	// hop. Writing a baseline at our NewPath now would evict that richer relocated
	// record through the byid index (one path per file id), collapsing the node's
	// facts to a bare id. So move the bytes only and leave every baseline to the op
	// that owns the node's final location. (idPath/idErr were resolved up front.)
	if idErr == nil && idPath != op.Path && idPath != op.NewPath {
		return nil
	}

	// Choose the baseline to record at the destination, preferring the richest facts
	// available for the moved node (dst/dstErr were read up front).
	var moved store.Baseline
	switch {
	case dstErr == nil && dst.FileID == op.FileID:
		// reconcile relocated the moved node's facts here (or a prior apply did); they
		// are authoritative - keep them rather than overwriting with a poorer record.
		moved = dst
	case srcIsMoved:
		// No facts at the destination yet, but the source still holds the node we move
		// (a directly-seeded op, or a transient chain hop): carry its facts across.
		moved = src
	case movedToDest:
		// This apply just moved our node onto the destination, overwriting whatever node
		// (and baseline) was there. A pre-existing destination baseline now describes an
		// overwritten node that no longer lives at this path, so it must NOT be kept;
		// record our moved id (bare facts - we have nothing richer). Keeping the stale
		// foreign baseline would leave the destination identifying the overwritten node
		// while disk holds ours.
		moved = store.Baseline{FileID: op.FileID}
	case dstErr == nil:
		// The move was a swallowed no-op (the source was already gone) and the destination
		// still holds another node's baseline; keep it rather than clobbering with a bare
		// record - this apply did not place our node here.
		moved = dst
	default:
		// Nothing to carry: record the bare moved id.
		moved = store.Baseline{FileID: op.FileID}
	}
	// Normally a relocated node is reconciled with the server, so advance it to synced.
	// But a node that was diverged (StateConflict) before the server moved it is STILL
	// diverged after the move: the on-disk rename only carries the local bytes across, it
	// never downloads the server's version. Forcing StateSynced here would mark divergent
	// content as agreeing with the server, and since the recorded LocalHash equals the
	// on-disk (divergent) hash the next localScan sees no change either - the conflict
	// disappears silently. This is reachable when a server move races a local modify: the
	// keep-both resolver cannot resolve a moved-away node (QueryNode of the canonical path
	// returns ErrPathError, the old parent no longer lists it), dead-letters the
	// resolve-conflict op as "gone" leaving the divergent bytes in place, and this inbound
	// rename then relocates them. Preserve the conflict state so the divergence stays
	// surfaced rather than being silently buried. A genuine delete still
	// discards the local edit via applyRemove (server authority), unchanged.
	if moved.State != store.StateConflict {
		moved.State = store.StateSynced
	}
	// ...with one exception: a BARE record for a node we never tracked. This apply moved
	// whatever stood at the source onto the destination and has nothing to say about it - no
	// ServerHash, no LocalHash, no kind - because the source held content that byid could not
	// resolve to op.FileID, i.e. by this file's own stated invariant, the user's own local
	// content. Calling that StateSynced asserts an agreement no transfer ever established.
	//
	// The INBOUND consequence of the lie is already contained (the empty-hash gates read such
	// a record as "no trustworthy divergence base" and funnel the file into a keep-both copy).
	// The OUTBOUND one was not: localReconcile drives StateSynced records, sees "" != the
	// on-disk hash and plans a push, and guardUploadTarget - finding an empty ServerHash on
	// both sides - degrades to node identity and lets those unrelated bytes overwrite the
	// server's node. compare_mtime accepts any not-older write, so nothing further refuses it,
	// and the node's real content is gone from both replicas.
	//
	// StateConflict is the honest state and is never less protective (every gate tests
	// != StateSynced). It also makes the divergence VISIBLE as Unresolved rather than leaving
	// a path that silently drives no work - the same choice repairSupersededRecord makes for
	// its own non-synced leftovers. A later inbound apply at the path ends in markSynced and
	// clears it; nothing else can, which is correct, because nothing else has the evidence.
	// Scoped to a FILE. A directory record legitimately carries no hashes, so the same test
	// would catch every untracked directory move, and freezing one has consequences a file does
	// not have (localReconcile drives only StateSynced records, so the whole subtree beneath it
	// stops being pushed). The vector proved here is the file one - the scan compares LocalHash
	// against the on-disk hash and pushes the difference - so the directory case is left for a
	// change that comes with a scenario of its own.
	if movedToDest && !nodeTracked && !movedIsDir && moved.ServerHash == "" && moved.LocalHash == "" {
		e.log.Warn("engine: inbound rename relocated content it cannot attribute to the moved node; recording it as unresolved rather than agreed",
			"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID)
		moved.State = store.StateConflict
	}
	if err := e.store.PutBaseline(op.ViewID, op.NewPath, moved); err != nil {
		return fmt.Errorf("write moved baseline: %w", err)
	}
	// Drop the source baseline only when it is the node we moved - never when a new
	// node has since taken the source path, and never when it was already relocated away.
	if op.Path != op.NewPath && srcIsMoved {
		if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
			return fmt.Errorf("drop source baseline: %w", err)
		}
	}
	return nil
}

// renameSource is the source-baseline classification an inbound rename turns on.
type renameSource struct {
	baseline store.Baseline
	// isMoved reports that the source baseline still records the node we are moving (a
	// directly-seeded op, or a transient chain hop): its facts are carried to the destination
	// and the source baseline is dropped once the move applies.
	isMoved bool
	// recreated reports that a DIFFERENT node sits at the source path - a same-batch
	// move-then-recreate wrote it before this rename ran. Only then, and only once the move has
	// already been applied, is this a superseded replay we must not re-run.
	recreated bool
}

// classifyRenameSource reads the source-path baseline once and classifies it relative to the
// node op moves, distinguishing our own still-present node (isMoved) from a foreign node a
// same-batch recreate left at the source (recreated). A missing baseline classifies as neither.
// A corrupt (undecodable) record classifies as neither too: this read runs
// FIRST in applyRename, so a strict error here wedged the whole rename apply transiently forever
// - reconcile's relocatePending had already skip-and-Warned the same record (its comment promises
// the apply-side Put heals the destination key), and that healing Put sat below, unreachable. As
// absent, the move still runs (byid resolves the node) and the destination PutBaseline's
// drop-moved-orphan sweep clears the corrupt source key.
func (e *Engine) classifyRenameSource(op store.Op) (renameSource, error) {
	src, srcErr := e.store.GetBaseline(op.ViewID, op.Path)
	if errors.Is(srcErr, store.ErrCorruptRecord) {
		e.warnCorruptBaseline("rename-source", op.ViewID, op.Path, srcErr)
		return renameSource{}, nil
	}
	if srcErr != nil && !isNotFound(srcErr) {
		return renameSource{}, fmt.Errorf("load source baseline: %w", srcErr)
	}
	return renameSource{
		baseline:  src,
		isMoved:   srcErr == nil && src.FileID == op.FileID,
		recreated: srcErr == nil && src.FileID != op.FileID,
	}, nil
}

// resolveMovedIsDir reports whether the node being moved is a directory, read from whichever
// baseline describes it. os.Rename moves a directory's entire subtree on disk in one call, so when
// the node lands at the destination the descendant baselines must be relocated to the new prefix
// too - otherwise persisted state diverges from disk and a later child delete/move/scan targets the
// stale old prefix. It first consults the source (when it still holds our node) and the
// destination (when reconcile relocated the facts there); a transient hop of a multi-move chain
// (a->b->c) describes the node at NEITHER endpoint of this hop, so it falls back to the baseline at
// the byid-tracked path so a directory hop still recognises itself and relocates its descendants
// under each hop's prefix.
func (e *Engine) resolveMovedIsDir(op store.Op, src store.Baseline, srcIsMoved bool, dst store.Baseline, dstErr error, idPath string, nodeTracked bool) (bool, error) {
	movedIsDir := (srcIsMoved && src.FileType == "dir") ||
		(dstErr == nil && dst.FileID == op.FileID && dst.FileType == "dir")
	if !movedIsDir && nodeTracked && idPath != op.Path && idPath != op.NewPath {
		tb, terr := e.store.GetBaseline(op.ViewID, idPath)
		if errors.Is(terr, store.ErrCorruptRecord) {
			// An undecodable hop record cannot prove the moved node is a directory; leave
			// movedIsDir false rather than failing the apply transiently forever.
			// Worst case the descendant relocation is skipped and the stale-prefix
			// records are later swept protected/healed by the scan-side tolerance - the
			// fail-safe direction, never a wrong relocation.
			e.warnCorruptBaseline("rename-moved-type", op.ViewID, idPath, terr)
			terr = store.ErrNotFound
		}
		if terr != nil && !isNotFound(terr) {
			return false, fmt.Errorf("resolve moved node type %d/%q: %w", op.ViewID, idPath, terr)
		}
		if terr == nil {
			movedIsDir = tb.FileType == "dir"
		}
	}
	return movedIsDir, nil
}

// dropColdStartGapRename handles a rename whose moved node is absent from disk once the on-disk
// move found no source. Two shapes reach it (see applyRename's gone-source branch): source AND
// destination both absent - a create predating our event-log window (the cold-start gap, Phase
// 6c) - and an UNTRACKED moved node (byid miss) whose destination holds provably foreign content.
// Marking the destination synced would record a node we do not have, and a later scan would push
// a delete of the live server node (over a foreign DIRECTORY, the whole server subtree). So it
// cleans up any baseline reconcile relocated to the destination (or a directly-seeded source
// baseline) and dead-letters the op with its own repair (dropRenameCompensated). That
// compensation is what materialises the node: a server-side move emits ONE compacted row, no
// later modify names it, and cold start does not re-run (finding H2) - never assume a later event
// or the 6c pull will. A same-id rename fed by this one is doomed rather than trusting a false
// feeder, and the moved id is left untracked.
//
// For a DIRECTORY the cleanup cascades: relocatePending already rebased the descendants
// (relocateDescendantBaselines, MoveBaseline verbatim, so StateSynced stays StateSynced), and
// stranded synced descendants would make the next localScan push a spurious OpRemove of the
// freshly moved server node. dropDescendantBaselines runs under the root's prefix, mirroring
// applyRemove's success path, resolveRejectedRename's phantom cascade and guardRemoveDir.
// Directory-ness is movedIsDir (resolveMovedIsDir) OR op.IsDir, since both on-disk endpoints are
// absent and cannot testify. The descendant drop runs BEFORE the root DeleteBaseline, so a crash
// keeps the root's dst.FileID match that re-enters this branch on replay.
//
// The unconditional dropDescendantBaselines (not the onDiskIsNodeKind-guarded
// dropStrandedBaseline/dropPhantomDescendantBaselines) is safe because both cleanup arms are gated
// on a baseline carrying op.FileID, indexed in byid, so only the both-endpoints-absent shape
// reaches them; the untracked-node shape has no such baseline (nodeTracked is false) and cleans up
// nothing, so it cannot orphan a live on-disk descendant of the foreign occupant.
//
// That proof needs a NON-EMPTY op.FileID, so both arms require one. An id-less rename survives
// reconcile's backfill when the source is untracked, local-only or corrupt, and putBaselineTx
// never indexes an empty id, so every id-less rename is routed here. With an empty id,
// dst.FileID == op.FileID and srcIsMoved match any local-only record, and a sparse id-less
// directory move onto the user's local tree would untrack descendants that carry server ids -
// re-uploaded as duplicates, orphaning the originals. An id-less op cleans up nothing, leaving at
// worst an inert StatePending record (localReconcile drives only StateSynced records).
func (e *Engine) dropColdStartGapRename(op store.Op, srcIsMoved, movedIsDir bool, dst store.Baseline, dstErr error) error {
	e.log.Warn("engine: dropping rename whose moved node is absent from disk",
		"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID)
	if op.FileID == "" {
		// No identity to prove ownership of either endpoint's baseline with; drop the op only.
		// An id-less drop also carries no compensation: a download addresses the server node by
		// id, so there is nothing to re-request with.
		return &droppedRename{}
	}
	isDir := compensatedKind(op, movedIsDir)
	// The moved node is absent from disk but the server holds it at the destination: the drop
	// re-requests it there (finding H2). Built up front so both cleanup arms return the same
	// signal.
	drop := dropRenameCompensated(op, isDir)
	// A StateSynced destination record for our own node is NOT the stranded phantom this
	// cleanup exists to sweep: only an apply - or a deliberate re-emission marker - ever
	// advances a baseline to synced, and reconcile's relocatePending leaves the relocated
	// record StatePending. The marker case is load-bearing and reachable in the very
	// renamed-in-place interleaving: a local delete of a/x races a server-side in-place
	// rename a/x -> a/y, so applyRemoveOut classifies the node MOVED, the PathByID
	// cross-check re-emits via revertRelocatedRemove (which flips the relocated record at
	// a/y to StateSynced precisely so detectRemoves re-emits the delete on the new path),
	// and the inbound OpRename serialized behind it on the shared a/x key then runs in the
	// SAME batch - its source deleted by the user and its destination never materialised,
	// landing here. Deleting the marker would silently discard the user's deletion AND drop
	// the live server node out of the local replica (no baseline, no file), recoverable only
	// by a cold start. Leave it: a synced record with no on-disk file is exactly the
	// detectRemoves shape, so the next scan re-emits the OpRemove the revert intended. The
	// same reasoning covers a crash-replay of a completed move whose file was deleted
	// locally afterwards.
	if dstErr == nil && dst.FileID == op.FileID && dst.State == store.StateSynced {
		e.log.Info("engine: keeping the synced destination baseline of a dropped rename so the pending local delete is re-emitted there",
			"view", op.ViewID, "to", op.NewPath, "file_id", op.FileID)
		// This arm carries NO compensation. The kept record is the marker that re-emits the
		// user's pending local DELETE at the destination (revertRelocatedRemove), so re-requesting
		// the node here would resurrect exactly the file the user deleted - and race the delete
		// the marker exists to produce. The node is not lost either way: the outbound delete is
		// the agreed outcome.
		if srcIsMoved {
			return e.dropColdStartGapSource(op, isDir, &droppedRename{})
		}
		return &droppedRename{}
	}
	if dstErr == nil && dst.FileID == op.FileID {
		if isDir {
			if err := e.dropDescendantBaselines(op.ViewID, op.NewPath); err != nil {
				return err
			}
		}
		if err := e.store.DeleteBaseline(op.ViewID, op.NewPath); err != nil {
			return fmt.Errorf("drop stranded destination baseline: %w", err)
		}
	}
	if srcIsMoved {
		return e.dropColdStartGapSource(op, isDir, drop)
	}
	return drop
}

// droppedRename is the errDeadLettered signal applyRename returns for a move it dropped rather
// than applied, carrying the compensating inbound op (if any) the drop must leave behind.
//
// Unwrap returns errDeadLettered, so every existing errors.Is(err, errDeadLettered) test - the
// executor's drop arm, the doom propagation, the tests - keeps matching unchanged; the executor
// additionally errors.As's this type to learn what to re-request.
type droppedRename struct {
	// compensation is the inbound op to enqueue with the dead-letter trace, or nil when the
	// drop leaves nothing to repair (an id-less move, or the kept-marker arm).
	compensation *store.Op
}

func (d *droppedRename) Error() string {
	if d.compensation == nil {
		return "engine: inbound rename dropped"
	}
	return fmt.Sprintf("engine: inbound rename dropped, re-requesting the node as %s %q",
		d.compensation.Type, d.compensation.Path)
}

// Unwrap keeps the drop indistinguishable from a plain errDeadLettered for every classifier.
func (d *droppedRename) Unwrap() error { return errDeadLettered }

// compensatedKind resolves the kind a dropped rename's compensation must carry, corroborating
// the move event's own word with a kind read off a baseline that IDENTIFIES the moved node.
//
// The event alone is not enough: a sparse move reports is_dir false whatever the node really is
// (store.Op.MovedBaseIsDir's own doc comment), and a compensation typed from it makes the repair
// for a DIRECTORY rename an inbound OpDownload addressed at a directory node. Inbound escalation
// is deliberately closed (DESIGN §13), so such an op used to retry every drain forever behind the
// paced backoff while blockedKeys held every path-overlapping op behind it - and the directory H2
// promises to repair was never repaired. A wedge is strictly worse than the inert drop the
// compensation replaced, so an id-matched witness is preferred wherever one survives; where none
// does, errDownloadNodeIsDir is the backstop that converts the doomed download into the OpMkdir
// once the SERVER has placed the node's kind (inbound_download.go). The backstop costs a failed
// transfer and a probe turn, which is why it is a backstop and not the primary witness.
//
// The corroborating witness must be ID-MATCHED, which is why it is passed in rather than read off
// op.MovedBaseIsDir. That field is captured by reconcile from snap.node(ev.FileID, ev.FromPath),
// which falls back to the baseline tracked at the SOURCE PATH whenever the file id misses the byid
// index - and a byid miss is the precondition of every arm that calls this helper (nodeTracked is
// false, the node was never materialised). So in exactly these shapes MovedBaseIsDir describes
// whatever node happens to sit at the source path, not the node being moved: a foreign DIRECTORY
// squatting the source of a moved FILE typed the repair as an OpMkdir, which applyMkdir then
// markSynced as a directory carrying the file's id - the server file is never fetched, and a later
// local delete of that directory pushes an outbound OpRemove of the live server FILE. applyRename's
// callers pass resolveMovedIsDir's answer (source/destination/byid-hop baselines, every arm gated
// on FileID == op.FileID); the executor's doomed-feeder arm has no such baseline left to read and
// passes false, which is why its call site spells that out.
//
// The disjunction only ever turns a FALSE into a true, so a kind the event does state is never
// contradicted; the executor's doomed-feeder arm and applyRename's own drop arms all take this one
// resolution so they cannot diverge - including dropColdStartGapRename, which used to spell the
// same expression inline and so was exactly the divergence this helper claims to prevent.
//
// When neither witness answers - the moved node is tracked nowhere and the event is sparse - the
// kind stays false and the repair is an OpDownload. For a node that really is a directory the
// apply-time backstop above converts it; what stays residual is the cost of reaching that
// conversion (one failed transfer plus one probe turn) and the subtree the OpMkdir does not
// re-request, the same gap dropColdStartGapRename's untracked arm carries. DESIGN §13 records it.
func compensatedKind(op store.Op, movedIsDir bool) bool {
	return op.IsDir || movedIsDir
}

// dropRenameCompensated builds the drop signal for an inbound rename whose moved node never
// reached its destination, together with the compensating op that re-requests it there.
//
// The compensation is deliberately minimal: a bare inbound OpDownload (OpMkdir for a directory)
// at op.NewPath carrying the moved node's file id, which is what the download verb addresses.
// It records no content facts, because this client has none it can trust - the last-agreed
// baseline was just dropped as describing a node that is not on disk - and applyDownload derives
// the server's facts from the transfer itself. An occupied destination is NOT special-cased here:
// an empty BaseLocalHash routes applyDownload through untrackedFilePresent, so a foreign file at
// the destination is preserved as a _Conflict copy by the same never-lose funnel that guards
// every other fresh create.
//
// An id-less move gets no compensation (the caller filters it): the download verb resolves a
// node by id, so there is nothing to ask for.
//
// DECLARED RESIDUAL - a DIRECTORY compensation repairs the node, NOT its subtree. The drop has
// just cascaded dropDescendantBaselines over both endpoints, so the descendants the server still
// holds under the moved directory are recorded nowhere; the OpMkdir re-creates the directory and
// markSyncs it, and no later event names those children (a server-side directory move emits ONE
// move row, the cold start does not re-run, and localReconcile drives only records that exist).
// They therefore stay missing from this replica until a cold start is forced. Re-requesting them
// needs a server-side enumeration at drop time, which this repair does not do - so the gap is
// NAMED in the dead-letter reason (droppedRenameReason) and warned at the drop site instead of
// being silent, the call H6 made for the fold-key collision. DESIGN §13 records it.
func dropRenameCompensated(op store.Op, isDir bool) error {
	if op.FileID == "" {
		return &droppedRename{}
	}
	comp := store.Op{
		Type:      store.OpDownload,
		Direction: store.DirInbound,
		ViewID:    op.ViewID,
		Path:      op.NewPath,
		FileID:    op.FileID,
		// The move event's own sync id: applyDownload passes it as the target sync id of the
		// transfer and records it into the rebuilt baseline, so the re-requested node lands with
		// the same provenance the rename would have given it.
		ServerSyncID: op.ServerSyncID,
	}
	if isDir {
		comp.Type = store.OpMkdir
	}
	return &droppedRename{compensation: &comp}
}

// dropColdStartGapSource is dropColdStartGapRename's source-side cleanup arm, split out so the
// kept-synced-destination early return above shares it verbatim. It carries the same non-empty
// op.FileID precondition as its caller (srcIsMoved is src.FileID == op.FileID, which only proves
// ownership for a real id) and the same descendants-first ordering. drop is the caller's drop
// signal (with or without a compensation) and is returned once the cleanup succeeds, so the
// compensation decision stays with the branch that has the evidence for it.
//
// A DEGENERATE move - one whose endpoints are the same path - has no source record distinct from
// the destination one, so this arm must not run for it: src and dst are one key, both identity
// tests hold, and the delete below would take exactly the record the caller's destination arm has
// already decided the fate of. In the kept-marker arm that is the marker itself (and, for a
// directory, its whole subtree via the cascade) - the record whose deletion that arm's comment
// spells out as discarding the user's pending delete AND dropping the live server node out of the
// replica. In the other arm the destination delete has already happened, so there is nothing left
// to do either. applyRename's success path carries the same op.Path != op.NewPath guard on its own
// source drop, and relocateDescendantBaselines/dropSelfEchoMoveSource carry it too; a server
// rename between two spellings that reconcile folds to one NFC path (an NFD -> NFC rename of the
// same name) is what produces the shape, and opForEvent does not filter it out.
func (e *Engine) dropColdStartGapSource(op store.Op, isDir bool, drop error) error {
	if op.Path == op.NewPath {
		return drop
	}
	if isDir {
		if err := e.dropDescendantBaselines(op.ViewID, op.Path); err != nil {
			return err
		}
	}
	if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
		return fmt.Errorf("drop stranded source baseline: %w", err)
	}
	return drop
}

// compensateDroppedRename resolves the compensating op a dropped inbound rename leaves behind
// together with the StatePending record that arms its never-lose gate, returning nils when the
// drop needs none. It is the single entry point both drop sites use - applyRename's own drops
// and the executor's doomed-feeder arm - so the duplicate guard and the pending record can never
// diverge between them. Any store failure is returned so the caller leaves its op queued:
// dropping the rename with the repair half-done is the one outcome worse than not repairing it
// at all.
//
// Neither result is written here. Both are handed to deadLetterCompensateAndAck, which commits
// them with the trace and the Ack in ONE transaction - see its doc comment for why writing the
// record on either side of that commit is unsafe.
func (e *Engine) compensateDroppedRename(dropErr error, qo store.QueuedOp) (*store.Op, *store.TrackedBaseline, error) {
	comp, err := e.renameCompensation(dropErr, qo)
	if err != nil || comp == nil {
		return nil, nil, err
	}
	track, err := e.compensationTracking(*comp)
	if err != nil {
		return nil, nil, err
	}
	return comp, track, nil
}

// droppedRenameReason renders the dead-letter reason for a dropped rename, naming the
// compensation when there is one. The trace still says the rename did not apply - the operator
// must see that - and now also says the node was re-requested, so a trace with no such clause is
// the honest signal that this drop left nothing behind.
// A DIRECTORY compensation additionally says what it did NOT repair: the subtree the server still
// holds under the moved node is not re-requested (dropRenameCompensated's declared residual), and
// an operator reading the trace must see that the repair is partial rather than infer a complete
// one from the re-request clause.
func droppedRenameReason(base string, comp *store.Op) string {
	if comp == nil {
		return base
	}
	reason := fmt.Sprintf("%s; re-requested the node as an inbound %s at %q", base, comp.Type, comp.Path)
	if comp.Type == store.OpMkdir {
		reason += "; the directory's server subtree is NOT re-requested and stays missing from this replica until a cold start"
	}
	return reason
}

// renameCompensation resolves the compensating op a dropped inbound rename must leave behind:
// the one applyRename attached to its drop signal, unless the queue already carries other
// inbound work for the same node.
//
// The coverage check is what keeps a same-node CHAIN from materialising the node twice. A
// same-batch a->b->c whose hops both drop would otherwise re-request the node at b AND at c: the
// first download lands a file at the abandoned path b, the second at c, and markSynced evicts
// one of the two baselines through the byid index - leaving an untracked file the next scan
// re-uploads as a duplicate server node. The hops serialise on their shared endpoint, so the
// earlier hop always sees the later one still queued and defers to it; the last hop is the one
// that compensates, at the node's final destination. The op being dropped is excluded from the
// scan by its own seq: it is still in the queue at this point (the Ack rides in the same
// transaction as the trace).
//
// A node the byid index still resolves to some OTHER path gets no compensation either. Every
// compensating drop reasons from "our node was never materialised", but the executor's
// doomed-feeder arm runs dropStrandedBaseline first, and that helper deliberately PRESERVES a
// pending record whose resolved path holds a real entity of the node's kind - the node IS on
// disk there. Re-requesting it at the rename's destination would then download a second copy,
// and the pending record's own write would evict the preserved one through the byid index (one
// path per file id), orphaning the live file for the next scan to re-upload as a duplicate.
func (e *Engine) renameCompensation(dropErr error, qo store.QueuedOp) (*store.Op, error) {
	var dr *droppedRename
	if !errors.As(dropErr, &dr) || dr.compensation == nil {
		return nil, nil
	}
	covered, err := e.queuedInboundCoversNode(qo.ViewID, qo.FileID, qo.Seq)
	if err != nil {
		return nil, err
	}
	if covered {
		e.log.Info("engine: not re-requesting a dropped rename's node; the queue already carries inbound work for it",
			"view", qo.ViewID, "file_id", qo.FileID, "to", qo.NewPath)
		return nil, nil
	}
	comp := *dr.compensation
	tracked, terr := e.store.PathByID(qo.ViewID, qo.FileID)
	switch {
	case isNotFound(terr):
	case terr != nil:
		return nil, fmt.Errorf("engine: resolve dropped rename's node %d/%q: %w", qo.ViewID, qo.FileID, terr)
	case normpath.Key(tracked) != normpath.Key(comp.Path):
		e.log.Info("engine: not re-requesting a dropped rename's node; it is still tracked elsewhere",
			"view", qo.ViewID, "file_id", qo.FileID, "tracked_at", tracked, "to", qo.NewPath)
		return nil, nil
	default:
		// The node is tracked at THIS path. Before adopting the spelling, one shape must be
		// excluded outright: a StateSynced record for our own node at the destination is the
		// re-emission MARKER revertRelocatedRemove leaves so the user's pending local delete is
		// re-derived on the new path (dropColdStartGapRename's kept-marker arm refuses to
		// compensate for exactly this reason). dropStrandedBaseline deliberately PRESERVES a synced
		// record, so the executor's doomed-feeder arm reaches here with the marker intact - and a
		// compensation would then download back precisely the file the user deleted, racing the
		// outbound delete the marker exists to produce. The exclusion lives here, in the shared
		// entry point, so both drop sites obey it.
		//
		// The read's failure modes decide toward NOT compensating, because the compensation is the
		// destructive direction here: it downloads the node back, and if the unreadable record WAS
		// the marker it downloads back precisely the file the user deleted. An undecodable record
		// therefore cannot rule the marker out and suppresses the repair (the same reading
		// compensationTracking gives ErrCorruptRecord, which likewise declines to write over it),
		// and a backend failure propagates so the caller leaves the op queued rather than deciding
		// on a read that did not happen. Only a definitive "no record here" proves no marker.
		b, berr := e.store.GetBaseline(qo.ViewID, tracked)
		switch {
		case isNotFound(berr):
		case errors.Is(berr, store.ErrCorruptRecord):
			e.log.Warn("engine: not re-requesting a dropped rename's node; the destination's record is undecodable and may be the marker that re-emits the local delete",
				"view", qo.ViewID, "file_id", qo.FileID, "to", tracked)
			return nil, nil
		case berr != nil:
			return nil, fmt.Errorf("engine: read dropped rename's destination record %d/%q: %w",
				qo.ViewID, tracked, berr)
		case b.FileID == qo.FileID && b.State == store.StateSynced:
			e.log.Info("engine: not re-requesting a dropped rename's node; the destination holds the marker that re-emits the local delete",
				"view", qo.ViewID, "file_id", qo.FileID, "to", tracked)
			return nil, nil
		}
		// Otherwise the compensation is wanted - but possibly under a
		// different SPELLING (the comparison above folds, the store does not): a case-only or
		// NFC/NFD variant on a folding host. Address the compensation at the spelling the store
		// holds, exactly as trackedSpelling/adoptResolveSpelling do for the other path-addressed
		// verbs. Otherwise compensationTracking would read the op's spelling, miss the record,
		// and return a bare pending TrackedBaseline whose write evicts the live one through the
		// byid index - the very hazard its own doc comment says it avoids.
		comp.Path = tracked
	}
	if comp.Type == store.OpMkdir {
		// Declared residual (dropRenameCompensated): the directory is repaired, its subtree is
		// not. Say so where an operator can see it, rather than letting a clean status imply the
		// replica holds everything the server does.
		e.log.Warn("engine: re-requesting a dropped rename's DIRECTORY node only; its server subtree is not re-requested and stays missing until a cold start",
			"view", qo.ViewID, "file_id", qo.FileID, "to", comp.Path)
	}
	return &comp, nil
}

// compensationTracking builds the StatePending baseline a reconcile-produced inbound op always
// leaves at its target, for the compensating op a dropped rename enqueues instead. It only reads
// the store; the write rides in deadLetterCompensateAndAck's transaction.
//
// It is not bookkeeping symmetry - it is what arms the never-lose gate. applyDownload's
// fresh-create funnel (untrackedFilePresent) fires only when a not-yet-synced record sits at the
// target: a present file with NO baseline and no byid entry for the node is read as "not
// reconcile-produced" and overwritten DIRECTLY. The drop that precedes this compensation clears
// exactly those records, so without the pending write the repair would clobber whatever user
// content occupies the destination - turning a fix for one data-loss bug into another. With
// it, the destination behaves like any other fresh server create: a foreign occupant is preserved
// as a _Conflict copy. (The mkdir arm is already covered by guardMkdirOccupant, which derives an
// empty base from the compensation's absent capture fields and preserves the occupant whole.)
//
// A record already at the destination (including an undecodable one) needs no write and gets
// none: overwriting it would evict ANOTHER node's baseline through the byid index, and every
// shape it can take already fires a gate of its own.
func (e *Engine) compensationTracking(comp store.Op) (*store.TrackedBaseline, error) {
	_, err := e.store.GetBaseline(comp.ViewID, comp.Path)
	switch {
	case isNotFound(err):
	case err == nil, errors.Is(err, store.ErrCorruptRecord):
		return nil, nil
	default:
		return nil, fmt.Errorf("engine: read compensation target %d/%q: %w", comp.ViewID, comp.Path, err)
	}
	return &store.TrackedBaseline{
		ViewID: comp.ViewID,
		Path:   comp.Path,
		Base: store.Baseline{
			FileID:       comp.FileID,
			FileType:     fileType(comp.Type == store.OpMkdir),
			ServerSyncID: comp.ServerSyncID,
			State:        store.StatePending,
		},
	}, nil
}

// queuedInboundCoversNode reports whether the durable queue holds an INBOUND op for fileID other
// than exceptSeq - any type, since a queued rename, download or mkdir of the node all end with
// the node materialised (or dropped and compensated in turn) by an op that already exists. A
// scan failure propagates so the caller leaves its op queued rather than deciding on incomplete
// information, mirroring queuedInboundRenameCarries; an empty file id matches nothing.
func (e *Engine) queuedInboundCoversNode(viewID uint64, fileID string, exceptSeq uint64) (bool, error) {
	if fileID == "" {
		return false, nil
	}
	var found bool
	if err := e.scanQueuedInView(viewID, "inbound ops", func(qo store.QueuedOp) error {
		if qo.Seq == exceptSeq || qo.Direction != store.DirInbound || qo.FileID != fileID {
			return nil
		}
		found = true
		// One match settles the question; stop decoding the rest of the queue.
		return errStopScan
	}); err != nil {
		return false, err
	}
	return found, nil
}

// relocateDescendantBaselines moves every baseline recorded under the old directory prefix
// (oldDir + "/") to the corresponding path under newDir, after an on-disk directory rename
// carried the subtree across. The codebase models a directory move as a single event that
// re-homes the whole subtree (no per-descendant events - see reconcile's prefix re-basing),
// so without this the descendants' files records and byid entries would be left under the old
// prefix, diverging from disk: a later child delete would resolve the node by id to a path that
// no longer exists and scans would target stale locations.
//
// Each descendant is relocated by a single store.MoveBaseline, which writes the new path and
// drops the old record in one transaction: for an indexed node the store moves it through the
// byid index, and for a local-only descendant (no file id) the same transaction deletes the
// stale old record. Folding both into one transaction is what keeps the relocation crash-replay
// safe - a separate Put then Delete could crash between them and leave an empty-id duplicate
// under the old prefix that the destination-occupied guard below then skips forever, stranding it.
// The subtree is collected first and mutated after the read transaction closes
// (scanBaselinePrefix holds a live cursor), the same stream-then-mutate shape
// dropDescendantBaselines uses.
//
// The scan is bounded to the moved subtree by a store-side cursor prefix
// (store.ScanBaselinesUnderPrefix): a directory move used to materialise and
// decode EVERY baseline of the view - O(view) JSON decodes and O(view) memory per move, on the
// inbound apply path - where only the descendants can ever be relocated.
//
// Accepted behaviour: a descendant whose stored value no longer decodes is
// not relocated and stays under the old prefix (it is Warned, not moved: its file id - the
// identity proof the destination guard below needs - is unknowable from the rotted bytes). That
// is the fail-safe direction - the stranded record is later swept by localReconcile's
// ScanBaselinesForView skip-and-Warn (it never diffs it as a delete) and heals when its key is
// next overwritten - rather than aborting the whole rename apply on one bad record. The
// destination check below tolerates a corrupt occupant symmetrically.
func (e *Engine) relocateDescendantBaselines(viewID uint64, oldDir, newDir string) error {
	// A move whose endpoints resolve to the same directory carries nothing across. Without
	// this guard each descendant would be written back to its own path and then deleted
	// (the PutBaseline is a no-op overwrite, but the DeleteBaseline that follows is not),
	// wiping the whole subtree's baselines. Both the executor and the self-echo path call
	// this helper, so a degenerate move event (identical or canonically equivalent
	// endpoints) must be a no-op here rather than destroying tracking for the subtree.
	if path.Clean(oldDir) == path.Clean(newDir) {
		return nil
	}
	var moved []store.PathBaseline
	skipped, err := e.scanBaselinePrefix(viewID, oldDir, func(pb store.PathBaseline) error {
		moved = append(moved, pb)
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine: list baselines to relocate %d/%q: %w", viewID, oldDir, err)
	}
	for _, p := range skipped {
		e.warnCorruptBaseline("relocate-descendant-src", viewID, p, store.ErrCorruptRecord)
	}
	// One transaction per CHUNK of descendants, not per descendant. Each one costs a read plus
	// an fsync'd write, and this runs EAGERLY inside reconcile - on the loop goroutine that also
	// owns polling and the drain - so a server-side rename of a large tree stalled the whole sync
	// cycle for as long as the commits took. The rollback unit stays small, and a rolled-back
	// chunk is simply not relocated yet: the op stays queued and the retry re-lists what is left
	// under the old prefix, which is exactly what a partial pass leaves behind anyway.
	for start := 0; start < len(moved); start += subtreeBaselineBatch {
		chunk := moved[start:min(start+subtreeBaselineBatch, len(moved))]
		if err := e.batchStore(func(b *store.Batch) error {
			return e.relocateDescendantChunk(b, viewID, oldDir, newDir, chunk)
		}); err != nil {
			return err
		}
	}
	return nil
}

// relocateDescendantChunk relocates one batch of descendants inside w's transaction. It is the
// body of the loop above, unchanged in every decision it makes - only the writer differs, so a
// batched relocation and an unbatched one resolve an occupied destination identically.
func (e *Engine) relocateDescendantChunk(w store.Writer, viewID uint64, oldDir, newDir string, moved []store.PathBaseline) error {
	for _, pb := range moved {
		newPath := newDir + pb.Path[len(oldDir):]
		// Never clobber a different node already recorded at the destination. reconcile
		// rebases a moved directory's descendants to the new prefix as it folds the move
		// event (relocatePending), before a later same-batch event can repopulate the old
		// prefix. So a baseline still sitting under the old prefix at apply time may be a
		// freshly created node - move a->b followed by create a/x re-creates a/x with a new
		// id - not a genuine descendant of the moved tree. Relocating it onto the real moved
		// child already at the destination would strand that child and, on the new node's
		// download, delete its file (the data-loss case). Leave such a node where it is; the
		// genuine descendant is already at newPath.
		//
		// Identity can only be established by a matching non-empty file id. The store treats
		// an empty file id as a distinct, unindexed node (PutBaseline), so two empty ids that
		// compare equal are NOT the same node: relocating a local-only descendant onto an
		// occupied local-only destination would overwrite that node's baseline and delete the
		// source. So leave an occupied destination untouched whenever the moved
		// record carries no id - only an exact non-empty id match proves the same node and
		// makes the relocation a safe idempotent overwrite.
		existing, gerr := w.GetBaseline(viewID, newPath)
		switch {
		case errors.Is(gerr, store.ErrCorruptRecord):
			// An undecodable record already occupies the destination: leave it (and the
			// source) in place rather than aborting the whole rename apply - the same
			// conservative "occupied destination, skip" the id-mismatch branch takes, and
			// fail-safe since we cannot prove the occupant is the same node.
			e.warnCorruptBaseline("relocate-descendant-dest", viewID, newPath, gerr)
			continue
		case gerr != nil && !isNotFound(gerr):
			return fmt.Errorf("engine: check relocate destination %d/%q: %w", viewID, newPath, gerr)
		}
		if gerr == nil && (pb.FileID == "" || existing.FileID != pb.FileID) {
			continue
		}
		if err := w.MoveBaseline(viewID, pb.Path, newPath, pb.Baseline); err != nil {
			return fmt.Errorf("engine: relocate descendant baseline %d/%q->%q: %w", viewID, pb.Path, newPath, err)
		}
	}
	return nil
}

// guardRenameDest preserves a locally-diverged or untracked file at an inbound rename's
// destination before the move clobbers it. It runs only for a real pending move: when
// the source is already gone the move either applied (a replay) or never had a source, so there is
// nothing about to overwrite the destination - and preserving then would set OUR own already-moved
// node aside. Because os.Rename is an atomic move, a still-present source proves the move has not
// run, so any regular file at the destination is foreign content (not ours).
//
// The divergence basis is captured at reconcile time (DestUntracked / DestBaseLocalHash), before
// relocatePending overwrote the destination baseline: an untracked destination preserves any
// regular file found there; a foreign tracked destination preserves it only when its on-disk
// content diverges from the captured last-synced hash. A synced foreign record with NO recorded
// hash - the inbound-rename bare/promoted-pending write anomaly, or a dir record - is captured as
// DestUntracked by reconcile (the bare-record doctrine carried to the capture side), so a no-basis
// op only arises from records predating the capture fields and is left to server authority.
// preserveDivergentLocal does the no-replace rename-aside + durable Conflict record, so the
// subsequent fsutil.Rename lands our node on the now-free path.
//
// Live recheck: the capture describes the destination as it stood PRE-BATCH,
// but a download queued for the destination's occupant node can apply between reconcile and this
// op's drain slot (they share the path key, so the download runs first) and advance the live
// destination baseline to StateSynced. The on-disk file is then freshly synced SERVER content, not
// pre-batch user content - preserving it whole on the stale DestUntracked capture would haul
// just-downloaded server bytes into a spurious _Conflict copy that the next scan re-uploads. When
// the live baseline (dst, read up front by applyRename) records a FOREIGN node as StateSynced, its
// LocalHash supersedes the captured basis: a matching on-disk file overwrites directly (the
// live-adoption mirror), an edit landed AFTER that apply still diverges and is preserved,
// and a live synced record with an EMPTY hash preserves any regular file whole - that shape is not
// a freshly-downloaded occupant (markSynced always derives a hash) but the inbound-rename
// bare/promoted-pending write anomaly or a dir baseline a file locally replaced, so there
// is no trustworthy base to clobber against (never-lose, coordinated with removeBaseFor). An
// OURS-synced record (dst.FileID == op.FileID) keeps the captured behaviour: that shape is the
// already-applied replay whose capture branch left both fields empty, and the early return below
// skips the guard for it exactly as before.
func (e *Engine) guardRenameDest(op store.Op, from, to string, dst store.Baseline, dstTracked bool) error {
	if renameHasNoSource(from) {
		return nil
	}
	// base is the divergence basis preserveDivergentLocal checks against: the captured
	// last-synced hash for a foreign tracked destination, or "" for an untracked one
	// (preserving the file whole).
	base := op.DestBaseLocalHash
	haveBasis := op.DestUntracked || base != ""
	if dstTracked && dst.State == store.StateSynced && dst.FileID != op.FileID {
		// A live synced FOREIGN record supersedes the reconcile-time capture. Its LocalHash is the
		// divergence base - EXCEPT an empty one: markSynced never leaves a file record hash-less, so
		// an empty LocalHash here is a dir baseline a regular file locally replaced, or the
		// inbound-rename bare/promoted-pending write anomaly. With no trustworthy base,
		// preserve any regular file at the destination whole rather than clobbering it under a false
		// server-authority read (never-lose, coordinated with removeBaseFor / relocatedRemoveBase).
		// A freshly-downloaded occupant always carries a non-empty hash, so the
		// "adopt the live synced hash, overwrite matching server bytes directly" path is unchanged.
		base = dst.LocalHash
		haveBasis = true
	}
	if !haveBasis {
		return nil // tracked destination with no divergence basis; server authority
	}
	fi, err := os.Lstat(to)
	if err != nil {
		return nil // destination absent (clean move) or unstattable; nothing to clobber
	}
	if !fi.Mode().IsRegular() {
		return nil // only a regular file is silently clobberable by os.Rename
	}
	if _, err := e.preserveDivergentLocal(op.ViewID, op.NewPath, to, base); err != nil {
		return err
	}
	return nil
}

// renameHasNoSource reports whether a failed rename has nothing left to move on disk
// because its source is already gone. os.Rename only fails with a missing source when
// the source was absent before the call (it never deletes the source itself), so a
// gone source means the rename did no damage and there is nothing to retry: either the
// move already applied (the destination holds the node) or the node was never
// materialised on disk - e.g. its creating download was dropped as superseded, or a
// same-batch chain left the source empty. In every such case the on-disk move is a
// no-op and must not wedge the queue; the baseline bookkeeping in applyRename still
// runs. A failure that leaves the source still present (or a stat error) is a real
// error and is surfaced by the caller.
func renameHasNoSource(from string) bool {
	_, err := os.Lstat(from)
	return os.IsNotExist(err)
}

// destExists reports whether the rename destination is present on disk. Paired with a
// gone source, a present destination means the move effectively took effect (the node
// is where it belongs, so swallowing the failed rename is safe); an absent destination
// means the move never happened and there is no node to record.
func destExists(to string) bool {
	_, err := os.Lstat(to)
	return err == nil
}
