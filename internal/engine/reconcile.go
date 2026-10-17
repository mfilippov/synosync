package engine

// This file carries the Phase-6b remote-event reconcile pass: it turns a batch of
// pull_event changes (ascending by sync_id) into the inbound durable-queue ops the
// executor drains, while folding the server facts into the baseline. It is the
// engine's policy core and touches only the store - no network - so the golden
// scenarios (create/modify/delete/move, self-echo) are exercised against an
// in-memory store with a fake client.
//
// Variant 1 ("remote-driven core") treats the server as the sole authority: a
// remote change is applied unconditionally - EXCEPT the Phase-6d inbound conflict
// funnel. A remote create/modify for a path that already has a queued DirOutbound
// upload is NOT clobbered here: reconcile leaves the stale server base + StatePending
// local facts intact so the pending push funnels through the server's 24579 stale-base
// rejection into the one keep-both resolver (conflict.go). Conflict resolution itself
// (server-wins-with-_Conflict-copy) lives in conflict.go; the cold-start full-catalog
// pull (the one-time first reconciliation for a never-synced view) lives in
// coldstart.go (DESIGN §6/§13).

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// isNotFound reports whether err is the store's "no such record" sentinel, used to
// treat an absent baseline as a no-op rather than a failure.
func isNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}

// reconcile folds a batch of remote events for one view into the store: each
// non-self-echo event is mapped to an inbound store.Op, the op enqueued, and its
// baseline marked StatePending; each self-echo event (committed by this
// client) skips the queue but advances the baseline to StateSynced. It returns the
// ops it enqueued, in the order they were enqueued, and the new cursor - the
// highest sync_id seen, never below the incoming cursor. The caller persists the
// cursor only after the executor has applied the ops, so a crash mid-batch replays
// from the durable queue rather than skipping ahead.
//
// Events are applied strictly by ascending sync_id: the input is defensively
// re-sorted by sync_id and the resulting ops are enqueued in that same order, never
// reordered. This keeps causal sequences on a single path correct (e.g. delete-then-
// create of the same path stays delete-then-create). The one elision is a download
// whose node a later event in the same batch destroys - a delete, or a move claiming
// its path for a different node (see supersededByLaterDelete): the node is already
// gone server-side, so enqueuing the download would only wedge the queue - the
// remove/rename that supersedes it is enqueued instead. Directory ordering needs no
// reshuffle here: fsutil.Mkdir / a download's parent-mkdir create missing parents
// (MkdirAll) and fsutil.Remove deletes recursively (RemoveAll), so a parent and child
// in one batch apply safely whatever order they run in.
func (e *Engine) reconcile(viewID uint64, events []actions.Event, cursor int64) ([]store.Op, int64, error) {
	sorted := e.normaliseBatch(viewID, events, cursor)
	sorted, rescans, seen := e.scopeExcluded(viewID, sorted)
	if err := e.addRescans(viewID, rescans); err != nil {
		return nil, cursor, err
	}

	bc, err := e.readBatchContext(viewID, sorted)
	if err != nil {
		return nil, cursor, err
	}
	inheritedBases, queuedDownloadIDs := bc.inheritedBases, bc.queuedDownloadIDs
	pendingUploads, snap := bc.pendingUploads, bc.snap

	newCursor := cursor
	if seen > newCursor {
		newCursor = seen
	}
	var ops []store.Op
	// downloadedThisBatch tracks the file ids for which a download has already been enqueued
	// earlier in THIS batch, complementing queuedDownloadIDs (the prior-batch queue snapshot)
	// so the supersession gate below covers the same-batch [create f, modify f] case too - the
	// create's download is in the durable queue but not in the batch-start snapshot.
	downloadedThisBatch := make(map[string]bool)
	// deferredDownloads maps the file id of every create/modify whose download the keep-both
	// funnel below deferred back to that event, for the rest of THIS batch. The deferral rests
	// on an invariant - the deferred node stays on the disputed path, so the queued upload's
	// 24579 rejection funnels into the resolver, which downloads the server's version there -
	// and a later same-batch MOVE of that node breaks it: the node leaves the path, nothing
	// ever materialises it (its rename dead-letters, the node having never been tracked), and
	// its create event is already consumed (the cursor advanced past it), so the server node is
	// permanently missing from the replica with only a generic Dropped trace.
	// Registering it here lets the move re-emit the download at the node's new path.
	deferredDownloads := make(map[string]actions.Event)
	// provenDeferrals holds, per node, the deferred create/modify whose divergence from the
	// queued upload's base the event ITSELF proves (finding H1's cheap addition). Each survivor
	// becomes a keep-both resolve after the batch - keyed by file id, so a second modify of the
	// same node merely refreshes the entry and a move rescue removes it along with the deferral it
	// belongs to.
	provenDeferrals := make(map[string]actions.Event)
	for i, ev := range sorted {
		if ev.SyncID > newCursor {
			newCursor = ev.SyncID
		}
		if !eventContainable(ev) {
			// The server supplied a structurally-invalid path (absolute, escaping via
			// "..", or collapsing to the view root). It can never be materialised, so
			// skip it BEFORE touching any baseline - relocating or rewriting a baseline
			// for a path the executor will only dead-letter would strand the affected
			// node untracked. The cursor still advances above, so the
			// unappliable event is acknowledged rather than re-pulled forever. A move
			// arriving FROM such a path onto a containable one was already rewritten as a
			// create by rescueUncontainableSources, before this batch's other passes ran.
			e.skipUncontainableEvent(viewID, ev)
			continue
		}
		if aliased, ok := eventAliasesIDRoot(ev); ok {
			// The event names a ROOT path beginning with "id:", which aliases the wire's
			// id-addressing form (id:<file_id>). Materialising it would let a later outbound
			// rename/remove round-trip that literal name back into the "path" parameter and,
			// if the DSM dispatches by prefix, mutate a FOREIGN node's id (DESIGN §13).
			// Skip it here - the cursor still advances above, mirroring the non-containable
			// skip and the scanner-side protect - so the symmetric injection can never take
			// hold, without touching any baseline for a path we will never sync.
			e.log.Warn("engine: skipping event whose root path aliases id-addressing",
				"view", viewID, "action", ev.Action, "sync_id", ev.SyncID, "path", aliased)
			continue
		}
		if e.isSelfEcho(ev) {
			if err := e.applySelfEcho(viewID, ev); err != nil {
				return nil, cursor, err
			}
			continue
		}
		// Backfill the moved id of an id-less move from the source baseline. A server
		// move that omits the file id for a node we DO track at the source path would otherwise
		// dead-letter and diverge silently: relocatePending's id gate ("F" != "") skips the
		// relocation so the source stays StateSynced, and at apply time PathByID of the empty id
		// reads the node as untracked (nodeTracked=false) while classifyRenameSource sees a
		// "recreated" foreign source, so the srcRecreated && !nodeTracked arm drops the op - the
		// file stays at the old path locally (synced, agreement, so the scan never re-emits it)
		// while the server holds it at the new one, never converging. Adopting the tracked
		// source's real id makes the whole apply path treat it as an ordinary id-carrying move -
		// relocatePending relocates the baseline, PathByID resolves the node, the wrong-kind
		// guard is correctly skipped (the source IS ours), and a directory's descendants relocate
		// - reproducing the server state under any id-less-move semantics. Only a tracked,
		// non-empty-id source is adopted; an untracked or corrupt source leaves the move id-less
		// for the existing cold-start-gap / wrong-kind handling to reach, and the fix never
		// touches an id-carrying event (the replay/recreate classification is unchanged).
		if ev.Action == "move" && ev.FileID == "" {
			b, berr := e.store.GetBaseline(viewID, ev.FromPath)
			switch {
			case berr == nil && b.FileID != "":
				ev.FileID = b.FileID
			case berr == nil:
				// A tracked but local-only source (empty file id): nothing to adopt.
			case isNotFound(berr):
				// No baseline under the exact NFC key. On a case-insensitive MATCHING platform
				// the tracked source may sit under a case-variant spelling - a server case-only
				// rename (Foo.txt->foo.txt) before this move that we have not folded onto the
				// stored key - which the exact-key lookup misses, so the move stays id-less and
				// dead-letters into the same permanent path divergence the exact-key backfill
				// closed, only for the case-variant spelling. Retry by fold key (mirroring
				// trackedFoldIndex / parentBaselineByFoldedKey) and adopt BOTH the id and the
				// stored spelling: rewriting ev.FromPath to the record we actually hold aligns
				// every downstream exact-key read (relocatePending, classifyRenameSource) and the
				// PathByID hop check in applyRename with that spelling, so the whole apply runs the
				// ordinary id-carrying flow (relocate rich facts, disk move by id) instead of the
				// byte-level case difference reading as an untracked source / transient hop and
				// diverging. The fallback is gated OFF on a case-sensitive filesystem, where
				// "foo.txt" and "Foo.txt" are genuinely distinct nodes and a fold match would adopt
				// the wrong one's id (and on a case-sensitive FS the exact NFC key already matches
				// any real same-node spelling, so the fallback could only mis-match). No /
				// ambiguous / corrupt / a local-only fold match leaves the move id-less for the
				// cold-start-gap / wrong-kind path, unchanged.
				if normpath.CaseInsensitive() {
					pb, ok, ferr := e.moveSourceByFoldedKey(viewID, ev.FromPath)
					switch {
					case ferr != nil:
						return nil, cursor, fmt.Errorf("engine: fold-resolve id-less move source %d/%q: %w", viewID, ev.FromPath, ferr)
					case ok && pb.FileID != "":
						ev.FromPath = pb.Path
						ev.FileID = pb.FileID
					}
				}
			case errors.Is(berr, store.ErrCorruptRecord):
				// Undecodable source: cannot adopt an id; leave it id-less rather than aborting.
				e.warnCorruptBaseline("idless-move-source", viewID, ev.FromPath, berr)
			default:
				return nil, cursor, fmt.Errorf("engine: resolve id-less move source %d/%q: %w", viewID, ev.FromPath, berr)
			}
		}
		// A move of a node whose download this batch deferred to the keep-both resolver
		// rescues that deferral: the resolver can only arbitrate the DISPUTED path, and the
		// node has just left it, so re-emit the deferred create/modify at the move's
		// destination and let the rest of the loop handle it as an ordinary inbound
		// download there. The rename op itself is deliberately NOT
		// enqueued: the deferral placed nothing on disk and wrote no baseline, so for the
		// motivating shape (a brand-new server node at a path our node never occupied)
		// applyRename could only dead-letter on the foreign source or drag the user's
		// still-unpushed local file across the tree.
		//
		// The gate is deliberately NOT narrowed to that never-tracked shape, because the
		// deferral of a modify of a TRACKED node - the primary keep-both funnel: a queued
		// OpUpload, a StatePending record and the user's edited file all sitting at the
		// source - is worse off with the rename than without it. There the rename DOES apply
		// (the source exists), carrying the edited file to the destination, and relocatePending
		// has already moved the record there holding the edit's own LocalHash; the queued
		// upload then exits through relocatedUploadPath's revert and the record settles
		// StateSynced against local facts the server never saw, while the deferred modify's
		// event is already consumed (the cursor advanced). Both replicas then read "synced"
		// over different content - a silent permanent divergence with no _Conflict record.
		// Re-emitting the download instead leaves the edited file untracked at the source for
		// the next scan to push as a separate node and lands the server's content at the
		// destination: duplication and churn rather than silent loss of either side. The
		// residual (arbitrating that shape through keep-both at the destination, which needs
		// the op and its baseline to agree on a path markUpsertPending's byid redirect
		// currently splits) is recorded in DESIGN §13.
		// Re-entering the loop's normal path means the destination gets the same treatment
		// any server create does - including a second deferral if the DESTINATION also has
		// a queued upload (the re-registration below), the same-batch supersession elision,
		// and the fresh-create keep-both guards over any untracked file found there.
		if ev.Action == "move" && ev.FileID != "" {
			if dev, ok := deferredDownloads[ev.FileID]; ok {
				delete(deferredDownloads, ev.FileID)
				// The proven-divergence resolve registered for that deferral belongs to the path the
				// node is leaving; drop it with the deferral. If the destination is disputed too, the
				// re-emitted event registers a fresh one there when it defers again.
				delete(provenDeferrals, ev.FileID)
				e.log.Info("engine: re-emitting deferred inbound download at the moved node's new path",
					"view", viewID, "from", ev.FromPath, "to", ev.Path,
					"file_id", ev.FileID, "sync_id", ev.SyncID)
				ev = rescueDeferredMove(dev, ev)
			}
		}
		op, ok := opForEvent(viewID, ev)
		if !ok {
			continue
		}
		if op.Type == store.OpDownload && pendingUploads[normpath.Key(ev.Path)] {
			// A local content change for this exact path is already queued for upload.
			// Defer to the keep-both resolver: skip the inbound download and leave the
			// baseline's server facts (the stale base) untouched so the queued upload
			// still fires and draws the 24579 rejection. The cursor still advances above,
			// so the event is acknowledged rather than re-pulled forever.
			//
			// Remember the deferred node so a later same-batch move can re-emit its
			// download at the path it lands on (see the move rescue above). Only an
			// id-carrying event can be followed across a move; a sparse id-less create
			// leaves the residual (as does a move arriving in a LATER batch - the
			// registration is per-batch and the cursor has already advanced past the
			// create, so nothing durable ties them together; DESIGN §13).
			if ev.FileID != "" {
				deferredDownloads[ev.FileID] = ev
			}
			// The deferral used to rest ENTIRELY on the queued upload drawing a 24579 the live
			// server almost never sends (finding H1, §2): "compare_mtime" accepts any write whose
			// mtime is not older, so the funnel never fired, the download was already dropped and
			// the cursor already advanced past it. Here, though, the divergence is not a hope - it
			// is PROVEN, locally and for free: the event reports content the pre-batch baseline
			// never agreed on while a local edit of the same path is queued for push. That is
			// exactly DESIGN §6's "local change + remote change", so enqueue the keep-both
			// resolution now rather than betting on a server verdict.
			//
			// The upload keeps its place in the queue ahead of this resolve (both address the same
			// path, so serialize.go orders them): it now carries its own pre-turn divergence proof
			// (guardUploadTarget) and funnels a SECOND resolve if it agrees. A duplicate resolve is
			// harmless by construction - once the first has run, the canonical path holds the server
			// version and the second takes the false-conflict fast path - which is the same replay
			// tolerance funnelUploadConflict already documents.
			//
			// The resolve is REGISTERED here and enqueued after the batch, not on the spot: a later
			// event can still take the node off the disputed path (the move rescue below re-emits
			// the download at its destination) or destroy it outright, and a resolve already in the
			// queue for a path the node has left resolves against nothing - it re-funnels to the new
			// path and then wedges on a server that answers for neither.
			if ev.FileID != "" && provenDeferralDiverged(ev, snap) &&
				!supersededByLaterDelete(sorted, i, ev.Path, ev.FileID) {
				provenDeferrals[ev.FileID] = ev
			}
			e.log.Info("engine: deferring inbound download to keep-both conflict resolver",
				"view", viewID, "path", ev.Path, "sync_id", ev.SyncID)
			continue
		}
		if op.Type == store.OpDownload && supersededByLaterDelete(sorted, i, ev.Path, ev.FileID) {
			// A later event in this same batch destroys this node - a delete at its
			// current path or, if it is moved first, at the path it lands on (an
			// ancestor delete counts too), or an overwriting move claiming its exact
			// path for a different node: the downloaded bytes
			// would be wiped, and the server node is already gone, so the download
			// would fail and - because its conflicting rename/remove can never run
			// behind a failed predecessor - wedge the queue forever (the loop's
			// leading drain would keep failing and never poll again). Skip the doomed
			// download; the rename/remove ops still apply and carry the path to its
			// correct server-side state.
			continue
		}
		if op.Type == store.OpDownload {
			// Capture the last-synced local facts so applyDownload can guard against silently
			// clobbering a local edit the cheap scan (size+mtime short-circuit, Phase 9d)
			// missed: a same-size, same-mtime-second edit queues no outbound upload, so without
			// this the inbound download would overwrite it before the periodic full re-hash ever
			// sees it (data loss, not just latency). Only a StateSynced baseline with a recorded
			// local hash is a meaningful base, and it is read from the PRE-BATCH snapshot - an
			// earlier event in this same batch may already have rewritten the live baseline
			// through markPending. The motivating interleaving is [move a->b, modify b]: the
			// move relocates the baseline to b and flips it StatePending (markUpsertPending then
			// zeroes LocalHash), so a live read at the modify's gate captures nothing and
			// applyDownload would clobber the cheap-scan-missed edit the rename just carried to
			// b. The snapshot resolves the node by file id first (mirroring
			// markUpsertPending below), so a case-variant path ("foo.txt" for a node stored as
			// "Foo.txt") or a same-batch-moved path still recovers the real synced base.
			//
			// A node with no pre-batch synced record inherits the base an ALREADY-QUEUED
			// inbound op for it captured (inheritedBases, seeded from the durable queue): a
			// prior-batch download or rename flipped the baseline StatePending before this batch
			// arrived, so the snapshot cannot see the synced hash, but the queued op still carries
			// it - keyed by file id, move-invariant, so it follows the node across paths and every
			// download for a diverged node funnels rather than the second one clobbering. A
			// genuinely fresh create (no prior baseline, no prior queued op) leaves the base
			// empty and the apply-time check is correctly skipped.
			key := downloadBaseKey(ev.FileID, ev.Path)
			base := ""
			if b, ok := snap.node(ev.FileID, ev.Path); ok && b.State == store.StateSynced && b.LocalHash != "" {
				base = b.LocalHash
			}
			if base == "" {
				base = inheritedBases[key].hash
			}
			if base != "" {
				op.BaseLocalHash = base
				inheritedBases[key] = inheritedBase{hash: base}
			}
		}
		if op.Type == store.OpMkdir {
			// Capture whether a node was tracked at the mkdir's target path before this batch,
			// and its last-synced facts (mirroring the OpRemove capture
			// below): a server dir-create can land on a path holding a TRACKED regular file -
			// a file<->dir create/create collision - and markUpsertPending below overwrites
			// that baseline with the pending dir record, erasing the only evidence applyMkdir's
			// occupant gate has to tell a clean server-authoritative replacement from a
			// cheap-scan-missed local edit. Without the gate the mkdir dead-letters on the
			// occupant (ENOTDIR), the cleanup drops the baseline, and the next scan re-uploads
			// the file into the same collision forever. The snapshot resolves id-first: the dir
			// event's fresh id misses byID and falls back to the path, finding the occupant's
			// record; a re-reported tracked dir resolves to its own dir record (BaseIsDir), which
			// the gate conservatively reads as preserve-whole. A node whose synced record a
			// PRIOR batch's queued op already flipped inherits its base by the tracked path's
			// fold key (recordInheritedBase registers every id-carrying base under both keys).
			if b, ok := snap.node(ev.FileID, ev.Path); ok && b.State == store.StateSynced {
				op.BaseWasSynced = true
				op.BaseLocalHash = b.LocalHash
				op.BaseIsDir = b.FileType == "dir"
			} else if ib, ok := inheritedBases[normpath.Key(ev.Path)]; ok {
				op.BaseWasSynced = true
				op.BaseLocalHash = ib.hash
				op.BaseIsDir = ib.isDir
			}
		}
		if op.Type == store.OpRename {
			// Carry the MOVED node's pre-batch synced base on the rename op itself. Within one
			// batch a later modify's gate recovers that base from the snapshot (id-first), but a
			// modify arriving in a LATER batch before any drain cannot: by then relocatePending
			// has flipped the live baseline StatePending, the fresh snapshot sees no synced
			// record, and queuedDownloadBases used to inherit only from queued OpDownloads - a
			// rename carried nothing, so the cross-batch [move a->b] + [modify b] download ran
			// with an empty base and clobbered a cheap-scan-missed local edit the rename had
			// just carried to b. The queued rename now preserves the base for
			// queuedInboundBases to inherit, keyed move-invariantly by file id.
			if b, ok := snap.node(ev.FileID, ev.FromPath); ok && b.State == store.StateSynced {
				op.MovedBaseSynced = true
				op.MovedBaseLocalHash = b.LocalHash
				// The base's KIND comes from the pre-batch BASELINE, not from ev.IsDir: a
				// sparse move event reports IsDir false regardless of the node's real kind,
				// and a cross-batch delete inheriting this base would then read a synced
				// DIRECTORY's structurally-empty hash as "synced file without hash → server
				// authority" and destroy a regular file that locally replaced the directory
				// with no _Conflict copy - the direct remove-gate capture below already reads
				// the kind from the baseline, so the inherited producer must too
				// (mirroring BaseIsDir).
				op.MovedBaseIsDir = b.FileType == "dir"
			}
			// Capture the destination's pre-move base for applyRename's cluster-A guard: a
			// locally diverged or untracked file at the destination must not be silently
			// clobbered by the server move (the keep-both symmetry the inbound download guard
			// already has). ev.Path is the move's destination (op.NewPath); ev.FromPath is the
			// source. The destination is read from the PRE-BATCH snapshot: an earlier event in
			// this batch may already have rewritten the live record through markPending
			// (relocatePending/markUpsertPending), and what the guard protects - the on-disk
			// file at the destination - predates the batch, so its base must too.
			d, ok := snap.atPath(ev.Path)
			switch {
			case !ok:
				// No pre-batch baseline at the destination: a regular file found there at apply
				// time is user content never synced, so it must be preserved (DestUntracked). A
				// clean move whose destination is empty on disk simply finds nothing to preserve.
				op.DestUntracked = true
			case ev.FileID != "" && d.FileID == ev.FileID:
				// The destination already holds THIS node (an idempotent/replay reconcile): no
				// guard needed, the move is a no-op overwrite of our own node. Gated on a
				// non-empty ev.FileID: a sparse/empty-id move event would make "" == "" match a
				// not-yet-uploaded local destination (StatePending baseline with FileID == "")
				// and skip the guard, letting applyRename clobber the user's file. An empty id
				// falls through to the untracked branch below like any other foreign destination.
			case d.State == store.StateSynced:
				// A different (foreign) node is tracked at the destination as StateSynced. A FILE
				// record with a recorded LocalHash carries a genuine last-synced fact: capture it
				// and let applyRename detect a cheap-scan-missed local edit by comparing on-disk
				// content against it. A synced record with NO trustworthy base is treated as
				// untracked instead (never-lose doctrine): a FILE record with an
				// EMPTY LocalHash is the anomalous inbound-rename bare/promoted-pending write
				// (markSynced never leaves a file record hash-less), and a DIRECTORY record's
				// empty hash is structural, not a fact about any file - either way there is no
				// base to compare against, so guardRenameDest must preserve any regular file
				// found there whole rather than clobber it under a false server-authority read
				// (DestUntracked; the guard only ever preserves regular files, so a directory
				// occupant is unaffected). This is the capture-side mirror of removeBaseFor /
				// relocatedRemoveBase / untrackedFilePresent and of guardRenameDest's own
				// live-recheck arm - which cannot cover this shape: an overwrite-move of a
				// TRACKED node has relocatePending evict the bare record before apply, so by
				// then the live destination baseline is OUR pending record and this
				// reconcile-time capture is the only gate left.
				if d.FileType != "dir" && d.LocalHash != "" {
					op.DestBaseLocalHash = d.LocalHash
				} else {
					op.DestUntracked = true
				}
			default:
				// A foreign node is tracked at the destination but NOT synced - StatePending with
				// a queued outbound upload, or StateConflict. Its LocalHash, if any, records the
				// unpushed/divergent ON-DISK content, not a last-synced base, so a hash-vs-base
				// check would wrongly read "no divergence" and let the server move silently clobber
				// the user's file. Treat it like an untracked destination (mirroring the download
				// base-capture StateSynced gate above) so guardRenameDest preserves any regular
				// file found there.
				op.DestUntracked = true
			}
		}
		if op.Type == store.OpRemove {
			// Capture whether the removed node's baseline was StateSynced before this batch, and
			// its last-synced hash: by apply time flipPending has made it StatePending, which keeps
			// LocalHash but erases whether that was a synced base or unpushed on-disk content.
			// Only a synced baseline is a trustworthy divergence base; a non-synced one (a
			// queued-upload StatePending, a StateConflict) would make guardRemoveDivergence compare
			// the file against itself and let the server's delete clobber unpushed content (as the
			// rename DestUntracked / download base-capture StateSynced gates above).
			//
			// The record is read from the PRE-BATCH snapshot, id-first, because an earlier
			// same-batch move may already have relocated and flipped it ([move a->b, delete b]); a
			// live read would capture no base and resurrect a clean deleted file as a spurious
			// _Conflict copy. A sparse id-less delete is first mapped back through the batch's moves
			// (preBatchDeletePath): its event path names where the node sits NOW, while the
			// pre-batch record lives under the source spelling.
			//
			// BaseIsDir carries the base's node KIND: a synced directory baseline has an empty
			// LocalHash, and without the kind the guard would read "synced file with no recorded
			// hash → server authority" and destroy a regular file that locally replaced the
			// directory. With it, guardRemoveDivergence preserves such a file whole.
			//
			// A node whose live baseline a PRIOR batch already flipped StatePending (a queued [move
			// a->b], or a [modify f] download flipped by markUpsertPending) inherits its base from
			// that queued op (inheritedBases, seeded from the durable queue, mirroring the download
			// gate above). recordInheritedBase registers each base under BOTH the file-id key and
			// the folded tracked-path key, so a sparse id-less delete (path key, resolved by
			// preBatchDeletePath) inherits it too. Presence in the map means the node WAS synced
			// pre-batch, so it sets BaseWasSynced; without it the guard falls into removeBaseFor's
			// preserve-whole default and resurrects and re-uploads a clean deleted file.
			//
			// The resolved record must also IDENTIFY the node the event names (baseIdentifiesNode):
			// snap.node falls back to the path on a byID miss, and the path may now hold a
			// DIFFERENT node (our offline edit pushed as fresh node N after another client deleted
			// F, before F's delete event was polled). N's baseline records the current on-disk
			// hash, so capturing it would make the guard pass by construction and F's delete would
			// destroy the edit with no _Conflict copy; a foreign record yields no base, so the file
			// is preserved whole. The mkdir gate above deliberately skips this check: its subject
			// is the PATH's occupant, by construction a different node.
			capPath := ev.Path
			if ev.FileID == "" {
				capPath = preBatchDeletePath(sorted, i, ev.Path)
			}
			if b, ok := snap.node(ev.FileID, capPath); ok && b.State == store.StateSynced &&
				baseIdentifiesNode(b, ev.FileID) {
				op.BaseWasSynced = true
				op.BaseLocalHash = b.LocalHash
				op.BaseIsDir = b.FileType == "dir"
			} else if ib, ok := inheritedBases[downloadBaseKey(ev.FileID, capPath)]; ok {
				op.BaseWasSynced = true
				op.BaseLocalHash = ib.hash
				op.BaseIsDir = ib.isDir
			}
		}
		if op.Type == store.OpDownload && op.FileID != "" {
			// This newer create/modify supersedes every OLDER queued download of the same
			// node: drop them now, before this one is enqueued, so a stale download whose
			// redirect gate the markPending below extinguishes (a self-echo move relocated
			// the node, then this modify flips the record StatePending) can never race the
			// fresh one on disjoint conflict keys (see
			// dropSupersededSameIDDownloads for the carried-rename exemption). It runs
			// strictly after the elision checks above - a deferred/superseded event must
			// not drop the old op when no replacement is enqueued.
			//
			// Only run the whole-queue supersession scan when a same-id download can
			// actually exist - one from a prior batch (queuedDownloadIDs, the batch-start
			// queue snapshot) or one enqueued earlier in this batch (downloadedThisBatch).
			// A fresh-id download (every file of a cold catch-up backlog) skips it, keeping
			// reconcile O(queue) per batch instead of O(downloads x queue): without the gate
			// a large backlog paged into one syncView lease was quadratic.
			if queuedDownloadIDs[op.FileID] || downloadedThisBatch[op.FileID] {
				if err := e.dropSupersededSameIDDownloads(viewID, op.FileID); err != nil {
					return nil, cursor, err
				}
			}
			downloadedThisBatch[op.FileID] = true
		}
		// The op is enqueued BEFORE markPending rewrites the baseline (mirroring
		// localReconcile's op-before-baseline order): the durable queue is the apply/replay
		// unit, so of the two orderings this one's failure window is the survivable one -
		// an enqueued-but-unmarked event replays idempotently (the cursor was not advanced,
		// the replay's snapshot still sees the pre-event record and re-captures the same
		// base, and the duplicate op is one of the harmless kinds syncView already
		// documents). The reverse order - every baseline mutated in the loop, the whole
		// batch enqueued only after it - left a window where a crash produced pending
		// baselines with NO queued ops: the replay's snapshot then saw no synced record,
		// captured no divergence base, and the re-enqueued download set an untouched local
		// file aside as a spurious _Conflict copy plus a re-upload.
		//
		// Ordering is not sufficient for a CREATE, though, which is why the pair is
		// committed atomically there - see enqueueAndMarkPending.
		if err := e.enqueueAndMarkPending(op, viewID, ev); err != nil {
			return nil, cursor, err
		}
		ops = append(ops, op)
	}
	// Every deferral the batch PROVED diverged and did not later carry off its path becomes a
	// keep-both resolve now (finding H1's cheap addition). It runs after the loop so a node the
	// batch moved or destroyed is already out of the set.
	if err := e.enqueueProvenDeferralConflicts(viewID, snap, provenDeferrals); err != nil {
		return nil, cursor, err
	}
	return ops, newCursor, nil
}

// isSelfEcho reports whether an event was committed by this client's own session
// and so must not be re-applied to disk. An empty selfSessionID never matches, so a
// client that has not yet learned its session id treats every event as remote. The
// committer_session_id wire semantics still need verification against a live DSM
// (DESIGN §13).
func (e *Engine) isSelfEcho(ev actions.Event) bool {
	return e.selfSessionID != "" && ev.CommitterSessionID == e.selfSessionID
}

// opForEvent maps a remote event to the inbound store.Op that applies it, and
// whether the event produces an op at all. create/modify of a file download it;
// create/modify of a directory ensure it exists (idempotent mkdir); move renames
// from the event's source path; delete removes. An unrecognised action yields no op.
func opForEvent(viewID uint64, ev actions.Event) (store.Op, bool) {
	op := store.Op{Direction: store.DirInbound, ViewID: viewID, FileID: ev.FileID}
	switch ev.Action {
	case "create", "modify":
		// Carry the event's sync id so applyDownload/applyMkdir can stamp it onto the
		// synced baseline even when an earlier dead-lettered op for the same node has
		// already dropped the pending baseline reconcile pre-wrote (so the rebuilt
		// record keeps ServerSyncID rather than resetting it to zero).
		op.ServerSyncID = ev.SyncID
		if ev.IsDir {
			op.Type = store.OpMkdir
			op.Path = ev.Path
			op.LocalMTime = ev.MTime
			return op, true
		}
		op.Type = store.OpDownload
		op.Path = ev.Path
		// Carry the server-reported content facts so applyDownload can (a) pick the chunked
		// resume_download path for a file over the resume threshold (LocalSize) rather than
		// buffering it whole through single-frame Download, and (b) verify the whole-file MD4
		// of a chunk-assembled file (LocalHash), since the per-chunk resume replies only
		// describe their own chunk. The single-frame Download path still re-derives these from
		// its verified reply, so threading them changes nothing for small files.
		op.LocalHash = ev.Hash
		op.LocalSize = ev.Size
		op.LocalMTime = ev.MTime
		return op, true
	case "move":
		op.Type = store.OpRename
		op.Path = ev.FromPath
		op.NewPath = ev.Path
		// Carry the move's own position, exactly as the create/modify and delete arms do.
		// dropRenameCompensated stamps it onto the compensating download a dropped rename
		// leaves behind (finding H2), which is what gives the re-requested node the same
		// provenance the rename would have given it: applyDownload only advances a rebuilt
		// baseline's ServerSyncID when the op carries one, and a node whose record is left
		// stamped 0 is invisible to the positional guards that read it - H3's
		// removeSupersededByLaterObservation and downloadRelocatedBySelfEcho both disarm
		// themselves at 0.
		op.ServerSyncID = ev.SyncID
		// Thread the moved node's kind so applyRename's foreign-source guard can tell our
		// never-materialised node from unrelated user content of a different kind sitting at
		// the on-disk source (a file vs a directory) and drop the rename rather than dragging
		// that content across the tree mislabelled as op.FileID.
		op.IsDir = ev.IsDir
		return op, true
	case "delete":
		op.Type = store.OpRemove
		op.Path = ev.Path
		// Carry the delete's own position so applyRemove can refuse a path the server has
		// described AGAIN since (finding H3): a baseline stamped above this sync id is a
		// LATER statement about the path than the delete, so the node standing there is a
		// successor the delete never named. The field is additive and read by no other
		// remove-side code, so an op enqueued before this existed decodes to 0 and keeps
		// the historical behaviour (the guard disarms itself at 0).
		op.ServerSyncID = ev.SyncID
		return op, true
	default:
		return store.Op{}, false
	}
}

// provenDeferralDiverged reports whether a create/modify the keep-both funnel is about to DEFER to
// a queued outbound upload is already PROVEN to diverge from the base that upload is pushing
// against - the event reports a content hash the pre-batch baseline never agreed on, while a local
// edit of the same path waits to be pushed. That is DESIGN §6's "local change + remote change →
// conflict", decided from the batch alone with no server turn (finding H1's cheap addition, measured 14.08.2026).
//
// Before this, the deferral advanced the cursor on nothing but the HOPE that the queued upload
// would draw a 24579 from the server. It almost never does - the measured "compare_mtime" policy
// accepts any write whose mtime is not older - so the remote version was consumed and then
// overwritten. The upload's own pre-turn probe (guardUploadTarget) is the primary fix; this is the
// half that needs no round trip, and it also covers an upload that never reaches its probe
// (dropped, relocated, its source vanished).
//
// It is deliberately conservative, requiring positive proof on every side:
//   - the event must carry a content hash (Event.Hash is tolerantly decoded, and a bare event is
//     no evidence - the shared reply-evidence rule);
//   - a pre-batch baseline must exist for the node and carry a ServerHash to compare against (an
//     empty base would read as divergence for every deferral);
//   - the two must actually differ.
//
// Anything short of that leaves the deferral exactly as it was.
func provenDeferralDiverged(ev actions.Event, snap batchBaselines) bool {
	if ev.Hash == "" {
		return false
	}
	b, ok := snap.node(ev.FileID, ev.Path)
	return ok && b.ServerHash != "" && b.ServerHash != ev.Hash
}

// enqueueProvenDeferralConflicts turns every deferral whose divergence the batch PROVED
// (provenDeferralDiverged) and that survived the batch into a keep-both resolve, after the
// per-event loop has finished. Deferring the enqueue to here is what makes it safe: a later event
// can still carry the node off the disputed path (the move rescue re-emits its download at the
// destination and removes the registration) or destroy it, and a resolve queued for a path its
// node has left resolves against nothing.
//
// The resolve mirrors funnelUploadConflict's shape - it carries the node's file id (so the
// resolver's server-vanished disambiguation keys on the stable identity rather than the leaf name),
// the baseline's mode, and the event's sync id. It is enqueued alongside the still-queued upload
// for the same path, which serialises AHEAD of it: if that upload's own pre-turn probe agrees, it
// funnels a second resolve, and the duplicate is harmless by construction - once the first has run,
// the canonical path holds the server version and the second takes the false-conflict fast path,
// the same replay tolerance funnelUploadConflict already documents.
//
// Nothing here mutates a baseline, so the "enqueue before baseline mutation" invariant is untouched.
func (e *Engine) enqueueProvenDeferralConflicts(viewID uint64, snap batchBaselines, proven map[string]actions.Event) error {
	if len(proven) == 0 {
		return nil
	}
	// Deterministic order: the queue is a durable FIFO and a map walk would make the drain order
	// (and every test over it) depend on Go's map iteration.
	ids := make([]string, 0, len(proven))
	for id := range proven {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	// The map dedups WITHIN a batch; the queue is what dedups across them. The deferral re-proves
	// itself on every poll batch for as long as the upload sits queued (the pre-batch snapshot
	// cannot move until something applies, and each new peer edit brings a fresh hash), so without
	// the cross-batch dedup the queue grew by one resolve per batch for the whole life of a wedged
	// upload - every one of them serialised behind it on the same path key.
	//
	// ONE scan answers it for the whole set. Asking per deferral streamed and unmarshalled the
	// entire durable queue once per entry, and the set grows with exactly the backlog shape the
	// queue does - N locally-edited files with queued uploads, the same N edited by a peer, one
	// pull_event page - so it was N full walks of an N-op queue inside one reconcile call, on the
	// loop goroutine that also owns the drain and the watcher wake. It is the quadratic shape this
	// file already guards on its two siblings (pendingOutboundUploads' one-scan-per-batch, and
	// dropSupersededSameIDDownloads' gate).
	resolves, qerr := e.queuedResolvePaths(viewID)
	if qerr != nil {
		return qerr
	}
	for _, id := range ids {
		ev := proven[id]
		// A resolve already queued for the path will do the same work when it drains.
		if resolves[normpath.Key(ev.Path)] {
			continue
		}
		var perm uint32
		if b, ok := snap.node(ev.FileID, ev.Path); ok {
			perm = b.UnixPerm
		}
		e.log.Info("engine: deferred inbound download is provably diverged from the queued upload's base; enqueuing the keep-both resolution rather than waiting for a server verdict",
			"view", viewID, "path", ev.Path, "sync_id", ev.SyncID, "file_id", ev.FileID, "event_hash", ev.Hash)
		if _, err := e.enqueueOp(store.Op{
			Type:         store.OpResolveConflict,
			Direction:    store.DirOutbound,
			ViewID:       viewID,
			Path:         ev.Path,
			FileID:       ev.FileID,
			UnixPerm:     perm,
			ServerSyncID: ev.SyncID,
		}); err != nil {
			return fmt.Errorf("engine: enqueue deferred-divergence resolve %d/%q: %w", viewID, ev.Path, err)
		}
		// The set is the dedup, so it has to learn about what this loop itself enqueues: two
		// events of DIFFERENT nodes proven at one path would otherwise both queue a resolve
		// (mirroring localreconcile's queued.add).
		resolves[normpath.Key(ev.Path)] = true
	}
	return nil
}

// queuedResolvePaths collects the fold keys of every OpResolveConflict the durable queue already
// holds for the view, in ONE scan. The keys are folded so a case-only or NFC/NFD variant of the
// same spelling counts as the same path. A scan failure propagates: the caller would rather leave
// the batch unfinished (and retry it) than enqueue duplicates on an unread queue.
func (e *Engine) queuedResolvePaths(viewID uint64) (map[string]bool, error) {
	keys := make(map[string]bool)
	if err := e.scanQueuedInView(viewID, "resolves", func(qo store.QueuedOp) error {
		if qo.Type != store.OpResolveConflict {
			return nil
		}
		keys[normpath.Key(qo.Path)] = true
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

// rescueUncontainableSources rewrites every rescuable move in the batch in place, before any
// batch-wide pass reads it (see the call site in reconcile). Events it cannot rescue are left
// exactly as the server sent them, for the per-event loop's skip to report.
func (e *Engine) rescueUncontainableSources(viewID uint64, sorted []actions.Event) {
	for i, ev := range sorted {
		if eventContainable(ev) {
			continue
		}
		resc, ok := rescueUncontainableSource(ev)
		if !ok {
			if ev.Action == "move" && ev.FromPath != "" && !ev.KindReported && containable(ev.Path) {
				// The one rescuable SHAPE the evidence gate holds back: the operator's
				// documented remedy will not converge until the node's kind is known, so
				// say so rather than letting it read as an ordinary structural skip.
				e.log.Warn("engine: not rescuing a move off a non-containable path: the event reports no node kind",
					"view", viewID, "from", ev.FromPath, "to", ev.Path,
					"file_id", ev.FileID, "sync_id", ev.SyncID)
			}
			continue
		}
		e.log.Info("engine: re-emitting a move off a non-containable path as a create at its destination",
			"view", viewID, "from", ev.FromPath, "to", ev.Path,
			"file_id", ev.FileID, "sync_id", ev.SyncID, "is_dir", ev.IsDir)
		sorted[i] = resc
	}
}

// rescueUncontainableSource rebuilds a move whose SOURCE path is non-containable, but whose
// DESTINATION is containable, as a plain create at that destination. Without it the event is
// skipped and the node never converges, which breaks the documented operator remedy for an
// unrepresentable name (namemangle.go, DESIGN §6): Windows cannot spell "notes.", the operator
// renames it to "notes" on the server, and the server sends one move whose FromPath this host
// still cannot spell.
//
// The rewrite is safe because a non-containable source can never have been materialised or
// tracked here (its create was skipped and abs() dead-letters any op naming it), so there is no
// baseline to relocate and nothing on disk to carry. The destination is re-checked, and the
// rebuilt event re-enters the ordinary loop - the id-root alias guard, the keep-both deferral,
// same-batch supersession elision and the never-lose gates all still run.
//
// The reverse (a containable source moving ONTO an unspellable name) is NOT rescued: the node is
// leaving, and skipping keeps the local copy (the never-lose direction). Children skipped under
// an unrepresentable ancestor stay missing, since a server directory rename emits no events for
// descendants - a residual recorded in DESIGN §13.
//
// The rescue REQUIRES the event to state the node's kind (KindReported): opForEvent dispatches a
// create on ev.IsDir alone, a sparse move reports IsDir false regardless, and unlike other move
// consumers there is no baseline to fall back on. A kind-less rescue would enqueue OpDownload
// against a DIRECTORY's file id; inbound ops are excluded from the deterministic-rejection
// dead-letter (DESIGN §13), so it would retry every drain forever and serialize.go would block
// every path-overlapping successor. Without the evidence the event is skipped with its own Warn.
//
// It equally REQUIRES a reported source path: containable("") is false, so an absent from_path
// (tolerated by decodeEvent, common in sparse moves) would pass the non-containable test on no
// evidence - and the node is usually tracked at the path it moved off. Rewriting that rename into
// a create skips relocatePending: a file converges only via a whole re-download plus
// removeRelocatedPriorFile, and a DIRECTORY never converges (PutBaseline's byid move-detection
// drops the source record, leaving the old directory untracked over tracked children, which the
// next local scan pushes back as a duplicate). An absent source is skipped.
func rescueUncontainableSource(ev actions.Event) (actions.Event, bool) {
	if ev.Action != "move" || ev.FromPath == "" || containable(ev.FromPath) || !containable(ev.Path) {
		return actions.Event{}, false
	}
	if !ev.KindReported {
		return actions.Event{}, false
	}
	res := ev
	res.Action = "create"
	res.FromPath = ""
	res.FromFileID = ""
	return res, true
}

// rescueDeferredMove rebuilds the create/modify event dev - whose inbound download this
// batch deferred to the keep-both resolver - as the same create/modify at the path the
// later move mv carried the node to, so the reconcile loop re-emits its download there.
// Everything about the node except its location is unchanged, so the
// content facts come from the deferred event (a move event is typically sparse) and are
// overridden only by whatever the move actually reports. The sync id is the move's: it is
// the later fact about the node, and ServerSyncID must never rewind (the monotonic stamp
// guard). FromPath is cleared so the rebuilt event is indistinguishable from a plain
// create at the destination for every downstream consumer.
func rescueDeferredMove(dev, mv actions.Event) actions.Event {
	res := dev
	res.Path = mv.Path
	res.FromPath = ""
	res.FromFileID = ""
	res.SyncID = mv.SyncID
	if mv.Hash != "" {
		res.Hash = mv.Hash
	}
	if mv.Size != 0 {
		res.Size = mv.Size
	}
	if mv.MTime != 0 {
		res.MTime = mv.MTime
	}
	return res
}

// supersededByLaterDelete reports whether a download targeting path (the node with
// file id fileID) is destroyed by some event later in the same ascending-sync_id batch,
// making the download both pointless and doomed: by the time it runs the server node
// is already gone, so it fails, and its conflicting rename/remove can never run behind
// a failed predecessor, wedging the queue. Skipping the download lets those ops carry
// the path to its correct state instead. Two event shapes destroy the node: a delete of
// it (or of an ancestor directory), and an OVERWRITING MOVE - a move claiming the node's
// exact path as its destination for a DIFFERENT node, which destroys the occupant
// server-side without ever emitting an explicit delete event (before
// that the machinery keyed exclusively on deletes, so the occupant's download failed
// every drain forever with the authoritative rename wedged behind it on the shared path).
//
// startAfter is the index after which to scan: i for a download produced by event i in
// this batch, or -1 for a download already sitting in the durable queue from an earlier
// batch (which logically precedes every event here). The node is followed through any
// moves between startAfter and its destruction: a download of "a" that is moved a->b and
// then deleted at b is still superseded (a bare comparison of the delete against the
// download's original path misses this). A move OF the node is recognised by the node's
// own file id (a move event carries it) or by its source path; an ancestor-directory move
// re-bases the tracked path by prefix. Only a move matching neither - a different node
// arriving from elsewhere - can be an overwriting one, and only landing EXACTLY on the
// tracked path (folded): a move landing on an ancestor directory's path is deliberately
// NOT read as destroying the subtree, because a server that merges directories on such a
// move leaves the descendants alive, and dropping a live descendant's download would
// silently leave it missing from the replica - that
// residual, if a server ever destroys a subtree this way, is covered by the apply-time
// node-gone probe (errDownloadNodeGone). Re-creates after the destruction are untouched -
// their own later event enqueues a fresh download from an index past it. A delete is
// matched by node identity when both sides carry a file id (deleteSupersedes), so a
// delete of a DIFFERENT node that merely lands on the download's path never drops it.
func supersededByLaterDelete(sorted []actions.Event, startAfter int, path, fileID string) bool {
	cur := path
	for j := startAfter + 1; j < len(sorted); j++ {
		next := sorted[j]
		if !eventContainable(next) {
			// reconcile discards a non-containable event entirely (it never produces an
			// op), so it must not drive the supersession decision either: an invalid move
			// must not re-base the tracked path, and an invalid delete must not be read as
			// removing a node. Letting a discarded event influence this would either drop a
			// still-valid download or, worse, hide a genuine later delete behind a bogus
			// rebase and leave a doomed download wedging the queue.
			continue
		}
		switch next.Action {
		case "delete":
			if deleteSupersedes(next, cur, fileID) {
				return true
			}
		case "move":
			// An id match relocates the tracked node even when the paths say nothing:
			// a tracked path can go stale, the id cannot. Otherwise the ordinary rule
			// applies - cur is the moved node, or sits under it (relocateUnderMove).
			moved := false
			if fileID != "" && next.FileID == fileID {
				cur, moved = next.Path, true
			} else {
				cur, moved = relocateUnderMove(cur, next.FromPath, next.Path)
			}
			if moved {
				continue
			}
			if normpath.Key(next.Path) == normpath.Key(cur) {
				// An overwriting move: a DIFFERENT node (the same-node shapes are the ones
				// that relocated cur above - by id, or by source path for a sparse id-less
				// move of this node) lands exactly on the tracked path, destroying its occupant
				// server-side with no explicit delete event. The
				// download's node is gone just as surely as under a delete, so it is
				// superseded; the overwriting rename op still applies and carries the path
				// to the moved node's content.
				return true
			}
		}
	}
	return false
}

// deleteSupersedes reports whether a delete event removes the node a download targets -
// tracked at cur with file id fileID. Paths are view-relative, slash-separated server
// paths, compared folded (normpath.Key) so a case-only divergence still matches on a
// case-insensitive filesystem (identity over the NFC form on a case-sensitive one).
//
// An ancestor-directory delete matches by folded path alone, whatever the ids say: the
// delete carries the directory's own id, never a descendant's, and removing the
// directory removes everything under it. For the same-node candidate identity is
// authoritative when both sides carry an id: a delete of a DIFFERENT node that merely
// lands on the download's path - a later create+delete at a path the download's node
// has since moved away from, or a case-fold neighbour ("A" vs "a") whose folded key
// collides on a case-insensitive filesystem - must not drop the still-valid download,
// which would silently leave the node missing from the local replica until its next
// server-side modification. Conversely a matching id supersedes even
// when the paths disagree: a tracked path can go stale, the id cannot. An event or op
// without an id keeps the folded path-equality match, so sparse id-less deletes still
// supersede.
func deleteSupersedes(del actions.Event, cur, fileID string) bool {
	dk, pk := normpath.Key(del.Path), normpath.Key(cur)
	if keyUnder(pk, dk) {
		return true
	}
	if del.FileID != "" && fileID != "" {
		return del.FileID == fileID
	}
	return dk == pk
}

// preBatchDeletePath maps an id-less delete's path back through the batch's earlier moves
// to the path the deleted node was tracked at before the batch began, so the remove-base
// capture gate can find its pre-batch record. An id-carrying delete
// resolves through the snapshot's byID map and never needs this; a sparse one only has its
// path, which - after a same-batch [move a->b] - names the node's CURRENT slot while the
// pre-batch baseline lives under the source spelling, so the capture would miss and the
// apply-time guard would resurrect a cleanly deleted file as a spurious _Conflict copy.
//
// Walking sorted[0:i] in reverse (mirroring supersededByLaterDelete's forward walk), a move
// whose destination folds equal to the tracked path re-bases it to the move's source, and an
// ancestor-directory move re-bases it by prefix (rebaseMoved, reversed). A create claiming
// the tracked path ends the walk: the deleted node was (re)created in-batch there, so the
// current path IS the earliest one this node was known by - whatever sat at it pre-batch is
// a different node, matching the gate's historical exact-path read for a create+delete
// batch. A modify does not end the walk (content only, the node predates it), and a
// non-containable event is skipped exactly as the reconcile loop discards it. Self-echo
// moves participate: they are server-side facts like any other, and the pre-batch snapshot
// they help resolve into predates them too.
func preBatchDeletePath(sorted []actions.Event, i int, path string) string {
	cur := path
	for j := i - 1; j >= 0; j-- {
		prev := sorted[j]
		if !eventContainable(prev) {
			continue
		}
		ck := normpath.Key(cur)
		switch prev.Action {
		case "create":
			if normpath.Key(prev.Path) == ck {
				return cur
			}
		case "move":
			// Reversed: this walk reads history BACKWARDS, so a move is undone rather
			// than applied - the destination is where the path was, the source is where
			// it came from.
			cur, _ = relocateUnderMove(cur, prev.Path, prev.FromPath)
		}
	}
	return cur
}

// dropSupersededQueuedDownloads removes from the durable inbound queue any already-
// enqueued download whose node this batch ultimately destroys - a node created in an
// earlier batch whose download has not yet applied. A delete in the batch (or an
// overwriting move claiming the node's path for a different node)
// confirms the node is gone server-side, so the download is doomed: it can never
// complete, and because it shares the destroyed node's path the remove/rename would
// chain behind it and never run, wedging the queue (the leading drain would fail every
// cycle). A failed download leaves no file on disk, so dropping the op and letting the
// (idempotent) remove - or the overwriting rename - run converges the path to the
// server's state.
//
// The node is followed through this batch's moves (supersededByLaterDelete with
// startAfter -1), so a download queued at "a" that the batch moves a->b and then deletes
// at b is dropped even though the queue still records the pre-move path "a"; descendants
// of a deleted directory (and directories moved before deletion) are caught too. The
// batch's own ops are enqueued only later, inside the reconcile loop, so the scan here
// holds strictly the earlier batches' downloads.
func (e *Engine) dropSupersededQueuedDownloads(viewID uint64, batch []actions.Event) error {
	// Only a delete or a move supersedes a queued download (supersededByLaterDelete's
	// terminal conditions - a delete of the node, or an overwriting move claiming its
	// path for a different node); a batch of pure creates/modifies can never drop one.
	// Skip the whole-queue scan in that common case so a large retained backlog (e.g.
	// drains that keep failing while the pool reconnects) is not decoded on every poll
	// cycle that cannot supersede. The scan still runs - and must, since it has to find
	// every superseded download to keep one from wedging behind its own remove/rename -
	// whenever the batch carries either trigger.
	trigger := false
	for _, ev := range batch {
		if (ev.Action == "delete" || ev.Action == "move") && eventContainable(ev) {
			trigger = true
			break
		}
	}
	if !trigger {
		return nil
	}
	// Stream the queue rather than loading it whole: a download that keeps failing while
	// the pool reconnects can leave a large retained backlog, and decoding every record
	// into one slice here would reintroduce the unbounded-memory cost the bounded drain
	// avoids. The scan must still examine every op (any one could be a superseded
	// download), but it is paged: each pass collects at most dropPageSize of the view's
	// queued downloads as CANDIDATES, leaves the read transaction, then matches and acks
	// them. The supersession match needs the node's CURRENT tracked path (a rename in a
	// batch between the enqueue and this delete relocated the baseline without rewriting
	// the queued op's Path), and PathByID opens its own read transaction
	// that must not nest inside the scan's (the queuedPaths pattern) - nor may the queue be
	// mutated while ScanPending's cursor is live - so both the resolution and the Ack run
	// after the page's scan returns. Without the page cap an ancestor-directory delete that
	// supersedes the whole queue would retain O(queue-size) candidates in memory - exactly
	// the cost the bounded drain exists to avoid. Resuming by last-examined seq
	// (not last-matched) means each op is decoded at most once across all pages, so the
	// paging stays O(queue-size) in time.
	type candidate struct {
		seq    uint64
		path   string
		fileID string
	}
	// The queued-rename discriminator below asks the same question of
	// every superseded candidate, and answering it per candidate re-scans the whole durable
	// queue each time - O(candidates x queue) decodes, quadratic precisely in the shape this
	// paging exists for (an ancestor delete superseding a large retained backlog, on the
	// syncView worker lease). Collect the answer set ONCE instead, lazily: a pass that
	// supersedes nothing - the overwhelmingly common one - still pays no extra scan, and the
	// worst case pays exactly one. The set stays accurate for the whole pass because the loop
	// only acks DOWNLOADS; no rename is enqueued or removed here.
	var renameIDs map[string]struct{}
	renameCarries := func(fileID string) (bool, error) {
		if fileID == "" {
			return false, nil
		}
		if renameIDs == nil {
			ids, err := e.queuedInboundRenameIDs(viewID)
			if err != nil {
				return false, err
			}
			renameIDs = ids
		}
		_, ok := renameIDs[fileID]
		return ok, nil
	}
	for afterSeq := uint64(0); ; {
		var page []candidate
		var lastExamined uint64
		full := false
		err := e.store.ScanPendingFrom(afterSeq, func(qo store.QueuedOp) error {
			lastExamined = qo.Seq
			if qo.ViewID == viewID && qo.Type == store.OpDownload {
				page = append(page, candidate{seq: qo.Seq, path: qo.Path, fileID: qo.FileID})
			}
			if len(page) >= dropPageSize {
				full = true
				return errStopScan
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopScan) {
			return fmt.Errorf("engine: scan queue for superseded downloads: %w", err)
		}
		for _, c := range page {
			cur, err := e.queuedDownloadCurrentPath(viewID, c.fileID, c.path)
			if err != nil {
				return err
			}
			if !supersededByLaterDelete(batch, -1, cur, c.fileID) {
				continue
			}
			e.log.Warn("engine: dropping queued download superseded by a later delete",
				"view", viewID, "path", cur, "file_id", c.fileID)
			// The superseded download's node was never materialised on disk, but reconcile
			// pre-wrote its StatePending baseline (markUpsertPending / relocatePending), indexed
			// in byid. For a delete trigger the batch's own trailing OpRemove drops that record,
			// so the path self-heals; but an OVERWRITING-MOVE trigger
			// supersedes via a rename of a DIFFERENT node that never mentions the victim, and
			// when that mover is untracked at its source (relocatePending a no-op) AND its rename
			// dead-letters (the cleanup at inbound_rename.go touches only the mover), the victim's
			// phantom StatePending record is left behind forever: localReconcile only drives
			// StateSynced, so the path silently drops out of outbound sync until the next inbound
			// event (an overwrite-move stranding a superseded download's
			// baseline). dropStrandedBaseline
			// resolves the victim by byid and drops only a non-synced phantom whose node is not
			// materialised on disk. Two residual sub-cases are no-ops here and accepted
			// (verdict-agreed):
			//   (a) an id-less candidate - dropStrandedBaseline returns early on an empty file id,
			//       and an id-less download cannot be fetched anyway, so its baseline is left as-is;
			//   (b) a modify-flavoured node whose real on-disk file is still present - the
			//       onDiskIsNodeKind guard preserves the record (dropping it would orphan the live
			//       file); a full fix needs revert-style resolution, out of scope for this drop.
			// An untracked node and an already-synced record are also no-ops, so the delete
			// trigger's trailing OpRemove stays correct and no real file is orphaned.
			//
			// The cleanup runs BEFORE the Ack (mirroring the errDownloadNodeGone /
			// dropSupersededSameIDDownloads order; the reorder was chosen over folding both into
			// one Update transaction, since the baseline drop consults the on-disk node via Lstat,
			// which must not run inside a store transaction): the two mutations are separate
			// Update transactions, and cleanup-first makes the crash window recoverable - a crash
			// after the drop commits but before the Ack leaves the op queued with its cleanup
			// already applied, and the replayed batch (its per-page cursor never advanced) re-runs
			// both idempotently, while a drained retry converges through the ErrPathError →
			// downloadNodeGone backstop. The reverse order left the op acked with a stranded
			// phantom StatePending record nothing would ever clean - the stranded-record outcome,
			// narrowed to the crash window.
			// One more no-op, and the reason the drop consults the queue first: a still-queued
			// inbound rename for the SAME node is the materialisation this record is waiting
			// for, so the record is not stranded. dropStrandedBaseline
			// probes the node's byid path - which reconcile already relocated to the rename's
			// destination - while the real file is still at the un-renamed source, so the probe
			// reads "no node of this kind here" and drops a perfectly live record. The rename
			// then finds nodeTracked == false, takes the untracked-source overwrite path and
			// writes a BARE {FileID} baseline over the destination (the bare-record anomaly), so
			// the node loses every recorded fact for the trailing delete's never-lose gates to
			// read. Leave the record to its rename: when the rename applies it advances the
			// record itself, and when it dead-letters its own cleanup (resolveRejectedRename)
			// resolves it.
			carried, cerr := renameCarries(c.fileID)
			if cerr != nil {
				return cerr
			}
			if !carried {
				if err := e.dropStrandedBaseline(viewID, c.fileID); err != nil {
					return err
				}
			}
			if err := e.ackOp(c.seq); err != nil {
				return fmt.Errorf("engine: drop superseded download %d (seq %d): %w", viewID, c.seq, err)
			}
			// The op left the queue outside the executor's drain: drop any stuck-counter
			// entry it accumulated so it cannot linger as a false Stuck count.
			e.clearOpFailures(c.seq)
		}
		if !full {
			return nil
		}
		// Resume strictly past the op that filled the page (any matched ops in it were just
		// acked); every op up to and including lastExamined has been handled, so no record
		// is re-decoded.
		afterSeq = lastExamined
	}
}

// dropSupersededSameIDDownloads removes from the durable inbound queue every already-enqueued
// download of the SAME node (matched strictly by non-empty file id) when a NEWER create/modify
// event for that node is about to enqueue a fresh download. A download always
// fetches the node's CURRENT content by id, so once a newer event describes the node the older op
// is pure liability - and actively dangerous when a SELF-ECHO move relocated the node between the
// two events: the newer modify's markUpsertPending flips the relocated record StatePending, which
// extinguishes the older download's redirect gate (downloadRelocatedBySelfEcho requires
// StateSynced at cur), and the two ops' enqueue-time paths (the stale source, the current
// destination) share no serialize key, so before this drop they applied in PARALLEL with
// order-dependent divergent outcomes: the freshly-synced file deleted at its current path (the
// replica permanently holding the node under its OLD name), or a synced baseline recorded over an
// empty on-disk path whose next scan pushed an outbound OpRemove of the live server node, or
// spurious _Conflict copies.
//
// A download carried by a QUEUED inbound OpRename of the same id is deliberately left alone
// (queuedInboundRenameCarries, the same discriminator the redirect gate uses): in the
// non-self-echo [modify a, move a->b] flow the rename bridges both paths' conflict
// keys - download, rename and any later download already serialise in enqueue order - and the
// carried download landing at op.Path before the rename carries it is the designed, well-tested
// flow, so the supersession must not rewire it.
//
// The victim's pending baseline is resolved BEFORE its Ack (dropStrandedBaseline - the
// errDownloadNodeGone cleanup-before-ack order), so a crash between the two leaves the op queued
// with its cleanup already applied (the replay re-runs both idempotently) rather than acked with
// a stranded phantom record. In the common shape the call is a no-op: the record is byid-shared
// with the new download, which markUpsertPending immediately re-owns, and dropStrandedBaseline
// preserves any record whose node is materialised on disk.
func (e *Engine) dropSupersededSameIDDownloads(viewID uint64, fileID string) error {
	if fileID == "" {
		return nil
	}
	carried, err := e.queuedInboundRenameCarries(viewID, fileID)
	if err != nil {
		return err
	}
	if carried {
		return nil
	}
	// Page the queue like dropSupersededQueuedDownloads: candidates are collected per page
	// and acked outside the live ScanPendingFrom cursor (the queue must not be mutated while
	// the scan's read transaction is open), resuming strictly past the last examined seq so
	// each op is decoded at most once. Every match is older than the superseding download by
	// construction - it is not enqueued yet.
	type candidate struct {
		seq  uint64
		path string
	}
	for afterSeq := uint64(0); ; {
		var page []candidate
		var lastExamined uint64
		full := false
		err := e.store.ScanPendingFrom(afterSeq, func(qo store.QueuedOp) error {
			lastExamined = qo.Seq
			if qo.ViewID == viewID && qo.Type == store.OpDownload && qo.FileID == fileID {
				page = append(page, candidate{seq: qo.Seq, path: qo.Path})
			}
			if len(page) >= dropPageSize {
				full = true
				return errStopScan
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopScan) {
			return fmt.Errorf("engine: scan queue for same-id downloads: %w", err)
		}
		for _, c := range page {
			e.log.Warn("engine: dropping queued download superseded by a newer event for the same node",
				"view", viewID, "path", c.path, "file_id", fileID)
			if err := e.dropStrandedBaseline(viewID, fileID); err != nil {
				return err
			}
			if err := e.ackOp(c.seq); err != nil {
				return fmt.Errorf("engine: drop superseded same-id download %d (seq %d): %w", viewID, c.seq, err)
			}
			// The op left the queue outside the executor's drain: drop any stuck-counter
			// entry it accumulated so it cannot linger as a false Stuck count.
			e.clearOpFailures(c.seq)
		}
		if !full {
			return nil
		}
		afterSeq = lastExamined
	}
}

// queuedDownloadCurrentPath resolves the path a queued download's node is CURRENTLY
// tracked at. The op's Path is the path at enqueue time; a rename reconciled in a batch
// between the enqueue and now relocates the baseline (relocatePending) without rewriting
// the queued op, so matching a later delete against the stale path would drop a
// still-valid download of a node that has since moved away - the delete's (unrelated)
// node dies, ours lives on server-side, and the local replica silently never materialises
// it. The byid index follows the node across moves (mirroring
// queuedPaths' resolve for a queued OpResolveConflict); a download with no file id, or
// one whose node the store no longer tracks, falls back to the op's recorded path.
func (e *Engine) queuedDownloadCurrentPath(viewID uint64, fileID, opPath string) (string, error) {
	if fileID == "" {
		return opPath, nil
	}
	cur, err := e.store.PathByID(viewID, fileID)
	if isNotFound(err) {
		return opPath, nil
	}
	if err != nil {
		return "", fmt.Errorf("engine: resolve queued download %d/%q: %w", viewID, fileID, err)
	}
	return cur, nil
}

// pendingOutboundUploads returns the set of view-relative paths in viewID that already
// have a queued DirOutbound OpUpload - a local content change the scanner emitted but the
// executor has not yet pushed. reconcile uses it to defer a colliding inbound download to
// the keep-both resolver (see reconcile). The queue is only scanned when the batch carries
// a file create/modify event (the only inbound op that can collide with an upload): a
// delete/move-only batch returns an empty set without touching the queue, mirroring
// dropSupersededQueuedDownloads' trigger guard so a large retained backlog is not decoded
// on every poll that cannot conflict.
func (e *Engine) pendingOutboundUploads(viewID uint64, batch []actions.Event) (map[string]bool, error) {
	hasUpsert := false
	for _, ev := range batch {
		if (ev.Action == "create" || ev.Action == "modify") && !ev.IsDir && eventContainable(ev) {
			hasUpsert = true
			break
		}
	}
	if !hasUpsert {
		return nil, nil
	}
	paths := make(map[string]bool)
	err := e.scanQueuedInView(viewID, "pending uploads", func(qo store.QueuedOp) error {
		if qo.Direction == store.DirOutbound && qo.Type == store.OpUpload {
			// Key by the folded form so an inbound download for a case-variant path (Foo vs
			// foo) funnels against the queued upload on a case-insensitive filesystem.
			paths[normpath.Key(qo.Path)] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

// inheritedBase is the pre-batch synced base an already-queued inbound op preserved for a
// node whose LIVE baseline a prior batch already flipped StatePending. Presence in the
// inheritedBases map means the node WAS StateSynced before this batch: the download-gate
// inherits hash to keep a cross-batch second download from clobbering a cheap-scan-missed
// local edit, and the remove-gate inherits all of {synced, hash, isDir} so a cross-batch
// delete resolves the clean file against its real base and deletes it cleanly instead of
// resurrecting it as a spurious _Conflict copy. hash is the node's
// last-synced local content hash (empty for a synced directory or a synced empty file);
// isDir its node kind (so the remove-gate can preserve a file that locally replaced a
// tracked directory rather than reading the dir's structurally-empty hash as server
// authority - the BaseIsDir contract).
type inheritedBase struct {
	hash  string
	isDir bool
}

// dropConsumedEvents removes from a sorted batch every event positioned STRICTLY BELOW the
// durable cursor. Such an event was already folded into the store by an earlier pass: the
// cursor records the highest sync id consumed, so anything under it is by definition consumed.
// Re-delivery is a server anomaly (or a page replayed after a crash between the enqueue and the
// per-page cursor advance), and reconcile is idempotent for the common shapes but not
// universally so - a stale delete replayed after a recreate, or a stale move replayed after the
// node moved on, rewrites baselines against a world that has moved past it.
//
// Two exemptions keep the filter from dropping anything legitimate. An event AT the cursor is
// kept: whether the server's pull_event window is inclusive or exclusive of the requested sync
// id is not a confirmed contract, and a page whose events sit exactly at the cursor is the
// stalled-page shape the syncView spin guard already handles by reconciling it once and
// latching. And an UNPOSITIONED event (SyncID 0) carries no position in the log at all, so it
// cannot be judged consumed and is folded as before rather than silently dropped whenever the
// view has a cursor.
//
// It runs on the whole batch before ANY pass reads it, not per-event inside the reconcile loop:
// the batch-wide passes (queuedInboundBases, dropSupersededQueuedDownloads,
// pendingOutboundUploads, snapshotBatchBaselines) each consume the entire slice, and the
// supersession drop among them mutates the durable queue - see the call site for the divergence
// a stale delete reaching it produces. Returning the input slice unchanged when nothing is
// filtered keeps the common path allocation-free.
//
// The drops are reported in ONE summary Warn rather than per event (mirroring the cold start's
// non-create reporting): the shapes that trigger this filter are a
// history-replaying server and a page replayed after a crash between the enqueue and the per-page
// cursor advance, both server-sized - a per-event Warn would flood the size-capped rotating log in
// exactly the scenario the operator needs it to diagnose, evicting the surrounding history. Each
// dropped event is still recorded individually at Debug.
func (e *Engine) dropConsumedEvents(viewID uint64, sorted []actions.Event, cursor int64) []actions.Event {
	if cursor <= 0 {
		return sorted
	}
	consumed := func(ev actions.Event) bool { return ev.SyncID > 0 && ev.SyncID < cursor }
	keep := true
	for _, ev := range sorted {
		if consumed(ev) {
			keep = false
			break
		}
	}
	if keep {
		return sorted
	}
	kept := make([]actions.Event, 0, len(sorted))
	var (
		dropped int
		first   actions.Event
		lowest  int64
		highest int64
	)
	for _, ev := range sorted {
		if consumed(ev) {
			if dropped == 0 {
				first, lowest, highest = ev, ev.SyncID, ev.SyncID
			}
			dropped++
			if ev.SyncID < lowest {
				lowest = ev.SyncID
			}
			if ev.SyncID > highest {
				highest = ev.SyncID
			}
			e.log.Debug("engine: skipping event positioned below the durable cursor (already consumed)",
				"view", viewID, "action", ev.Action, "sync_id", ev.SyncID, "cursor", cursor, "path", ev.Path)
			continue
		}
		kept = append(kept, ev)
	}
	e.log.Warn("engine: skipping events positioned below the durable cursor (already consumed)",
		"view", viewID, "count", dropped, "cursor", cursor,
		"lowest_sync_id", lowest, "highest_sync_id", highest,
		"first_action", first.Action, "first_path", first.Path)
	return kept
}

// queuedInboundBases returns, keyed by BOTH downloadBaseKey forms (the file-id key AND the
// folded key of the node's tracked path - see recordInheritedBase), the pre-batch synced base
// every already-queued inbound op for viewID preserved - the divergence base of a node whose
// baseline a prior batch flipped to StatePending (wiping the synced state) before this batch
// arrived. Two op kinds carry one: an OpDownload's BaseLocalHash (the node's
// first download captured it before markPending wiped the synced record) and an OpRename's
// MovedBaseSynced/MovedBaseLocalHash (relocatePending flipped the moved node StatePending, so a
// cross-batch [move a->b] + [modify b] or [move a->b] + [delete b] would otherwise find no base
// anywhere - for the download-gate and the remove-gate alike).
// reconcile seeds its per-batch capture map with these so every later inbound op for the same
// node inherits the original base and funnels/resolves correctly instead of clobbering or
// resurrecting content. The map is non-nil so the reconcile loop can write fresh same-batch
// download captures into it.
//
// The queue is scanned when the batch carries a file create/modify (produces a download needing
// a base), a dir create/modify (produces a mkdir whose occupant gate needs one)
// OR a delete (produces a remove needing one), mirroring pendingOutboundUploads so a
// move-only batch - which needs none - is not decoded. A move-only batch CAN drop a queued
// download whose node an overwriting move destroys (dropSupersededQueuedDownloads now triggers
// on moves too) without its base being seeded here first: that base
// described the DESTROYED occupant node, which no later remove-gate ever resolves - the
// overwriting rename's own destination capture reads the pre-batch snapshot, not this map -
// so nothing downstream needs it.
// Unlike the download-gate's non-empty-hash requirement, a rename's base is recorded whenever
// MovedBaseSynced is set even with an empty hash (a synced directory or empty file), so the
// remove-gate can inherit its kind; presence in the map alone signals "was synced".
//
// It also returns the set of non-empty file ids that already have a QUEUED inbound OpDownload
// (regardless of whether that download carries a base). reconcile uses it to gate
// dropSupersededSameIDDownloads: only a download whose id already has a queued download - a
// prior-batch one recorded here, or one enqueued earlier in the same batch - can supersede
// anything, so a fresh-id download (the overwhelmingly common case, including every file of a
// cold catch-up backlog) skips the whole-queue supersession scan entirely. Without the gate
// reconcile ran two full queue scans per download op, making a large backlog paged into one
// syncView lease O(downloads x queue) - quadratic - where this one shared scan keeps it
// O(queue) per batch.
func (e *Engine) queuedInboundBases(viewID uint64, batch []actions.Event) (map[string]inheritedBase, map[string]bool, error) {
	relevant := false
	for _, ev := range batch {
		if !eventContainable(ev) {
			continue
		}
		if ev.Action == "delete" || ev.Action == "create" || ev.Action == "modify" {
			relevant = true
			break
		}
	}
	if !relevant {
		return nil, nil, nil
	}
	bases := make(map[string]inheritedBase)
	downloadIDs := make(map[string]bool)
	// Queued inbound renames that carry no base of their own, recorded for the post-pass below:
	// the destination path they will move their node to, keyed by the node's id.
	type renameDest struct{ fileID, newPath string }
	var renameDests []renameDest
	err := e.scanQueuedInView(viewID, "inbound bases", func(qo store.QueuedOp) error {
		if qo.Direction != store.DirInbound {
			return nil
		}
		if qo.Type == store.OpDownload && qo.FileID != "" {
			// Every queued inbound download's node id (not only those carrying a base):
			// the gate above only needs to know a same-id download exists, so a fresh-id
			// download can skip the supersession scan.
			downloadIDs[qo.FileID] = true
		}
		// First capture wins in effect: every queued op that preserves a base for one node
		// records the same original synced facts (each captured it from the node's pre-batch
		// synced record), so a later duplicate is a harmless no-op overwrite of the same value.
		// Each base is registered under BOTH keys a gate may look it up by (see
		// recordInheritedBase): the file-id key so the base follows the node across a move,
		// and the folded key of the path the node ends up tracked at (a download's path, a
		// rename's destination) so a SPARSE ID-LESS delete - whose lookup key degrades to the
		// path form - still inherits it. The rename's kind comes from MovedBaseIsDir (the
		// pre-batch BASELINE's kind), never the event-reported qo.IsDir a sparse move leaves
		// false.
		switch {
		case qo.Type == store.OpDownload && qo.BaseLocalHash != "":
			recordInheritedBase(bases, qo.FileID, qo.Path, inheritedBase{hash: qo.BaseLocalHash})
		case qo.Type == store.OpRename && qo.MovedBaseSynced:
			recordInheritedBase(bases, qo.FileID, qo.NewPath, inheritedBase{hash: qo.MovedBaseLocalHash, isDir: qo.MovedBaseIsDir})
		case qo.Type == store.OpRename && qo.FileID != "":
			renameDests = append(renameDests, renameDest{fileID: qo.FileID, newPath: qo.NewPath})
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	// A queued rename with no base of its own still MOVES its node, so it carries the path
	// alias a sparse id-less event needs. The producing shape is the three-batch
	// [modify f@a] + [move a->b] + [delete b (id-less)]: batch 1's
	// download captured f's pre-batch synced base (registered under the id key AND path a),
	// but by batch 2 the live record was already StatePending, so the rename's own
	// MovedBaseSynced capture found nothing and registered no key at all. The batch-3 delete
	// is sparse, so its lookup key degrades to the folded path b - which nothing registered -
	// and BaseWasSynced stayed false, resurrecting the perfectly clean deleted file as a
	// spurious _Conflict copy the next scan re-uploaded (the exact outcome already
	// closed for the two-batch shape). Alias the rename's destination onto the base already
	// held for the same NODE: both describe the same pre-batch synced record, so the alias
	// adds no new fact, only the path key the move made reachable. A rename that registered
	// its own base is untouched (its own capture is at least as good), and a node with no
	// base anywhere stays absent, so a genuinely fresh node still captures nothing.
	for _, rd := range renameDests {
		ib, ok := bases[downloadBaseKey(rd.fileID, "")]
		if !ok {
			continue
		}
		if _, exists := bases[normpath.Key(rd.newPath)]; !exists {
			bases[normpath.Key(rd.newPath)] = ib
		}
	}
	return bases, downloadIDs, nil
}

// queuedInboundRenameCarries reports whether a QUEUED inbound OpRename for the given file id is
// pending in the view - i.e. whether some rename will carry a same-id download from its
// enqueue-time op.Path to the node's current location. It is the discriminator
// downloadRelocatedBySelfEcho uses to tell a self-echo move (which enqueues NO op, so nothing
// carries the download and it must be redirected) from a non-self-echo [modify a,
// move a->b] batch (whose queued inbound OpRename must carry the download from
// op.Path). A scan failure propagates so the caller leaves the op queued to retry rather than
// deciding on incomplete information; an empty file id trivially matches nothing.
func (e *Engine) queuedInboundRenameCarries(viewID uint64, fileID string) (bool, error) {
	if fileID == "" {
		return false, nil
	}
	var found bool
	err := e.scanQueuedInView(viewID, "inbound renames", func(qo store.QueuedOp) error {
		if qo.Direction == store.DirInbound && qo.Type == store.OpRename && qo.FileID == fileID {
			found = true
			// One match settles the question; stop decoding the rest of the queue rather
			// than draining it for an answer that can no longer change.
			return errStopScan
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// queuedInboundRenameIDs is the SET form of queuedInboundRenameCarries: the file ids of every
// queued inbound OpRename in the view, collected in one queue scan. It exists for the caller
// that asks the same question about MANY file ids in a row - dropSupersededQueuedDownloads,
// whose per-candidate loop would otherwise re-scan the whole durable queue once per superseded
// download, making a pass that documents itself as O(queue-size) quadratic exactly when it
// matters most (an ancestor-directory delete over a large retained backlog, where the candidate
// count approaches the queue size, stalling the poll loop on its worker lease).
//
// The caller may ack DOWNLOADS while consulting the set; that never enqueues or removes a
// rename, so a set collected once up front stays accurate for the whole pass.
func (e *Engine) queuedInboundRenameIDs(viewID uint64) (map[string]struct{}, error) {
	ids := make(map[string]struct{})
	err := e.scanQueuedInView(viewID, "inbound renames", func(qo store.QueuedOp) error {
		if qo.Direction == store.DirInbound && qo.Type == store.OpRename && qo.FileID != "" {
			ids[qo.FileID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// batchBaselines is the pre-batch snapshot of every baseline the batch's capture gates
// consult, taken BEFORE the first markPending mutates the store. The
// gates (download-base, mkdir-occupant, rename-dest, remove-base) need the LAST-SYNCED facts for the
// nodes the batch touches, but an earlier event in the same batch may already have
// rewritten those baselines through markPending - e.g. in [move a->b, modify b] the move
// relocates the baseline to b and flips it StatePending, so a live download-base read at
// the modify finds no synced hash, captures no base, and applyDownload clobbers a
// cheap-scan-missed local edit the rename just carried to b. Reading the pre-batch
// snapshot removes the class: what a gate needs is the state before this batch, never the
// half-folded intermediate. Only the gates read the snapshot - markPending and its helpers
// keep operating on the live store, since they ARE the sequential event fold.
//
// byID holds the pre-batch baseline for every file id the batch touches (move-invariant:
// the node is found wherever it was tracked); byPath holds the same records keyed by the
// folded form (normpath.Key) of the path they were tracked at, matching the engine's
// case-fold matching invariant, plus the path-only lookups. Absence from both maps means
// the node/path was untracked pre-batch.
type batchBaselines struct {
	byID   map[string]store.Baseline
	byPath map[string]store.Baseline
}

// node resolves the pre-batch baseline for a node, by file id first (mirroring
// markUpsertPending's id-first resolution, so a case-variant or same-batch-moved path
// still finds the tracked record) and falling back to the path a node with no id - or an
// id the store never tracked - was addressed by.
func (s batchBaselines) node(fileID, path string) (store.Baseline, bool) {
	if fileID != "" {
		if b, ok := s.byID[fileID]; ok {
			return b, true
		}
	}
	return s.atPath(path)
}

// atPath resolves the pre-batch baseline tracked at a path (folded, per the engine's
// case-fold matching invariant), regardless of which node holds it.
func (s batchBaselines) atPath(path string) (store.Baseline, bool) {
	b, ok := s.byPath[normpath.Key(path)]
	return b, ok
}

// snapshotBatchBaselines walks the batch once, before any event is applied, and captures
// the pre-batch baseline for everything a capture gate will consult: the node a file
// create/modify downloads onto (download-base), the path a dir create/modify materialises
// at (the OpMkdir occupant gate), a move's destination path
// (rename-dest) and the node a delete removes (remove-base). Self-echo events enqueue no ops and run no
// gates, and non-containable events are skipped by the reconcile loop before their gates,
// so neither is captured - a rescuable move has already been rewritten into its containable
// create shape by then (rescueUncontainableSources), so it IS captured, at the destination its
// gates will read. First capture wins per key; every capture happens before the
// first markPending, so a duplicate would read identical state anyway.
func (e *Engine) snapshotBatchBaselines(viewID uint64, sorted []actions.Event) (batchBaselines, error) {
	snap := batchBaselines{byID: make(map[string]store.Baseline), byPath: make(map[string]store.Baseline)}
	record := func(path string, b store.Baseline) {
		if b.FileID != "" {
			if _, ok := snap.byID[b.FileID]; !ok {
				snap.byID[b.FileID] = b
			}
		}
		k := normpath.Key(path)
		if _, ok := snap.byPath[k]; !ok {
			snap.byPath[k] = b
		}
	}
	capturePath := func(path string) error {
		if _, ok := snap.byPath[normpath.Key(path)]; ok {
			return nil // this path (or a fold-equal one) was already captured pre-batch
		}
		b, err := e.store.GetBaseline(viewID, path)
		switch {
		case isNotFound(err):
			return nil
		case errors.Is(err, store.ErrCorruptRecord):
			// The record is present but undecodable. Treat it as absent (fail-safe: an
			// unreadable synced base becomes no base, which funnels the apply-time gate
			// toward preserve rather than clobber) instead of aborting the whole batch -
			// the loop's markPending/apply then heals the key with a tolerant write.
			e.warnCorruptBaseline("snapshot", viewID, path, err)
			return nil
		case err != nil:
			return fmt.Errorf("engine: snapshot baseline %d/%q: %w", viewID, path, err)
		}
		record(path, b)
		return nil
	}
	captureNode := func(fileID, path string) error {
		if fileID != "" {
			if _, ok := snap.byID[fileID]; ok {
				return nil
			}
			cur, err := e.store.PathByID(viewID, fileID)
			switch {
			case isNotFound(err):
				// Untracked by id pre-batch: fall through to the path lookup below,
				// mirroring the gates' historical exact-path fallback.
			case err != nil:
				return fmt.Errorf("engine: snapshot node %d/%q: %w", viewID, fileID, err)
			default:
				b, gerr := e.store.GetBaseline(viewID, cur)
				switch {
				case gerr == nil:
					record(cur, b)
					return nil
				case errors.Is(gerr, store.ErrCorruptRecord):
					// The node's own record exists at cur but no longer decodes: treat the
					// node as untracked (fail-safe) rather than reading ev.Path, which holds a
					// DIFFERENT node's slot. The loop's markPending/apply heals cur's key.
					e.warnCorruptBaseline("snapshot", viewID, cur, gerr)
					return nil
				case !isNotFound(gerr):
					return fmt.Errorf("engine: snapshot baseline %d/%q: %w", viewID, cur, gerr)
				}
				// byid points here but no baseline is at cur (index drift): fall through to
				// the event-path lookup below, mirroring the historical NotFound behaviour.
			}
		}
		return capturePath(path)
	}
	for i, ev := range sorted {
		if !eventContainable(ev) || e.isSelfEcho(ev) {
			continue
		}
		switch ev.Action {
		case "create", "modify":
			// A FILE create/modify feeds the download-base gate; a DIR create/modify feeds
			// the OpMkdir occupant gate - a server dir-create can land
			// on a path holding a tracked regular file (a file<->dir create/create
			// collision), and the gate needs that node's pre-batch synced facts before
			// markUpsertPending overwrites them with the pending dir record. Both resolve
			// id-first: a dir event's fresh id misses byID and falls back to the path the
			// occupant is tracked at, while a re-reported tracked dir resolves to its own
			// (dir) record - which the gate conservatively reads as preserve-whole.
			if err := captureNode(ev.FileID, ev.Path); err != nil {
				return batchBaselines{}, err
			}
		case "move":
			// The rename-dest gate reads the destination path's pre-move baseline; the
			// moved-base capture reads the moved node's own pre-batch record (id-first, source
			// path fallback) so the queued rename can carry its last-synced base forward for a
			// later batch's download to inherit.
			if err := capturePath(ev.Path); err != nil {
				return batchBaselines{}, err
			}
			if err := captureNode(ev.FileID, ev.FromPath); err != nil {
				return batchBaselines{}, err
			}
		case "delete":
			if ev.FileID == "" {
				// A sparse id-less delete's remove-base gate resolves through the batch's
				// earlier moves to the node's pre-batch path (preBatchDeletePath), so that
				// path must be in the snapshot too. A non-self-echo move already captured
				// its own source, but a SELF-ECHO move is skipped by this loop entirely, so
				// without this capture the resolved path would miss.
				if err := capturePath(preBatchDeletePath(sorted, i, ev.Path)); err != nil {
					return batchBaselines{}, err
				}
			}
			if err := captureNode(ev.FileID, ev.Path); err != nil {
				return batchBaselines{}, err
			}
		}
	}
	return snap, nil
}

// downloadBaseKey is the map key under which an inbound download's last-synced base hash is
// recorded for cross-download inheritance (reconcile's per-batch capture map and the
// queuedInboundBases seed). It keys by the node's file id when present - move-invariant, so the
// base follows the node across a same-file move (or a directory move's descendants) within a
// batch - and falls back to the folded path only for the rare node with no file id (which cannot
// actually produce a working download, since the transport fetches by id). The "\x00id\x00" prefix
// namespaces id keys away from path keys, which can never contain a NUL (containable rejects it).
func downloadBaseKey(fileID, path string) string {
	if fileID != "" {
		return "\x00id\x00" + fileID
	}
	return normpath.Key(path)
}

// recordInheritedBase registers a queued inbound op's preserved base in the
// queuedInboundBases seed under BOTH keys a capture gate may look it up by: the
// move-invariant file-id key (an id-carrying event's downloadBaseKey) and the folded key of
// the path the node is currently tracked at (a download's Path, a rename's NewPath). A
// SPARSE ID-LESS delete's lookup key degrades to the path form - downloadBaseKey with an
// empty id - so an id-keyed-only registration missed it entirely: the cross-batch
// [move a->b (id)] + [delete b (id-less)] interleaving found no base, BaseWasSynced stayed
// false, and the apply-time guard resurrected a perfectly clean deleted file as a spurious
// _Conflict copy the next scan re-uploaded (the same shape the
// deleteSupersedes path-equality fallback already handles for the supersession drop). The
// dual write keeps the scan single-pass: one map insert per key, no per-lookup queue walk.
// An id-less op's downloadBaseKey already IS the path key, so it registers once.
func recordInheritedBase(bases map[string]inheritedBase, fileID, path string, ib inheritedBase) {
	bases[downloadBaseKey(fileID, path)] = ib
	if fileID != "" {
		bases[normpath.Key(path)] = ib
	}
}

// dropPageSize bounds how many superseded-download seqs dropSupersededQueuedDownloads
// collects before acking them and resuming, so a delete that supersedes a large backlog
// drops it in bounded pages instead of retaining one seq per queued op.
const dropPageSize = 256

// errStopScan is a sentinel a ScanPending/ScanPendingFrom callback returns to end the scan
// early (e.g. a page is full) without it being treated as a real error.
var errStopScan = errors.New("engine: stop scan")

// containable reports whether a view-relative server path can be safely materialised
// under the sync root: it must be lexically local (not absolute, no ".." component,
// non-empty) and not collapse to the view root itself. It mirrors the executor's abs()
// path check, applied here - before any baseline mutation - so a structurally-invalid
// path never relocates or rewrites a baseline only to be dead-lettered at apply time.
func containable(p string) bool {
	if strings.IndexByte(p, 0) >= 0 {
		// A NUL byte is invalid in an OS path on every platform, yet filepath.IsLocal
		// accepts it on Unix. os.Lstat then returns EINVAL (not IsNotExist), so an op built
		// from such a path would neither apply nor dead-letter - it would fail every drain
		// and wedge the queue. Reject it here, before any baseline mutation, so the event is
		// skipped (the cursor still advances) rather than enqueued.
		return false
	}
	rel := filepath.FromSlash(p)
	if filepath.ToSlash(rel) != p {
		// On Windows filepath treats '\' as a separator, so a server path carrying a literal
		// backslash (e.g. "a\b") aliases the slash form "a/b": both resolve to the same on-disk
		// file via abs()'s filepath.Join, yet the store keys baselines under the distinct raw
		// paths "a\b" and "a/b", drifting two records onto one file. The wire is slash-separated,
		// so require the OS form to round-trip back to the original - rejecting any path whose
		// separators alias. This is a no-op on Unix, where '\' is an ordinary filename byte and
		// FromSlash/ToSlash are identities, so a legitimately backslash-named file is still
		// accepted there.
		return false
	}
	if !hostRepresentablePath(rel) {
		// A component this host cannot spell verbatim - on Windows, one ending in a dot or a
		// space, which Win32 silently STRIPS rather than rejects (namemangle.go). Every
		// path-addressed gate downstream would read "the call succeeded" as "the path I
		// addressed is the path I landed": the promote writes bytes under a different on-disk
		// name than op.Path, markSynced records a StateSynced baseline for a path that does not
		// exist, and - when the server also holds the stripped twin - the apply contaminates,
		// and the following scan destroys, a healthy unrelated node. This is the same class the
		// separator-aliasing rejection above closes ("two store keys, one on-disk file"), so it
		// is closed the same way and in the same place: before any baseline mutation. The record
		// stays on the server untouched; the operator renames it there or on a peer that can
		// represent it.
		return false
	}
	clean := filepath.Clean(rel)
	// Require the path to already be canonical. The store keys baselines by the raw server
	// path, while the executor's abs() materialises them through filepath.Join, which cleans.
	// A non-canonical path like "d/." (or a trailing-slash / redundant "." segment) would key
	// the baseline at "d/." while disk sits at "d", so a later event naming "d" - including a
	// canonically-equivalent no-op move's own descendant-preserving guard - could not find the
	// root baseline. Reject it here (the event is skipped, the cursor still
	// advances) so store keys stay aligned with disk; the codebase already assumes clean,
	// no-trailing-slash paths (relocateDescendantBaselines keys its prefix as oldDir+"/").
	return filepath.IsLocal(rel) && clean != "." && clean == rel
}

// idAddressingPrefix is the wire's id-addressing marker: the "path" parameter of
// download/resume_download (id:<file_id>) and upload (id:<parent_id>/<name>) shares its
// single slot with view-relative addressing (rename/remove/query_node/list). A
// view-relative path whose FIRST component literally begins with this marker would,
// verbatim, alias id-addressing, so the engine must never originate or apply such a path
// (DESIGN §13).
const idAddressingPrefix = "id:"

// isRootIDAlias reports whether a view-relative path is a ROOT-level name that aliases the
// wire's id-addressing form: it has no directory component (a nested "a/id:foo" carries
// the prefix mid-parameter, not at the start of the whole path, so it is not exposed) and
// its name begins with "id:" case-insensitively. Windows cannot create a ':' in a
// filename, so this is a no-op there; on Linux/macOS a legitimate-looking local name
// "id:12345" at a view root would, if used in Remove(viewID, "id:12345")/Rename or fed to
// a read probe, address the FOREIGN node with that file id should the DSM dispatch the
// parameter by prefix. Until the server's parse semantics are confirmed with a live DSM
// the engine treats such a path as unsyncable - protected on the scanner side (scanner.go)
// and skipped on the inbound side (the reconcile loop) - so it can never round-trip into an
// id-addressed mutation of an unrelated node.
func isRootIDAlias(p string) bool {
	if strings.ContainsRune(p, '/') {
		return false
	}
	return len(p) >= len(idAddressingPrefix) &&
		strings.EqualFold(p[:len(idAddressingPrefix)], idAddressingPrefix)
}

// eventAliasesIDRoot reports whether any path a server event would materialise is a
// root-level id-addressing alias (isRootIDAlias), returning the offending path. A move is
// checked on both endpoints; create/modify/delete on their single path; an unrecognised
// action touches nothing. The reconcile loop skips such an event (the cursor still
// advances) as the inbound mirror of the scanner-side protect - a symmetric-injection
// guard so a crafted server event can never materialise a local "id:*" node a later
// outbound mutation could alias onto a foreign node's id.
func eventAliasesIDRoot(ev actions.Event) (string, bool) {
	switch ev.Action {
	case "move":
		if isRootIDAlias(ev.FromPath) {
			return ev.FromPath, true
		}
		if isRootIDAlias(ev.Path) {
			return ev.Path, true
		}
	case "create", "modify", "delete":
		if isRootIDAlias(ev.Path) {
			return ev.Path, true
		}
	}
	return "", false
}

// eventContainable reports whether every path an event would touch is containable. A
// move is validated on both endpoints; create/modify/delete on their single path. An
// unrecognised action touches nothing and is treated as containable (it produces no op).
func eventContainable(ev actions.Event) bool {
	switch ev.Action {
	case "move":
		return containable(ev.FromPath) && containable(ev.Path)
	case "create", "modify", "delete":
		return containable(ev.Path)
	default:
		return true
	}
}

// markPending records that a remote change is owed at the event's path. For
// create/modify it writes a fresh pending baseline carrying the server facts. For a
// move it relocates the source baseline to the destination path (pending) so the moved
// node's recorded facts travel with it; for a delete it flips the existing baseline to
// pending. A path with no prior baseline (a brand-new delete, or a move whose source we
// never tracked) simply has nothing to carry.
// enqueueAndMarkPending commits an op and the baseline that records what it owes.
//
// For a CREATE or MODIFY the two writes go in ONE transaction, because ordering alone does
// not protect that arm. The pending baseline markUpsertPending writes is the only thing that
// arms the fresh-create never-lose gate: with no record and no byid entry at the target path,
// untrackedFilePresent reads the occupant as "not reconcile-produced" and applyDownload
// promotes over it with a REPLACING rename and no divergence base, so an untracked user file
// there is destroyed with no _Conflict copy. Nothing re-arms it after a crash either - the
// restart's cycle drains the queue BEFORE it re-pulls the batch (coldStartPending → localScan
// → drainQueue → pollOnce), and localScan skips any path that already has a conflicting queued
// op, so the replay argument above never gets a chance to run. Committing both writes together
// removes the window: the queue record exists if and only if its gate does.
//
// A MOVE and a DELETE stay on the plain ordered path, because their gates do not depend on
// this write. A delete's divergence base was captured into the op itself (BaseWasSynced /
// BaseLocalHash) before either write; flipPending only marks the record. A move's relocation
// is itself designed to be replayable descendants-first (relocatePending), and losing it
// leaves the node still tracked at the source, which is exactly what applyRename expects to
// find. Batching those two would additionally mean threading a prefix scan and a delete
// through the Writer seam for relocateDescendantBaselines, which is a wider change than the
// defect needs.
func (e *Engine) enqueueAndMarkPending(op store.Op, viewID uint64, ev actions.Event) error {
	if ev.Action == "create" || ev.Action == "modify" {
		return e.batchStore(func(b *store.Batch) error {
			if _, err := b.Enqueue(op); err != nil {
				return fmt.Errorf("engine: enqueue %s %d/%q: %w", op.Type, op.ViewID, op.Path, err)
			}
			return e.markPending(b, viewID, ev)
		})
	}
	if _, err := e.enqueueOp(op); err != nil {
		return fmt.Errorf("engine: enqueue %s %d/%q: %w", op.Type, op.ViewID, op.Path, err)
	}
	return e.markPending(e.store, viewID, ev)
}

// It takes a store.Writer rather than reaching for e.store, so the caller can run it in the
// SAME transaction as the enqueue that arms it (see the enqueue site in reconcile). Inside a
// Batch that is not a preference but a requirement: a direct e.store call from within one
// deadlocks on bbolt's writer mutex, with no timeout and no panic to diagnose it.
func (e *Engine) markPending(w store.Writer, viewID uint64, ev actions.Event) error {
	switch ev.Action {
	case "create", "modify":
		return e.markUpsertPending(w, viewID, ev)
	case "move":
		return e.relocatePending(w, viewID, ev)
	case "delete":
		return e.flipPending(w, viewID, ev.Path, ev.FileID)
	default:
		return nil
	}
}

// markUpsertPending records a remote create/modify by writing a pending baseline carrying
// the server facts. Normally that lands at the event's path. But when the node (by file id)
// is already tracked at a different path, the fresh baseline is written at that existing path
// instead of relocating it to ev.Path. This matters for the rejected-rename stopgap: a move
// a->link/b refused at a symlink leaves resolveRejectedRename re-tracking the node at its
// still-reachable source a, while the server reports it (and a later modify) at the
// unappliable destination link/b. Relocating the modify's baseline to link/b would re-point
// the byid index there; the modify's own download then dead-letters and dropStrandedBaseline
// drops the link/b record (abs(link/b) fails, so it never sees the real file at a), stranding
// the node untracked - and a later server delete can no longer resolve and remove it (the
// move/modify/delete-in-three-batches case). Keeping the node at its reachable path preserves
// the association so dropStrandedBaseline sees the on-disk file and the later delete honours
// it there. An empty file id, or a node already tracked at ev.Path, falls back to a plain
// PutBaseline at ev.Path. On the normal path the move precedes the modify (ascending sync_id),
// so the node is already tracked at ev.Path and nothing changes; should the download instead
// land at ev.Path successfully, its markSynced re-points the byid index there and drops this
// record, so the temporary placement self-heals.
func (e *Engine) markUpsertPending(w store.Writer, viewID uint64, ev actions.Event) error {
	dst := ev.Path
	if ev.FileID != "" {
		cur, err := w.PathByID(viewID, ev.FileID)
		switch {
		case isNotFound(err):
		case err != nil:
			return fmt.Errorf("engine: resolve tracked node %d/%q: %w", viewID, ev.FileID, err)
		case cur != ev.Path:
			dst = cur
		}
	}
	return w.PutBaseline(viewID, dst, baselineFromEvent(ev, store.StatePending))
}

// relocatePending carries the source baseline of a move event to the destination path,
// flipped to StatePending, so the moved node's recorded facts (hash, mtime, size, type,
// perms) travel with it instead of being stranded - and lost - when a later create in
// the same batch overwrites the source path (the executor would otherwise have only the
// move event's bare file id to rebuild the destination baseline from). The relocation
// is guarded by file id: it runs only while the source baseline still identifies the
// node being moved (its FileID matches the event's). On a replay the source path has
// since been taken by a different (recreated) node or already relocated, so the guard
// fails and the destination baseline written by the first apply is left intact. A
// source with no tracked baseline is a no-op.
//
// A directory move also re-homes the whole subtree: its descendant baselines must travel
// to the new prefix BEFORE a later same-batch event can overwrite a descendant's
// old-prefix baseline. The motivating case is move a->b followed by create a/x: deferring
// the descendant rebase to the executor lets that create overwrite the moved child's
// baseline at a/x first, and the executor's prefix scan then drags the new node onto the
// moved child and deletes it (data loss). Rebasing eagerly carries the child to b/x before
// a/x is recreated; the executor's mirror call then finds the new node at a/x with the
// moved child already at b/x and, via its destination-collision guard, leaves both in place.
func (e *Engine) relocatePending(w store.Writer, viewID uint64, ev actions.Event) error {
	b, err := w.GetBaseline(viewID, ev.FromPath)
	switch {
	case isNotFound(err):
		return nil
	case errors.Is(err, store.ErrCorruptRecord):
		// The source record is undecodable: skip the relocation (there is nothing usable
		// to carry) rather than aborting reconcile. The OpRename is still enqueued and
		// applyRename's markSynced heals the destination key; the source's corrupt record
		// is swept by the destination Put's byid drop.
		e.warnCorruptBaseline("relocate-pending", viewID, ev.FromPath, err)
		return nil
	case err != nil:
		return fmt.Errorf("engine: load baseline %d/%q: %w", viewID, ev.FromPath, err)
	}
	if ev.FileID == "" || b.FileID != ev.FileID {
		// The source no longer holds the node being moved; leave both paths untouched.
		//
		// The empty-id test is not redundant with the inequality. Two empty ids compare
		// EQUAL, so an id-less move event relocated whatever record happened to sit at the
		// source - typically a LOCAL-ONLY baseline, which is exactly the record that carries
		// no id. Nothing there establishes that the event and the record describe the same
		// node. The store already refuses that inference on its own side (putBaselineTx:
		// "an empty file_id is not a real identity ... the move-detection below would treat
		// unrelated records as moves of one another, deleting live baselines"), and the
		// destination capture in this same pass guards the identical degeneration; reconcile
		// was the one place still reading "" == "" as proof.
		//
		// The damage was worst for a DIRECTORY: the descendant rebase below carried children
		// holding real server ids under the new prefix while their files stayed where they
		// were, so the next scan diffed the moved records as vanished and pushed outbound
		// removes of live server nodes, plus duplicate creates at the old paths.
		return nil
	}
	// Rebase descendants BEFORE re-homing the root, so the root baseline is the LAST
	// record moved. Each store mutation is its own transaction, so this relocation is not
	// atomic; the order is what makes it replayable. The root Put at ev.Path drops the
	// source record through the byid index, so once it runs a replay can no longer find the
	// node at ev.FromPath and would bail at the GetBaseline above without ever rebasing the
	// descendants - leaving them stranded under the old prefix for a later same-batch create
	// to overwrite (the moved-child data loss this rebase exists to prevent). Doing the
	// descendants first means the only crash window leaves the root still at ev.FromPath: a
	// replay re-finds it, re-runs the (idempotent) descendant rebase, then re-homes the root.
	// The dangerous "root moved, descendants stranded" state is therefore unreachable. The
	// relocation is idempotent (a replay whose first apply already moved them finds an empty
	// old prefix), matching the self-echo move path (applySelfEchoMove).
	if b.FileType == "dir" {
		if err := e.relocateDescendantBaselines(viewID, ev.FromPath, ev.Path); err != nil {
			return err
		}
	}
	b.State = store.StatePending
	// PutBaseline at the destination drops the orphan source record through the byid
	// index (same file id, new path), so no explicit source delete is needed.
	if err := w.PutBaseline(viewID, ev.Path, b); err != nil {
		return fmt.Errorf("engine: relocate baseline %d/%q->%q: %w", viewID, ev.FromPath, ev.Path, err)
	}
	return nil
}

// moveSourceByFoldedKey resolves the source baseline of an id-less move whose FromPath differs
// from the stored path only by Unicode form / case - the case-variant fallback the id-less move
// backfill (in reconcile above) uses when its exact-key GetBaseline misses. Callers gate it on a
// case-insensitive matching platform (normpath.CaseInsensitive): a fold match on a case-sensitive
// filesystem would adopt a genuinely-distinct case-variant node's id and relocate the wrong file.
// It streams the view's baselines (mirroring trackedFoldIndex / parentBaselineByFoldedKey) and
// returns the UNIQUE baseline whose folded key matches fromPath. No match, an ambiguous match
// (more than one baseline folding to the same key, which a consistent store should never hold -
// the case-fold collision guard keeps such spellings protected/unsynced), or a transient scan error
// returns found=false, leaving the move id-less for the existing cold-start-gap / wrong-kind path.
// A record the stream skipped as corrupt is not adopted either (found stays false): the caller
// leaves such a source id-less, matching the exact-key ErrCorruptRecord arm.
func (e *Engine) moveSourceByFoldedKey(viewID uint64, fromPath string) (store.PathBaseline, bool, error) {
	want := normpath.Key(fromPath)
	var match store.PathBaseline
	found := false
	ambiguous := false
	_, err := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		if normpath.Key(pb.Path) != want {
			return nil
		}
		if found {
			ambiguous = true // refuse rather than guess which case-variant node the move meant
			return errStopBaselineScan
		}
		match = pb
		found = true
		return nil
	})
	if err != nil && !errors.Is(err, errStopBaselineScan) {
		return store.PathBaseline{}, false, err
	}
	if ambiguous || !found {
		return store.PathBaseline{}, false, nil
	}
	return match, true, nil
}

// baseIdentifiesNode reports whether a baseline resolved for an id-carrying event actually
// describes the node that event names. It is the shared "another node took the path" gate the
// path-addressed inbound remove pipeline needs: both the pre-batch capture
// (snap.node) and the live reads (flipPending, applyRemove) resolve their target by path once
// the file id misses the byid index, and a successor node at that path - our own offline edit
// re-pushed as a fresh server node after another client deleted the old one - is NOT the
// subject of the server's delete. With either id empty there is no identity to compare, so the
// historical path-only behaviour stands (it is the only thing a sparse event can do), mirroring
// applySelfEchoDelete / dropSelfEchoMoveSource / dropRemoveBaselineAt.
func baseIdentifiesNode(b store.Baseline, fileID string) bool {
	return fileID == "" || b.FileID == "" || b.FileID == fileID
}

// flipPending sets an existing baseline at path to StatePending, preserving the rest
// of the record. An absent baseline is a no-op: there is nothing tracked to mark. A
// corrupt (undecodable) record is also a no-op here - there is nothing usable to
// preserve - but the OpRemove this delete produced is still enqueued, and applyRemove's
// DeleteBaseline (tolerant of a corrupt value) heals the key rather than the read
// wedging reconcile forever.
//
// A record identifying a DIFFERENT node (baseIdentifiesNode) is left alone: the delete
// names fileID, and a successor node at the path belongs to its own producer (an outbound
// upload's synced record, or another event's queued download). Flipping it would erase the
// state that tells a trustworthy last-synced base from an unpushed one for that node's own
// gates, and mark a perfectly synced record as owing remote work it does not
// (mirroring applySelfEchoDelete's identity gate).
func (e *Engine) flipPending(w store.Writer, viewID uint64, path, fileID string) error {
	b, err := w.GetBaseline(viewID, path)
	switch {
	case isNotFound(err):
		return nil
	case errors.Is(err, store.ErrCorruptRecord):
		e.warnCorruptBaseline("flip-pending", viewID, path, err)
		return nil
	case err != nil:
		return fmt.Errorf("engine: load baseline %d/%q: %w", viewID, path, err)
	}
	if !baseIdentifiesNode(b, fileID) {
		return nil
	}
	b.State = store.StatePending
	return w.PutBaseline(viewID, path, b)
}

// applySelfEcho advances the baseline for an event this client itself committed,
// without enqueuing any disk work - the change is already on disk. A delete drops
// the baseline; a create/modify overlays the baseline at the event's path (advancing
// it to synced only when no queued apply still owns it - see overlaySelfEchoFacts); a
// move relocates and merges the existing baseline (see applySelfEchoMove). A move's synced
// Put at the new path drops the orphan at the old path through the store's byid index,
// so no explicit source delete is needed. An unrecognised action is skipped, mirroring
// opForEvent: the non-self-echo path produces no op for an unknown
// action, so this path must not plant a baseline for one either - a non-conformant or
// updated server emitting a new action type with our committer id on an in-view path with
// no local file would otherwise write a phantom StateSynced record (empty facts) that the
// next local scan diffs as vanished, pushing a spurious outbound OpRemove of a live server
// node.
func (e *Engine) applySelfEcho(viewID uint64, ev actions.Event) error {
	switch ev.Action {
	case "delete":
		return e.applySelfEchoDelete(viewID, ev)
	case "move":
		return e.applySelfEchoMove(viewID, ev)
	case "create", "modify":
		b, ok, err := e.selfEchoCreateBaseline(viewID, ev)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := e.store.PutBaseline(viewID, ev.Path, b); err != nil {
			return fmt.Errorf("engine: self-echo baseline %d/%q: %w", viewID, ev.Path, err)
		}
		return nil
	default:
		e.log.Debug("engine: skipping self-echo event with unrecognised action",
			"view", viewID, "action", ev.Action, "sync_id", ev.SyncID, "path", ev.Path)
		return nil
	}
}

// applySelfEchoDelete drops the baseline for a delete this client itself committed. The
// drop is identity-gated, mirroring dropSelfEchoMoveSource: when both the event and the
// tracked baseline carry a file id and they differ, the path has since been taken by a
// different node - our delete of the old node was followed by a create at the same path
// (already reconciled, possibly earlier in this same batch), and the late echo of the
// delete must not drop the recreated node's baseline. Dropping it would be self-healing
// (the next scan re-detects the file) but would churn a spurious full re-upload of an
// already-synced node. With either id empty there is no identity to compare, so the path
// snapshot keeps the unconditional drop.
func (e *Engine) applySelfEchoDelete(viewID uint64, ev actions.Event) error {
	if ev.FileID != "" {
		b, err := e.store.GetBaseline(viewID, ev.Path)
		switch {
		case isNotFound(err):
			// Nothing tracked at the path; the delete already converged.
			return nil
		case errors.Is(err, store.ErrCorruptRecord):
			// Undecodable: the file id cannot be compared, so fall through to the delete -
			// DeleteBaseline is tolerant of a corrupt value and heals the key.
			e.warnCorruptBaseline("self-echo-delete", viewID, ev.Path, err)
		case err != nil:
			return fmt.Errorf("engine: self-echo delete load baseline %d/%q: %w", viewID, ev.Path, err)
		case b.FileID != "" && b.FileID != ev.FileID:
			// A different node was recreated at the path; leave it tracked.
			return nil
		}
	}
	if err := e.store.DeleteBaseline(viewID, ev.Path); err != nil {
		return fmt.Errorf("engine: self-echo delete baseline %d/%q: %w", viewID, ev.Path, err)
	}
	return nil
}

// selfEchoCreateBaseline resolves the baseline to record at a self-echo create/modify's path.
// Like a move (selfEchoMoveBaseline), the event commonly omits hash, size and mtime when the
// committing client already holds them; rebuilding from the sparse event (baselineFromEvent)
// would zero LocalHash while marking the record synced, and the next local scan would re-upload
// the unchanged file. So the existing baseline is carried forward with only the reported facts
// overlaid; with nothing tracked at the path it falls back to the bare event.
//
// The overlay is identity-gated (as applySelfEchoDelete / dropSelfEchoMoveSource): when the event
// and the record both carry a file id and they DIFFER, another node has taken the path, and
// overlaying our echo would bind our id to the successor's path in byid and cost the successor's
// producer its record - so ok=false leaves the record untouched. With either id empty there is no
// identity to compare and the path-only behaviour stands, as everywhere else in this file.
//
// The mismatch is directional (finding H3): the record may equally name the DEAD node and the echo
// its successor - an offline create pushed into a directory another client deleted makes the
// server auto-create a fresh parent node. Skipping there leaves the record on a corpse, and the
// inbound delete of the old node then matches it (baseIdentifiesNode) and destroys the live
// directory with a clean status. selfEchoSupersedesRecord therefore decides by POSITION, the
// server's only total order: an echo STRICTLY ABOVE the record's ServerSyncID is adopted; one at
// or below it is stale and skipped.
//
// The residual is a record written by an outbound apply: markSynced records no server position, so
// a late echo of the dead node can be adopted until the successor's own (necessarily higher) echo
// adopts it back - the same batch in the measured interleavings. For a directory,
// removeSupersededByLaterObservation's DESCENDANT arm (a child stamped by its own echo inside the
// successor) is the second line; its path-level arm cannot help, as the adopted echo's position is
// never later than a delete of the same node. For a FILE there is no second line: two pushes to
// one path with a node replacement between them and a batch boundary between the echoes leave the
// record on the dead node, the id gate passes, and removeBaseFor's base matches the disk, so no
// _Conflict copy is made. This is a DECLARED RESIDUAL (DESIGN §13): both directions lose at
// ServerSyncID 0, so closing it needs an outbound apply that records the pushed node's position.
func (e *Engine) selfEchoCreateBaseline(viewID uint64, ev actions.Event) (store.Baseline, bool, error) {
	b, err := e.store.GetBaseline(viewID, ev.Path)
	switch {
	case isNotFound(err):
		if e.selfEchoNodeLivesElsewhere(viewID, ev) {
			return store.Baseline{}, false, nil
		}
		return baselineFromEvent(ev, store.StateSynced), true, nil
	case errors.Is(err, store.ErrCorruptRecord):
		// Undecodable prior: rebuild from the bare event (the last resort, as when nothing
		// is tracked) so the synced Put below overwrites and heals the key.
		e.warnCorruptBaseline("self-echo-create", viewID, ev.Path, err)
		if e.selfEchoNodeLivesElsewhere(viewID, ev) {
			return store.Baseline{}, false, nil
		}
		return baselineFromEvent(ev, store.StateSynced), true, nil
	case err != nil:
		return store.Baseline{}, false, fmt.Errorf("engine: self-echo load baseline %d/%q: %w", viewID, ev.Path, err)
	case !baseIdentifiesNode(b, ev.FileID) && !selfEchoSupersedesRecord(b, ev):
		e.log.Debug("engine: skipping self-echo create/modify, another node holds the path",
			"view", viewID, "path", ev.Path, "echo_id", ev.FileID, "tracked_id", b.FileID,
			"echo_sync_id", ev.SyncID, "tracked_sync_id", b.ServerSyncID)
		return store.Baseline{}, false, nil
	default:
		if !baseIdentifiesNode(b, ev.FileID) {
			e.log.Debug("engine: self-echo create/modify adopts the successor node at the path",
				"view", viewID, "path", ev.Path, "echo_id", ev.FileID, "tracked_id", b.FileID,
				"echo_sync_id", ev.SyncID, "tracked_sync_id", b.ServerSyncID)
		}
		overlaySelfEchoFacts(&b, ev)
		return b, true, nil
	}
}

// selfEchoNodeLivesElsewhere reports whether the echoed node is already tracked at a DIFFERENT
// path, which makes the arm that rebuilds a record from the bare event unsafe.
//
// Those two arms run when ev.Path carries no usable record, and they read that as "nothing is
// tracked here, so record what the echo says". The lookup is by PATH, so it cannot see that the
// node itself is tracked somewhere else - and it routinely is, because we moved it: push a
// modify of N at p, then rename p→q locally and push that too, and the modify's echo arrives
// after planRename has already relocated N's record to q. Writing a fresh record at p then
// plants a phantom where nothing exists on disk AND evicts the live record at q, because
// putBaselineTx keeps one path per file id and drops the orphan. The node ends up untracked at
// its real location: the keep-both funnel later sets our own file aside as a _Conflict copy and
// uploads a duplicate.
//
// Skipping is the conservative answer and matches what the rest of this function does when the
// path's identity cannot be established: the record at q stays intact, and the node's server
// facts are re-established by its own later event or by the next scan. markUpsertPending already
// resolves id-first for the same reason; this closes the same gap on the echo side.
func (e *Engine) selfEchoNodeLivesElsewhere(viewID uint64, ev actions.Event) bool {
	if ev.FileID == "" {
		return false
	}
	cur, err := e.store.PathByID(viewID, ev.FileID)
	if err != nil || cur == ev.Path {
		return false
	}
	e.log.Debug("engine: skipping self-echo create/modify, the node is tracked at another path",
		"view", viewID, "echo_path", ev.Path, "tracked_path", cur, "file_id", ev.FileID)
	return true
}

// selfEchoSupersedesRecord reports whether a self-echo create/modify naming a DIFFERENT node
// than the record at its path is the LATER statement about that path, and so must be adopted
// rather than skipped (finding H3; see selfEchoCreateBaseline for the replica-losing shape).
//
// The comparison is strict and positional: only an echo carrying its own sync id ABOVE the
// record's ServerSyncID can claim to describe a newer server state. An unpositioned echo
// (SyncID 0 - a sparse event with no place in the log) proves nothing and never supersedes,
// and an echo at the same position is not later either. The record's local content facts are
// preserved by the overlay: they describe the bytes on disk, which the echo confirms are ours.
func selfEchoSupersedesRecord(b store.Baseline, ev actions.Event) bool {
	return ev.SyncID > 0 && ev.SyncID > b.ServerSyncID
}

// overlaySelfEchoFacts advances a self-echoed create/modify node's baseline,
// preserving its recorded content facts and refreshing only what the event actually
// reports (a sparse event omits the unchanged hash/size/mtime, so an unconditional copy
// would zero them and trigger a spurious re-upload). The file id and type are refreshed
// only when the event reports them, so a sparse echo never blanks the id the outbound
// apply assigned.
//
// The STATE advances to synced only when the record is not owned by a queued apply: an
// existing StatePending or StateConflict is preserved - only the
// apply path, never reconcile, advances a baseline to synced (the invariant
// supersededReplay in inbound_rename.go leans on as proof "a prior apply already ran").
// The unconditional flip forged exactly that proof: a foreign [move F a->b] relocates
// F's baseline to b as StatePending (relocatePending), and our own upload's echo
// (modify F@b, committed before the move but delivered after it) then stamped that
// relocated record synced still in reconcile - the queued rename read the move as
// already applied and skipped the on-disk rename, a same-batch recreate's download
// landed over the un-moved file at the source, and the next local scan diffed the
// synced-but-fileless destination into an outbound OpRemove deleting the live server
// node. The echo confirms content facts only; the owning queued op (inbound
// rename/download, or an outbound upload replay) advances the state itself when it
// applies. A StateConflict record is likewise owned by the keep-both resolver. The
// normal cycle is unaffected: the outbound apply records the upload's baseline as
// synced before the echo can arrive, and an echo racing a still-queued upload replay
// leaves the record pending for the (idempotent) replay to advance.
//
// A non-zero Size is always adopted. A zero Size is adopted only when the event is a genuine
// truncate-to-zero - evidenced by the empty-content hash - never merely
// because the event reports SOME hash or mtime. A self-echo of our own truncate-to-zero carries
// the empty-content MD4 (and a fresh mtime) but Size 0; the old "if ev.Size != 0" guard kept the
// stale LocalSize, so the cheap-scan (size,mtime) hint could never match again and the file was
// re-hashed on every scan forever. But a content modify or a metadata/mtime touch whose echo
// omits Size (Size 0 with a REAL hash and/or a fresh mtime) is not empty - adopting its zero Size
// would zero the LocalSize the outbound apply recorded and break the very same hint (the mirror
// of the truncate bug, and exactly the case overlaySelfEchoMoveFacts's strict guard warns of),
// so such an echo keeps the recorded LocalSize. Only the empty-content hash proves a true zero.
func overlaySelfEchoFacts(b *store.Baseline, ev actions.Event) {
	if ev.FileID != "" {
		b.FileID = ev.FileID
	}
	if ft := fileType(ev.IsDir); ft == "dir" || b.FileType == "" {
		b.FileType = ft
	}
	if ev.SyncID > b.ServerSyncID {
		// ServerSyncID must never rewind (the monotonic stamp guard rescueDeferredMove states).
		// A tolerantly-decoded event that omits sync_id reaches here as 0, and the unconditional
		// write erased the record's stamp with it - disarming both positional gates finding H3
		// installed (removeSupersededByLaterObservation and downloadRelocatedBySelfEcho both read
		// a 0 stamp as "proves nothing"), on exactly the paths where the stamp is the only line
		// of defence left. selfEchoSupersedesRecord already refuses to let an unpositioned echo
		// decide anything; the WRITE must be as careful as the decision.
		b.ServerSyncID = ev.SyncID
	}
	if ev.Hash != "" {
		b.ServerHash = ev.Hash
		b.LocalHash = ev.Hash
	}
	if ev.MTime != 0 {
		b.ServerMTime = ev.MTime
		b.LocalMTime = ev.MTime
	}
	if ev.Size != 0 || ev.Hash == emptyContentMD4 {
		b.LocalSize = ev.Size
	}
	if b.State != store.StatePending && b.State != store.StateConflict {
		b.State = store.StateSynced
	}
}

// applySelfEchoMove advances the baseline for a move this client itself committed. A
// move event carries the paths and ids but commonly omits the content facts (hash,
// size, mtime) and file type - the bytes did not change, only the path did. Rebuilding
// the destination baseline straight from the sparse event (baselineFromEvent) would
// therefore zero those fields while marking the record synced, losing facts a later
// reconcile relies on. Instead, carry the richest baseline we hold for the
// moved node to the destination, overlaying only the facts the event actually reports,
// mirroring the non-self-echo relocatePending path.
//
// The Put at ev.Path drops an indexed node's orphan source record through the byid index,
// but a local-only node (empty file id) is unindexed, so the Put alone would strand the
// baseline at ev.FromPath. dropSelfEchoMoveSource then deletes the source - but
// only when it still identifies the moved node, never a different node recreated at the
// source path on replay (the recreated-source case relies on the file id distinguishing the
// two, so it cannot blindly delete ev.FromPath).
//
// When the moved node is a directory, the local commit moved its whole subtree on disk,
// so the descendant baselines are re-homed under the new prefix too - no executor op is
// queued for a self-echo, so this is the only place that can do it. The
// descendants are rebased BEFORE the root move, mirroring relocatePending: the root move
// drops the source record, and selfEchoMoveBaseline resolves an unindexed (empty-id) node
// by reading ev.FromPath, so moving the root first would leave a crash between it and the
// descendant rebase unable to re-find the node - stranding the subtree under the old prefix
// for a later same-batch event to overwrite. Descendants-first means the only crash window
// leaves the root still at ev.FromPath, which a replay re-finds. The relocation is
// idempotent: on a replay whose first apply already re-homed them, the old-prefix scan is
// empty.
func (e *Engine) applySelfEchoMove(viewID uint64, ev actions.Event) error {
	b, err := e.selfEchoMoveBaseline(viewID, ev)
	if err != nil {
		return err
	}
	if b.FileType == "dir" {
		if err := e.relocateDescendantBaselines(viewID, ev.FromPath, ev.Path); err != nil {
			return err
		}
	}
	if err := e.store.PutBaseline(viewID, ev.Path, b); err != nil {
		return fmt.Errorf("engine: self-echo move baseline %d/%q->%q: %w", viewID, ev.FromPath, ev.Path, err)
	}
	return e.dropSelfEchoMoveSource(viewID, ev, b)
}

// dropSelfEchoMoveSource removes the source baseline left at ev.FromPath after a self-echo
// move's destination Put. For an indexed node (non-empty file id) the Put already dropped the
// source through the byid index, so this is a no-op; for a local-only node (empty file id) the
// source is unindexed and this is the only delete. The delete is guarded: it runs
// only while the record at ev.FromPath still identifies the moved node (same file id, including
// the empty-id case). On a replay where the source path has since been taken by a different
// recreated node, the ids differ and the source is left untouched - mirroring the file-id guard
// the indexed path gets for free from the byid index. A self-move (ev.FromPath == ev.Path) drops
// nothing: the destination Put just refreshed the only record.
func (e *Engine) dropSelfEchoMoveSource(viewID uint64, ev actions.Event, b store.Baseline) error {
	if ev.FromPath == ev.Path {
		return nil
	}
	src, err := e.store.GetBaseline(viewID, ev.FromPath)
	switch {
	case isNotFound(err):
		return nil
	case errors.Is(err, store.ErrCorruptRecord):
		// The source record is undecodable: skip the guarded delete (the file id cannot be
		// compared, so a blind delete could drop a recreated node) and leave it. An indexed
		// node's source was already dropped by the destination Put's byid index; a stray
		// corrupt record heals when its key is next overwritten.
		e.warnCorruptBaseline("self-echo-move-source", viewID, ev.FromPath, err)
		return nil
	case err != nil:
		return fmt.Errorf("engine: self-echo move load source %d/%q: %w", viewID, ev.FromPath, err)
	}
	if src.FileID != b.FileID {
		// A different node was recreated at the source path; leave it in place.
		return nil
	}
	if err := e.store.DeleteBaseline(viewID, ev.FromPath); err != nil {
		return fmt.Errorf("engine: self-echo move drop source %d/%q: %w", viewID, ev.FromPath, err)
	}
	return nil
}

// selfEchoMoveBaseline resolves the baseline to record at a self-echo move's destination,
// preferring the richest facts available for the moved node. It resolves the node by its
// file id: the richest baseline we hold for it sits wherever the byid index currently
// points - the source, a prior apply's destination, or the far end of a multi-hop chain -
// not necessarily at ev.FromPath. Reading the source path blindly is wrong on replay: after
// move a->b plus a same-batch recreate of a, ev.FromPath holds the recreated node, so its
// facts would be copied onto the move; a multi-hop a->b->c replay would write a
// sparse baseline at the first hop and evict the rich record at c through the byid index.
// With the node not byid-tracked (an empty file id, or no index entry), it trusts the source
// baseline only when that record actually identifies the moved node, else falls back to the
// bare event - which omits the unchanged content facts and so is the last resort.
func (e *Engine) selfEchoMoveBaseline(viewID uint64, ev actions.Event) (store.Baseline, error) {
	if ev.FileID != "" {
		cur, err := e.store.PathByID(viewID, ev.FileID)
		switch {
		case isNotFound(err):
		case err != nil:
			return store.Baseline{}, fmt.Errorf("engine: self-echo move resolve node %d/%q: %w", viewID, ev.FileID, err)
		default:
			b, gerr := e.store.GetBaseline(viewID, cur)
			switch {
			case errors.Is(gerr, store.ErrCorruptRecord):
				// The byid-resolved record is undecodable: treat it as absent and fall
				// back to the bare event below, so the synced Put heals the key rather than
				// the read wedging the self-echo.
				e.warnCorruptBaseline("self-echo-move", viewID, cur, gerr)
			case gerr != nil && !isNotFound(gerr):
				return store.Baseline{}, fmt.Errorf("engine: self-echo move load tracked baseline %d/%q: %w", viewID, cur, gerr)
			case gerr == nil && b.FileID == ev.FileID:
				overlaySelfEchoMoveFacts(&b, ev)
				return b, nil
			}
		}
	}
	b, err := e.store.GetBaseline(viewID, ev.FromPath)
	switch {
	case isNotFound(err):
		return baselineFromEvent(ev, store.StateSynced), nil
	case errors.Is(err, store.ErrCorruptRecord):
		e.warnCorruptBaseline("self-echo-move", viewID, ev.FromPath, err)
		return baselineFromEvent(ev, store.StateSynced), nil
	case err != nil:
		return store.Baseline{}, fmt.Errorf("engine: self-echo move load baseline %d/%q: %w", viewID, ev.FromPath, err)
	case b.FileID == ev.FileID:
		overlaySelfEchoMoveFacts(&b, ev)
		return b, nil
	default:
		return baselineFromEvent(ev, store.StateSynced), nil
	}
}

// overlaySelfEchoMoveFacts marks a moved node's baseline synced, preserving its recorded
// facts and refreshing only what the move event actually reports (a move commonly omits
// the unchanged content facts and file type, so an unconditional copy would zero them).
// Unlike overlaySelfEchoFacts (create/modify), Size keeps the strict "ev.Size != 0" guard:
// a move never changes content, so the truncate-to-zero case cannot
// arrive as a move, and adopting a zero Size from a move that merely reports a fresh mtime
// would zero the moved node's real recorded size.
//
// Also unlike overlaySelfEchoFacts, the move overlay DOES advance an existing StatePending
// record to synced (evaluated and deliberately kept). Both call sites
// gate the overlay on b.FileID == ev.FileID, so it only ever touches the moved node's OWN
// record, and the synced-with-newer-ServerSyncID stamp it leaves at the destination is the
// load-bearing signal downloadRelocatedBySelfEcho keys on to redirect a queued download
// after a self-echo move (which enqueues no rename to carry it) - preserving pending here
// would break that redirect. Nor can the stamp forge supersededReplay's
// proof: the destination Put re-keys the record under ev.Path, so a queued foreign rename
// of the same node finds no same-id record at ITS destination, and the only interleaving
// that leaves one there ends in our own committed move to that destination - where our
// outbound apply already placed the bytes locally, so a skipped on-disk rename is
// genuinely superseded.
func overlaySelfEchoMoveFacts(b *store.Baseline, ev actions.Event) {
	b.FileID = ev.FileID
	if ev.SyncID > b.ServerSyncID {
		// The monotonic stamp guard, as in overlaySelfEchoFacts: a move whose sync_id the reply
		// omitted must not erase the stamp the moved node's record already carries - that stamp
		// is what downloadRelocatedBySelfEcho's "cur is strictly newer" redirect and the inbound
		// remove's positional gate both key on.
		b.ServerSyncID = ev.SyncID
	}
	if ev.Hash != "" {
		b.ServerHash = ev.Hash
		b.LocalHash = ev.Hash
	}
	if ev.MTime != 0 {
		b.ServerMTime = ev.MTime
		b.LocalMTime = ev.MTime
	}
	if ev.Size != 0 {
		b.LocalSize = ev.Size
	}
	b.State = store.StateSynced
}

// baselineFromEvent builds a baseline record from an event in the given state. The
// server facts are always recorded; a StateSynced record additionally copies them
// into the local facts, since a synced baseline means local and server agree.
func baselineFromEvent(ev actions.Event, state store.BaselineState) store.Baseline {
	b := store.Baseline{
		FileID:       ev.FileID,
		FileType:     fileType(ev.IsDir),
		ServerHash:   ev.Hash,
		ServerMTime:  ev.MTime,
		ServerSyncID: ev.SyncID,
		State:        state,
	}
	if state == store.StateSynced {
		b.LocalHash = ev.Hash
		b.LocalMTime = ev.MTime
		b.LocalSize = ev.Size
	}
	return b
}

// fileType maps the directory flag to the server's node-kind string recorded in a
// baseline's FileType field.
func fileType(isDir bool) string {
	if isDir {
		return "dir"
	}
	return "file"
}

// The batch prologue: two phases that run to completion before a single event is applied,
// each with its own ordering story, split out so reconcile's body starts at the per-event
// loop rather than 85 lines into the file.
//
// The order between them is not arrangement, it is the contract: normaliseBatch settles what
// the batch IS (which events survive, and in what shape), and only then does readBatchContext
// read the store and the queue against it. Both phases consume the WHOLE slice, which is why
// neither can be folded into the loop - see each one's comment.

// normaliseBatch turns the server's events into the batch reconcile actually processes:
// NFC-normalised paths, ordered by sync id, already-consumed events dropped, and rescuable
// moves rewritten as creates. It returns a private copy, so the caller's slice is untouched.
//
// Every step here must precede the batch-wide passes in readBatchContext, and the loop.
//
// Normalising at the boundary means all downstream keys, baseline writes and PathByID
// comparisons hold one canonical encoding regardless of how the server encoded the name
// (macOS NFD versus NFC).
//
// Dropping consumed events must not wait for the per-event loop, because the batch-wide
// passes are DESTRUCTIVE: dropSupersededQueuedDownloads acks queued ops and drops their
// baselines. A re-delivered, already-consumed ancestor delete reaching it would ack a
// legitimately queued OpDownload for a descendant (deleteSupersedes matches by ancestor
// prefix regardless of file id) and drop the download's baseline, while the loop then
// skipped that same delete so no OpRemove replaced it - the server node would never
// materialise locally, and neither the queue nor the next scan would re-detect it.
//
// Rescuing a move OFF a non-containable path onto a containable one has the same reason:
// all four batch-wide passes gate on eventContainable, which is false for the move's
// ORIGINAL shape. Rewriting inside the loop instead left the rescued create invisible to
// them - no pre-batch baseline captured for its destination (so its divergence gate ran
// with an empty base and could set a clean local file aside as a spurious _Conflict copy)
// and hasUpsert stayed false (so the keep-both DEFERRAL against a queued outbound upload at
// that destination never fired). Both are reachable through the documented remedy itself,
// when the operator renames "notes." to a "notes" the user has also just created locally.
// One rewrite up front gives every pass one event shape.
func (e *Engine) normaliseBatch(viewID uint64, events []actions.Event, cursor int64) []actions.Event {
	sorted := append([]actions.Event(nil), events...)
	for i := range sorted {
		sorted[i].Path = normpath.Normalize(sorted[i].Path)
		sorted[i].FromPath = normpath.Normalize(sorted[i].FromPath)
	}
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].SyncID < sorted[j].SyncID })
	sorted = e.dropConsumedEvents(viewID, sorted, cursor)
	e.rescueUncontainableSources(viewID, sorted)
	return sorted
}

// batchContext is what the per-event loop needs to know about the store and the queue as they
// stood BEFORE the batch touched either.
type batchContext struct {
	// inheritedBases carries each node's last-synced base forward from an already-queued
	// inbound op, keyed move-invariantly.
	inheritedBases map[string]inheritedBase
	// queuedDownloadIDs is the batch-start snapshot of node ids with a queued download.
	queuedDownloadIDs map[string]bool
	// pendingUploads is the set of paths with a queued outbound upload.
	pendingUploads map[string]bool
	// snap is the pre-batch baseline state every capture gate consults.
	snap batchBaselines
}

// readBatchContext runs the four batch-wide passes, in the one order their dependencies allow.
//
// queuedInboundBases first, and specifically BEFORE dropSupersededQueuedDownloads acks any
// doomed download. The Phase-9d divergence guards - the inbound download guard
// (BaseLocalHash) and the remove guard (BaseWasSynced/BaseLocalHash/BaseIsDir) - both need a
// node's last-synced base, but markPending wipes the synced state from the live baseline. So
// a SECOND inbound op for the same node, whether later in this batch or already queued from a
// prior one, would find no base and either clobber an unrecorded local edit or resurrect a
// perfectly clean deleted file as a spurious _Conflict copy. A [modify f] from batch 1 whose
// download THIS batch's [delete f] supersedes still carries f's pre-batch synced base, and the
// remove gate needs it to delete the clean file cleanly - the base would vanish with the acked
// op otherwise.
//
// dropSupersededQueuedDownloads then drops any prior-batch queued download this batch
// ultimately deletes, following the node through the batch's moves, so a doomed download whose
// node is already gone server-side can never wedge the queue behind its own remove. Following
// the moves is the point: the download was queued at a path the batch moves before deleting,
// so a bare path comparison against the delete would miss it - the queue holds the pre-move
// path.
//
// pendingOutboundUploads is the Phase-6d inbound conflict funnel's input. A remote
// create/modify for a path that already has a queued outbound upload must NOT be applied:
// clobbering the local file with the server's bytes and overwriting the pending baseline's
// local facts would silently lose the local edit. The queued upload is left intact to fire
// against the now-stale server base, draw the 24579 rejection, and funnel into the keep-both
// resolver. It reads the queue only when the batch carries a file create/modify - the only
// events that could collide - so a delete/move-only batch skips the scan.
//
// snapshotBatchBaselines last, but still before the loop's first markPending: the capture
// gates need the last-synced facts as they stood before this batch, and a live read mid-batch
// sees the half-folded intermediate an earlier event left.
func (e *Engine) readBatchContext(viewID uint64, sorted []actions.Event) (batchContext, error) {
	var bc batchContext
	var err error
	if bc.inheritedBases, bc.queuedDownloadIDs, err = e.queuedInboundBases(viewID, sorted); err != nil {
		return batchContext{}, err
	}
	if err = e.dropSupersededQueuedDownloads(viewID, sorted); err != nil {
		return batchContext{}, err
	}
	if bc.pendingUploads, err = e.pendingOutboundUploads(viewID, sorted); err != nil {
		return batchContext{}, err
	}
	if bc.snap, err = e.snapshotBatchBaselines(viewID, sorted); err != nil {
		return batchContext{}, err
	}
	return bc, nil
}
