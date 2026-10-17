package engine

// This file carries the inbound OpRemove apply path: it deletes the node on disk
// (idempotently) and drops its baseline, plus the two relocated-node helpers
// (removeRelocatedNode, removeRelocatedPriorFile) that honour a delete for a node a
// prior rejected rename re-tracked at a different on-disk path.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// removeNode is fsutil.Remove behind a package var so tests can inject the errno a directory
// removal reports when it loses a race. The race itself is not
// reproducible on demand: os.RemoveAll reports ENOTEMPTY/ERROR_DIR_NOT_EMPTY only when an
// entry appears BETWEEN its single readdir pass and the final rmdir - every deterministic
// construction (an unreadable child, an unwritable subdirectory) makes the recursion's own
// errno win instead, because RemoveAll deliberately surfaces the underlying cause. Every subtree
// delete an apply performs routes through this var - applyRemove's own delete,
// removeRelocatedPriorFile's cleanup of a download's stale prior copy, dropRemoveBaselineAt's
// relocated delete (deadletter.go, reached from applyRemove via removeRelocatedNode), and
// guardMkdirOccupant's clearing of a matched occupant so a mkdir can land - so the classifier's
// verb gate (existsBlocksForever) is exercised on the real apply path rather than in isolation.
var removeNode = fsutil.Remove

// applyRemove deletes the node on disk (idempotently) and drops its baseline. The delete
// names the node by its server path (op.Path), but a prior rejected rename may have
// re-tracked the same file id at a different, still-reachable on-disk path. When op.Path now
// holds nothing the on-disk remove and baseline drop here are a no-op, so the node would
// survive locally after the server deleted it; removeRelocatedNode resolves by file id and
// honours the delete at the relocated path too.
func (e *Engine) applyRemove(ctx context.Context, op store.Op) error {
	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	// On a byte-preserving filesystem (Linux, Windows) the node may sit under a non-NFC (e.g.
	// NFD) name while the op carries the canonical NFC path the scanner normalized to, so the
	// literal NFC path does not exist on disk. Resolve to the real on-disk spelling before the
	// divergence guard and the delete, mirroring the outbound resolveNFCPath and keep-both
	// onDiskAbs resolution - otherwise os.RemoveAll(absNFC) is a no-op, the baseline is dropped,
	// and the file lingers untracked until the next scan re-uploads it (a delete/re-create
	// ping-pong). onDiskAbs returns abs unchanged when the literal path exists or nothing matches.
	// When the fallback resolves to a different spelling, onDiskAbsNoSymlink re-checks that real
	// path for a symlink component (e.abs's noSymlinkEscape only walked the absent literal NFC
	// path) so os.RemoveAll can never follow a symlinked component outside the sync root.
	abs, err = e.onDiskAbsNoSymlink(abs)
	if err != nil {
		return err
	}
	// Identity gate: the server's delete names a NODE (op.FileID), but everything below
	// addresses it by PATH. When a different node holds op.Path the delete does not authorise
	// destroying it - the motivating interleaving is the laptop-sleep race:
	// machine A edits p offline while machine B deletes p's node F; the loop order
	// (localScan -> drainQueue -> pollOnce) pushes A's edit BEFORE the delete event is polled,
	// so the server assigns a fresh node N at the freed path and markSynced records it. The
	// delete of F then found N's record here, and since N's last-synced hash IS the current
	// on-disk content the divergence guard read "no divergence" and fsutil.Remove destroyed the
	// just-pushed edit with no _Conflict copy - after which the vanished-but-synced record made
	// the next scan emit an outbound OpRemove that deleted N server-side too, losing the edit
	// from both replicas. Leave the successor's file and record alone and ack the op
	// idempotently (our node is already gone from this path); the delete is still honoured
	// wherever the byid index still tracks op.FileID (removeRelocatedNode below). This is the
	// live-read mirror of the dead-letter twin dropRemoveBaselineAt's identical guard and of the
	// reconcile-time capture/flipPending id gates.
	// One read serves both this gate and the wasDir fallback below; nothing between them
	// mutates the key (only an os.Lstat runs), so a second decode of the same record would be
	// pure duplication on the hot inbound-delete path.
	live, gerr := e.store.GetBaseline(op.ViewID, op.Path)
	tracked := gerr == nil
	if errors.Is(gerr, store.ErrCorruptRecord) {
		// Treated as untracked below (fail toward the never-lose default), but it must not be
		// swallowed: this one read feeds BOTH the identity gate and the live-baseline wasDir
		// signal, so a bit-rotted record here can silently skip the descendant-baseline drop
		// (re-opening the outbound-OpRemove wedge) with nothing in the log to
		// explain it. Every sibling corrupt-read site Warns the same way.
		e.warnCorruptBaseline("inbound-remove", op.ViewID, op.Path, gerr)
	}
	switch {
	case tracked && op.FileID != "" && !baseIdentifiesNode(live, op.FileID):
		e.log.Warn("engine: inbound remove skipped, another node holds the path",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID)
		// This refusal owes the same record repair as the positional one below, and it is the arm
		// the MEASURED H3 shape actually takes: reconcile's flipPending marks the record pending
		// for the delete, then the successor's self-echo adopts the record to the live node while
		// overlaySelfEchoFacts deliberately preserves StatePending - so by drain time the ids
		// differ and this gate fires first. Left unrepaired the record stays pending forever, and
		// localReconcile drives ops from StateSynced records only: the path is frozen AND
		// invisible (no drop, nothing in Stuck, nothing in Unresolved), which is exactly the
		// silent-unsyncable class finding H6 closes. Wiring the repair to only one of the two
		// gates left the primary shape in it.
		if derr := e.repairSupersededRecord(op, tracked, live); derr != nil {
			return derr
		}
		return e.removeRelocatedNode(ctx, op)
	case gerr != nil && !isNotFound(gerr) && !errors.Is(gerr, store.ErrCorruptRecord):
		// A backend failure must never be read as "no record": the gate below and the wasDir
		// signal both decide destructive behaviour from it. (An identity gate is only run for
		// an id-carrying op, but the read itself is unconditional, so the error arm is too.)
		return fmt.Errorf("engine: load remove baseline %d/%q: %w", op.ViewID, op.Path, gerr)
	}
	// Decide directory-ness BEFORE the mutations below erase the evidence:
	// a server delete event names only the directory, never its descendants, so the descendant
	// baselines must be dropped here alongside the directory's own - leaving them would make the
	// next scan diff each descendant as a vanished file and emit an outbound OpRemove whose
	// absence confirmation (listing the already-deleted parent) can never conclude, wedging the
	// queue permanently. Three signals, each covering a case the others miss: the reconcile-time
	// capture (op.BaseIsDir - survives a crash replay after the store/disk state is gone), the
	// on-disk node (a directory occupying the path under a non-dir/absent baseline), and the live
	// baseline's recorded type (a non-synced dir base the capture gate skipped).
	wasDir := op.BaseIsDir
	if !wasDir {
		if fi, statErr := os.Lstat(abs); statErr == nil && fi.IsDir() {
			wasDir = true
		}
	}
	if !wasDir && tracked && live.FileType == "dir" {
		wasDir = true
	}
	// Positional gate, the second line behind the id gate above (finding H3). The delete names a
	// node AND a position in the log; a baseline stamped STRICTLY ABOVE that position is a later
	// statement by the server about the same path, so what stands there is a successor the delete
	// never named. The measured shape is a returning client whose offline create is pushed into a
	// directory the peer deleted meanwhile: the server auto-creates a FRESH parent node, our
	// record keeps naming the deleted one (so the id gate agrees to delete), the child inside is
	// perfectly in sync with ITS live node (so the divergence sweep sees nothing to preserve) -
	// and the recursive delete took the user's own new file off disk with an empty queue, zero
	// dead-letters and a clean status. The child's own later stamp is the evidence that survives
	// even when the parent's record is stale, which is why the directory arm looks at descendants.
	if superseded, at, err := e.removeSupersededByLaterObservation(op, tracked, live, wasDir); err != nil {
		return err
	} else if superseded {
		e.log.Warn("engine: inbound remove skipped, the path was described after the delete",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID,
			"delete_sync_id", op.ServerSyncID, "observed_at", at)
		// Ack the op idempotently, exactly as the id gate does: the node the server deleted is
		// not the one standing here. Nothing here forges agreement about a record whose node may
		// well be dead. The delete is still honoured wherever the byid index tracks the deleted
		// id at another path.
		if derr := e.repairSupersededRecord(op, tracked, live); derr != nil {
			return derr
		}
		return e.removeRelocatedNode(ctx, op)
	}
	// Cluster-A keep-both guard: the server is deleting this node, but if the local copy holds
	// content the server never saw (a cheap-scan-missed edit, or an untracked file), os.RemoveAll
	// would destroy it irrecoverably. Set the divergent content aside as a _Conflict copy before
	// the delete so it survives (the inbound download path already guards its target this way).
	if err := e.guardRemoveDivergence(ctx, op, abs); err != nil {
		return err
	}
	if err := removeNode(abs); err != nil {
		return err
	}
	if wasDir {
		// The recursive on-disk delete above took the whole subtree; drop the descendants'
		// baselines with it (idempotent - the guard's preserve branch may already have run it).
		// This MUST run BEFORE op.Path's own baseline is deleted: op.BaseIsDir is captured only
		// for a reconcile-time StateSynced base (reconcile.go), so a non-synced dir base relies
		// on the live-baseline FileType signal to re-derive wasDir. If DeleteBaseline(op.Path)
		// ran first and this drop then failed, the retry would find op.Path's baseline already
		// gone and the on-disk dir already removed - all three wasDir signals false - silently
		// skipping the drop and stranding the descendant baselines forever (the very outbound-
		// OpRemove wedge). Ordered this way, a failure leaves the op queued with
		// op.Path's baseline intact, so the next drain re-derives wasDir and completes the drop.
		if err := e.dropDescendantBaselines(op.ViewID, op.Path); err != nil {
			return err
		}
	}
	if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
		return fmt.Errorf("drop baseline: %w", err)
	}
	return e.removeRelocatedNode(ctx, op)
}

// repairSupersededRecord resolves the baseline left behind by a delete
// removeSupersededByLaterObservation just refused. It has three arms.
//
// # The first arm: a DIRECTORY record still naming the deleted node
//
// It is dropped. Kept, it would address every later push into the directory: resolveParentID
// answers from Baseline.FileID whatever the record's state, so each child create goes to
// `id:<dead parent>/name` and gets 32769 - the bare ErrPathError, with no ServerErrorCode, so it
// neither escalates nor paces and retries at full drain rate forever while blockedKeys holds the
// subtree. Nothing else heals it: localReconcile drives ops from StateSynced records only.
//
// Dropping is the repair already used for a baseline that provably names the wrong node
// (errRenameSourceGone, revertRelocatedOutbound's corrupt arm): the directory becomes untracked,
// which every never-lose gate treats MORE protectively, and the next localScan re-detects it as a
// create whose OpMkdir adopts the live successor through applyMkdirOut's existing-directory probe,
// restamping the record. Descendant records are kept: they describe their own nodes, and the
// re-adopted parent is enqueued ahead of any child op (sorted path order). Only a directory
// qualifies, because only there is the record provably stale AND the re-adoption path available.
//
// # The second arm: the record the refusal PROVED current
//
// A record naming a DIFFERENT node, stamped strictly above the delete's position, is the server
// describing a live successor after the delete. Reconcile flipped it to StatePending only for this
// remove, so with the remove refused the suspended agreement is restored (StateSynced). This is
// where the measured H3 shape ends (the self-echo adopts the successor, the id gate refuses the
// stale delete); StateConflict here would report Unresolved forever, never resync the directory
// and never propagate a later local delete of it.
//
// Every operand must be real, or the record falls to the third arm: a delete with no position
// (op.ServerSyncID == 0) proves nothing; a record naming the SAME node still describes the corpse
// (restoring it would push to a dead file id); and an ID-LESS record is no successor -
// `live.FileID != op.FileID` reads an empty id as a different node, the opposite of
// baseIdentifiesNode. baselineFromEvent writes such id-less records for inbound create/modify
// events without an id, whose download is still queued yet invisible to queuedInboundCoversNode
// (false for an empty id), so restoring them would forge StateSynced for content never transferred.
//
// # The third arm: the record the refusal cannot repair
//
// Reconcile flipped the record to StatePending before enqueuing (flipPending does so for an
// ID-LESS delete whatever the record names, as baseIdentifiesNode passes with no id to compare),
// and the refusal acks the op. A pending record emits no work of its own (localReconcile drives
// StateSynced only) and is INVISIBLE - no dead-letter, nothing in `Stuck` or `Unresolved` - until
// the server describes the path again. That silent-unsyncable class is what H6 closes, so the
// record is marked StateConflict, counted in the per-view `Unresolved` number.
//
// StateConflict forges no agreement and is never less protective than StatePending: every
// never-lose gate tests `!= StateSynced` (subtreeHasDivergence, untrackedFilePresent,
// removeBaseFor), and a later inbound apply still advances it through markSynced. A StateSynced
// record is left alone: it is not frozen, and marking it would STOP the path syncing.
func (e *Engine) repairSupersededRecord(op store.Op, tracked bool, live store.Baseline) error {
	// The refusal owes the SUBTREE this repair, not only the node, and it owes it on every arm -
	// including the id gate's early return below and the untracked case, where the directory's own
	// record is already gone but its descendants' records are not.
	if err := e.surfaceSupersededDescendants(op); err != nil {
		return err
	}
	if !tracked {
		return nil // no record to repair
	}
	if op.FileID != "" && live.FileID == op.FileID && live.FileType == fileType(true) {
		if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
			return fmt.Errorf("engine: drop superseded directory baseline %d/%q: %w", op.ViewID, op.Path, err)
		}
		e.log.Info("engine: dropped the stale record of a superseded directory; the next scan re-adopts the live node",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID)
		return nil
	}
	if live.State == store.StateSynced || live.State == store.StateConflict {
		return nil // not frozen, or already surfaced
	}
	// A record a queued OUTBOUND op owns is left UNTOUCHED, for the same reason the rename arm
	// below leaves its own alone: the repair's whole premise is that THIS remove is what suspended
	// the record, and for an outbound owner that is provably false - markLocalPending suspended it
	// when localReconcile enqueued the push, preserving the record's server facts (FileID,
	// ServerHash, ServerSyncID) and writing the UN-PUSHED local bytes into LocalHash.
	//
	// flipPending deliberately leaves a record naming a different node alone, so a record can sit
	// at StateSynced while this delete is already queued, and the next scan's edit then hands it
	// to an OpUpload. The delete holds the lower seq, serialises ahead of that push and reaches
	// this repair with live.FileID (the successor) stamped above op.ServerSyncID - every condition
	// of the restore arm below satisfied, and queuedInboundCoversNode blind to the owner because
	// it filters on DirInbound. Restoring there writes StateSynced over content the server has
	// never received: it breaks the "only apply advances a baseline to StateSynced" invariant, and
	// if the owning push later dead-letters the record is left claiming agreement with LocalHash
	// equal to the bytes on disk - localReconcile re-derives nothing, so the edit silently never
	// reaches the server, and the never-lose gates (subtreeHasDivergence, untrackedFilePresent,
	// removeBaseFor) all read StateSynced as agreed content and let an ancestor's delete destroy
	// it with no _Conflict copy.
	//
	// Untouched rather than StateConflict: the owner is queued, so the path is neither frozen nor
	// unowned (the H6 test), its apply ends in markSynced, and a drop routes through the
	// dead-letter cleanup that resolves the pending record it expects to find.
	if owned, oerr := e.queuedOutboundOwnsPath(op.ViewID, op.Path); oerr != nil {
		return oerr
	} else if owned {
		e.log.Info("engine: leaving the refused delete's record to its queued outbound op",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID)
		return nil
	}
	if op.ServerSyncID != 0 && live.FileID != "" && live.ServerSyncID > op.ServerSyncID && live.FileID != op.FileID {
		// The restore rests on ONE premise: this remove is the op that suspended the record, so
		// the op leaving the queue restores the agreement it suspended. That holds for the
		// measured H3 shape, where the successor arrives as a SELF-ECHO and enqueues nothing -
		// and it is false the moment the successor arrived as an ordinary inbound event, which
		// enqueues a download of its own (a same-batch [delete F@p, create N@p] does exactly
		// that: the remove holds the lower seq and drains first). Restoring there would forge
		// StateSynced for a node whose bytes were never transferred, breaking the "only apply
		// advances a baseline to StateSynced" invariant on the one arm that is LESS protective
		// than the StatePending it replaces: with an empty LocalHash and the predecessor's
		// content still on disk, a scan that runs before the download applies diffs the path
		// into an OpUpload that pushes the deleted file's bytes under the successor's identity,
		// and a download that leaves the queue without applying is preserved by
		// dropStrandedBaseline precisely BECAUSE the record now claims agreement.
		//
		// The queue is the discriminator, as it is for the directory-delete precondition: a node
		// a queued inbound op still owes is not agreed content. Such a record falls through to
		// StateConflict below - never less protective than pending (every gate tests
		// != StateSynced), reported in `Unresolved`, and advanced by the owning op's own
		// markSynced when it applies.
		covered, cerr := e.queuedInboundCoversNode(op.ViewID, live.FileID, 0)
		if cerr != nil {
			return cerr
		}
		if !covered {
			live.State = store.StateSynced
			if err := e.store.PutBaseline(op.ViewID, op.Path, live); err != nil {
				return fmt.Errorf("engine: restore superseded remove target %d/%q: %w", op.ViewID, op.Path, err)
			}
			e.log.Info("engine: restored the refused delete's path; its record names a node the server described after the delete",
				"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID,
				"delete_sync_id", op.ServerSyncID, "tracked_sync_id", live.ServerSyncID)
			return nil
		}
		e.log.Info("engine: not restoring the refused delete's path; a queued inbound op still owes the successor node",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID)
	}
	// One class of owning op must be left its record UNTOUCHED, and it is decided by what that
	// owner's apply does with the state it finds. A download or a mkdir ends in markSynced, which
	// overwrites the state whatever it was - so the mark below is exactly what it claims to be
	// there: visible in Unresolved until the owner applies, then gone. A RENAME does not:
	// applyRename deliberately PRESERVES StateConflict when it relocates a node (a divergence the
	// server's move carries across is still a divergence), so a record marked here comes out of
	// the relocation still StateConflict - and localReconcile drives only StateSynced records,
	// with no further server event owed for a move that has already applied. The mark would then
	// be the thing that freezes the path, permanently, which is the H6 class this repair closes
	// rather than opens.
	//
	// The shape is one poll batch of [delete F@p, move N: q->p]: reconcile relocates N's facts
	// onto p and queues the rename, the remove holds the lower seq and drains first, and the id
	// gate fires. Leaving the record as the rename's own apply expects to find it costs nothing on
	// the H6 axis either - a record a queued op owes is neither frozen nor unowned.
	carried, rerr := e.queuedInboundRenameCarries(op.ViewID, live.FileID)
	if rerr != nil {
		return rerr
	}
	if carried {
		e.log.Info("engine: leaving the refused delete's path to the queued rename that owes the node; a conflict mark would survive the relocation",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID)
		return nil
	}
	if live.FileID == "" && live.FileType == fileType(true) {
		// One record shape the mark is NOT harmless for, and it is the one exception to the
		// "never less protective than StatePending" argument above: resolveParentID answers from
		// the PARENT directory's record, and an id-less one is terminal (errParentConflicted)
		// when it is StateConflict and transient (errParentUnknown) when it is not. So marking an
		// id-less DIRECTORY record here dead-letters every queued outbound op beneath it -
		// permanently, since localReconcile drives StateSynced records only - where leaving it
		// pending lets those children wait for the mkdir that still owes the directory its server
		// id. That mkdir is exactly what an id-less pending directory record means: the record
		// carries no id BECAUSE the work that would give it one has not applied yet, so the record
		// is neither frozen nor unowned and the H6 argument does not reach it. Surfaced as a Warn
		// instead, so the refusal is still readable.
		e.log.Warn("engine: leaving the refused delete's path pending; its record is an id-less directory, and marking it unresolved would dead-letter every queued op beneath it",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID)
		return nil
	}
	live.State = store.StateConflict
	if err := e.store.PutBaseline(op.ViewID, op.Path, live); err != nil {
		return fmt.Errorf("engine: mark superseded remove target %d/%q unresolved: %w", op.ViewID, op.Path, err)
	}
	e.log.Info("engine: marked the refused delete's path unresolved; it is protected but not synchronised",
		"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID, "tracked_id", live.FileID)
	return nil
}

// surfaceSupersededDescendants resolves the records BENEATH a directory whose delete the gates
// above have just refused. repairSupersededRecord owns the node's own record; the descendants
// were the half nothing owned.
//
// `remove` is RECURSIVE on the server, and a recursive remove reports ONLY the directory it
// names - measured 27.08.2026 (TestIntegration_RecursiveRemoveTombstones: one delete row for the
// directory, zero for its four descendants, re-read after a settle window so the absence is
// corroborated rather than read once). So the children's server nodes died with the directory and
// NO later event will ever name them. Their records meanwhile sit at StateSynced over local files
// that are still on disk, and every mechanism that could notice reads that state as agreement:
// localReconcile drives StateSynced records and finds disk == LocalHash, so it diffs nothing; the
// server never re-describes a dead node; cold start does not re-run. The subtree then looks
// synchronised in the one replica that still holds it - no _Conflict copy, no dead-letter,
// nothing in Stuck or Unresolved. That is the silent-unsyncable class finding H6 exists to close,
// reached through the REFUSAL path instead of the apply path (where dropDescendantBaselines
// already retires exactly these records).
//
// The repair is to make them VISIBLE, not to act on them. StateConflict is never less protective
// (every never-lose gate tests != StateSynced) and it is what `Unresolved` counts, so the operator
// sees the paths. It deliberately does NOT re-upload the subtree: that would resurrect nodes a
// peer deliberately deleted, on evidence this client does not have, which is the outbound
// doctrine's own refusal - a mutation must prove itself - applied to the delete axis. The cost is
// the declared freeze the sibling H7 refusal already carries (DESIGN §13): the paths stop syncing
// until the operator resolves them, which is the trade this codebase makes everywhere it cannot
// prove which side is right.
func (e *Engine) surfaceSupersededDescendants(op store.Op) error {
	// Without the delete's own position there is nothing to compare a descendant's stamp against,
	// and marking every descendant would sweep in the children the SUCCESSOR legitimately owns.
	// Fail toward touching nothing. Measured: this DSM emits no unpositioned event (0 of 523 rows,
	// TestIntegration_EventShapes), so the arm costs nothing against it.
	if op.ServerSyncID <= 0 {
		return nil
	}
	type candidate struct {
		path string
		base store.Baseline
	}
	var stale []candidate
	// Collect first, write after: the callback runs inside the store's read transaction, and a
	// write from in there is the deadlock the store's own docs name. dropDescendantBaselines
	// collects its paths for the same reason.
	skipped, err := e.scanBaselinePrefix(op.ViewID, op.Path, func(pb store.PathBaseline) error {
		// A record the server described AFTER the delete is not one the recursive remove took:
		// the server cannot report a node inside a directory it has already deleted, so a later
		// stamp proves the node belongs to the successor standing here. This is the same
		// positional evidence removeSupersededByLaterObservation refuses on, read one level down.
		//
		// An UNSTAMPED record (0) is the absence of that evidence, never proof of doom, and it is
		// skipped for the same reason this function bails when the OP carries no position. Every
		// sibling gate reads a 0 the same way - removeSupersededByLaterObservation disarms,
		// selfEchoSupersedesRecord requires a positive id, overlaySelfEchoFacts records that a 0
		// stamp proves nothing. Two ordinary paths produce one: a cold start stamps its whole
		// catalog 0, and a file this client created carries 0 until its own create echo is
		// reconciled (markSynced never sets a server position). The mixed-stamp subtree is what
		// makes this reachable rather than theoretical: the delete is refused as soon as ANY
		// descendant is stamped above it, and the sweep then sees the whole subtree - including a
		// sibling whose upload echo has not been polled yet. Freezing that file is the inverse of
		// what the node-level arm refuses to do, and it stops the user's later edits reaching the
		// server at all.
		if pb.ServerSyncID <= 0 || pb.ServerSyncID > op.ServerSyncID {
			return nil
		}
		// Only a record claiming AGREEMENT is the harm this closes. A non-synced record already
		// drives no work while being visible (StateConflict) or owned by the op that suspended it
		// (StatePending), so re-marking it would at best be a no-op and at worst overwrite the
		// state its owner's apply expects to find.
		if pb.State != store.StateSynced {
			return nil
		}
		stale = append(stale, candidate{path: pb.Path, base: pb.Baseline})
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine: list descendants of the refused delete %d/%q: %w", op.ViewID, op.Path, err)
	}
	for _, p := range skipped {
		// An undecodable record cannot be positioned or classified, so it is reported and left -
		// the same treatment every sibling corrupt-read site gives it.
		e.warnCorruptBaseline("superseded-descendants", op.ViewID, p, store.ErrCorruptRecord)
	}
	if len(stale) == 0 {
		return nil
	}
	outbound, inbound, err := e.queuedOwnersInView(op.ViewID)
	if err != nil {
		return err
	}
	marked := 0
	for _, c := range stale {
		// A record a queued op still owes is neither frozen nor unowned, and its owner's apply
		// expects the state it left: the same deference the node-level arms show, for the same
		// reasons (an outbound owner carries un-pushed bytes in LocalHash; an inbound owner ends
		// in its own markSynced, or in a dead-letter cleanup that resolves the record it finds).
		if _, owned := outbound[normpath.Key(c.path)]; owned {
			continue
		}
		if c.base.FileID != "" {
			if _, covered := inbound[c.base.FileID]; covered {
				continue
			}
		}
		// The one record shape the mark is not harmless for, exactly as at the node level:
		// resolveParentID reads an id-less DIRECTORY record as terminal (errParentConflicted) when
		// it is StateConflict, so marking one dead-letters every queued op beneath it.
		if c.base.FileID == "" && c.base.FileType == fileType(true) {
			e.log.Warn("engine: leaving a superseded descendant pending; its record is an id-less directory, and marking it unresolved would dead-letter every queued op beneath it",
				"view", op.ViewID, "path", c.path, "deleted_id", op.FileID)
			continue
		}
		c.base.State = store.StateConflict
		if err := e.store.PutBaseline(op.ViewID, c.path, c.base); err != nil {
			return fmt.Errorf("engine: surface superseded descendant %d/%q: %w", op.ViewID, c.path, err)
		}
		marked++
	}
	if marked > 0 {
		e.log.Warn("engine: the refused delete's subtree was destroyed on the server and cannot be re-described; its records are marked unresolved rather than left claiming agreement",
			"view", op.ViewID, "path", op.Path, "deleted_id", op.FileID,
			"delete_sync_id", op.ServerSyncID, "descendants_marked", marked)
	}
	return nil
}

// queuedOwnersInView collects, in ONE pass over the durable queue, the outbound-owned path keys
// and the inbound-owed node ids of a view.
//
// The per-record helpers (queuedOutboundOwnsPath, queuedInboundCoversNode) each scan the whole
// queue and stop at the first match, which is right for one question and wrong for a subtree:
// asking them per descendant costs O(subtree x queue) decodes inside an apply. One pass answers
// both questions for every descendant at once, the way localReconcile's queuedPaths builds its
// index once per scan rather than per candidate.
func (e *Engine) queuedOwnersInView(viewID uint64) (outbound, inbound map[string]struct{}, err error) {
	outbound, inbound = make(map[string]struct{}), make(map[string]struct{})
	// A scan failure propagates: deciding "nothing owns this" from a queue that could not be
	// read is how a repair overwrites the state an owner was relying on.
	if serr := e.scanQueuedInView(viewID, "owners", func(qo store.QueuedOp) error {
		switch qo.Direction {
		case store.DirOutbound:
			// Both endpoints, since localReconcile relocates the baseline to a rename's
			// destination - the same rule queuedOutboundOwnsPath applies.
			outbound[normpath.Key(qo.Path)] = struct{}{}
			if qo.NewPath != "" {
				outbound[normpath.Key(qo.NewPath)] = struct{}{}
			}
		case store.DirInbound:
			if qo.FileID != "" {
				inbound[qo.FileID] = struct{}{}
			}
		}
		return nil
	}); serr != nil {
		return nil, nil, serr
	}
	return outbound, inbound, nil
}

// queuedOutboundOwnsPath reports whether a QUEUED OUTBOUND op still owes the given path in the
// view. It is the discriminator repairSupersededRecord uses to tell the record THIS delete
// suspended (reconcile's flipPending) from one localReconcile's markLocalPending suspended when
// it enqueued a push - the latter carries un-pushed local bytes in LocalHash, so forging
// agreement on it is a never-lose violation, not a repair.
//
// Both endpoints of an outbound rename count: localReconcile relocates the baseline to the
// destination, so a rename owes its NewPath as much as its Path. Matching is fold-keyed, like
// every other path comparison the engine makes against store keys. A scan failure propagates so
// the caller leaves the record alone rather than deciding on incomplete information.
func (e *Engine) queuedOutboundOwnsPath(viewID uint64, p string) (bool, error) {
	key := normpath.Key(p)
	var found bool
	if err := e.scanQueuedInView(viewID, "outbound ops", func(qo store.QueuedOp) error {
		if qo.Direction != store.DirOutbound {
			return nil
		}
		if normpath.Key(qo.Path) != key && (qo.NewPath == "" || normpath.Key(qo.NewPath) != key) {
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

// removeSupersededByLaterObservation reports whether the path an inbound remove addresses has
// been described by the server AFTER the delete that produced the op, which makes the node
// standing there a successor the delete never named (finding H3). It returns the path whose
// record carries the later stamp, for the operator-facing Warn.
//
// Two arms, because the evidence sits in different records depending on how the successor
// arrived:
//
//   - op.Path's OWN record stamped above the delete. Reconcile flipped that record to pending
//     but preserved its stamp, so this reads the same fact the id gate would if the successor's
//     id had reached us. It is the only arm an ID-LESS delete has (baseIdentifiesNode passes
//     unconditionally there, so the id gate cannot fire at all).
//   - For a DIRECTORY, any DESCENDANT record stamped above the delete. The measured shape leaves
//     the directory's own record naming the dead node - nothing ever restamped it, since the
//     server auto-created the replacement parent and our push learned no id for it - while the
//     child we uploaded into it carries the position of its own create echo. The server cannot
//     describe a node inside a directory it has already deleted, so such a stamp proves the
//     directory on disk is a live successor and the recursive delete would take the child with it.
//
// State is deliberately not consulted: ServerSyncID means "the sync id at which this server state
// was observed" whatever the record's state, and a pending record under the doomed directory is
// exactly the queued-download case that must not be deleted out from under its op either.
//
// The guard disarms itself when the delete carries no position (op.ServerSyncID == 0 - an op
// enqueued before the field was threaded, or an unpositioned event): with nothing to compare
// against, the historical behaviour stands. A corrupt descendant record is not evidence of a
// later observation (its bytes cannot be read); it is already handled by the divergence sweep
// and the drop pass, so it is ignored here rather than freezing every delete behind it.
func (e *Engine) removeSupersededByLaterObservation(op store.Op, tracked bool, live store.Baseline, wasDir bool) (bool, string, error) {
	if op.ServerSyncID == 0 {
		return false, "", nil
	}
	if tracked && live.ServerSyncID > op.ServerSyncID {
		return true, op.Path, nil
	}
	if !wasDir {
		return false, "", nil
	}
	found := ""
	if _, err := e.scanBaselinePrefix(op.ViewID, op.Path, func(pb store.PathBaseline) error {
		if pb.ServerSyncID > op.ServerSyncID {
			found = pb.Path
			return errStopBaselineScan
		}
		return nil
	}); err != nil && !errors.Is(err, errStopBaselineScan) {
		// A scan failure is not permission to delete: leave the op queued to retry rather than
		// destroying a subtree on incomplete information (the never-lose direction).
		return false, "", fmt.Errorf("engine: scan descendants for remove %d/%q: %w", op.ViewID, op.Path, err)
	}
	return found != "", found, nil
}

// guardRemoveDivergence preserves locally-diverged or untracked content the inbound remove would
// otherwise destroy. It dispatches on the on-disk node kind:
//
//   - A regular FILE is preserved when its content diverges from the baseline's last-synced local
//     hash, or when nothing is tracked there at all (an untracked user file the server happens to
//     be deleting). A tracked file whose baseline carries no recorded local hash, or whose on-disk
//     content still matches it, is left to the server's authoritative delete (no divergence basis).
//   - A DIRECTORY whose subtree holds any divergent or untracked regular file is moved aside whole
//     to a _Conflict directory rather than recursively deleted, so no edited/untracked descendant
//     is silently lost; a fully in-sync subtree is left for the normal recursive delete.
//
// A missing or unstattable path is a no-op (the delete is already a no-op or will surface its own
// error). On preservation the node has been moved aside, so the caller's fsutil.Remove no-ops.
func (e *Engine) guardRemoveDivergence(ctx context.Context, op store.Op, abs string) error {
	fi, err := os.Lstat(abs)
	if err != nil {
		return nil // already gone, or unstattable; leave it to fsutil.Remove
	}
	if fi.IsDir() {
		return e.guardRemoveDir(ctx, op, abs)
	}
	if !fi.Mode().IsRegular() {
		return nil // a symlink/device is not user content to preserve here
	}

	base := removeBaseFor(op)
	if _, err := e.preserveDivergentLocal(op.ViewID, op.Path, abs, base); err != nil {
		return err
	}
	return nil
}

// removeBaseFor resolves the divergence base for a file remove from the facts reconcile captured on
// the op (BaseWasSynced / BaseLocalHash), not the live baseline - by apply time markPending has
// flipped the baseline to StatePending, erasing the state that distinguishes a trustworthy
// last-synced hash from an unpushed/divergent one. It returns the base a caller feeds to
// preserveDivergentLocal: a non-empty hash triggers a hash-vs-base divergence check (a match
// honours the server's delete), and an empty string preserves any present regular file whole (no
// trustworthy base). Outcomes mirror the inbound download/rename capture gates and
// relocatedRemoveBase's identical never-lose direction:
//
//   - The reconcile-time baseline was StateSynced with a recorded local hash → that hash is the
//     divergence base (compare on-disk content against it; a match honours the server's delete).
//   - StateSynced but a DIRECTORY base (BaseIsDir) → empty base: the caller only reaches here for a
//     regular file on disk, so the user replaced the tracked directory with a file the server never
//     saw. A dir baseline's LocalHash is empty structurally, not evidence the file matches anything
//     synced, so the file is preserved whole.
//   - StateSynced (file base) but no recorded local hash → empty base: an anomalous state - a
//     StateSynced file baseline always carries a non-empty LocalHash (markSynced derives one); the
//     only producers of an empty one are the inbound-rename bare/promoted-pending writes (the
//     bare-record anomaly). There is no trustworthy divergence base, so the on-disk file is preserved
//     whole rather than deleted under a false server-authority read. This is the mirror of
//     relocatedRemoveBase's identical arm and the never-lose direction; a genuine
//     server-authoritative delete of such a record instead DROPS the baseline outright rather than
//     routing through this gate (coordinated with relocatedRemoveBase).
//   - Anything else (untracked, a non-synced base, or an op produced before the field existed) →
//     empty base: the on-disk file is preserved whole, since a non-synced LocalHash may be the
//     still-unpushed ON-DISK content and trusting it as its own base would let the delete silently
//     clobber it. This is the safe, never-lose default.
func removeBaseFor(op store.Op) (base string) {
	if !op.BaseWasSynced {
		return "" // untracked or non-synced base: preserve any present regular file whole
	}
	if op.BaseIsDir {
		return "" // regular file replaced a tracked directory: preserve it whole
	}
	if op.BaseLocalHash != "" {
		return op.BaseLocalHash
	}
	return "" // synced file base with no recorded local hash: anomalous, preserve whole
}

// relocatedRemoveBase mirrors removeBaseFor for the relocated-node delete paths
// (removeRelocatedNode via dropRemoveBaselineAt, and removeRelocatedPriorFile), which resolve
// their target through the byid index and so guard against the LIVE baseline at that path -
// the remove op's reconcile-time capture (BaseWasSynced/BaseLocalHash) describes op.Path's
// baseline, not this relocated one's, and unlike op.Path this baseline was never flipped by
// markPending, so its state is still meaningful. The outcomes are the same
// never-lose matrix:
//
//   - StateSynced with a recorded local hash → that hash is the divergence base (compare the
//     on-disk content against it; a match honours the server's delete).
//   - StateSynced but a DIRECTORY record (FileType) → empty base with ok=true: the caller only
//     reaches here for a non-directory occupant, so a regular file replaced the tracked
//     directory locally. The dir record's empty LocalHash is structural, not a synced fact
//     about the file, so it must not drive the server-authority branch below - the file is
//     preserved whole (mirroring removeBaseFor's BaseIsDir gate).
//   - StateSynced (file record) with no recorded local hash → empty base with ok=true: an
//     anomalous state (a StateSynced file baseline always carries a non-empty LocalHash -
//     markSynced derives one; the inbound-rename bare-write producers of an empty one are the
//     bare-record anomaly), so there is no trustworthy divergence base and the on-disk file is
//     preserved whole rather than treated as server authority and deleted (a safety net,
//     coordinated with removeBaseFor's identical never-lose direction). Where a genuine
//     server-authoritative delete of such a record is required, the caller instead DROPS the
//     baseline outright (dropRemoveBaselineAt with removeFile=false / the outbound
//     source-gone path) rather than routing through this gate.
//   - Anything else (a pending re-track left by a rejected rename, a conflict, or an absent
//     record) → empty base: the on-disk file is preserved whole, since a non-synced
//     LocalHash cannot be trusted as its own divergence base (it may describe server facts
//     ahead of disk, or unpushed on-disk content).
//
// Every branch now returns "preserve" (an empty base preserves the file whole via
// preserveDivergentLocal), so it mirrors removeBaseFor's single-return never-lose shape -
// only a synced file record with a recorded LocalHash yields a real divergence base to
// hash-check against.
func relocatedRemoveBase(b store.Baseline) (base string) {
	if b.State != store.StateSynced {
		return ""
	}
	if b.FileType == "dir" {
		return ""
	}
	if b.LocalHash != "" {
		return b.LocalHash
	}
	return "" // synced file record with no recorded local hash: anomalous, preserve whole
}

// preserveRelocatedDivergence runs the cluster-A keep-both guard for the relocated-node
// delete paths (dropRemoveBaselineAt, removeRelocatedPriorFile) ahead of their
// fsutil.Remove. A regular file is hash-checked against the relocated path's live
// baseline (relocatedRemoveBase) via preserveDivergentLocal. A DIRECTORY occupant runs
// the same subtree sweep as the primary guardRemoveDir: preserveDivergentLocal alone
// reads a directory as errUploadSourceGone - nothing to preserve - and the caller's
// fsutil.Remove (an os.RemoveAll) would then destroy locally edited or untracked
// descendants with no _Conflict copy. A preserved directory's stale descendant baselines
// are dropped so the next scan re-detects and re-uploads the relocated tree clean
// (mirroring guardRemoveDir). An Lstat miss (vanished, or a non-regular node) falls
// through to preserveDivergentLocal's no-content-to-lose handling.
func (e *Engine) preserveRelocatedDivergence(ctx context.Context, viewID uint64, rel, abs string, b store.Baseline) error {
	if fi, lerr := os.Lstat(abs); lerr == nil && fi.IsDir() {
		diverged, derr := e.subtreeHasDivergence(ctx, viewID, rel, abs)
		if derr != nil {
			return derr
		}
		if !diverged {
			return nil
		}
		if err := e.preserveAside(viewID, rel, abs); err != nil {
			return err
		}
		return e.dropDescendantBaselines(viewID, rel)
	}
	base := relocatedRemoveBase(b)
	if _, err := e.preserveDivergentLocal(viewID, rel, abs, base); err != nil {
		return err
	}
	return nil
}

// guardRemoveDir preserves a directory the server is deleting when its subtree holds any
// locally-diverged or untracked regular file, by moving the whole directory aside to a _Conflict
// copy (a no-replace rename + durable Conflict record) instead of recursively deleting it. The
// preserved tree is ordinary local content the next localScan re-detects and re-uploads, so no
// edited/untracked descendant is lost. A subtree that is fully in sync with its baselines is left
// untouched so the caller's recursive delete honours the server's authoritative removal.
func (e *Engine) guardRemoveDir(ctx context.Context, op store.Op, abs string) error {
	diverged, err := e.subtreeHasDivergence(ctx, op.ViewID, op.Path, abs)
	if err != nil {
		return err
	}
	if !diverged {
		return nil
	}
	if err := e.preserveAside(op.ViewID, op.Path, abs); err != nil {
		return err
	}
	// The whole subtree was moved aside under the _Conflict path, so the descendant
	// baselines still recorded under op.Path's old prefix now describe nothing on disk.
	// applyRemove drops op.Path's own baseline but not the descendants; leaving them would
	// make the next localScan diff each as a vanished file and emit a spurious outbound
	// OpRemove. Drop them here so the relocated subtree is re-detected and re-uploaded clean.
	return e.dropDescendantBaselines(op.ViewID, op.Path)
}

// subtreeHasDivergence reports whether any regular file under dirAbs (the on-disk location of the
// view-relative dirRel) holds content the server has not seen: an untracked file (no baseline), a
// descendant whose baseline is not StateSynced (a queued-but-unpushed local edit or an unresolved
// conflict, whose recorded LocalHash may be the on-disk content itself and so is no trustworthy
// divergence base - mirrors removeBaseFor's preserve-whole default for the single-file case), or a
// StateSynced tracked file whose on-disk hash differs from its baseline's last-synced local hash.
// Only a StateSynced FILE record carrying a non-empty LocalHash that matches the on-disk content is
// read as in sync; the two remaining synced shapes carry the same never-lose arcs every sibling gate
// has (removeBaseFor / relocatedRemoveBase), since the walk only ever reaches a REGULAR file:
//
//   - a synced DIRECTORY record (FileType) with a regular file on disk means the user replaced the
//     tracked directory with a file the server never saw; the dir record's LocalHash is empty
//     structurally, not evidence of a synced file, so it is divergence;
//   - a synced FILE record with an EMPTY LocalHash is the anomalous inbound-rename bare/promoted-
//     pending write, not a trustworthy base, so it is divergence too.
//
// Reading either as "clean" let a server delete of the ancestor directory destroy the file with no
// _Conflict copy and no record, and dropDescendantBaselines then erased the evidence. An unreadable
// subtree or file is treated conservatively as divergence so a remove cannot silently delete content
// it could not verify. The walk stops at the first divergence found.
func (e *Engine) subtreeHasDivergence(ctx context.Context, viewID uint64, dirRel, dirAbs string) (bool, error) {
	var found bool
	scanned := 0
	walkErr := filepath.WalkDir(dirAbs, func(p string, d fs.DirEntry, werr error) error {
		// The sweep is the same unbounded walk+hash class as scanView:
		// a clean delete of a large directory is its worst case (every descendant is hashed
		// before the sweep can conclude), and it runs on a drain worker, so it extends the
		// shutdown window into ordinary inbound removes. Cancellation surfaces as an ERROR,
		// never as "no divergence found": the caller must leave the op queued rather than
		// delete a subtree it did not finish verifying.
		//
		// Counted BEFORE the increment so the predicate is "every Nth entry starting with the
		// first" (scanned%N == 1 counted after would degenerate to "never check" at N == 1).
		// The batch bounds the STAT cost only; the hash below checks on its own, since a batch
		// of whole-file hashes is exactly the latency this guard exists to bound.
		if scanned%scanCtxCheckEvery == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		}
		scanned++
		if werr != nil {
			found = true // cannot verify this entry; do not risk a silent delete
			return filepath.SkipAll
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		rel, rerr := filepath.Rel(dirAbs, p)
		if rerr != nil {
			found = true
			return filepath.SkipAll
		}
		childRel := normpath.Normalize(path.Join(dirRel, filepath.ToSlash(rel)))
		b, gerr := e.store.GetBaseline(viewID, childRel)
		switch {
		case isNotFound(gerr):
			found = true // untracked descendant the server never saw
			return filepath.SkipAll
		case errors.Is(gerr, store.ErrCorruptRecord):
			// An undecodable descendant record cannot verify the on-disk content against a
			// synced base - the same "cannot verify, do not risk a silent delete" posture as
			// the neighbouring walk/hash-failure arms, so preserve the directory whole.
			// A strict error here retried the whole remove transiently
			// forever; on the preserve path the caller's dropDescendantBaselines then clears
			// (heals) the corrupt key.
			e.warnCorruptBaseline("subtree-divergence", viewID, childRel, gerr)
			found = true
			return filepath.SkipAll
		case gerr != nil:
			return fmt.Errorf("engine: load subtree baseline %d/%q: %w", viewID, childRel, gerr)
		case b.State != store.StateSynced:
			found = true // non-synced descendant: an unpushed local edit/conflict; preserve whole
			return filepath.SkipAll
		case b.FileType == "dir":
			// A regular file on disk under a synced DIRECTORY record: the user replaced the
			// tracked directory with a file the server never saw. The dir record's LocalHash is
			// empty structurally, so it is no synced fact about this file
			// (mirroring removeBaseFor's BaseIsDir arm).
			found = true
			return filepath.SkipAll
		case b.LocalHash == "":
			// Synced FILE record with no recorded local hash: either the inbound-rename
			// bare/promoted-pending write anomaly or one of the deliberate "re-verify on the
			// next scan" markers (revertRelocatedOutbound / resolveVanishedUpload clear the
			// local facts under StateSynced). Neither is a trustworthy divergence base, so
			// preserve whole rather than delete under a false server-authority read
			// (mirroring removeBaseFor / relocatedRemoveBase). The cost of
			// the marker case is one over-conservative subtree set-aside inside a scan
			// interval, which the next scan re-uploads clean - the never-lose direction.
			found = true
			return filepath.SkipAll
		}
		// About to read the whole file; see the batched check above for why this one is
		// unconditional.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if beforeHashHook != nil {
			beforeHashHook()
		}
		sum, herr := e.hashLocalFileMD4(p)
		if herr != nil {
			found = true // cannot hash (vanished/swapped/unreadable): do not risk a silent delete
			return filepath.SkipAll
		}
		if sum != b.LocalHash {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return false, walkErr
	}
	return found, nil
}

// removeRelocatedNode honours a delete for op's node at a path other than op.Path. A prior
// rejected rename re-tracks a moved node at its still-reachable source (resolveRejectedRename),
// so a later delete whose server path (op.Path) holds nothing would otherwise be acked while
// the node lives on at its relocated, byid-indexed location. It resolves the
// node by file id; if byid points somewhere other than op.Path, it removes that on-disk file
// and drops its baseline via dropRemoveBaselineAt (which preserves a synced record or a
// different file id at the path). Deleting op.Path's baseline above already removed op.FileID's
// byid entry when it lived there, so the common case resolves to not-found and is a no-op.
//
// Serialisation note (serialize.go "byid-resolved side effects"): the relocated path is NOT in
// this op's serializeKeys, so the scheduler does not order this delete against a concurrent op
// that owns it; the interleaving is direction-safe because dropRemoveBaselineAt runs the
// cluster-A keep-both guard before the on-disk remove.
func (e *Engine) removeRelocatedNode(ctx context.Context, op store.Op) error {
	if op.FileID == "" {
		return nil
	}
	path, err := e.store.PathByID(op.ViewID, op.FileID)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: resolve removed node %d/%q: %w", op.ViewID, op.FileID, err)
	}
	if path == op.Path {
		return nil // already handled at op.Path
	}
	// matchedByID: path was resolved through byid for op.FileID, so it holds exactly the
	// node the server deleted - honour the delete even if its baseline is StateSynced. A
	// rejected move can leave the node synced at a path the delete never names (the server
	// moved it, we did not); without this the synced guard below would refuse to remove it
	// and the file plus baseline would survive the delete forever.
	return e.dropRemoveBaselineAt(ctx, op, path, true, true)
}

// removeRelocatedPriorFile deletes the stale on-disk file a node left at a previously
// tracked path once an apply has successfully landed the same node (by file id) at op.Path.
// A rejected rename re-tracks a moved node at its still-reachable source while the server
// path stays unappliable (resolveRejectedRename); when that path later becomes appliable
// and the download succeeds at op.Path, markSynced re-points the byid index to op.Path and
// drops the old baseline - but the original file still sits at the prior path, untracked.
// A later server delete targets only op.Path and never reaches it, so the orphan would
// survive forever. Remove it here, before markSynced re-points the index.
//
// The byid index guarantees the prior path's baseline carries op.FileID (PutBaseline keeps
// them consistent), so the file removed is unambiguously this node's own stale copy, never a
// foreign node's. Must run before markSynced, while byid still points at the prior path. It
// is a no-op when the node is untracked, already tracked at op.Path (the normal case and any
// replay), or when the prior path no longer resolves safely (escapes the root or sits behind
// a symlink) - that anomaly is left to the periodic local scan (Phase 6c) rather than forcing a
// remove through an unsafe path.
//
// Serialisation note (serialize.go "byid-resolved side effects"): the prior path is NOT in this
// op's serializeKeys, so the scheduler does not order this delete against a concurrent op that
// owns it; the interleaving is direction-safe because the preserveRelocatedDivergence guard
// below sets aside any divergent/untracked content as a _Conflict copy before the remove.
func (e *Engine) removeRelocatedPriorFile(ctx context.Context, op store.Op) error {
	if op.FileID == "" {
		return nil
	}
	prev, err := e.store.PathByID(op.ViewID, op.FileID)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: resolve relocated node %d/%q: %w", op.ViewID, op.FileID, err)
	}
	if prev == op.Path {
		return nil
	}
	abs, err := e.abs(op.ViewID, prev)
	if err != nil {
		return nil // prior path no longer safe to touch; leave it for the local scan (6c)
	}
	// Resolve the real on-disk spelling before the guard and the remove, mirroring applyRemove:
	// on a byte-preserving filesystem the prior copy may sit under a
	// non-NFC (NFD) name, and a bare NFC abs would Lstat-miss it - the Remove below would
	// no-op and the stale file would survive untracked, re-uploaded by the next scan (a
	// delete/re-create ping-pong). A resolution behind a symlink is unsafe to touch; leave it
	// for the local scan, the same contract as e.abs rejecting the path above.
	abs, err = e.onDiskAbsNoSymlink(abs)
	if err != nil {
		return nil //nolint:nilerr // intentional: unsafe resolved path is left to the local scan
	}
	// Cluster-A keep-both guard: the stale copy may have been edited at its
	// re-tracked path since the last sync - removing it by byid identity alone would destroy
	// that edit with no _Conflict copy. Run the same funnel as the primary
	// guardRemoveDivergence/guardRemoveDir paths, with a file's divergence base resolved from
	// the prior path's live baseline (relocatedRemoveBase): divergent or unverifiable content
	// (including a directory that took the path, swept like guardRemoveDir) is set aside first
	// and the Remove below no-ops on the vacated path.
	b, gerr := e.store.GetBaseline(op.ViewID, prev)
	if errors.Is(gerr, store.ErrCorruptRecord) {
		// An undecodable record supplies no trustworthy divergence base; the zero Baseline's
		// non-synced state makes preserveRelocatedDivergence set the stale copy aside whole -
		// the never-lose direction - instead of the strict read wedging the apply forever.
		// markSynced's tolerant Put at op.Path then re-points byid and
		// its drop-moved-orphan sweep clears the corrupt key.
		e.warnCorruptBaseline("relocated-prior", op.ViewID, prev, gerr)
		b, gerr = store.Baseline{}, nil
	}
	if gerr != nil && !isNotFound(gerr) {
		return fmt.Errorf("engine: load relocated prior baseline %d/%q: %w", op.ViewID, prev, gerr)
	}
	// An absent record (unreachable - the byid index guarantees one at prev) decodes as a zero
	// Baseline, whose non-synced state preserves any present regular file whole: the safe default.
	if perr := e.preserveRelocatedDivergence(ctx, op.ViewID, prev, abs, b); perr != nil {
		return perr
	}
	if err := removeNode(abs); err != nil {
		return fmt.Errorf("engine: remove relocated stale file %d/%q: %w", op.ViewID, prev, err)
	}
	return nil
}
