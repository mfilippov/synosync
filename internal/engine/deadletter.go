package engine

// This file carries the executor's dead-letter reconcile helpers: when an op is
// dropped rather than applied (a structurally invalid path, a pre-existing symlink
// along its on-disk location, or a doomed rename whose feeder was itself dropped),
// these helpers resolve the baseline reconcile pre-wrote so a dropped op leaves
// neither a stranded byid entry nor an orphaned on-disk file. They are disk- and
// store-only; the scheduler/dispatcher that decides to dead-letter lives in
// executor.go.

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/mfilippov/synosync/internal/store"
)

// deadLetterRecord builds the durable trace for a dropped op.
func deadLetterRecord(op store.Op, reason string, recordedAt int64) store.DeadLetter {
	return store.DeadLetter{
		Type:       op.Type,
		Direction:  op.Direction,
		ViewID:     op.ViewID,
		Path:       op.Path,
		NewPath:    op.NewPath,
		FileID:     op.FileID,
		Reason:     reason,
		RecordedAt: recordedAt,
	}
}

// deadLetterAndAck records a durable trace of a dropped op and acks its queue record in one
// atomic store transaction, so a retry can never write the trace twice. It is the
// single dead-letter primitive for every permanent-drop site - the inbound executor paths and
// the outbound/keep-both apply paths alike. It MUST
// be called only after any baseline cleanup the drop needs has already succeeded: the trace
// marks the op resolved and the atomic ack removes it from the queue, so writing the trace
// before a cleanup that then fails would leave the op queued and re-traced on the next
// drain. On a store failure the op is left queued (no trace written, the transaction is
// atomic) and retried - the cleanup helpers are idempotent, so re-running is safe.
func (e *Engine) deadLetterAndAck(op store.Op, seq uint64, reason string) error {
	return e.deadLetterCompensateAndAck(op, seq, reason, nil, nil)
}

// deadLetterCompensateAndAck is deadLetterAndAck with a compensating op enqueued in the SAME
// store transaction as the trace and the Ack. A drop that leaves work undone needs one: the
// inbound rename whose moved node is absent from disk used to be dropped outright, on the
// promise that "a subsequent modify event (or the 6c full-catalog pull) will materialise the
// node" - a compensation the two-client stand proved never arrives (finding H2), because
// nothing touches that node again, the cold start does not re-run and the server's event log
// compacts the move row away. Enqueuing the replacement inside the trace+ack transaction keeps
// the drop atomic in both directions: the op cannot leave the queue without its replacement,
// and a retried drop cannot enqueue the replacement twice.
//
// track carries the compensation's own StatePending record into the same transaction, for the
// same indivisibility: written before the commit it survives a drop that then fails, and the
// byid-indexed claim it leaves at the rename's destination makes the replayed rename read its
// never-materialised node as tracked - disarming exactly the foreign-source guards that
// produced the drop. Written after, a crash leaves the repair queued with its gate unarmed.
func (e *Engine) deadLetterCompensateAndAck(op store.Op, seq uint64, reason string,
	compensation *store.Op, track *store.TrackedBaseline) error {
	if _, err := recordDeadLetterAndAck(e, deadLetterRecord(op, reason, e.now().Unix()), compensation, track, seq); err != nil {
		return fmt.Errorf("engine: dead-letter and ack %d: %w", seq, err)
	}
	return nil
}

// recordDeadLetterAndAck is the single store write deadLetterAndAck performs, behind a seam so
// a test can inject a store-level failure (e.g. ENOSPC on a full disk) into every dead-letter
// drop site without a store interface - mirroring the cleanupRejected/dropDoomedRename seams.
// It verifies the ENOSPC latch fires on the deadLetterAndAck cleanup arms too.
var recordDeadLetterAndAck = func(e *Engine, dl store.DeadLetter, comp *store.Op,
	track *store.TrackedBaseline, seq uint64) (uint64, error) {
	return e.store.RecordDeadLetterAndAck(dl, seq, comp, track)
}

// resolveRejectedRename resolves the baseline of a rename the executor refused because one
// of its endpoints is unappliable (a pre-existing symlink along the source or destination).
// The on-disk move never ran, so the moved node's file - if it was ever materialised - is
// still wherever it sat before this op, and dropping its baseline outright (as the
// download/mkdir cleanup does) would orphan that still-present file. So: re-track the
// baseline (pending) at the path that still holds the file on disk; only when no file is
// reachable anywhere is the record a genuine phantom and dropped. A synced record is never
// disturbed: it is proof a real apply placed the node.
//
// The file can be at one of two reachable paths, and which one depends on what reconcile did
// this batch:
//   - op.Path, the rename's source. The common case: reconcile's relocatePending carried the
//     baseline from op.Path to the (rejected) destination, so PathByID now points at the
//     destination while the file never left the source.
//   - the path PathByID already tracks the node at. When a prior rejected rename re-tracked
//     this same node at its still-reachable location, this batch's relocatePending found no
//     baseline at op.FromPath (it lives elsewhere) and left it in place - so op.Path is the
//     stale server source (often the symlink that got the op rejected) and the file is at the
//     tracked path. Checking only op.Path here deleted that tracked baseline and orphaned the
//     real file (e.g. move a->link/b rejected re-tracks f at a, then link/b->c is
//     rejected and must not drop the baseline at a).
//
// Re-track at the first reachable candidate that holds a file (op.Path first, preserving the
// common case's behaviour); abs rejects a candidate whose path escapes or holds a symlink, so
// such a candidate is skipped, not materialised.
func (e *Engine) resolveRejectedRename(op store.Op) error {
	if op.FileID == "" {
		return nil
	}
	path, err := e.store.PathByID(op.ViewID, op.FileID)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: resolve rejected rename node %d/%q: %w", op.ViewID, op.FileID, err)
	}
	b, err := e.store.GetBaseline(op.ViewID, path)
	if isNotFound(err) {
		return nil
	}
	if errors.Is(err, store.ErrCorruptRecord) {
		// An undecodable record at the byid-indexed path: the index itself proves it is this
		// node's (PutBaseline keeps them consistent), so rebuild a minimal record from the op's
		// id and let the resolution below run - the re-track Put or the final Delete heals the
		// key - rather than the strict read wedging the dead-letter cleanup that exists to
		// unwedge the queue. The unknown FileType skips the directory
		// descendant carry-back/drop (fail-safe: stale-prefix records are swept protected by
		// the scan-side tolerance, never wrongly relocated).
		e.warnCorruptBaseline("rejected-rename", op.ViewID, path, err)
		b, err = store.Baseline{FileID: op.FileID}, nil
	}
	if err != nil {
		return fmt.Errorf("engine: load rejected rename baseline %d/%q: %w", op.ViewID, path, err)
	}
	if b.FileID != op.FileID || b.State == store.StateSynced {
		return nil
	}
	for _, candidate := range []string{op.Path, path} {
		abs, absErr := e.abs(op.ViewID, candidate)
		if absErr != nil {
			continue
		}
		// Resolve the candidate's real on-disk spelling before probing existence: on a byte-
		// preserving filesystem the file may sit under a non-NFC (NFD) name while candidate is the
		// canonical NFC key, so a bare NFC Lstat would miss it, skip the candidate, and - finding no
		// reachable file at either candidate - drop the baseline of a live file as a phantom, which
		// the next scan re-uploads as a duplicate (mirrors applyRemove). A
		// resolution landing on a symlink component is unsafe to re-track, so skip the candidate.
		abs, absErr = e.onDiskAbsNoSymlink(abs)
		if absErr != nil {
			continue
		}
		if _, statErr := os.Lstat(abs); statErr != nil {
			continue
		}
		// Never re-track the moved node on top of a *different* tracked node. op.Path is the
		// server's stale rename source, and a different node may now occupy it on disk (its own
		// pending/synced baseline). The Lstat above only proves something exists there, not that
		// it is our file id's bytes; re-tracking b at op.Path anyway would overwrite that foreign
		// baseline and, through the byid index, drop our node's actual indexed record - binding
		// our file id to unrelated bytes. Skip such a candidate so we fall through to
		// the indexed path, which holds our node by construction (b.FileID == op.FileID checked
		// above). The indexed path itself never needs this guard for the same reason.
		if candidate != path {
			existing, exErr := e.store.GetBaseline(op.ViewID, candidate)
			switch {
			case isNotFound(exErr):
				// No record here - the candidate is free for our node.
			case errors.Is(exErr, store.ErrCorruptRecord):
				// An undecodable occupant cannot be proven to be our node, so the candidate is
				// skipped like a foreign one (the conservative direction, mirroring
				// relocateDescendantBaselines' corrupt-destination skip) rather than the strict
				// read wedging the cleanup. The fall-through to the indexed
				// path - ours by construction - still re-tracks the node.
				e.warnCorruptBaseline("rejected-rename-source", op.ViewID, candidate, exErr)
				continue
			case exErr != nil:
				return fmt.Errorf("engine: inspect rejected rename source %d/%q: %w", op.ViewID, candidate, exErr)
			case existing.FileID != op.FileID:
				// A different node holds this path. "Different" includes an empty file id: an
				// empty id is a real local-only/unindexed baseline (e.g. a create event the
				// server gave no id), not a free path. Our node's id is non-empty (guarded at
				// the top of this func), so this catches both a foreign non-empty id and an
				// empty one; only a genuinely absent record (isNotFound) is free to claim.
				// Overwriting a foreign baseline would, through the byid index, drop our node's
				// real indexed record and bind our file id to unrelated bytes.
				continue
			}
		}
		// For a rejected directory move, reconcile's relocatePending rebased the whole subtree's
		// descendant baselines to the (rejected) destination prefix before the drain ran, but the
		// on-disk move never happened - the descendant files are still under the source. When we
		// re-home the root back at op.Path (the rename source), carry the descendant baselines back
		// too, or they stay stranded at the destination while their files sit at the source: a later
		// child delete (which the server names under the destination) would then resolve through the
		// stranded index, target the empty destination, and leave the real source file behind.
		// Reverse from `path` - the root's CURRENT indexed location - not op.NewPath:
		// a same-batch multi-hop move (a->link/b->c, both folded by reconcile before the drain)
		// leaves the root indexed at the final hop c with its descendants under c, not under this
		// op's immediate destination link/b. Reversing from op.NewPath would scan the wrong prefix
		// and leave the descendants stranded at c (the multi-hop case); `path` is the prefix the
		// descendants actually sit under in every case (single-hop: path == op.NewPath). This
		// mirrors relocatePending's forward rebase in reverse, and only fires when re-homing at
		// op.Path (the source); the destination candidate's files are unreachable behind the
		// symlink, so there is nothing to carry back there. relocateDescendantBaselines is
		// idempotent and guarded, so a non-directory node or an already-reversed/empty subtree is a
		// no-op. Run it before the root Put, so the root record is the last one moved - the same
		// crash-replay ordering relocatePending uses.
		if b.FileType == "dir" && candidate == op.Path {
			if err := e.relocateDescendantBaselines(op.ViewID, path, op.Path); err != nil {
				return err
			}
		}
		// Re-track at this candidate as pending, not synced: the executor cannot prove the
		// on-disk bytes match b's recorded facts, because a same-batch modify of the moved
		// node (move a->link/b, then modify link/b - both rejected at the symlink) advances
		// b's hash/mtime past what the untouched file still holds. Stamping synced would
		// attach the newer content's facts to the stale bytes and read as truth;
		// pending keeps the node tracked (no orphan) rather than orphaning a live file.
		// PutBaseline re-points the byid index and drops any orphan record at the node's prior
		// path, so the node is tracked only where its file actually is.
		//
		// This record is NOT re-verified by the next local scan (contrary to an
		// earlier claim that it would be): localReconcile drives only StateSynced
		// baselines - a StatePending record is skipped as owned by the inbound/conflict paths
		// - and this dead-letter left no queued op behind to own it. So the re-tracked path
		// stays out of OUTBOUND sync until something else resolves it: a later server event
		// for the node (which reconciles against the record and can advance it), or the
		// operator clearing the symlink so the next inbound apply lands. That is the
		// conservative direction - the file and its tracking both survive, nothing is deleted
		// or clobbered - but it is a freeze, and the accepted residual is recorded in
		// DESIGN §13. It is not repaired here by stamping synced (that would assert an
		// agreement the executor cannot prove, and the bare/promoted-synced shapes are exactly
		// what the never-lose gates must distrust) nor by re-enqueuing an outbound upload
		// (the path is unappliable - the symlink that rejected the rename is still there -
		// so the upload would dead-letter in turn and churn a Dropped trace every drain).
		b.State = store.StatePending
		if err := e.store.PutBaseline(op.ViewID, candidate, b); err != nil {
			return fmt.Errorf("engine: re-track rejected rename node %d/%q: %w", op.ViewID, candidate, err)
		}
		return nil
	}
	// No candidate held a reachable file: the moved node is a genuine phantom. For a
	// directory that makes its whole subtree phantom too - reconcile's relocatePending
	// rebased the descendant baselines to `path` (the root's current indexed prefix), but
	// the on-disk move never ran and the root is reachable nowhere, so `path` does not exist
	// on disk and the descendants under it are byid entries pointing at files that are not
	// there. Dropping only the root would strand them: no op resolves a baseline under a
	// directory that does not exist (the multi-hop a->link/b->c-then-recreate-a case, where
	// the source candidate is occupied by the recreated node and the destination is
	// unreachable). Drop the subtree before the root so nothing is left behind.
	if b.FileType == "dir" {
		if err := e.dropDescendantBaselines(op.ViewID, path); err != nil {
			return err
		}
	}
	if err := e.store.DeleteBaseline(op.ViewID, path); err != nil {
		return fmt.Errorf("engine: drop rejected rename baseline %d/%q: %w", op.ViewID, path, err)
	}
	return nil
}

// dropDescendantBaselines deletes every baseline recorded under dir's prefix (dir + "/").
// It is used when a directory's root baseline is dropped as a genuine phantom - its node is
// reachable on disk nowhere - so the descendant baselines reconcile rebased to the same
// (unreachable) prefix are phantoms too: their byid entries point under a directory that
// does not exist on disk, where no future op will resolve them. It also drops a CORRUPT
// (undecodable) descendant record under the prefix: every caller drops the
// subtree's records because the subtree is gone or preserved aside, so a corrupt one describes
// nothing either - and the materialising BaselinesForView silently omits it, which used to
// leave the rotted key fossilised under the dead prefix forever. The scan streams first and
// mutates after (the prefix cursor is live in a read transaction, and DeleteBaseline removes an
// undecodable value anyway - the heal). The stream is bounded to the subtree by the store's
// cursor prefix, so a drop decodes the subtree's records rather than the
// whole view's; skipped therefore already carries only paths under the prefix.
func (e *Engine) dropDescendantBaselines(viewID uint64, dir string) error {
	var paths []string
	skipped, err := e.scanBaselinePrefix(viewID, dir, func(pb store.PathBaseline) error {
		paths = append(paths, pb.Path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine: list baselines to drop %d/%q: %w", viewID, dir, err)
	}
	for _, p := range skipped {
		e.warnCorruptBaseline("drop-descendants", viewID, p, store.ErrCorruptRecord)
		paths = append(paths, p)
	}
	// One transaction per CHUNK, for the same reason the relocation batches: a directory the
	// server deleted retires one baseline per descendant, each an fsync'd commit, and this runs
	// on the sync loop's own goroutine. A rolled-back chunk is simply not dropped yet - the op
	// stays queued and the retry re-lists what is left under the prefix.
	for start := 0; start < len(paths); start += subtreeBaselineBatch {
		chunk := paths[start:min(start+subtreeBaselineBatch, len(paths))]
		if err := e.batchStore(func(b *store.Batch) error {
			for _, p := range chunk {
				if err := b.DeleteBaseline(viewID, p); err != nil {
					return fmt.Errorf("engine: drop descendant baseline %d/%q: %w", viewID, p, err)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// subtreeBaselineBatch is how many descendant baselines share one durable transaction when a
// directory move or delete retires or relocates a whole subtree.
//
// The size matches coldStartEnqueueBatch for the same trade: large enough to amortise the fsync
// over hundreds of records instead of paying one each, small enough that a failed chunk is cheap
// to re-derive. Unlike cold start these sweeps run EAGERLY inside reconcile, on the goroutine
// that also owns the poll and the drain, so the commits they used to pay per record stalled the
// whole sync cycle for the length of the subtree.
const subtreeBaselineBatch = coldStartEnqueueBatch

// dropRejectedRemoveBaseline resolves the baseline of a remove the executor refused because its
// path is unappliable (a pre-existing symlink along it). The op is acked (dead-lettered) so it
// cannot wedge the queue, but its target node is gone server-side (the op came from a delete
// event), so leaving a tracked record behind would strand it forever with no queued op to
// resolve it. It cleans up in two places:
//
//   - At op.Path (the server's path for the node): drop the pending record this remove targeted,
//     but only it. The node is unreachable locally there (behind the symlink), so there is
//     nothing on disk to remove. A baseline now carrying a different file id, or one already
//     synced (a later create that took the path), is a different node and is preserved.
//   - At the node's actually-reachable path, resolved through the byid index by the stable file
//     id. A prior rejected rename (move a->link/b refused at the symlink) re-tracks the moved
//     node at its still-reachable source a, so the delete event's link/b path misses it entirely.
//     Honour the server delete at that reachable path: remove the on-disk file and drop its
//     baseline. Without this the re-tracked node and its file would survive the server delete
//     permanently. The node is identified by file id alone - no content/hash comparison is
//     needed, so this is not the deferred (6c) local-divergence handling.
func (e *Engine) dropRejectedRemoveBaseline(ctx context.Context, op store.Op) error {
	if err := e.dropRemoveBaselineAt(ctx, op, op.Path, false, false); err != nil {
		return err
	}
	// A dead-lettered DIRECTORY remove leaves the directory in place on disk (the symlink
	// blocked the apply), but reconcile may have pre-written phantom StatePending descendant
	// baselines under it - e.g. a superseded id-less download the byid-keyed
	// dropStrandedBaseline could not resolve, or any other never-materialised
	// descendant reconcile relocated below the prefix. applyRemove's success path drops the
	// whole subtree's baselines; the dead-letter path dropped only op.Path,
	// stranding those phantoms StatePending forever - localReconcile drives only StateSynced,
	// so the path silently falls out of outbound sync. Drop only the
	// phantom (non-synced, unmaterialised) descendants here; a synced descendant or one with a
	// live on-disk file of its kind is preserved so no real file is orphaned. A file op.Path
	// has no descendants, so this is a no-op for it.
	if err := e.dropPhantomDescendantBaselines(op.ViewID, op.Path); err != nil {
		return err
	}
	// Resolve the node by its stable file id. If it is tracked at a path other than the one the
	// delete named, that is where a rejected rename left it reachable on disk - honour the delete
	// there too, removing the file as well as the baseline (shared with applyRemove's success
	// path, which faces the same relocated-node case once op.Path becomes valid again).
	return e.removeRelocatedNode(ctx, op)
}

// dropPhantomDescendantBaselines drops every NON-SYNCED (phantom) descendant baseline under
// dir's prefix (dir + "/") whose node is not materialised on disk. It is the dead-letter mirror
// of applyRemove's success-path dropDescendantBaselines, for the case where a directory OpRemove
// is dead-lettered (a pre-existing symlink along its on-disk path) rather than applied: the
// directory was NOT removed, so - unlike the success path - a descendant's real on-disk file may
// still be present and must never be orphaned. So a descendant is dropped only when it is
// non-synced (a StatePending record reconcile pre-wrote; a synced record is proof a real apply
// placed the node and is authoritative) AND no on-disk entity of its own kind sits at its path
// (mirroring dropStrandedBaseline's onDiskIsNodeKind guard). A corrupt (undecodable) descendant
// is left alone: its state/kind are unknowable, so it cannot be proven a phantom, and the
// scan-side tolerance already protects it. The scan streams first and mutates after (the prefix
// cursor is live in a read transaction) and is bounded to the subtree by the store's cursor
// prefix rather than filtering a whole-view stream in Go.
func (e *Engine) dropPhantomDescendantBaselines(viewID uint64, dir string) error {
	type phantom struct {
		path     string
		fileType string
	}
	var phantoms []phantom
	if _, err := e.scanBaselinePrefix(viewID, dir, func(pb store.PathBaseline) error {
		if pb.State != store.StateSynced {
			phantoms = append(phantoms, phantom{path: pb.Path, fileType: pb.FileType})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("engine: scan phantom descendants %d/%q: %w", viewID, dir, err)
	}
	for _, ph := range phantoms {
		if abs, absErr := e.abs(viewID, ph.path); absErr == nil {
			// Resolve the real on-disk (possibly NFD) spelling before probing, mirroring
			// dropStrandedBaseline / dropRemoveBaselineAt: a bare NFC Lstat
			// would miss a live descendant materialised under its decomposed name, read it as a
			// phantom, and drop its baseline - orphaning the file for the next scan to re-upload
			// as a duplicate. A symlink-component resolution (onDiskAbsNoSymlink error) falls
			// back to the literal abs, which stays correctly droppable.
			if resolved, resErr := e.onDiskAbsNoSymlink(abs); resErr == nil {
				abs = resolved
			}
			if fi, statErr := os.Lstat(abs); statErr == nil && onDiskIsNodeKind(fi, ph.fileType) {
				// A real on-disk entity of the node's kind sits here: not a phantom, preserve it.
				continue
			}
		}
		if err := e.store.DeleteBaseline(viewID, ph.path); err != nil {
			return fmt.Errorf("engine: drop phantom descendant %d/%q: %w", viewID, ph.path, err)
		}
	}
	return nil
}

// dropRemoveBaselineAt drops the baseline at path for a dead-lettered remove of op's node,
// optionally removing the on-disk file first. It is a no-op unless a baseline for op.FileID
// actually sits there: a different file id (a node that took the path) is always preserved.
// A synced record (proof a real apply placed the node) is preserved too UNLESS matchedByID -
// when path was resolved through byid for op.FileID, it holds exactly the node the server
// deleted, so the delete is honoured regardless of state; a path-keyed lookup
// (matchedByID false) keeps the synced guard, since the server's delete path may unappliably
// name a node a real apply legitimately placed. When removeFile is set the node is reachable on
// disk (a rejected rename re-tracked it), so the file is deleted to honour the server delete;
// abs rejecting the path (an escape or symlink) leaves nothing safely removable, so only the
// stale baseline is dropped.
//
// Serialisation note (serialize.go "byid-resolved side effects"): when path is byid-resolved
// (matchedByID / a relocated node), it is NOT in the calling op's serializeKeys, so the
// scheduler does not order this delete against a concurrent op that owns it; the interleaving is
// direction-safe because the preserveRelocatedDivergence guard sets aside divergent/untracked
// content as a _Conflict copy before the on-disk remove.
func (e *Engine) dropRemoveBaselineAt(ctx context.Context, op store.Op, path string, removeFile, matchedByID bool) error {
	b, err := e.store.GetBaseline(op.ViewID, path)
	if isNotFound(err) {
		return nil
	}
	corrupt := errors.Is(err, store.ErrCorruptRecord)
	if corrupt {
		// An undecodable record: complete the cleanup with the guards below skipped - they
		// protect a decodably-different or synced node, neither of which a corrupt value can
		// prove, and a corrupt record provides no tracking value either way (the scan protects
		// it, every apply read treats it absent). The tolerant DeleteBaseline below removes the
		// value anyway (the heal); with removeFile the zero Baseline's non-synced state makes
		// preserveRelocatedDivergence set any on-disk content aside whole first (never-lose).
		// Deleting a record that hypothetically belonged to a live synced node only makes its
		// path untracked - the next scan re-uploads and the server-side gate converges it -
		// whereas the strict read wedged the dead-letter cleanup forever.
		e.warnCorruptBaseline("rejected-remove", op.ViewID, path, err)
		b, err = store.Baseline{}, nil
	}
	if err != nil {
		return fmt.Errorf("engine: load rejected remove baseline %d/%q: %w", op.ViewID, path, err)
	}
	if !corrupt && !matchedByID && b.State == store.StateSynced {
		return nil
	}
	cleared := false
	if !corrupt && op.FileID != "" && b.FileID != op.FileID {
		// A different node has taken the path (a same-batch delete-then-create): its pending
		// baseline belongs to the create's own download, not this remove. Leave it.
		return nil
	}
	if removeFile {
		abs, absErr := e.abs(op.ViewID, path)
		if absErr == nil {
			// Resolve the real on-disk spelling before the guard and the remove, mirroring
			// applyRemove: on a byte-preserving filesystem the relocated
			// node may sit under a non-NFC (NFD) name, and a bare NFC abs would Lstat-miss it -
			// the Remove below would no-op, the baseline drop would orphan the file untracked,
			// and the next scan would re-upload it (a delete/re-create ping-pong). A resolution
			// that lands on a symlink component leaves nothing safely removable, so only the
			// stale baseline is dropped - the same contract as e.abs rejecting the path.
			abs, absErr = e.onDiskAbsNoSymlink(abs)
		}
		if absErr == nil {
			// Cluster-A keep-both guard: the relocated file may hold a local
			// edit the server never saw - this path deletes by byid identity without the primary
			// path's hash-vs-base check (guardRemoveDivergence), so run the same funnel first
			// (preserveRelocatedDivergence), with a file's divergence base resolved from the
			// LIVE baseline at the relocated path (relocatedRemoveBase): content that diverges
			// from a synced base, or cannot be verified against one - including a directory,
			// swept like guardRemoveDir, or a synced file record with an anomalous empty hash
			// - is set aside as a _Conflict copy and the Remove below no-ops on the
			// vacated path. A synced record with a recorded hash matching on disk keeps the
			// server-authoritative delete.
			if perr := e.preserveRelocatedDivergence(ctx, op.ViewID, path, abs, b); perr != nil {
				return perr
			}
			// Through the removeNode seam, like the other two subtree deletes: this one runs
			// on the LIVE apply path too (removeRelocatedNode calls it under OpRemove), so it
			// can lose the same os.RemoveAll create race, and the classifier's verb gate must
			// be drivable here as well.
			if err := removeNode(abs); err != nil {
				return fmt.Errorf("engine: remove relocated deleted node %d/%q: %w", op.ViewID, path, err)
			}
			cleared = true
		}
	}
	// The on-disk subtree is gone (removed, or moved wholesale into a _Conflict copy by the
	// guard above), so the descendant baselines under it describe files that are no longer
	// where they say. Dropping only the ROOT left them behind as StateSynced records with
	// nothing on disk, and the next scan diffed each one as a vanished file: an outbound
	// OpRemove per descendant, every one of them against a node the server's own RECURSIVE
	// delete had already taken, each paced through the absence-corroboration streak before it
	// could be resolved. That is precisely the churn applyRemove's own descendant drop exists
	// to prevent; this path simply never did it.
	//
	// Conditioned on having actually cleared the path. When the path could not be resolved -
	// it escapes the root, or sits behind a symlink - nothing was removed and the files may
	// well still be there, so dropping their records would make a live subtree untracked and
	// hand the next scan a re-upload of the whole thing.
	if cleared {
		if err := e.dropDescendantBaselines(op.ViewID, path); err != nil {
			return err
		}
	}
	if err := e.store.DeleteBaseline(op.ViewID, path); err != nil {
		return fmt.Errorf("engine: drop rejected remove baseline %d/%q: %w", op.ViewID, path, err)
	}
	return nil
}

// dropStrandedBaseline removes the pending baseline reconcile relocated for a node whose
// materialisation never happened - a doomed rename whose feeder was dead-lettered. Reconcile
// moves a node's baseline to its destination ahead of the apply, so when the apply is dropped
// the record is left behind, indexed in byid, describing a node that does not exist on disk.
// A later move (in this batch or a future one) would resolve the moved node through that stale
// index, conclude it is "tracked", skip the foreign-source guard, and drag unrelated bytes onto
// the destination. The node is resolved through byid because reconcile may
// have relocated it past the dropped op's own paths (a same-batch a->b->c chain leaves it at c);
// only a still-pending record is dropped, since a synced baseline is proof a real apply placed
// the node and is therefore authoritative. An empty file id, an untracked node, or an already
// synced record is a no-op.
//
// A pending record is also preserved when a real entity OF THE NODE'S OWN KIND sits at its
// resolved path: that node IS materialised on disk, so it is not a phantom. This matters when a
// rejected-rename cleanup has re-tracked a still-present source file as pending and a sibling op's
// dead-letter then runs this drop for the same file id - dropping it would orphan a real on-disk
// file. The on-disk kind must match the baseline's recorded type, not merely exist: a
// file download that dead-lettered because an untracked DIRECTORY occupies its target path
// (os.Rename → fs.ErrExist, the file-over-dir route in executor.go) leaves that blocking directory
// on disk, but the directory is the OBSTACLE, not our (file) node - it was never materialised. A
// bare "something exists" check would mistake the obstacle for the node and keep the phantom file
// baseline indexed over it; a later same-id server rename would then see nodeTracked==true and drag
// the untracked directory across the tree, mislabelling it as the server file (corruption). So a
// type-mismatched occupant is treated as a phantom and dropped, exactly like an absent one. When
// the path cannot be resolved on disk (it escapes or sits behind a symlink), the node was never
// materialisable there, so it is also dropped.
func (e *Engine) dropStrandedBaseline(viewID uint64, fileID string) error {
	if fileID == "" {
		return nil
	}
	path, err := e.store.PathByID(viewID, fileID)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: resolve stranded node %d/%q: %w", viewID, fileID, err)
	}
	b, err := e.store.GetBaseline(viewID, path)
	if isNotFound(err) {
		return nil
	}
	if errors.Is(err, store.ErrCorruptRecord) {
		// An undecodable record at the byid-indexed path is this node's by index consistency,
		// but its state/kind are unknowable, so neither the synced guard nor the on-disk-kind
		// preserve below can be evaluated. Drop it (the tolerant DeleteBaseline removes the
		// value anyway - the heal): a corrupt record tracks nothing (the scan protects it,
		// apply reads treat it absent), so if the node IS materialised on disk the untracked
		// file simply re-diffs on the next scan and converges through the server-side gates -
		// whereas the strict read wedged the dead-letter cleanup forever.
		e.warnCorruptBaseline("stranded", viewID, path, err)
		if derr := e.store.DeleteBaseline(viewID, path); derr != nil {
			return fmt.Errorf("engine: drop corrupt stranded baseline %d/%q: %w", viewID, path, derr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: load stranded baseline %d/%q: %w", viewID, path, err)
	}
	if b.FileID != fileID || b.State == store.StateSynced {
		return nil
	}
	if abs, absErr := e.abs(viewID, path); absErr == nil {
		// Resolve the real on-disk spelling before probing existence, mirroring
		// dropRemoveBaselineAt / applyRemove and resolveRejectedRename:
		// on a byte-preserving filesystem the node may be materialised under a
		// non-NFC (NFD) name while path is the canonical NFC key, so a bare NFC Lstat would
		// miss the live file, read it as a phantom, and drop the baseline - the next scan then
		// re-uploads the orphaned file as a duplicate server node. The rejected-rename recovery
		// GUARANTEES such NFD-backed pending records exist (resolveRejectedRename re-tracks a live
		// NFD source StatePending under the NFC key), so without this resolution the two fixes are
		// actively inconsistent and the miss is reachable. A resolution landing on a symlink
		// component (onDiskAbsNoSymlink error) falls back to the literal abs - the conservative
		// preserve direction is unavailable there, but a symlinked node is never a materialised
		// node of the recorded kind, so it stays correctly droppable.
		if resolved, resErr := e.onDiskAbsNoSymlink(abs); resErr == nil {
			abs = resolved
		}
		if fi, statErr := os.Lstat(abs); statErr == nil && onDiskIsNodeKind(fi, b.FileType) {
			// A real entity of the node's own kind is on disk at this path: the node is
			// materialised, not a phantom.
			return nil
		}
	}
	if err := e.store.DeleteBaseline(viewID, path); err != nil {
		return fmt.Errorf("engine: drop stranded baseline %d/%q: %w", viewID, path, err)
	}
	return nil
}

// onDiskIsNodeKind reports whether the on-disk entity matches the baseline's recorded node kind:
// a "dir" baseline wants a directory, anything else (a regular file, or an unset type - a stranded
// download baseline records a file) wants a regular file. dropStrandedBaseline uses it to tell a
// materialised node from a blocking obstacle of a different kind (an untracked directory that made
// a file download fail with fs.ErrExist), which must not be mistaken for the node.
func onDiskIsNodeKind(fi os.FileInfo, fileType string) bool {
	if fileType == "dir" {
		return fi.IsDir()
	}
	return fi.Mode().IsRegular()
}
