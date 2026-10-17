package engine

// This file carries the Phase-6c outbound (local→server push) apply path. It is the
// mirror of executor.go's inbound apply: drainBatch schedules and serialises outbound
// ops by the same path-overlap conflict keys (so a parent mkdir precedes a child
// upload, and a rename serialises on both endpoints), but a DirOutbound op is applied
// to the server over a leased worker connection rather than to local disk. The
// local-reconcile pass (localreconcile.go) produced the ops and flipped each affected
// baseline to StatePending carrying the local facts; a successful push records the
// server-assigned facts (file id, server hash/mtime) and advances the baseline to
// StateSynced.
//
// Conflict seam: the server is the authority for divergence. A stale-base rejection
// (actions.ErrConflictVersion, code 24579) is non-fatal. For a file upload it funnels
// into the Phase-6d keep-both resolver (funnelUploadConflict enqueues an
// OpResolveConflict and acks the upload): the server version wins on the canonical path
// and the divergent local content is preserved as a _Conflict copy pushed back as a new
// file. For any other diverged push (a rename) the baseline is marked StateConflict and
// the op acked, leaving that case's resolution out of keep-both scope rather than
// silently overwriting a diverged remote. A parent directory whose server id is not yet
// known (errParentUnknown) leaves the op queued to retry once the parent's own op has
// recorded its id. A structurally invalid path is dead-lettered like the inbound side. A
// broken connection is re-dialed by the Transport's WithWorker (Broken()) and the op
// retried on the next drain.

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// uploadResumeThreshold is the default for limits.UploadResumeThreshold: the file size
// at or below which a push uses the single-frame Upload action; a larger file is sent
// with the chunked resume_upload protocol so it is never buffered whole in memory. The
// engine owns this resume-vs-small decision (DESIGN §4); the lower action layer carries
// only the per-frame mechanics. The effective value is config-tunable
// (config.Limits.UploadResumeThreshold); this constant is the safe default and test oracle.
const uploadResumeThreshold = 4 << 20 // 4 MiB

// uploadChunkSize survives only as a test oracle. There is no upload chunking on
// the wire - resume_upload carries the whole file in one content frame, streamed
// off disk - so nothing in the client reads a chunk size; the corresponding
// limits knob was removed in schema v9.
const uploadChunkSize = 1 << 20 // 1 MiB

// outboundClient is the set of action-client push operations the outbound executor
// drives over a leased worker connection. *actions.Client satisfies it; the executor
// type-asserts the leased syncClient to it (mirroring brokenReporter) so the inbound
// test fakes need no push stubs. The path-addressed actions (Rename/Remove/List)
// take a view-relative path scoped by view_id (a bare path is ambiguous across
// views); the create actions (Upload/Mkdir/ResumeGetToken) address the parent via
// its server id (id:<parent_id>/<name>).
type outboundClient interface {
	Upload(ctx context.Context, parentID, name string, mtime, rootNodeID int64, content []byte) (actions.Node, error)
	Mkdir(ctx context.Context, parentID, name string, mtime, rootNodeID int64) (actions.Node, error)
	Rename(ctx context.Context, viewID, rootNodeID int64, fromPath, toPath string) error
	Remove(ctx context.Context, viewID int64, path string) error
	// List's second result is the enumeration evidence (actions.Client.List): false
	// means the reply carried no children array at all, so an empty node slice is NOT proof
	// the directory is empty. Every absence prover below requires it.
	List(ctx context.Context, viewID int64, path string) ([]actions.Node, bool, error)
	ResumeGetToken(ctx context.Context, req actions.ResumeRequest) (actions.ResumeToken, error)
	ResumeUpload(ctx context.Context, token string, spec actions.ResumeUploadSpec, body io.Reader) (actions.Node, error)
}

// errNoOutbound marks a leased connection whose client does not implement the push
// operations - a programming error (the production client always does), surfaced rather
// than panicking so a misconfigured transport fails the op instead of the process.
var errNoOutbound = errors.New("engine: leased client does not support outbound push")

// errUploadSourceGone marks an OpUpload whose local source file no longer exists when the
// push tries to read it - the user deleted or moved it in the scan→apply window. Leaving
// the op queued would wedge it forever (the file never comes back), and because
// localReconcile skips a path that already has a queued op, the deletion would never be
// re-detected either. applyOutbound recognises this sentinel and resolves the pending
// baseline so the next scan reconciles the new on-disk reality.
var errUploadSourceGone = errors.New("engine: upload source no longer exists")

// applyOutbound performs a single queued DirOutbound operation against the server and,
// on success, advances its baseline to StateSynced and Acks the queue record. It mirrors
// applyOp's inbound bookkeeping with outbound-specific error handling. This function
// decides WHICH push to make and handles a clean one; what a refusal MEANS -
// stale base, rejected path, unknown parent, a divergence the client proved itself -
// is classifyOutboundFailure, which the error arm hands off to.
func (e *Engine) applyOutbound(ctx context.Context, qo store.QueuedOp) error {
	op := qo.Op
	// A view the account now holds read-only cannot accept any push: the server would reject
	// every upload/mkdir/rename/remove as unauthorized and the op would retry forever.
	// localScan already skips read-only views when generating ops, but an op enqueued while
	// the view was writable (its privilege since downgraded to "ro") would still drain here.
	// Treat it like a push that can never land: mark the baseline StateConflict (a divergence
	// the read-only side cannot resolve, left for 6d keep-both) and dead-letter the op so it
	// cannot wedge the queue with permanent unauthorized retries.
	if e.viewIsReadOnly(op.ViewID) {
		e.log.Warn("engine: outbound op targets a read-only view; marking conflict and dropping",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		// Record the trace and ack atomically (after the conflict-marking cleanup), so an ack
		// failure leaves the op queued WITHOUT a trace and a retry cannot double-count Dropped
		// (mirrors the inbound dead-letter sites).
		if dlErr := e.deadLetterAndAck(op, qo.Seq, "outbound op targets a read-only view"); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	var err error
	switch op.Type {
	case store.OpUpload:
		err = e.applyUploadOut(ctx, op)
	case store.OpMkdir:
		err = e.applyMkdirOut(ctx, op)
	case store.OpRename:
		err = e.applyRenameOut(ctx, op)
	case store.OpRemove:
		err = e.applyRemoveOut(ctx, op)
	default:
		// An unknown outbound op type can never be applied by any handler, so returning a
		// plain error would treat it as transient and retry forever, wedging the whole queue
		// - the same failure mode dead-lettered on the inbound path. It is currently
		// unreachable (localReconcile/coldStart only enqueue known types), but dead-letter it
		// like a rejected path so a future/corrupt op type cannot starve every view.
		e.log.Warn("engine: dropping outbound op with unknown type",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		// No baseline cleanup needed; record the trace and ack atomically so a retry cannot
		// double-count Dropped.
		if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("unknown outbound op type %q", op.Type)); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if err != nil {
		return e.classifyOutboundFailure(ctx, qo, err)
	}
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack outbound %d: %w", qo.Seq, err)
	}
	// The outbound clean-success path is its own, so it records its own progress. The
	// earlier acks in this function are the degenerate ones - a read-only view, a
	// vanished source, a dead-lettered push - which unwedge the queue without moving
	// content, and counting them would report work that never happened.
	e.progress.record(transferredBytes(op))
	return nil
}

// viewIsReadOnly reports whether the account holds viewID read-only, so an outbound push to
// it can never succeed. A view that cannot be loaded is treated as writable - the conservative
// default that preserves the prior push-everything behaviour and matches isReadOnlyView's
// handling of an empty/unknown privilege.
func (e *Engine) viewIsReadOnly(viewID uint64) bool {
	v, err := e.store.GetView(viewID)
	if err != nil {
		return false
	}
	return isReadOnlyView(v.Privilege)
}

// baselineFromOp reconstructs a baseline from an outbound op's own facts. It is used when a
// conflict is recorded for a path whose pending baseline was never written (the crash/error
// window between localReconcile's enqueue and its baseline flip - see plannedOp and
// markOutboundConflict). The FileType is inferred from the op kind (mkdir → dir,
// otherwise file) and the local content facts are carried so a later keep-both resolution (6d)
// has something to compare against. State is left to the caller.
func baselineFromOp(op store.Op) store.Baseline {
	// Two witnesses of the kind, because the op that reaches here need not be the one that
	// CREATES the node: OpMkdir states it by verb, and op.BaseIsDir is planRemove's durable
	// capture of the vanished record's kind (localreconcile.go), which the refused directory
	// delete (removeDirDivergedError, finding H7) is precisely the caller of. Typing that
	// synthesised record as a FILE at a DIRECTORY path is not inert: verifyServerSubtreeAgrees
	// reads a server child reported as a directory over a file-typed baseline as "a peer
	// replaced the node" and refuses TERMINALLY, so one mistyped record would poison the
	// deletion of every ancestor above it. revertRelocatedRemove already reads the same field.
	ft := fileType(op.Type == store.OpMkdir || op.BaseIsDir)
	return store.Baseline{
		FileID:     op.FileID,
		FileType:   ft,
		LocalHash:  op.LocalHash,
		LocalSize:  op.LocalSize,
		LocalMTime: op.LocalMTime,
		UnixPerm:   op.UnixPerm,
	}
}

// dropGoneParentBaseline resolves the pending baseline an outbound op whose parent directory
// was removed server-side (errParentGone) left behind, so the local content becomes
// untracked. The affected path is the rename's destination (where localReconcile relocated
// the baseline) and op.Path otherwise. DeleteBaseline is idempotent, so a baseline the
// preceding inbound remove already dropped (Order B) is a no-op. Making the content untracked
// is what lets the trailing inbound OpRemove's guardRemoveDir preserve it as a _Conflict copy
// (Order A): a still-tracked-and-synced baseline would instead read the on-disk file as
// server-authoritative and delete it.
//
// The cleanup is relocation-aware: errParentGone also fires when the
// parent was RENAMED server-side (the ancestor walk is leaf-name-blind, accepted), and by
// then reconcile's relocatePending has already re-homed the op's baseline under the new
// prefix - a cleanup keyed only on the op's stale path would silently no-op and strand that
// relocated record StatePending forever (localReconcile only re-drives StateSynced records,
// so nothing would ever re-emit it). An op carrying the node's file id resolves the record's
// ACTUAL path through the byid index and resolves it there: a create (mkdir) is deleted so
// the next scan re-creates it fresh, a modify/rename is reverted for re-emission
// (StateSynced + cleared local facts). An id-less create additionally re-bases its path
// through the QUEUED inbound renames (the relocation's own producers, still serialized
// behind this op) and deletes the record at the re-based path too.
func (e *Engine) dropGoneParentBaseline(op store.Op) error {
	p := op.Path
	if op.Type == store.OpRename && op.NewPath != "" {
		p = op.NewPath
	}
	if op.FileID != "" {
		cur, err := e.store.PathByID(op.ViewID, op.FileID)
		switch {
		case isNotFound(err):
			// No indexed record: fall through to the plain path-keyed delete.
		case err != nil:
			return fmt.Errorf("engine: resolve gone-parent baseline %d/%q: %w", op.ViewID, op.FileID, err)
		case cur != p:
			e.log.Info("engine: gone-parent cleanup found the op's baseline relocated; resolving it at the tracked path",
				"view", op.ViewID, "op_path", p, "tracked_path", cur, "file_id", op.FileID)
			if op.Type == store.OpMkdir {
				if derr := e.store.DeleteBaseline(op.ViewID, cur); derr != nil {
					return fmt.Errorf("engine: drop relocated gone-parent baseline %d/%q: %w", op.ViewID, cur, derr)
				}
				return nil
			}
			return e.revertRelocatedOutbound(op.ViewID, cur)
		}
	}
	if err := e.store.DeleteBaseline(op.ViewID, p); err != nil {
		return fmt.Errorf("engine: drop gone-parent baseline %d/%q: %w", op.ViewID, p, err)
	}
	if op.FileID == "" {
		// An id-less create has no byid entry to resolve a relocation through; follow the
		// queued inbound renames instead (best-effort - a residual stale record is the
		// pre-fix stranding, never a new wrong deletion).
		if reb := e.relocatedByQueuedInboundRenames(op.ViewID, p); reb != p {
			e.log.Info("engine: gone-parent cleanup re-based the op's path through queued inbound renames",
				"view", op.ViewID, "op_path", p, "rebased_path", reb)
			if err := e.store.DeleteBaseline(op.ViewID, reb); err != nil {
				return fmt.Errorf("engine: drop relocated gone-parent baseline %d/%q: %w", op.ViewID, reb, err)
			}
		}
	}
	return nil
}

// revertRelocatedOutbound reverts a relocated node's StatePending baseline to StateSynced
// and clears the local facts (LocalHash/LocalSize/LocalMTime), so the next local scan
// re-diffs the on-disk file at the node's CURRENT path and re-emits the outbound op there.
// It resolves the ancestor-relocation races: an outbound upload/rename addressed the node's
// old path, but a server-side rename of an ancestor relocated the node's baseline
// (relocatePending) before the op drained, so the stale op cannot execute and its relocated
// baseline would otherwise sit StatePending, inert forever. Clearing the local facts is
// load-bearing (the resolveVanishedUpload precedent): they describe the very
// content sitting on disk, so a synced baseline keeping them would make the next scan read
// "no change" and silently swallow the un-pushed edit/move. An empty LocalHash can never
// equal a real file's hash, so the scan always re-diffs the path into a fresh op. A
// StateConflict record is left untouched (the divergence is owed to keep-both and must stay
// surfaced); an absent record or one already reverted is a no-op, so a replay after a
// failed ack re-runs this harmlessly.
func (e *Engine) revertRelocatedOutbound(viewID uint64, path string) error {
	b, err := e.store.GetBaseline(viewID, path)
	if isNotFound(err) {
		return nil
	}
	if errors.Is(err, store.ErrCorruptRecord) {
		// An undecodable record cannot be reverted field-by-field, and synthesising a synced
		// record from unknown facts could mislead the scan's move/modify detection. Delete it
		// instead (DeleteBaseline removes an undecodable value anyway - the heal): the node's
		// on-disk content is still present (this path handles an upload/rename whose local
		// bytes never left the disk), so the now-untracked path re-diffs as a fresh create on
		// the next scan - the same data-safe re-emission the revert wants, worst case a
		// keep-both detour - rather than the strict read wedging the cleanup forever.
		e.warnCorruptBaseline("revert-relocated-outbound", viewID, path, err)
		if derr := e.store.DeleteBaseline(viewID, path); derr != nil {
			return fmt.Errorf("engine: drop corrupt relocated outbound baseline %d/%q: %w", viewID, path, derr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("engine: load relocated outbound baseline %d/%q: %w", viewID, path, err)
	}
	if b.State == store.StateConflict {
		return nil
	}
	if b.State == store.StateSynced && b.LocalHash == "" && b.LocalSize == 0 && b.LocalMTime == 0 {
		return nil
	}
	b.State = store.StateSynced
	b.LocalHash = ""
	b.LocalSize = 0
	b.LocalMTime = 0
	if err := e.store.PutBaseline(viewID, path, b); err != nil {
		return fmt.Errorf("engine: revert relocated outbound baseline %d/%q: %w", viewID, path, err)
	}
	return nil
}

// relocatedByQueuedInboundRenames maps a view-relative path through every QUEUED inbound
// rename of the view (in seq order), mirroring the rebaseMoved chain reconcile runs over a
// batch's move events (preBatchDeletePath, direction reversed). An id-less create op has no
// byid entry to resolve its relocated baseline through, but the inbound OpRename whose
// relocatePending re-homed that baseline is still in the durable queue - serialized BEHIND
// the op being cleaned up - so following the queued moves finds where the pending record
// now lives. Paths are compared fold-keyed (normpath.Key), matching relocatePending's own
// matching. Best-effort: a scan failure returns the path unchanged, so the cleanup then
// no-ops at the stale key - the pre-fix residual, never a new wrong deletion.
func (e *Engine) relocatedByQueuedInboundRenames(viewID uint64, p string) string {
	cur := p
	err := e.scanQueuedInView(viewID, "inbound renames", func(qo store.QueuedOp) error {
		if qo.Direction != store.DirInbound || qo.Type != store.OpRename {
			return nil
		}
		cur, _ = relocateUnderMove(cur, qo.Path, qo.NewPath)
		return nil
	})
	if err != nil {
		return p
	}
	return cur
}

// funnelUploadConflict turns a stale-base (24579) rejection of a file upload into a
// keep-both resolution: it enqueues an OpResolveConflict for the upload's view+path and
// acks the original upload. The resolve op is enqueued BEFORE the upload is acked so a
// crash between the two leaves the upload queued - it re-hits 24579 on the next drain and
// re-enqueues - rather than losing the resolution and stranding the baseline StatePending
// with no queued op (localReconcile only acts on StateSynced records, so it would never
// re-detect it). A duplicate resolve produced by such a replay is harmless: once the first
// resolve has run, the canonical path matches the server and the second takes the
// false-conflict fast path. The resolve op carries UnixPerm so the resolver records the
// canonical and copy baselines with the right mode, and the conflicting upload's FileID
// (the server node id for a modify; empty for a fresh create) so the resolver's
// server-vanished disambiguation (confirmAbsentByParentList) can key on the stable id
// rather than the leaf name - a name-only check is fooled by a concurrent server-side
// rename-away of the node.
func (e *Engine) funnelUploadConflict(qo store.QueuedOp) error {
	op := qo.Op
	e.log.Info("engine: outbound upload hit stale-base conflict; funnelling to keep-both resolver",
		"view", op.ViewID, "path", op.Path)
	if _, err := e.store.Enqueue(store.Op{
		Type:      store.OpResolveConflict,
		Direction: store.DirOutbound,
		ViewID:    op.ViewID,
		Path:      op.Path,
		FileID:    op.FileID,
		UnixPerm:  op.UnixPerm,
	}); err != nil {
		return fmt.Errorf("engine: enqueue resolve-conflict %d/%q: %w", op.ViewID, op.Path, err)
	}
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack conflicted upload %d: %w", qo.Seq, err)
	}
	return nil
}

// funnelRenameConflict turns an outbound rename whose destination is occupied server-side into
// a keep-both resolution at that DESTINATION, mirroring funnelUploadConflict.
//
// The path is op.NewPath, not op.Path: that is where the local file now sits (localReconcile
// relocated its baseline there when it planned the move) and where the server's foreign node
// lives, so it is the one path where the two versions actually meet. The resolver sets the
// local file aside as a _Conflict copy, lands the server's node on the canonical path and
// uploads the copy back as a node of its own.
//
// The op carries the MOVED node's FileID. The resolver uses it only for its server-vanished
// disambiguation, and the moved node is the one whose absence would matter here - the occupant
// is what the resolve is about to download.
//
// Enqueue-before-ack like its siblings: a crash between the two leaves the rename queued, it
// re-probes the same occupied destination on the next drain and re-funnels, rather than losing
// the resolution and stranding a baseline no pass would drive again.
func (e *Engine) funnelRenameConflict(qo store.QueuedOp) error {
	op := qo.Op
	if _, err := e.store.Enqueue(store.Op{
		Type:      store.OpResolveConflict,
		Direction: store.DirOutbound,
		ViewID:    op.ViewID,
		Path:      op.NewPath,
		FileID:    op.FileID,
		UnixPerm:  op.UnixPerm,
	}); err != nil {
		return fmt.Errorf("engine: enqueue resolve-conflict %d/%q: %w", op.ViewID, op.NewPath, err)
	}
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack occupied-destination rename %d: %w", qo.Seq, err)
	}
	return nil
}

// funnelTypeCollision turns a confirmed file<->dir create/create collision
// (errServerTypeCollision) into a keep-both resolution, mirroring funnelUploadConflict: it
// enqueues an OpResolveConflict for the create's view+path and acks the original op. The
// resolve op is enqueued BEFORE the ack so a crash between the two leaves the create queued
// - it re-probes the collision on the next drain and re-funnels - rather than losing the
// resolution; a duplicate resolve from such a replay is harmless (the second finds the
// canonical path already resolved). The op's FileID (empty for a fresh create; the node id
// for a modify whose file the server replaced with a directory) and UnixPerm ride along for
// the resolver's server-vanished disambiguation and baseline stamping, exactly as the
// stale-base funnel carries them.
func (e *Engine) funnelTypeCollision(qo store.QueuedOp) error {
	op := qo.Op
	e.log.Info("engine: outbound create collides with a server node of the opposite type; funnelling to keep-both resolver",
		"type", op.Type, "view", op.ViewID, "path", op.Path)
	if _, err := e.store.Enqueue(store.Op{
		Type:      store.OpResolveConflict,
		Direction: store.DirOutbound,
		ViewID:    op.ViewID,
		Path:      op.Path,
		FileID:    op.FileID,
		UnixPerm:  op.UnixPerm,
	}); err != nil {
		return fmt.Errorf("engine: enqueue resolve-conflict %d/%q: %w", op.ViewID, op.Path, err)
	}
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack type-collision create %d: %w", qo.Seq, err)
	}
	return nil
}

// markOutboundConflict flips the baseline at op's affected path to StateConflict after a
// stale-base rejection, preserving the recorded facts so the deferred keep-both
// resolution (6d) can compare them. The affected path is the op's destination for a
// rename (where local-reconcile relocated the baseline) and its Path otherwise.
func (e *Engine) markOutboundConflict(op store.Op) error {
	p := op.Path
	if op.Type == store.OpRename && op.NewPath != "" {
		p = op.NewPath
	}
	b, err := e.store.GetBaseline(op.ViewID, p)
	if isNotFound(err) {
		// The pending baseline localReconcile pre-writes can be absent here: it enqueues the op
		// BEFORE flipping the baseline (see plannedOp), so a crash or a PutBaseline failure between
		// the two leaves a durable op with no baseline - and for a fresh create there was no prior
		// record to fall back on either. Silently returning would ack the op while leaving the path
		// baseline-less, so the next scan re-detects it as a brand-new create and re-enqueues it
		// forever, hitting the same conflict each time and never settling. Synthesise a
		// conflict record from the op's own facts so the divergence is recorded as StateConflict and
		// the path is no longer re-detected as a fresh create.
		b = baselineFromOp(op)
	} else if errors.Is(err, store.ErrCorruptRecord) {
		// An undecodable record: rebuild it from the op's own facts, exactly like the absent
		// branch - the fresh StateConflict Put below overwrites (heals) the corrupt key while
		// recording the divergence, instead of the strict read retrying the op transiently
		// forever.
		e.warnCorruptBaseline("mark-conflict", op.ViewID, p, err)
		b = baselineFromOp(op)
	} else if err != nil {
		return fmt.Errorf("engine: load conflicted baseline %d/%q: %w", op.ViewID, p, err)
	}
	b.State = store.StateConflict
	if err := e.store.PutBaseline(op.ViewID, p, b); err != nil {
		return fmt.Errorf("engine: write conflicted baseline %d/%q: %w", op.ViewID, p, err)
	}
	return nil
}

// classifyOutboundFailure decides what a failed outbound push MEANS, which is the whole
// difference between a retry, a keep-both copy and a permanent drop.
//
// It was the back three-quarters of applyOutbound, and it is one story rather than a
// continuation of that function's: applyOutbound decides WHICH push to make, this decides
// what its refusal costs. The order of the arms below is the classification, so read them in
// order; each says what it means and what it does about it.
//
// A returned error keeps the op queued (the record stays un-Acked); errDeadLettered means the
// op was dropped with a durable trace; nil means the failure was resolved into other work -
// a keep-both funnel, a conflict marking - and the op is done.
func (e *Engine) classifyOutboundFailure(ctx context.Context, qo store.QueuedOp, err error) error {
	op := qo.Op
	if errors.Is(err, errUploadTargetDiverged) {
		// The push's own pre-turn probe proved the server no longer holds the version this
		// upload was written against (finding H1): a peer overwrote the node, or took the
		// create's path. The server does not arbitrate this for an upload - its
		// "compare_mtime" policy would ACCEPT the write and destroy that version everywhere -
		// so the client's proof stands in for the 24579 that never comes, and takes the same
		// exit: keep-both, where the server version wins the canonical path and the local
		// bytes survive as a _Conflict copy.
		e.log.Warn("engine: outbound upload target diverged server-side; funnelling to keep-both resolver instead of overwriting it",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID, "err", err)
		return e.funnelUploadConflict(qo)
	}
	if errors.Is(err, errRenameTargetOccupied) {
		// The move's own pre-turn probe found another client's node standing on the
		// destination path (guardRenameTarget). The server arbitrates this no better than
		// it arbitrates an upload: rename offers no "only if free" and, on the destructive
		// reading of the verb, would take the occupant with it. So the client's proof
		// stands in for a conflict the protocol never reports, and takes the same exit as
		// H1's - keep-both at the destination, where the server's node wins the canonical
		// path and the locally moved file survives as a _Conflict copy.
		e.log.Warn("engine: outbound rename destination is held by another server node; funnelling to keep-both resolver instead of moving onto it",
			"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID, "err", err)
		return e.funnelRenameConflict(qo)
	}
	if errors.Is(err, actions.ErrConflictVersion) {
		// The server rejected the push because its base version is stale - a remote
		// change diverged from the baseline we pushed against. For a file upload this is
		// DESIGN §2's primary case (concurrent content modification): funnel it into the
		// keep-both resolver rather than only recording the divergence, so the server
		// version wins on the canonical path and the local content is preserved as a
		// _Conflict copy (6d).
		if op.Type == store.OpUpload {
			return e.funnelUploadConflict(qo)
		}
		// Any other diverged push (a rename) is out of keep-both scope: mark the baseline
		// StateConflict (resolution owed) and ack so the op does not wedge the queue;
		// never silently overwrite the diverged remote (6d).
		e.log.Warn("engine: outbound push hit stale-base conflict; marking conflict",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack conflicted %d: %w", qo.Seq, ackErr)
		}
		return nil
	}
	if errors.Is(err, errRejectedPath) {
		// A structurally invalid local path can never be pushed; dead-letter it so it
		// cannot wedge the queue, mirroring the inbound side. The pending baseline is
		// left in place - the next local scan only acts on StateSynced records, so a
		// stranded pending record is inert rather than re-enqueued.
		e.log.Warn("engine: dropping unappliable outbound op with rejected path",
			"type", op.Type, "view", op.ViewID, "path", op.Path, "err", err)
		// The pending baseline is left in place (the next local scan only acts on StateSynced
		// records, so it is inert); record the trace and ack atomically so a retry cannot
		// double-count Dropped.
		if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("outbound rejected path: %v", err)); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if errors.Is(err, errParentConflicted) {
		// The parent directory diverged (a rejected parent mkdir is stuck in StateConflict
		// with no server id). The child can never land under it, so propagate the conflict:
		// mark this op's baseline StateConflict (owed to 6d keep-both) and dead-letter it
		// instead of retrying forever, which would otherwise wedge the queue and starve later
		// batches/views.
		e.log.Warn("engine: outbound op parent is conflicted; marking conflict and dropping",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		// Record the trace and ack atomically (after the conflict-marking cleanup) so a
		// retry cannot double-count Dropped.
		if dlErr := e.deadLetterAndAck(op, qo.Seq, "outbound op parent is conflicted"); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if errors.Is(err, errParentCorrupt) {
		// The parent directory's baseline record is corrupt, so its server id can never be
		// resolved and no queued op will heal it. Dead-letter this child rather than
		// retrying it forever and wedging the queue - the same terminal
		// treatment as errParentConflicted, and mark the baseline StateConflict rather than
		// leaving it StatePending. localReconcile only re-drives StateSynced records, so a
		// stranded StatePending baseline is NOT inert as errRejectedPath's is: unlike a
		// structurally invalid path (permanently unsyncable, so pending-and-forgotten is
		// correct), a corrupt-parent child CAN recover once the record heals, and leaving
		// it pending would silently swallow that path - even a fresh local edit would be
		// skipped, and the divergence would never surface. StateConflict makes it visible in
		// the Unresolved count (a "child under a conflicted parent", exactly what
		// countUnresolved already surfaces) and matches both errParentConflicted here and
		// applyResolveConflict's handling of this same error, so the child stays observable
		// until the corruption is cleared (an inbound rewrite of the parent + child, or a
		// Reset/re-pair) rather than vanishing into a stale pending record.
		e.log.Warn("engine: outbound op parent baseline is corrupt; marking conflict and dropping",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		if dlErr := e.deadLetterAndAck(op, qo.Seq, "outbound op parent baseline is corrupt"); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if errors.Is(err, errUploadSourceGone) {
		// The file this upload was to push vanished before it applied. It can never be sent,
		// but the path may have an acknowledged remote update riding on it: reconcile defers
		// a remote create/modify whose path has a queued upload to THIS upload's 24579 funnel
		// and advances the cursor, so simply dropping the op would lose that update. Decide
		// between recovering the remote version and propagating a genuine local delete.
		return e.handleVanishedUpload(ctx, qo)
	}
	if errors.Is(err, errParentGone) {
		// The op's parent directory was removed server-side (confirmed by the ancestor walk)
		// - an upload/mkdir/rename can never land under a directory that no longer
		// exists, and no queued op will resurrect it (the parent is gone from BOTH sides).
		// Dead-letter it terminally rather than retrying forever, which - drained ahead of
		// the inbound OpRemove of that directory - would also block that authoritative remove
		// and its never-lose guardRemoveDir preserve. Drop the pending baseline so the local
		// content becomes untracked; the trailing inbound OpRemove then preserves the edited
		// file/subtree as a _Conflict copy (Order A), or - when the remove already ran and
		// preserveAside carried it off - the content is already safe in the _Conflict copy
		// (Order B).
		e.log.Warn("engine: outbound op parent removed server-side; dropping op",
			"type", op.Type, "view", op.ViewID, "path", op.Path)
		if cerr := e.dropGoneParentBaseline(op); cerr != nil {
			return cerr
		}
		if dlErr := e.deadLetterAndAck(op, qo.Seq, "outbound op parent removed server-side"); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	if errors.Is(err, errRenameSourceGone) {
		// The rename's source node is positively gone server-side while both parent
		// directories are alive: another client deleted it or moved it away, so
		// the stale move can never execute - and the authoritative inbound OpRemove/
		// OpRename is serialized BEHIND it on the shared source key (a permanent mutual
		// wedge before this exit). A race resolution, not a failure: DROP the pending
		// baseline at the destination (the node no longer exists server-side under this
		// identity, so there is no move to re-emit) - the still-on-disk file becomes
		// untracked and the next scan re-uploads it as a fresh create, data-safe (worst
		// case a spurious duplicate under a cross-client move-vs-move race, which
		// keep-both tolerates). DELETING rather than reverting to StateSynced is
		// load-bearing: a synced record would make the trailing inbound OpRemove resolve
		// the node through byid to the destination and set the user's moved file aside as
		// a spurious _Conflict copy (relocatedRemoveBase preserves a synced-without-hash
		// record whole rather than deleting it outright; either way a
		// stale StateSynced record here is wrong). No durable dead-letter trace (mirroring
		// removeNodeMovedError below);
		// the ack + errDeadLettered only tell the scheduler the op left the queue
		// without applying.
		e.log.Info("engine: outbound rename source node gone server-side; dropping stale move for re-detection",
			"view", op.ViewID, "from", op.Path, "to", op.NewPath, "file_id", op.FileID)
		p := op.NewPath
		if p == "" {
			p = op.Path
		}
		if cerr := e.store.DeleteBaseline(op.ViewID, p); cerr != nil {
			return fmt.Errorf("engine: drop source-gone rename baseline %d/%q: %w", op.ViewID, p, cerr)
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack source-gone rename %d: %w", qo.Seq, ackErr)
		}
		return errDeadLettered
	}
	if errors.Is(err, errServerTypeCollision) {
		// A confirmed file<->dir create/create collision: the server
		// holds a node of the OPPOSITE kind, with a non-empty id, at the create's path, so
		// the op can never land as-is - and nothing else in this matrix matches its bare
		// ErrPathError, so pre-fix it retried forever, wedging its conflict chain. Funnel it
		// into the keep-both resolver, mirroring the 24579 stale-base branch: enqueue an
		// OpResolveConflict (before the ack, so a crash between the two replays the create
		// and re-funnels - a duplicate resolve is harmless) and ack the create. For a
		// local-dir-vs-server-file MKDIR the resolver runs the full keep-both - the server
		// file wins on the canonical path and the local directory is set aside whole as a
		// _Conflict copy (downloadCanonical's fs.ErrExist intruder path) the next scan
		// re-uploads - and its pending baseline is flipped StateConflict FIRST so a child
		// create queued behind the mkdir resolves terminally via errParentConflicted (the
		// 24579-mkdir precedent) instead of wedging forever on an id-less pending parent
		// whose op has left the queue. For a local-file-vs-server-dir UPLOAD the resolver's
		// errConflictServerDir doctrine dead-letters it observably (StateConflict + Dropped).
		if op.Type == store.OpMkdir {
			if cerr := e.markOutboundConflict(op); cerr != nil {
				return cerr
			}
		}
		return e.funnelTypeCollision(qo)
	}
	var relocated *uploadNodeRelocatedError
	if errors.As(err, &relocated) {
		// The upload's parent baseline was relocated by a server-side rename of an ancestor
		// before this op drained: the op addresses the old path, whose
		// parent baseline no longer exists, and the inbound OpRename that would move the
		// on-disk file is serialized BEHIND it - waiting is a cyclic wedge. Revert the
		// node's relocated baseline for re-emission (StateSynced + cleared local facts, the
		// resolveVanishedUpload precedent) so - once the inbound rename applies - the next
		// scan re-diffs the still-on-disk edit at the new path and re-emits the upload under
		// the live parent. A race resolution, not a failure or a genuine drop, so no durable
		// dead-letter trace is written (mirroring removeNodeMovedError below); the ack +
		// errDeadLettered only tells the scheduler the op left the queue without applying.
		e.log.Info("engine: outbound upload parent relocated server-side; re-emitting on the new path",
			"view", op.ViewID, "old_path", op.Path, "new_path", relocated.currentPath, "file_id", op.FileID)
		if rerr := e.revertRelocatedOutbound(op.ViewID, relocated.currentPath); rerr != nil {
			return rerr
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack relocated upload %d: %w", qo.Seq, ackErr)
		}
		return errDeadLettered
	}
	if errors.Is(err, errRemoveDirLocallyPresent) {
		// The directory this delete addresses stands on disk again, so the local deletion the
		// op was derived from is stale (outbound_removedir.go): a peer's child download
		// serialized AHEAD of this remove recreates the tree and markSyncs the child, after
		// which the subtree enumeration would read the peer's node as agreed content and the
		// recursive server delete would destroy it everywhere - finding H7 through the door
		// the queue evidence cannot see. A race resolution, not a failure and not a
		// divergence, so no durable trace is written (mirroring the relocated-upload arm):
		// revert the pending baseline to StateSynced with the local facts cleared and ack, and
		// the next scan re-diffs the tree as it now IS - re-emitting the delete if it is still
		// wanted, and propagating the children the user really did remove through their own
		// removes, each with its own precondition.
		e.log.Info("engine: outbound directory remove is stale; the directory is on disk again, re-deriving on the next scan",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
		if rerr := e.revertRelocatedOutbound(op.ViewID, op.Path); rerr != nil {
			return rerr
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack stale directory remove %d: %w", qo.Seq, ackErr)
		}
		return errDeadLettered
	}
	var dirDiverged *removeDirDivergedError
	if errors.As(err, &dirDiverged) {
		// The outbound remove addresses a DIRECTORY whose server subtree holds a node this
		// client never agreed on (finding H7). The server's `remove` is RECURSIVE, so going
		// ahead would destroy that node on the server and in every replica with no _Conflict
		// copy - the never-lose violation the two-client stand measured. Resolve it like the
		// stale-base rejection of a non-upload push: mark the baseline StateConflict (the
		// divergence is owed to keep-both and stays visible in the Unresolved count) and
		// dead-letter with the offending child named in the trace, so the refusal leaves a
		// track an operator can read rather than a silently retried op. The known children's
		// own removes, serialized behind this one, still delete the content this client DID
		// agree on - only the peer's node survives, which is exactly the wanted outcome.
		e.log.Warn("engine: outbound directory remove refused; the server subtree holds content this client never agreed on",
			"view", op.ViewID, "path", op.Path, "child", dirDiverged.child, "reason", dirDiverged.reason)
		if cerr := e.markOutboundConflict(op); cerr != nil {
			return cerr
		}
		if dlErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf(
			"outbound directory remove refused: %s (%q)", dirDiverged.reason, dirDiverged.child)); dlErr != nil {
			return dlErr
		}
		return errDeadLettered
	}
	var divergedRemove *removeServerDivergedError
	if errors.As(err, &divergedRemove) {
		// The outbound remove's pre-turn probe found the server holding a NEWER version of the
		// node than the base this client last synced: another client
		// modified the file after the local delete was scanned, and the queued path-addressed
		// delete would destroy that edit with no _Conflict copy. DESIGN §6 makes a local change
		// racing a remote change a conflict, and the server is the authority for divergence:
		// recover the server version (enqueue an inbound download onto the now-empty path) and
		// drop the stale delete. A race resolution, not a failure or a genuine drop, so no
		// durable dead-letter trace is written (mirroring removeNodeMovedError below); the ack
		// + errDeadLettered only tell the scheduler the op left the queue without applying.
		e.log.Warn("engine: outbound remove target diverged server-side after the local delete; recovering the server version instead of deleting it",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
		if rerr := e.recoverDivergedRemove(op, divergedRemove.node); rerr != nil {
			return rerr
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack diverged remove %d: %w", qo.Seq, ackErr)
		}
		return errDeadLettered
	}
	var moved *removeNodeMovedError
	if errors.As(err, &moved) {
		// The outbound remove's target node was relocated server-side by an inbound rename of
		// an ancestor before this op drained: the ancestor walk read the renamed
		// ancestor as absent and would otherwise have acked a delete that never ran. Revert the
		// node's relocated baseline to StateSynced so the next local scan re-emits an OpRemove
		// against its current path, then ack the stale op. This is a race resolution, not a
		// failure or a genuine drop, so it does NOT write a durable dead-letter trace (mirroring
		// handleVanishedUpload) - it Acks and returns errDeadLettered only so the scheduler
		// treats the op as "left the queue without applying" (clearing its stuck counter) rather
		// than as a completed apply.
		e.log.Info("engine: outbound remove target relocated server-side; re-emitting on the new path",
			"view", op.ViewID, "old_path", op.Path, "new_path", moved.currentPath, "file_id", op.FileID)
		if rerr := e.revertRelocatedRemove(op, moved.currentPath); rerr != nil {
			return rerr
		}
		if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
			return fmt.Errorf("engine: ack relocated remove %d: %w", qo.Seq, ackErr)
		}
		return errDeadLettered
	}
	// errParentUnknown and any transient transport/server error leave the op queued:
	// the parent's own op records its id (or the connection recovers) and the next
	// drain retries. Returning the error keeps the record un-Acked.
	return fmt.Errorf("engine: apply outbound %s %d/%q: %w", op.Type, op.ViewID, op.Path, err)
}
