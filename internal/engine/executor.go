package engine

// This file carries the Phase-6b executor: it drains the inbound durable queue the
// reconcile pass filled and applies each op to local disk. Network transfers
// (download) run over a leased worker connection from the Transport seam; directory
// operations (mkdir/rename/remove) are disk-only through internal/fsutil. After an
// op succeeds the baseline is advanced to StateSynced (recording the local facts)
// and the queue record is Acked, so a crash mid-drain replays only the not-yet-Acked
// ops on restart.
//
// Concurrency follows DESIGN §6: ops are serialised by the on-disk locations they
// touch and parallelised across independent paths through the worker pool. The
// conflict-key invariant that decides which ops touch overlapping locations - every
// op's resolved path(s), and the "equals / contains / is contained by" overlap test -
// is defined in serialize.go. drainQueue chains every op behind every earlier op it
// conflicts with, so overlapping ops (the same path, or an ancestor/descendant pair
// such as a download to a/b and a remove of a) apply in FIFO (enqueue, i.e. ascending
// sync_id) order, while independent paths proceed concurrently. To keep the predecessor
// bookkeeping linear, an op records only a minimal covering set of predecessors - a
// newer conflicting op transitively waits on the older ones it also conflicts with - so
// a run of same-path ops chains in O(n) links rather than O(n²). The pool's worker
// count caps the real parallelism of the downloads the chains hand out.
//
// The enqueue order is the ascending-sync_id order reconcile preserves; the executor
// never reorders ops, so a same-path sequence such as delete-then-create lands in the
// order the server emitted it.
//
// The per-operation apply handlers live in their own files: inbound_download.go,
// inbound_mkdir.go, inbound_rename.go and inbound_remove.go (the DirInbound path),
// outbound.go (DirOutbound) and conflict.go (keep-both). The dead-letter reconcile
// helpers live in deadletter.go and the shared path/baseline helpers (markSynced and
// the abs/noSymlinkEscape containment guards) in enginepath.go, leaving this file the
// scheduler/dispatcher (drainQueue/drainBatch, viewDir, applyOp) and its sentinels.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// errRejectedPath marks an op whose target can never be applied: either the server
// supplied a structurally invalid view name or path (one that escapes the sync root),
// or a pre-existing symlink sits along the on-disk path (which the engine never
// creates, so it cannot be resolved without operator intervention). Such an op can
// never succeed, so leaving it queued would keep the drain retrying it forever: it
// never leaves the queue, and - because the scheduler serialises overlapping-path ops
// - every conflicting same-path successor skips and stays queued behind it every
// drain (batch drag), so that path's chain never converges. applyOp recognises this
// sentinel and dead-letters the op (drops it with a durable trace surfaced as the
// Dropped count) so the chain flows again and the drop is observable, instead of a
// silent forever-retry. (The failing op no longer starves other views - a failed
// syncView/drain is log-and-continue - but its own conflict chain
// stays wedged until it is dropped.)
var errRejectedPath = errors.New("engine: server path rejected")

// errDeadLettered is the signal that an op was dropped rather than applied. It is not a
// failure - the op was acked and the queue keeps flowing - but drainBatch must distinguish
// it from a clean apply: a rename whose feeding op was dropped is itself doomed (its node
// never reached the source path on disk), so the chain scheduler propagates the dead mark to
// drop the dependent rename instead of letting it drag unrelated bytes onto the destination.
// It travels two ways: applyOp returns it for a rejected-path dead-letter, and
// applyRename returns it directly when it drops a move whose node is not where the move needs
// it (a foreign-source or never-materialised node) - both outcomes leave nothing at the
// destination, so a same-id rename fed by either must be doomed, not allowed to trust a false
// feeder.
var errDeadLettered = errors.New("engine: op dead-lettered")

// errOpReplaced is the signal that an op left the queue having been REPLACED, in the same
// transaction, by another queued op that will place the very same node at the very same output
// key (today: the inbound download the server placed at a directory, re-requested as an inbound
// OpMkdir).
//
// It is deliberately NOT errDeadLettered. That signal means "left the queue WITHOUT placing its
// node", which arms the cross-window doom (deadOutputs): a same-node inbound rename fed by the
// key is then dropped permanently, with a dead-letter trace and its relocated baseline dropped -
// and its own compensation is suppressed on top, because queuedInboundCoversNode sees the
// replacement op. The node is not absent here, it is merely still queued, so this takes the
// STILL-QUEUED treatment instead: the op's stuck counter clears (it did leave the queue), its
// keys block their overlapping successors for this pass so nothing applies past the replacement,
// and the dependent rename simply re-runs once the replacement has drained.
var errOpReplaced = errors.New("engine: op replaced by a compensating op")

// stuckThreshold is the number of consecutive failed applies after which a queued op is
// escalated as stuck: counted in StuckOps - the `Stuck` line in
// `status` - and logged with a one-time Warn. Ten failed drains is far past any expected
// transient (a broken connection re-dials within one drain; an unknown parent resolves as
// soon as the parent's own op runs), while still surfacing a genuine wedge within about a
// minute of default 5s scan cycles.
const stuckThreshold = 10

// noteOpFailure records one more failed transient apply for a queued op, escalating with
// a single Warn the moment the count reaches stuckThreshold: the op has retried far past
// any expected transient and would otherwise keep retrying invisibly forever (only the
// terminal classes are dead-lettered; everything else stays queued). This is the CULPRIT
// path - the op that failed its own apply; a successor merely serialised behind it goes
// through noteOpBlocked instead, whose Warn names this op as the head to unstick.
// Later failures stay silent - the counter keeps the op in StuckOps - and a
// success or dead-letter clears the entry via clearOpFailures.
//
// err is the failure that just occurred, used only for diagnosis at the escalation point: a
// reply that breached a decoder cap (an extreme-but-legal directory fan-out or event page -
// isDecoderCapError) never converges, because neither `list` nor `pull_event` lets the client
// ask for a smaller reply, so the retry re-requests the identical frame forever. That class was
// previously visible only as a bare Stuck count with an untyped transient underneath (the gap
// warnIfDecoderCap's doc records for the List consumers). Emitting it at the escalation rather
// than per drain keeps the size-capped rotating log from flooding.
//
// The escalation gate is "at or past the threshold, once per streak", not
// "exactly at the threshold": the counter is monotonic across drains and is cleared only by a
// success or a dead-letter, neither of which a wedged op reaches, so a single-drain gate handed
// the diagnosis to whichever error happened to land on the Nth failure. An unrelated transient
// there (a reconnect, an op-turn timeout, a store hiccup) permanently ate the decoder-cap
// diagnosis for an op that then breached the cap on every later drain - the log never carried
// the one line naming the real cause. Each flavour of Warn latches independently in opWarned,
// so the cap diagnosis can still fire on a later drain while the stuck Warn stays one-shot.
func (e *Engine) noteOpFailure(qo store.QueuedOp, err error) {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	e.opFailures[qo.Seq]++
	if e.opFailures[qo.Seq] < stuckThreshold {
		return
	}
	latch := e.opWarned[qo.Seq]
	if !latch.stuck {
		latch.stuck = true
		attrs := []any{
			"type", qo.Type, "direction", qo.Direction, "view", qo.ViewID,
			"path", qo.Path, "seq", qo.Seq, "failures", e.opFailures[qo.Seq],
		}
		// Name the server's own rejection code when the op is being refused by an errno the
		// action layer does not map: an anonymous stuck counter tells an operator nothing,
		// while the code is the one actionable fact (a share quota, an ACL, a naming policy)
		// - and it is also the evidence escalateDeterministicServerFailure will act on if the
		// same code keeps coming back. stuckMu is already held.
		// The same slot also carries the SYNTHETIC streak codes (a resumed upload the server
		// never reports as complete), which are negative and name nothing on
		// their own - serverErrStreakReason spells those out in words.
		if st, ok := e.opServerErrs[qo.Seq]; ok {
			attrs = append(attrs, "server_errno", st.code, "server_errno_streak", st.count,
				"reason", serverErrStreakReason(st.code))
		}
		e.log.Warn("engine: op stuck: kept failing across drains and will keep retrying", attrs...)
	}
	if !latch.capped && isDecoderCapError(err) {
		latch.capped = true
		e.warnIfDecoderCap(err, qo.ViewID, "op apply: "+string(qo.Type))
	}
	e.setOpWarnLatch(qo.Seq, latch)
}

// opWarnLatch records which one-shot escalation Warns an op has already emitted during its
// current failure streak, so each flavour fires once per streak instead of the whole
// escalation hanging on a single drain's counter equality. The three
// flavours latch independently because they diagnose different things and become true at
// different times: `stuck` (this op keeps failing its own apply), `blocked` (this op keeps
// being skipped behind a failing predecessor - it can become the culprit LATER, after having
// been a victim past the threshold), and `capped` (the failure that just happened was a
// decoder-cap breach, which an op may only start returning after it is already past the
// threshold). clearOpFailures drops the whole latch with the streak, so an op that recovers
// and later wedges again re-earns every Warn.
type opWarnLatch struct {
	stuck   bool
	blocked bool
	capped  bool
}

// setOpWarnLatch stores an op's Warn latch, allocating the map for an Engine built by a test
// without New. Callers hold stuckMu.
func (e *Engine) setOpWarnLatch(seq uint64, latch opWarnLatch) {
	if e.opWarned == nil {
		e.opWarned = make(map[uint64]opWarnLatch)
	}
	e.opWarned[seq] = latch
}

// chainCulprit identifies the self-failing HEAD of a blocked op's conflict chain - the op
// that keeps failing its own apply, as opposed to the successors merely serialised behind
// it - so noteOpBlocked's escalation Warn can name the op to unstick rather than a
// blameless victim. It is resolved once, the first time an op is
// found skipped behind a bad predecessor, and propagated down the chain so every further
// successor points at the same head rather than at its immediate neighbour.
type chainCulprit struct {
	seq  uint64
	typ  store.OpType
	path string
}

// noteOpBlocked records one more drain in which a queued op could not run because a
// conflicting predecessor is failing, escalating with a single Warn the moment the count
// reaches stuckThreshold - like noteOpFailure, but the op is a VICTIM, not the culprit: it
// never attempted its own apply, it is only serialised behind an op that keeps failing.
// The escalation names the head-of-chain (self-failing) op so an operator reading `status`
// sees which op to unstick, not the blameless successor queued behind it.
// It shares opFailures/StuckOps with noteOpFailure - a blocked op is genuinely
// wedged and belongs in the `Stuck` count - and clearOpFailures drops the entry the moment
// the op finally runs or leaves the queue. culprit is nil only when the blocking op lived
// in an earlier window of this drain pass (its conflict key carried forward via
// blockedKeys), so no in-batch head is available to name.
// Like noteOpFailure the escalation gate is "at or past the threshold, once per streak":
// both helpers bump the SAME counter, so an op that spent its first
// stuckThreshold drains blocked and only then started failing its own apply used to step over
// both single-drain gates and never emit either Warn - the last line in the log kept blaming a
// predecessor that had long since left the queue. The blocked latch is separate from the stuck
// one so each of the two roles is reported once per streak.
func (e *Engine) noteOpBlocked(qo store.QueuedOp, culprit *chainCulprit) {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	e.opFailures[qo.Seq]++
	if e.opFailures[qo.Seq] < stuckThreshold {
		return
	}
	latch := e.opWarned[qo.Seq]
	if latch.blocked {
		return
	}
	latch.blocked = true
	e.setOpWarnLatch(qo.Seq, latch)
	failures := e.opFailures[qo.Seq]
	if culprit == nil {
		e.log.Warn("engine: op stuck: blocked behind a conflicting op from an earlier drain window that keeps failing",
			"type", qo.Type, "direction", qo.Direction, "view", qo.ViewID,
			"path", qo.Path, "seq", qo.Seq, "failures", failures)
		return
	}
	e.log.Warn("engine: op stuck: blocked behind a conflicting op that keeps failing",
		"type", qo.Type, "direction", qo.Direction, "view", qo.ViewID,
		"path", qo.Path, "seq", qo.Seq, "failures", failures,
		"blocked_by_seq", culprit.seq, "blocked_by_type", culprit.typ, "blocked_by_path", culprit.path)
}

// clearOpFailures drops an op's failure counter once it leaves the queue - applied and
// acked, dead-lettered, or dropped as superseded by reconcile - so StuckOps self-heals
// and the counter map cannot outgrow the live queue.
func (e *Engine) clearOpFailures(seq uint64) {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	delete(e.opFailures, seq)
	// The deterministic-server-rejection streak is the same kind of per-op bookkeeping and
	// leaves the queue with the op - and so does the retry pause armed
	// from that streak (opbackoff.go, finding H4): the op is gone, so a re-used seq must not
	// inherit its pacing any more than it inherits its streak.
	delete(e.opServerErrs, seq)
	delete(e.opRetryAfter, seq)
	// The escalation Warns are latched per streak, so they leave with the streak: an op that
	// converges and later wedges again is a new wedge and deserves the log lines again.
	delete(e.opWarned, seq)
}

// carryDoom records that a doomed rename could not have its doom RESOLVED this pass (the
// stranded-baseline cleanup or the atomic trace+ack failed at the store level, e.g. ENOSPC),
// keyed exactly like deadOutputs - the moved node's file id plus the resolved on-disk key its
// source was to be materialised at.
//
// It exists because the doom's evidence is pass-local while the rename that carries it is
// durable: the feeder that proved the node never reached the source has already left the queue
// with its own trace, and both the in-window feeder scan and the deadOutputs set die with the
// drainQueue call. A doom that failed to be recorded therefore used to evaporate at the pass
// boundary, and the next pass's applyRename - finding no feeder, no dead mark and (depending on
// which arm failed) either a stranded pending baseline or none at all - moved whatever same-kind
// untracked entity sat at the source onto the destination and stamped it with the moved node's
// identity.
//
// The mark is released the moment the doom IS resolved, and by the same "a live op of this node
// at this key clears it" rule deadOutputs uses, so a producer that later genuinely materialises
// the node does not leave a legitimate rename doomed forever. It is in-memory only, like the
// stuck counters: a daemon restart loses it, which leaves the narrow original haul window open
// across a restart (DESIGN §13) - the durable half would need a field on the queued op, i.e. a
// schema bump.
func (e *Engine) carryDoom(fileID, key string) {
	if fileID == "" || key == "" {
		return
	}
	e.doomMu.Lock()
	defer e.doomMu.Unlock()
	if e.carriedDooms == nil {
		e.carriedDooms = make(map[string]bool)
	}
	e.carriedDooms[fileID+"\x00"+key] = true
}

// carriedDoom reports whether an earlier pass left an unresolved doom for this node at this key.
func (e *Engine) carriedDoom(fileID, key string) bool {
	if fileID == "" || key == "" {
		return false
	}
	e.doomMu.Lock()
	defer e.doomMu.Unlock()
	return e.carriedDooms[fileID+"\x00"+key]
}

// clearCarriedDoom releases a carried doom: either it was finally recorded (the rename left the
// queue with its trace) or a live op of the same node at the same key superseded it.
func (e *Engine) clearCarriedDoom(fileID, key string) {
	if fileID == "" || key == "" {
		return
	}
	e.doomMu.Lock()
	defer e.doomMu.Unlock()
	delete(e.carriedDooms, fileID+"\x00"+key)
}

// dropDoomedRename resolves a doomed rename's stranded state: it drops the pending baseline
// reconcile relocated for the never-materialised node. The dead-letter trace and the queue
// ack are NOT done here - drainBatch records them via deadLetterAndAck only after this
// cleanup succeeds, so a cleanup failure cannot leave a trace behind that a retry then
// double-counts. It is a package var so a test can inject a store-level failure
// (ENOSPC/corruption) and assert the failure marks the op bad - keeping a conflicting
// successor from applying out of order on the same path. The default delegates to the store;
// the doomed branch in drainBatch is the only caller.
var dropDoomedRename = func(e *Engine, qo store.QueuedOp) error {
	return e.dropStrandedBaseline(qo.ViewID, qo.FileID)
}

// cleanupRejected resolves the pending baseline a dead-lettered (unappliable-path) op left
// behind, dispatched by op type, because each strands a different thing:
//   - download/mkdir: the node was never put on disk, so the pending baseline is a phantom.
//     Drop it (following any same-batch relocation through byid), or a later move resolves
//     through it (nodeTracked==true), skips applyRename's foreign-source guard and drags
//     unrelated content onto its destination.
//   - rename: reconcile relocated the moved node's baseline to the (rejected) destination, but
//     the on-disk move never ran, so the source file is still in place. Dropping the relocated
//     record outright would orphan that real file; move the baseline back to the source when the
//     file is there, dropping it only when nothing is on disk.
//   - remove: the node is gone server-side (the op came from a delete). Since the op is acked,
//     leaving a tracked record behind would strand it forever with nothing to retry - drop the
//     pending record this remove targeted, and, if a prior rejected rename re-tracked the same
//     file id at a still-reachable path, honour the delete there too.
//
// It is a package var so a test can inject a store-level failure (ENOSPC/corruption) and assert
// drainBatch routes it through the disk-full latch.
var cleanupRejected = func(e *Engine, ctx context.Context, op store.Op) error {
	switch op.Type {
	case store.OpRename:
		return e.resolveRejectedRename(op)
	case store.OpRemove:
		return e.dropRejectedRemoveBaseline(ctx, op)
	default:
		return e.dropStrandedBaseline(op.ViewID, op.FileID)
	}
}

// dropStrandedDownloadBaseline is the errDownloadNodeGone arm's phantom-baseline cleanup:
// a download whose node is positively gone server-side had its pending baseline dropped
// (following any same-batch relocation through byid). It is a package var, like the two
// cleanup seams above, so a test can inject a store-level failure (ENOSPC/corruption) and
// assert applyOp routes it through the disk-full latch too. The default
// delegates to the shared helper; the errDownloadNodeGone arm is the only caller.
var dropStrandedDownloadBaseline = func(e *Engine, viewID uint64, fileID string) error {
	return e.dropStrandedBaseline(viewID, fileID)
}

// drainQueue applies every operation currently in the inbound durable queue to
// local disk, preserving FIFO (ascending sync_id) order between ops whose paths
// overlap while parallelising independent ones. Each op is dispatched to its own
// goroutine that first waits for every earlier op it conflicts with, so conflicting
// ops never overlap and always run in enqueue order, while independent paths proceed
// concurrently (bounded by the worker pool).
//
// Two ops conflict when one touches a resolved on-disk path that equals, contains or
// is contained by a path the other touches (see keysConflict). A rename touches both
// its source and its destination, and a download to a/b conflicts with a remove of a,
// so neither a later create at a rename's source nor an ancestor/descendant pair can
// ever apply out of order. Keys are the op's path resolved under the sync root (view
// directory + view-relative path), so identical relative paths in different views with
// distinct on-disk directories are independent and run concurrently, while two views
// that resolve to the same directory serialise.
//
// When an op fails, every later op that conflicts with it (transitively, through the
// chain of predecessors, and across window boundaries through the blocked-key set) is
// skipped (left un-Acked) so a failure never lets a dependent op apply out of order;
// independent paths still complete. Skipped and failed ops remain in the queue for the
// next drain.
//
// The queue is drained in bounded batches of limits.DrainBatch ops so a large backlog (an
// offline accumulation, or a cold-start catalog pull) cannot spawn an unbounded
// number of goroutines, hold an unbounded number of disk-only operations in flight, or
// drive the O(n²) predecessor scan over the whole queue at once. Each batch fully
// settles before the next begins, and the read cursor advances past every batch -
// including one that left ops queued (a transient failure, its skipped conflicting
// successors, or an absent-view suppression) - so a head window saturated with
// permanently-failing ops can no longer be re-read forever while every op behind it
// (including other, healthy views) starves. Ordering across
// window boundaries is preserved by the blocked-key set: the conflict keys of every op
// a window left queued carry forward, and a later window's op that overlaps them is
// skipped (left queued) exactly as an in-batch conflicting successor of a failed op
// is, so conflicting ops still apply strictly in enqueue order. The first error is
// remembered and returned once the whole queue has been paged; paging stops early on a
// terminal error (returned directly, so an earlier window's transient failure never
// masks it) or a cancelled ctx, where continuing would only fail every remaining op
// against a dead pool.
func (e *Engine) drainQueue(ctx context.Context) error {
	// One pass = one round of retries for every queued op, which is the unit the absence
	// corroboration counts its agreeing probes in (absencestreak.go): two path-errors inside
	// ONE apply are one server condition observed twice, not the repetition a verdict rests on.
	e.beginDrainPass()
	before, beforeBytes, _ := e.progress.snapshot()
	err := e.drainQueueWindows(ctx)
	// One line per pass that MOVED something, so a long sync leaves a trail in the log an
	// operator can read after the fact - not only in a `status` they had to be watching.
	// A pass that applied nothing says nothing, so an idle daemon stays quiet.
	if ops, bytes, window := e.progress.snapshot(); ops > before {
		e.log.Info("engine: drain pass applied work",
			"ops", ops-before, "bytes", bytes-beforeBytes,
			"total_ops", ops, "total_bytes", bytes, "moving_for", window.Round(time.Second))
	}
	// The queue readers above quarantine any record whose key or value no longer decodes,
	// dropping it WITHOUT being able to name the baseline it left pending.
	// Re-derive those owners now, while the pass's apply goroutines have settled
	// and nothing else is mutating the queue. The sweep is store- and disk-only, so it runs
	// even when the pass failed - but never masks that failure, and never runs on a
	// cancelled context, where its enqueues would only be re-derived next pass anyway.
	if ctx.Err() == nil {
		if rerr := e.repairQuarantinedBaselines(ctx); rerr != nil {
			if err == nil {
				return rerr
			}
			e.log.Warn("engine: repairing baselines stranded by a quarantined queue record failed; retrying next drain", "err", rerr)
		}
	}
	return err
}

// drainQueueWindows is drainQueue's paging loop: it reads the queue one bounded window at
// a time, applies each window through drainBatch and advances the cursor past it. It is
// split out so drainQueue has a single place to run the post-pass quarantine repair, on
// every exit - including the early returns for a store read failure and a terminal error.
func (e *Engine) drainQueueWindows(ctx context.Context) error {
	var (
		afterSeq uint64
		firstErr error
		blocked  blockedKeys
		dead     deadOutputs
	)
	for {
		// A streaming cold start hands over what its walk listed since the last window, so the
		// pass transfers it without waiting for the walk to end (coldstartstream.go).
		if e.coldIngest != nil && e.coldIngest() {
			return firstErr
		}
		// Read at most one window from the queue rather than the whole backlog: each
		// pass Acks the ops it applies, and the cursor pages past the ops it leaves
		// queued. Loading everything here would re-decode the entire remaining queue on
		// every pass, turning a large backlog into quadratic scanning and unbounded
		// memory.
		ops, err := e.store.PendingOpsNFrom(afterSeq, e.limits.DrainBatch)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("engine: read queue: %w", err)
			}
			return firstErr
		}
		if len(ops) == 0 {
			return firstErr
		}
		if err := e.drainBatch(ctx, ops, &blocked, &dead); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// Stop paging on a terminal error from THIS batch, not on firstErr: an
			// earlier window's transient failure would otherwise mask a later window's
			// terminal error (a paused/closed pool), so isTerminal(firstErr) read the
			// stale transient and Run kept retrying instead of stopping to re-pair.
			// Return the terminal error itself so Run's isTerminal check sees it.
			if isTerminal(err) {
				return err
			}
		}
		if ctx.Err() != nil {
			return firstErr
		}
		afterSeq = ops[len(ops)-1].Seq
	}
}

// blockedKeys accumulates, across the windows of one drainQueue pass, the conflict
// keys of every op a window left queued - a transient failure, a successor skipped
// behind one, or an absent-view suppression - so a later window's op that overlaps
// any of them is skipped rather than applied out of order (the
// cursor now pages past a failing window instead of stopping the drain, so this set
// is what carries the enqueue-order invariant across window boundaries). Membership
// is the serialize.go overlap test - equals, contains or is contained by - answered
// in O(path depth) per key via two indexes: exact holds every blocked key (hit when
// a blocked key equals or is an ancestor of the probe, found by walking the probe's
// ancestor chain), and anc holds every strict ancestor of a blocked key (hit when
// the probe itself is an ancestor of a blocked key), so a saturated multi-thousand-op
// queue does not turn the per-op check into a full pairwise scan. The zero value is
// ready to use.
type blockedKeys struct {
	exact map[string]bool // every blocked key
	anc   map[string]bool // every strict ancestor of a blocked key
}

// add records an op's conflict keys (cleanKey-anchored, as serializeKeys produces)
// as blocked for the remainder of the drain pass.
func (b *blockedKeys) add(keys []string) {
	if b.exact == nil {
		b.exact = make(map[string]bool)
		b.anc = make(map[string]bool)
	}
	sep := string(filepath.Separator)
	for _, k := range keys {
		if b.exact[k] {
			continue
		}
		b.exact[k] = true
		for k != sep {
			if i := strings.LastIndex(k, sep); i > 0 {
				k = k[:i]
			} else {
				k = sep
			}
			if b.anc[k] {
				break // this ancestor chain is already recorded
			}
			b.anc[k] = true
		}
	}
}

// conflicts reports whether any of an op's conflict keys overlaps a blocked key
// under the serialize.go invariant (pathsOverlap: equal, contains or contained by).
func (b *blockedKeys) conflicts(keys []string) bool {
	if len(b.exact) == 0 {
		return false
	}
	sep := string(filepath.Separator)
	for _, k := range keys {
		if b.anc[k] {
			return true // k is a strict ancestor of a blocked key
		}
		for {
			if b.exact[k] {
				return true // a blocked key equals or is an ancestor of the probe
			}
			if k == sep {
				break
			}
			if i := strings.LastIndex(k, sep); i > 0 {
				k = k[:i]
			} else {
				k = sep
			}
		}
	}
	return false
}

// deadOutputs carries, across the windows of one drainQueue pass, the (file id, output
// key) pairs of the ops that left the queue DEAD - dead-lettered or doomed - so a rename
// scheduled in a LATER window whose source is one of those outputs is doomed exactly as an
// in-window successor of a dead feeder is.
//
// Without it the doom stopped at the window boundary: blockedKeys carries
// only the keys of ops a window left QUEUED (bad), and drainBatch's feeder lookup only
// scans the ops scheduled in the CURRENT window, so a dead op's mark was visible to
// neither. A DrainBatch boundary falling between a download/mkdir (or an earlier hop of a
// move chain) and the rename it feeds therefore left that rename with no feeder and no
// block: it fell through to applyRename's own guards, and a same-kind untracked local file
// sitting at its source passes them (byid miss + matching kind), so fsutil.Rename hauled
// the user's file onto the destination and stamped it with the moved node's identity as a
// bare synced record. The content survives (it is misplaced, and the next scan re-discovers
// it), but the move is unrelated to anything the server asked for.
//
// An op that later SUCCEEDS at the same (file id, output key) clears the mark: it really
// did materialise the node there, so a rename fed by it is legitimate. A transiently failed
// op needs no clearing - it stays queued, so blockedKeys already skips its successors.
// Membership is keyed by file id first because that is what makes an op a rename's FEEDER
// (the same node), not merely a path-overlapping neighbour. The zero value is ready to use.
type deadOutputs struct {
	keys map[string]map[string]bool // file id -> set of resolved output keys
}

// add records that an op with this file id left the queue dead after failing to place its
// node at key.
func (d *deadOutputs) add(fileID, key string) {
	if d.keys == nil {
		d.keys = make(map[string]map[string]bool)
	}
	set := d.keys[fileID]
	if set == nil {
		set = make(map[string]bool)
		d.keys[fileID] = set
	}
	set[key] = true
}

// clear drops a recorded dead output once a later op of the same node genuinely placed it
// at key, so a rename fed by that op is not wrongly doomed.
func (d *deadOutputs) clear(fileID, key string) {
	set := d.keys[fileID]
	if set == nil {
		return
	}
	delete(set, key)
	if len(set) == 0 {
		delete(d.keys, fileID)
	}
}

// has reports whether an earlier window of this drain pass left this node's output at key
// dead - i.e. nothing of ours reached that path.
func (d *deadOutputs) has(fileID, key string) bool {
	return d.keys[fileID][key]
}

// maxDrainBatch is the default for limits.DrainBatch: it bounds how many queued ops
// drainQueue schedules concurrently in one pass, capping in-flight goroutines,
// concurrent disk-only operations and the per-pass predecessor scan regardless of how
// large the durable queue has grown. The effective value is config-tunable
// (config.Limits.DrainBatch); this constant is the safe default and the test oracle.
const maxDrainBatch = 256

// drainBatch applies one batch of queued ops (already in ascending-seq order),
// serialising conflicting ops and parallelising independent ones as described on
// drainQueue. It returns the first error once every goroutine has settled; on a nil
// return every op in the batch was applied, dead-lettered or doomed (acked) - or left
// queued without a failure (skipped behind a prior window's blocked key, or
// suppressed because its view is marked absent).
//
// Two outcomes propagate down a chain. A transient FAILURE marks the op bad, so every
// conflicting successor skips and stays queued to retry next drain (ordering preserved).
// A DEAD-LETTER (rejected path/symlink) or a DOOM marks the op dead: a rename that fed
// off a dead op - i.e. whose on-disk source was to be produced by it - is itself doomed
// and dropped, because the node never reached the source path, so re-running the rename
// would drag whatever unrelated bytes sit there onto the destination and stamp them with
// the moved node's identity. A download/mkdir successor re-materialises the
// node from the server, so it is never doomed by a dead feeder; only renames, which trust
// their on-disk source, are.
//
// blocked carries the enqueue-order invariant across the windows of one drainQueue
// pass: an op whose keys overlap a key an earlier window left
// queued is skipped exactly like the in-batch successor of a failed op (left queued,
// no failure counted), and every op THIS batch leaves queued has its keys added on
// the way out so later windows skip its successors in turn. dead carries the DOOM the
// same way: the (file id, output key) of every op this batch dropped
// without placing its node, so a rename fed by it in a later window is doomed rather
// than trusting whatever unrelated content sits at its source.
//
// An op whose view is marked absent is
// suppressed the same way: it was enqueued before the server revoked the view, so
// every verb it would run is doomed to a spurious rejection - before this skip such
// ops retried transiently forever, and a head window saturated with them starved the
// whole queue. It does not run, does not count as a failure (no stuck escalation) and
// is not deleted - the view may reappear (the periodic re-enumeration clears
// the mark), at which point the ops drain normally again. The suppression is Warned
// once per absence transition, not per drain.
func (e *Engine) drainBatch(ctx context.Context, ops []store.QueuedOp, blocked *blockedKeys, dead *deadOutputs) error {
	var (
		wg sync.WaitGroup
		mu sync.Mutex
		// firstErr is set-once: the first error any goroutine reports (in non-deterministic
		// completion order) is returned; a later error in the SAME batch is dropped. This can
		// mask a TERMINAL error that fires after a transient one - the batch returns the
		// transient (retryable) error, so drainQueue/Run retry instead of stopping.
		// Accepted, not fixed: the masking is bounded to one drain cycle. A truly
		// terminal condition (a vanished session, a closed pool) re-surfaces on the next
		// pollOnce/EventClient turn - those channels are not batched behind firstErr - so Run
		// stops on the following cycle at worst; and every op that failed this batch stays
		// queued, so nothing is lost by the extra retry. The set-once contract also keeps the
		// return deterministic-in-outcome (one error, not an arbitrary join) for the caller's
		// terminal classification.
		firstErr error
	)
	// scheduled records, in enqueue order, the op, its conflict keys and completion
	// channel of every op dispatched so far. It is built sequentially in this goroutine
	// so the conflict chaining is deterministic; each spawned goroutine only reads the
	// predecessors (and feeder) captured for it.
	scheduled := make([]*scheduledOp, 0, len(ops))
	dirs := make(map[uint64]string) // per-view on-disk directory, resolved once
	// slots bounds how many of this batch's applies contend for a worker at once; see the
	// apply below.
	slots := make(chan struct{}, 2*max(1, e.limits.Workers))

	// outputKey is the resolved on-disk key an op places its node at - a rename's
	// destination, a download/mkdir's path - and whether the op places a node at all (a
	// remove does not). It is the key a later rename's source must match for that op to be
	// the rename's feeder.
	//
	// It is derived from the op's DECLARED path (opSerializeKeys), so it holds only while
	// the apply path writes where the op says. applyDownload's self-echo redirect
	// (inbound_download.go) is the one place that rebases op.Path at apply time, and it
	// rebases onto the node's byid path - which opSerializeKeys already folded into this
	// op's key set, so the redirected write lands on a key the scheduler ordered this op
	// against and a feeder lookup keyed on it still matches. A future redirect to a path
	// OUTSIDE the op's serialize keys would break that: the download would place the node
	// at a key no rename can find (its feeder silently lost) while this stale key wrongly
	// claims the node arrived there. Any such redirect must extend opSerializeKeys first.
	outputKey := func(s *scheduledOp) (string, bool) {
		switch s.op.Type {
		case store.OpRename:
			return s.keys[len(s.keys)-1], true
		case store.OpDownload, store.OpMkdir:
			return s.keys[0], true
		default:
			return "", false
		}
	}

	for _, qo := range ops {
		keys := e.opSerializeKeys(e.viewDir(qo.ViewID, dirs), qo.Op)
		s := &scheduledOp{op: qo.Op, seq: qo.Seq, keys: keys, done: make(chan struct{})}
		// An absent-marked view's ops are suppressed, not run (see the func doc). The
		// check runs here on the scheduling goroutine - the engine loop's own - because
		// absentViews is loop-serial state (no lock; see the field doc). Warn once per
		// absence transition: reconcileAbsentViews clears the warned mark when the view
		// reappears, so a later revocation Warns again.
		suppress := e.viewSuppressed(qo.ViewID)
		if suppress && !e.absentQueueWarned[qo.ViewID] {
			e.absentQueueWarned[qo.ViewID] = true
			// Two causes reach this gate - the server stopped enumerating the view,
			// or the operator did not select it - and reporting the first for both
			// sent an operator looking for a server problem they do not have.
			e.log.Warn("engine: suppressing queued ops of a "+suppressedViewReason(e, qo.ViewID)+"; they stay queued and resume if it is synced again",
				"view", qo.ViewID)
		}
		// An op overlapping a key an earlier window of this drain pass left queued must
		// not apply out of order ahead of it; skip it exactly like the in-batch
		// successor of a failed op (left queued, no failure counted).
		preBlocked := blocked.conflicts(keys)
		// Chain this op behind the conflicting ops already scheduled, keeping only a
		// minimal covering set: when a newer conflicting predecessor itself conflicts
		// with one already collected, the newer one transitively waits on the older, so
		// the older link can be dropped. A run of same-path ops therefore chains
		// linearly (O(n) links retained) instead of each op retaining every prior one
		// (O(n²)), which a remote streaming many same-path events could otherwise use to
		// exhaust memory.
		var preds []*scheduledOp
		for _, prior := range scheduled {
			if !keysConflict(prior.keys, keys) {
				continue
			}
			kept := preds[:0]
			for _, p := range preds {
				if !keysConflict(p.keys, prior.keys) {
					kept = append(kept, p)
				}
			}
			preds = append(kept, prior)
		}
		// A rename depends on its source path holding the very node it moves. Find that
		// node's feeder: the latest prior op with the same (non-empty) file id whose output
		// path is this rename's source. If the feeder is dead - dead-lettered or itself
		// doomed - the node never reached the source, so the rename is dropped rather than
		// applied (see the type doc). The feeder always conflicts with this op (its output
		// equals our source), so it is transitively a predecessor and its done channel is
		// closed before we read its dead flag.
		var feeder *scheduledOp
		// deadFeeder is the cross-window half of the same rule: an EARLIER window of this
		// drain pass dropped an op of this node that was to place it at our source, and the
		// dead mark left with that window's scheduled slice. Consulted only when no in-window
		// producer exists - a later producer supersedes an earlier drop, and the in-window
		// feeder is by construction the latest one.
		deadFeeder := false
		// Only an INBOUND rename trusts an on-disk source materialised by a feeder op
		// (a download/mkdir of the moved node). An outbound rename's source is the local
		// file itself, produced by no queued op, so it has no feeder - and must never be
		// doomed by an unrelated same-file-id op that happens to share its source key (e.g.
		// an inbound op for the same node in the same batch), which would wrongly drop the
		// pending baseline localReconcile already relocated and trigger a duplicate re-upload.
		if qo.Direction == store.DirInbound && qo.Type == store.OpRename && qo.FileID != "" {
			srcKey := keys[0]
			for _, prior := range scheduled {
				if prior.op.FileID != qo.FileID {
					continue
				}
				// Only an INBOUND producer materialises the node at its output key - it is
				// the op that puts the server's bytes there. An OUTBOUND op of the same node
				// (a push of a local rename/mkdir the user already performed) reads a path
				// that is ALREADY on disk and never creates it, so its death says nothing
				// about whether our source holds the node: treating it as a feeder would doom
				// a perfectly applicable inbound rename - dead-letter trace plus the
				// relocated baseline dropped - over a failed PUSH.
				if prior.op.Direction != store.DirInbound {
					continue
				}
				if out, ok := outputKey(prior); ok && out == srcKey {
					feeder = prior // latest matching producer wins
				}
			}
			if feeder == nil {
				// carriedDoom is the CROSS-PASS half of the same rule: a
				// previous drain pass proved this rename doomed but could not record the doom
				// (a store-level failure in the cleanup or the trace+ack), so the rename is
				// still queued while its feeder - the only evidence of the doom - has long
				// left the queue. Without it the doom evaporated at the pass boundary.
				deadFeeder = dead.has(qo.FileID, srcKey) || e.carriedDoom(qo.FileID, srcKey)
			}
		}
		scheduled = append(scheduled, s)
		wg.Add(1)
		go func(qo store.QueuedOp, s *scheduledOp, preds []*scheduledOp, feeder *scheduledOp, suppress, preBlocked, deadFeeder bool) {
			defer wg.Done()
			defer close(s.done)
			for _, p := range preds {
				<-p.done
			}
			if feeder != nil {
				<-feeder.done
			}
			mu.Lock()
			if suppress {
				// The op's view is absent: leave it queued without running it or
				// counting a failure, and mark it bad so a conflicting successor (in
				// this batch via preds, in a later window via blocked) skips behind it
				// rather than applying out of order. Checked ahead of the doomed branch:
				// dooming would mutate the store/queue of a view whose lifecycle is
				// currently unknowable.
				s.bad = true
				mu.Unlock()
				return
			}
			doomed := deadFeeder || (feeder != nil && feeder.dead)
			skip := false
			if !doomed {
				// Prefer an in-batch bad predecessor over a cross-window preBlocked mark:
				// the former carries a *sched whose head-of-chain culprit we can name in
				// the escalation Warn, the latter only a conflict key from an earlier
				// window. Either way s.bad is set, so the skip is identical.
				for _, p := range preds {
					if p.bad {
						s.bad = true
						// Point at the self-failing HEAD of the chain: an already-blocked
						// predecessor forwards the head it recorded; a self-failing one is
						// itself the head.
						if p.culprit != nil {
							s.culprit = p.culprit
						} else {
							s.culprit = &chainCulprit{seq: p.seq, typ: p.op.Type, path: p.op.Path}
						}
						break
					}
				}
				if !s.bad && preBlocked {
					s.bad = true // culprit stays nil: the head lived in an earlier window
				}
				skip = s.bad
			}
			if doomed {
				s.dead = true
			}
			culprit := s.culprit
			mu.Unlock()
			if doomed {
				// The op that should have placed this node at the rename's source was
				// dead-lettered or doomed, so the source does not hold our node. Drop the
				// rename instead of moving whatever unrelated bytes sit there.
				e.log.Warn("engine: dropping rename whose feeding op was dead-lettered",
					"view", qo.ViewID, "from", qo.Path, "to", qo.NewPath, "file_id", qo.FileID)
				// Reconcile relocated the moved node's baseline to its (never-reached)
				// destination before the drain ran. The node was never materialised on disk, so
				// drop that stranded pending baseline FIRST: otherwise the byid index keeps
				// pointing at a node that does not exist, and a later move (in this or a future
				// batch) would resolve through it and misidentify unrelated destination content
				// (which is also what makes the cross-batch move chain reachable).
				// Only after the cleanup succeeds is the trace recorded and the op acked
				// atomically, so a cleanup failure leaves no trace for a retry to double-count.
				if err := dropDoomedRename(e, qo); err != nil {
					// The cleanup failed at the store level, possibly with ENOSPC on a full disk:
					// route it through the disk-full latch so `status` shows `Disk: full` instead of
					// retrying silently, matching applyOp's apply-error site. The
					// returned error is unchanged.
					err = e.noteApplyResult(qo.Op, err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					// The cleanup failed (a store-level ENOSPC/corruption), so this op is not
					// actually resolved: the stranded baseline may still be indexed and the queue
					// record un-acked. Mark it bad (mirroring the transient-failure branch) so a
					// conflicting successor in this batch skips rather than applying out of order on
					// the same path. No trace was written.
					s.bad = true
					// ...and NOT dead: the op is still in the queue, so the post-loop doctrine
					// below (a still-queued op clears, never arms, the cross-window doom) applies
					// to it exactly as it does to a transiently failed apply.
					s.dead = false
					s.carried = s.keys[0]
					mu.Unlock()
					// The doom itself must survive this pass, which is what "retried on the next
					// drain" used to assume without any mechanism behind it: the evidence for the
					// doom is the FEEDER's death, and the feeder left the queue in THIS pass (its
					// trace is written, its record acked), while the in-window feeder scan and the
					// deadOutputs set are both pass-local. On the next pass the rename would find
					// feeder == nil and dead.has() == false and fall through to applyRename, where
					// a same-kind untracked entity at the source passes the remaining guards and
					// gets hauled onto the destination under the moved node's identity - exactly
					// the haul deadOutputs exists to prevent.
					e.carryDoom(qo.FileID, s.keys[0])
					e.noteOpFailure(qo, err)
					return
				}
				// A doomed rename leaves its node exactly as absent from the replica as the
				// gone-source drop does - and it is the arm a same-node CHAIN ends on, since the
				// first hop defers its own repair to the still-queued second one. So it carries
				// the same compensation: re-request the node at this hop's destination
				// (finding H2). The compensation is resolved AFTER dropDoomedRename, whose
				// byid-resolved cleanup decides whether the node is still tracked elsewhere -
				// which is what renameCompensation reads to skip a repair that would evict it.
				// The KIND rests on the move event alone here, and deliberately on NOTHING else.
				// This arm holds no baseline that identifies the moved node: the only record that
				// ever did was the one reconcile relocated to the destination, and the feeder -
				// matched to this rename by the same file id - had its own dead-letter cleanup
				// drop it through byid before this line runs. op.MovedBaseIsDir is NOT a
				// substitute: reconcile captures it through a path fallback whenever the file id
				// misses byid, which is this arm's own precondition, so it describes whatever node
				// sat at the source path (see compensatedKind). An unprovable kind therefore stays
				// false and the repair is a download - the failure direction that can only stall
				// (observably, paced) and that errDownloadNodeIsDir converts into the mkdir once
				// the server has placed the kind, never the mkdir guessed here, which would
				// record a server FILE as a local directory and later push an outbound remove
				// of it.
				comp, track, err := e.compensateDroppedRename(dropRenameCompensated(qo.Op, compensatedKind(qo.Op, false)), qo)
				if err == nil {
					err = e.deadLetterCompensateAndAck(qo.Op, qo.Seq,
						droppedRenameReason("rename feeder dead-lettered", comp), comp, track)
				}
				if err != nil {
					// A store-level compensation or record+ack failure can be ENOSPC on a full
					// disk: latch it (mirroring the dropDoomedRename sibling above) so `status`
					// shows `Disk: full` instead of retrying silently.
					err = e.noteApplyResult(qo.Op, err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					// The atomic record+ack failed: no trace was written and the op is still
					// queued. Mark it bad so a conflicting successor skips; the cleanup above is
					// idempotent, so the next drain re-runs it and retries the record+ack.
					s.bad = true
					// Still queued, so not dead - see the sibling arm above.
					s.dead = false
					s.carried = s.keys[0]
					mu.Unlock()
					// Carry the doom across the pass for the same reason as the sibling arm: the
					// dropped baseline no longer names our node, so the next pass's applyRename
					// would treat a same-kind untracked source as a possible not-yet-tracked node
					// of ours and move it.
					e.carryDoom(qo.FileID, s.keys[0])
					e.noteOpFailure(qo, err)
					return
				}
				// The doomed rename left the queue (trace + ack); drop any stuck-counter
				// entry it accumulated in earlier drains so it cannot linger as a false
				// Stuck count, and release any carried doom a previous
				// pass recorded for it - the doom is resolved, so keeping the mark would
				// wrongly doom a future legitimate rename of the same node from that path.
				e.clearOpFailures(qo.Seq)
				e.clearCarriedDoom(qo.FileID, s.keys[0])
				return
			}
			if skip {
				// A conflicting predecessor failed or was skipped; do not apply out
				// of order. Leave this op queued for the next drain - but count the
				// blocked drain so a victim wedged behind a permanently-failing op
				// surfaces in `status` too, distinguished by a Warn that names the
				// head-of-chain culprit rather than blaming the successor.
				e.noteOpBlocked(qo, culprit)
				return
			}
			if left, paused := e.opRetryPending(qo.Seq); paused {
				// This op's failures have repeated with the identical server result code, so a
				// geometric pause was armed before its next attempt (opbackoff.go, finding H4).
				// The pause has not elapsed: leave the op queued WITHOUT attempting it and
				// without counting a failure - it did not run, so it is neither newly stuck nor
				// newly proven deterministic. Mark it bad exactly as a transient failure does, so
				// its conflict chain keeps enqueue order (in-batch successors skip, later windows
				// skip on blockedKeys) rather than applying past a still-pending op. It is not an
				// error for the BATCH: a paced retry is the intended outcome, and reporting one
				// would make every cycle log a failure it deliberately deferred.
				e.log.Debug("engine: deferring retry of a deterministically rejected op",
					"type", qo.Type, "direction", qo.Direction, "view", qo.ViewID,
					"path", qo.Path, "seq", qo.Seq, "retry_in", left)
				mu.Lock()
				s.bad = true
				mu.Unlock()
				return
			}
			// At most two applies per worker contend for the pool at a time. The pool serves
			// waiting leases in arrival order, so a window of goroutines all parked in Acquire
			// put every other lease - a streaming cold start's next listing above all - behind
			// the whole window: measured on the stand, the walk fell from ~300 to ~30
			// entries/s. Two, not one: an apply keeps its slot through its LOCAL work too
			// (write, fsync, commit), and with one per worker the worker sits idle through it.
			// Taken AFTER the predecessors are done, never before: a slot held while waiting on a
			// predecessor that needs a slot would deadlock the batch.
			held := false
			select {
			case slots <- struct{}{}:
				held = true
			case <-ctx.Done():
				// applyOp sees the same context and fails at once; it still runs, so the
				// outcome bookkeeping below stays the one place a failure is recorded.
			}
			err := e.applyOp(ctx, qo)
			if held {
				<-slots
			}
			if err == nil {
				// The op resolved (applied+acked, or acked by its own conflict funnel):
				// clear its stuck counter so StuckOps self-heals.
				e.clearOpFailures(qo.Seq)
				return
			}
			if errors.Is(err, errOpReplaced) {
				// The op left the queue but its node did not: a compensating op enqueued in the
				// same transaction places it at the same output key. So it takes the STILL-QUEUED
				// treatment - stuck counter cleared (this op is gone), keys blocked so its
				// overlapping successors skip this pass rather than applying past a node that has
				// not landed yet, and NO dead mark, since the doom would drop a dependent rename
				// permanently over work that is merely pending. Not an error for the batch, like
				// the paced-retry deferral above.
				e.clearOpFailures(qo.Seq)
				mu.Lock()
				s.bad = true
				mu.Unlock()
				return
			}
			if errors.Is(err, errDeadLettered) {
				// The op was acked and dropped (rejected path). It did not fail, but it
				// did not apply either: mark it dead so a rename feeding off it is doomed.
				// It left the queue, so its stuck counter is cleared like a success.
				e.clearOpFailures(qo.Seq)
				mu.Lock()
				s.dead = true
				mu.Unlock()
				return
			}
			// The op failed transiently and stays queued for the next drain: bump its
			// stuck counter so one that never converges surfaces in `status` instead of
			// retrying invisibly forever.
			e.noteOpFailure(qo, err)
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			s.bad = true
			mu.Unlock()
		}(qo, s, preds, feeder, suppress, preBlocked, deadFeeder)
	}
	wg.Wait()
	e.settleBatchOutcomes(scheduled, blocked, dead, outputKey)
	return firstErr
}

// viewDir resolves the on-disk subdirectory name an op's view materialises in,
// memoising the lookup in cache so a drain reads each view once. It mirrors abs's
// name-or-numeric-id mapping (without abs's containment rejection - a malformed name is
// a fine serialisation key, and the op fails its own containment check at apply time),
// so two views that would land in the same directory produce overlapping keys and
// serialise. A view that cannot be read falls back to its numeric id.
func (e *Engine) viewDir(viewID uint64, cache map[uint64]string) string {
	if d, ok := cache[viewID]; ok {
		return d
	}
	// A flat layout puts the single selected view straight in the sync root, so
	// its directory component is empty. Serialisation keys stay correct because
	// exactly one view can be flat (config.Views.Validate), so no two views can
	// collapse onto the same key.
	d := strconv.FormatUint(viewID, 10)
	if v, err := e.store.GetView(viewID); err == nil && v.Name != "" {
		d = v.Name
	}
	// config.Views.Dir is the single answer to "which subdirectory", and it is
	// empty under a flat layout. Three resolvers compute this - here, abs() and
	// resolveViewDir - and the one that was missed deleted a whole view.
	d = e.views.Dir(d)
	cache[viewID] = d
	return d
}

// The op serialisation invariant - serializeKeys, cleanKey, keysConflict,
// pathsOverlap and isAncestor - lives in serialize.go.

// applyOp performs a single queued operation against local disk and, on success,
// advances its baseline to StateSynced and Acks the queue record. A failure leaves
// the record in place to retry on the next drain. An op whose path can never be applied
// (rejected view name/path or a pre-existing symlink) is dead-lettered - acked and
// dropped - and applyOp returns errDeadLettered so the caller can doom any rename that
// fed off it rather than treating the drop as a successful apply.
func (e *Engine) applyOp(ctx context.Context, qo store.QueuedOp) error {
	op := qo.Op
	// Keep-both conflict resolution touches both disk and the server and is
	// direction-agnostic, so route it ahead of the Direction split (Phase 6d,
	// conflict.go). It returns early with its own error mapping, so the ENOSPC
	// disk-full latch is applied centrally via noteApplyResult - otherwise a
	// disk-full keep-both resolve would retry without ever surfacing in `status`.
	if op.Type == store.OpResolveConflict {
		// The resolver makes the same push-class turns as an outbound apply (QueryNode,
		// Download, Upload) but leaves through this early return, ahead of the only site the
		// deterministic-rejection escalation was wired at - so a persistent UNMAPPED server
		// code from any of them fell into applyResolveConflict's transient fall-through and
		// retried every drain forever, with blockedKeys serialising the authoritative inbound
		// remove/rename of the same path behind it. Its terminal exit is
		// exactly the escalation's own (markOutboundConflict + durable trace + ack), the same
		// one this verb already uses for nameTooLong/errConflictServerGone.
		//
		// The byid-tracked spelling is adopted HERE rather than inside the resolver, so the
		// escalation's own baseline marking and dead-letter trace name the same path the
		// resolver's turns addressed - an adoption confined to the resolver's local copy would
		// leave the escalation writing markOutboundConflict at the stale key, the phantom
		// baseline whose file id collides with the real one that this adoption exists to prevent.
		rqo, aerr := e.adoptResolveSpelling(qo)
		if aerr != nil {
			return e.escalateDeterministicServerFailure(rqo, e.noteApplyResult(rqo.Op, aerr))
		}
		return e.escalateDeterministicServerFailure(rqo, e.noteApplyResult(rqo.Op, e.applyResolveConflict(ctx, rqo)))
	}
	// The outbound (local→server push) side is a separate apply path with its own
	// error mapping (a stale-base conflict is non-fatal, an unknown parent id leaves
	// the op queued): route it before the inbound switch (Phase 6c, outbound.go). Like
	// the keep-both path it returns early, so its disk-full latch also runs through
	// noteApplyResult.
	if op.Direction == store.DirOutbound {
		// escalateDeterministicServerFailure passes the result through unchanged until the
		// push has been rejected with one and the same UNMAPPED server result code on
		// unmappedServerErrDrains consecutive drains, and then dead-letters it: an unmapped
		// code has no typed sentinel and no terminal class, so such a push otherwise retried
		// forever with every overlapping successor serialised behind it.
		return e.escalateDeterministicServerFailure(qo, e.noteApplyResult(op, e.applyOutbound(ctx, qo)))
	}
	var err error
	switch op.Type {
	case store.OpDownload:
		err = e.applyDownload(ctx, op)
	case store.OpMkdir:
		err = e.applyMkdir(op)
	case store.OpRename:
		err = e.applyRename(op)
	case store.OpRemove:
		err = e.applyRemove(ctx, op)
	default:
		// An unknown op type can never be applied by any handler, so returning a plain
		// error would treat it as transient and retry forever, wedging the whole queue
		// (the same failure mode closed for EEXIST/ENOTDIR). It is currently unreachable
		// (reconcile only enqueues known types), but dead-letter it like a rejected path -
		// record a durable trace, Ack and signal the drop - so a future/corrupt op type
		// cannot starve every view.
		e.log.Warn("engine: dropping op with unknown type",
			"type", op.Type, "direction", op.Direction, "view", op.ViewID, "path", op.Path)
		// No baseline cleanup needed for an unknown type; record the trace and ack
		// atomically so a retry cannot double-count it.
		if ackErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("unknown op type %q", op.Type)); ackErr != nil {
			// A store-level record+ack failure can be ENOSPC on a full disk: latch it so
			// `status` shows `Disk: full` instead of retrying silently. The returned
			// error is unchanged.
			return e.noteApplyResult(op, ackErr)
		}
		return errDeadLettered
	}
	if err != nil {
		// A non-directory file occupying a path component (os.MkdirAll → syscall.ENOTDIR)
		// means a server child arrived under a path the local side holds as a regular file.
		// The canonical case is the cold-start file↔dir type mismatch (DESIGN §6): the server
		// holds a directory `foo` (with children) where the local tree holds a file `foo`; the
		// cold start surfaces `foo` as a keep-both conflict and defers its whole subtree, but a
		// later incremental event for a child such as `foo/bar` is reconciled without that
		// subtree knowledge and enqueues an inbound download/mkdir whose parent mkdir then hits
		// ENOTDIR. Under the never-delete policy the blocking local file never disappears on its
		// own, so the op can never succeed and retrying it forever would wedge the queue. Like a
		// rejected path, route it through the dead-letter path, leaving the parent's surfaced
		// conflict for the operator to resolve.
		//
		// The mirror case is file-over-dir: a server file arrives at a path the local side holds
		// as an untracked directory. On Unix the download promote (os.Rename(tmp, dir)) and an
		// inbound rename onto that path both fail with EEXIST/ENOTEMPTY - fs.ErrExist - but only
		// because Go's os.Rename carries a userspace dir-guard (os/file_unix.go Lstats the
		// destination and synthesises EEXIST for an existing directory before the syscall); the
		// no-replace RenameNoReplace paths get ERROR_ALREADY_EXISTS → fs.ErrExist on Windows too.
		// The REPLACING rename has no such guard on Windows: os.Rename goes straight to
		// MoveFileEx(MOVEFILE_REPLACE_EXISTING), which fails with ERROR_ACCESS_DENIED -
		// fs.ErrPermission, NOT fs.ErrExist - when the destination is a directory, so the errno
		// alone cannot classify the shape there. permissionBlockedByDirTarget
		// recognises it platform-independently: a file- OR dir-placing op whose Lstat'ed target
		// is an existing directory - the dir-placing verbs (a directory rename, the mkdir subtree
		// relocate) hit the very same Windows ACCESS_DENIED while Unix dead-letters them through
		// the fs.ErrExist arm. A permission failure with a file or absent target (a
		// read-only-attribute occupant, transient parent-ACL noise) deliberately stays transient
		// - see the helper's doc comment. The local directory never vanishes on its own under the
		// never-delete policy, so this op can never succeed either; dead-letter it on the same
		// path so a file-vs-dir divergence between clients cannot wedge every view forever.
		//
		// Both readings above rest on the occupant being PERMANENT, which is why the fs.ErrExist
		// test runs through existsBlocksForever rather than errors.Is directly:
		// Go folds the transient "directory not empty" refusal (ENOTEMPTY / ERROR_DIR_NOT_EMPTY)
		// into fs.ErrExist too, and that one is reported by a directory REMOVAL losing a race with
		// a concurrent create, not by an occupant that never clears.
		//
		// The third permanent shape is a path component the local filesystem cannot hold at
		// all - over the per-name limit, or (on Windows) spelled in a way the OS rejects.
		// DSM's limits are not the local ones, so a server name can arrive that no retry will
		// ever make appliable; nameTooLong recognises it per-OS. The
		// engine's own generated names are budgeted below the limit (stagingTempPattern,
		// conflictCopyName), so reaching here means the name came from the server.
		if errors.Is(err, errRejectedPath) || errors.Is(err, syscall.ENOTDIR) || existsBlocksForever(op, err) ||
			nameTooLong(err) ||
			(errors.Is(err, fs.ErrPermission) && e.permissionBlockedByDirTarget(op)) {
			// The server supplied a path/view name that escapes the sync root (or a
			// pre-existing symlink sits along it), or a local non-directory blocks a path
			// component: it can never be applied, and leaving it queued would keep the drain
			// retrying it forever while every conflicting same-path successor stays queued
			// behind it (batch drag). Dead-letter it - drop the op with a warning so its
			// conflict chain flows again and the drop is observable as the Dropped count.
			e.log.Warn("engine: dropping unappliable op",
				"type", op.Type, "view", op.ViewID, "path", op.Path, "err", err)
			// reconcile wrote (create/modify→download/mkdir) or relocated (move→rename) a
			// pending baseline for this node before the drain ran. A symlink-rejected path
			// passes reconcile's lexical containable() check, so unlike a structurally invalid
			// path (skipped before any baseline write) it really does leave a record behind.
			// The op is acked below either way, so the baseline must be resolved here -
			// cleanupRejected dispatches by op type (its doc comment covers each shape).
			if cleanupErr := cleanupRejected(e, ctx, op); cleanupErr != nil {
				// The cleanup itself failed at the store level. It returns early (leaving the op
				// queued, un-acked, with no trace, so a retry cannot double-count), but
				// a full disk surfaces the failure as ENOSPC: route it through the disk-full latch
				// so it lights `Disk: full` in status instead of retrying silently, matching the
				// apply-error site below.
				return e.noteApplyResult(op, cleanupErr)
			}
			// Cleanup succeeded; record the trace and ack atomically. Recording only after
			// the cleanup means a cleanup failure above (which returns early, leaving the op
			// queued) writes no trace, so a retry cannot double-count it.
			if ackErr := e.deadLetterAndAck(op, qo.Seq, fmt.Sprintf("unappliable: %v", err)); ackErr != nil {
				// A store-level record+ack failure can be ENOSPC on a full disk: latch it so
				// `status` shows `Disk: full` instead of retrying silently. The returned
				// error is unchanged.
				return e.noteApplyResult(op, ackErr)
			}
			return errDeadLettered
		}
		if errors.Is(err, errDeadLettered) {
			// applyRename dropped the move itself: its node is not where the move needs it
			// (a foreign-source path, or a node that was never materialised on disk). It
			// already logged the reason and cleaned up any baseline it had relocated. Ack it
			// (drop) and propagate the dead signal so a same-id rename fed by it is doomed
			// rather than trusting a false feeder.
			// applyRename already cleaned up any baseline it relocated, so there is no
			// cleanup step here; record the trace and ack atomically - together
			// with the compensating op that re-requests the dropped node (finding H2), so the
			// drop stops being the permanent replica divergence the two-client stand measured.
			comp, track, compErr := e.compensateDroppedRename(err, qo)
			if compErr != nil {
				// The queue scan that decides whether the node is already covered, or a read
				// behind the pending record that arms the compensation's never-lose gate, failed
				// at the store level: leave the op queued (no trace, no ack) and retry rather
				// than dropping it blind and losing the repair. applyRename's own cleanup is
				// idempotent, so the next drain re-derives the same drop.
				return e.noteApplyResult(op, compErr)
			}
			if ackErr := e.deadLetterCompensateAndAck(op, qo.Seq,
				droppedRenameReason("rename source missing or foreign", comp), comp, track); ackErr != nil {
				// A store-level record+ack failure can be ENOSPC on a full disk: latch it so
				// `status` shows `Disk: full` instead of retrying silently. The returned
				// error is unchanged.
				return e.noteApplyResult(op, ackErr)
			}
			return errDeadLettered
		}
		if errors.Is(err, errDownloadNodeIsDir) {
			// The server proved the download's own node (id-matched) is a DIRECTORY, so the
			// download verb can never materialise it. Replace the op with the inbound OpMkdir
			// that can, in ONE transaction with the trace and the Ack: a bare Ack followed by an
			// Enqueue would lose the repair on a crash between them, and the reverse order would
			// leave a duplicate on replay. This is the terminal exit for compensatedKind's
			// declared residual - a dropped inbound rename of a DIRECTORY whose kind no surviving
			// baseline could state re-requests it as a download, and until now that download
			// retried forever with every path-overlapping op wedged behind it.
			//
			// No TrackedBaseline rides along: this replaces one inbound op with another at the
			// SAME path under the SAME id, so the StatePending record that armed the download's
			// never-lose gate (reconcile's pre-write, or compensationTracking's for a
			// rename-compensation download) is already there and still arms the mkdir's.
			//
			// The path comes from the SIGNAL, not from this op: applyDownload works on a copy and
			// may rebase it onto the node's current tracked location (the self-echo move
			// redirect), and it is that path the kind probe answered for. Placing the mkdir at the
			// queue record's stale spelling would markSync a path the node has left - evicting the
			// live baseline through the byid index, or renaming the user's relocated subtree back.
			compPath := op.Path
			var isDirAt *downloadIsDirAt
			if errors.As(err, &isDirAt) && isDirAt.path != "" {
				compPath = isDirAt.path
			}
			e.log.Warn("engine: inbound download addresses a directory server-side; re-requesting the node as a mkdir",
				"view", op.ViewID, "path", compPath, "op_path", op.Path, "file_id", op.FileID)
			comp := store.Op{
				Type:         store.OpMkdir,
				Direction:    store.DirInbound,
				ViewID:       op.ViewID,
				Path:         compPath,
				FileID:       op.FileID,
				IsDir:        true,
				ServerSyncID: op.ServerSyncID,
			}
			if ackErr := e.deadLetterCompensateAndAck(op, qo.Seq,
				"inbound download addressed a node the server holds as a directory"+
					"; re-requested the node as an inbound mkdir"+
					"; the directory's server subtree is NOT re-requested and stays missing from this replica until a cold start",
				&comp, nil); ackErr != nil {
				// A store-level record+ack failure can be ENOSPC on a full disk: latch it so
				// `status` shows `Disk: full` instead of retrying silently, mirroring every
				// other drop arm here. The returned error is unchanged.
				return e.noteApplyResult(op, ackErr)
			}
			// errOpReplaced, not errDeadLettered: the node is NOT missing from the output key -
			// the compensating OpMkdir standing at the same path under the same id will place it.
			// Reporting the drop would doom a same-node inbound rename fed by this key across the
			// drain windows, permanently, over an op that is merely still queued.
			return errOpReplaced
		}
		if errors.Is(err, errDownloadNodeGone) {
			// The download's node is positively gone server-side (strict id-matched parent
			// listing): destroyed with no delete event this client
			// processed - the overwrite-move shape. A race resolution, not a failure or an
			// operator-facing drop: clean the phantom pending baseline reconcile pre-wrote
			// (the node never reached disk; dropStrandedBaseline follows any same-batch
			// relocation through byid and keeps a genuinely materialised or synced record),
			// ack WITHOUT a durable dead-letter trace (mirroring errRenameSourceGone), and
			// return errDeadLettered so the scheduler treats the op as "left the queue
			// without applying" - clearing its stuck counter and dooming any same-id rename
			// fed by it rather than letting that rename drag unrelated source bytes.
			e.log.Info("engine: inbound download node gone server-side; dropping doomed download",
				"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
			if cleanupErr := dropStrandedDownloadBaseline(e, op.ViewID, op.FileID); cleanupErr != nil {
				// The store-level cleanup failed, possibly with ENOSPC on a full disk: route it
				// through the disk-full latch so `status` shows `Disk: full` instead of retrying
				// silently, matching cleanupRejected/dropDoomedRename. The
				// returned error is unchanged.
				return e.noteApplyResult(op, cleanupErr)
			}
			if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
				return e.noteApplyResult(op, fmt.Errorf("engine: ack node-gone download %d: %w", qo.Seq, ackErr))
			}
			return errDeadLettered
		}
		// A disk-full failure (ENOSPC) is transient - freeing space lets the very same
		// op succeed on the next drain - so it is NOT dead-lettered; the op stays queued
		// and retries. noteApplyResult latches it for the `status` line (centralised so the
		// outbound/conflict paths share the same site); other errors leave the flag as-is.
		//
		// paceInboundServerFailure arms the repeat-rejection pause and NOTHING else - inbound
		// work never escalates to a dead-letter (servererr.go), but an inbound op the server
		// keeps refusing the same way must not re-issue its whole turn every drain either. That
		// is the direction finding H5's 32772 was actually measured on.
		return e.paceInboundServerFailure(qo.Seq,
			e.noteApplyResult(op, fmt.Errorf("engine: apply %s %d/%q: %w", op.Type, op.ViewID, op.Path, err)))
	}
	// A clean apply proves the disk is writable again, so clear any latched disk-full
	// condition via noteApplyResult (CompareAndSwap on the flag). The return is the
	// passed-in nil, so it is intentionally discarded.
	_ = e.noteApplyResult(op, nil)
	if err := e.store.Ack(qo.Seq); err != nil {
		return fmt.Errorf("engine: ack %d: %w", qo.Seq, err)
	}
	// Counted AFTER the ack, so the figure an operator reads never claims work the queue
	// still owes: an op that applied but failed to ack is retried, and counting it here
	// would report it twice.
	e.progress.record(transferredBytes(op))
	e.log.Debug("engine: applied op",
		"type", op.Type, "direction", op.Direction, "view", op.ViewID, "path", op.Path)
	return nil
}

// noteApplyResult centralises the ENOSPC disk-full operability latch across all three apply
// paths (the inbound switch, the outbound push and the keep-both resolve). The outbound and
// conflict paths return early before the inbound switch's latch site, so without this helper
// a disk-full failure there would retry silently - the op stays queued but `status` never
// shows `Disk: full`. An apply error the host reports as out of space or out of quota
// latches the diskFull flag (surfaced as `Disk: full` in status) and is NOT dead-lettered:
// freeing space lets the same op succeed on the next drain. A clean apply (nil) proves the
// disk is writable again and clears the latch (CompareAndSwap keeps the common already-false
// case lock- and contention-free). Any other error leaves the flag as-is. The error is
// returned unchanged so callers can `return e.noteApplyResult(op, apply(...))`.
//
// The classification lives in isDiskFull (diskfull.go) rather than a bare
// errors.Is(err, syscall.ENOSPC) here: ENOSPC alone made the latch dead code on Windows and
// missed EDQUOT on Unix.
//
// ANY clean apply clears it, including an outbound one that never touched the sync volume, and
// that is deliberate rather than an oversight. It was tried the other way - clear only on an
// apply that writes local content - on the reasoning that a push proves nothing about the disk
// holding the user's files. The reverse case is worse: an outbound apply can be what LATCHES
// the flag in the first place, since it writes baselines to state.db, which routinely sits on a
// different volume from the sync root. Under the strict rule, filling the state disk latched
// the flag and freeing it never cleared one, because only an inbound apply could - and a view
// with no inbound work would show `Disk: full` forever. The loose rule's failure is a flicker
// the next failing apply re-latches; the strict rule's is a permanent lie. For an operability
// hint, self-correcting beats precise.
func (e *Engine) noteApplyResult(op store.Op, err error) error {
	switch {
	case err == nil:
		e.diskFull.CompareAndSwap(true, false)
	case isDiskFull(err):
		e.diskFull.Store(true)
		e.log.Warn("engine: apply failed: disk full",
			"type", op.Type, "direction", op.Direction, "view", op.ViewID, "path", op.Path)
	}
	return err
}

// existsBlocksForever reports whether an fs.ErrExist inbound apply failure is the PERMANENT
// "an untracked local node occupies the target" shape applyOp's classifier dead-letters, as
// opposed to the transient "directory not empty" shape Go folds into the same sentinel.
//
// The arm's rationale is occupancy: a local file or directory sitting where a file- or
// dir-PLACING apply must land never vanishes on its own under the never-delete policy, so
// retrying is futile and the op is dropped with a durable trace. That reasoning does not
// transfer to ENOTEMPTY/ERROR_DIR_NOT_EMPTY, which no placing verb produces - per OS, and for
// different reasons: on Unix os.Rename Lstats its destination and synthesises EEXIST for ANY
// existing directory, empty or not, while on Windows the replacing move is MoveFileEx with no
// such pre-check and a directory destination surfaces as ERROR_ACCESS_DENIED (fs.ErrPermission,
// which permissionBlockedByDirTarget below owns), never as fs.ErrExist at all. RenameNoReplace
// and MkdirAll report EEXIST/ERROR_ALREADY_EXISTS on both. So it comes from a directory
// REMOVAL whose blocker appeared after the sweep that authorised the delete, and it clears by
// itself. The two verbs whose apply removes a directory are therefore the ones gated here:
//
//   - OpRemove: applyRemove's fsutil.Remove (and removeRelocatedNode's) walks the subtree with
//     os.RemoveAll, which reads each directory ONCE - a file created in it during that pass
//     surfaces as ENOTEMPTY (reproduced on the host: "unlinkat ...: directory not empty", with
//     errors.Is(err, fs.ErrExist) true). Dead-lettering it drops the directory's baseline and its
//     descendants' (dropRejectedRemoveBaseline), leaving the half-deleted tree untracked on disk -
//     so the next scan diffs it as a fresh local create and RE-UPLOADS it, resurrecting a
//     server-deleted subtree on every replica. Nothing about that is recoverable by a later
//     drain, while staying queued costs only a retry the racing creator's exit resolves.
//   - OpDownload: removeRelocatedPriorFile clears the node's stale copy at its previous tracked
//     path (inbound_remove.go) with the same os.RemoveAll, so the identical race dead-letters an
//     op of an entirely different verb - the download - whose own target was never occupied.
//
// A persistently-occupied remove is not left to spin unobserved: the pre-delete divergence sweep
// (guardRemoveDivergence) moves a subtree holding any untracked file aside as a _Conflict copy on
// the next drain, so a blocker that outlives the race is resolved rather than retried forever,
// and the Stuck counter keeps the interim visible either way.
//
// Every other verb keeps the previous classification for every fs.ErrExist shape, and so does a
// genuine EEXIST/ERROR_ALREADY_EXISTS on the two verbs above - a no-replace promote whose target
// is really occupied still dead-letters.
func existsBlocksForever(op store.Op, err error) bool {
	if !errors.Is(err, fs.ErrExist) {
		return false
	}
	if !dirNotEmpty(err) {
		return true
	}
	switch op.Type {
	case store.OpRemove, store.OpDownload:
		return false
	default:
		return true
	}
}

// permissionBlockedByDirTarget reports whether a permission-denied inbound apply failure is
// really the "file lands on a path occupied by an untracked local DIRECTORY" shape that the
// fs.ErrExist arm of applyOp's classifier dead-letters. On Unix that shape always surfaces
// as fs.ErrExist because os.Rename's userspace dir-guard rejects a replacing rename onto an
// existing directory before the syscall; on Windows the guard does not exist and
// MoveFileEx(MOVEFILE_REPLACE_EXISTING) fails with ERROR_ACCESS_DENIED → fs.ErrPermission,
// so without this check the replacing download promote (inbound_download.go) and
// fsutil.Rename (inbound_rename.go) would read as transient and retry forever, wedging
// every same-path successor behind the op. The check is deliberately
// platform-independent - an Lstat of the op's resolved target, not an errno mapping - so an
// EACCES that surfaces before the rename on Unix too (e.g. the staging-temp create under a
// read-only parent while a directory occupant blocks the target) resolves the same way.
// Soundness does not depend on WHICH apply stage produced the permission error: a
// file-placing op whose target is held by a directory can never apply while that directory
// persists, and under the never-delete policy it never vanishes on its own - exactly the
// fs.ErrExist rationale.
//
// DIR-placing ops resolve the same way. A dir-over-dir move is the
// mirror of the file-over-dir shape and Unix already dead-letters it: os.Rename's userspace
// guard synthesises EEXIST for ANY existing-directory destination, whatever the source is
// (and a non-empty occupant gives ENOTEMPTY), so both a directory rename (inbound_rename.go)
// and the mkdir subtree relocate (relocateTrackedDirForMkdir, the rejected-move recovery)
// end in the fs.ErrExist arm there. Windows has no such guard: MoveFileEx onto an existing
// directory reports ERROR_ACCESS_DENIED for an empty AND a non-empty occupant alike
// (verified on the host), so those two verbs used to retry forever with blockedKeys starving
// BOTH path prefixes - the whole moved subtree at the source and everything under the
// destination. The occupant is an untracked local directory that never vanishes on its own
// under the never-delete policy, exactly as in the file-placing case, so the probe now
// covers op.IsDir renames and mkdir-relocates too and the platforms reach the same outcome.
//
// Every other permission shape deliberately stays transient (returns false), because a lone
// ErrPermission is not proof the op can never apply and the Stuck counter keeps the retries
// observable: a regular-file occupant (transient ACL noise), an absent
// target, a plain mkdir (its target existing as a directory is its SUCCESS shape, so a
// permission failure there came from a later stage - SetMeta, an occupant guard - and says
// nothing permanent; only the relocate shape, where a tracked directory really is being
// renamed onto the target, qualifies) and the remove verb (whose permission failure says
// nothing about an occupant).
//
// The one non-transient regular-file shape is handled BELOW this classifier rather than by
// it: a Windows destination carrying FILE_ATTRIBUTE_READONLY also fails
// MoveFileEx(MOVEFILE_REPLACE_EXISTING) with ERROR_ACCESS_DENIED, and that attribute never
// clears itself - the op was deterministically inapplicable while every retry re-downloaded
// the whole body first. It is not classified here because the apply
// itself can resolve it: fsutil.RenameReplace clears the attribute and retries once, behind
// the same divergence/dest guards that already proved the destination may be replaced. What
// reaches this classifier as ErrPermission is therefore genuine ACL noise again, and staying
// transient for it is right.
func (e *Engine) permissionBlockedByDirTarget(op store.Op) bool {
	// rel is the target the failing apply placed a node at; src, when non-empty, is the
	// view-relative path the SAME node is being moved from, so a target that resolves to
	// the source itself can be told apart from a foreign occupant (see below).
	var rel, src string
	switch op.Type {
	case store.OpDownload:
		// A download always places a regular file; op.IsDir is never set for it.
		if op.IsDir {
			return false
		}
		rel = op.Path
	case store.OpRename:
		rel, src = op.NewPath, op.Path
	case store.OpMkdir:
		// Only the rejected-move recovery renames an existing subtree onto op.Path; a plain
		// mkdir is idempotent over a directory occupant and must stay transient.
		oldPath, relocates := e.mkdirRelocatesTrackedDir(op)
		if !relocates {
			return false
		}
		rel, src = op.Path, oldPath
	default:
		return false
	}
	abs, err := e.abs(op.ViewID, rel)
	if err != nil {
		return false
	}
	// Resolve to the real on-disk (possibly NFD) spelling the failing apply collided with,
	// mirroring the promote/rename target resolution. A resolution
	// failure (a symlink appeared along the path) conservatively stays transient - the next
	// drain's apply surfaces it as errRejectedPath.
	if resolved, rerr := e.onDiskAbsPrefixNoSymlink(abs); rerr == nil {
		abs = resolved
	}
	fi, err := os.Lstat(abs)
	if err != nil || !fi.IsDir() {
		return false
	}
	// A directory-moving op whose destination resolves to the very node being moved has no
	// occupant at all: a case-only rename on a case-insensitive filesystem (server renames
	// Docs → docs) or an NFD↔NFC-only rename on a normalising one addresses the SOURCE
	// directory through a second spelling, and the Lstat above then finds our own tracked
	// subtree. The terminal classification's whole premise - an untracked local directory
	// that never vanishes on its own under the never-delete policy - is false there, so a
	// permission failure on that shape is ordinary transient noise (a process holding the
	// directory as its CWD, an open handle, an AV scanner) and must keep retrying rather
	// than dead-letter the rename forever. The byte compares that already guard the
	// same-path case (mkdirRelocatesTrackedDir's oldPath == op.Path,
	// relocateTrackedDirForMkdir's) cannot see these second spellings; identity on disk can.
	if src != "" && e.resolvesToSameNode(op.ViewID, src, fi) {
		return false
	}
	return true
}

// resolvesToSameNode reports whether the view-relative path rel names the very on-disk node
// fi describes. Every read failure answers false - the conservative direction for its caller,
// which only uses a positive answer to WITHHOLD a terminal classification.
func (e *Engine) resolvesToSameNode(viewID uint64, rel string, fi os.FileInfo) bool {
	abs, err := e.abs(viewID, rel)
	if err != nil {
		return false
	}
	// The source names an existing node, so it resolves all-or-nothing (onDiskAbsNoSymlink),
	// exactly as the applies that move it do.
	if resolved, rerr := e.onDiskAbsNoSymlink(abs); rerr == nil {
		abs = resolved
	}
	sfi, err := os.Lstat(abs)
	if err != nil {
		return false
	}
	return os.SameFile(sfi, fi)
}

// mkdirRelocatesTrackedDir reports whether an inbound OpMkdir is the rejected-move recovery
// shape - its directory node is tracked by file id at a DIFFERENT on-disk path that still
// holds the subtree - so applyMkdir renames that subtree onto op.Path
// (relocateTrackedDirForMkdir) instead of creating a fresh directory, and returns the old
// path that rename moves FROM. It mirrors that
// function's own gates (byid lookup, a dir baseline carrying the op's file id at the old
// path, a resolvable non-symlink old path present on disk) so permissionBlockedByDirTarget
// only classifies a permission failure as dir-over-dir when a replacing directory rename is
// what actually ran. Every read failure answers "not the relocate shape", keeping the
// classification conservative: the op stays transient and retries.
func (e *Engine) mkdirRelocatesTrackedDir(op store.Op) (string, bool) {
	if op.FileID == "" {
		return "", false
	}
	oldPath, err := e.store.PathByID(op.ViewID, op.FileID)
	if err != nil || oldPath == "" || oldPath == op.Path {
		return "", false
	}
	ob, err := e.store.GetBaseline(op.ViewID, oldPath)
	if err != nil || ob.FileID != op.FileID || ob.FileType != "dir" {
		return "", false
	}
	oldAbs, err := e.abs(op.ViewID, oldPath)
	if err != nil {
		return "", false
	}
	// Resolve the old path's real on-disk (possibly NFD) spelling exactly as the relocate
	// does - INCLUDING its failure arm: relocateTrackedDirForMkdir returns "not the relocate
	// shape" when the resolution fails (a symlink crept along the old path) and falls back to
	// a PLAIN mkdir, so answering true here would classify a plain mkdir's permission failure
	// as dir-over-dir and dead-letter an op that is merely blocked transiently. Every read
	// failure answers "not the relocate shape".
	resolved, rerr := e.onDiskAbsNoSymlink(oldAbs)
	if rerr != nil {
		return "", false
	}
	fi, err := os.Lstat(resolved)
	if err != nil || !fi.IsDir() {
		return "", false
	}
	return oldPath, true
}

// suppressedViewReason names why a view is being left alone, for the one message
// that reports it.
func suppressedViewReason(e *Engine, viewID uint64) string {
	if e.absentViews[viewID] {
		return "view absent from the server enumeration"
	}
	return "view that is not selected"
}

// scheduledOp is one op dispatched by a drain batch, with the conflict keys it serialises
// on, the channel its successors wait on, and how it ended.
//
// It sits at package level, rather than inside drainBatch where it used to, so the batch's
// closing bookkeeping (settleBatchOutcomes) can be a function of its own instead of another
// fifty lines at the bottom of a four-hundred-line body.
//
// The three outcome fields are written by the op's own goroutine under drainBatch's mutex
// and read after wg.Wait, which is the only reason reading them unlocked below is safe.
type scheduledOp struct {
	op      store.Op
	seq     uint64
	keys    []string
	done    chan struct{}
	bad     bool          // failed/skipped transiently: successors skip and stay queued
	dead    bool          // dead-lettered or doomed: dropped; a rename fed by it is doomed
	culprit *chainCulprit // when skipped: the self-failing head of this op's chain (nil if self-failing or blocked cross-window)
	// carried is the key THIS pass armed a cross-pass doom at (empty if none), so the
	// settling pass cannot erase the mark it just recorded when a rename's source and
	// destination fold to the SAME key - a case-only rename on a case-insensitive
	// filesystem, or an NFD↔NFC-only rename anywhere (cleanKey folds both). carryDoom keys
	// on keys[0] while outputKey answers keys[len-1].
	carried string
}

// settleBatchOutcomes records what a finished drain batch means for the drain pass's LATER
// windows, once every goroutine has joined.
//
// Two marks, one loop. Everything the batch left queued - failed, skipped behind a failure,
// suppressed - blocks its overlapping successors. Everything it dropped without placing its
// node dooms a same-node rename fed by it in a later window. The loop runs in
// enqueue order, so a later op of the same node at the same output key wins: anything but a
// drop clears an earlier drop's mark.
//
// A STILL-QUEUED op clears the doom too, not just a success. It will place the node at that
// key on a retry, so the earlier window's drop no longer describes the node's fate. Leaving
// the mark would doom a later window's rename PERMANENTLY - dead-letter trace plus the
// relocated baseline dropped - over a producer that is merely retrying, an unrecoverable path
// divergence in place of a self-healing wait. Ordering is still enforced, because the same op
// added its keys to blocked.
//
// Only a dead INBOUND op may ARM the doom, the same asymmetry the in-window feeder scan
// enforces: a dead outbound op merely failed to push a local change
// whose bytes are already on disk. Clearing stays unconditional, because an op of this node
// at this key that did not die is evidence the node is there - and the failure direction of a
// spurious clear is a rename that still runs through applyRename's own never-lose guards,
// while a spurious add is a permanent drop.
//
// outputKey is passed in rather than recomputed: it resolves through the batch's per-view
// directory cache, and it must answer here exactly what it answered while scheduling.
func (e *Engine) settleBatchOutcomes(scheduled []*scheduledOp, blocked *blockedKeys, dead *deadOutputs,
	outputKey func(*scheduledOp) (string, bool)) {
	for _, s := range scheduled {
		if s.bad {
			blocked.add(s.keys)
		}
		out, places := outputKey(s)
		if !places || s.op.FileID == "" {
			continue
		}
		if s.dead && s.op.Direction == store.DirInbound {
			dead.add(s.op.FileID, out)
			continue
		}
		dead.clear(s.op.FileID, out)
		// The cross-pass carry follows the same clear rule, with one exception: the doom THIS
		// pass just armed. A rename whose source and destination fold to one key has
		// outputKey == the carry key, so an unconditional clear would erase the mark in the
		// very pass that recorded it, and the next pass's applyRename would haul whatever
		// same-kind untracked entity sat at the source. The op is still queued and still
		// doomed; its own presence is no evidence the node reached that key.
		if s.carried == "" || s.carried != out {
			e.clearCarriedDoom(s.op.FileID, out)
		}
	}
}
