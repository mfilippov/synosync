package engine

// This file makes a fold-key collision OBSERVABLE (two-client finding H6).
//
// scanKeyCollisions/protectCollisions already do the safe thing with two on-disk names that
// fold to one normpath.Key - the NFD and NFC encodings of one string, which NTFS and ext4
// both hold as two distinct directory entries: neither file is synced, so the merge-join's
// last-wins can never clobber or silently drop one of them. What they do not do is TELL
// anyone. The stand measured the consequence: one client created `café.txt` (NFC), the other
// the NFD spelling of the same name, and the second file sat on disk looking synchronised -
// absent from `status`, from the dead-letter trace and from the stuck-op count - while it was
// in fact excluded from every pass and would never be backed up. A file the client has
// decided it cannot sync is exactly the thing an operator must be able to see.
//
// The trace is a bare dead-letter (store.RecordDeadLetter), which is the one drop in the
// codebase with no queued op to ack: the durable queue is keyed by path and the store by
// view+path, and the whole point of the collision is that the two files share one key, so
// neither could ever represent the second file. The record therefore names what the operator
// has to act on - the on-disk path, how many entries collided, and the already-synced node
// holding the key when there is one - and the remedy, which is a rename.
//
// Recording is idempotent per (view, fold key) via the engine's collisionNoted memo: a
// collision persists across every 5s scan until a human renames one of the files, and one
// record per scan would retire the whole 1000-entry FIFO ring in under an hour, taking every
// other durable drop trace with it. The memo is dropped when the key stops colliding, so the
// same key colliding again later is reported again. Because the memo is process-local and the
// collision is not, it is seeded from the durable ring on the first pass over each view
// (seedCollisionNoted) - otherwise a restart re-recorded every still-colliding key and a
// restart loop churned exactly the ring this rule protects.
//
// The memo bounds repetition over TIME. collisionTraceBurstCap bounds ONE pass's burst, which is
// the same harm reached in one go rather than over an hour: a tree holding more colliding keys
// than the ring holds records retires the whole ring in a single scan. Past the cap the keys are
// counted into one overflow record instead of being written individually.

import (
	"errors"
	"fmt"
	"sort"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// noteCollisionTraces records one durable dead-letter for every fold-key collision in view
// that has not already been traced, and forgets the keys that no longer collide. collisions
// is the CURRENT full set for the view (scanKeyCollisions over the pass's whole scan), which
// is what makes the forgetting sound: a key absent from it is a key the scan just proved
// collision-free.
//
// observed is what makes that soundness conditional, and it is not optional: it is the caller's
// vouching that this pass really enumerated the tracked tree. A pass over an unavailable view root
// (unmounted, missing, transiently unreadable) reaches its reconcile with an EMPTY collisions map -
// which says nothing about the keys, and must not be read as "the scan just proved them
// collision-free". Hence the two call sites both hand it a positive enumeration (localReconcileWith
// calls this past every availability guard with len(entries) > 0; the cold-start planner likewise).
// Retiring the memo on such a pass re-armed the recording for every still-colliding key, and
// because seedCollisionNoted has already run for the view the durable ring could not correct
// it: one mount flap per pass wrote a duplicate trace and evicted the genuine op-drop traces
// this rule exists to protect - the restart-loop churn seedCollisionNoted closes, reached
// through a different door. A pass that enumerated nothing therefore forgets nothing; the
// record loop below is a no-op for it anyway.
//
// It is best-effort observability, exactly like every other dead-letter write: a store
// failure is logged and the key is left un-noted so the next pass retries, never propagated
// into the reconcile pass - a trace that cannot be written must not stop the pass that keeps
// both files protected.
func (e *Engine) noteCollisionTraces(viewID uint64, collisions map[string][]string, observed bool) {
	e.seedCollisionNoted(viewID)
	noted := e.collisionNoted[viewID]
	if observed {
		for k := range noted {
			if collisions[k] == nil {
				// Renamed (or deleted): the key is free again, so a recurrence is news.
				delete(noted, k)
			}
		}
	}
	// The memo bounds REPETITION, not the size of one pass's burst, and the two are different
	// failure modes: a view arriving with more un-noted keys than the durable ring holds retires
	// the whole ring in a single pass - evicting every genuine op-drop trace - which is exactly
	// the harm the once-per-key rule was written to prevent, reached in one go instead of one
	// record at a time. An NFD tree merged onto an NFC one produces that shape wholesale. So the
	// burst is capped and the remainder is summarised in ONE record, and the summarised keys are
	// noted like traced ones: leaving them un-noted would put the same burst back on the next
	// pass, which is the churn the cap exists to stop.
	// The budget is spent by POSITION in the sorted key list, not by records actually written, so
	// the sample is the first collisionTraceBurstCap keys of the view whatever the memo already
	// holds. Counting only writes looked equivalent and was not: after a restart seedCollisionNoted
	// notes the traced sample, those keys then cost nothing, and the budget fell to the NEXT
	// cap keys - so each restart traced a fresh batch and walked the whole collision set through
	// the ring, one restart at a time. That is the churn the cap was added to stop.
	var summarised []string
	for i, k := range sortedKeys(collisions) {
		paths := collisions[k]
		if i >= collisionTraceBurstCap {
			if noted[k] {
				continue
			}
			// Counted and reported once below rather than individually. The count is what an
			// operator acts on here: the remedy for every one of them is the same rename, and
			// the traced sample above already names the shape. The keys are noted only once
			// that summary is DURABLE, for the same reason a per-key trace notes nothing on a
			// failed write (recordCollisionTrace): noting them regardless would let one
			// transient store error hide the whole overflow permanently - the next pass would
			// find every key already noted, summarise nothing and write nothing, which is the
			// silent-unsyncable class H6 exists to close, reached through the cap instead of
			// through the memo.
			summarised = append(summarised, k)
			continue
		}
		if noted[k] {
			continue
		}
		tracked, ok, err := e.syncedCollisionPath(viewID, paths)
		if err != nil {
			e.log.Warn("engine: cannot read the baseline behind a fold-key collision; leaving it untraced for this pass",
				"view", viewID, "fold_key", k, "paths", paths, "error", err)
			continue
		}
		dl := store.DeadLetter{
			Type:      store.OpFoldCollision,
			Direction: store.DirOutbound,
			ViewID:    viewID,
			// paths is sorted and non-empty (scanKeyCollisions only emits keys with ≥2
			// entries); the first is the one an operator can go and look at. The NFD/NFC
			// pair reaches here as two copies of one NFC string, so naming one names both.
			Path:       paths[0],
			Reason:     collisionReason(paths, tracked, ok),
			RecordedAt: e.now().Unix(),
		}
		if _, err := recordCollisionTrace(e, dl); err != nil {
			e.log.Warn("engine: cannot record the fold-key collision trace",
				"view", viewID, "fold_key", k, "paths", paths, "error", err)
			continue
		}
		if noted == nil {
			noted = make(map[string]bool)
			e.collisionNoted[viewID] = noted
		}
		noted[k] = true
	}
	if len(summarised) > 0 && e.noteCollisionOverflow(viewID, len(summarised)) {
		if noted == nil {
			noted = make(map[string]bool)
			e.collisionNoted[viewID] = noted
		}
		for _, k := range summarised {
			noted[k] = true
		}
	}
}

// collisionTraceBurstCap bounds how many individual fold-key collision traces ONE pass over a view
// may write. The durable ring holds store's maxDeadLetters records in total and every other kind of
// trace is a genuine dropped op, so a burst has to stay a small fraction of it: a view can exceed
// the whole ring's capacity in collisions alone (an NFD tree merged onto an NFC one), and writing
// one record per key would then evict every op-drop trace in a single scan - the harm the
// once-per-key memo exists to prevent, arrived at wholesale rather than over an hour. The keys past
// the cap are not silently forgotten: they are counted into one overflow record.
const collisionTraceBurstCap = 32

// noteCollisionOverflow records the ONE trace standing for every fold-key collision a pass left
// untraced under collisionTraceBurstCap, and reports whether that record is durable. It is
// deliberately not memoised itself: the keys behind it are noted by the caller ON SUCCESS, so the
// overflow is recomputed only when the un-noted set changes - and re-recording it after a restart
// (seedCollisionNoted can only seed from records that exist, and an overflow record names no key)
// costs exactly one record per restart rather than the burst it replaced. The sample traced above
// IS seeded back, because the keys are taken in sorted order, so the same pass writes the same
// sample every time. A false return leaves those keys un-noted so the next pass retries the
// summary, the same rule recordCollisionTrace states for an individual trace.
func (e *Engine) noteCollisionOverflow(viewID uint64, n int) bool {
	dl := store.DeadLetter{
		Type:      store.OpFoldCollision,
		Direction: store.DirOutbound,
		ViewID:    viewID,
		Reason: fmt.Sprintf("%d further on-disk names fold to sync keys already held by another "+
			"name (an NFD/NFC or case-only difference) and are traced only in this summary; none of "+
			"them can sync until one of each pair is renamed", n),
		RecordedAt: e.now().Unix(),
	}
	if _, err := recordCollisionTrace(e, dl); err != nil {
		e.log.Warn("engine: cannot record the fold-key collision overflow trace",
			"view", viewID, "collisions", n, "error", err)
		return false
	}
	return true
}

// sortedKeys returns m's keys in ascending order, so the sample of collisions a capped pass traces
// is the SAME one every pass and every restart. Map order would otherwise pick a different subset
// each time, and each pass would then find un-noted keys the ring already holds a trace for -
// re-recording them forever, which is the churn the cap is here to stop.
func sortedKeys(m map[string][]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// recordCollisionTrace is the single store WRITE noteCollisionTraces performs, behind a seam for
// the same reason recordDeadLetterAndAck has one: the failure that matters here is the write's
// alone, and it cannot be reached by failing the whole store - the reads (ListDeadLetters, then
// GetBaseline) fail first and exit down a different arm. The invariant the seam makes testable is
// that a failed write leaves the key UN-noted, so the next pass retries; noting it regardless
// would let one transient store error hide a fold-key collision permanently, which is the
// silent-unsyncable class H6 exists to close.
var recordCollisionTrace = func(e *Engine, dl store.DeadLetter) (uint64, error) {
	return e.store.RecordDeadLetter(dl)
}

// seedCollisionNoted primes the view's in-memory memo from the durable dead-letter ring, once
// per view per process. Without it the memo's idempotence was only process-local while the
// collision it suppresses is on-disk state that outlives the daemon: every restart re-recorded a
// trace for a still-colliding key, and a supervised daemon in a restart loop churned the
// 1000-entry FIFO - evicting the real op-drop traces the ring exists for, which is the failure
// mode the once-per-key rule was written to prevent.
//
// A read failure leaves the view unseeded so the next pass retries; re-recording a trace is a
// far smaller harm than dropping the pass that keeps both colliding files protected. The seed is
// keyed by the trace's own path folded to a sync key - the same key the collision is keyed by,
// since the whole point of a collision is that every colliding spelling folds to it.
//
// A collision trace is recognised in the ring by its own store.OpFoldCollision type. It is the
// one dead-letter with no queued op behind it, so that type value never names a queue op and
// exists purely as this discriminator - which is the point: the earlier reading matched a
// substring of the operator-facing Reason prose, so rewording collisionReason silently unseeded
// every memo and re-armed exactly the ring churn this function was written to prevent, with
// nothing in the compiler or the tests to say so.
func (e *Engine) seedCollisionNoted(viewID uint64) {
	if e.collisionSeeded[viewID] {
		return
	}
	traces, err := e.store.ListDeadLetters()
	if err != nil {
		e.log.Warn("engine: cannot read the dead-letter trace ring to seed the fold-key collision memo; a persisting collision may be re-traced",
			"view", viewID, "error", err)
		return
	}
	noted := e.collisionNoted[viewID]
	for _, t := range traces {
		if t.ViewID != viewID || t.Type != store.OpFoldCollision {
			continue
		}
		if t.Path == "" {
			// The overflow summary (noteCollisionOverflow) names no key, so it seeds none. A
			// real collision path is never empty, so this only ever skips that record.
			continue
		}
		if noted == nil {
			noted = make(map[string]bool)
			e.collisionNoted[viewID] = noted
		}
		noted[normpath.Key(t.Path)] = true
	}
	e.collisionSeeded[viewID] = true
}

// collisionReason renders the operator-facing cause. The two shapes are worth distinguishing:
// when one of the colliding names is already tracked, the user is looking at a file that
// LOOKS synced and a twin that never will be, which is the shape the stand measured; when
// none is, neither file has ever reached the server and both are equally stranded.
func collisionReason(paths []string, tracked string, hasTracked bool) string {
	if hasTracked {
		return fmt.Sprintf("local name collides with the already-synced %q: %d on-disk names "+
			"fold to one sync key (an NFD/NFC or case-only difference), so only one of them can "+
			"ever sync; rename one of them", tracked, len(paths))
	}
	return fmt.Sprintf("%d on-disk names fold to one sync key at %q (an NFD/NFC or case-only "+
		"difference); none of them can sync until one is renamed", len(paths), paths[0])
}

// syncedCollisionPath reports the colliding path that already has a SYNCED baseline, if any -
// the node whose key the other on-disk name is stuck behind. A pending or conflicted record is
// deliberately not treated as the holder: it names work still in flight, not an agreement the
// second file is losing to. Absence is not an error (the local-only collision is the common
// shape); anything else is, so a store hiccup cannot be reported as "nothing is tracked here".
//
// A record that no longer DECODES is treated as absent, exactly as every other engine read site
// treats it (applyRemove, classifyRenameSource, removeTargetFacts, uploadDivergenceBase - all via
// warnCorruptBaseline). Reporting it as a store failure instead was not conservative here, it was
// silencing: corruption is durable state, not a hiccup, so the caller's "leave it untraced for
// this pass" retried forever - the collision stayed invisible in `status` (the H6 class this file
// exists to close) while every 5s scan added a Warn. The trace is written instead, with the
// "none of them can sync until one is renamed" wording, which is the correct never-lose reading
// when no record can prove a synced holder.
func (e *Engine) syncedCollisionPath(viewID uint64, paths []string) (string, bool, error) {
	seen := make(map[string]bool, len(paths))
	for _, p := range paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		b, err := e.store.GetBaseline(viewID, p)
		switch {
		case err == nil:
			if b.State == store.StateSynced {
				return p, true, nil
			}
		case errors.Is(err, store.ErrNotFound):
		case errors.Is(err, store.ErrCorruptRecord):
			e.warnCorruptBaseline("collision-trace", viewID, p, err)
		default:
			return "", false, err
		}
	}
	return "", false, nil
}
