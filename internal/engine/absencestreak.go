package engine

// This file carries the node-level corroboration-by-repetition the absence provers rest on.
//
// absenceCorroboratedByNodeProbe turns a parent listing that omitted a node into an actionable
// absence by asking the node's own path directly. The answer that CORROBORATES the absence is the
// path-error sentinel - and errno 28672 conflates "no such path" with a server-side operation
// timeout by this client's own contract (actions/errors.go). So the corroboration accepted, as one
// turn's verdict, a signal that a briefly overloaded DSM produces for a perfectly healthy node:
// Remove times out with 28672, the postcondition listing of the parent comes back as one page
// without the leaf, the corroborating probe times out with 28672 too - and the outbound remove
// acks, drops the baseline, and leaves the still-present server file permanently outside the
// replica with the user's delete silently lost.
//
// The view-level twin of exactly this mechanism already refuses to act on one path-error:
// reconcileAbsentViews requires absentRootConfirmations agreeing root probes before it suppresses
// a view, "because a timeout does not repeat N times in a row, while a revoked view answers
// path-error every single refresh". This is that doctrine carried down to the node level, which is
// where the destructive consumers actually live.
//
// The evidence is REPETITION, so the streak counts at most one confirmation per drain pass: two
// probes issued inside one apply would be one server hiccup answered twice, not two independent
// observations. Anything that is not a path-error - a node that answers, a transport failure, an
// id-less reply - clears the streak: the proof is an UNBROKEN run.
//
// A pass count alone is not a duration, though. Nothing separates two drain
// passes in wall clock: loop.go runs the leading drain of a cycle and then a trailing drain for
// every view syncView changed, so two passes land seconds - even sub-seconds - apart, both inside
// one multi-second window of DSM overload, and the conflated verdict is delivered exactly as it was
// before the corroboration existed. This codebase's own doctrine already closed that hole for the
// sibling mechanism: unmappedServerErrMinAge (servererr.go) requires the identical-code streak to
// have ALSO spanned wall clock, "because the drain count alone is not a duration". So a counted
// probe additionally needs absenceStreakMinAge since the last counted one; a probe inside the floor
// is still an observation that keeps the run alive, it just does not advance the count.
//
// Convergence is preserved, not merely bounded: a genuinely absent node answers path-error every
// pass, so the pass after the floor elapses settles it. The cost of the fix is one floor's worth of
// delay per genuine delete whose postcondition needed corroborating at all.

import (
	"strconv"
	"time"
)

// absentNodeConfirmations is how many CONSECUTIVE path-error probes of a node's own path must
// agree before a listing-derived absence becomes actionable. It mirrors absentRootConfirmations
// (the view-level twin) for the same reason and at the same price: one extra drain pass before a
// genuine deletion is acked, in exchange for a single conflated 28672 never being able to ack a
// delete the server did not perform.
const absentNodeConfirmations = 2

// absenceStreakMinAge is the WALL-CLOCK floor between two probes counted as consecutive
// confirmations. Drain passes carry no minimum spacing - loop.go's leading
// drain and syncView's trailing drain run in one cycle iteration - so absentNodeConfirmations alone
// could be satisfied by two probes issued a second apart, i.e. by ONE window of server overload
// answering 28672 twice. With absentNodeConfirmations == 2 this makes the verdict "N agreeing probes
// AND at least this much time since the first", the same shape unmappedServerErrMinAge gives the
// deterministic-rejection streak; the floor is per counted probe, so a larger threshold scales the
// span rather than leaving the extra confirmations free.
//
// The price is latency on a genuine delete whose absence needs corroborating at all: the op stays
// queued (never lost, never applied wrongly) for one floor. A minute of that is cheap against
// acking a delete the server never performed.
const absenceStreakMinAge = 60 * time.Second

// absenceStreakMaxSpan bounds how much WALL CLOCK may separate two probes still counted as one
// run. A run is evidence that the path-error REPEATS; two observations far apart are two
// unrelated blips, not a repetition, so a stale entry restarts at one rather than completing a
// verdict.
//
// It is measured in time, and it has to be, because the floor it lives beside is. This bound
// used to count DRAIN PASSES, and that made an op's own retry schedule decide whether its
// evidence counted. A stuck op backs off exponentially, so its probes grow further apart, while
// the daemon's pass counter races ahead on everything else it drains. Measured against a live
// stand, one op's consecutive probes of a single path landed at passes 488, 742, 1251, 2273,
// 4271, 6772 … 24320 - gaps that double, because the gap IS the backoff. Every one exceeded a
// 64-pass ceiling, so the run restarted at 1 on each probe and the verdict was unreachable.
//
// TestIntegration_TwoClientsDeleteVsEdit never converged for exactly that: client B correctly
// preserved an edit that raced a peer's delete, then could not push it back because the
// target's deletion could not be proved, for 300 s, the count reading 1 every time.
//
// Ten minutes is far longer than the floor and far shorter than "unrelated": a sentinel that
// stops repeating for that long is describing a different condition.
const absenceStreakMaxSpan = 10 * time.Minute

// absenceVerdictMaxAge bounds how stale a REACHED verdict may be and still answer a later probe
// on its own. A settled streak is deliberately not cleared when it converges (the ancestor walk
// needs every rung settled in the same pass, see absenceCorroboratedByNodeProbe), so without an
// age bound it stays authoritative for as long as the run may span - and within that window
// another client can re-create the very path whose absence converged, after which ONE conflated
// 28672 rides the old evidence straight to a destructive verdict: exactly the hole this file
// exists to close. markSynced refutes it only when THIS client records agreement at the path,
// which a failed inbound download never reaches. A verdict older than this restarts at one, so
// the destructive act always rests on evidence from the passes around it; the walk pays nothing,
// since it re-reads its rungs in the same pass or the next.
const absenceVerdictMaxAge = 2

// absenceStreakCap is the soft bound on the streak map. An entry normally lives only between one
// probe of a path and its next, so the live set tracks the ops in flight; the cap is the backstop
// for a path whose op leaves the queue right after its first probe (dead-lettered, superseded,
// acked by another arm), which nothing else prunes. Crossing it evicts every entry a run can no
// longer be continued from - the SAME condition, in the same unit, that noteAbsentProbe would
// restart at one on the next probe, so the sweep can never cost a verdict the probe would have
// honoured.
//
// Read it as a sweep TRIGGER, not as a size limit: the map's real bound is the distinct paths
// probed within absenceStreakMaxSpan, and it is stated that way deliberately. Evicting live
// entries to hold a number would defeat the whole point - under a mass deletion, thousands of
// fresh entries appear at once while the op whose run must survive is by construction one of the
// OLDEST, so "evict the oldest down to the cap" would discard precisely the evidence the span
// exists to protect, in precisely the situation it was measured failing. Correctness fixes the
// eviction rule; what the cap can still bound is the sweep's COST, which
// pruneAbsenceStreaksLocked does.
const absenceStreakCap = 4096

// absenceProbeStreak is the per-path evidence of a repeated path-error: how many consecutive
// probes answered with the sentinel, the drain pass the last observation landed in (so a second
// probe inside the same pass does not count twice) and when the last COUNTED probe landed (so a
// second probe inside absenceStreakMinAge does not count either - a pass count is not a duration).
type absenceProbeStreak struct {
	count int
	pass  uint64
	// counted is the wall-clock reading of the last probe that advanced count. A later probe of
	// the same run refreshes pass (the run is unbroken) but only advances count once the floor
	// has elapsed since this instant, so the streak cannot be completed inside one overload
	// window no matter how many passes it spans.
	counted time.Time
}

// absenceMemo is the memory absenceCorroboratedByNodeProbe needs to corroborate a path-error by
// repetition. The Engine implements it; the free absence provers take it as a parameter because
// they are deliberately engine-free (they run against a leased client, not the Engine).
//
// A NIL memo means the caller has no memory to corroborate with, and the single probe's verdict
// stands - the single-probe behaviour, the same degradation a client that does not
// implement nodeProber already gets. Every production caller passes the Engine; the nil path
// exists for the narrow seams that call the provers directly.
type absenceMemo interface {
	// noteAbsentProbe folds one path-error probe of p into its streak and reports the streak
	// length. At most one confirmation is counted per drain pass, and at most one per
	// absenceStreakMinAge of wall clock.
	noteAbsentProbe(viewID int64, p string) int
	// clearAbsentProbe drops p's streak: the run of path-errors was broken by an answer that is
	// not one.
	clearAbsentProbe(viewID int64, p string)
}

// absenceStreakKey keys the streak map by view and view-relative path. The path is used verbatim
// (it is the same string the probe addresses), so no normalisation can make two different server
// paths share a streak.
func absenceStreakKey(viewID int64, p string) string {
	return strconv.FormatInt(viewID, 10) + "\x00" + p
}

// noteAbsentProbe records one path-error probe of p and reports how many consecutive probes have
// now answered that way. A second probe within the SAME drain pass reports the streak unchanged:
// the corroboration is repetition across passes, and two questions asked inside one apply are one
// server condition observed twice. So does a probe within absenceStreakMinAge of the last counted
// one, however many passes have gone by: two answers from inside one window of server overload are
// one condition observed twice as well. Such a probe still refreshes the run -
// the sentinel did repeat - so the streak survives to be completed by the first probe past the
// floor instead of ageing out and restarting.
func (e *Engine) noteAbsentProbe(viewID int64, p string) int {
	key := absenceStreakKey(viewID, p)
	now := e.now()
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	if e.nodeAbsentConfirms == nil {
		e.nodeAbsentConfirms = make(map[string]absenceProbeStreak)
	}
	pass := e.drainPass
	st, tracked := e.nodeAbsentConfirms[key]
	switch {
	case tracked && st.pass == pass:
		return st.count
	case tracked && st.count >= absentNodeConfirmations && pass-st.pass > absenceVerdictMaxAge:
		// A verdict this old is evidence about the node as it was, not as it is: the path may
		// have been re-created since, and answering the current probe from it would let one
		// conflated 28672 authorise a destructive apply. Start a fresh run.
		st.count, st.counted = 1, now
	case tracked && now.Sub(st.counted) <= absenceStreakMaxSpan:
		switch {
		case st.count >= absentNodeConfirmations:
			// Clamped: past the threshold the count carries no more information, and an op that
			// keeps probing for the lifetime of the daemon must not grow one without bound.
		case now.Sub(st.counted) < absenceStreakMinAge:
			// Inside the floor: the run continues (st.pass is refreshed below) but this answer is
			// not independent of the one already counted, so it may not complete a verdict.
		default:
			st.count, st.counted = st.count+1, now
		}
	default:
		st.count, st.counted = 1, now
	}
	st.pass = pass
	e.nodeAbsentConfirms[key] = st
	e.pruneAbsenceStreaksLocked(pass, now)
	return st.count
}

// clearAbsentProbe drops p's streak. Callers hold no lock.
//
// Two things call it: a probe that answered with anything other than the sentinel (the run is
// broken), and markSynced - recording agreement at a path is proof the node occupies it, and a
// streak that already reached its verdict is deliberately left in place for the ancestor walk's
// sake, so without that reset a re-created path could have a later absence corroborated by a
// single conflated 28672 riding on the previous deletion's evidence.
func (e *Engine) clearAbsentProbe(viewID int64, p string) {
	key := absenceStreakKey(viewID, p)
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	delete(e.nodeAbsentConfirms, key)
}

// pruneAbsenceStreaksLocked drops entries a run can no longer be continued from once the map
// crosses absenceStreakCap. Callers hold stuckMu.
//
// Eviction reads the same clock, against the same bound, as the run continuity it backs up. It
// used to evict by DRAIN PASSES while continuity was also counted in passes; when continuity
// moved to wall clock the sweep kept the pass bound, which quietly restored the bug it was
// paired with - a stuck op's probes are hundreds of passes apart because that gap IS its backoff,
// so precisely the entries the wall-clock bound exists to keep alive were the ones a sweep threw
// away. And it bites hardest where it hurts most: crossing the cap needs thousands of distinct
// probed paths, i.e. a large subtree gone server-side, which is exactly when those ops are
// backing off.
//
// Tying it to absenceStreakMaxSpan makes the sweep unobservable by construction: it deletes only
// entries whose next probe would restart the run at one anyway, so no verdict in flight can be
// lost to it, whatever the daemon's pass counter is doing.
//
// Two guards bound its cost, because a wall-clock rule can leave the map over the cap with
// nothing yet evictable - a state a pass-based rule reached only briefly:
//
//   - At most ONCE per drain pass. A second sweep inside the same pass can only find entries the
//     first left, aged by at most that pass's own duration, while the map only grows as the pass
//     keeps probing.
//   - Not at all until the oldest survivor could have aged out. Without this, an over-cap map of
//     entries that are all still live pays a full walk under stuckMu every pass - up to ~80 times
//     a second in the regime opbackoff.go measured - to delete nothing, for as long as ten
//     minutes. stuckMu is what the other drain goroutines and the daemon's StuckOps() status read
//     contend for, and the flood that crosses the cap (a large subtree gone server-side) is
//     exactly when they are busiest.
//
// The idle mark is sound because it is computed over the SURVIVORS: every entry added after a
// sweep is younger than the oldest of them, so nothing can become evictable before the recorded
// instant. Skipping the walk therefore never delays an eviction - it only declines to look when
// looking provably cannot find anything.
func (e *Engine) pruneAbsenceStreaksLocked(pass uint64, now time.Time) {
	if len(e.nodeAbsentConfirms) <= absenceStreakCap || e.absenceStreaksPruned == pass {
		return
	}
	if !e.absenceSweepIdleUntil.IsZero() && now.Before(e.absenceSweepIdleUntil) {
		return
	}
	e.absenceStreaksPruned = pass
	var oldest time.Time
	for k, st := range e.nodeAbsentConfirms {
		if now.Sub(st.counted) > absenceStreakMaxSpan {
			delete(e.nodeAbsentConfirms, k)
			continue
		}
		if oldest.IsZero() || st.counted.Before(oldest) {
			oldest = st.counted
		}
	}
	// The earliest instant any survivor becomes evictable. Zero only if the sweep emptied the
	// map, which leaves it under the cap and short-circuits above anyway.
	e.absenceSweepIdleUntil = time.Time{}
	if !oldest.IsZero() {
		e.absenceSweepIdleUntil = oldest.Add(absenceStreakMaxSpan)
	}
}

// beginDrainPass advances the drain-pass counter. drainQueue calls it once per pass, which is what
// makes "consecutive probes" mean "across drains" rather than "within one apply".
func (e *Engine) beginDrainPass() {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	e.drainPass++
}
