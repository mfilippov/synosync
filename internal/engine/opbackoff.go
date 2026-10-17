package engine

// This file carries the per-op retry backoff for a push the server keeps refusing the same
// deterministic way (two-client finding H4).
//
// Measured on the live stand (14.08.2026, TestIntegration_TwoClientsFileDirClash): a `mkdir`
// the server refused with 24581 was re-issued **23 610 times in ~5 minutes** - about 80
// requests per second - before the deterministic-rejection escalation's five-minute floor
// finally dropped it. The scenario itself triggered a sync only once per second, so that rate
// was the engine's own: nothing between a failed apply and the next drain's re-apply of the
// same op imposed any delay at all. drainQueue is entered once per cycle plus once per changed
// view (syncView's trailing drain), and a wake - a watcher event, a `synosync sync` - starts
// the next cycle immediately, so under local churn the drains come back-to-back and every one
// of them re-sends the whole doomed turn (for an upload, the whole BODY).
//
// The pause is armed on the ONE shape that is known to be pointless to repeat quickly: an
// unbroken run of the identical server result code on a push-class op - exactly the evidence
// escalateDeterministicServerFailure already accumulates, which is why the arming lives at
// that single site. Nothing else is slowed: a first failure never waits, a broken connection
// (which the pool re-dials underneath) never waits, and any change in the failure clears the
// streak and with it the pause.
//
// It is in-memory only, like the streak itself and the stuck counters: a daemon restart
// re-earns the pause after one more identical rejection, which is the same trade every other
// per-op counter here makes. The map is keyed by queue seq and dropped by clearOpFailures the
// moment the op leaves the queue, so it cannot outgrow the live queue.

import "time"

// retryBackoffBase is the pause after the SECOND identical rejection - the first repetition,
// i.e. the first evidence that re-sending immediately is pointless. A single failure is never
// delayed at all, so a genuine transient still recovers on the very next drain.
const retryBackoffBase = 500 * time.Millisecond

// retryBackoffCap bounds the geometric growth. It is deliberately of the order of the loop's
// own cadence (config.Limits.LocalScanInterval defaults to 5s, and a drain runs at least once
// per cycle), so a doomed op costs about one turn per cycle instead of the measured ~80 per
// second - a ~400x reduction - while a server-side condition that does clear is still re-probed
// promptly.
//
// It stays BELOW the escalation's wall-clock floors on purpose (asserted in the tests): if the
// pause were the longer of the two, the pacing rather than the accumulated evidence would decide
// when an op dead-letters, and a floor tuned as "minutes of identical rejections" would in
// practice mean "however long the backoff takes to get there".
const retryBackoffCap = 5 * time.Second

// retryBackoff is the pause to impose after a streak of n identical rejections: none for the
// first failure, retryBackoffBase for the second, doubling from there and capped at
// retryBackoffCap. The doubling is computed with a bounded loop rather than a shift so a long
// streak cannot overflow the shift count.
func retryBackoff(streak int) time.Duration {
	if streak < 2 {
		return 0
	}
	d := retryBackoffBase
	for i := 2; i < streak; i++ {
		d *= 2
		if d >= retryBackoffCap {
			return retryBackoffCap
		}
	}
	return d
}

// deferOpRetry arms (or re-arms) the pause before the next attempt of an op whose failure has
// now repeated streak times with the identical server result code. A streak of one arms
// nothing, so the common single-failure case keeps retrying on the very next drain.
func (e *Engine) deferOpRetry(seq uint64, streak int) {
	d := retryBackoff(streak)
	if d <= 0 {
		return
	}
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	if e.opRetryAfter == nil {
		e.opRetryAfter = make(map[uint64]time.Time)
	}
	e.opRetryAfter[seq] = e.now().Add(d)
}

// opRetryPending reports whether an op is still inside the pause armed for it, and how much of
// it is left (for the diagnostic log line). An elapsed pause is dropped as it is read, so the
// map holds only live entries between drains.
func (e *Engine) opRetryPending(seq uint64) (time.Duration, bool) {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	at, armed := e.opRetryAfter[seq]
	if !armed {
		return 0, false
	}
	if left := at.Sub(e.now()); left > 0 {
		return left, true
	}
	delete(e.opRetryAfter, seq)
	return 0, false
}
