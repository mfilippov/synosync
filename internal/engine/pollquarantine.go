package engine

// This file isolates a single poisoned member of the SHARED long_poll subscription.
// pollOnce subscribes to every eligible view in one turn, so a
// server that rejects the whole subscribe set because of ONE bad member failed inbound sync for
// EVERY view: the error is not a deadline, so Run retried it after pollRetryDelay with an
// identical subscribe set, forever, at roughly one Warn per second. Every other shared stage had
// already been isolated per view (localScan, coldStartPending and syncView all log-and-continue);
// long_poll was the last shared-fate one, and its only quarantine - absentViews - is fed solely
// by the query_user_info enumeration, so a view the enumeration keeps listing while long_poll
// keeps rejecting it had no exit at all.
//
// The isolation is deliberately conservative:
//
//   - It arms only after pollFailBisectThreshold consecutive failures of the shared poll that the
//     SERVER stated itself (pollServerRejected). A broken socket, a malformed reply, a cancelled
//     context and a client-side deadline keep the pre-existing recovery path (re-dial, retry)
//     untouched - those are not evidence about any member of the set. The streak is classified on
//     exactly the axis the verdict uses - POSITIVE evidence of a server verdict - never as an
//     exclusion list over transport sentinels: such a list arms the probes on every failure shape
//     it forgot to name, spending a worker lease per view against a server that is simply down.
//     (ErrConnBroken now rides the poisoning turn as well as the subsequent ones, so an exclusion
//     list would catch that particular shape today - but the classification does not depend on
//     which shapes carry it, which is the point.)
//   - The probes run on WORKER leases, not on the event connection, so a probe that breaks its
//     connection is re-dialled by withWorker and the event connection the next poll needs is
//     never disturbed.
//   - A view is quarantined only when its OWN single-member subscription is rejected BY THE
//     SERVER - an application-level errno (pollViewRejected). That is positive evidence about
//     that view, not an inference from the set's failure. A transport, decode or budget failure
//     of the probe itself is no verdict at all: it says nothing about membership, and treating it
//     as one would let a server- or network-wide outage quarantine every view at once and stop
//     inbound sync account-wide for pollPoisonRetryCycles on non-evidence. If no probe produces a
//     rejection, nothing is quarantined (the failure was about the set, the connection, or the
//     server as a whole) and the ordinary retry continues.
//   - A quarantine is temporary and observable: it Warns once, keeps the view out of the shared
//     subscribe set, and expires after pollPoisonRetryCycles cycles so the view re-probes itself
//     by simply rejoining. A still-poisoned view earns the mark again after the same threshold.
//   - A sweep that attributes the rejection to NO member suppresses the next
//     pollProbeSuppressCycles arming attempts. A set-level (or server-wide) rejection the probes
//     can never attribute would otherwise re-run the whole sweep every pollFailBisectThreshold
//     retries for as long as it lasts - more turns and more log volume than the spin this file
//     exists to stop.
//
// Whether a live DSM can reject a subscribe set because of one member is unconfirmed (DESIGN
// §13); the mechanism is safe either way - with no such rejection the threshold is never reached
// and not a single extra turn is issued.

import (
	"context"
	"errors"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// pollFailBisectThreshold is how many consecutive non-connection-level shared-long_poll failures
// must accumulate before the per-view probes run. Three failures at pollRetryDelay are seconds of
// evidence, far past a one-off server hiccup, while still bounding the outage to seconds rather
// than the lifetime of the daemon.
const pollFailBisectThreshold = 3

// pollPoisonRetryCycles is how many poll cycles a quarantine holds before the view rejoins the
// shared subscription to re-prove itself. A server-side fix (a repaired share, a restored
// permission) must not need a daemon restart to take effect; a still-broken view costs one more
// detection round.
const pollPoisonRetryCycles = 20

// pollProbeSuppressCycles is how many poll cycles must elapse after a sweep that quarantined
// nobody before the probes may arm again. The sweep already proved the failing set is not
// attributable to one member, and nothing about the account changes in the seconds the streak
// takes to rebuild: re-probing every pollFailBisectThreshold retries would issue N worker turns
// and 2+N Warn lines every few seconds for the whole outage. It matches pollPoisonRetryCycles so
// both exits are one detection round apart.
const pollProbeSuppressCycles = pollPoisonRetryCycles

// pollProbeTimeout is the long_poll timeout (seconds) used for a single-view probe: the shortest
// meaningful subscribe-and-wait turn, since the probe only needs the server's ACCEPTANCE, not a
// change. A healthy view answers with an empty change set as soon as the timeout elapses.
const pollProbeTimeout = 1

// pollProbeBudget bounds one probe turn's wall clock so the diagnosis cannot itself become the
// stall it diagnoses.
const pollProbeBudget = 30 * time.Second

// notePollFailure records one shared-long_poll failure and reports whether the per-view probes
// should run now. Only a rejection the SERVER stated itself (pollServerRejected) is counted:
// a broken socket, a malformed reply, a decode failure or a cancelled context say nothing about
// the subscribe set's membership and already have their own recovery, so they neither count nor
// clear the streak. A single-member set is never bisected - there is nothing to isolate it from,
// and the caller's ordinary retry is the right response. Nor is a set the previous sweep could
// not attribute to any member re-probed until the suppression has aged out.
func (e *Engine) notePollFailure(err error, subscribe []int64) bool {
	if !pollServerRejected(err) {
		return false
	}
	e.pollFailStreak++
	if e.pollProbeSuppress > 0 {
		return false
	}
	return e.pollFailStreak >= pollFailBisectThreshold && len(subscribe) > 1
}

// clearPollFailStreak resets the streak after a poll that reached the server (a reply, or our own
// client-side deadline on a quiet connection), so only an UNBROKEN run of rejections arms the
// probes.
func (e *Engine) clearPollFailStreak() {
	e.pollFailStreak = 0
}

// probePoisonedPollViews subscribes to each member of the failing set on its own worker lease and
// quarantines every view whose single-member subscription is rejected. It returns the number of
// views quarantined; a terminal error (a cancelled run, a paused pool) is returned so the caller
// can stop, and any other probe failure is logged and treated as "no evidence about this view".
func (e *Engine) probePoisonedPollViews(ctx context.Context, subscribe []int64) (int, error) {
	e.log.Warn("engine: the shared long poll keeps failing; probing each subscribed view to isolate the cause",
		"views", len(subscribe), "failures", e.pollFailStreak)
	quarantined := 0
	for _, id := range subscribe {
		if err := ctx.Err(); err != nil {
			return quarantined, err
		}
		probeErr := e.probeOnePollView(ctx, id)
		switch {
		case probeErr == nil:
			continue
		case isTerminal(probeErr):
			return quarantined, probeErr
		case pollViewRejected(probeErr):
			view := uint64(id) // #nosec G115 -- a view_id is a non-negative identifier
			e.poisonedViews[view] = 0
			quarantined++
			e.log.Warn("engine: view rejected on its OWN long poll subscription; quarantining it from the shared "+
				"subscription so the other views keep syncing (it rejoins to re-probe itself later)",
				"view", id, "cycles", pollPoisonRetryCycles, "err", probeErr)
		default:
			// The probe itself failed (a broken or unreadable connection, a malformed reply, its
			// own budget): the server never rejected this view, so there is no verdict to act on
			// and the pool has already re-dialled the worker. Leave the view alone.
			e.log.Warn("engine: long poll probe failed without a server rejection; no verdict for this view",
				"view", id, "err", probeErr)
		}
	}
	e.clearPollFailStreak()
	if quarantined == 0 {
		// Not attributable to one member: hold the probes off for a while rather than re-running
		// the whole sweep every pollFailBisectThreshold retries for the duration of the condition.
		e.pollProbeSuppress = pollProbeSuppressCycles
		e.log.Warn("engine: no view was rejected on its OWN long poll probe; the shared rejection is not "+
			"attributable to one member, so the poll keeps retrying unchanged",
			"probes_suppressed_cycles", pollProbeSuppressCycles)
	}
	return quarantined, nil
}

// pollServerRejected reports whether err is the SERVER's own application-level rejection of a
// long_poll subscription - the only shape that is evidence about the subscribed set rather than
// about the connection carrying it. An unmapped result code (*actions.ServerError) is the shape a
// subscribe refusal takes; the path-error sentinel covers a view whose root the server can no
// longer resolve. Everything else - a socket error, a decode failure, actions.ErrConnBroken, a
// budget expiry - is a failure of the TURN, not a statement about what was subscribed.
//
// The test is deliberately POSITIVE rather than an exclusion list over transport sentinels.
// An exclusion list classifies as a server rejection every failure shape it forgot to name, and
// the set of shapes is not stable: ErrConnBroken, for one, used to guard only the turns AFTER
// the poisoned one and now rides the poisoning turn too (actions.Client.doIdle wraps sentinel and
// cause). Requiring a result code or the path-error sentinel is invariant under that drift.
func pollServerRejected(err error) bool {
	if _, ok := actions.ServerErrorCode(err); ok {
		return true
	}
	return errors.Is(err, actions.ErrPathError)
}

// pollViewRejected is pollServerRejected read on the single-member axis: the server rejecting a
// view's OWN subscription is positive evidence about that view, and the only shape that may
// quarantine it. A probe that fails on transport, decode or its own budget is no verdict at all.
func pollViewRejected(err error) bool { return pollServerRejected(err) }

// errPollProbeBudget classifies a probe aborted by its OWN pollProbeBudget while the caller's
// context is still live: a transient diagnosis failure, never a reason to stop Run. The raw
// context.DeadlineExceeded the abort produces is indistinguishable from the run context's own
// expiry, which isTerminal reads as "stop Run" - so an unmapped probe timeout crashed the daemon
// on exactly the failing server that armed the probes (the same unmapped-timeout hazard, one stage
// further out). Long_poll is deliberately excluded from turnLimitedClient's mapping, so the probe
// must do it itself.
var errPollProbeBudget = errors.New("engine: long poll probe exceeded its own budget")

// pollProbeBudgetError carries the aborted probe's underlying error for the log line while
// matching only the errPollProbeBudget sentinel. Like turnTimeoutError it deliberately has NO
// Unwrap, so errors.Is can never reach context.DeadlineExceeded through it.
type pollProbeBudgetError struct{ cause error }

func (e *pollProbeBudgetError) Error() string {
	return errPollProbeBudget.Error() + ": " + e.cause.Error()
}

// Is matches the errPollProbeBudget sentinel without unwrapping to the context error.
func (e *pollProbeBudgetError) Is(target error) bool { return target == errPollProbeBudget }

// probeOnePollView runs one single-view long_poll turn on a worker lease under pollProbeBudget,
// mapping an expiry of that budget to the typed transient errPollProbeBudget (see above). The
// probe ctx is parent + the budget, so a DeadlineExceeded surfacing while the parent is still
// live can only be the budget; a dead parent passes the error through unchanged so a real run
// cancellation stays terminal.
func (e *Engine) probeOnePollView(ctx context.Context, id int64) error {
	pctx, cancel := context.WithTimeout(ctx, pollProbeBudget)
	defer cancel()
	err := e.withWorker(pctx, func(c syncClient) error {
		_, lerr := c.LongPoll(pctx, []actions.Subscription{{ViewID: id}}, pollProbeTimeout)
		return lerr
	})
	if err == nil || ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &pollProbeBudgetError{cause: err}
}

// agePollQuarantines advances every quarantine by one poll cycle and releases the ones that have
// served pollPoisonRetryCycles, so the view rejoins the shared subscription and re-proves itself.
// It also ages the probe suppression an unattributable sweep left behind. Called once per
// pollOnce, before the subscribe set is built.
func (e *Engine) agePollQuarantines() {
	if e.pollProbeSuppress > 0 {
		e.pollProbeSuppress--
	}
	for view, cycles := range e.poisonedViews {
		cycles++
		if cycles < pollPoisonRetryCycles {
			e.poisonedViews[view] = cycles
			continue
		}
		delete(e.poisonedViews, view)
		e.log.Info("engine: long poll quarantine expired; the view rejoins the shared subscription to re-probe itself",
			"view", view)
	}
}
