package engine

// This file carries the terminal classification of DETERMINISTIC server rejections.
//
// actions.errorForCode types exactly five result codes; any other non-zero errno is an untyped
// *actions.ServerError, and the executor's terminal classes are all LOCAL shapes. Without this
// file an unmapped server rejection (a share quota, an ACL, a naming policy) fails every drain
// with no retry cap, age escalation or dead-letter, and because drainBatch serialises overlapping
// successors behind it (blockedKeys), a refused `mkdir X` wedges every create under X forever.
// This is the nametoolong doctrine ("a path component the host can never create is terminal, not
// transient") applied to the server side.
//
// An unknown code is NOT by itself proof of permanence, so the escalation is conservative:
//
//   - Only an op failing unmappedServerErrDrains consecutive drains with the SAME unmapped code,
//     over at least unmappedServerErrMinAge of wall clock, escalates. Alternating codes, or any
//     other failure in between, reset the streak.
//   - Only PUSH-CLASS ops escalate: an outbound apply, and the keep-both resolver
//     (OpResolveConflict, push-class turns from applyOp's early return). Dropping either deletes
//     nothing: the local file stays, the server is asked nothing, and the baseline is marked
//     StateConflict so the path shows in `Unresolved` (the errParentConflicted precedent). An
//     INBOUND op never escalates - its event is already consumed and no later drain re-derives it.
//   - The unknown-code threshold sits above stuckThreshold, so such an op is first counted in
//     `Stuck` with a Warn naming the server code, and only then dropped.
//   - The two MEASURED name refusals take the shorter nameRejectionDrains/nameRejectionMinAge
//     (finding H4; see serverStructuralRejection). Their window is deliberately BELOW
//     stuckThreshold: a name-refused push is dropped before it counts as `Stuck`, observable via
//     the escalation Warn and the durable trace (`Dropped`).
//   - The one code measured TRANSIENT (32772, finding H5) is counted and paced but never
//     escalates: a code the server clears on its own cannot prove permanence (see
//     serverTransientRejection).
//
// Repetition is also PACED here: each drain that folds a repeat rejection into the streak arms a
// geometric pause before the op's next attempt (opbackoff.go); unpaced, a doomed turn re-issues
// every drain. The pacing - and ONLY the pacing - extends to inbound applies through
// paceInboundServerFailure: the spin is direction-blind (32772 was measured on an inbound
// `download`), but an inbound drop would lose a server change whose event is already consumed.
//
// A dead-lettered outbound op leaves a durable trace carrying the code, observable in `status`
// (Dropped).

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// unmappedServerErrDrains is the number of CONSECUTIVE drains an outbound op must fail with
// the same unmapped server result code before the rejection is treated as deterministic and
// the op is dead-lettered. It is twice stuckThreshold so the op is already counted in
// `Stuck` (and its escalation Warn has already named the code) well before it is dropped.
const unmappedServerErrDrains = 2 * stuckThreshold

// unmappedServerErrMinAge is how long the identical-code streak must ALSO have lasted in wall
// clock. The drain count alone is not a duration: drainQueue runs once per cycle plus once per
// changed view (syncView's trailing drain), so an account with remote activity can burn the
// whole count in a couple of cycles - seconds, not the "minutes of identical rejections" the
// evidence argument rests on. An unknown code is not by itself proof of permanence, and the
// escalation is one-way for the op (its baseline is marked StateConflict, which localReconcile
// then leaves alone, so the push is never re-derived by a later scan), so the floor keeps a
// server-side blip under load from permanently abandoning a healthy local change.
const unmappedServerErrMinAge = 5 * time.Minute

// nameRejectionDrains and nameRejectionMinAge are the SHORTENED confirmation the two measured
// name-refusal codes take instead of the unknown-code window above (two-client finding H4).
//
// The generic thresholds exist because an unknown code is not proof of permanence: it may well
// report a transient server-side condition this client cannot name, so the escalation waits out
// twenty drains and five minutes before abandoning a healthy local change. 24580 and 24581 are
// not unknown. Both were measured on the stand and both are verdicts about the NAME the push
// asks for - a fold-equal sibling already exists (DSM's uniqueness is case-insensitive), or a
// node of the other type already holds the path. Neither premise changes because the client
// waits: the same request refused once is refused identically five minutes later, which is
// exactly what the stand recorded (23 610 attempts, one outcome). Spending the full window on
// them buys nothing and costs a lot: the push re-sends its whole body every drain, and
// blockedKeys holds every path-overlapping op - including the authoritative INBOUND work for
// the same path - behind it for the duration.
//
// What the shortened window still buys is corroboration by repetition, the same doctrine every
// verdict-by-errno here rests on: a single reply can be misread, a server can answer oddly
// once. So the drop takes nameRejectionDrains agreeing rejections AND nameRejectionMinAge of
// wall clock - a pass count is not a duration (drainQueue runs once per cycle plus once per
// changed view, so three passes can land inside one second), and the pause armed between
// attempts (opbackoff.go) is what makes the count and the clock advance together.
const (
	nameRejectionDrains = 3
	nameRejectionMinAge = 10 * time.Second
)

// serverStructuralRejection reports whether a streak code is one of the MEASURED refusals whose
// determinism is known rather than inferred, so it takes the shortened confirmation above. The
// synthetic negative codes and every unknown code answer false and keep the conservative
// window.
//
// 24584 joins the two name refusals because it is the same clash seen from the other side.
// When one peer makes a path a FILE and another makes it a DIRECTORY, the loser's mkdir for the
// path draws 24581 and is dropped -- but every push BENEATH that path draws 24584, because the
// parent it names is the peer's file. Left in the unknown-code window those descendants held the
// full five minutes, and blockedKeys holds every path-overlapping op behind them: including the
// INBOUND download of that very path, which is the only thing that resolves the clash. So the
// two peers could not converge until the window expired, and TestIntegration_TwoClientsFileDirClash
// timed out at three minutes with the queue wedged. The refusal also desynced the connection on
// every retry (see actions.CodeParentNotADirectory), so waiting cost a session per drain.
//
// The determinism argument is the one 24580/24581 rest on, unchanged: the premise is a fact
// about the server's tree, not about timing, and it does not change because the client asks
// again. Measured three times out of three, for both verbs.
func serverStructuralRejection(code int64) bool {
	return code == actions.CodeNameCaseCollision ||
		code == actions.CodeNameTypeCollision ||
		code == actions.CodeParentNotADirectory
}

// serverTransientRejection reports whether a streak code is one whose transience was MEASURED,
// so no run of it - however long - is evidence of permanence and the op must never be
// dead-lettered for it (two-client finding H5). Today that is exactly 32772: a live DSM answers
// it for a read of a node another client has just overwritten and serves the identical request a
// second later, so the escalation counting it was counting the server settling, not refusing.
//
// It is deliberately NOT expressed by mapping the code to a bare sentinel in
// actions.errorForCode, which would drop it out of the streak entirely: the streak is also what
// arms the retry pacing (opbackoff.go), and an op that keeps drawing a transient code should
// still back off rather than re-issue its whole body every drain. So the code stays counted and
// paced, and only the terminal exit is closed to it.
//
// uploadAbsenceUnprovedCode joins it for the opposite reason: it is not a server verdict at all
// but this client waiting for corroboration it will get, so a run of it is evidence of nothing
// except that the streak has not reached absentNodeConfirmations yet. Dead-lettering a push for
// its own precondition still gathering evidence would drop the user's file, and the wait is
// self-limiting - so it is paced like a repeat rejection and denied the terminal exit.
// CodeStaleCursor is here for a third reason again: it is CONTENTION, not a refusal. INVARIANTS
// says 40962 is retried and never treated as a failure, and actions.Rename does retry it - but
// only renameCursorAttempts times, after which the raw *ServerError reaches the engine as an
// ordinary unmapped code and a sustained contender on one node could dead-letter the move. The
// verdict "another writer touched this node between the read and the rename" is never evidence
// that the move can never happen, so the streak may pace it and must not drop it.
func serverTransientRejection(code int64) bool {
	return code == actions.CodeNodeBusy ||
		code == actions.CodeStaleCursor ||
		code == uploadAbsenceUnprovedCode ||
		code == removeAbsenceUnprovedCode ||
		code == uploadNoFileIDCode ||
		code == renameTargetUnprovedCode
}

// escalationThresholds picks the confirmation a streak code must satisfy before its op is
// dead-lettered: the shortened one for a measured name refusal, the conservative one for
// anything else. A measured-transient code never reaches a threshold at all (see
// serverTransientRejection), so it is not represented here.
func escalationThresholds(code int64) (int, time.Duration) {
	if serverStructuralRejection(code) {
		return nameRejectionDrains, nameRejectionMinAge
	}
	return unmappedServerErrDrains, unmappedServerErrMinAge
}

// resumeNoCompletionCode is the synthetic streak code standing in for errResumeNoCompletion -
// a chunked upload the server never reports as complete in any recognised form.
// It is NEGATIVE so it can never collide with a real server result code (an errno is
// non-negative), and it rides the same machinery for the same reason: an unbroken run of the
// identical non-answer is the same shape of determinism evidence as an unbroken run of one
// unmapped errno, and without it the op re-streamed its last chunk every drain forever. Every
// other failure class in between (a token error, a broken connection, a real errno) restarts
// the streak exactly as it does for a server code.
const resumeNoCompletionCode int64 = -1

// resolveNoServerIDCode is the synthetic streak code standing in for errResolveNoServerID -
// a keep-both resolve whose server node answers without a file id, so the resolution cannot
// run at all (conflict.go). Like resumeNoCompletionCode it is NEGATIVE so it can never collide
// with a real errno, and distinct from it so a trace names which non-answer repeated.
//
// This reply shape is reachable from the vanished-upload funnel: a DSM
// that systematically omits file_id funnels into the resolver and fails there every drain,
// with no result code for the escalation to count and blockedKeys holding the authoritative
// inbound remove/rename of the same path behind it. An unbroken run of the identical non-answer
// is the same determinism evidence, so it takes the same exit.
const resolveNoServerIDCode int64 = -2

// removeDirUnverifiedCode is the synthetic streak code standing in for errRemoveDirUnverified -
// an outbound DIRECTORY delete whose server subtree could not be enumerated well enough to prove
// it holds only agreed content (outbound_removedir.go). Negative for the same reason as its two
// siblings, and distinct so a trace names which non-answer repeated.
//
// It needs one because the arm carries no result code of its own: an errno-only `list` reply
// (listed == false), a descendant listing refused with the bare ErrPathError sentinel, or a child
// the reply cannot identify all leave a plain error, so ServerErrorCode answers false, the streak
// clears and deferOpRetry arms nothing. The op then re-walks the WHOLE server subtree - one `list`
// turn per directory, on a worker lease - every drain, forever: the H4 spin the pacing exists to
// stop, on the most expensive turn in the engine. Folded in here it is paced like any repeat
// rejection and, on the conservative unknown-code window, takes the same observable exit
// (StateConflict + a durable trace) instead of wedging its subtree behind blockedKeys.
const removeDirUnverifiedCode int64 = -3

// uploadAbsenceUnprovedCode is the synthetic streak code standing in for
// errUploadTargetAbsenceUnproved - a MODIFY whose target probe answered the path-error sentinel
// too few times to prove the node gone (outbound_uploadguard.go). Negative like its three
// siblings, and distinct so a trace names which non-answer repeated.
//
// The arm carries no result code of its own (ErrPathError is a mapped sentinel, not an unmapped
// errno), so without this ServerErrorCode answers false, noteServerErr clears the streak AND the
// pause, and the op re-opens its source and re-issues the probe on every drain for as long as the
// corroboration takes - absentNodeConfirmations probes spaced by absenceStreakMinAge, i.e. minutes
// of exactly the H4 spin (~80 attempts a second under churn) that this pacing exists
// to stop. Folded in here it backs off geometrically instead, which costs nothing: the pause caps
// at retryBackoffCap, well under the 60 s the corroboration is spaced by anyway.
//
// It is a TRANSIENT code (serverTransientRejection): the streak it stands for is this client's own
// evidence-gathering, not a server refusal, and it clears itself the moment the probe answers.
const uploadAbsenceUnprovedCode int64 = -4

// removeAbsenceUnprovedCode is the synthetic streak code standing in for
// errRemoveAbsenceUnproved - an outbound OpRemove whose post-turn classification has not yet
// settled the target's fate (outbound_mutate.go). Negative like its four siblings, and distinct so
// a trace names which non-answer repeated.
//
// It is the DELETE-axis twin of uploadAbsenceUnprovedCode, for the same mechanical reason: the arm
// returns the mapped ErrPathError sentinel, which carries no unmapped errno, so without this the
// streak and the pause are cleared on every attempt and the op re-issues `remove` + the parent
// `list` + the corroborating QueryNode on every drain for the whole corroboration window. It is the
// most frequently reached instance of that spin, because every child of a directory the server's
// RECURSIVE remove already took enters it.
//
// TRANSIENT for the same reason as its upload twin: the streak is this client's own
// evidence-gathering (or a bounded wait on an inbound rename event), never a server verdict, so it
// is paced and never dead-lettered.
const removeAbsenceUnprovedCode int64 = -5

// uploadNoFileIDCode is the synthetic streak code standing in for errUploadNoFileID - a placing
// turn (upload or mkdir) that succeeded while naming no node, with the recovery probe unable to
// supply one. Negative like its five siblings, and distinct so a trace names which non-answer
// repeated.
//
// It closes the last arm of the H4 class the other codes cover: the error carried no
// *actions.ServerError and no synthetic code, so noteServerErr took its !ok branch and cleared
// BOTH the streak and the retry pause on every drain. The upload arm is the expensive one - a
// retry re-streams the entire file body, since guardUploadTarget reads a server hash equal to
// ours as its own lost reply and proceeds - so an id-omitting server turned every fresh create
// into an unbounded full-body re-upload loop.
//
// TRANSIENT, like the two absence codes: the server did the work and only the client's own
// bookkeeping is incomplete, so dropping the op would abandon a file the server already holds
// while the next scan re-derives the same push anyway. Pacing is the whole remedy.
const uploadNoFileIDCode int64 = -6

// renameTargetUnprovedCode is the synthetic streak code standing in for
// errRenameTargetUnproved - an outbound rename whose destination probe neither produced a node
// nor said the path is free (guardRenameTarget).
//
// Negative like its six siblings. TRANSIENT, for the same reason as the two absence codes: it
// is not a server verdict at all but this client's own precondition still gathering evidence,
// and a move must not be dropped because its guard has not finished asking. Pacing is the whole
// remedy - without it the probe repeats at full drain cadence.
const renameTargetUnprovedCode int64 = -7

// renameUnsettledCode is the synthetic streak code standing in for a rename that failed with a
// bare ErrPathError no arm could settle: both parents probed alive, the source node still at
// its path, the destination proved free before the turn.
//
// Unlike its siblings this one stands in for a MAPPED sentinel, which is exactly the problem it
// fixes: actions.ErrPathError carries no *actions.ServerError, so ServerErrorCode answered
// false, noteServerErr took its !ok branch and cleared BOTH the streak and the retry pause on
// every drain. The rename was the one mutating verb with no synthetic code of its own, so a
// server refusing it deterministically - the shape a destination-occupancy refusal would take -
// re-issued the turn plus two ancestor walks every drain forever, with every path-overlapping
// op, including the peer's inbound apply for the same node, held behind it on blockedKeys.
//
// NOT transient: the shape it names is a deterministic refusal the client has run out of
// questions for, so a streak of it takes the ordinary escalation exit and the operator gets a
// dead-letter naming the reason instead of a permanently stuck queue.
const renameUnsettledCode int64 = -8

// serverErrStreak is the per-op evidence of a deterministic server rejection: the unmapped
// result code the op last failed with, how many consecutive drains it has failed with exactly
// that code, and when the streak began. A different code (or any failure that is not an
// unmapped server rejection) restarts the streak - see noteServerErr.
type serverErrStreak struct {
	code  int64
	count int
	since time.Time
}

// unprovedByTransport reports whether a precondition failure that carries no server result code
// of its own failed because the CONNECTION did, rather than because the server answered
// something. The synthetic streak codes stand in for "the server keeps giving the same
// non-answer", which is evidence of determinism only when a turn actually completed; a wrapped
// transport failure is the shape noteServerErr's !ok branch clears the streak for everywhere
// else, and folding it in under a synthetic code would let an outage supply the evidence for a
// terminal exit.
//
// The set is isTerminalRefreshError's transport half (engine.go), minus the session-level
// verdicts, which stop the run rather than reaching any streak.
func unprovedByTransport(err error) bool {
	return errors.Is(err, actions.ErrConnBroken) ||
		errors.Is(err, actions.ErrTransferStalled) ||
		errors.Is(err, errTurnTimeout) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// noteServerErr folds one outbound apply failure into the op's deterministic-rejection
// streak and reports the code, the new consecutive count and how long the streak has run.
// A failure that is not an unmapped server rejection (a broken connection, a turn timeout, a
// store hiccup, or a typed code with its own handling) clears the streak and reports zeroes:
// the proof this escalation rests on is an UNBROKEN run of identical codes.
func (e *Engine) noteServerErr(seq uint64, err error) (int64, int, time.Duration) {
	code, ok := actions.ServerErrorCode(err)
	switch {
	case ok:
	case errors.Is(err, errResumeNoCompletion):
		// A resume upload that streamed every byte and was never told it finished carries
		// no result code at all - the reply was a SUCCESS the client cannot read as one. It
		// is folded in under a synthetic code so the identical-run evidence, the wall-clock
		// floor and the dead-letter exit are the ones already proven for server rejections.
		code, ok = resumeNoCompletionCode, true
	case errors.Is(err, errResolveNoServerID):
		// A resolve whose server node answers with no file id carries no result code either
		// - the reply was not an error at all, just an unusable one - and it repeats for as
		// long as the server keeps omitting the field. Same machinery, same evidence.
		code, ok = resolveNoServerIDCode, true
	case errors.Is(err, errRemoveDirUnverified) && !unprovedByTransport(err):
		// A directory delete whose subtree enumeration could not prove the subtree agreed. The
		// listing that defeated it may itself carry an unmapped code - the `case ok` arm above
		// already claimed that shape, so a real number always wins - but the shapes that do not
		// (an errno-only reply, a bare ErrPathError, an unidentifiable child) carry none at all
		// and would otherwise re-walk the whole server subtree every drain, unpaced.
		//
		// A walk defeated by the CONNECTION is excluded (unprovedByTransport). This synthetic code
		// is not a serverTransientRejection, so a streak of it reaches the terminal exit - and a
		// broken socket, a stalled turn or an expired per-turn deadline says nothing about the
		// server refusing this request, which is the rule the !ok branch below already applies to
		// every unwrapped transport failure. Without the exclusion a server that keeps every lease
		// but times out its `list` turns for unmappedServerErrMinAge dead-lettered the delete: the
		// user's deletion never reaches the server, and markOutboundConflict leaves a StateConflict
		// record at a path with no local file, which localReconcile never drives again.
		code, ok = removeDirUnverifiedCode, true
	case errors.Is(err, errUploadTargetAbsenceUnproved):
		// A push whose target answered the path-error sentinel too few times to prove the node
		// gone. Same reason as the arm above - the sentinel is a mapped error, not an unmapped
		// errno, so nothing else here would count it and the op would re-probe every drain for
		// the whole corroboration window. Paced but never dead-lettered
		// (serverTransientRejection): the streak is this client's own evidence, not a refusal.
		code, ok = uploadAbsenceUnprovedCode, true
	case errors.Is(err, errUploadNoFileID):
		// A placing turn that named no node. Not a server refusal - the work landed - so the arm
		// exists purely to pace the retry, and serverTransientRejection keeps it out of the
		// dead-letter exit.
		code, ok = uploadNoFileIDCode, true
	case errors.Is(err, errRenameTargetUnproved):
		// A move whose destination probe came back inconclusive. Paced, never dead-lettered
		// (serverTransientRejection): the guard is this client's own evidence gathering.
		code, ok = renameTargetUnprovedCode, true
	case errors.Is(err, errRenameUnsettled):
		// A move the server refused with a bare path error that no arm could explain. The
		// sentinel is mapped, so nothing else here would count it and the op re-issued the turn
		// every drain forever while holding the peer's op for the same node behind it.
		code, ok = renameUnsettledCode, true
	case errors.Is(err, errRemoveAbsenceUnproved):
		// A delete whose target the post-turn classification could not place yet. Same reason as
		// the two arms above - the answer is the mapped path-error sentinel, not an unmapped errno
		// - and the same treatment: paced, never dead-lettered (serverTransientRejection).
		code, ok = removeAbsenceUnprovedCode, true
	}
	now := e.now()
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	if !ok {
		delete(e.opServerErrs, seq)
		// The failure class changed, so the pacing evidence is gone with the streak: a broken
		// connection or a store hiccup says nothing about the server refusing this request, and
		// holding the op back would delay the retry that recovers from it (opbackoff.go).
		delete(e.opRetryAfter, seq)
		return 0, 0, 0
	}
	st, tracked := e.opServerErrs[seq]
	if !tracked || st.code != code {
		st = serverErrStreak{code: code, count: 1, since: now}
	} else {
		st.count++
	}
	e.opServerErrs[seq] = st
	return st.code, st.count, now.Sub(st.since)
}

// The streak is dropped when the op leaves the queue by clearOpFailures (executor.go), which
// every "the op left the queue" site already goes through, so the map cannot outgrow the live
// queue and a re-used seq can never inherit a stale streak.

// serverErrStreakReason renders a streak code for the operator: a real result code names
// itself (the trace wording is unchanged from v16 so existing log lines read the same),
// while the synthetic resume code spells out what actually happened - an operator handed
// "error code -1" would have nothing to act on.
func serverErrStreakReason(code int64) string {
	switch code {
	case resumeNoCompletionCode:
		return "server never reported the resumed upload as complete"
	case resolveNoServerIDCode:
		return "server described the conflicting node without a file id"
	case removeDirUnverifiedCode:
		return "server subtree of the directory could not be enumerated well enough to prove the delete safe"
	case uploadAbsenceUnprovedCode:
		return "the push target answered the path-error sentinel too few times to prove the node gone"
	case removeAbsenceUnprovedCode:
		return "the delete target's fate could not be proved from the post-turn listing"
	case uploadNoFileIDCode:
		return "the server accepted the create but named no node, and the recovery probe could not supply one"
	case actions.CodeNameCaseCollision:
		// A REAL result code, unlike the two synthetic ones above, and it still reports its
		// NUMBER to ServerErrorCode (actions.ServerError.Unwrap) so this escalation sees it.
		// What it does not carry is a cause an operator can act on: the code
		// alone reads as an anonymous refusal, when the fix is one rename away. The
		// generic prefix is kept verbatim so the trace still names the number and
		// existing log-line matches still hold; only the cause is appended.
		return fmt.Sprintf("server rejected the push with error code %d: "+
			"a sibling in that directory differs from the name only by case", code)
	case actions.CodeNameTypeCollision:
		// The other measured name refusal (finding H4): the path is held server-side by a
		// node of the other type - the stand's shape was a peer's FILE `thing` against this
		// client's `mkdir thing`. Same wording discipline as 24580: number first, cause
		// appended, because the remedy (rename one of the two) is the operator's to make.
		return fmt.Sprintf("server rejected the push with error code %d: "+
			"the name is held by a server node of a different type", code)
	}
	return fmt.Sprintf("server rejected the push with error code %d", code)
}

// paceInboundServerFailure is the INBOUND half of the repeat-rejection pacing: it folds a failed
// inbound apply into the op's identical-code streak and arms the same geometric pause, and does
// nothing else.
//
// Escalation is deliberately closed to inbound work (see the file header) - dead-lettering it
// would drop a server change this client has already consumed the event for. The PACING is a
// different question, and leaving it out left the measured shape of finding H5 unpaced: 32772 was
// measured on a `download` of a node another client had just overwritten (docs/INTEGRATION.md),
// i.e. on an INBOUND op, and such a download re-issued its whole turn on every drain - the same
// ~80-requests-a-second spin finding H4 measured on the push side. Pacing it costs nothing but a
// bounded delay (retryBackoffCap, of the order of one scan interval) and never changes an outcome:
// the op stays queued exactly as it did, and any change of failure class clears the streak and the
// pause with it.
//
// The error is returned unchanged so callers can `return e.paceInboundServerFailure(seq, apply(…))`.
func (e *Engine) paceInboundServerFailure(seq uint64, err error) error {
	if err == nil || errors.Is(err, errDeadLettered) {
		return err
	}
	_, count, _ := e.noteServerErr(seq, err)
	e.deferOpRetry(seq, count)
	return err
}

// markEscalatedConflict is the escalation's baseline-marking step. It is a package var, like
// dropDoomedRename (executor.go), solely so a test can inject a store-level failure (ENOSPC on a
// full disk) and assert the failure routes through the disk-full latch instead of retrying
// silently.
var markEscalatedConflict = (*Engine).markOutboundConflict

// escalateDeterministicServerFailure is applyOp's post-classifier for the push-class paths
// (the outbound applies and the keep-both resolver): it passes every result through
// unchanged until an op has failed
// unmappedServerErrDrains consecutive drains - spanning at least unmappedServerErrMinAge -
// with one and the same unmapped server result
// code, and then dead-letters it - marking the baseline StateConflict first (so the
// divergence stays visible in `Unresolved` and a child create resolves terminally through
// errParentConflicted rather than wedging on an id-less pending parent) and recording a
// durable trace carrying the code.
//
// It is data-safe by construction: nothing is deleted locally or server-side; the op is
// only stopped from retrying. Ordering matters - the conflict marking runs BEFORE the
// atomic trace+ack, so a marking failure leaves the op queued with no trace and a retry
// cannot double-count Dropped.
func (e *Engine) escalateDeterministicServerFailure(qo store.QueuedOp, err error) error {
	if err == nil || errors.Is(err, errDeadLettered) {
		// The op left the queue (applied, funnelled or dropped); drainBatch clears its
		// counters. Nothing to escalate, and nothing to reset that is not already reset.
		return err
	}
	code, count, age := e.noteServerErr(qo.Seq, err)
	if serverTransientRejection(code) {
		// A code MEASURED to clear on its own (32772, finding H5). No streak of it is proof
		// of anything, so it never escalates however long it runs - but it is still paced,
		// because "the server is settling that node" is precisely the condition where
		// re-issuing the whole push every drain helps least.
		e.deferOpRetry(qo.Seq, count)
		return err
	}
	drains, minAge := escalationThresholds(code)
	if count < drains || age < minAge {
		// Not proven terminal yet - but the failure HAS now repeated identically, so
		// re-sending the same turn on the very next drain is pointless. Arm the geometric
		// pause (opbackoff.go): a streak of one is left un-paused, so a genuine transient
		// still retries immediately, while a deterministic refusal stops costing ~80
		// requests a second (finding H4). The count keeps advancing across the pauses, which
		// is what makes the wall-clock floor above reachable without a busy loop.
		e.deferOpRetry(qo.Seq, count)
		return err
	}
	e.log.Warn("engine: push failed the same deterministic way across drains; marking conflict and dropping",
		"type", qo.Type, "direction", qo.Direction, "view", qo.ViewID, "path", qo.Path,
		"seq", qo.Seq, "server_errno", code, "reason", serverErrStreakReason(code),
		"failures", count, "streak_age", age)
	if cerr := markEscalatedConflict(e, qo.Op); cerr != nil {
		// A store-level failure here can be ENOSPC on a full disk: route it through the
		// disk-full latch so `status` shows `Disk: full` instead of retrying silently, as
		// every sibling dead-letter arm in executor.go does. The op stays
		// queued with no trace, so a retry cannot double-count it.
		//
		// Arm the pause on the way out. The terminal exit is the ONE path that leaves this
		// function without one, and it is the path taken by an op whose evidence is already
		// complete: every following drain re-issues the whole doomed turn (for an upload, the
		// whole body) at full drain cadence for as long as the store stays unwritable - the
		// ~80 requests a second H4 measured, re-opened by the very failure that stops the drop.
		e.deferOpRetry(qo.Seq, count)
		return e.noteApplyResult(qo.Op, cerr)
	}
	if dlErr := e.deadLetterAndAck(qo.Op, qo.Seq,
		fmt.Sprintf("%s on %d consecutive drains", serverErrStreakReason(code), count)); dlErr != nil {
		// Same latch on the atomic trace+ack: the returned error is unchanged.
		// Paced for the same reason as the marking failure above - the op is still queued and
		// its next attempt is just as pointless as the one that got here.
		e.deferOpRetry(qo.Seq, count)
		return e.noteApplyResult(qo.Op, dlErr)
	}
	return errDeadLettered
}
