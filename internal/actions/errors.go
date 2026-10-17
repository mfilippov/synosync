package actions

// This file maps the server's numeric action result codes (carried in the reply's
// "errno" field) to typed errors, mirroring the errno/typed-sentinel pattern in
// internal/auth. Callers dispatch on the sentinels with errors.Is - e.g.
// errors.Is(err, ErrConflictVersion) to keep-both/resync, or
// errors.Is(err, auth.ErrSessionVanished) to fall back to re-pairing.

import (
	"errors"
	"fmt"

	"github.com/mfilippov/synosync/internal/auth"
)

// Server action result codes carried in the reply's "errno" field. Zero is
// success; the codes below are the ones the action layer reacts to specifically.
const (
	// CodeOK is the success result.
	CodeOK = 0
	// CodeSessionVanished (8193) means the presented session token is no longer
	// valid server-side; the caller must re-pair. Mapped to auth.ErrSessionVanished
	// so the existing reuse/pairing fallback in cmd/synosync keeps working.
	CodeSessionVanished = 8193
	// CodeClientVersion (20481) means the server rejected THIS binary's client_version.
	// It is the same deterministic, session-level verdict the connect axis already treats
	// as terminal (auth.ErrClientVersion → transport.ErrPoolClientRejected → "update
	// synosync"), so it is mapped to the SHARED auth sentinel rather than left as an
	// unmapped *ServerError: consumed per-op it looked like an unknown deterministic
	// rejection and dead-lettered one healthy push after another, each with a StateConflict
	// baseline localReconcile never re-derives, while the one actionable hint - upgrade the
	// client - was reachable only through a reconnect.
	CodeClientVersion = auth.CodeClientVersion
	// CodeConflictVersion (24579) means the request's base version is stale - the
	// node changed server-side since the caller last saw it. Non-fatal: the engine
	// can resync or keep-both.
	CodeConflictVersion = 24579
	// CodeNameCaseCollision (24580) is what a CREATE draws when the name it asks for
	// differs from an existing sibling only by CASE. Measured 02.08.2026 (DSM
	// 7.2.2-72806 / Drive Server 4.0.3-27892, TestIntegration_CaseOnlyRename): DSM's
	// path LOOKUP is case-SENSITIVE - query_node of the old spelling after a case-only
	// rename answers a path error - while its uniqueness is case-INsensitive, so
	// `notes.txt` and `Notes.txt` cannot coexist in one view. A case-sensitive local
	// host holding both therefore has one file it can never push.
	//
	// It is reported as a *ServerError that ALSO matches ErrNameCaseCollision (see
	// ServerError.Unwrap): a code mapped the ordinary way - errorForCode returning a bare
	// wrapped sentinel - leaves ServerErrorCode false, and that is exactly what CLEARS the
	// engine's deterministic-rejection streak (engine/servererr.go), so the push would
	// retry every drain forever, wedging every path-overlapping successor behind it,
	// instead of dead-lettering with a durable trace. Carrying both keeps the numeric
	// evidence the streak counts AND gives callers a sentinel to match; the engine treats
	// it as terminal for a push after a much shorter confirmation than an unknown code
	// gets (engine.serverStructuralRejection), because the refusal is about the NAME, which no
	// amount of waiting changes.
	CodeNameCaseCollision = 24580
	// CodeNameTypeCollision (24581) is what a CREATE draws when the name it asks for is
	// already held by a node of a DIFFERENT type - measured on the two-client stand
	// (14.08.2026, TestIntegration_TwoClientsFileDirClash): one client created a FILE
	// `thing` while the other created a DIRECTORY `thing/` offline, and the returning
	// client's `mkdir` was refused with 24581 on every attempt for the whole
	// unmappedServerErrMinAge window (~80 attempts/second, finding H4).
	//
	// Like 24580 it is deterministic - the occupant does not move because the client asked
	// again - so it is reported as a *ServerError that also matches ErrNameTypeCollision,
	// for exactly the reasons on CodeNameCaseCollision above.
	CodeNameTypeCollision = 24581
	// CodeParentNotADirectory (24584, 0x6008) is what a CREATE draws when the PARENT node it
	// names is a regular file rather than a directory. Measured on the stand
	// (2026-08-25, DSM 7.2.2-72806 / Drive 4.0.3-27892): uploading `child.txt` under a file's
	// node id answered it three times out of three, and `mkdir` under the same parent answered
	// it identically -- one code for both verbs.
	//
	// It is the sibling of 24581 and reaches the client from the other side of the same clash.
	// When one peer makes a path a FILE and another makes it a DIRECTORY, the loser's mkdir for
	// the path itself draws 24581, while every push BENEATH that path draws this: the parent it
	// asks for is the peer's file. Like 24580 and 24581 the premise does not change because the
	// client waits -- the occupant does not move because the request is repeated -- so it takes
	// the same shortened confirmation rather than the unknown-code window (engine/servererr.go).
	//
	// Measured asymmetry worth knowing: an upload is a two-frame turn, and this refusal of the
	// first frame leaves the body unsent, so the connection is desynced and the NEXT action on it
	// reports ErrConnBroken. A refused mkdir does not. An op wedged on this code therefore cost a
	// pool session on every retry, not just a round trip.
	CodeParentNotADirectory = 24584
	// CodeNodeBusy (32772) is what a live DSM answers for a read of a node whose content
	// the server is still settling - measured on the two-client stand (14.08.2026,
	// TestIntegration_TwoClientsRapidChurn, finding H5): a `download` issued right after
	// another client overwrote the node answered 32772 once, and the identical request a
	// second later served the file. It is TRANSIENT, the exact opposite of the two name
	// refusals above, and it is the only code measured to be so.
	//
	// Like them it is reported as a *ServerError that ALSO matches its sentinel
	// (ErrNodeBusy) rather than being mapped by errorForCode, but for the mirror-image
	// reason: the engine must keep SEEING the number so the repeat-rejection pacing still
	// arms (engine/opbackoff.go, ~80 requests a second otherwise), while the escalation
	// that would eventually dead-letter a push refuses to count it at all
	// (engine.serverTransientRejection). A code known to clear on its own is never
	// evidence of permanence, and before the mapping an outbound op unlucky enough to
	// draw it repeatedly accumulated it into the deterministic-rejection streak.
	//
	// The malformed-request shape shares the number - a `download` whose params omit the
	// (root_node_id, root_node_path) scoping draws 32772 too (download.go) - and is kept
	// out of reach the same way 40961's is: by pinning the request shape in the wire-shape
	// tests, not by reading the code.
	CodeNodeBusy = 32772
	// CodePathError (28672) reports a path error or operation timeout. Measured on a
	// live DSM it is the INPUT-validation / soft-failure code: a malformed path (an
	// empty one), a below-floor long_poll timeout, and a rename whose SOURCE does not
	// exist all answer it. A plain "node not found" does NOT - see CodeNodeNotFound.
	CodePathError = 28672
	// CodeNodeNotFound (32769) is the server's "no such node" verdict for the
	// path/id-addressed read and place verbs: query_node, list, download and an
	// upload under a missing parent id. Measured on DSM 7.2 / Drive 4.0.3.
	//
	// It maps to the SAME ErrPathError sentinel as 28672 because it is precisely the
	// evidence the engine's absence provers consume; mapping only 28672 left every
	// one of them (resolveServerNodeAbsence, nodeGoneByID, confirmAbsentByParentList,
	// ancestorConfirmsAbsent, the view-root probe) unable to recognise a real absence
	// against a live server.
	CodeNodeNotFound = 32769
	// CodeMutateNotApplied (40961) is what the MUTATING verbs (remove, rename) answer
	// when they will not apply. Measured on a live DSM it is AMBIGUOUS by design: an
	// absent target draws it, and so does a malformed request against a target that
	// demonstrably exists (a remove whose path lacks the leading slash, or one whose
	// sync_id is 0, is refused with 40961 and deletes nothing).
	//
	// It maps to ErrPathError because the absent case is the common one and the engine
	// needs it: an already-absent node is exactly the postcondition an outbound remove
	// treats as satisfied, and without the mapping every such remove retries until it
	// dead-letters, so the deletion never converges. The malformed case is kept out of
	// reach by pinning the request shape in TestRemoveWireShape / TestRenameWireShape -
	// if those ever regress, this code starts meaning "your request was wrong" and the
	// engine would read it as "already gone", acking a deletion that never happened.
	CodeMutateNotApplied = 40961
	// CodeStaleCursor (40962) means the max_id/sync_id the request carried is behind
	// the server's current log position. It is the optimistic-concurrency rejection
	// of the mutating verbs: ANY change committed between reading the cursor and
	// sending the request invalidates it, so the caller must re-read and retry rather
	// than treat it as a failure (Rename does exactly that).
	CodeStaleCursor = 40962
	// CodeHashMismatch (36870) means a transferred body's content hash did not match
	// the value the peer advertised.
	CodeHashMismatch = 36870
	// CodeResumeToken (45057, 0xB001) means the resume token presented for a
	// large-file transfer is missing, invalid, expired or unknown server-side. The
	// caller must obtain a fresh token (ResumeGetToken) and restart the transfer.
	//
	// Measured on a live DSM for BOTH resume verbs - a bogus token draws it from
	// resume_upload and resume_download alike, and so does omitting the token
	// entirely. The value this constant used to carry (40976) was a guess that
	// appears in no capture and in no analyzer doc; the server never emits it, so the
	// re-token recovery armed on it could never fire.
	CodeResumeToken = 45057
)

// Typed sentinel errors for the result codes above. Errors returned by the action
// methods wrap the matching sentinel, so callers can match them with errors.Is.
var (
	// ErrConflictVersion reports a stale base version (24579); non-fatal.
	ErrConflictVersion = errors.New("actions: stale base version (conflict)")
	// ErrPathError reports that the addressed node is absent, unreachable, or that
	// the request could not be applied to the path - the union of 28672 (malformed
	// path / soft failure), 32769 (node not found on the read and place verbs) and
	// 40961 (a mutating verb that would not apply). It is the single signal the
	// engine's absence provers read, and it is deliberately NOT proof on its own:
	// every verdict built on it is corroborated by repetition (absencestreak.go).
	ErrPathError = errors.New("actions: path error or operation timed out")
	// ErrHashMismatch reports a content-hash mismatch (36870).
	ErrHashMismatch = errors.New("actions: content hash mismatch")
	// ErrResumeToken reports a missing, invalid, expired or unknown resume token
	// (45057); the caller must obtain a fresh token and restart the transfer.
	ErrResumeToken = errors.New("actions: invalid or expired resume token")
	// ErrUnexpectedReply reports a malformed reply: not a Map, or a wrong-typed
	// errno that cannot be interpreted as a result code.
	ErrUnexpectedReply = errors.New("actions: unexpected or malformed reply")
	// ErrTransferStalled reports that a streamed download reply (download /
	// resume_download) delivered no bytes at all for downloadIdleTimeout while the
	// socket stayed open - a wedged peer, not a slow one. It is transient: the
	// connection is poisoned (the turn aborted mid-decode), so the pool re-dials the
	// worker and the op stays queued for the next drain. See idle.go for why the
	// bound is an idle budget rather than a fixed per-turn deadline.
	ErrTransferStalled = errors.New("actions: transfer stalled (no read progress)")
	// ErrNameCaseCollision reports that a create was refused because a sibling in the
	// target directory differs from the requested name only by CASE (24580). DSM's
	// uniqueness is case-INsensitive, so the two names cannot coexist in one view and no
	// retry ever makes the push land: it is a verdict about the NAME.
	ErrNameCaseCollision = errors.New("actions: name collides with a sibling differing only by case")
	// ErrNameTypeCollision reports that a create was refused because the requested name is
	// already held by a node of a different type (24581) - the canonical shape is `mkdir`
	// onto a path a FILE occupies. Also a verdict about the name, not a moment.
	ErrNameTypeCollision = errors.New("actions: name is held by a node of a different type")

	// ErrParentNotADirectory reports CodeParentNotADirectory: the parent node a create named is
	// a regular file, so nothing can be created under it. Like the two name refusals it is
	// derived from the code in ServerError.Unwrap rather than replacing it, so the engine's
	// identical-code streak still sees the number.
	ErrParentNotADirectory = errors.New("actions: the parent node is a file, not a directory")
	// ErrNodeBusy reports that the server refused a read of a node it is still settling
	// (32772) - measured for a download issued moments after another client overwrote the
	// node, with the identical request succeeding a second later. It is transient: the
	// caller retries, and the engine never counts it as evidence of a deterministic
	// rejection.
	ErrNodeBusy = errors.New("actions: server is still settling that node; retry")
	// ErrConnBroken reports that a previous turn was interrupted mid-I/O (a read,
	// write or decode error, including a context cancellation), leaving the
	// connection byte-misaligned. The Client refuses further turns rather than risk
	// reading a stale reply as the next action's; the caller must establish a fresh
	// session.
	ErrConnBroken = errors.New("actions: connection broken by an interrupted turn")
)

// ServerError carries a server result code the action layer does not map to one of the
// typed sentinels above. It exists so a caller can recover the numeric code rather than
// only the message: the engine escalates an op that keeps failing with the SAME unmapped
// code across drains as a deterministic (terminal) server rejection, and that proof needs
// the code itself, not a formatted string. The mapped codes above stay
// sentinel-matched - a caller that wants "any known rejection" keeps using errors.Is.
type ServerError struct {
	// Code is the server's numeric "errno" field from the rejected reply.
	Code int64
}

// Error reports the numeric server code. The wording is unchanged from the pre-typed
// generic error so existing log lines and message assertions read the same.
func (e *ServerError) Error() string {
	return fmt.Sprintf("actions: server returned error code %d", e.Code)
}

// Unwrap exposes the typed sentinel a few codes carry IN ADDITION to their number, so
// errors.Is(err, ErrNameCaseCollision) matches while ServerErrorCode still answers
// (24580, true). Mapping those codes the ordinary way - errorForCode returning a bare
// wrapped sentinel - would have made ServerErrorCode false and silently cleared the
// engine's deterministic-rejection streak, turning a terminal name refusal back into a
// forever-retry (engine/servererr.go). The transient 32772 keeps its number for the
// opposite half of the same machinery: the repeat-rejection PACING is armed off the streak
// too, and a code that clears on its own should still be retried politely rather than ~80
// times a second. Deriving the sentinel from the code rather than
// storing it in a field means every *ServerError with that code matches, including the
// ones tests and other packages construct directly.
//
// Every other code unwraps to nil, which errors.Is handles as "no further cause".
func (e *ServerError) Unwrap() error {
	switch e.Code {
	case CodeNameCaseCollision:
		return ErrNameCaseCollision
	case CodeNameTypeCollision:
		return ErrNameTypeCollision
	case CodeNodeBusy:
		return ErrNodeBusy
	case CodeParentNotADirectory:
		return ErrParentNotADirectory
	}
	return nil
}

// ServerErrorCode reports the unmapped server result code carried by err, if any. It is
// false for nil, for a transport/decode failure and for every code errorForCode maps to a
// bare wrapped sentinel (those have dedicated handling and must not be lumped into the
// generic "unknown deterministic rejection" class). The two name-refusal codes and the
// transient 32772 are the deliberate exception: they answer true here AND match their
// sentinel, because the engine's per-code handling of them - terminal for the name
// refusals, never-terminal-but-paced for 32772 - is built ON the identical-code streak
// this function feeds (see ServerError.Unwrap).
func ServerErrorCode(err error) (int64, bool) {
	var se *ServerError
	if errors.As(err, &se) {
		return se.Code, true
	}
	return 0, false
}

// errorForCode maps a non-zero server result code to a typed error, wrapping the
// matching sentinel so callers can match it with errors.Is. Unknown codes get a
// *ServerError that still reports the numeric value and lets a caller recover it.
func errorForCode(code int64) error {
	switch code {
	case CodeConflictVersion:
		return fmt.Errorf("%w (code %d)", ErrConflictVersion, code)
	case CodePathError, CodeNodeNotFound, CodeMutateNotApplied:
		return fmt.Errorf("%w (code %d)", ErrPathError, code)
	case CodeHashMismatch:
		return fmt.Errorf("%w (code %d)", ErrHashMismatch, code)
	case CodeResumeToken:
		return fmt.Errorf("%w (code %d)", ErrResumeToken, code)
	case CodeSessionVanished:
		return fmt.Errorf("%w (code %d)", auth.ErrSessionVanished, code)
	case CodeClientVersion:
		return fmt.Errorf("%w (code %d)", auth.ErrClientVersion, code)
	default:
		// Unknown codes - plus the two name refusals and the transient 32772, which are
		// typed through ServerError.Unwrap rather than here so they keep reporting their
		// number to ServerErrorCode (see that method).
		return &ServerError{Code: code}
	}
}
