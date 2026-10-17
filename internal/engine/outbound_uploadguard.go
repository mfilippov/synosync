package engine

// Outbound UPLOAD divergence precondition (two-client finding H1, measured 14.08.2026;
// rationale in docs/INVARIANTS.md, "upload guard").
//
// The client used to delegate ALL outbound divergence protection to the server. uploadParams
// sends conflict_policy "compare_mtime" with max_id/sync_id zeroed - the request carries no base
// version at all - and the engine simply waited for the 24579 (ErrConflictVersion) rejection to
// funnel a push into keep-both; reconcile even DEFERS an inbound download whose path has a queued
// upload on that promise, advancing the cursor as it does so.
//
// What a live DSM actually does with that policy was measured on the two-client stand
// (TestIntegration_UploadConflictSemantics): it ACCEPTS the write whenever the request's mtime is
// newer than or equal to the node's, and rejects only an OLDER one. "compare_mtime" is not a
// base-version check, it is "your mtime must not be older" - and an offline edit is almost always
// newer. So 24579 never arrives in the typical case: the returning client overwrote the peer's
// version on the server, then downloaded its own bytes back over the local copy, with no
// _Conflict copy, no dead-letter and a clean `status`. The protocol offers no optimistic lock to
// fall back on either - TestIntegration_UploadBaseVersionProbe pushed a deliberately STALE
// max_id/sync_id and the server took the write anyway.
//
// The push therefore has to prove for itself that the node it is about to overwrite is still the
// version this client last agreed on. guardUploadTarget asks the server with one path-addressed
// query_node immediately before the destructive turn:
//   - a MODIFY compares the server's current content hash against the last agreed
//     Baseline.ServerHash;
//   - a CREATE has no base to compare, so its question is OCCUPANCY: anything file-shaped already
//     holding the path is a node another client put there, and the create would overwrite it.
//
// Divergence funnels into exactly the keep-both resolver 24579 funnels into
// (funnelUploadConflict): the server version wins the canonical path and the local bytes survive
// as a _Conflict copy pushed back as a separate file. The window between probe and write stays
// open - the protocol cannot close it - but it shrinks from "always" to one round trip.
//
// Everything the probe cannot DECIDE leaves the op queued rather than uploading (fail toward "do
// not push"): a transport failure, a server-side timeout. The one thing it cannot leave queued is a
// reply that answers NOTHING, since that is indistinguishable from the free path every create is
// entitled to: such a reply proves no occupancy, so the push proceeds - for a modify with the
// degradation logged, because a guard that has gone inert must at least be observable. The
// path-error sentinel is read as "nothing is there" only where that reading cannot be destructive
// - see uploadTargetAbsent - after which the ordinary upload proceeds and the existing post-turn
// machinery (the lost-reply probe, the parent-gone walk, the vanished-source resolution) owns the
// outcome exactly as before.

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// errUploadTargetDiverged marks an outbound OpUpload whose pre-turn probe proved the server does
// not hold the version this push was written against: a MODIFY whose node now carries content
// this client never agreed on (or whose path a different node has taken over), or a CREATE whose
// path is already occupied by a node another client put there. Pushing anyway would destroy that
// content on the server AND, once the overwrite echoes back, in every replica - finding H1's
// silent loss. applyOutbound routes it into funnelUploadConflict, the same keep-both resolution
// the server's own 24579 rejection takes.
var errUploadTargetDiverged = errors.New("engine: outbound upload target diverged server-side")

// errUploadTargetAbsenceUnproved marks a MODIFY whose pre-turn probe answered with the path-error
// sentinel too few times to prove the node is really gone. It is a TRANSIENT: the op stays queued
// and re-probes on a later drain pass, and it is deliberately NOT errUploadTargetDiverged, which
// applyOutbound would funnel into keep-both - nothing has been proved about the target either way,
// and manufacturing a _Conflict copy out of an unanswered probe is its own kind of wrong.
var errUploadTargetAbsenceUnproved = errors.New("engine: outbound upload target absence unproved")

// errUploadNoFileID reports a placing turn (upload or mkdir) that SUCCEEDED but whose reply named
// no node, with the recovery probe unable to supply one either.
//
// It exists to make that answer PACEABLE. The apply cannot advance the baseline - an empty id
// drops the node from the byid index and every later modify/move/remove of it misresolves - so
// the op stays queued, and a queued op with no recognised failure class retries at full drain
// cadence. For the upload arm each retry re-streams the whole file body, because guardUploadTarget
// treats a server hash equal to ours as its own lost reply and proceeds. Measured 27.08.2026
// (TestIntegration_FileIDPresence): this DSM names the node on every surface, so the arm is
// unreachable against it - but it is one reply shape away from being an ~80-requests-per-second
// spin, which is the H4 measurement this pacing exists to prevent.
var errUploadNoFileID = errors.New("engine: placing turn returned no file id")

// uploadIsCreate reports whether an outbound OpUpload is a CREATE - a push of a node this client
// has never agreed a version of - as opposed to a MODIFY of a node it has.
//
// The question decides which of two incompatible guards the push gets: a create's is OCCUPANCY
// (anything file-shaped holding the path is a peer's node the push would overwrite), a modify's is
// CONTENT (the server must still hold the version this edit was written against). Applying the
// create's question to a modify funnels EVERY ordinary edit into keep-both, since the node the
// modify is about to update is of course occupying its own path.
//
// The op's file id alone cannot answer it. detectCreatesAndModifies plans a modify with the synced
// record's FileID verbatim (localreconcile.go), and that field is routinely EMPTY: a DSM whose
// replies omit file_id - the shape modelled throughout this package, and the one that reshaped the
// sibling delete precondition (removeTargetFacts' id-less arm) - leaves every synced record id-less,
// so every modify of every file in the view would read as a create. The local edit would be set
// aside as a _Conflict copy and the server's older bytes downloaded back over the canonical path,
// on every save, which is not a conservative reading of H1 but a new loss of its own.
//
// The answer is therefore POSITIVE rather than an absence of evidence: BaseWasSynced, which only
// detectCreatesAndModifies' modify arm sets, because only it holds a StateSynced record. An
// absence-of-facts reading (no id AND no captured base) is not enough on its own, because a
// StateSynced record can legitimately carry neither: applyRename records bare facts
// (store.Baseline{FileID: op.FileID}) for a node whose id-less move left nothing richer to carry,
// and revertRelocatedOutbound clears LocalHash while restoring StateSynced for a re-emitted push.
// Against an id-less DSM both leave a synced record with no id and no local hash, whose next
// ordinary edit reads as a create, takes the occupancy guard, and is funnelled into a spurious
// _Conflict copy with the server's older bytes downloaded back over the canonical path - the very
// churn this discrimination exists to prevent, reached through the one shape the old fact-based
// reading could not see.
//
// BaseLocalHash and FileID remain the fallback for an op enqueued before the flag was carried
// (it decodes to false), preserving the historical reading rather than reclassifying a queued push.
// Cold start's and queuerepair's pushes set neither: a create has no agreement at all, and
// queuerepair's stranded record is StatePending, its LocalHash already the un-pushed edit - those
// keep the occupancy reading, which is the conservative one to hold when no agreement can be named.
func uploadIsCreate(op store.Op) bool {
	return !op.BaseWasSynced && op.FileID == "" && op.BaseLocalHash == ""
}

// uploadDivergenceBase reads the last-agreed server hash guardUploadTarget compares the server's
// current content against. markLocalPending preserves the server facts when planUpload flips the
// baseline StatePending, so the record at op.Path still carries the ServerHash of the version this
// client last agreed with.
//
// The fallback when that field is empty is NOT the record's LocalHash, the way
// removeTargetFacts's is. For a remove the two describe the same bytes (the file is gone;
// markLocalPending preserved the facts of the last agreement), but planUpload has just STAMPED
// LocalHash with the new, un-pushed edit - the very content whose push is being guarded. Comparing
// the server against that would read "diverged" for every ordinary modify and funnel every push
// into keep-both.
//
// It is op.BaseLocalHash instead: planUpload's reconcile-time capture of the SYNCED record's
// LocalHash, read before that stamp overwrote it, so it names the content this push is written
// against. Carrying it matters because the ServerHash is not reliably there - applyUploadOut
// assigns the upload reply's hash verbatim, so a reply shape that omits it BLANKS the field on
// every successful push and leaves this guard with nothing to compare for the whole rest of the
// file's life. The stand measured the small-file `upload` reply as hash-bearing but left the
// large-file `resume_upload` reply's facts explicitly open (docs/INTEGRATION.md), and the
// degradation this guard would otherwise take is to node IDENTITY - which a peer's overwrite of
// the same node always satisfies, i.e. exactly the silent H1 loss, not a weaker form of it. The
// sibling delete precondition compensates for the same blanking (removeTargetFacts); this is
// its upload-shaped equivalent, differing only in where the base has to come from.
//
// A create has no base at all and never reads the record. An absent record (the crash
// window between localReconcile's Enqueue and its baseline flip) and a corrupt one fall back to
// the op's own capture, which is independent of the record precisely so it survives both; only if
// that is empty too does the decision drop to node identity - the same node is the unchanged one,
// so the push proceeds, mirroring serverDivergedForVanishedUpload's missing-hash rule. Only a
// backend failure propagates: a destructive overwrite is never decided from an unread store.
func (e *Engine) uploadDivergenceBase(op store.Op) (string, error) {
	if uploadIsCreate(op) {
		return "", nil // a create has no agreed base; its guard is occupancy, not content
	}
	b, err := e.store.GetBaseline(op.ViewID, op.Path)
	switch {
	case isNotFound(err):
		return op.BaseLocalHash, nil
	case errors.Is(err, store.ErrCorruptRecord):
		e.warnCorruptBaseline("upload-divergence-base", op.ViewID, op.Path, err)
		return op.BaseLocalHash, nil
	case err != nil:
		return "", fmt.Errorf("engine: load upload divergence base %d/%q: %w", op.ViewID, op.Path, err)
	}
	if b.ServerHash != "" {
		return b.ServerHash, nil
	}
	return op.BaseLocalHash, nil
}

// uploadTargetAbsent decides what the path-error sentinel means for a push whose target probe
// answered with it, and it decides it differently for the two op classes - because the sentinel is
// not one answer.
//
// ErrPathError is the shared mapping for 32769 ("no such node") AND 28672 (input validation, which
// a live DSM also returns for a server-side timeout), so a single one is never proof of absence.
// That is why every other absence verdict in the engine is corroborated by repetition
// (absencestreak.go), and the sibling directory-delete precondition
// refuses exactly this input at the root of its walk (outbound_removedir.go): one timed-out
// probe was enough there to authorise a recursive wipe.
//
// For a MODIFY the reading is destructive in the same way. "The node is gone, so there is nothing
// to overwrite" makes the whole H1 precondition inert on one timed-out probe: the upload then runs
// with conflict_policy "compare_mtime", which accepts any write whose mtime is not older, and the
// peer's version is destroyed on the server and - once the overwrite echoes back - in every
// replica. So the absence is corroborated first, and an uncorroborated one leaves the op queued
// (fail toward NOT pushing) to be re-probed on a later drain pass. Liveness is unaffected for a
// node the server really has deleted: absentNodeConfirmations agreeing probes absenceStreakMinAge
// apart settle it, after which the push proceeds and re-creates the file exactly as before. The
// wait costs only an open and one QueryNode per attempt - nothing is hashed, streamed or mutated
// behind it - but the cost that matters is the RATE, not the per-attempt work: unpaced, the op
// re-issues that pair on every drain for the whole corroboration window, which under churn is the
// same ~80-attempts-a-second spin finding H4 measured. So the arm carries a synthetic streak code
// (uploadAbsenceUnprovedCode) exactly as the subtree walk it mirrors does - TRANSIENT, so the
// pacing applies and the dead-letter exit never does: a push must not be dropped for its own
// precondition still gathering the evidence that will arrive.
//
// For a CREATE the sentinel is the ORDINARY answer: the path is free, which is the whole reason
// the file is being created. Corroborating it would put absentNodeConfirmations drain passes and
// absenceStreakMinAge of wall clock in front of EVERY new file, which is not a conservative
// guard but a broken one. It keeps the single-probe reading, and the occupancy question it asks
// is unchanged.
func (e *Engine) uploadTargetAbsent(viewID int64, op store.Op) error {
	if uploadIsCreate(op) {
		return nil
	}
	if seen := e.noteAbsentProbe(viewID, op.Path); seen < absentNodeConfirmations {
		return fmt.Errorf(
			"%w: probe of %d/%q answered with the path-error sentinel %d time(s), fewer than the %d agreeing probes an absence needs (28672 also reports a server-side timeout)",
			errUploadTargetAbsenceUnproved, op.ViewID, op.Path, seen, absentNodeConfirmations)
	}
	return nil
}

// guardUploadTarget proves, against the SERVER, that the node an outbound OpUpload is about to
// write is still the version this client last agreed on (finding H1). It runs on the upload's own
// worker lease, immediately before the destructive turn, exactly as applyRemoveOut's delete
// precondition does.
//
// It returns:
//   - nil - the push may proceed;
//   - errUploadTargetDiverged - proven divergence; applyOutbound funnels it into keep-both;
//   - errServerTypeCollision - the path holds a DIRECTORY, which no file upload can ever land on;
//     the existing funnel dead-letters it observably through the resolver;
//   - any other error - undecided (including a MODIFY whose target answered the path-error
//     sentinel too few times to prove the node gone, uploadTargetAbsent), so the op stays queued
//     and nothing is overwritten.
//
// The evidence rules mirror the engine's other divergence gates:
//   - a hash comparison is trusted only when BOTH hashes are present (Node.Hash is tolerantly
//     decoded and a base can legitimately be empty, so a present server hash against an empty base
//     would read as divergence on every push);
//   - a reply that asserts NOTHING (nodeAnswered false - a tolerant `file: {}` decodes exactly
//     like an absent node) is never proof of occupancy; a bare {} is never proof.
//
// The one adoption it makes is content-keyed: when the server already holds the very bytes this op
// intends to push (its scan-time op.LocalHash), the "occupant" is this client's own upload whose
// reply was lost, not a peer's file, so the push proceeds and its ordinary lost-reply recovery
// takes over. A CREATE whose occupant reports no hash at all cannot be told apart from a peer's
// file that way, and there the funnel wins: a spurious _Conflict copy of identical content is
// churn, an overwritten peer file is loss, and DESIGN §6 settles that trade - the same call
// resolveConflict makes.
func (e *Engine) guardUploadTarget(ctx context.Context, c syncClient, op store.Op, baseServerHash string) error {
	viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	server, qerr := c.QueryNode(ctx, viewID, op.Path)
	if !errors.Is(qerr, actions.ErrPathError) {
		// Whatever this probe answered, it was not the sentinel: the run of path-errors - the only
		// thing that tells a real absence apart from a repeatedly timing-out server - is broken, so
		// the next sentinel starts a fresh streak. Same rule, and the same shared streak map, as
		// absenceCorroboratedByNodeProbe; this guard just applies it to the probe it already makes
		// for itself rather than paying for a second identical round trip.
		e.clearAbsentProbe(viewID, op.Path)
	}
	switch {
	case errors.Is(qerr, actions.ErrPathError):
		return e.uploadTargetAbsent(viewID, op)
	case qerr != nil:
		// Undecided. Never push on it - that is precisely the "assume it is still ours" default
		// this precondition exists to remove.
		return fmt.Errorf("engine: probe upload target %d/%q: %w", op.ViewID, op.Path, qerr)
	}
	if server.IsDir {
		// A file upload can never land on a directory node. Route it to the existing confirmed
		// file<->dir collision funnel rather than keep-both's file-shaped resolution.
		//
		// Tested BEFORE the nodeAnswered exit below, because is_dir is occupancy evidence in its
		// own right: it is a fact the server cannot echo back from a query_node request (which
		// carries a path and nothing else), and it is exactly the fact a DIRECTORY reply can
		// carry when none of nodeAnswered's four are present - a directory has no content hash,
		// and a sparse reply may omit size and mtime as well. Read in the other order, such a
		// reply left the guard silent and the push ran onto a directory, to be refused by the
		// server as 24581 and dead-lettered on the shortened name-rejection window instead of
		// resolved through the funnel this branch exists to reach.
		return fmt.Errorf("%w: upload %d/%q found a server directory", errServerTypeCollision, op.ViewID, op.Path)
	}
	if !nodeAnswered(server) && uploadIsCreate(op) {
		// A reply asserting no fact a server cannot echo back from the request says nothing about
		// occupancy, and for a CREATE - whose only question IS occupancy - reading it as "a node is
		// there" would funnel every push into keep-both. So the create proceeds.
		//
		// The exit is scoped to the create deliberately. A MODIFY reaching the same reply ends at
		// the identity-degradation arm below, which returns nil just as this would but LOGS the
		// inertness first; exiting here instead made the H1 guard silently vacuous against a DSM
		// whose query_node reply carries none of nodeAnswered's four facts - the one shape where
		// observing the degradation matters most.
		return nil
	}
	if server.Hash != "" && op.LocalHash != "" && server.Hash == op.LocalHash {
		// The server already holds the exact bytes this op set out to push: this is our own
		// earlier attempt whose reply was lost, not a peer's version. Proceed; the upload's own
		// idempotent recovery records it.
		return nil
	}
	if uploadIsCreate(op) {
		return fmt.Errorf("%w: create %d/%q found server node %q already occupying the path",
			errUploadTargetDiverged, op.ViewID, op.Path, server.FileID)
	}
	switch {
	case op.FileID != "" && server.FileID != "" && server.FileID != op.FileID:
		// Both ids present and different: a node this client never agreed on holds the path.
		//
		// The op's own id has to be present for that to mean anything. An ID-LESS MODIFY reaches
		// here (uploadIsCreate reads the base capture, not the id, precisely so it can), and its
		// empty id against an answering server's real one is the ABSENCE of identity evidence, not
		// contradicting identity - every record is id-less against a DSM that omits the field,
		// while its query_node reply may still carry one. Read as a mismatch it funnelled exactly
		// the modifies the class discrimination above exists to protect. The content comparison
		// below is the evidence such an op does have.
		return fmt.Errorf("%w: modify %d/%q found a different server node %q holding the path",
			errUploadTargetDiverged, op.ViewID, op.Path, server.FileID)
	case contentDiverged(server.Hash, baseServerHash):
		return fmt.Errorf("%w: modify %d/%q found content that is not the last agreed version",
			errUploadTargetDiverged, op.ViewID, op.Path)
	case server.Hash == "" || baseServerHash == "":
		// Identity matched but no content comparison was possible - a DSM that omits the hash
		// from its replies, or a base recorded from such a reply. The precondition degrades to
		// node identity here (the same node is the unchanged one), which is the same degradation
		// removeTargetFacts documents; log it so the inertness is observable rather than
		// silent.
		//
		// At WARN, not Debug: config.Defaults ships LogLevel "info", so a Debug line never
		// reaches the rotating log on a default install - and against a DSM that systematically
		// omits the hash the whole H1 precondition is vacuous on EVERY push, which is the silent
		// overwrite class this guard exists to close. The two sibling degradations of exactly
		// this evidence class (outbound_upload.go's recovery probe, conflict.go's resolve node)
		// already Warn.
		e.log.Warn("engine: upload divergence precondition degraded to node identity; no content hash to compare",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID,
			"server_hash", server.Hash, "base_hash", baseServerHash)
	}
	return nil
}
