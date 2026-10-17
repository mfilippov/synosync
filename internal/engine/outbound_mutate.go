package engine

// This file carries the outbound (local→server) mkdir/rename/remove apply paths, split out
// of outbound.go behaviour-preservingly (DESIGN §13): applyMkdirOut/applyRenameOut/
// applyRemoveOut drive a directory create, a move, and a delete against the server over a
// leased worker connection, each tolerating a lost reply idempotently. The two parent-listing
// absence classifiers they (and the keep-both resolver) lean on - confirmAbsentByParentList
// and resolveServerNodeAbsence - live here too. The dispatcher (applyOutbound), the upload
// path and the conflict/baseline glue stay in outbound.go.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// errRemoveAbsenceUnproved marks an outbound OpRemove whose post-turn classification could not
// yet settle the target's fate: the parent listing was inconclusive, still reports the node, or
// reports it alive under a new leaf name the store has not re-indexed. All three leave the op
// queued for a later drain, and all three used to do so UNPACED - the raw actions.ErrPathError
// they returned is a MAPPED sentinel, so ServerErrorCode answers false, noteServerErr clears the
// streak AND the pause, and the op re-issued `remove` plus the parent `list` plus a corroborating
// QueryNode on every drain for the whole wait.
//
// That wait is not short: an absence is corroborated by absentNodeConfirmations probes spaced by
// absenceStreakMinAge (absencestreak.go), and it is entered by EVERY child of a directory the
// server's recursive `remove` already took - the most frequent instance of the ~80-requests-a-second
// spin finding H4 measured, not a rare one. Folded into the streak under its own synthetic code it
// backs off geometrically instead, and it is TRANSIENT (serverTransientRejection): the streak is
// this client's own evidence-gathering, not a server verdict, so the dead-letter exit never applies
// - a delete must not be dropped for its own postcondition still gathering the evidence that will
// arrive.
var errRemoveAbsenceUnproved = errors.New("engine: outbound remove target fate unproved")

// applyMkdirOut creates a directory on the server under its resolved parent and records
// the server-assigned file id into the baseline as StateSynced.
func (e *Engine) applyMkdirOut(ctx context.Context, op store.Op) error {
	// Validate containment first so a structurally bad path dead-letters rather than being
	// sent. This must precede resolveParentID: for a traversal path like "../escape",
	// path.Dir is ".." and resolveParentID would return errParentUnknown (leaving the op
	// queued forever) before the containment check could reject it - matching applyRenameOut
	// and applyRemoveOut, which both check containment first.
	if !containable(op.Path) {
		return fmt.Errorf("%w: outbound mkdir path %q in view %d", errRejectedPath, op.Path, op.ViewID)
	}
	parentID, err := e.resolveParentID(op.ViewID, op.Path)
	if err != nil {
		// Order B: a preceding inbound OpRemove of the whole parent directory
		// already applied and guardRemoveDir/preserveAside carried the local subtree into a
		// _Conflict copy, dropping the parent baseline - so resolveParentID can never resolve
		// it and no parent op will ever run. The mkdir addresses a directory that is now gone
		// from op.Path on disk too (moved under the _Conflict prefix). Distinguish that
		// permanent dead-end from an ordinary errParentUnknown wait (parent op pending, the
		// local directory still present) by probing the local target: gone → terminal.
		if errors.Is(err, errParentUnknown) && e.localTargetGone(op.ViewID, op.Path) {
			return errParentGone
		}
		// The local directory is still present but NO baseline tracks the parent at all (not
		// merely an id-less pending parent, which stays the plain transient wait). That is a
		// permanent dead-end: either a sibling's dead-letter cleanup dropped the parent
		// baseline after the server deleted the ancestor, or a server-side rename relocated it
		// (relocatePending) - and in both cases the authoritative inbound op is serialized
		// BEHIND this mkdir, so waiting is a cyclic wedge. Confirm the
		// parent gone server-side with the same ancestor walk the server-turn exit below uses;
		// a renamed ancestor also reads gone (leaf-blind, accepted as data-safe - the
		// relocation-aware cleanup re-bases the pending baseline through the queued inbound
		// renames and the next scan re-creates the directory under the live parent).
		if errors.Is(err, errParentBaselineAbsent) && e.parentGoneConfirmed(ctx, op.ViewID, op.Path) {
			return errParentGone
		}
		return err
	}
	name := path.Base(op.Path)

	viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	var node actions.Node
	werr := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		n, merr := pc.Mkdir(ctx, parentID, name, time.Now().Unix(), e.viewRootNodeID(op.ViewID))
		if merr != nil {
			// A prior attempt may have committed the mkdir server-side before its reply was
			// lost (the connection dropped after the directory was created); retrying then
			// returns a path error forever. Verify the postcondition: if the directory now
			// exists on the server, adopt it as a success rather than wedging the op.
			// On any uncertainty (the query fails, or the node is absent / not a directory
			// with no id) the original error is surfaced and the op is retried.
			if errors.Is(merr, actions.ErrPathError) {
				existing, qerr := c.QueryNode(ctx, viewID, op.Path)
				if qerr == nil && existing.IsDir {
					node = existing
					return nil
				}
				// A clean probe that instead found a regular FILE (non-empty id) at the path is a
				// confirmed file<->dir create/create collision: another client
				// created a file where this client created a directory. The mkdir can never land -
				// the adopt branch above rightly refuses a non-directory, and nothing else in
				// applyOutbound's matrix matches a bare ErrPathError - so pre-fix the op retried
				// forever, wedging its conflict chain (cold start funnels this exact divergence via
				// planColdConflict; the incremental path had no equivalent). Surface the typed
				// collision so applyOutbound funnels it terminally into the keep-both resolver.
				if qerr == nil && !existing.IsDir && existing.FileID != "" {
					return fmt.Errorf("%w: mkdir %d/%q found server file %q",
						errServerTypeCollision, op.ViewID, op.Path, existing.FileID)
				}
			}
			return merr
		}
		node = n
		// A successful mkdir that did not echo a file id would advance the baseline to
		// StateSynced with an empty FileID and ack the op - yet every descendant create then
		// fails forever (resolveParentID → errParentUnknown) with the mkdir already gone from
		// the queue. Recover the id by querying the directory we just created; if
		// it still cannot be obtained, fail the op (below) so it is retried rather than acked
		// with an unusable baseline. Require the recovered node to be a directory, mirroring
		// the ErrPathError replay branch above: a concurrent process could have replaced the
		// path with a regular file between our mkdir and the probe, and adopting that file's id
		// as a directory baseline would strand every descendant op behind a non-directory parent.
		if node.FileID == "" {
			existing, qerr := c.QueryNode(ctx, viewID, op.Path)
			if qerr != nil {
				// A probe that BREAKS the connection propagates so WithWorker re-dials the
				// poisoned worker instead of releasing a byte-misaligned socket back to the
				// pool (mirroring recoverUploadFileID); a clean lookup
				// failure keeps the original fallback - the post-turn empty-id guard below
				// re-queues the op for a later retry.
				if br, ok := c.(brokenReporter); ok && br.Broken() {
					return qerr
				}
				return nil
			}
			if existing.FileID != "" && existing.IsDir {
				node = existing
			}
		}
		return nil
	})
	if werr != nil {
		// Order A: the mkdir failed with a path error and the parent directory was
		// deleted server-side (confirmed by the ancestor walk). It can never land under a
		// directory that no longer exists → terminal drop, mirroring the remove verb's exit.
		if errors.Is(werr, actions.ErrPathError) && e.parentGoneConfirmed(ctx, op.ViewID, op.Path) {
			return errParentGone
		}
		return werr
	}
	if node.FileID == "" {
		// No id from the mkdir reply and none recoverable: leaving the op un-acked retries it
		// (an already-created directory comes back as ErrPathError, handled above) rather than
		// stranding descendants behind an idless parent baseline. It carries errUploadNoFileID
		// for the same reason its upload twin does - an unpaced identical non-answer re-issues
		// every drain - though a mkdir turn costs one request rather than a whole file body.
		return fmt.Errorf("%w: mkdir %d/%q returned no file id and none could be recovered; will retry",
			errUploadNoFileID, op.ViewID, op.Path)
	}
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = node.FileID
		b.FileType = fileType(true)
		b.ServerMTime = node.MTime
		b.LocalMTime = op.LocalMTime
		b.UnixPerm = op.UnixPerm
	})
}

// applyRenameOut moves a node on the server from its old to its new view-relative path.
// The local-reconcile pass already relocated the baseline to NewPath (StatePending) when
// it emitted the op, so a successful rename only advances that record to StateSynced with
// the new local facts.
func (e *Engine) applyRenameOut(ctx context.Context, op store.Op) error {
	if !containable(op.Path) || !containable(op.NewPath) {
		return fmt.Errorf("%w: outbound rename %q->%q in view %d", errRejectedPath, op.Path, op.NewPath, op.ViewID)
	}
	werr := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
		if gerr := guardRenameTarget(ctx, c, viewID, op); gerr != nil {
			return gerr
		}
		rerr := pc.Rename(ctx, viewID, e.viewRootNodeID(op.ViewID), op.Path, op.NewPath)
		if rerr != nil {
			// A prior attempt may have committed the rename server-side before its reply was
			// lost; retrying then fails forever because the source is already gone. Verify the
			// postcondition: if the moved node now sits at the destination, the move took
			// effect - treat it as a success. The destination's id must MATCH the
			// moved node; on any uncertainty surface the original error.
			//
			// The match is unconditional now. It used to be waived for an id-less op
			// (op.FileID == "" || ...), which would adopt ANY node standing at the destination
			// as our completed move - the silent-divergence shape guardRenameTarget exists to
			// prevent, reached from the other side. planRename always carries a FileID, so the
			// waiver was unreachable; it is removed rather than left as a trap for the first
			// producer of an id-less rename.
			if errors.Is(rerr, actions.ErrPathError) && op.FileID != "" {
				if existing, qerr := c.QueryNode(ctx, viewID, op.NewPath); qerr == nil && existing.FileID == op.FileID {
					return nil
				}
			}
			return rerr
		}
		return nil
	})
	if werr != nil {
		// Order A/B: the rename failed with a path error because a directory the move
		// addresses was deleted server-side (confirmed by the ancestor walk). A rename touches TWO
		// endpoints - the SOURCE (op.Path) and the DESTINATION (op.NewPath) - and pc.Rename fails
		// with ErrPathError when EITHER the source is gone (its own parent directory deleted) OR the
		// destination's parent is gone. Probe both: the move can never land while either directory
		// is missing → terminal drop, mirroring the remove verb. Checking only the destination
		// missed the cross-directory move whose SOURCE parent was deleted while the destination
		// parent stayed live (e.g. a/x → b/y with a/ gone, b/ present) - the op wedged the queue
		// forever and, drained ahead of the inbound OpRemove(a/), blocked that authoritative remove
		// too. Probe the destination first (the common intra-directory move short-circuits with a
		// single server turn); dropGoneParentBaseline then drops the NewPath baseline so the next
		// local scan re-detects the moved file as untracked and re-uploads it into the still-live
		// destination directory.
		//
		// A server-side RENAME of the source's parent (a -> c, not a delete) is NOT distinguished
		// here: pc.Rename addresses op.Path BY PATH (it does not call resolveParentID / address by
		// id), so a renamed source parent also makes op.Path unresolvable → ErrPathError →
		// parentGoneConfirmed(op.Path) reads "gone" via the leaf-name-blind ancestor walk. Dead-
		// lettering that case too is deliberate and data-safe: dropGoneParentBaseline drops the
		// NewPath baseline and the next scan re-uploads the still-on-disk file at the destination -
		// no data loss, at worst a spurious duplicate of the node that survived under the renamed
		// parent (a cross-client move-vs-move conflict keep-both tolerates). The remove verb's
		// PathByID cross-check has no rename analog: the user's local move already relocated this
		// node's baseline to NewPath (planRename's MoveBaseline), so PathByID reflects NEITHER
		// server outcome and cannot tell deleted from renamed, and the protocol has no id-addressed
		// node query to check the server. Leaving the op transient instead would permanently wedge
		// the queue AND block the inbound OpRename(a -> c) serialized behind it on the ancestor key
		// - strictly worse. See ancestorConfirmsAbsent's LEAF-NAME BLINDNESS note.
		if errors.Is(werr, actions.ErrPathError) &&
			(e.parentGoneConfirmed(ctx, op.ViewID, op.NewPath) || e.parentGoneConfirmed(ctx, op.ViewID, op.Path)) {
			return errParentGone
		}
		// Both parent probes read the parents ALIVE, the destination probe found
		// nothing, and the rename still failed with a path error - the remaining suspect is the
		// source NODE itself: another client deleted it (or moved it away) server-side after the
		// local move was scanned. Without this exit the op retried forever - nothing else in
		// applyOutbound's matrix matches a bare ErrPathError - and the authoritative inbound
		// OpRemove/OpRename serialized BEHIND it on the shared source key never ran: a permanent
		// mutual wedge, cured only by Reset. Confirm the source node positively away from
		// op.Path by listing its parent (the same proof the remove verb uses), so only a
		// positively-settled node takes the terminal exit and an inconclusive listing stays
		// transient. Gated on a non-empty FileID:
		// planRename always carries one, and an id-less op must never be dropped on a name-only
		// listing (a node renamed away in place would falsely read gone).
		//
		// "Still visible by id under a DIFFERENT leaf name" is now a POSITIVE
		// exit too, not a "present" that keeps the op transient forever - see
		// renameSourceNodeNotAtPath. A node renamed in place server-side (a/x -> a/z) makes
		// op.Path permanently unresolvable, so retrying could only repeat the same probe with the
		// same answer while the inbound rename behind it starved.
		if errors.Is(werr, actions.ErrPathError) && op.FileID != "" && e.renameSourceNodeNotAtPath(ctx, op) {
			return errRenameSourceGone
		}
		// Every question this client can ask has now been asked and none of them explains the
		// refusal: both parents are alive, the source node is still at its path, and
		// guardRenameTarget proved the destination free before the turn. A bare ErrPathError
		// carries no *actions.ServerError, so without a code of its own noteServerErr cleared
		// the streak AND the retry pause on every drain and the op re-issued the turn plus two
		// ancestor walks forever, holding every path-overlapping op - including the peer's
		// inbound apply for this very node - behind it on blockedKeys. Name it so the ordinary
		// pacing and escalation apply and the operator gets a dead-letter instead of a wedge.
		if errors.Is(werr, actions.ErrPathError) {
			return fmt.Errorf("%w: %d/%q->%q: %w", errRenameUnsettled, op.ViewID, op.Path, op.NewPath, werr)
		}
		return werr
	}
	return e.markSynced(op.ViewID, op.NewPath, func(b *store.Baseline) {
		// Normal path: reconcile's MoveBaseline already relocated the source baseline to
		// NewPath carrying its FileID/FileType, so this branch is skipped and only the local
		// facts are refreshed. Crash path: a crash between the durable Enqueue and that
		// MoveBaseline (separate transactions) can leave the source baseline still synced at
		// op.Path with no record at NewPath; markSynced would then write a fresh baseline with
		// an EMPTY file id (dropping the node from byid) and strand the synced source baseline.
		// Carry the moved node's id (op.FileID, a file by construction) so the record is
		// correctly indexed - and PutBaseline's single-Put move logic then drops the stranded
		// source baseline that still indexes this file id.
		if b.FileID == "" {
			b.FileID = op.FileID
			b.FileType = fileType(false)
		}
		b.LocalHash = op.LocalHash
		b.LocalSize = op.LocalSize
		b.LocalMTime = op.LocalMTime
		b.UnixPerm = op.UnixPerm
	})
}

// errRenameTargetOccupied marks an outbound OpRename whose DESTINATION is held server-side by
// a node that is not the one being moved. applyOutbound funnels it into the keep-both resolver.
var errRenameTargetOccupied = errors.New("engine: outbound rename destination is held by another server node")

// errRenameUnsettled marks an outbound OpRename the server refused with a bare path error that
// none of this file's probes could explain. It carries a synthetic streak code so the refusal is
// paced and eventually dead-lettered instead of re-issued every drain forever.
var errRenameUnsettled = errors.New("engine: outbound rename refused for a reason no probe could settle")

// errRenameTargetUnproved marks an outbound OpRename whose destination probe was inconclusive -
// the server neither produced a node nor said the path is free. Transient: the op stays queued
// and the probe repeats, paced, rather than mutating on an unanswered question.
var errRenameTargetUnproved = errors.New("engine: outbound rename destination could not be proved free")

// guardRenameTarget proves, before the turn, that the rename's destination is free.
//
// It is the third of the client-proves-it preconditions, alongside guardUploadTarget (H1) and
// verifyServerSubtreeAgrees (H7), and it exists for the same reason: the verb offers the client
// no way to say "only if the destination is empty". Unlike those two the SERVER's behaviour here
// is unmeasured - docs/INTEGRATION.md pins rename's absence code and its cursor rule, but nothing
// has ever renamed onto an occupied path against a live DSM - and both possible answers are bad
// without this guard:
//
//   - if the server REPLACES the occupant, a local move silently destroys a node another client
//     created, everywhere, through a verb that never reports a conflict;
//   - if the server REFUSES with 40961, the op returns a bare ErrPathError that no arm below can
//     settle (both parents alive, the source still at its path), which noteServerErr reads as
//     "not a code" - no streak, no pacing - so it re-issues the rename plus two ancestor walks
//     every drain forever, holding every path-overlapping op behind it on blockedKeys.
//
// A node at the destination carrying our own file id is not an occupant: it is this op's own
// lost reply, and the turn below is skipped by returning nil after the postcondition adopts it.
// That case is left to the existing post-failure adoption so both routes stay in one place; the
// guard only has to let it through.
//
// Refuses toward NOT mutating: an inconclusive probe leaves the op queued.
func guardRenameTarget(ctx context.Context, c syncClient, viewID int64, op store.Op) error {
	prober, ok := c.(nodeProber)
	if !ok {
		return errNoOutbound
	}
	existing, qerr := prober.QueryNode(ctx, viewID, op.NewPath)
	switch {
	case errors.Is(qerr, actions.ErrPathError):
		// The destination is free - the answer this verb needs and cannot ask for.
		return nil
	case qerr != nil:
		return fmt.Errorf("%w: %d/%q: %w", errRenameTargetUnproved, op.ViewID, op.NewPath, qerr)
	case existing.FileID == "":
		// A node with no id proves neither occupancy nor absence.
		return fmt.Errorf("%w: %d/%q answered a node with no id", errRenameTargetUnproved, op.ViewID, op.NewPath)
	case existing.FileID == op.FileID:
		// Our own node already sits there: a prior attempt landed and lost its reply. Let the
		// turn run; its ErrPathError postcondition adopts this same evidence as success.
		return nil
	default:
		return fmt.Errorf("%w: %d/%q holds node %q, moving %q there would destroy it",
			errRenameTargetOccupied, op.ViewID, op.NewPath, existing.FileID, op.FileID)
	}
}

// errRenameSourceGone marks an outbound OpRename whose source NODE is positively confirmed
// no longer to sit at the op's source path while both parent directories are alive:
// another client deleted it, moved it away, or renamed it in
// place under the same parent. The stale move can never execute - the source path holds
// nothing to move - so applyOutbound resolves it as a race (drop the destination's pending
// baseline so the on-disk file becomes untracked and the next scan re-uploads it as a fresh
// create, ack, NO durable trace) rather than leaving it queued, where it would permanently
// block the authoritative inbound OpRemove/OpRename behind it.
var errRenameSourceGone = errors.New("engine: outbound rename source node no longer at the source path")

// renameSourceNodeNotAtPath reports whether an outbound rename's source node is unambiguously
// no longer at op.Path server-side, by listing its parent directory over a fresh worker lease
// (run strictly AFTER the op's primary lease returned, like parentGoneConfirmed, so no nested
// lease can deadlock at numWorkers=2). Only POSITIVE proof returns true; an inconclusive
// listing (a probe timeout, an unidentifiable child, a lease failure) reports false, so the
// caller keeps the op queued rather than dropping it on a transient blip.
//
// Two positive shapes qualify, both from the single listing resolveServerNodeAbsence classifies:
//   - GONE: the node is neither matched by id nor occupying the leaf - deleted, or moved to
//     another directory (the listing cannot see where).
//   - MOVED: the node is still visible BY ID under a DIFFERENT leaf name in the same parent -
//     another client renamed it in place (a/x -> a/z), so op.Path resolves to nothing.
//
// The MOVED shape is a positive exit. Before it became one, the id-match reported the
// node "present" and the op stayed transient FOREVER: pc.Rename can never resolve op.Path
// again, so every drain repeated the same probe with the same answer while the authoritative
// inbound OpRename serialized behind it on the shared source key never ran - a permanent
// mutual wedge visible only as Stuck and curable only by Reset. Its resolution is the same as
// GONE's, and data-safe for the same reason: nothing local is deleted (the moved file stays on
// disk, merely untracked) and the next scan re-uploads it as a fresh create. The server node
// surviving under its new name is the accepted move-vs-move residual already documented for
// the cross-directory variant - the client re-materialises it from a later modify event or the
// cold-start pull.
//
// Accepted residual: a CROSS-directory move whose prior attempt committed server-side but
// lost its reply, when the destination postcondition probe ALSO fails transiently, reads
// "source gone" here - the node now lives under the destination parent, which the source
// parent's listing cannot see. That triple coincidence is data-safe: the resolution only
// untracks the still-on-disk file, and the next scan re-uploads it (at worst a keep-both
// false-conflict adoption of the identical content), never a loss.
func (e *Engine) renameSourceNodeNotAtPath(ctx context.Context, op store.Op) bool {
	vid := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	var settled bool
	err := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		gone, moved, cerr := resolveServerNodeAbsence(ctx, pc, e, vid, op.Path, op.FileID)
		if cerr != nil {
			return cerr
		}
		settled = gone || moved
		return nil
	})
	return err == nil && settled
}

// removeNodeMovedError marks an outbound OpRemove whose target node the server RELOCATED
// (an inbound rename of an ancestor directory already reconciled, relocating the node's
// baseline) rather than deleted. The remove addressed the node's OLD path, so it can never
// execute there; and because ancestorConfirmsAbsent matches an ancestor by leaf NAME only, a
// renamed ancestor (a/b -> a/x) reads as "a/b gone" and falsely confirms the still-present
// child gone too - which would ack the remove as a bogus idempotent success, stranding the
// node on the server and freezing the relocated baseline StatePending forever. It
// carries the node's CURRENT indexed path so applyOutbound can revert that baseline to
// StateSynced and let the next scan re-emit an OpRemove against the new path (the
// resolveVanishedUpload precedent).
type removeNodeMovedError struct{ currentPath string }

func (e *removeNodeMovedError) Error() string {
	return fmt.Sprintf("engine: outbound remove target relocated to %q; re-emitting on the new path", e.currentPath)
}

// removeServerDivergedError marks an outbound OpRemove whose target node the server MODIFIED
// after the local delete was scanned: the queued delete addresses the node
// by path, so executing it would destroy another client's edit with no keep-both copy - exactly
// the shape DESIGN §6 calls a conflict ("local change + remote change → conflict"), which the
// remove verb alone carried no precondition against. It carries the server's current facts so
// applyOutbound can recover that version (recoverDivergedRemove enqueues an inbound download)
// instead of deleting it.
type removeServerDivergedError struct{ node actions.Node }

func (e *removeServerDivergedError) Error() string {
	return fmt.Sprintf("engine: outbound remove target %q diverged server-side; recovering the server version", e.node.FileID)
}

// applyRemoveOut deletes a node on the server. The node is gone locally (its disappearance
// produced the op), so on success the baseline is dropped. A server-side path error means the
// node is already gone server-side too - an idempotent success - so the baseline is dropped and
// the op acked rather than retried forever.
//
// Before the destructive turn an id-carrying remove PROBES the path (DECISION: probe only when the
// op carries a FileID, at one extra QueryNode per delete - see DESIGN §6). A delete can sit queued
// arbitrarily long and addresses the node BY PATH, so without a precondition it destroys whatever
// the path holds when it drains:
//   - a DIFFERENT node holds the path (another client took it over): deleting would remove a
//     foreign node. Our node's fate is settled by an id-addressed probe (nodeGoneByID): positively
//     gone acks and drops the baseline; still visible by id is the MOVED outcome - never the ack,
//     which would lose the delete.
//   - the SAME node whose content moved off our synced base (a peer's modify racing our delete):
//     surface removeServerDivergedError so the server version is recovered, not destroyed.
//
// A hash comparison is trusted only when BOTH hashes are present (as in
// serverDivergedForVanishedUpload: Node.Hash is tolerantly decoded and the base ServerHash may be
// empty); otherwise node identity decides and the same id proceeds. Present-and-different hashes
// are IDENTITY-FREE evidence, so they are checked ahead of the id-less arm (else a DSM that omits
// file_id makes the precondition inert) but behind the different-id arm, whose outcomes (ack,
// MOVED wait) destroy nothing.
//
// An ErrPathError probe (absent OR timeout, indistinguishable) and an id-less reply with no
// divergence proof fall through to the Remove, whose postcondition machinery resolves them; any
// other probe error leaves the op queued (WithWorker re-dials a broken connection). After the
// turn, an ErrPathError postcondition listing that shows the node under a DIFFERENT leaf name is
// MOVED, not "present" (op.Path can never resolve again, so re-probing it would wedge): it only
// unlocks the PathByID cross-check that re-emits the delete on the node's current path once
// reconcile folds the rename; until then the op stays queued.
//
// A DIRECTORY target takes a SECOND precondition (finding H7): the server's `remove` of a
// directory is RECURSIVE, so a node a peer added since we last looked would be destroyed
// everywhere. verifyServerSubtreeAgrees (outbound_removedir.go) enumerates the subtree
// server-side and refuses on anything the base cannot account for; anything unproven leaves the
// op queued rather than deleting on unproven emptiness.
func (e *Engine) applyRemoveOut(ctx context.Context, op store.Op) error {
	if !containable(op.Path) {
		return fmt.Errorf("%w: outbound remove path %q in view %d", errRejectedPath, op.Path, op.ViewID)
	}
	// One decode of the record at op.Path serves both preconditions: the divergence base the
	// pre-turn probe compares against, and the recorded KIND. A DIRECTORY delete needs a second,
	// subtree-wide precondition, because the server's `remove` is RECURSIVE (finding H7) and so
	// destroys children this client never saw along with the ones it did; verifyServerSubtreeAgrees
	// proves the subtree agreed before the destructive turn.
	baseServerHash, recordIsDir, kindUnknown, kindPlaced, bherr := e.removeTargetFacts(op)
	if bherr != nil {
		return bherr
	}
	// The record is not the only witness of the target's kind, and it is the one that can be
	// GONE by apply time (planRemove's own flip crashed, an ancestor's teardown cascaded, a
	// success-path DeleteBaseline whose Ack then failed). op.BaseIsDir is the durable copy
	// planRemove stamped from the vanished baseline, and the pre-turn probe below contributes a
	// third: reading an absent record as "not a directory" would skip the whole recursive-delete
	// precondition, which is the one input here that fails toward DELETING.
	targetIsDir := recordIsDir || op.BaseIsDir
	if targetIsDir && e.removeDirLocallyPresent(op) {
		// The directory is back on disk, so the local deletion this op was derived from is no
		// longer the state a RECURSIVE server delete would be acting on - the reachable
		// interleaving is a peer's child download applying ahead of this remove and recreating
		// the tree, after which the subtree enumeration below reads the peer's node as agreed
		// content and deletes it everywhere (errRemoveDirLocallyPresent). Resolve it locally,
		// before the lease: applyOutbound reverts the pending baseline and acks, and the next
		// scan re-diffs the tree as it now is.
		return fmt.Errorf("%w: %d/%q", errRemoveDirLocallyPresent, op.ViewID, op.Path)
	}
	var (
		confirmedGone  bool
		occupied       bool
		movedAway      bool
		diverged       actions.Node
		divergedProven bool
	)
	werr := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
		// Set only by POSITIVE evidence that the target is a FILE, for the corrupt-record arm
		// below. The witness is the one verifyServerSubtreeAgrees already rests on and the stand
		// measured (docs/INTEGRATION.md): a file carries a content hash and a directory carries
		// none, so a hash on a node whose is_dir is not set proves a file stands there.
		targetProvenFile := false
		// Set when the pre-turn probe answered with the path-error sentinel, which is the ONE
		// non-answer the unplaced-target refusal below still lets through: see its comment.
		probeSaidAbsent := false
		// Placing the target's KIND is a PATH-addressed question, so it needs no identity - and
		// when the record is corrupt and the durable capture says nothing, this probe is the only
		// witness left. Gating it behind op.FileID left an id-less op unplaceable BY CONSTRUCTION:
		// no probe ran, so the refusal below fired on every attempt, paced, and finally
		// dead-lettered the delete with a StateConflict record at a path holding no local file -
		// which localReconcile never drives again. That is the same permanent freeze the arm was
		// reshaped to avoid, reached through the other door, and it is not exotic: a DSM whose
		// replies omit file_id (the shape the id-less tests model throughout) leaves EVERY synced
		// record id-less, so every outbound remove takes this path. The identity arms below do
		// need an id and stay gated; only the kind question is asked without one.
		//
		// The gate is "no witness has PLACED the target as a directory yet", not "the record was
		// corrupt": an ABSENT record answers nothing about the kind either (removeTargetFacts'
		// kindPlaced), and reading its zero value as a placed FILE skipped this probe entirely for
		// an id-less op whose record another path dropped between the Enqueue and the flip - the
		// remaining witness, op.BaseIsDir, is false for an op queued before planRemove carried the
		// stamp and for a record that mistyped a directory (a sparse create event omits is_dir).
		// All three witnesses then failed silently and the RECURSIVE Remove went out with nothing
		// proved about the subtree, which is finding H7 itself. A record that DID decode has
		// answered, so a plain file delete still pays no extra turn.
		// The second disjunct is what keeps the file-standing-where-a-directory-was refusal below
		// REACHABLE. With `!kindPlaced && !targetIsDir` a directory delete off a record that
		// decoded placed the kind, so no probe ran at all for an id-less op - and against a DSM
		// whose replies omit file_id (the shape this file's id-less arm exists for) EVERY outbound
		// remove is id-less, so targetProvenFile could never be set and the refusal was dead code
		// precisely where it matters. A peer replacing the tracked directory with a FILE then went
		// undetected, verifyServerSubtreeAgrees `list`ed a path that is a file, and a server
		// answering that the way it answers an empty directory enumerates nothing and lets the
		// RECURSIVE remove destroy the replacement. A directory delete is about to make N list
		// turns anyway, so one path-addressed probe ahead of them is not a cost worth trading the
		// guarantee for; a plain FILE delete still pays nothing extra.
		needKindProbe := !kindPlaced || targetIsDir
		if op.FileID != "" || needKindProbe {
			server, qerr := c.QueryNode(ctx, viewID, op.Path)
			if !errors.Is(qerr, actions.ErrPathError) {
				// Whatever this probe answered, it was not the sentinel: the run of path-errors
				// - the only thing that tells a real absence apart from a repeatedly timing-out
				// server - is broken, so the next sentinel starts a fresh streak. Without this
				// the contradiction verifyServerSubtreeAgrees' root arm claims to close stayed
				// open: that arm reads an ErrPathError LISTING of the directory as "nothing to
				// enumerate" once absenceCorroboratedByNodeProbe has seen absentNodeConfirmations
				// sentinel probes, and those probes are counted in the same shared streak map -
				// so a probe that ANSWERED here, positively, for this very path moments earlier
				// left the streak intact and the recursive delete went ahead on an absence the
				// same apply had just refuted. Same rule, and the same map, as
				// guardUploadTarget's sibling gate on the upload axis.
				e.clearAbsentProbe(viewID, op.Path)
			}
			if qerr == nil && server.IsDir {
				// The SERVER says the target is a directory. A sparse reply omits is_dir (so
				// false is no evidence either way), but a positive one is proof, and it is the
				// only witness left when no record and no captured kind survive to apply time.
				targetIsDir = true
			}
			if qerr == nil && !server.IsDir && server.Hash != "" {
				targetProvenFile = true
			}
			probeSaidAbsent = errors.Is(qerr, actions.ErrPathError)
			switch {
			case qerr != nil && !errors.Is(qerr, actions.ErrPathError):
				// A transport/protocol failure is no evidence either way; never delete on it.
				return qerr
			case qerr != nil:
				// ErrPathError: the node is absent OR the probe timed out - the Remove below and
				// its parent-listing postcondition are the arbiters, exactly as before.
			case op.FileID == "":
				// The probe ran only to place the KIND (needKindProbe). Every arm below compares
				// the reply against an identity this op does not carry, and the different-id arm
				// would read ANY answering node as a foreign occupant and hand nodeGoneByID an
				// empty id to classify. The kind facts are already taken above; the id-less op
				// keeps its historical fall-through to the Remove, still behind the unplaced-
				// target refusal that follows - a probe that placed nothing changes nothing.
			case server.FileID != "" && server.FileID != op.FileID:
				// A DIFFERENT node holds op.Path. That proves the path is not ours any more -
				// it does NOT prove our node is gone, which is what the ack + baseline drop
				// below asserts. The two fates are routinely confused by the same shape: the
				// rotate pattern (mv notes.txt notes.old.txt; touch notes.txt) leaves our node
				// alive under a new leaf name in the SAME directory while a stranger's node
				// takes the path. Acking there silently loses the user's delete AND drops the
				// baseline, so the surviving server node falls out of the replica entirely
				// (the later rename event finds nodeTracked false and dead-letters) until some
				// future modify re-materialises it.
				//
				// Disambiguate with the id-addressed classifier rather than the path-addressed
				// one: nodeGoneByID deliberately does not read the foreign occupant of our leaf
				// as presence, which is exactly the state we are in. Positive absence keeps the
				// idempotent ack; a node still visible by id is the MOVED outcome
				// and takes its treatment - the PathByID cross-check re-emits the
				// delete on the node's current path once reconcile has folded the rename, and
				// until then the op stays queued as a bounded wait. An inconclusive listing
				// propagates as transient, mirroring every other absence probe here.
				gone, gerr := nodeGoneByID(ctx, pc, e, viewID, op.Path, op.FileID)
				if gerr != nil {
					return gerr
				}
				if gone {
					confirmedGone = true
					occupied = true // only to render the foreign-occupant Warn below
				} else {
					movedAway = true
				}
				return nil
			case contentDiverged(server.Hash, baseServerHash):
				// PROVEN divergence, and the proof needs no identity at all: the path holds content
				// that is not the content we last agreed on, so the path-addressed delete below would
				// destroy an edit this client never saw. This branch therefore sits AHEAD of the
				// id-less arm: a reply that carries a hash but omits file_id
				// (decodeNode tolerates the omission, and a DSM that systematically omits it made the
				// whole v14 precondition inert) used to fall straight through to the destructive
				// Remove. It stays BEHIND the different-id arm, which is not a case of missing
				// identity but of PRESENT contradicting identity: there the node at the path is
				// provably not ours, and nodeGoneByID settles our own node's fate (an idempotent ack
				// or the MOVED wait) without destroying anything either way.
				diverged = server
				divergedProven = true
				return nil
			case server.FileID == "":
				// A present node reported without an id AND without a divergence proof carries
				// nothing to decide on (decodeNode tolerates the omitted id, and either hash may be
				// missing); keep the historical behaviour rather than guessing.
			}
		}
		if targetProvenFile && targetIsDir {
			// The base places the target as a DIRECTORY and the probe proves a FILE stands there
			// now: a peer replaced the node. This is the delete TARGET's own instance of the
			// kind-change divergence verifyServerSubtreeAgrees refuses for a CHILD
			// (outbound_removedir.go's mirror arm), and without it the two witnesses simply
			// disagreed and the directory arm below won: the subtree walk then `list`ed a path that
			// is a file, and a server answering that the way it answers an EMPTY DIRECTORY
			// (node_list:[] with listed==true - the shape the stand did measure, for a directory)
			// enumerates nothing, returns the verification nil, and lets the recursive Remove
			// destroy the peer's replacement on the server and in every replica. That premise -
			// what `list` of a FILE does - is exactly the unmeasured one the child arm exists not to
			// rest on, so the target may not rest on it either.
			//
			// The evidence and its exclusion are the child arm's verbatim: a content hash with
			// is_dir UNSET, since query_node carries a file's hash and a directory carries none
			// (docs/INTEGRATION.md), while a positively-set is_dir has already been consumed above
			// as proof of a directory. applyOutbound funnels this into StateConflict plus a durable
			// trace naming the path - the never-lose direction, never a silent delete.
			return &removeDirDivergedError{
				child:  op.Path,
				reason: "the server holds a file where the baseline recorded a directory (another client replaced it)",
			}
		}
		// NO witness placed the target's kind: the record did not (kindPlaced false - it was
		// corrupt, or gone by apply time), op.BaseIsDir (planRemove's durable capture, taken while
		// the record still decoded) is false, and the pre-turn probe proved neither a directory
		// (which would have taken the verified path above) nor a file. The unplaced target may be
		// a directory, and the server's `remove` is RECURSIVE - issuing it would destroy a subtree
		// with nothing proved about it, which is finding H7 itself. A target the probe placed as a
		// FILE has no such hazard and proceeds: making an unplaceable target refuse every remove
		// froze plain file deletes permanently (the delete dead-letters on removeDirUnverifiedCode
		// and markOutboundConflict leaves a StateConflict record at a path with no local file,
		// which localReconcile never drives again).
		//
		// The gate is kindPlaced, not kindUnknown. Keying it on the CORRUPT record alone left the
		// ABSENT one - the same non-answer, reached through the documented window between
		// localReconcile's Enqueue and planRemove's flip, an ancestor's teardown, or a
		// success-path DeleteBaseline whose Ack failed - falling through with its zero value read
		// as a placed FILE. Combined with an op predating the BaseIsDir stamp (or one whose record
		// mistyped a directory) and a sparse probe reply that carries neither is_dir nor a hash,
		// all three witnesses failed silently and the RECURSIVE Remove went out anyway: exactly
		// the hole the kindPlaced flag was introduced for, one gate further down.
		//
		// The path-error sentinel keeps its fall-through, and only for a record that was merely
		// ABSENT. There the ordinary reading is "the node is gone", the Remove below and its
		// parent-listing postcondition are the arbiters that were always meant to settle it, and
		// refusing instead would freeze the idempotent re-delete of an already-deleted node in the
		// StateConflict dead end described above. A CORRUPT record is excluded from that
		// relaxation as before: undecodable bytes are precisely where the kind is unknowable, so
		// the ambiguity of 28672 ("absent OR a server-side timeout") must not be resolved toward
		// deleting on top of it.
		if !kindPlaced && !targetIsDir && !targetProvenFile && (!probeSaidAbsent || kindUnknown) {
			return fmt.Errorf(
				"%w: no witness placed the target of %d/%q (its record answered nothing, nor did the captured kind or the probe), so a recursive directory delete cannot be ruled out",
				errRemoveDirUnverified, op.ViewID, op.Path)
		}
		if targetIsDir {
			if !recordIsDir && !op.BaseIsDir && e.removeDirLocallyPresent(op) {
				// Only the probe testified to the kind, so the stale-delete guard above ran on a
				// target it did not yet know was a directory. Apply it here rather than skipping
				// it: it is the arm that owns the interleaving the subtree enumeration cannot see
				// (a peer's child download recreating the tree ahead of this remove).
				return fmt.Errorf("%w: %d/%q", errRemoveDirLocallyPresent, op.ViewID, op.Path)
			}
			// Prove the whole server subtree is content this client last agreed on. Anything it
			// cannot prove keeps the op queued (errRemoveDirUnverified); anything it disproves is
			// a divergence applyOutbound funnels into StateConflict with a durable trace, never a
			// recursive delete of a peer's node.
			if verr := e.verifyServerSubtreeAgrees(ctx, pc, viewID, op); verr != nil {
				return verr
			}
		}
		rerr := pc.Remove(ctx, viewID, op.Path)
		if errors.Is(rerr, actions.ErrPathError) {
			// ErrPathError (28672) covers BOTH "node already absent" - an idempotent delete - and
			// a server-side operation timeout where the delete may never have executed. Treating
			// it as success unconditionally would drop the baseline and ack a remove that never
			// ran, leaving the file present on the server forever. Verify
			// the postcondition instead, accepting ONLY an UNAMBIGUOUS proof of absence.
			//
			// QueryNode alone cannot supply it: a server reports an absent node ALSO with
			// ErrPathError (catalog_test.go), which is indistinguishable from a probe timeout, so
			// a node genuinely gone (a remove whose success reply was lost) would never confirm
			// and the op would wedge forever. Listing the
			// PARENT directory disambiguates: a clean listing (no error) that omits the child is
			// positive proof the node is gone, whereas a timeout makes the listing itself error
			// (inconclusive → retry).
			//
			// A clean QueryNode that returns a node WITHOUT a file_id is NOT proof of absence: a
			// present node tolerates an omitted id (decodeNode, types.go), so a node still on the
			// server but reported with incomplete metadata would be mistaken for gone - acking a
			// remove that never ran and dropping its baseline. The parent listing is
			// the sole arbiter; it keys on the node's file id and so cannot be fooled that way.
			//
			// The listing is classified by resolveServerNodeAbsence rather
			// than the plain absence check, so a node still visible BY ID under a DIFFERENT leaf
			// name (another client renamed it in place, a/x -> a/y) is recognised as MOVED instead
			// of a bare "present". A moved node makes op.Path permanently unresolvable, so the
			// plain check kept re-probing it every drain with the same answer forever while the
			// authoritative inbound OpRename serialized behind it starved. It is NOT an absence,
			// though: it must never reach the idempotent ack + baseline drop below (that would
			// lose the user's delete and strand the node server-side), so it only enables the
			// PathByID cross-check and otherwise stays transient.
			gone, moved, cerr := resolveServerNodeAbsence(ctx, pc, e, viewID, op.Path, op.FileID)
			switch {
			case cerr != nil:
			case gone:
				confirmedGone = true
				return nil
			case moved:
				movedAway = true
				return nil
			}
			// Neither absence nor a move is proved yet (an inconclusive listing, or one that still
			// reports the node): the op stays queued and re-classifies on a later drain. Carry the
			// pacing sentinel so the repeat is backed off rather than re-issued at full drain
			// cadence for the whole corroboration window - the raw sentinel carries no streak code
			// of its own. errors.Is(actions.ErrPathError) still holds for every existing classifier.
			return fmt.Errorf("%w: remove of %d/%q was answered with the path-error sentinel and the node's fate is not yet proved: %w",
				errRemoveAbsenceUnproved, op.ViewID, op.Path, rerr)
		}
		return rerr
	})
	if werr != nil {
		return werr
	}
	if divergedProven {
		// The node we were told to delete holds content the server accepted after our local
		// delete was scanned. Deleting it would silently destroy that edit (DESIGN §6 makes it
		// a conflict, not a delete); applyOutbound recovers it instead.
		return &removeServerDivergedError{node: diverged}
	}
	if (confirmedGone || occupied || movedAway) && op.FileID != "" {
		// The parent listing (or the ancestor walk one level up) reported the node absent - but
		// absence proved by a leaf-name-matching walk cannot tell a DELETED ancestor from a
		// RENAMED one: a server rename of a/b -> a/x lists "a" without "b", so the walk reads
		// "a/b gone" and confirms the still-present child gone too, falsely. Cross-check
		// the node's identity against the store: if its file id is now indexed at a DIFFERENT
		// path, an inbound rename of an ancestor already relocated it server-side, so this remove
		// is stale (it addressed the old path) and never ran. Re-emit it on the new path rather
		// than acking a delete the server never performed - which would leave the node on the
		// server and its relocated baseline stuck StatePending, inert forever (localReconcile only
		// drives StateSynced records). A node with no queued FileID, or one still indexed at
		// op.Path (a genuine local delete: planRemove flips the baseline pending but leaves byid
		// pointing at op.Path), keeps the idempotent-absence ack below. The residual pre-reconcile
		// window - a drain that outran the parent's inbound rename event, so byid still points at
		// op.Path - is accepted: the protocol has no id-addressed remove to fall back on.
		//
		// The pre-turn probe's foreign-occupant outcome (occupied) takes the
		// same cross-check for the same reason: our node is not at op.Path any more, and if the
		// store already knows where it went, the delete belongs on THAT path rather than being
		// acked away. Only when the node is nowhere indexed does the ack + baseline drop below
		// stand (its server-side fate is then unknowable from here - accepted residual: a delete
		// that never propagates beats deleting a stranger's node).
		//
		// The server-confirmed MOVED outcome takes the cross-check as its
		// ONLY exit: the listing proved the node alive under a new name, so the ack + drop below
		// would silently lose the delete. When the store has already caught up - reconcile's
		// relocatePending folded the inbound rename event, so byid names the new path - this
		// re-emits the delete there (revertRelocatedRemove), which is exactly the outcome the
		// probe's evidence supports. When it has not caught up yet, the fall-through below keeps
		// the op transient until the event arrives.
		if cur, perr := e.store.PathByID(op.ViewID, op.FileID); perr == nil && cur != op.Path {
			return &removeNodeMovedError{currentPath: cur}
		}
	}
	if movedAway {
		// The node is alive server-side under a different leaf name, but the store still indexes
		// it at op.Path: the inbound rename event has not been reconciled yet. Neither deleting
		// (the path resolves to nothing) nor acking (which would drop the baseline and lose the
		// delete) is right, so keep the op queued - the event's arrival relocates the baseline and
		// the cross-check above re-emits the delete on the new path on a later drain. Unlike the
		// pre-fix behaviour this is a bounded wait on a pending event, not a permanent wedge on a
		// probe whose answer can never change.
		// Paced under the same streak code as the unproved-absence exit above (and deliberately the
		// SAME one, so a wait that alternates between the two answers keeps its pause instead of
		// resetting the streak on every change): both mean "keep waiting", and both otherwise
		// re-issued the whole classification on every drain.
		return fmt.Errorf("engine: outbound remove %d/%q: target node renamed away server-side and not yet re-indexed locally: %w: %w",
			op.ViewID, op.Path, errRemoveAbsenceUnproved, actions.ErrPathError)
	}
	if occupied {
		e.log.Warn("engine: outbound remove target path is held by a different server node; dropping the stale delete",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
	}
	if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
		return fmt.Errorf("engine: drop removed baseline %d/%q: %w", op.ViewID, op.Path, err)
	}
	return nil
}

// The divergence base and the recorded kind an outbound remove's preconditions read both come
// from ONE decode of the record at op.Path - removeTargetFacts (outbound_removedir.go).

// recoverDivergedRemove resolves an outbound OpRemove whose target the server modified after the
// local delete was scanned (removeServerDivergedError): the server's version wins over the stale
// local delete, so an inbound OpDownload for the node is enqueued and the delete is dropped. The
// local file is already gone (its disappearance produced the op), so the download materialises
// the diverged server content with nothing to clobber - the never-lose direction, and the same
// "server is the authority for divergence" rule the 24579 upload funnel follows.
//
// It deliberately leaves the baseline untouched. The StatePending record planRemove left at
// op.Path still names the node and carries the OLD ServerHash - which is exactly what makes the
// resolution replay-safe: a crash between this enqueue and the op's Ack re-drains the remove,
// whose probe re-reads that unchanged base, re-detects the divergence and re-enqueues a
// duplicate download (harmless - the second re-fetches identical content, mirroring
// funnelUploadConflict's duplicate-resolve replay). Writing the fresh server facts here would
// instead erase the divergence signal and let that replay delete the server's edit. The
// download's own markSynced updates the record once the content lands.
func (e *Engine) recoverDivergedRemove(op store.Op, server actions.Node) error {
	fileID := server.FileID
	if fileID == "" {
		// The divergence was proved by content alone, from a reply that omitted file_id.
		// Downloads are id-addressed, so fall back to the id the op itself
		// asserts for the path - the node we last synced there. If the server did replace the
		// node behind our back, this fetches OUR node's content instead of the stranger's, which
		// is still the never-lose direction: the local file is already gone, nothing is
		// clobbered, and the replacement arrives through its own create event.
		fileID = op.FileID
	}
	if _, err := e.enqueueOp(store.Op{
		Type:       store.OpDownload,
		Direction:  store.DirInbound,
		ViewID:     op.ViewID,
		Path:       op.Path,
		FileID:     fileID,
		LocalHash:  server.Hash,
		LocalSize:  server.Size,
		LocalMTime: server.MTime,
		UnixPerm:   op.UnixPerm,
	}); err != nil {
		return fmt.Errorf("engine: enqueue diverged-remove download %d/%q: %w", op.ViewID, op.Path, err)
	}
	return nil
}

// revertRelocatedRemove reverts a relocated node's baseline to StateSynced so the next local
// scan re-detects the still-deleted-on-disk file and re-emits an OpRemove against the node's
// CURRENT path. It resolves the outbound OpRemove case: the remove addressed the node's
// old path, but an inbound rename of an ancestor relocated the node (and its baseline)
// server-side before the remove drained, so the stale op cannot execute and its baseline sits
// StatePending, inert forever. The revert mirrors resolveVanishedUpload - it only
// flips the state; the local facts already describe the last-synced server content (still what
// the server holds), and a StateSynced baseline with no on-disk file is exactly what
// detectRemoves keys on. It is idempotent (a baseline already synced, or dropped by an
// intervening op, is a no-op) so a failed ack that re-drains this op re-runs it harmlessly.
//
// A StateConflict record at the relocated path is left untouched
// (mirroring revertRelocatedOutbound): the divergence is owed to keep-both and must stay
// surfaced - flipping it to StateSynced would bury the divergence and let the scan's
// re-emitted OpRemove delete the diverged server copy the resolver has not reconciled yet.
//
// A corrupt (undecodable) record at the relocated path is OVERWRITTEN with a minimal synced
// record rebuilt from the op's own facts: deleting it instead would leave
// the path untracked with no on-disk file - nothing for the scan to diff - so the local delete
// would silently never propagate; the strict read before this arm wedged the cleanup forever.
// The Put heals the key, and a synced record with no local facts and no on-disk file is exactly
// the detectRemoves shape, so the OpRemove re-emission survives the rot.
func (e *Engine) revertRelocatedRemove(op store.Op, path string) error {
	b, err := e.store.GetBaseline(op.ViewID, path)
	if isNotFound(err) {
		return nil
	}
	if errors.Is(err, store.ErrCorruptRecord) {
		e.warnCorruptBaseline("revert-relocated-remove", op.ViewID, path, err)
		b, err = store.Baseline{FileID: op.FileID, FileType: fileType(op.BaseIsDir)}, nil
	}
	if err != nil {
		return fmt.Errorf("engine: load relocated remove baseline %d/%q: %w", op.ViewID, path, err)
	}
	if b.State == store.StateConflict {
		// The divergence is owed to keep-both and must stay surfaced
		// (mirroring revertRelocatedOutbound); flipping it synced would let the re-emitted
		// OpRemove delete the diverged server copy.
		return nil
	}
	if b.State == store.StateSynced {
		return nil
	}
	b.State = store.StateSynced
	if err := e.store.PutBaseline(op.ViewID, path, b); err != nil {
		return fmt.Errorf("engine: revert relocated remove baseline %d/%q: %w", op.ViewID, path, err)
	}
	return nil
}

// confirmAbsentByParentList reports whether the node identified by fileID at the
// slash-separated view-relative path p is unambiguously gone, by listing its parent directory.
// Returning (true, nil) requires POSITIVE proof of absence; (false, nil) means the node is
// still present; and a non-nil error means inconclusive (the caller leaves the op queued to
// retry). This distinguishes a genuinely-removed node from a probe timeout, which a bare
// QueryNode (ErrPathError for both absence and timeout) cannot.
//
// It is the absence-only projection of resolveServerNodeAbsence, which OWNS the id/name/
// unidentifiable-child matching rules and additionally splits "still present" into
// renamed-away vs. leaf-still-occupied; see its doc for those rules rather than a second
// copy here. The two were duplicated line for line until most
// callers moved onto the classifier, so the most safety-critical predicate in the outbound path
// has exactly one implementation - and one description of it.
func confirmAbsentByParentList(ctx context.Context, pc outboundClient, memo absenceMemo, viewID int64, p, fileID string) (bool, error) {
	gone, _, err := resolveServerNodeAbsence(ctx, pc, memo, viewID, p, fileID)
	return gone, err
}

// resolveServerNodeAbsence classifies, from a single parent listing, what became of the node
// identified by fileID at path p after a QueryNode(p) returned ErrPathError (which conflates a
// true server-side absence with a probe timeout). It mirrors confirmAbsentByParentList's
// id/name matching but distinguishes the two ways the node can survive so the keep-both resolver
// can tell "deleted" from "moved away":
//   - gone:  positive proof of absence - the node is neither matched by id nor occupying the
//     leaf, and no unidentifiable child could hide it.
//   - moved: the node lives on under a DIFFERENT leaf name in the same parent (matched by file
//     id, name != leaf) - it was renamed/moved server-side away from p.
//
// (false, false, nil) means the leaf is still occupied (the ErrPathError was a timeout) and a
// non-nil error means the listing itself was inconclusive - both leave the op queued to retry.
func resolveServerNodeAbsence(ctx context.Context, pc outboundClient, memo absenceMemo, viewID int64, p, fileID string) (gone, moved bool, err error) {
	parent := path.Dir(p)
	if parent == "." || parent == "/" {
		parent = viewRootPath
	}
	children, listed, err := pc.List(ctx, viewID, parent)
	if err != nil {
		if ancestorConfirmsAbsent(ctx, pc, memo, viewID, parent, err) {
			// The whole parent directory is gone server-side, and the node with it. The
			// ancestor walk cannot see a rename, so this reads as gone (never moved) - the
			// conservative outcome for the keep-both resolver, which dead-letters instead of
			// retrying forever ahead of the authoritative inbound remove queued behind it on
			// the same path (a mutual wedge otherwise).
			//
			// The walk's verdict is still LISTING-derived, one directory further up: it ends at
			// a grandparent listing that omitted the parent, which a page boundary produces
			// exactly like a delete. Corroborate p itself before the verdict becomes actionable
			// - this arc used to return gone straight from the walk, so an
			// id-carrying caller whose own probe would have refuted the absence never issued it,
			// and a paged listing could ack a delete the server never performed. A node still
			// answering at p means the walk read a page, not a deletion: neither gone nor moved,
			// so the caller retries.
			corroborated, perr := absenceCorroboratedByNodeProbe(ctx, pc, memo, viewID, p, fileID)
			if perr != nil {
				return false, false, perr
			}
			if !corroborated {
				return false, false, nil
			}
			return true, false, nil
		}
		return false, false, err
	}
	if !listed {
		// The turn succeeded but enumerated nothing (a verdict-only reply, no listing):
		// iterating the zero-length slice would read as "the parent is empty, so the node
		// is gone" and ack a remove the server never performed. Absent
		// enumeration is no evidence at all - inconclusive, so the caller retries.
		return false, false, fmt.Errorf("engine: parent listing of %q carried no listing; absence of %q inconclusive", parent, p)
	}
	leaf := path.Base(p)
	for _, ch := range children {
		if fileID != "" && ch.FileID == fileID {
			// The node is still present. A different leaf name means it was renamed away from p;
			// the same leaf name means p is still occupied (the ErrPathError was a timeout).
			return false, ch.Name != leaf, nil
		}
		if ch.Name == leaf {
			return false, false, nil // something still occupies the leaf path
		}
		if ch.Name == "" && (fileID == "" || ch.FileID == "") {
			// An unidentifiable child could be the node we are checking; absence is inconclusive.
			return false, false, fmt.Errorf("engine: parent listing of %q has an unidentifiable child; absence of %q inconclusive", parent, p)
		}
	}
	// The listing enumerated children and none of them is ours - but a listing is only evidence
	// of what it CONTAINS: `list` carries no page/total/end marker, so a paged or truncated reply
	// omits healthy nodes exactly like a deleted one. Corroborate with a
	// direct probe of the node's own path, which no pagination can hide, before the absence
	// becomes actionable.
	corroborated, perr := absenceCorroboratedByNodeProbe(ctx, pc, memo, viewID, p, fileID)
	if perr != nil {
		return false, false, perr
	}
	if !corroborated {
		// The node is still sitting at p: the listing that omitted it was incomplete. That is
		// the "leaf still occupied" outcome - neither gone nor moved - so the caller retries.
		return false, false, nil
	}
	return true, false, nil
}

// nodeProber is the optional QueryNode capability an absence prover uses to corroborate a
// listing-derived absence with a direct, path-addressed look at the node itself. The production
// action client implements it; a narrow leased client that does not simply skips the
// corroboration (the behaviour before the probe existed), like brokenReporter elsewhere.
type nodeProber interface {
	QueryNode(ctx context.Context, viewID int64, path string) (actions.Node, error)
}

// absenceCorroboratedByNodeProbe answers whether a parent listing that did not contain a node may
// be trusted as proof the node is absent from p.
//
// The listing is the only enumeration the protocol offers and it carries no completeness evidence
// whatsoever - no total, no page cursor, no end marker - so "the node is not in the reply" is
// indistinguishable from "the node is past the slice the server chose to send". The consumers of
// that answer are destructive or terminal (ack a remove the server may never have performed,
// dead-letter a live download, duplicate a create), which is exactly where an unproven absence
// must not be actionable.
//
// QueryNode addresses the node's own path directly, so no pagination can hide it:
//   - a node bearing our file id at p DISPROVES the absence (false, nil) - the listing was partial;
//   - the path-error sentinel CORROBORATES it, but only once absentNodeConfirmations consecutive
//     probes have agreed, spread over absenceStreakMinAge of wall clock as well
//     as over drain passes (passes carry no minimum spacing, so a count alone
//     is not a duration): errno 28672 conflates "no such path" with a
//     server-side timeout by this client's own contract, so a single one is the same evidence a
//     briefly overloaded DSM produces for a perfectly healthy node - and so are two answered by one
//     window of that overload. The first agreeing probe
//     reports inconclusive (false, err) and leaves the op queued; the first pass past the floor
//     settles it.
//     See absencestreak.go for the doctrine and the view-level twin it is carried down from;
//   - a reply describing some other node - a DIFFERENT non-empty id - is not our node, so the
//     listing's verdict stands (true, nil); the leaf-occupancy rules above already decided what
//     that means for this caller;
//   - a reply carrying NO id at all is no evidence either way (false, err): something answered for
//     p but nothing says whether it is our node, and a bare {} is never proof (the same rule the
//     id-less-child arms of confirmAbsentByParentList/nodeGoneByID and viewRootProbeConfirmsAbsent
//     apply). Corroborating an absence on it would hand the destructive consumers a verdict the
//     server never gave;
//   - any other probe failure is inconclusive (false, err): the caller leaves the op queued, the
//     same outcome an inconclusive listing produces.
//
// An ID-LESS caller (fileID == "") is corroborated too. It used to skip
// the probe entirely on the grounds that without an id nothing can tell OUR node from whatever
// occupies p - true, but it answers the wrong question: an id-less caller does not ask "is MY node
// still there", it asks "is this PATH still occupied at all" (the ancestor walk asks it of a
// directory whose id it never had; serverDivergedForVanishedUpload asks it of a fresh create that
// has no id yet). ANY node that answers for p - any id, any kind, even a bare {} - occupies it and
// refutes the absence with no identity needed, so the strongest form of the evidence is available
// precisely where the skip discarded it. Skipping it made every id-less verdict rest on the
// listing alone, and a single paged or truncated grandparent listing then became positive proof
// that a whole subtree is gone - a class never actionable on its own - acking
// a delete the server never performed.
func absenceCorroboratedByNodeProbe(ctx context.Context, pc outboundClient, memo absenceMemo, viewID int64, p, fileID string) (bool, error) {
	prober, ok := pc.(nodeProber)
	if !ok {
		return true, nil
	}
	n, err := prober.QueryNode(ctx, viewID, p)
	if !errors.Is(err, actions.ErrPathError) && memo != nil {
		// Whatever this probe answered, it was not the sentinel: the run of path-errors - the
		// only thing that distinguishes a real absence from a repeatedly timing-out server - is
		// broken, so the next sentinel starts a fresh streak.
		memo.clearAbsentProbe(viewID, p)
	}
	switch {
	case errors.Is(err, actions.ErrPathError):
		if memo == nil {
			// No memory to corroborate by repetition; the single probe's verdict stands (the
			// behaviour before repetition was required, and the same degradation a client that does
			// not implement nodeProber already gets).
			return true, nil
		}
		if seen := memo.noteAbsentProbe(viewID, p); seen < absentNodeConfirmations {
			return false, fmt.Errorf(
				"engine: probe of %q answered with the path-error sentinel %d time(s), fewer than the %d agreeing probes an absence needs (28672 also reports a server-side timeout); absence inconclusive",
				p, seen, absentNodeConfirmations)
		}
		// The sentinel repeated across drain passes: a timeout does not, a deleted node does.
		// The streak is deliberately NOT cleared here. The ancestor walk corroborates several
		// paths - each rung plus the node itself - and every one of them must be settled in the
		// SAME pass for the verdict to reach the caller; resetting a rung the moment it agrees
		// would make it alternate between "agreed" and "starting over" forever and the walk would
		// never converge. The resets that keep the evidence honest are the ones that cannot
		// alternate with the walk: a single answering probe (above), and markSynced recording
		// agreement at the path - a node this client just materialised there refutes every
		// remembered path-error for it, so a re-created path cannot have its NEXT absence
		// corroborated on one probe by the previous deletion's streak.
		return true, nil
	case err != nil:
		return false, fmt.Errorf("engine: probing %q to corroborate its absence failed; absence inconclusive: %w", p, err)
	case fileID == "":
		// The id-less question is occupancy, not identity: something answered for p, so p is not
		// empty and the listing that omitted it was incomplete.
		return false, nil
	case n.FileID == fileID:
		return false, nil
	case n.FileID == "":
		// A node answered for p but carried no id: it could be ours. Reading that as
		// corroboration would let a tolerant `file: {}` reply ack an unperformed remove or
		// dead-letter a live download on no evidence at all.
		return false, fmt.Errorf("engine: probe of %q carried no node id; the absence of %q is inconclusive", p, fileID)
	default:
		return true, nil
	}
}

// nodeGoneByID reports whether the node identified by fileID (non-empty by contract) at the
// view-relative path p is positively gone server-side, by listing its parent directory and
// matching STRICTLY by file id. It deliberately differs from
// confirmAbsentByParentList/resolveServerNodeAbsence in one rule: a child merely OCCUPYING p's
// leaf name under a DIFFERENT non-empty id does NOT read as presence. Those classifiers answer
// "is this PATH still occupied" (the remove verb's postcondition); this one answers "does this
// NODE still exist" - its motivating caller is an inbound download whose path an overwriting
// move claimed for another node, where the leaf is occupied precisely BECAUSE the downloaded
// node was destroyed, so the occupancy rule would read the wedge as "present" forever.
//
// Only positive proof returns true: a child bearing fileID (under any name - a node renamed in
// place stays visible by id) reports present, an id-less child could hide the node so the
// listing is inconclusive (an error, mirroring the unidentifiable-child rule of the siblings),
// and a listing failure is inconclusive unless the ancestor walk positively confirms the parent
// directory itself gone (a node beneath a gone directory is gone with it - leaf-name-blind, see
// ancestorConfirmsAbsent's audit note for why that conflation is data-safe for this consumer).
func nodeGoneByID(ctx context.Context, pc outboundClient, memo absenceMemo, viewID int64, p, fileID string) (bool, error) {
	parent := path.Dir(p)
	if parent == "." || parent == "/" {
		parent = viewRootPath
	}
	children, listed, err := pc.List(ctx, viewID, parent)
	if err != nil {
		if ancestorConfirmsAbsent(ctx, pc, memo, viewID, parent, err) {
			return true, nil
		}
		return false, err
	}
	if !listed {
		// No children array in the reply is no enumeration, hence no proof of absence - the
		// sibling rule of resolveServerNodeAbsence.
		return false, fmt.Errorf("engine: parent listing of %q carried no listing; absence of node %q inconclusive", parent, fileID)
	}
	for _, ch := range children {
		if ch.FileID == fileID {
			return false, nil // the node is still present (possibly renamed in place)
		}
		if ch.FileID == "" {
			// An id-less child could be the very node we are checking; absence cannot be
			// proven past it.
			return false, fmt.Errorf("engine: parent listing of %q has an id-less child; absence of node %q inconclusive", parent, fileID)
		}
	}
	// A listing proves only what it contains (see absenceCorroboratedByNodeProbe): corroborate
	// with a direct probe of the node's path before reporting it destroyed, so a paged or
	// truncated listing cannot dead-letter a live node.
	corroborated, perr := absenceCorroboratedByNodeProbe(ctx, pc, memo, viewID, p, fileID)
	if perr != nil {
		return false, perr
	}
	return corroborated, nil
}

// ancestorConfirmsAbsent reports whether a failed List(parent) can be turned into POSITIVE proof
// of absence by confirming the parent directory itself is gone. An ErrPathError from List(parent)
// conflates "parent gone" with a server-side timeout, and a server delete of a whole directory
// produces exactly that for every tracked descendant, so without this walk such ops would stay
// queued as inconclusive forever. A parent confirmed absent (by confirmAbsentByParentList's rules
// against ITS parent's listing) proves every path beneath it absent.
//
// The recursion strips one component per step, so its depth is bounded by the path's; the view
// root terminates it (the root is never absent, so false is returned and the caller surfaces the
// original error to retry). The parent's file id is unknown here, so it matches by leaf name only:
// any nameless child is inconclusive, and the verdict is corroborated by a direct QueryNode of the
// ANCESTOR's path - any node answering there refutes the absence, so a paged grandparent listing
// cannot promote a live directory (and its subtree) to "gone". A non-ErrPathError listing failure
// is never evidence and reports false.
//
// LEAF-NAME BLINDNESS. Matching by name cannot tell a DELETED ancestor from a RENAMED one (after a
// server rename a/b -> a/x, "a" lists without "b", so every path under a/b reads absent). Each
// caller must be safe under that conflation:
//   - applyRemoveOut cross-checks the file id against the store (PathByID) BEFORE acking: a node
//     re-indexed elsewhere is moved, so the remove is re-emitted on the new path, not acked.
//   - resolveServerNodeAbsence (keep-both resolver) folds ancestor-gone to gone DELIBERATELY: it
//     dead-letters instead of wedging ahead of the authoritative inbound remove queued behind it,
//     and dead-lettering preserves rather than deletes.
//   - serverDivergedForVanishedUpload reads ancestor-gone as "not diverged" (a local delete to
//     propagate): the pending baseline is reverted and the next scan re-emits, so a renamed parent
//     at worst defers a push.
//   - the CREATE verbs (applyUploadOut/applyMkdirOut via parentGoneConfirmed): after a server turn
//     that addressed the parent by id (resolveParentID; a rename preserves the id) an ErrPathError
//     genuinely means deleted. With the parent baseline ABSENT (errParentBaselineAbsent) an id-less
//     create runs the walk and a renamed ancestor DOES read gone - accepted as data-safe:
//     dropGoneParentBaseline re-bases the pending baseline through the queued inbound renames, the
//     content stays on disk, and the next scan re-uploads it under the live parent (at worst a
//     duplicate). An id-carrying modify never gets here: its byid entry proves the relocation
//     locally (uploadNodeRelocatedError).
//   - the MOVE verb (applyRenameOut via parentGoneConfirmed) is the EXCEPTION: pc.Rename addresses
//     both endpoints BY PATH without resolveParentID, so a server rename of the source's parent
//     reads as gone. Accepted as data-safe: dead-lettering drops the NewPath baseline
//     (dropGoneParentBaseline), the file becomes untracked and is re-uploaded at the destination
//     (at worst a duplicate keep-both tolerates). PathByID cannot help (the local move already
//     relocated the baseline) and no id-addressed query exists; staying transient would wedge the
//     queue AND the inbound OpRename serialized behind it.
//   - renameSourceNodeNotAtPath (the MOVE verb's source-node check) reaches the walk only when the
//     source parent's listing errors after both parent probes read alive (a race), and resolves in
//     the same data-safe direction as the MOVE verb.
//   - nodeGoneByID (the inbound download's node-gone probe) folds ancestor-gone to "node gone": the
//     download is dropped and its phantom pending baseline cleaned (dropStrandedBaseline). Nothing
//     local exists yet, so a renamed ancestor only defers materialisation until the node's next
//     server event or the cold-start pull. Reaching it needs the id-addressed download turn and the
//     parent listing to fail while the grandparent lists cleanly without the parent.
func ancestorConfirmsAbsent(ctx context.Context, pc outboundClient, memo absenceMemo, viewID int64, parent string, lerr error) bool {
	if !errors.Is(lerr, actions.ErrPathError) || parent == viewRootPath {
		return false
	}
	gone, err := confirmAbsentByParentList(ctx, pc, memo, viewID, parent, "")
	return err == nil && gone
}

// parentGoneConfirmed leases a worker and reports whether the PARENT directory of the
// view-relative path p is unambiguously gone server-side. It is the create/move-verb
// counterpart of the ancestor-walk exit applyRemoveOut already has: an
// outbound Upload/Mkdir/Rename under a directory the server has deleted fails with
// ErrPathError forever, and - unlike the remove verb - had no way to turn that
// never-provable error into positive proof of the parent's absence, so the op wedged the
// queue (and, drained ahead of the inbound OpRemove, the authoritative remove behind it)
// forever.
//
// A top-level entry's parent is the view root, which is never gone, so that returns false
// (leave the op queued for the normal errParentUnknown retry). Otherwise it runs the same
// ancestorConfirmsAbsent walk the remove verb uses, feeding it ErrPathError as the
// access error (the caller reached here BECAUSE its own turn against the parent - an
// Upload to the parent id, a Mkdir under it, a Rename addressing a path beneath it - came
// back ErrPathError). Only positive proof returns true; a leased-worker/listing failure or
// a still-present parent reports false so the caller keeps the op queued rather than
// dropping it on a transient blip.
func (e *Engine) parentGoneConfirmed(ctx context.Context, viewID uint64, p string) bool {
	parent := path.Dir(p)
	if parent == "." || parent == "/" || parent == "" {
		return false
	}
	vid := int64(viewID) // #nosec G115 -- a view_id is a non-negative identifier
	var gone bool
	err := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		gone = ancestorConfirmsAbsent(ctx, pc, e, vid, parent, actions.ErrPathError)
		return nil
	})
	return err == nil && gone
}

// errServerTypeCollision marks an outbound create (OpMkdir/OpUpload) whose ErrPathError
// probe positively found a server node of the OPPOSITE kind - with a non-empty id - at the
// op's path: a file<->dir create/create collision, where another
// client created a file at the path this client created a directory (or vice versa). The
// create can never land as-is, and nothing else in applyOutbound's matrix matches its bare
// ErrPathError (the parent is alive, so parentGoneConfirmed reads false), so pre-fix the op
// retried forever - the same divergence the cold start funnels via planColdConflict left
// the incremental path with no terminal exit. applyOutbound funnels it into the keep-both
// resolver (funnelTypeCollision), mirroring the 24579 stale-base branch: a local-dir-vs-
// server-file mkdir resolves fully (server file wins on the canonical path, the local
// directory is set aside whole as a _Conflict copy the next scan re-uploads), and a
// local-file-vs-server-dir upload dead-letters observably through the resolver's
// errConflictServerDir doctrine (StateConflict + Dropped) instead of retrying invisibly.
var errServerTypeCollision = errors.New("engine: outbound create collides with a server node of the opposite type")

// serverDirOccupiesPath leases a worker and reports whether a DIRECTORY node with a
// non-empty id now occupies the view-relative path p server-side - the upload half of the
// type-collision probe (the mkdir half runs inline on the primary lease,
// which already holds the QueryNode reply). Only POSITIVE proof returns true: a probe
// failure (including the ErrPathError a deleted parent produces), a non-directory node, or
// a directory reported without an id all report false so the caller keeps the op queued -
// a transient blip must never be escalated into a terminal keep-both funnel.
func (e *Engine) serverDirOccupiesPath(ctx context.Context, viewID uint64, p string) bool {
	vid := int64(viewID) // #nosec G115 -- a view_id is a non-negative identifier
	var isDir bool
	err := e.withWorker(ctx, func(c syncClient) error {
		existing, qerr := c.QueryNode(ctx, vid, p)
		if qerr != nil {
			return qerr
		}
		isDir = existing.IsDir && existing.FileID != ""
		return nil
	})
	return err == nil && isDir
}
