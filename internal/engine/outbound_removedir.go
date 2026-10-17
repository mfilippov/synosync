package engine

// Outbound DIRECTORY delete precondition (two-client finding H7, measured 14.08.2026;
// rationale in docs/INVARIANTS.md, "recursive remove").
//
// A live DSM's `remove` of a directory is RECURSIVE: it succeeds on a NON-empty directory and
// takes the whole subtree with it (TestIntegration_RemoveDirectorySemantics). The client assumed
// the opposite - that an unknown child would make the delete fail - so a node another client
// added after this client last looked into the directory was invisible to every never-lose gate
// and was destroyed on the server AND in every replica, with no _Conflict copy, no dead-letter
// and an empty queue. The measured two-client scenario (…DeleteDirMirror) lost the peer's file in
// 2.5 seconds.
//
// The emptiness a directory delete relies on therefore has to be proved against the SERVER, not
// against the baseline: before the destructive turn the server subtree is enumerated with `list`
// and compared with the baselines under the same prefix. A child the base never recorded, or one
// it recorded with different content, means the local delete raced a remote change - DESIGN §6's
// conflict, not a delete - so the op is refused and funnelled into the same StateConflict +
// durable-trace resolution the stale-base (24579) rejection of a non-upload push takes.
//
// Everything the verification cannot PROVE leaves the delete unperformed: a listing that carried
// no enumeration (`listed == false`) is not proof of emptiness, and neither is a failed or
// unidentifiable one - those keep the op queued as an ordinary transient, exactly like every
// other absence prover in outbound_mutate.go.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// errRemoveDirUnverified marks an outbound directory delete whose server subtree could NOT be
// enumerated well enough to prove it holds only content this client last agreed on: the listing
// failed, carried no enumeration at all, or named a child nothing can identify. It is transient
// by design - the op stays queued and the destructive turn never runs - because the alternative
// (deleting on unproven emptiness) is exactly the recursive wipe this precondition exists to
// prevent.
var errRemoveDirUnverified = errors.New("engine: outbound directory remove could not verify the server subtree")

// errRemoveDirLocallyPresent marks an outbound OpRemove of a DIRECTORY whose path holds a
// directory on disk AGAIN by the time the op drains, which makes the local deletion the op was
// derived from no longer the truth the destructive turn would act on.
//
// The queued-inbound discriminator below rests on an ordering that holds only one way round.
// When the local delete is scanned first (the measured H7 shape - a returning client) the
// directory's remove does hold the lower seq and the peer's child download is still owed. The
// reverse interleaving is reachable too: a peer's create is polled and its OpDownload enqueued,
// THEN a localScan observes the local delete and enqueues the remove at a higher seq. The two
// serialise on overlapping paths, so the download applies FIRST - recreating the directory and
// markSynced-ing the child - and by verification time that child is a synced baseline with a
// matching server hash and no longer owed. The enumeration would then pass and the recursive
// remove would destroy the peer's node on the server and in its replica: finding H7 again,
// through the one door the queue evidence cannot see.
//
// The directory standing on disk is the local, evidence-based proof of exactly that state, and it
// costs one Lstat before the lease. It is not a failure and not a divergence: applyOutbound
// reverts the pending baseline and acks, so the next scan re-diffs the tree as it now IS - a
// delete that is still wanted is re-emitted, and children the user really did remove propagate
// through their own removes, each with its own precondition.
var errRemoveDirLocallyPresent = errors.New("engine: outbound directory remove is stale; the directory is on disk again")

// removeDirLocallyPresent reports whether op.Path holds a DIRECTORY on disk, which disproves the
// local deletion an outbound directory remove was derived from (see errRemoveDirLocallyPresent).
//
// It answers false for everything it cannot read as a live directory: a path e.abs refuses (one
// that escapes the root or sits behind a symlink) and an Lstat that does not answer both keep the
// historical flow, since neither is evidence the directory came back - the server-side
// preconditions remain the arbiter there. A non-directory occupant answers false on purpose:
// detectRemoves deliberately emits an OpRemove for a path whose on-disk TYPE changed (a file now
// standing where a tracked directory was), and that delete must still run so the replacement can
// be created. The NFC path is resolved to its real on-disk spelling first, mirroring applyRemove.
func (e *Engine) removeDirLocallyPresent(op store.Op) bool {
	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return false
	}
	fi, serr := os.Lstat(e.onDiskAbs(abs))
	return serr == nil && fi.IsDir()
}

// removeDirDivergedError marks an outbound OpRemove of a DIRECTORY whose server subtree holds a
// node this client never agreed on: one the baseline does not know at all (another client created
// it while this client was away), one whose identity changed, or one whose content the server
// accepted after the local delete was scanned. Because the server's `remove` is recursive, going
// ahead would destroy that node everywhere with no keep-both copy - the never-lose violation
// finding H7 measured. It carries the offending child's path and a human reason so applyOutbound
// can record both in the dead-letter trace.
type removeDirDivergedError struct {
	child  string
	reason string
}

func (e *removeDirDivergedError) Error() string {
	return fmt.Sprintf("engine: outbound remove of a directory refused: %s (%q)", e.reason, e.child)
}

// removeTargetFacts reads, from ONE decode of the record at op.Path, what an outbound OpRemove's
// preconditions need: the last-synced server hash the pre-turn probe compares against, and whether
// the node was recorded as a DIRECTORY (which arms the recursive-delete subtree precondition).
// planRemove flips the vanished baseline to StatePending without touching either field. One read,
// not two, so the absent/corrupt/backend triage cannot drift apart (as with applyRemove's single
// GetBaseline).
//
// The kind has three witnesses the caller ORs: this record, op.BaseIsDir (planRemove's durable
// copy, localreconcile.go, which survives a record lost between the Enqueue and the flip), and the
// pre-turn probe's is_dir - so a lost record cannot silently disarm the precondition.
//
// The base falls back from ServerHash to LocalHash: both are MD4 of the agreed bytes, but a DSM
// that omits the hash from its upload reply leaves ServerHash empty (applyUploadOut assigns
// node.Hash verbatim) while markSynced always derives LocalHash. Without the fallback the
// precondition would be inert for every locally pushed file on such a server; a false divergence
// costs only a download recovery, never data.
//
// Outcomes:
//   - An absent record (the crash window between Enqueue and flip, or a teardown elsewhere) yields
//     an empty base, so the probe falls back to node identity (same id → the delete proceeds, as
//     in serverDivergedForVanishedUpload), and "not a directory" as the ABSENCE of an answer.
//   - A CORRUPT record yields an empty base and kindUnknown, NOT a refusal. Reading it as "not a
//     directory" would fail toward deleting a bit-rotted directory with nothing proved about its
//     subtree (verifyServerSubtreeAgrees refuses on a corrupt record beneath it for the same
//     reason). The refusal belongs to the CALLER, once the other two witnesses fail to place the
//     kind: refusing here would block every remove, files included, and errRemoveDirUnverified is
//     terminal by evidence (removeDirUnverifiedCode is not a serverTransientRejection), so after
//     unmappedServerErrDrains/unmappedServerErrMinAge markOutboundConflict would freeze the path
//     in StateConflict for good. A file delete has no recursive hazard and must not pay that.
//   - kindPlaced is true only for a record that DECODED; it tells the caller whether "not a
//     directory" is evidence or no answer, and so whether the pre-turn probe (the third witness)
//     is worth a turn. Reading an absent record as a placed FILE would skip the probe for an
//     ID-LESS op, so an op predating the BaseIsDir stamp on a DSM that omits file_id would reach
//     the RECURSIVE Remove with no witness consulted.
//   - Only a backend failure propagates - a destructive delete is never decided from an unread
//     store.
func (e *Engine) removeTargetFacts(op store.Op) (base string, isDir, kindUnknown, kindPlaced bool, err error) {
	b, gerr := e.store.GetBaseline(op.ViewID, op.Path)
	switch {
	case isNotFound(gerr):
		return "", false, false, false, nil
	case errors.Is(gerr, store.ErrCorruptRecord):
		e.warnCorruptBaseline("remove-target", op.ViewID, op.Path, gerr)
		return "", false, true, false, nil
	case gerr != nil:
		return "", false, false, false,
			fmt.Errorf("engine: load remove target facts %d/%q: %w", op.ViewID, op.Path, gerr)
	}
	isDir = b.FileType == fileType(true)
	// A record that decoded has not thereby ANSWERED. FileType is unset on a routine shape -
	// applyRename writes store.Baseline{FileID: …} with no FileType and forces StateSynced on its
	// overwrite/no-facts arms, which is the same "an UNSET FileType states nothing" the child arms
	// of verifyServerSubtreeAgrees below already spell out. Reporting that record as kindPlaced
	// read "decoded" as "said file": isDir was false, planRemove's BaseIsDir was false for the same
	// reason (it reads the same field), so targetIsDir was false, needKindProbe was false, and for
	// an id-less op no probe ran at all. All three witnesses then went unconsulted, the unplaced-
	// target refusal was disarmed by kindPlaced itself, and the RECURSIVE Remove went out with
	// nothing proved about the subtree - finding H7, through the one door this flag exists to shut.
	// Only a POSITIVELY set kind places the target; anything else buys the path-addressed probe.
	placed := b.FileType == fileType(true) || b.FileType == fileType(false)
	if op.FileID == "" {
		return "", isDir, false, placed, nil // an id-less remove needs no divergence base
	}
	if b.ServerHash != "" {
		return b.ServerHash, isDir, false, placed, nil
	}
	return b.LocalHash, isDir, false, placed, nil
}

// removeDirVerifyCap bounds the subtree enumeration unconditionally, independently of the opt-in
// limits.ColdStartCatalogCap it also honours.
//
// The cold-start pull can afford to ship uncapped: it runs ONCE per view, off the critical path.
// This walk runs inline on a destructive op, holds a worker lease for its whole duration (with
// limits.op_turn_timeout_s shipped disabled, nothing else bounds it), and re-runs on every
// attempt of the op - so "unlimited" here means one `list` turn per subdirectory of an arbitrarily
// large tree, per drain, which is the "bounded work" convention's exact counter-example. The
// ceiling is far above any directory a delete plausibly targets, so hitting it means the subtree
// is too large to prove agreed in one turn - reported as divergence, since an unverifiable subtree
// must never be deleted.
const removeDirVerifyCap = 50_000

// safeChildName reports whether a name a `list` reply gives for a child is one plain path
// component the walk may join onto its parent. A reply is untrusted input: "." and ".." resolve
// back onto directories the walk has already enumerated (path.Join CLEANS its result), a name
// carrying a separator addresses somewhere else entirely, and an empty one identifies nothing.
// None of them can be read as a child, and the caller refuses the delete rather than guessing.
//
// The separator is "/" and only "/". A backslash is an ORDINARY rune in a name here: the wire is
// slash-separated, the walk joins with path.Join and compares against normpath.Key'd store paths,
// and nothing on this path touches the local filesystem. Refusing it was therefore not a safety
// margin but a liveness hole - a POSIX client can hold `dir/a\b`, and the directory above any such
// name could never be deleted again: every attempt returned errRemoveDirUnverified, got paced, and
// finally dead-lettered the delete with the record left StateConflict, for a child the client had
// itself synced.
func safeChildName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}

// serverListPath renders a view-relative path for the `list` verb, mapping the view root's empty
// / dotted spellings onto viewRootPath exactly as the absence provers in outbound_mutate.go do.
func serverListPath(p string) string {
	if p == "" || p == "." || p == "/" {
		return viewRootPath
	}
	return p
}

// verifyServerSubtreeAgrees proves, against the SERVER, that every node beneath the directory an
// outbound OpRemove addresses is content this client last agreed on - the precondition the
// recursive `remove` verb needs and the baseline alone cannot supply (finding H7).
//
// It walks the subtree breadth-first with `list`, matching each child against the baselines under
// the same prefix (fold-keyed, like every other path match in the engine):
//   - a child no baseline records is a node another client added → divergence;
//   - a tracked path now held by a DIFFERENT server node id is a takeover → divergence;
//   - a tracked FILE whose server hash differs from the agreed one is a peer's edit → divergence
//     (the same comparison and ServerHash→LocalHash fallback as removeTargetFacts); a hash is
//     trusted only when BOTH are present, since Node.Hash is tolerantly decoded and an empty base
//     is legitimate;
//   - a tracked FILE the server now reports as a DIRECTORY is a peer's replacement → divergence,
//     never a descend (an empty replacement directory would enumerate nothing and pass);
//   - a directory (by the server's report or the baseline's kind - a sparse reply omits is_dir)
//     is descended into.
//
// A baseline alone is NOT proof of agreement: reconcile records a StatePending baseline with the
// event's FileID and ServerHash for every queued, unapplied inbound create, so a peer's new node
// matches identity and hash. A path a queued INBOUND op still owes is therefore divergence: the
// delete is refused with a trace and the peer's file materialises normally. That covers the
// measured H7 order (local delete scanned first, so the child's op is still owed); the reverse
// order, where the child download already applied, is owned by the caller's
// errRemoveDirLocallyPresent guard, on local evidence, before the lease.
//
// The walk honours limits.ColdStartCatalogCap but is never uncapped, since it runs inline on
// every delete attempt: the shipped cap is 0 ("unlimited", and applyDefaults does not back-fill
// it), so removeDirVerifyCap is a default install's ceiling. Hitting either is divergence, not a
// transient - an unverifiable subtree must not be deleted, and a retry would re-hit the cap.
//
// Child names come off the wire and path.Join CLEANS, so a "." or ".." child would re-queue an
// enumerated directory and spin forever on a held worker lease; safeChildName refuses anything
// but one plain component, and the visited set makes any other repeat terminate.
//
// DECLARED RESIDUAL (DESIGN §13): a successful enumeration is trusted to be COMPLETE. Every other
// input fails toward not deleting, but a well-formed SHORT page reads as agreement and the
// recursive remove takes the omitted child. The absence provers are pagination-proof because a
// path-addressed QueryNode names one node; "nothing ELSE is here" has no such probe, so closing
// it needs a server cursor/count (not measured) or two agreeing walks in the absencestreak.go
// shape, at the cost of a second full walk per directory delete.
//
// Returns nil when the subtree is provably agreed, including one the server no longer holds: an
// ErrPathError listing of the ROOT enumerates nothing once a repeated path-addressed probe of the
// directory corroborates the absence (the destructive turn's postcondition then arbitrates); an
// uncorroborated one is the ordinary transient.
func (e *Engine) verifyServerSubtreeAgrees(ctx context.Context, pc outboundClient, viewID int64, op store.Op) error {
	// One number bounds BOTH halves of the verification. The server walk's own ceiling was
	// always here; the local snapshot below was not, so a directory whose baseline subtree is far
	// larger than the walk budget paid the full O(subtree) allocation - on a destructive op, under
	// a worker lease, on every attempt - before the budget could refuse anything. The two are
	// bounded by the same figure because they measure the same tree: a baseline set past the cap
	// implies a server subtree past it too, so the refusal the walk would reach anyway is simply
	// reached without the allocation.
	budget := removeDirVerifyCap
	if c := e.limits.ColdStartCatalogCap; c > 0 && c < budget {
		budget = c
	}
	known, skipped, overCap, err := e.subtreeBaselines(op.ViewID, op.Path, budget)
	if err != nil {
		return err
	}
	if overCap {
		return &removeDirDivergedError{
			child: op.Path,
			reason: fmt.Sprintf("the directory's own baseline subtree exceeds the %d-node verification cap, "+
				"so it cannot be proved agreed in one pass", budget),
		}
	}
	if len(skipped) > 0 {
		// A record under the directory no longer decodes, so nothing can say whether the server
		// node it describes is agreed content. Refuse with a trace rather than delete blind.
		return &removeDirDivergedError{
			child:  skipped[0],
			reason: "a baseline record under the directory is corrupt, so its server node cannot be proved agreed",
		}
	}
	// AFTER the refusals above, deliberately: this scan streams and decodes the WHOLE durable
	// queue with no early exit (its answer is a set membership test the walk below makes per
	// child, so there is nothing to stop early on), and paying that on a destructive op under a
	// worker lease for a subtree the cap has already refused is the "bounded work" convention's
	// counter-example a second time.
	owed, err := e.queuedInboundUnderPrefix(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	seen := 0
	visited := map[string]struct{}{normpath.Key(op.Path): {}}
	queue := []string{op.Path}
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		children, listed, lerr := pc.List(ctx, viewID, serverListPath(dir))
		switch {
		case errors.Is(lerr, actions.ErrPathError) && dir == op.Path:
			// The ROOT of the walk answered with the sentinel that conflates "no such path" with
			// a server-side timeout. Reading it as "nothing here to enumerate" used to fall
			// straight through to the RECURSIVE Remove below with nothing proved about the
			// subtree - and the pre-turn QueryNode in applyRemoveOut may have answered POSITIVELY
			// for this very path moments earlier, a contradiction this arm silently resolved in
			// favour of deleting. One timed-out listing was therefore enough to wipe a peer's node
			// from the server and from both replicas, which is finding H7 through the one door
			// this precondition left open (the DESCENDANT arm below already refuses exactly this
			// input, for exactly this reason).
			//
			// So the root's absence is corroborated the way every other absence verdict in the
			// engine is: a path-addressed probe of the directory itself, asked as the OCCUPANCY
			// question (an empty file id - ANY node answering for the path refutes the absence,
			// whoever owns it), repeated across drain passes and spaced in wall clock by
			// absencestreak.go. Only a corroborated absence enumerates nothing; anything else is
			// the ordinary transient that leaves the op queued. Liveness is unaffected for a
			// directory the server really has deleted: absentNodeConfirmations agreeing probes
			// absenceStreakMinAge apart settle well inside the dead-letter window this exit is
			// paced by (removeDirUnverifiedCode), after which the Remove's own ErrPathError
			// postcondition acks the op idempotently as before.
			//
			// The corroboration is REQUIRED here, not optional. absenceCorroboratedByNodeProbe
			// answers true for a client that does not implement nodeProber - a deliberate
			// degradation for its other callers, where it only falls back to the listing-derived
			// verdict they already had. On THIS arm that default is the opposite of the whole
			// point: it turns one ErrPathError listing straight back into "the subtree is provably
			// empty" and lets the recursive Remove run with nothing proved, which is the door the
			// arm exists to close. So the capability is checked first and its absence is the
			// ordinary transient, mirroring errNoOutbound's surface-rather-than-assume rule.
			if _, canProbe := pc.(nodeProber); !canProbe {
				return fmt.Errorf("%w: listing of %d/%q reported the path-error sentinel and this client cannot probe the path to corroborate the absence",
					errRemoveDirUnverified, op.ViewID, dir)
			}
			gone, perr := absenceCorroboratedByNodeProbe(ctx, pc, e, viewID, op.Path, "")
			if perr != nil {
				return fmt.Errorf("%w: corroborating the absence of %d/%q: %w",
					errRemoveDirUnverified, op.ViewID, dir, perr)
			}
			if !gone {
				return fmt.Errorf("%w: listing of %d/%q reported the path-error sentinel while a node still answers for the path, so its subtree is unproved",
					errRemoveDirUnverified, op.ViewID, dir)
			}
			continue
		case lerr != nil:
			// A DESCENDANT that the parent's listing just enumerated as present, whose own
			// listing then fails, is unverifiable - including on ErrPathError, which the
			// codebase treats as "absent OR a server-side timeout" everywhere else and
			// corroborates by repetition before acting on it. Reading one such answer as
			// "this directory holds nothing" would authorise the recursive wipe of a whole
			// subtree from a single timed-out probe, which is exactly the loss this
			// precondition exists to prevent, so it stays transient and the op stays queued.
			return fmt.Errorf("%w: listing %d/%q: %w", errRemoveDirUnverified, op.ViewID, dir, lerr)
		case !listed:
			// A successful turn that enumerated nothing is no evidence at all: reading its
			// zero-length slice as "empty" is precisely how an unproven emptiness would authorise
			// the recursive wipe (the listing-evidence rule, applied to the destructive side).
			return fmt.Errorf("%w: listing of %d/%q carried no enumeration, which is not proof it is empty",
				errRemoveDirUnverified, op.ViewID, dir)
		}
		for _, ch := range children {
			seen++
			if seen > budget {
				return &removeDirDivergedError{
					child:  dir,
					reason: fmt.Sprintf("the server subtree exceeds the %d-node verification cap", budget),
				}
			}
			if !safeChildName(ch.Name) {
				return fmt.Errorf("%w: listing of %d/%q names an unidentifiable child %q",
					errRemoveDirUnverified, op.ViewID, dir, ch.Name)
			}
			cp := path.Join(dir, ch.Name)
			ck := normpath.Key(cp)
			if _, awaited := owed[ck]; awaited {
				return &removeDirDivergedError{
					child:  cp,
					reason: "a queued inbound op still owes this node, so the baseline under the directory records a version this client has never held",
				}
			}
			b, tracked := known[ck]
			if !tracked {
				return &removeDirDivergedError{
					child:  cp,
					reason: "the server holds a node the baseline never recorded (another client created it after this client last saw the directory)",
				}
			}
			if b.FileID != "" && ch.FileID != "" && b.FileID != ch.FileID {
				return &removeDirDivergedError{
					child:  cp,
					reason: "a different server node now holds a tracked path",
				}
			}
			if ch.IsDir && b.FileType == fileType(false) {
				// The evidence is the POSITIVE type, exactly as the mirror arm below reads its
				// own: an UNSET FileType states nothing, and reading it as "the base recorded a
				// file" refused a tracked child DIRECTORY that merely carries a bare record. Bare
				// records are routine - applyRename writes store.Baseline{FileID: …} with no
				// FileType and forces StateSynced on its overwrite/no-facts arms - and the refusal
				// is terminal on the first occurrence (applyOutbound dead-letters
				// removeDirDivergedError), so it abandoned the user's deletion of every ancestor
				// above such a child for good, leaving a StateConflict record at a path with no
				// local directory that localReconcile never drives again. An unset type now falls
				// through to the descend arm, which VERIFIES the subtree instead of refusing it.
				//
				// The server holds a DIRECTORY where the base recorded a file: a peer replaced
				// the node. Descending instead would prove nothing - an EMPTY replacement
				// directory enumerates no children, the whole verification returns nil, and the
				// recursive remove destroys the peer's node everywhere, which is finding H7 for a
				// kind the base does know about.
				return &removeDirDivergedError{
					child:  cp,
					reason: "the server holds a directory where the baseline recorded a file (another client replaced it)",
				}
			}
			if !ch.IsDir && ch.Hash != "" && b.FileType == fileType(true) {
				// The MIRROR mismatch: the base recorded a directory and the server now holds a
				// file. The is_dir flag cannot decide it - a sparse reply simply omits the key, so
				// !ch.IsDir is no evidence - and the descend arm below would otherwise rest the
				// whole never-lose guarantee on an UNMEASURED premise: that `list` of a file
				// errors. Nothing on the stand pins that (docs/INTEGRATION.md settles the shape of
				// a listing of an EMPTY DIRECTORY, node_list:[] with listed==true, not of a file),
				// and a server that answered a file the same way would enumerate zero children,
				// return the verification nil, and let the recursive remove destroy the peer's
				// replacement - finding H7 again, through the one direction the flag cannot see.
				//
				// A content hash IS decidable evidence, and it is measured: query_node, upload and
				// pull_event all carry a file's hash and "a DIRECTORY carries none"
				// (docs/INTEGRATION.md). So a non-empty hash on a child the
				// base records as a directory proves a file stands there now. A hash-less reply
				// still descends and still depends on the safe exit, which is why this is a
				// refusal keyed on positive evidence rather than a relaxation of it.
				//
				// A POSITIVE is_dir excludes the arm outright, for the same reason the sibling
				// above trusts it: a flag the server cannot echo back from the request is proof
				// when set, and the arm above already consumed every base=file shape, so a child
				// arriving here with is_dir true is a directory the base AGREES is one. Without
				// the exclusion, a server that decorated a folder listing with any hash-shaped
				// value would refuse every directory delete with a subdirectory in it -
				// terminally (dead-letter, no retry), which abandons the user's deletion for
				// good on evidence of nothing.
				return &removeDirDivergedError{
					child:  cp,
					reason: "the server holds a file where the baseline recorded a directory (another client replaced it)",
				}
			}
			// The third disjunct covers the child NO witness places at all: a sparse listing
			// (is_dir omitted) of a child whose baseline is bare (FileType unset, the routine
			// applyRename shape) and whose entry carries no content hash. It matched none of the
			// divergence arms above, is not the base=dir descend case, and the hash compare below
			// short-circuits on an empty ch.Hash - so it was reported AGREED without a single fact
			// about it ever being read. A directory holding a peer's file waved through here is
			// destroyed by the recursive remove in both replicas, which is finding H7 again.
			// Descending rather than refusing is the same choice the base=file arm above makes for
			// an unset type ("VERIFIES the subtree instead of refusing it"), and it is never worse
			// than the fall-through it replaces: a hash-less entry is what a DIRECTORY listing
			// looks like (docs/INTEGRATION.md - a file carries a hash, a directory carries none),
			// so the ambiguous child is enumerated when it is one, and a server that answers a
			// FILE listing like an empty directory lands exactly where the old code already stood.
			if ch.IsDir || b.FileType == fileType(true) || (b.FileType == "" && ch.Hash == "") {
				// Descend once per directory. Two listings naming the same fold key - a server
				// that reports a child twice, or two spellings the host folds together - would
				// otherwise re-enumerate it, and a cycle among them would never terminate.
				if _, been := visited[ck]; !been {
					visited[ck] = struct{}{}
					queue = append(queue, cp)
				}
				continue
			}
			base := b.ServerHash
			if base == "" {
				base = b.LocalHash
			}
			if ch.Hash != "" && base != "" && ch.Hash != base {
				return &removeDirDivergedError{
					child:  cp,
					reason: "the server holds content that differs from the last agreed version (another client modified it)",
				}
			}
		}
	}
	return nil
}

// queuedInboundUnderPrefix collects the fold keys of every path a queued INBOUND op targets at
// or beneath dir - both endpoints of a rename, since either can be the one the server still
// lists. It is the evidence that separates a baseline recording AGREED content from one
// reconcile pre-wrote for work the client has not done: the latter always has an op behind it
// (the "enqueue before baseline mutation" rule), and that op is still in the queue when the
// directory's own remove runs, because the two serialise on overlapping paths and the remove
// holds the lower seq.
//
// A scan failure propagates so the caller leaves the delete unperformed rather than deciding
// from an unread queue, exactly like every other unprovable input here.
func (e *Engine) queuedInboundUnderPrefix(viewID uint64, dir string) (map[string]struct{}, error) {
	owed := make(map[string]struct{})
	note := func(p string) {
		if p != "" && underPrefix(dir, p) {
			owed[normpath.Key(p)] = struct{}{}
		}
	}
	if err := e.scanQueuedInView(viewID, "inbound ops under a directory", func(qo store.QueuedOp) error {
		if qo.Direction != store.DirInbound {
			return nil
		}
		note(qo.Path)
		note(qo.NewPath)
		return nil
	}); err != nil {
		return nil, err
	}
	return owed, nil
}

// underPrefix reports whether p lies at or strictly beneath dir, comparing fold keys so an NFD
// spelling or a case difference matches the way every other path comparison in the engine does.
func underPrefix(dir, p string) bool {
	dk, pk := normpath.Key(dir), normpath.Key(p)
	if dk == "" || dk == "." {
		return true // the view root contains everything
	}
	return keyAtOrUnder(pk, dk)
}

// errSubtreeBaselineBudget stops the baseline stream once it is provably past the verification
// cap. It never escapes subtreeBaselines: the over-cap fact is reported as the third result, and
// the caller renders it as a divergence refusal exactly as the server-side walk renders its own
// cap.
var errSubtreeBaselineBudget = errors.New("engine: subtree baseline snapshot exceeds the verification cap")

// subtreeBaselines collects the view's baselines recorded strictly beneath dir, keyed by
// normpath.Key so the server's spelling of a name matches the store's (NFC, original case) the
// same way every other path comparison in the engine does. The second result is the set of
// records the store could not decode, which the caller treats as unprovable rather than absent.
// The third reports that the subtree is past the cap.
//
// budget bounds the snapshot: the stream stops as soon as the budget-th record has been read, and
// the over-cap fact is returned EXPLICITLY rather than re-derived by the caller from len(known).
// A length comparison would encode the stop rule's off-by-one in the caller - load-bearing, since
// reading a truncated map as a complete one authorises a recursive delete from an incomplete
// baseline set - while expressing it nowhere in the signature. The bound itself matters because
// this runs inline on a destructive op under a worker lease, where an unbounded O(subtree)
// allocation is the "bounded work" convention's counter-example.
func (e *Engine) subtreeBaselines(viewID uint64, dir string, budget int) (map[string]store.Baseline, []string, bool, error) {
	known := make(map[string]store.Baseline)
	skipped, err := e.scanBaselinePrefix(viewID, dir, func(pb store.PathBaseline) error {
		if len(known) >= budget {
			return errSubtreeBaselineBudget
		}
		known[normpath.Key(pb.Path)] = pb.Baseline
		return nil
	})
	if errors.Is(err, errSubtreeBaselineBudget) {
		// Over the cap: report the fact with no error, so the refusal is rendered as the
		// divergence it is rather than as a store failure the op would retry forever. The map is
		// truncated and the caller must not read it.
		return known, skipped, true, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("engine: scan baselines under %d/%q: %w", viewID, dir, err)
	}
	return known, skipped, false, nil
}
