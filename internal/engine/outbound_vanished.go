package engine

// This file carries the outbound (local→server) vanished-upload path, split out of
// outbound.go behaviour-preservingly (DESIGN §13): the resolution for an OpUpload whose
// source file disappeared before the push ran (errUploadSourceGone). handleVanishedUpload
// decides between recovering a deferred remote version and propagating a genuine local
// delete; serverDivergedForVanishedUpload probes the server to make that call; and
// resolveVanishedUpload reconciles the pending baseline the dropped upload left behind. The
// dispatcher and the conflict/baseline glue stay in outbound.go.

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// handleVanishedUpload resolves an OpUpload whose source disappeared before it applied
// (errUploadSourceGone). The vanish is ambiguous: it can be a genuine local delete to push,
// OR it can sit on a path whose remote create/modify reconcile deferred to this upload's
// keep-both funnel (advancing the cursor on the promise the upload would fire - reconcile.go).
// Dropping the op unconditionally would, in the latter case, lose an acknowledged remote
// update: a fresh remote create would never materialise locally, and a remote modify could be
// pushed back to the server as a deletion.
//
// So it asks the server whether it holds a version that diverges from the base this upload was
// pushing against. When it does, the upload is funnelled to the keep-both resolver, whose
// source-gone path downloads that server version onto the canonical path (no copy - there is no
// local content left to preserve), recovering the deferred update. When the server holds nothing
// new (it matches the base, was never created, or is now a directory), the vanish is a genuine
// local delete: the pending baseline is reconciled so the next scan propagates it, and the op is
// dropped. An inconclusive probe (a transient/timeout) leaves the op queued to retry.
func (e *Engine) handleVanishedUpload(ctx context.Context, qo store.QueuedOp) error {
	op := qo.Op
	diverged, err := e.serverDivergedForVanishedUpload(ctx, op)
	if err != nil {
		// Inconclusive (a timeout or a broken connection re-dialed by WithWorker): leave the op
		// queued. The source is still gone, so the next drain re-hits this path and re-probes.
		return fmt.Errorf("engine: probe vanished upload %d/%q: %w", op.ViewID, op.Path, err)
	}
	if diverged {
		// The server diverged: a remote create/modify reconcile deferred here is real. Funnel to
		// the keep-both resolver (it acks this upload and enqueues the resolve op); its source-gone
		// path pulls the server version onto the canonical path rather than losing it.
		e.log.Info("engine: vanished upload has a diverged server version; funnelling to keep-both resolver",
			"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
		return e.funnelUploadConflict(qo)
	}
	// No new server version: the vanish is a genuine local delete. Drop the op and reconcile the
	// pending baseline so the next scan propagates the deletion.
	e.log.Warn("engine: upload source vanished before apply; dropping op and reconciling baseline",
		"view", op.ViewID, "path", op.Path, "file_id", op.FileID)
	if rerr := e.resolveVanishedUpload(op); rerr != nil {
		return rerr
	}
	if ackErr := e.store.Ack(qo.Seq); ackErr != nil {
		return fmt.Errorf("engine: ack vanished upload %d: %w", qo.Seq, ackErr)
	}
	return errDeadLettered
}

// nodeAnswered reports whether a query_node reply describes a node that is actually THERE. The
// reply map is decoded tolerantly, so an absent node and a `file: {}` a server sends in its place
// produce the same zero Node; only a positive fact distinguishes them. The facts counted are the
// ones a server cannot echo back from the request - an id, a content hash, a size or an mtime.
// The name and the path are deliberately excluded: both are request parameters, and a reply that
// merely repeats them says nothing about occupancy.
func nodeAnswered(n actions.Node) bool {
	return n.FileID != "" || n.Hash != "" || n.Size > 0 || n.MTime > 0
}

// serverDivergedForVanishedUpload reports whether the server holds a file at op.Path whose
// content diverges from the base this vanished upload was pushing against - the signal that a
// remote create/modify was deferred to this upload's keep-both funnel and must be recovered
// rather than dropped. A fresh create (no recorded server file id) treats any present
// server file as a deferred remote create; a modify compares the server hash against the last
// synced ServerHash on the pending baseline (markLocalPending preserves it). The content compare
// is trusted only when BOTH hashes are present: Node.Hash is tolerantly decoded (optional on the
// wire) and the base ServerHash can itself be empty (an earlier upload whose reply omitted the
// hash, preserved through a later modify), and a present server hash against an empty base would
// always read as divergence. With either hash missing the decision falls to node identity instead
// - a matching file id is the unchanged node (the local delete sticks), a different id is a
// replacement preserved through keep-both - so a missing hash is never misread as divergence.
//
// When both hashes ARE present and differ, that comparison is identity-free evidence and is
// evaluated BEFORE the id-less arm. A reply carrying a hash but omitting
// file_id used to short-circuit to "not diverged": the deferred remote create/modify was lost,
// and for a modify-vanish the baseline reverted to StateSynced so the next scan pushed an
// OpRemove over the server's edit. A FRESH create has neither hash to compare, so its own arm -
// "anything that answers for op.Path is the deferred remote create" (nodeAnswered) - likewise
// runs ahead of the id-less one, which otherwise dropped the recovery of a remote create that
// answered with content facts but no file id. A directory reply still short-circuits ahead of everything -
// keep-both's recovery downloads a file, so a directory is genuinely out of scope. A
// non-containable
// path, a server node that is absent or renamed away from op.Path (both classified from the
// parent listing by resolveServerNodeAbsence, exactly as the resolver does) or is a directory,
// all report "not diverged" - a genuine local delete. An ErrPathError that resolves into
// neither positive outcome (a server-side timeout) and any other transport error surface as an
// error so the op stays queued.
func (e *Engine) serverDivergedForVanishedUpload(ctx context.Context, op store.Op) (bool, error) {
	if !containable(op.Path) {
		return false, nil
	}
	var baseHash string
	b, berr := e.store.GetBaseline(op.ViewID, op.Path)
	switch {
	case isNotFound(berr):
	case errors.Is(berr, store.ErrCorruptRecord):
		// An undecodable record yields no base ServerHash; with the base missing the decision
		// below falls to node identity, which is documented never to misread a missing hash as
		// divergence - the safe fallback - rather than the strict read leaving the op queued
		// forever. The key is healed by whichever side resolves the vanish
		// (resolveVanishedUpload's tolerant Put/Delete, or the keep-both funnel's writes).
		e.warnCorruptBaseline("vanished-upload-base", op.ViewID, op.Path, berr)
	case berr != nil:
		return false, berr
	default:
		baseHash = b.ServerHash
	}

	var diverged bool
	werr := e.withWorker(ctx, func(c syncClient) error {
		viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
		server, qerr := c.QueryNode(ctx, viewID, op.Path)
		if qerr != nil {
			if errors.Is(qerr, actions.ErrPathError) {
				// Absent OR a server-side timeout, indistinguishable from the reply alone. Confirm
				// via the parent listing (as resolveConflict does): a clean listing that omits the
				// node proves it gone (no divergence - a genuine local delete); an inconclusive
				// listing leaves the original error so the op stays queued.
				//
				// The listing is classified by resolveServerNodeAbsence, so a
				// node still visible BY ID under a DIFFERENT leaf name (another client renamed it
				// in place, a/x -> a/y) counts as "nothing to recover at op.Path" rather than the
				// inconclusive error it produced before. That error requeued the op on a probe
				// whose answer could never change - a permanent wedge holding the authoritative
				// inbound OpRename behind it on the shared path key. There is genuinely no
				// deferred remote update sitting at op.Path (the node left it), and no local
				// content to lose (the source vanished), so the vanish resolves as a local delete:
				// the pending baseline is reverted and the next scan re-emits the deletion against
				// whatever path the node's own rename event maps it to.
				pc, ok := c.(outboundClient)
				if !ok {
					return errNoOutbound
				}
				gone, moved, cerr := resolveServerNodeAbsence(ctx, pc, e, viewID, op.Path, op.FileID)
				if cerr == nil && (gone || moved) {
					diverged = false
					return nil
				}
				return qerr
			}
			return qerr
		}
		if server.IsDir {
			// The path is now a directory - out of keep-both's file-content scope (the resolver's
			// source-gone path downloads a FILE onto the canonical path): treat as a local delete.
			diverged = false
			return nil
		}
		switch {
		case contentDiverged(server.Hash, baseHash):
			// PROVEN divergence, identity-free: the path holds content that is not the base this
			// upload was pushing against, so a deferred remote create/modify is sitting there and
			// must be recovered. This branch sits AHEAD of the id-less arm:
			// a reply carrying a hash but omitting file_id (decodeNode tolerates the omission)
			// used to be read as "nothing to recover", which propagates the local delete over the
			// server's edit - and for a modify-vanish it also flips the baseline to StateSynced,
			// so the next scan's OpRemove falls into the sibling hole in applyRemoveOut.
			diverged = true
		case op.FileID == "" && nodeAnswered(server):
			// A FRESH create: it has no id and no recorded base, so neither the hash-divergence
			// arm above nor identity below can speak for it - the only question it asks is whether
			// ANYTHING file-shaped occupies op.Path, and anything that does is the deferred remote
			// create this upload's funnel promised to recover. This sits ahead of the id-less arm
			// (the fresh-create half): behind it, a reply carrying content facts
			// but omitting file_id (decodeNode tolerates the omission) read as "nothing to
			// recover", the local delete was propagated, and the remote create - whose event
			// cursor reconcile had already advanced on this upload's promise - never materialised
			// locally at all.
			//
			// nodeAnswered is what keeps this from firing on nothing: a tolerant `file: {}` decodes
			// to the zero node exactly like an absent one, and a bare {} is never proof (the shared
			// reply-evidence rule).
			diverged = true
		case server.FileID == "":
			// A present node reported without an id AND without a divergence proof: nothing
			// file-shaped we can attribute (an empty reply decodes the same way). Historical
			// behaviour - treat as a local delete.
			diverged = false
		case server.Hash != "" && baseHash != "":
			// Both hashes present and EQUAL (the divergent half is the first branch): the server
			// still holds the base this upload was pushing against, so nothing was deferred here
			// and the vanish is a genuine local delete.
			diverged = false
		default:
			// No reliable content signal: either the server reply omitted the hash (Node.Hash
			// is tolerantly decoded) OR we never recorded a base hash (an earlier upload whose
			// reply omitted it, preserved through a later local modify by markLocalPending).
			// A missing hash on either side must NOT be read as divergence - doing so would
			// resurrect a genuinely deleted file. Fall back to node identity: the same file id
			// we last synced is the unchanged node, so the local delete should stick (not
			// diverged); a different id is a server-side replacement we cannot match to our
			// base, so preserve it through keep-both rather than push a delete over it.
			diverged = server.FileID != op.FileID
		}
		return nil
	})
	if werr != nil {
		return false, werr
	}
	return diverged, nil
}

// resolveVanishedUpload reconciles the pending baseline an upload left behind when its
// source file disappeared before the push ran (errUploadSourceGone) and the server held no
// diverged version to recover (handleVanishedUpload). A fresh create (no server file id) never
// reached the server, so its pending baseline is dropped outright. A modify whose file vanished
// still has a node on the server: revert the baseline to StateSynced so the next local scan sees
// a synced baseline with no on-disk file and emits the OpRemove that propagates the deletion to
// the server, rather than the deletion being lost behind the now-dropped upload op.
//
// The revert clears the local facts (LocalHash/LocalSize/LocalMTime): markLocalPending stamped
// them with the vanished edit's content, which the server never saw, and a synced baseline
// carrying that hash would silently swallow a recreate with identical content before the next
// scan (git checkout, a rebuilt artifact) - the re-hash matches, no OpUpload fires, and local
// and server diverge with a "synced" baseline. An empty LocalHash can never equal a real file's
// hash, so a recreate always re-diffs into an OpUpload; a file that stays deleted still emits
// the OpRemove (detectRemoves keys on the base-only row, not the local facts), and detectMoves
// already excludes empty-hash baselines as move sources. Server facts are untouched.
func (e *Engine) resolveVanishedUpload(op store.Op) error {
	if op.FileID == "" {
		if err := e.store.DeleteBaseline(op.ViewID, op.Path); err != nil {
			return fmt.Errorf("engine: drop vanished-create baseline %d/%q: %w", op.ViewID, op.Path, err)
		}
		return nil
	}
	b, err := e.store.GetBaseline(op.ViewID, op.Path)
	if isNotFound(err) {
		return nil
	}
	if errors.Is(err, store.ErrCorruptRecord) {
		// An undecodable record is rebuilt from the op's facts rather than deleted:
		// this branch is a MODIFY whose file vanished locally, so the next scan must
		// find a synced record with no on-disk file to emit the OpRemove - deleting would make
		// the path untracked and silently lose the delete propagation, and the strict read
		// before this arm left the op queued forever. The Put below (synced + cleared local
		// facts) heals the key and is exactly the detectRemoves shape.
		e.warnCorruptBaseline("vanished-upload-revert", op.ViewID, op.Path, err)
		b, err = store.Baseline{FileID: op.FileID, FileType: fileType(false)}, nil
	}
	if err != nil {
		return fmt.Errorf("engine: load vanished-upload baseline %d/%q: %w", op.ViewID, op.Path, err)
	}
	b.State = store.StateSynced
	b.LocalHash = ""
	b.LocalSize = 0
	b.LocalMTime = 0
	if err := e.store.PutBaseline(op.ViewID, op.Path, b); err != nil {
		return fmt.Errorf("engine: revert vanished-upload baseline %d/%q: %w", op.ViewID, op.Path, err)
	}
	return nil
}
