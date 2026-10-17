package engine

// This file carries the outbound (local→server) upload-apply path, split out of
// outbound.go behaviour-preservingly (DESIGN §13): the small-file-vs-resume decision and
// its lost-reply recovery probes. applyUploadOut is the dispatcher's OpUpload handler;
// probeUploadedNode/recoverUploadFileID recover a lost or id-less reply; resumeUploadFile
// streams a large file with the chunked resume_upload protocol. The conflict/baseline glue
// and the dispatcher itself stay in outbound.go.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// uploadNodeRelocatedError marks an outbound OpUpload (a modify - FileID non-empty) whose
// parent baseline is gone from the op's path because a server-side rename of an ancestor
// relocated the whole subtree's baselines (relocatePending) before the upload drained.
// The op addresses the OLD path, whose parent baseline no longer
// exists, so resolveParentID can never resolve it - while the authoritative inbound
// OpRename that would move the on-disk file is serialized BEHIND this op on the ancestor
// conflict key: a cyclic wait neither side can win. It carries the node's CURRENT indexed
// path (store.PathByID) so applyOutbound can revert that relocated baseline for
// re-emission (StateSynced + cleared local facts, the resolveVanishedUpload precedent) and
// ack the stale op - a race resolution without a durable dead-letter trace, mirroring
// removeNodeMovedError: once the inbound rename applies, the next scan
// re-diffs the edit at the new path and re-emits the upload under the live parent.
type uploadNodeRelocatedError struct{ currentPath string }

func (e *uploadNodeRelocatedError) Error() string {
	return fmt.Sprintf("engine: outbound upload target relocated to %q; re-emitting on the new path", e.currentPath)
}

// relocatedUploadPath reports the node's CURRENT indexed path when an id-carrying outbound
// OpUpload (a modify) addresses a stale one - a server-side rename relocated the node's
// baseline (relocatePending) after the op was enqueued - and "" when the op's path is still
// the tracked one. The byid index is positive local proof of the relocation, so no server
// turn is needed. localReconcile plans a modify with the BASELINE's stored path (planUpload's
// pb.Path), and it skips any path with a queued op, so in the absence of an inbound
// relocation the index always resolves back to op.Path; a genuine mismatch is the race.
//
// A backend failure is propagated rather than swallowed: deciding "not relocated" from an
// index that could not be read is exactly the destructive default this guard exists to
// prevent, and returning the error merely leaves the op queued for the next drain. An absent
// index entry (the node was dropped) is not a relocation and keeps the ordinary flow.
func (e *Engine) relocatedUploadPath(op store.Op) (string, error) {
	if op.FileID == "" {
		return "", nil
	}
	cur, err := e.store.PathByID(op.ViewID, op.FileID)
	switch {
	case isNotFound(err):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("engine: resolve upload target %d/%q: %w", op.ViewID, op.FileID, err)
	case cur == op.Path:
		return "", nil
	}
	return cur, nil
}

// applyUploadOut pushes a created or modified local file to the server. It resolves the
// parent directory's server id (leaving the op queued if the parent op has not yet run),
// reads the file under the sync root (an unsafe path dead-letters via abs), and picks the
// single-frame Upload for a small file or the chunked resume_upload for a large one. On
// success the server-assigned node facts are recorded into the baseline as StateSynced.
func (e *Engine) applyUploadOut(ctx context.Context, op store.Op) error {
	// Validate containment first so a structurally bad path dead-letters rather than being
	// sent. This must precede resolveParentID: for a traversal path like "../escape",
	// path.Dir is ".." and resolveParentID would return errParentUnknown (leaving the op
	// queued forever) before the containment check could reject it - matching applyMkdirOut,
	// applyRenameOut and applyRemoveOut, which all check containment first.
	if !containable(op.Path) {
		return fmt.Errorf("%w: outbound upload path %q in view %d", errRejectedPath, op.Path, op.ViewID)
	}
	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	// Pre-turn relocation redirect - the outbound mirror of the inbound self-echo
	// redirect (downloadRelocatedBySelfEcho). A server-side rename of the NODE ITSELF, in
	// particular a same-directory a/x -> a/y, leaves the parent baseline perfectly alive, so
	// resolveParentID succeeds and the errParentBaselineAbsent exit below is never reached -
	// yet relocatePending has already re-homed THIS modify's baseline to the new path while
	// the authoritative inbound OpRename is serialized BEHIND this op on the shared source
	// key. Uploading anyway addresses the FREED old path: the server mints a SECOND node
	// there carrying the edit, markSynced records it and re-points byid at it, the inbound
	// rename then carries the edited file to the new path (forcing that record synced), and
	// the next scan diffs the synced-but-fileless old path into an outbound OpRemove that
	// DELETES the only server copy of the edit - a silent permanent divergence (the server
	// keeps the OLD content under the new name, the client believes it is synced).
	// The byid index is positive LOCAL proof of the relocation, so consult it before the
	// server turn: a hit at a different path exits through uploadNodeRelocatedError, whose
	// handler reverts the relocated baseline for re-emission and acks without a durable
	// trace, so the next scan re-pushes the edit at the node's current path. An id-less
	// create carries no node identity and is unaffected.
	if cur, rerr := e.relocatedUploadPath(op); rerr != nil {
		return rerr
	} else if cur != "" {
		return &uploadNodeRelocatedError{currentPath: cur}
	}
	parentID, err := e.resolveParentID(op.ViewID, op.Path)
	if err != nil {
		// Order B: a preceding inbound OpRemove of the whole parent directory
		// already applied and preserveAside carried the edited subtree into a _Conflict copy,
		// dropping the parent baseline - so resolveParentID can never resolve it and no parent
		// op will ever run. The source file is gone from op.Path too (moved under the _Conflict
		// prefix). Distinguish that permanent dead-end from an ordinary errParentUnknown wait
		// (parent op pending, the source still on disk) by probing the source: gone → route
		// through the vanished-source path, which recovers a deferred remote version or drops
		// the op (exactly as if openUploadSource - which runs after resolveParentID, so it never
		// gets the chance here - had reported it gone). A present source keeps the op queued.
		if errors.Is(err, errParentUnknown) && e.uploadSourceGone(abs) {
			return errUploadSourceGone
		}
		// The source is still on disk but NO baseline tracks the parent at all (not merely an
		// id-less pending parent - that stays the plain transient wait below). Two permanent
		// dead-ends produce exactly this state, both with the authoritative inbound op
		// serialized BEHIND this upload, so waiting is a cyclic wedge:
		//   - a server-side rename of an ancestor: relocatePending re-homed the subtree's
		//     baselines (a/x -> c/x) at reconcile time, so this op's parent path resolves to
		//     nothing while the inbound OpRename that would move the on-disk file waits behind
		//     us. A modify carries the node's id, and the byid index already points at the
		//     relocated path - positive LOCAL proof, no server turn needed. That case no longer
		//     reaches here: the unconditional pre-turn redirect above already resolved
		//     it, since the ancestor rename relocates the node's OWN baseline too.
		//   - a server-side delete of an ancestor whose sibling mkdir dead-lettered first in
		//     this same drain (dropGoneParentBaseline removed the parent baseline): an id-less
		//     fresh create has no byid entry to consult, so confirm the parent gone with the
		//     same ancestor walk the server-turn exits use (a renamed ancestor also reads gone
		//     - leaf-blind, accepted as data-safe: the cleanup re-bases through the queued
		//     inbound renames and the next scan re-uploads under the live parent).
		if errors.Is(err, errParentBaselineAbsent) && op.FileID == "" &&
			e.parentGoneConfirmed(ctx, op.ViewID, op.Path) {
			return errParentGone
		}
		return err
	}
	name := path.Base(op.Path)

	// The last agreed server content this push is written against (finding H1). Read before the
	// lease so a store failure never costs a worker turn, mirroring removeTargetFacts.
	baseServerHash, bherr := e.uploadDivergenceBase(op)
	if bherr != nil {
		return bherr
	}

	var node actions.Node
	var liveHash string
	var liveSize, liveMTime int64
	werr := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		// Open the source ONCE, with a no-follow openat(O_NOFOLLOW) walk anchored at the sync
		// root, and confirm it is a regular file (openUploadSource). All facts - size, mtime,
		// content, hash - then come from that single descriptor rather than from name-based
		// re-stats, which closes three races against a concurrent swap in the scan→apply window:
		//   - a symlink swapped in for ANY path component (intermediate or final) after the
		//     path's symlink check (abs/noSymlinkEscape) could redirect os.ReadFile/os.Open
		//     outside the sync root; the no-follow walk refuses EVERY symlink component on every
		//     platform including Windows.
		//   - a directory/FIFO swapped in could block or wedge the read; the fstat regular-file
		//     check refuses it.
		//   - the small-vs-resume decision keyed on a name-stat that os.ReadFile then re-reads
		//     unbounded could buffer a file that grew past the threshold; keying on the fstat
		//     size and reading exactly that many bytes bounds the small path.
		// A source that vanished surfaces as errUploadSourceGone so applyOutbound drops the op
		// and reconciles the deletion.
		f, fi, oerr := e.openUploadSource(abs)
		if oerr != nil {
			return oerr
		}
		defer f.Close()
		liveSize = fi.Size()
		liveMTime = fi.ModTime().Unix()

		// H1 precondition: the server does NOT arbitrate divergence for an upload - its
		// "compare_mtime" policy accepts any write whose mtime is not older, so the 24579 the
		// engine waits for never arrives in the typical offline-edit case and the peer's version
		// is destroyed silently. Prove the target is still the version this client last agreed on
		// before the destructive turn; anything undecided keeps the op queued and pushes nothing
		// (outbound_uploadguard.go). One gate covers BOTH the single-frame and the resume path.
		//
		// It runs AFTER openUploadSource, not before: a source that vanished in the scan→apply
		// window has its own resolution (errUploadSourceGone → handleVanishedUpload), which asks
		// the server the same question with the answers a missing local file needs. Probing first
		// would divert that case - a path the server has since turned into a directory, say - into
		// an upload-shaped exit for an upload that can no longer happen. Nothing is hashed or sent
		// before the gate, so a push that will funnel still pays only for the open.
		if gerr := e.guardUploadTarget(ctx, c, op, baseServerHash); gerr != nil {
			return gerr
		}

		if liveSize <= e.limits.UploadResumeThreshold {
			// Read exactly the fstat-reported bytes from the open descriptor: bounded by the
			// threshold regardless of any concurrent growth, and a consistent snapshot to hash.
			content := make([]byte, liveSize)
			if _, rerr := io.ReadFull(f, content); rerr != nil {
				if errors.Is(rerr, io.ErrUnexpectedEOF) || errors.Is(rerr, io.EOF) {
					// The file shrank under us after the fstat; let the next scan reconcile.
					return errUploadSourceGone
				}
				return fmt.Errorf("read upload source %q: %w", abs, rerr)
			}
			// Hash the exact bytes uploaded (no extra I/O) so the recorded baseline matches
			// what reached the server, rather than the possibly-stale scan-time hash.
			liveHash = md4.ContentHash(content)
			n, uerr := pc.Upload(ctx, parentID, name, liveMTime, e.viewRootNodeID(op.ViewID), content)
			if uerr != nil {
				// The lost-reply recovery probe is NOT issued here: a small-file Upload poisons
				// its own connection on EVERY server rejection (the half-written body would
				// otherwise desync the next turn - actions/upload.go), so a QueryNode on this same
				// connection would immediately return ErrConnBroken. The probe is deferred to a
				// FRESH worker lease after this turn returns (see below). Just surface
				// the error so WithWorker re-dials the poisoned worker.
				return uerr
			}
			node = n
			return e.recoverUploadFileID(ctx, c, op, &node)
		}
		// Large file: hash the live bytes from the same descriptor, rewind, then stream - the
		// resume path advertises the hash up front, so a stale hash (a same-size rewrite the
		// second-resolution mtime check could miss) would fail the server's completion check
		// forever. Hashing the descriptor we then stream keeps the two
		// in step and avoids a second name-based open that could race a swap.
		//
		if e.afterUploadFstat != nil {
			e.afterUploadFstat()
		}
		// Hash exactly liveSize bytes (io.LimitReader), NOT the whole descriptor to EOF:
		// resumeUploadFile streams precisely liveSize bytes (for offset < size), so a concurrent
		// append between fstat and hashing would otherwise make the advertised hash cover more
		// bytes than are sent, and the server's completion check would never converge - the op
		// would fail on every drain until the file stops growing. Bounding the hash to liveSize
		// keeps the advertised hash equal to the streamed bytes regardless of any growth, exactly
		// as the small branch reads only liveSize bytes.
		h, herr := hashReaderMD4(io.LimitReader(f, liveSize))
		if herr != nil {
			return fmt.Errorf("hash upload source %q: %w", abs, herr)
		}
		if _, serr := f.Seek(0, io.SeekStart); serr != nil {
			return fmt.Errorf("seek upload source %q: %w", abs, serr)
		}
		liveHash = h
		n, uerr := e.resumeUploadFile(ctx, pc, op.ViewID, f, parentID, name, liveSize, liveHash, liveMTime)
		if uerr != nil {
			return uerr
		}
		node = n
		return e.recoverUploadFileID(ctx, c, op, &node)
	})
	if werr != nil {
		// A small-file Upload that failed with a path/stale-base error may actually have
		// landed server-side on a PRIOR attempt whose reply was lost (the connection dropped
		// after the bytes were written). The retry's own connection is poisoned (Upload breaks
		// it on every rejection), so the verification cannot run on it - probe on a FRESH
		// worker lease whether the node now at op.Path carries OUR content hash. Adopt it as
		// success only on a hash match, so a genuine remote divergence (different content)
		// still surfaces as the real conflict/error rather than being silently swallowed
		// (mirroring mkdir/rename's replay recovery). The probe is small-path only:
		// the chunked resume path recovers a lost reply through ResumeGetToken's committed
		// offset instead, so a large file falls straight through to the shared handling below.
		var recovered bool
		lostReply := errors.Is(werr, actions.ErrPathError)
		if liveSize <= e.limits.UploadResumeThreshold &&
			(lostReply || errors.Is(werr, actions.ErrConflictVersion)) {
			rec, perr := e.probeUploadedNode(ctx, op, liveHash, liveSize, lostReply)
			if perr == nil && rec.FileID != "" {
				node = rec
				recovered = true
			}
		}
		if !recovered {
			// Order A: the upload failed and no committed node could be confirmed.
			// If the failure was a path error the parent directory may have been deleted
			// server-side, so the upload can never converge - the op would otherwise retry
			// forever and, drained ahead of the inbound OpRemove serialised behind it, block
			// that authoritative remove's never-lose preserve. Confirm the parent is gone via
			// the ancestor walk; if so, drop the op terminally so the trailing inbound OpRemove
			// preserves the still-on-disk edit as a _Conflict copy. This runs for BOTH the small
			// path (after the lost-reply probe above could not recover a node) AND the large
			// resume path (whose ResumeGetToken against the stale parent id returns ErrPathError,
			// which the size-gated probe branch above deliberately skips) - mirroring the
			// unconditional exit applyMkdirOut/applyRenameOut already have. A non-path error, or
			// a still-present parent, keeps the op queued for the next drain.
			if errors.Is(werr, actions.ErrPathError) && e.parentGoneConfirmed(ctx, op.ViewID, op.Path) {
				return errParentGone
			}
			// The parent is alive and the failure is still a bare path error: probe whether a
			// server DIRECTORY (non-empty id) now occupies the upload's own path - the
			// file<->dir create/create collision (mirroring the mkdir
			// verb's opposite-type probe). A file upload can never land on a directory node,
			// and no other arm of applyOutbound's matrix matches, so pre-fix the op retried
			// forever. Only a positive probe escalates: funnel into the keep-both resolver,
			// whose errConflictServerDir doctrine dead-letters it observably (StateConflict +
			// Dropped); an inconclusive probe keeps the op queued for the next drain.
			if errors.Is(werr, actions.ErrPathError) && e.serverDirOccupiesPath(ctx, op.ViewID, op.Path) {
				return fmt.Errorf("%w: upload %d/%q found a server directory",
					errServerTypeCollision, op.ViewID, op.Path)
			}
			// A FRESH create that draws the path error and whose probe found an occupant it could
			// not name is this op's own earlier attempt coming back: the first push landed, the
			// reply named no node, and the recovery probe cannot supply one either because this
			// server does not name nodes. Report it as such rather than passing the bare mapped
			// sentinel up. The sentinel carries no code, so noteServerErr would clear the streak
			// AND the pause that the first attempt armed - and every drain after that re-streams
			// the whole file body again, which is exactly the spin errUploadNoFileID exists to
			// pace. Only the id-less CREATE arm qualifies: a modify keeps op.FileID, and a create
			// whose probe placed a node has already recovered above.
			if errors.Is(werr, actions.ErrPathError) && op.FileID == "" {
				return fmt.Errorf("%w: upload %d/%q found an unnameable occupant at its own path",
					errUploadNoFileID, op.ViewID, op.Path)
			}
			return werr
		}
	}
	// For a modify, op.FileID is the existing server node id; keep it if the upload reply
	// did not echo one (the exact reply key is unconfirmed against a live DSM, DESIGN §13),
	// so a successful push never blanks a known id and drops the node from the byid index.
	fileID := node.FileID
	if fileID == "" {
		fileID = op.FileID
	}
	if fileID == "" {
		// A FRESH create (op.FileID empty) whose reply omitted a file id and that the
		// recovery probe (recoverUploadFileID) could not fill: leaving it would advance the
		// baseline to StateSynced with an empty id and drop the node from the byid index, so
		// every later modify/move/remove of it would misresolve. Leave the op queued instead,
		// mirroring applyMkdirOut - a re-upload returns ErrPathError, recovered by the
		// lost-reply probe above.
		//
		// It carries errUploadNoFileID so noteServerErr can PACE the retry. Without the sentinel
		// this was a bare error: no *actions.ServerError, no synthetic code, so noteServerErr took
		// its !ok branch and cleared the streak AND the pause on every drain - and each retry
		// re-streams the WHOLE file body, since guardUploadTarget deliberately proceeds when the
		// server hash already equals ours (the lost-reply reading). Against a DSM that omits the
		// id that is an unbounded full-body re-upload once per drain, forever, which is the H4
		// spin reached through the one arm the -1…-5 codes did not cover.
		return fmt.Errorf("%w: upload %d/%q returned no file id and none could be recovered; will retry",
			errUploadNoFileID, op.ViewID, op.Path)
	}
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = fileID
		b.FileType = fileType(false)
		b.ServerHash = node.Hash
		b.ServerMTime = node.MTime
		b.LocalHash = liveHash
		b.LocalSize = liveSize
		b.LocalMTime = liveMTime
		b.UnixPerm = op.UnixPerm
	})
}

// probeUploadedNode re-checks, on a FRESH worker lease, whether a small-file upload that
// returned ErrPathError/ErrConflictVersion actually took effect server-side (a prior
// attempt committed it but its reply was lost). It cannot reuse the upload's own
// connection: a small Upload poisons the connection on every server rejection (the
// half-written body would desync the next turn), so a QueryNode there returns
// ErrConnBroken. The returned node is non-zero ONLY when a node at op.Path carries
// wantHash - the same content gate the in-line recovery used - so a genuine remote
// divergence is never adopted as our success. A probe-time lookup failure returns the
// zero node and the error for the caller to fall back to the original upload error.
// A reply that omits the hash (an unconfirmed reply shape, DESIGN §13) leaves that gate
// unprovable in its only form, and every lost reply of an already-committed upload would then
// funnel into a spurious keep-both - a duplicate copy born of a transport blip, on content the
// server already holds. So a hash-less reply degrades to identity+size:
// the node must be OUR node (a MODIFY's own file id, which the server echoes back) and hold
// exactly wantSize bytes. A fresh create carries no id to match, so it keeps the strict
// behaviour and falls through to the caller's existing handling - there is no identity evidence
// to degrade to, and adopting a same-sized stranger as our upload would record agreement with
// content we never sent.
//
// The size must be POSITIVE, not merely equal. Node.Size is tolerantly decoded and a reply that
// omits it decodes to zero exactly like a genuine empty file, so for a truncate-to-zero push
// (wantSize == 0) a reply carrying neither hash nor size would satisfy `existing.Size ==
// wantSize` on nothing at all - and identity is no counter-evidence, since a modify's probe
// addresses its own node and `existing.FileID == op.FileID` holds by construction. The degrade
// would then collapse to a bare adoption: the truncation is recorded StateSynced without ever
// having landed, never re-pushed, and the server's older content later downloads over it with
// no _Conflict copy. A bare {} is never proof (the shared reply-evidence rule), so a zero size
// falls through to the caller's strict handling; a genuine empty-file upload whose reply was
// lost is simply re-pushed.
//
// lostReply gates that degrade to the only failure it was written for: an ErrPathError, whose
// hypothesis is that a PRIOR attempt committed these very bytes and its reply was lost. On an
// ErrConflictVersion the server has instead STATED that the node moved off the base we pushed
// against - positive evidence our bytes did not land - and identity is no counter-evidence,
// since a modify's probe addresses its own node and `existing.FileID == op.FileID` holds by
// construction. Adopting there on size alone swallows another client's same-sized edit as our
// success: markSynced records our unsent content as agreed, the push is never retried, and the
// remote version later downloads over the local edit with no _Conflict copy. So a 24579 keeps
// the strict hash gate and falls through to funnelUploadConflict (DESIGN §6 never-lose).
func (e *Engine) probeUploadedNode(ctx context.Context, op store.Op, wantHash string, wantSize int64, lostReply bool) (actions.Node, error) {
	var node actions.Node
	err := e.withWorker(ctx, func(c syncClient) error {
		viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
		existing, qerr := c.QueryNode(ctx, viewID, op.Path)
		if qerr != nil {
			return qerr
		}
		if existing.FileID == "" || existing.IsDir {
			return nil
		}
		if existing.Hash != "" {
			if existing.Hash == wantHash {
				node = existing
			}
			return nil
		}
		if lostReply && op.FileID != "" && existing.FileID == op.FileID && existing.Size > 0 && existing.Size == wantSize {
			e.log.Warn("engine: upload recovery probe reply carried no content hash; adopting on identity+size evidence",
				"view", op.ViewID, "path", op.Path, "file_id", existing.FileID, "size", wantSize)
			node = existing
		}
		return nil
	})
	return node, err
}

// recoverUploadFileID fills a missing server file id on a freshly created node by
// re-querying it on the SAME (still-healthy) worker the upload just succeeded on, mirroring
// applyMkdirOut. A successful upload that did not echo a file id (the exact reply key is
// unconfirmed against a live DSM, DESIGN §13) would otherwise advance the baseline to
// StateSynced with an empty id and drop the node from the byid index. Only a CREATE
// (op.FileID empty) needs this - a modify keeps its existing id. The recovered node must be
// a regular file: a concurrent process could have replaced the path with a directory, and
// adopting that id as a file baseline would misresolve every later op (mirroring mkdir's
// is-dir gate). A query that BREAKS the connection propagates so WithWorker re-dials the
// poisoned worker; a clean lookup failure leaves the id empty and the post-turn guard
// re-queues the op for a later retry.
func (e *Engine) recoverUploadFileID(ctx context.Context, c syncClient, op store.Op, node *actions.Node) error {
	if node.FileID != "" || op.FileID != "" {
		return nil
	}
	viewID := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	existing, qerr := c.QueryNode(ctx, viewID, op.Path)
	if qerr != nil {
		if br, ok := c.(brokenReporter); ok && br.Broken() {
			return qerr
		}
		return nil
	}
	if existing.FileID != "" && !existing.IsDir {
		*node = existing
	}
	return nil
}

// errResumeNoCompletion is retained only so the escalation table keeps a stable
// synthetic code for it (servererr.go). It is UNREACHABLE since the resume upload
// became a whole-file turn: success is the reply's result verdict, so there is no
// longer a "streamed everything but was never told it finished" state to report.
var errResumeNoCompletion = errors.New("engine: resume upload streamed the whole file without a completing reply")

// resumeUploadFile transfers a large file under a resume token: it obtains a
// view-scoped token, then hands the whole file to a single two-frame resume_upload
// whose content frame is STREAMED straight off f, so the bytes are never buffered.
//
// There is no chunk loop and no resume offset: the protocol has no sproto-level
// chunking for uploads (a live DSM takes a 100 MiB file as one content frame) and
// resume_upload carries no offset field, so an interrupted transfer restarts from
// byte zero on the next drain. The `upload` + `dry_run:1` negotiation the real
// client performs first is advisory - the transfer succeeds without it - so it is
// skipped. The caller owns f's lifetime (close); no token is persisted across runs
// (DESIGN §4).
func (e *Engine) resumeUploadFile(ctx context.Context, pc outboundClient, viewID uint64, f *os.File, parentID, name string, size int64, hash string, mtime int64) (actions.Node, error) {
	tok, err := pc.ResumeGetToken(ctx, actions.ResumeRequest{
		ViewID:     int64(viewID), // #nosec G115 -- a view_id is a non-negative identifier
		RootNodeID: e.viewRootNodeID(viewID),
	})
	if err != nil {
		return actions.Node{}, err
	}
	// Always stream from byte zero: a retry re-sends the whole file.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return actions.Node{}, fmt.Errorf("seek upload source to 0: %w", err)
	}
	return pc.ResumeUpload(ctx, tok.Token, actions.ResumeUploadSpec{
		ParentID:   parentID,
		Name:       name,
		MTime:      mtime,
		RootNodeID: e.viewRootNodeID(viewID),
		Size:       size,
		Hash:       hash,
	}, io.LimitReader(f, size))
}
