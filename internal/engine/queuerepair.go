package engine

// This file carries the queue-quarantine repair sweep.
//
// Both reconcile passes are built so a pending baseline ALWAYS has an op that owns it:
// they enqueue first and flip the baseline second, so a crash in between leaves a
// replayable queued op, never a StatePending record with nothing to resolve it
// (docs/INVARIANTS.md#enqueue-first). Every engine-side drop of an op
// restores that invariant explicitly - deadLetterAndAck is called only after the
// matching baseline cleanup in deadletter.go has run.
//
// The store's queue quarantine is the one drop that cannot do this. When a queue
// record's key or value no longer decodes (latent file corruption - bbolt checksums
// only its meta pages), the readers skip it and quarantineCorruptQueueRecords moves it
// out of the queue into the dead-letter trace so it cannot poison every later read. But
// the record's view, path and type are exactly what is undecodable, so nothing can
// resolve the baseline the op left pending: the path freezes. localReconcile derives
// outbound ops only from StateSynced baselines, so every later LOCAL edit of that path
// is silently not pushed, and only a server event for the same path can heal it - which,
// for a file this client alone edits, never arrives. The operator sees a bare Dropped
// count with no view or path, indistinguishable from a handled engine-side drop.
//
// The sweep is the compensating half: after a drain pass in which the store quarantined
// at least one record, re-derive an op for every pending baseline no queued op covers,
// from the baseline and the disk - the same evidence localReconcile diffs from a synced
// baseline. It only ever ENQUEUES (or drops a record for a node that is not on disk at
// all), so the queue's single-consumer invariant is untouched, and it runs on the drain
// goroutine after the pass has settled, never concurrently with an apply.

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// repairDeferBackoffBase and repairDeferBackoffMax bound how often the sweep re-runs while it
// keeps deferring the SAME candidates.
//
// The re-arm rule assumes a deferral drains away: the overlapping op applies, and the next sweep
// either owns the record or finds it stranded and repairs it. A pending baseline sitting under a
// FOREVER-queued op breaks that assumption - the accepted forever-transient classes (an inbound
// unmapped errno, errParentUnknown) keep their op queued for the lifetime of the daemon, so the
// deferral never resolves and the sweep re-ran a ListViews plus a full ScanBaselines of every view
// on every single drain, for a candidate it never examines. That is O(all baselines) per drain
// forever, plus a Warn per drain, bought by nothing.
//
// So a deferral that repeats unchanged backs off geometrically to a low steady rate. The backoff
// is a floor on RE-EXAMINING the same deferred set, never on reacting to new evidence: a fresh
// quarantine, a change in the deferred set, or coverage that actually drained (repairDeferralHolds)
// all run the sweep at once. The base is small so an ordinary temporary overlap - a descendant of a
// queued directory rename - is still re-checked promptly.
const (
	repairDeferBackoffBase = 30 * time.Second
	repairDeferBackoffMax  = 30 * time.Minute
)

// repairDeferKeysCap bounds the remembered deferred-key set. The set is normally tiny (the
// candidates one quarantine window left overlapped), but a queued directory rename can defer a
// whole subtree, and this memory lives for the daemon's life. Past the cap the remembered keys are
// a sorted PREFIX of the set: the coverage re-check then reasons about a sample, so a change
// outside it waits for the backoff instead of being noticed immediately - a bounded delay, not a
// missed repair.
const repairDeferKeysCap = 4096

// repairDeferral is what the last sweep deferred, and when the next one may re-examine it.
// It is read and written only from the drain goroutine (drainQueue runs the sweep after the
// pass's apply goroutines have settled), like quarantineRepaired, so it needs no lock.
type repairDeferral struct {
	// keys are the deferred candidates' resolved on-disk conflict keys, sorted, so a later
	// sweep can tell "the same deferral again" from new evidence, and so the coverage that
	// caused it can be re-tested cheaply against a fresh queued index.
	keys []string
	// quarantined is the store's quarantine counter that armed the sweep which deferred these.
	// A different counter means a NEW quarantine arrived, whose stranded records have nothing
	// to do with this deferral, so the backoff must not hold it back.
	quarantined uint64
	// warned latches the operator Warn for this deferral, so an unchanging deferral is
	// reported once per streak rather than once per drain (the opWarned convention).
	warned  bool
	backoff time.Duration
	retryAt time.Time
}

// repairQuarantinedBaselines re-derives the ops the store's queue quarantine dropped
// without being able to name them. It is a no-op unless the store's quarantine counter
// has advanced since the last sweep, so the ordinary drain pays nothing for it: a
// healthy queue never quarantines anything.
//
// The counter is remembered only after a sweep completes, so a failed sweep (a store
// error, a cancelled context) is retried by the next drain rather than being marked
// done. A sweep that DEFERRED a candidate - one whose path merely overlaps a still-queued
// op rather than being owned by it - does not count as completed either: the coverage that
// hid it is temporary, while the arming counter advances only on the next quarantine, which
// for latent corruption may never come. Leaving the counter unadvanced re-runs the sweep on
// the following drain, and it self-terminates as soon as no candidate is deferred (a
// re-derived op names its own baseline's key exactly, so the next sweep reads it as owned).
// A deferral that does NOT drain away - the covering op is one of the accepted
// forever-transient classes - re-runs on a backoff instead of on every drain, and Warns once
// per streak (repairDeferral).
// It is in-memory like the other executor bookkeeping: a quarantine whose sweep
// never ran because the daemon died in between leaves the path frozen exactly as before
// this fix - a residual recorded in DESIGN §13, and a far narrower window than the
// unconditional freeze it replaces.
func (e *Engine) repairQuarantinedBaselines(ctx context.Context) error {
	quarantined := e.store.QuarantinedOps()
	if quarantined == e.quarantineRepaired {
		return nil
	}
	// One queued index for the whole sweep, built exactly like localScan's: the coverage
	// test is the executor's own path-overlap rule, so a pending baseline a queued op
	// legitimately owns - most importantly a descendant of a queued directory rename,
	// which reconcile's relocatePending rebases WITHOUT an op of its own - is never
	// mistaken for a stranded one. It is built BEFORE the backoff check, which re-tests the
	// previous deferral's coverage against it: the index costs O(queue), which the drain
	// pass that just ran already paid, while the per-view baseline streams it gates are the
	// O(all baselines) part.
	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		return err
	}
	if e.repairDeferralHolds(quarantined, queued) {
		return nil
	}
	views, err := e.store.ListViews()
	if err != nil {
		return fmt.Errorf("engine: list views for quarantine repair: %w", err)
	}
	var deferred []string
	for _, v := range views {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A read-only view's pushes are suppressed exactly as localScan suppresses them: the
		// server rejects every upload/mkdir there, and applyOutbound's read-only guard turns
		// such an op into a StateConflict baseline plus a dead-letter trace. Re-deriving a push
		// into one would therefore convert a recoverable freeze into a permanent surfaced
		// conflict localReconcile never revisits. The INBOUND arms still run: pulling the
		// server's own content into a read-only view is exactly what that view is for.
		d, err := e.repairViewBaselines(ctx, v.ViewID, isReadOnlyView(v.Privilege), queued, dirCache)
		if err != nil {
			return err
		}
		deferred = append(deferred, d...)
	}
	if len(deferred) > 0 {
		// Stay armed: these candidates were not examined, and nothing else will re-arm the sweep.
		e.noteRepairDeferral(quarantined, deferred)
		return nil
	}
	e.repairDeferred = repairDeferral{}
	e.quarantineRepaired = quarantined
	return nil
}

// repairDeferralHolds reports whether the previous sweep's deferral still bars re-running this
// one. It is the backoff's only gate, and it yields to every form of new evidence:
//
//   - a different quarantine counter - a NEW quarantine armed the sweep, and the records it
//     stranded have nothing to do with the deferred ones;
//   - coverage that changed - a remembered key that no longer overlaps a queued op, or that a
//     queued op now OWNS, means the deferral is resolvable right now, which is the ordinary
//     temporary case (the covering op drained) and must not wait out a backoff;
//   - the backoff having elapsed - the forever case, re-examined at a low steady rate.
//
// The coverage re-test is O(deferred × depth) against the index the caller already built, so the
// cheap check gates the expensive per-view baseline streams rather than the other way round.
func (e *Engine) repairDeferralHolds(quarantined uint64, queued queuedIndex) bool {
	d := e.repairDeferred
	if len(d.keys) == 0 || d.quarantined != quarantined || !e.now().Before(d.retryAt) {
		return false
	}
	for _, k := range d.keys {
		if !queued.conflict(k) || queued.owns(k) {
			return false
		}
	}
	return true
}

// noteRepairDeferral records what this sweep deferred and schedules the next re-examination.
//
// A deferral identical to the previous one is the same unresolved condition observed again, so it
// stretches the backoff and stays silent; anything else - a different candidate set, or a
// different arming quarantine - is new evidence, so the backoff restarts at its base and the
// operator is told once more. The Warn therefore fires once per STREAK (the opWarned convention),
// not once per drain: the whole point is that an unresolvable deferral used to
// re-log itself for the lifetime of the daemon.
func (e *Engine) noteRepairDeferral(quarantined uint64, keys []string) {
	sort.Strings(keys)
	if len(keys) > repairDeferKeysCap {
		keys = keys[:repairDeferKeysCap]
	}
	prev := e.repairDeferred
	backoff, warned := repairDeferBackoffBase, false
	if prev.quarantined == quarantined && slices.Equal(prev.keys, keys) {
		if backoff = prev.backoff * 2; backoff > repairDeferBackoffMax {
			backoff = repairDeferBackoffMax
		}
		warned = prev.warned
	}
	if !warned {
		e.log.Warn("engine: some pending baselines stranded by a quarantined queue record overlap a still-queued op; the repair sweep stays armed and re-examines them with a backoff",
			"deferred", len(keys), "retry_in", backoff.String())
		warned = true
	}
	e.repairDeferred = repairDeferral{
		keys:        keys,
		quarantined: quarantined,
		warned:      warned,
		backoff:     backoff,
		retryAt:     e.now().Add(backoff),
	}
}

// repairViewBaselines runs the sweep over one view. The candidates are collected from
// the baseline stream FIRST and acted on after it closes: the store's cursor is live
// inside the read transaction, and the repair itself enqueues and writes baselines.
//
// Candidates are repaired in ascending path order so a stranded directory's re-derived
// OpMkdir is enqueued ahead of a stranded child's upload, which then resolves its parent
// id against the directory the mkdir has synced. The list is the snapshot taken before
// any repair, deliberately: folding each freshly enqueued op back into the index (the
// way localReconcileWith does) would make a repaired directory cover its own stranded
// children by path overlap and leave them frozen until the next quarantine, which may
// never come.
//
// Each candidate is ctx-checked before it is repaired: a repair hashes the whole file at its
// path, so a view holding N stranded records is O(sum of their bytes) of work the drain
// goroutine would otherwise have to finish before a pause or a shutdown could return (the
// bounded-work convention, and scanView's own pre-read check).
//
// It returns the resolved conflict keys of the candidates it DEFERRED - pending baselines no
// queued op owns but whose path overlaps one, so the coverage test cannot yet tell a legitimately
// owned record from a stranded one. That is the temporary case (a descendant of a queued directory
// rename, an unrelated op queued at an ancestor while the quarantine landed), so the caller keeps
// the sweep armed rather than marking the quarantine repaired with those records never examined.
// The keys, not merely their count, because the caller re-tests exactly this coverage before
// spending another sweep on it (repairDeferralHolds).
func (e *Engine) repairViewBaselines(ctx context.Context, viewID uint64, readOnly bool, queued queuedIndex, dirCache map[uint64]string) ([]string, error) {
	viewDir := e.viewDir(viewID, dirCache)
	var (
		stranded []store.PathBaseline
		deferred []string
	)
	// A corrupt (skipped) baseline record is not repairable here - its state and path are
	// what failed to decode - and the scan-side tolerance already freezes and protects it
	// (dropSkippedBaselineRows), so the skip report is deliberately not consulted.
	if _, err := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		// StatePending only. A StateConflict record is a SURFACED keep-both awaiting the
		// resolver or the operator and is owned by neither reconcile pass, so re-deriving a
		// push for it would silently overtake a conflict the user has been told about.
		if pb.State != store.StatePending {
			return nil
		}
		key := foldedKey(viewDir, pb.Path)
		if queued.conflict(key) {
			// Owned by a queued op: the record has its resolver and nothing is stranded. Merely
			// OVERLAPPING one proves nothing about ownership, and the overlap drains away - so
			// count it, and let the caller re-arm rather than close the quarantine over it.
			if !queued.owns(key) {
				deferred = append(deferred, key)
			}
			return nil
		}
		stranded = append(stranded, pb)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("engine: stream baselines for quarantine repair %d: %w", viewID, err)
	}
	sort.Slice(stranded, func(i, j int) bool { return stranded[i].Path < stranded[j].Path })
	for _, pb := range stranded {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := e.repairStrandedBaseline(viewID, pb, readOnly); err != nil {
			return nil, err
		}
	}
	return deferred, nil
}

// repairStrandedBaseline re-derives one pending baseline's owner from what is on disk at
// its path, exactly as localReconcile derives an op for a synced baseline:
//
//   - a regular file → the push or the pull the record itself carries evidence for, see
//     repairStrandedFile.
//   - a directory the server has never held → an outbound OpMkdir; a directory it already
//     tracks proves no direction at all, see repairStrandedMkdir.
//   - nothing of either kind (PROVABLY absent, or a symlink/socket/FIFO occupying the path) →
//     see repairStrandedAbsent: the inbound-shaped record re-derives its lost pull, everything
//     else is a phantom and is dropped.
//   - a path that could not be resolved or probed at all (a store error, EIO/EACCES/ELOOP, a
//     symlink component) → left frozen: an undecidable probe is no evidence of absence, and the
//     only drop this sweep makes must rest on a definitive one.
//
// A destructive op is never re-derived. localReconcile turns a vanished SYNCED baseline
// into an OpRemove because a synced record is proof of last agreement; a pending record is
// not, so an absent path here is read as "never materialised", never as "delete it".
func (e *Engine) repairStrandedBaseline(viewID uint64, pb store.PathBaseline, readOnly bool) error {
	abs, err := e.abs(viewID, pb.Path)
	if err == nil {
		// Resolve the real on-disk (possibly NFD) spelling before probing, mirroring every
		// other phantom test (dropStrandedBaseline): a bare NFC Lstat would
		// miss a live file materialised under its decomposed name and drop a real node's
		// record.
		var resolved string
		if resolved, err = e.onDiskAbsNoSymlink(abs); err == nil {
			abs = resolved
		}
	}
	if err != nil {
		// A resolution failure is NOT evidence of absence. e.abs fails on a GetView store
		// error or an ErrCorruptRecord, on any non-IsNotExist Lstat along the chain (EIO,
		// EACCES, ELOOP on a network or FUSE mount) and on errRejectedPath for a symlink
		// component - none of which says anything about whether the node is materialised.
		// Dropping here would also bypass repairStrandedAbsent, whose whole point is that an
		// inbound-shaped record must be re-derived rather than deleted (see its header): one
		// EIO, or one operator-created symlink, would turn the recovery sweep into a silent
		// mass-untrack of every stranded record under it. Leave the record frozen with a Warn
		// - the pre-repair status quo, and the same arm every other undecidable shape takes.
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its path could not be resolved, so no direction is derivable",
			"view", viewID, "path", pb.Path, "err", err)
		return nil
	}
	fi, statErr := os.Lstat(abs)
	switch {
	case statErr == nil && fi.Mode().IsRegular():
		return e.repairStrandedFile(viewID, pb, abs, readOnly)
	case statErr == nil && fi.IsDir():
		return e.repairStrandedMkdir(viewID, pb, fi, readOnly)
	case statErr != nil && !os.IsNotExist(statErr):
		// The probe itself failed (EIO on a flaky mount, EACCES on a directory whose mode the
		// operator changed): it proves neither presence nor absence, so it must not reach
		// repairStrandedAbsent's drop arm. Frozen is the pre-repair status quo; a later sweep
		// re-probes.
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its path could not be probed, so no direction is derivable",
			"view", viewID, "path", pb.Path, "err", statErr)
		return nil
	}
	return e.repairStrandedAbsent(viewID, pb, statErr)
}

// repairStrandedAbsent resolves a stranded pending baseline whose path holds neither a file nor
// a directory. Two shapes reach it and they are distinguishable:
//
//   - a FILE record carrying the server facts and no local ones (a file id, no local hash) was
//     written by markUpsertPending, whose op is the inbound download - and a download's node is
//     precisely one that has not landed on disk yet, so "absent" is the NORMAL state for this
//     shape, not evidence of a phantom. Dropping the record here deletes the only description of
//     the server node the lost op was pulling: the cursor has already advanced past its event, so
//     nothing re-derives it and the server's file never arrives locally - the exact freeze this
//     sweep exists to end. Re-derive the pull instead (repairStrandedDownload); it materialises
//     the server's own content with nothing on disk to clobber.
//   - anything else - a record with local content evidence, a directory record, a bare record -
//     is a node that is not materialised and whose direction the record cannot prove (an outbound
//     remove's flip and an inbound rename's relocation both leave a local-hash-carrying record at
//     a path holding nothing). It is dropped, mirroring the dead-letter cleanup's
//     dropStrandedBaseline. Dropping only UNTRACKS the path: nothing on disk or on the server is
//     touched, and a later server event or local create re-tracks it.
func (e *Engine) repairStrandedAbsent(viewID uint64, pb store.PathBaseline, statErr error) error {
	if pb.FileType != "dir" && pb.LocalHash == "" && pb.FileID != "" {
		return e.repairStrandedDownload(viewID, pb)
	}
	e.log.Warn("engine: dropping a pending baseline stranded by a quarantined queue record; no file or directory is materialised at its path",
		"view", viewID, "path", pb.Path, "err", statErr)
	return e.dropRepairedBaseline(viewID, pb.Path)
}

// repairStrandedFile picks the direction of the op that re-owns a stranded pending baseline
// whose path holds a regular file. The record's own fields are the only evidence of which
// direction the lost op had, and the two shapes are distinguishable:
//
//   - LocalHash non-empty → the record carries local content evidence. planUpload stamps the
//     scanned hash of the EDIT it is pushing, so the recorded local bytes differ from the last
//     agreed server content: re-derive the outbound push. flipPending - the INBOUND delete's
//     baseline flip - preserves a previously synced record verbatim instead, so its local hash
//     still equals the agreed server hash and no local change is derivable from it at all; see
//     repairStrandedUpload, which refuses to push that shape rather than re-creating server-side
//     a node another client deleted.
//   - LocalHash empty with a file id → the record was written by markUpsertPending, which
//     writes the SERVER facts and no local ones (baselineFromEvent fills LocalHash only for a
//     synced state). The lost op was the inbound download, so the on-disk file is the STALE
//     pre-download content and pushing it would overwrite the server's newer version - the
//     stale-base rejection that would otherwise catch it is not something this client can
//     prove (Upload sends no base version, DESIGN §13). Re-derive the inbound download
//     instead: applyDownload's own never-lose gate sees a present regular file under a
//     not-yet-synced baseline, funnels it through keep-both and preserves the local bytes as a
//     _Conflict copy before the server version lands (untrackedFilePresent).
//   - Neither - no local hash and no file id - is a shape neither pass produces for a file.
//     Leave it stranded with a Warn: there is no evidence in either direction, and the freeze
//     is the pre-repair status quo rather than a guess that could overwrite one side.
func (e *Engine) repairStrandedFile(viewID uint64, pb store.PathBaseline, abs string, readOnly bool) error {
	switch {
	case pb.LocalHash != "":
		if readOnly {
			e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its view is read-only, so the push it would derive can never land",
				"view", viewID, "path", pb.Path)
			return nil
		}
		return e.repairStrandedUpload(viewID, pb, abs)
	case pb.FileID != "":
		return e.repairStrandedDownload(viewID, pb)
	}
	e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; it carries neither a local hash nor a file id, so no direction is derivable",
		"view", viewID, "path", pb.Path)
	return nil
}

// repairStrandedDownload enqueues the inbound pull that re-owns a stranded pending baseline
// written by the inbound reconcile: the server facts recorded on the record (file id, hash,
// mtime, sync id) are exactly the ones opForEvent threads onto an OpDownload, so the re-derived
// op is the one the quarantined record carried. The baseline is NOT rewritten - it is already
// StatePending at this path, which is precisely what applyDownload's fresh-create gate reads as
// "a reconcile-produced download over content the server never saw".
//
// LocalSize stays zero, so the op takes the single-frame streaming Download rather than the
// chunked resume path; the baseline records no server size to thread, and that shape is already
// the accepted one for an op produced before LocalSize was carried (inbound_download.go).
func (e *Engine) repairStrandedDownload(viewID uint64, pb store.PathBaseline) error {
	e.log.Warn("engine: re-deriving the inbound download for a pending baseline stranded by a quarantined queue record",
		"view", viewID, "path", pb.Path, "file_id", pb.FileID, "server_hash", pb.ServerHash)
	op := store.Op{
		Type:         store.OpDownload,
		Direction:    store.DirInbound,
		ViewID:       viewID,
		Path:         pb.Path,
		FileID:       pb.FileID,
		ServerSyncID: pb.ServerSyncID,
		LocalHash:    pb.ServerHash,
		LocalMTime:   pb.ServerMTime,
	}
	if _, err := e.store.Enqueue(op); err != nil {
		return fmt.Errorf("engine: enqueue repaired %s %d/%q: %w", op.Type, viewID, op.Path, err)
	}
	return nil
}

// repairStrandedUpload enqueues the outbound push that re-owns a stranded pending baseline
// whose path holds a regular file. The content is hashed through the same no-follow,
// regular-file-only descriptor the upload path itself opens (hashLocalFileMD4WithInfo), so
// the recorded facts describe exactly the bytes that were read and a file swapped for a
// symlink or a FIFO between the Lstat and the hash is refused rather than pushed.
//
// A source that vanishes in that window leaves the record alone: dropping it would untrack
// a file that may still be there (the next scan would then push it as a duplicate create),
// and the freeze it preserves is merely the pre-repair status quo for a rare race.
//
// The hash is also the direction's last check, and it is applied only to a record the SERVER has
// already seen. A record with no file id names a node the server has never held - a first push of
// a local create - so no upload of it can re-create anything another client deleted and the
// content compare has nothing to compare against; it is derivable on its own.
//
// For a record carrying a file id, a disk content still equal to the last agreed SERVER content
// carries no local change to push: the record was flipped to pending by something other than a
// local edit, and the one producer of that exact shape is the INBOUND delete's flipPending, which
// preserves a synced record verbatim. Pushing it would send the server bytes it already has -
// and, when the flip's queued OpRemove is the record the store quarantined, Upload is parent+name
// addressed (it carries no base version, DESIGN §13), so the push would RE-CREATE the node
// another client deleted and undo that deletion on every client. There is no evidence in either
// direction there, so the record is left frozen with a Warn: the pre-repair status quo, and the
// same arm a record carrying no direction evidence at all takes.
//
// That agreed content is read as the ServerHash, falling back to the LocalHash when the record
// carries none - the same fallback removeTargetFacts makes for the same reason. The two
// describe the same bytes at the last agreement, and a DSM that omits the hash from its upload
// reply (or a hash-less cold-start catalog adopted on size+kind) leaves ServerHash empty on a
// perfectly ordinary synced record. Without the fallback the guard is inert for every such
// record and the flipPending shape pushes after all, which is precisely the deletion-undo this
// arm exists to refuse. The price is the mirror case - a file id-carrying record whose lost op
// WAS a push and whose bytes have not changed since the flip, on a server that reports no
// hashes - which stays frozen with the Warn naming it, exactly as it was before this sweep
// existed. Never-lose settles the trade: unsynced-and-observable over a silent resurrection.
func (e *Engine) repairStrandedUpload(viewID uint64, pb store.PathBaseline, abs string) error {
	sum, fi, err := e.hashLocalFileMD4WithInfo(abs)
	if err != nil {
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its file could not be read",
			"view", viewID, "path", pb.Path, "err", err)
		return nil
	}
	if agreed := repairAgreedHash(pb); pb.FileID != "" && agreed != "" && sum == agreed {
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its file still holds the last agreed server content, so no outbound change is derivable",
			"view", viewID, "path", pb.Path, "file_id", pb.FileID, "hash", sum)
		return nil
	}
	en := scanEntry{
		relPath:  pb.Path,
		size:     fi.Size(),
		mtime:    fi.ModTime().Unix(),
		unixPerm: modeToDecimal(fi.Mode()),
		md4:      sum,
	}
	e.log.Warn("engine: re-deriving the outbound push for a pending baseline stranded by a quarantined queue record",
		"view", viewID, "path", pb.Path, "file_id", pb.FileID, "hash", sum)
	// No divergence-base capture: the record is StatePending, so its LocalHash is already the
	// un-pushed edit rather than the last agreed content. Handing that to the H1 guard would
	// compare the server against the very bytes being pushed and read every ordinary re-derived
	// push as divergence. The record's ServerHash (uploadDivergenceBase's primary base) is
	// preserved and still applies.
	return e.enqueueRepairedOp(viewID, e.planUpload(viewID, en, pb.FileID, pb.Path, "", false))
}

// repairAgreedHash is the content the record describes as last agreed with the server: its
// ServerHash, or its LocalHash when the reply that produced it carried no hash (see
// repairStrandedUpload / removeTargetFacts). Empty means the record describes no agreed
// content at all.
func repairAgreedHash(pb store.PathBaseline) string {
	if pb.ServerHash != "" {
		return pb.ServerHash
	}
	return pb.LocalHash
}

// repairStrandedMkdir enqueues the outbound directory create that re-owns a stranded
// pending baseline whose path holds a directory.
//
// Only a directory the server has never held (no file id) is derivable this way. A directory
// record IS its own content - there is no hash to compare - so for a node the server already
// knows, the shape a stranded outbound mkdir leaves and the shape an INBOUND delete's flipPending
// leaves are identical, and re-deriving the push would re-create server-side a directory another
// client deleted: the directory twin of repairStrandedUpload's refusal. Such a record is left
// frozen with a Warn, the pre-repair status quo.
func (e *Engine) repairStrandedMkdir(viewID uint64, pb store.PathBaseline, fi os.FileInfo, readOnly bool) error {
	if readOnly {
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its view is read-only, so the directory create it would derive can never land",
			"view", viewID, "path", pb.Path)
		return nil
	}
	if pb.FileID != "" {
		e.log.Warn("engine: leaving a pending baseline stranded by a quarantined queue record; its directory is already tracked server-side, so no direction is derivable from it",
			"view", viewID, "path", pb.Path, "file_id", pb.FileID)
		return nil
	}
	en := scanEntry{
		relPath:  pb.Path,
		isDir:    true,
		mtime:    fi.ModTime().Unix(),
		unixPerm: modeToDecimal(fi.Mode()),
	}
	e.log.Warn("engine: re-deriving the outbound directory create for a pending baseline stranded by a quarantined queue record",
		"view", viewID, "path", pb.Path, "file_id", pb.FileID)
	return e.enqueueRepairedOp(viewID, e.planMkdir(viewID, en))
}

// enqueueRepairedOp commits one re-derived op in the same order localReconcile commits its
// planned ops: the durable Enqueue FIRST, the baseline flip second, so a crash in between
// leaves a replayable queued op rather than re-creating the very stranded-pending state
// this sweep exists to repair.
func (e *Engine) enqueueRepairedOp(viewID uint64, p plannedOp) error {
	if _, err := e.store.Enqueue(p.op); err != nil {
		return fmt.Errorf("engine: enqueue repaired %s %d/%q: %w", p.op.Type, viewID, p.op.Path, err)
	}
	return p.apply(e.store)
}

// dropRepairedBaseline untracks a stranded pending record whose node is not materialised on
// disk. An already-absent record is fine (a concurrent apply may have resolved it), which
// DeleteBaseline treats as a no-op.
func (e *Engine) dropRepairedBaseline(viewID uint64, path string) error {
	if err := e.store.DeleteBaseline(viewID, path); err != nil && !isNotFound(err) {
		return fmt.Errorf("engine: drop stranded pending baseline %d/%q: %w", viewID, path, err)
	}
	return nil
}
