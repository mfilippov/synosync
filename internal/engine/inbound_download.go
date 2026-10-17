package engine

// This file carries the inbound OpDownload apply path: it streams a file from the
// server over a leased worker connection into a staging temp, atomically promotes it
// onto the target path, and advances the baseline to synced. It also holds the
// Phase-9d clobber guard (localDivergedFrom) and the keep-both funnel
// (funnelDownloadConflict) that protects a locally diverged file from being overwritten.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// downloadStagePerm is the placeholder mode a downloaded file's staging temp is
// chmodded to before it is renamed into place; the real permission bits are applied
// afterwards by fsutil.SetMeta from the op's wire-decimal UnixPerm (which also
// handles the Windows skip).
const downloadStagePerm = 0o644

// Staging-temp name budget. os.CreateTemp(dir, "."+base+".dl-*")
// inflates the target's base name by a leading dot, the ".dl-" marker and CreateTemp's
// random suffix - a decimal uint32, so at most 10 digits (os/tempfile.go nextRandom
// formats uint64(uint32(rand))). A base name near the per-component limit therefore
// produced a staging name PAST it, and the resulting ENAMETOOLONG is neither fs.ErrExist
// nor a recognised terminal error, so the op retried every drain forever, escalated to
// Stuck and - via blockedKeys - wedged every overlapping op behind it. That is
// self-inflicted: conflictCopyName deliberately emits names of exactly
// maxCopyNameComponentBytes, so our own keep-both copy would wedge the download of it on
// a second client (and on this one after a re-pair/cold start). stagingTempPattern keeps
// the generated name inside the same limit conflictCopyName budgets against.
const (
	// stagingDotPrefix hides the temp and marks it as ours; stagingMarker plus
	// stagingRandomMaxBytes cover CreateTemp's generated tail.
	stagingDotPrefix      = "."
	stagingMarker         = ".dl-"
	stagingRandomMaxBytes = 10
	// maxStagingBaseBytes is what is left of maxCopyNameComponentBytes (255, the
	// NAME_MAX/Windows per-component cap - see conflict.go) for the copied base name.
	maxStagingBaseBytes = maxCopyNameComponentBytes - len(stagingDotPrefix) - len(stagingMarker) - stagingRandomMaxBytes
)

// stagingTempPattern builds the os.CreateTemp pattern for a download's staging temp in
// the target's parent directory, truncating base - deterministically, on a UTF-8 rune
// boundary so a multibyte rune is never split, exactly as conflictCopyName does - so the
// longest name CreateTemp can generate from it stays within maxCopyNameComponentBytes.
// The truncation only shortens the human-readable middle: the leading dot and the
// ".dl-<digits>" tail are preserved, so the watcher's staging-name filter
// (watcher/filter.go isStagingName) still recognises the temp as self-induced, and the
// temp is removed by the promote either way.
func stagingTempPattern(base string) string {
	return stagingDotPrefix + truncateUTF8(base, maxStagingBaseBytes) + stagingMarker + "*"
}

// applyDownload streams a file from the server over a worker connection into a
// staging temp file in the destination directory, then atomically promotes it onto
// the target path and applies its metadata. Download verifies the content MD4 itself
// and may leave partial bytes in the temp file on error, so the temp is only
// promoted (fsynced and renamed into place) on a clean download and is always
// removed afterwards. The baseline is then advanced to synced with the
// server-reported facts.
func (e *Engine) applyDownload(ctx context.Context, op store.Op) error {
	// A SELF-ECHO move (our own already-pushed rename) can relocate this node AFTER the download
	// was enqueued while enqueuing NO inbound OpRename to carry the file - self-echo events skip
	// the queue (applySelfEchoMove). op.Path is then the stale pre-move server path, empty on
	// disk: materialising the download there would produce a ghost at the old name, while
	// removeRelocatedPriorFile below deletes the REAL relocated file (with no _Conflict copy) and
	// markSynced evicts the correct baseline through the byid index - a HIGH data-loss plus a
	// permanent silent divergence. Redirect the apply path to the node's CURRENT tracked
	// location when downloadRelocatedBySelfEcho confirms that shape; once redirected op.Path
	// equals the byid path, so removeRelocatedPriorFile no-ops (prev == op.Path). The
	// non-self-echo [modify a, move a->b] batch is deliberately NOT redirected - its
	// queued inbound OpRename carries the download from op.Path behind it as designed. The
	// redirected write stays inside the op's declared conflict-key set: the scheduler resolves
	// this same byid path into the op's serialize keys (opSerializeKeys), so
	// rebasing op.Path here never mutates a location the drain did not order this op against.
	if op.FileID != "" {
		cur, perr := e.store.PathByID(op.ViewID, op.FileID)
		switch {
		case isNotFound(perr):
		case perr != nil:
			return fmt.Errorf("engine: resolve download apply path %d/%q: %w", op.ViewID, op.FileID, perr)
		case cur != op.Path:
			redirect, rerr := e.downloadRelocatedBySelfEcho(op, cur)
			if rerr != nil {
				return rerr
			}
			if redirect {
				e.log.Info("engine: redirecting inbound download to the node's current path (self-echo move relocated it with no queued rename to carry it)",
					"view", op.ViewID, "file_id", op.FileID, "stale_path", op.Path, "current_path", cur)
				op.Path = cur
			}
		}
	}
	abs, err := e.abs(op.ViewID, op.Path)
	if err != nil {
		return err
	}
	// On a byte-preserving filesystem (Linux, Windows) the target may live under a non-NFC
	// (NFD) spelling while the op carries the canonical NFC path (Phase 9b). Resolve the real
	// on-disk spelling before the divergence gates and the promote, mirroring applyRemove and
	// applyRename: the gates already hash the NFD file through
	// openUploadSource's fallback, but a promote onto the literal NFC path would os.Rename the
	// staged temp into a SECOND directory entry next to the NFD original (whose stale content
	// survives) - the next scan folds both into one normpath.Key, scanKeyCollisions protects
	// both, and the path stops syncing until manual repair. The prefix resolution keeps
	// not-yet-existing components literal, so a fresh create still materialises under NFC
	// while an existing NFD parent directory is reused rather than duplicated.
	abs, err = e.onDiskAbsPrefixNoSymlink(abs)
	if err != nil {
		return err
	}

	// replace selects the promote policy below: a target that legitimately holds a tracked
	// file is overwritten atomically (os.Rename), while a target that held NOTHING when the
	// guards below ran must not clobber a file that appears DURING the transfer - the promote
	// is a no-replace rename and an intruder funnels through keep-both.
	replace := true

	// Phase-9d safety net: before overwriting an existing local file, guard against silently
	// losing an unrecorded local edit. The cheap scan (scanner.go) reuses the baseline hash
	// for a regular file whose on-disk (size, mtime) still match its baseline, so a local edit
	// that keeps the same size within the same mtime-second queues no outbound upload. If a
	// remote modify then arrives, reconcile finds no pending upload to funnel through keep-both
	// and we would clobber the local edit here - breaking the "a missed cheap-scan change only
	// adds latency, never loses data" invariant. Detect that divergence (only meaningful when a
	// last-synced base hash was captured at reconcile time) and route it to the keep-both
	// resolver instead of overwriting. The check runs before the download so a funneled conflict
	// costs no transfer; it is re-run just before the promote (recheck below) so an edit made
	// WHILE the transfer streamed is caught too.
	// baseHash is the divergence base the pre-check above ran against and the recheck below
	// re-runs against: the reconcile-captured pre-batch hash for a modify, or - for a
	// fresh-create op whose target already holds a prior same-batch download's output - the
	// live synced baseline's hash (see the else branch). Empty means no base exists and no
	// recheck runs.
	baseHash := op.BaseLocalHash
	if op.BaseLocalHash != "" {
		diverged, derr := e.localDivergedFrom(abs, op.BaseLocalHash)
		if derr != nil {
			return derr
		}
		if diverged {
			return e.funnelDownloadConflict(op)
		}
	} else {
		// Fresh server create (no last-synced base captured): if a regular local file already
		// occupies the target path, it is untracked user content the download would clobber with
		// an os.Rename. Route it through the keep-both resolver instead - it preserves the local
		// file as a _Conflict copy and downloads the server version onto the canonical path, and
		// fast-paths the case where the two already match. This closes the cluster-A gap where the
		// BaseLocalHash != "" guard above did not run for a brand-new server node (Law #1).
		present, occupied, liveBase, oerr := e.untrackedFilePresent(abs, op)
		if oerr != nil {
			return oerr
		}
		if occupied {
			return e.funnelDownloadConflict(op)
		}
		// A matching occupant under a synced-with-hash baseline is a prior same-batch
		// download's output (a create+modify batch: both ops carry an empty BaseLocalHash);
		// carry that live hash into the recheck so an edit landing WHILE this transfer
		// streams is caught exactly like the modify path's, instead of being replaced with
		// no comparison at all.
		baseHash = liveBase
		// A fresh-create target with no regular file present is promoted no-replace, so user
		// content that lands there during the transfer fails the rename with fs.ErrExist and
		// funnels through keep-both below instead of being silently clobbered.
		// A present file this guard decided to overwrite directly - a synced or
		// no-baseline occupant, see untrackedFilePresent - keeps the replacing promote, which
		// a no-replace rename could never land.
		replace = present
	}

	var node actions.Node
	fill := func(tmp *os.File) (int64, error) {
		if op.LocalSize > e.limits.UploadResumeThreshold {
			// Large file: stream it in offset-based resume_download chunks so it is never
			// buffered whole (DESIGN §4), mirroring the outbound resumeUploadFile path. An op
			// produced before LocalSize was threaded onto OpDownload (or a genuinely small
			// file) has LocalSize <= threshold and keeps the single-frame Download below.
			n, werr := e.resumeDownloadToTemp(ctx, op, tmp)
			if werr != nil {
				return 0, werr
			}
			node = n
		} else {
			werr := e.withWorker(ctx, func(c syncClient) error {
				n, derr := c.Download(ctx, op.FileID, e.viewRootNodeID(op.ViewID), op.ServerSyncID, tmp)
				node = n
				return derr
			})
			if werr != nil {
				return 0, werr
			}
		}
		// Derive whatever the tolerantly-decoded reply omitted from the staging temp itself,
		// while it still holds exactly the bytes that streamed and nothing else can reach it
		// - never from the promoted path after the rename.
		if err := deriveStagedDownloadFacts(tmp, &node); err != nil {
			return 0, err
		}
		return node.MTime, nil
	}
	// The modify path re-validates the target right before the rename: the pre-transfer guard
	// above leaves the whole transfer as a window in which a local edit could land and be
	// replaced - and once replaced the on-disk content equals the baseline, so the next scan
	// sees nothing and the edit is gone for good. The recheck shrinks that window from
	// transfer-length to the few syscalls between the re-hash and the rename.
	var recheck func() error
	if baseHash != "" {
		recheck = func() error {
			diverged, derr := e.localDivergedFrom(abs, baseHash)
			if derr != nil {
				return derr
			}
			if diverged {
				return errPromoteTargetDiverged
			}
			return nil
		}
	}
	localMTime, err := e.promoteStagedDownload(abs, replace, op.UnixPerm, fill, recheck)
	if err != nil {
		if errors.Is(err, errPromoteTargetDiverged) {
			// The target was edited while the transfer streamed; the staged temp is discarded
			// and the edit funnels through keep-both exactly like a pre-transfer divergence.
			return e.funnelDownloadConflict(op)
		}
		if !replace && errors.Is(err, fs.ErrExist) {
			return e.funnelFreshCreateIntruder(op, abs, err)
		}
		if replace && promoteFailedAtRename(err) && e.promoteBlockedByDirOccupant(abs) {
			// A REPLACING promote met a directory. The divergence gate proved the target held
			// the last-agreed base, and the recheck re-proved it moments ago - so the directory
			// arrived inside the recheck→rename window, or replaced a file whose kind the
			// recheck cannot see (localDivergedFrom hashes, and a directory answers
			// errUploadSourceGone, i.e. "not diverged"). Either way what stands there now is not
			// what this op was entitled to overwrite.
			//
			// Without this the op dead-letters, and the never-lose answer for a directory in the
			// way (preserve it, land the file) would apply or not depending on WHEN the
			// directory appeared rather than on what is there. It is dispatched by Lstat rather
			// than by errno on purpose: Linux answers fs.ErrExist here (os.Rename's userspace
			// dir-guard, measured - not EISDIR or ENOTEMPTY) while Windows answers
			// ERROR_ACCESS_DENIED → fs.ErrPermission, and a rule written on either errno would
			// hold on one platform only.
			return e.preserveDirOccupantAndRetry(op, abs)
		}
		if op.FileID != "" && serverRejectedDownload(err) && e.downloadNodeIsDir(ctx, op) {
			// The server places THIS node (id-matched) at a DIRECTORY. The download verb
			// addresses "id:<file id>" and can never materialise one, so the op is doomed by
			// KIND rather than by absence - and inbound escalation is deliberately closed
			// (servererr.go), so without this exit it retries every drain forever behind the
			// paced backoff while blockedKeys holds every path-overlapping op behind it. That
			// is the permanent queue wedge compensatedKind's residual walks into: a dropped
			// inbound rename of a DIRECTORY compensates with a download whenever no id-matched
			// baseline survives to state the kind, because a sparse move event reports is_dir
			// false whatever the node really is. The evidence for the conversion comes from the
			// server instead, at the one moment it is both cheap and decisive - after the
			// transfer has already failed. applyOp turns this into an OpMkdir at the same path -
			// the path THIS op resolved, which the redirect above may have rebased away from the
			// queue record's spelling, so it rides along with the signal (downloadIsDirAt).
			return &downloadIsDirAt{path: op.Path}
		}
		if errors.Is(err, actions.ErrPathError) && op.FileID != "" && e.downloadNodeGone(ctx, op) {
			// The download turn failed with the path-error shape AND the node is positively
			// confirmed absent server-side (strict id-matched parent listing): the node was
			// destroyed without a delete event this client processed - the canonical producer
			// is an overwrite-move whose event reconcile could not see (it never arrived, or
			// arrived after this op was already queued and unmatched by the supersession
			// machinery). The download can never complete, so surface the typed race signal
			// for applyOp to resolve (drop + baseline cleanup) instead of retrying forever
			// with the authoritative rename/remove wedged behind it.
			// An id-less download never probes: a name-only listing cannot positively
			// identify the node (mirroring renameSourceNodeNotAtPath's id gate), so it stays on
			// the transient path.
			return errDownloadNodeGone
		}
		return err
	}

	// A rejected rename may have re-tracked this node (by file id) at its still-reachable
	// source while the server path stayed unappliable; the original file then sat there
	// untracked-but-on-disk. Now that the download has landed the node at op.Path, that prior
	// copy is stale and must be removed before markSynced re-points the byid index away from
	// it - otherwise the file survives untracked and a later server delete (which targets only
	// op.Path) can never reach it.
	if err := e.removeRelocatedPriorFile(ctx, op); err != nil {
		return err
	}

	// node.Hash/node.Size are complete here: the reply may have omitted either (the
	// single-frame Download skipped its MD4 check with a Warn, and the resume path streamed
	// without whole-file verification), but deriveStagedDownloadFacts filled them in from the
	// sealed staging temp inside fill - see its doc for why the promoted path is NOT a legal
	// source for them.
	//
	// LocalMTime is the deliberate exception: it is an on-disk value by contract, not a content
	// fact, so it is the mtime promoteStagedDownload observed the filesystem actually store for
	// these bytes (stagedOnDiskMTime) - never the raw wire value, which the disk may hold in a
	// coarser or skipped form.
	return e.markSynced(op.ViewID, op.Path, func(b *store.Baseline) {
		b.FileID = op.FileID
		b.FileType = fileType(false)
		b.ServerHash = node.Hash
		b.ServerMTime = node.MTime
		if op.ServerSyncID > b.ServerSyncID {
			// Advance the recorded server position, but NEVER rewind it (a separate non-zero
			// guard is subsumed - a zero op.ServerSyncID never exceeds a stored value).
			// Monotonicity is load-bearing for the self-echo redirect: when a self-echo
			// move already relocated this baseline to op.Path (the redirect at the top of
			// applyDownload) and overlaid its NEWER sync id, lowering it to this OLDER download
			// event's id would destroy downloadRelocatedBySelfEcho's "cur is strictly newer"
			// proof - a crash-replay of this download (the durable queue op still carries the
			// stale pre-move path) would then fail to redirect, materialise a ghost at the old
			// path and delete the real relocated file (the exact self-echo data loss, reopened in
			// the at-least-once replay window between this PutBaseline and the Ack).
			b.ServerSyncID = op.ServerSyncID
		}
		b.LocalHash = node.Hash
		b.LocalMTime = localMTime
		b.LocalSize = node.Size
		b.UnixPerm = op.UnixPerm
	})
}

// stagedOnDiskMTime stamps the server-reported wireMTime onto the SEALED staging temp and
// reports the mtime the filesystem actually stored for it - which is exactly what the promoted
// file will carry, since a rename never alters a file's mtime and the temp lives in the target's
// own directory. It is the single source of every downloaded baseline's LocalMTime.
//
// The wire value cannot be recorded verbatim, because three separate things make the disk hold
// something else:
//
//   - Timestamp granularity. FAT/FAT32 stores 2-second resolution, and coarse network/FUSE
//     mounts round too, so what Chtimes was asked to store is not what a later stat reads back.
//     NTFS/ext4/APFS keep whole seconds exactly, which is why this only
//     ever bites on the odd mount.
//   - Out-of-range values. fsutil.SetMeta warns and SKIPS the stamp for a negative mtime or one
//     past the nanosecond-overflow bound (fsutil.MTimeOutOfRange), leaving the temp's ~now write
//     timestamp.
//   - A zero (unset) wire mtime. SetMeta leaves the modification time untouched, so the disk
//     again holds the write timestamp, not 0.
//
// In every one of those cases a baseline holding the wire value builds a cheap-scan (size, mtime)
// hint (loadScanHints/collectBaselines) that never matches the file again, so scanView re-opens
// and MD4-re-hashes it on EVERY scan, forever, with no Warn and no self-healing - the content
// hash agrees, so the baseline is never rewritten. Reading the value from disk instead is the
// mtime mirror of markSynced's hash/size derive-from-disk fallbacks and of adoptFalseConflict's
// "record the ACTUAL on-disk facts".
//
// It reads the TEMP, never the live path after the promote: a path reachable by every other
// writer on the host is not evidence about the bytes this download landed (the same
// reason keeps deriveStagedDownloadFacts on the temp). The temp is
// written by fill alone, so its mtime provably describes them. The stamp is not fsynced - the
// post-promote fsutil.SetMeta re-applies the identical value and is not either - so a crash can
// still leave the mtime unwritten; that costs one re-hash and heals itself, unlike the recorded
// mismatch this closes.
func (e *Engine) stagedOnDiskMTime(tmpName string, wireMTime int64) (int64, error) {
	// A wire mtime SetMeta would refuse to stamp is not stamped here either. With perm 0 the
	// call is already a no-op for both refusal shapes (SetMeta zeroes an out-of-range value and
	// then returns early on the zero), so skipping changes nothing on disk - but it keeps the
	// out-of-range Warn from firing TWICE per download, the first time naming a `.dl-` staging
	// temp that is deleted moments later and can never be inspected. The post-promote SetMeta
	// still reports it, against the path the operator can actually look at.
	if wireMTime != 0 && !fsutil.MTimeOutOfRange(wireMTime) {
		// Perm 0: the staging temp keeps downloadStagePerm here and the op's UnixPerm is applied
		// to the promoted path afterwards, so only the mtime axis is stamped this early.
		if err := e.fsSetMeta(tmpName, wireMTime, 0); err != nil {
			return 0, fmt.Errorf("stamp mtime on staging file %q: %w", tmpName, err)
		}
	}
	fi, err := os.Lstat(tmpName)
	if err != nil {
		return 0, fmt.Errorf("stat staging file for mtime %q: %w", tmpName, err)
	}
	return fi.ModTime().Unix(), nil
}

// downloadRelocatedBySelfEcho reports whether an inbound download's node was relocated to cur by
// a SELF-ECHO move (our own already-pushed rename) after the download's event, so op.Path is now
// a stale, on-disk-empty path and the download must land at cur instead. Two independent
// facts must both hold:
//
//   - The node's CURRENT tracked baseline (at cur) is StateSynced with a ServerSyncID strictly
//     newer than the download event's (op.ServerSyncID). A self-echo move overlays the move
//     event's higher sync id onto the synced baseline (overlaySelfEchoMoveFacts), so cur reflects
//     a server position newer than the download - positive proof op.Path was superseded by a
//     later move to cur. A rejected-rename re-track (the legitimate download-lands-at-op.Path case
//     removeRelocatedPriorFile exists to clean up) leaves cur at the SAME or an older sync id, so
//     it is NOT redirected - the download stays at op.Path (the server-authoritative target) and
//     the stale copy at cur is cleaned up as before.
//   - No queued inbound OpRename of this file id is pending. A non-self-echo [modify a, move a->b]
//     batch enqueues such a rename to carry the download from op.Path to b, so the
//     download must ride behind it; a self-echo move enqueues nothing, so nothing would carry it.
//
// A missing/corrupt baseline at cur, or a scan failure, is conservative: the first returns
// false (do not redirect), the second propagates so the op stays queued to retry rather than
// redirecting on incomplete information.
//
// A StatePending record at cur reads "do not redirect" safely because the stale-racer shape
// that used to hide behind it is removed at enqueue time: a trailing
// modify of the same node flips cur StatePending through markUpsertPending, but its own
// enqueue first drops this (older) download as superseded (dropSupersededSameIDDownloads),
// so no download is left to mis-apply at the stale path with the gate extinguished - and the
// scheduler serialises a remaining download on its byid-resolved current path too
// (opSerializeKeys), so the residual shapes cannot race on disjoint keys.
func (e *Engine) downloadRelocatedBySelfEcho(op store.Op, cur string) (bool, error) {
	b, err := e.store.GetBaseline(op.ViewID, cur)
	switch {
	case isNotFound(err), errors.Is(err, store.ErrCorruptRecord):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("engine: load relocated download baseline %d/%q: %w", op.ViewID, cur, err)
	}
	if b.State != store.StateSynced || op.ServerSyncID == 0 || b.ServerSyncID <= op.ServerSyncID {
		return false, nil
	}
	carried, err := e.queuedInboundRenameCarries(op.ViewID, op.FileID)
	if err != nil {
		return false, err
	}
	return !carried, nil
}

// errDownloadNodeGone marks an inbound OpDownload whose node is positively confirmed absent
// server-side: the node was destroyed with no delete event this client
// processed. The canonical producer is an overwriting move - a server move claiming the
// download's path for a DIFFERENT node destroys the occupant without an explicit delete -
// which reconcile's supersession machinery now recognises for the event shapes it can SEE;
// this exit is the apply-time backstop for the shapes it cannot (the event never arrived,
// arrived unparseably, or landed on an ancestor path the conservative exact-path match
// deliberately skips). The download can never complete, and every conflicting successor (the
// authoritative rename/remove behind it on the shared path key) would skip behind the failure
// forever, so applyOp resolves it as a race: drop the phantom pending baseline
// (dropStrandedBaseline), ack WITHOUT a durable dead-letter trace (mirroring
// errRenameSourceGone - a two-client race resolution, not an operator-facing drop), and
// signal errDeadLettered so a same-id rename fed by this download is doomed rather than
// dragging unrelated source bytes across the tree.
var errDownloadNodeGone = errors.New("engine: inbound download node gone server-side")

// errDownloadNodeIsDir marks an inbound OpDownload whose node the SERVER positively places at a
// DIRECTORY. Unlike errDownloadNodeGone this is not a race resolution and nothing is dropped: the
// node exists and must still reach the replica, only under the other verb. applyOp converts the
// op into an inbound OpMkdir at the same path, in the same transaction as the trace and the Ack.
//
// The exit exists because the kind of a re-requested node is not always knowable at the moment it
// is re-requested. dropRenameCompensated types its repair from compensatedKind, whose witnesses
// are all id-matched baselines - and a byid MISS is the precondition of every arm that
// compensates, so in exactly those shapes the only surviving witness is the move event's own
// is_dir, which a sparse row reports false whatever the node really is (store.Op.MovedBaseIsDir's
// doc comment). Typing the repair optimistically as an OpMkdir instead would be worse: it
// markSyncs a directory carrying a server FILE's id and a later local delete pushes an outbound
// remove of that live file. So the repair stays a download, the pessimistic direction, and the
// wedge that direction used to carry is closed HERE, on evidence the server itself supplies.
var errDownloadNodeIsDir = errors.New("engine: inbound download node is a directory server-side")

// downloadIsDirAt is the errDownloadNodeIsDir signal carrying the path the probe actually asked
// about, which is not always the path the QUEUE record holds.
//
// applyDownload takes op BY VALUE and may rebase op.Path onto the node's current tracked location
// before the transfer (the self-echo move redirect at the top of this file); downloadNodeIsDir
// then probes THAT path, while applyOp still holds the original spelling. Building the
// replacement OpMkdir from the stale path would place the directory where the node no longer is:
// markSynced there evicts the live baseline through the byid index (putBaselineTx), and a
// directory record still standing at the stale path makes relocateTrackedDirForMkdir rename the
// user's just-relocated subtree back to the old name - disk and baseline then agree, so
// localReconcile never re-derives anything and the divergence is silent and permanent. The
// resolved path therefore travels with the signal.
//
// Unwrap returns errDownloadNodeIsDir, so every errors.Is classifier keeps matching unchanged.
type downloadIsDirAt struct{ path string }

func (d *downloadIsDirAt) Error() string {
	return fmt.Sprintf("engine: inbound download node is a directory server-side at %q", d.path)
}

// Unwrap keeps the typed signal indistinguishable from the bare sentinel for every classifier.
func (d *downloadIsDirAt) Unwrap() error { return errDownloadNodeIsDir }

// serverRejectedDownload reports whether a failed download turn failed with a SERVER VERDICT - a
// typed result code, or the shared path-error sentinel the absence provers consume - as opposed
// to a local failure (a full disk, a promote error) or a transport blip. Only a verdict is worth
// spending a probe turn on: a local failure says nothing about the node, and the kind probe is
// gated on it for the same reason downloadNodeGone gates on ErrPathError.
func serverRejectedDownload(err error) bool {
	var se *actions.ServerError
	return errors.As(err, &se) || errors.Is(err, actions.ErrPathError)
}

// downloadNodeIsDir reports whether an inbound download's own node is positively a DIRECTORY
// server-side, by querying its path over a fresh worker lease (run strictly AFTER the download
// turn's own lease returned, like downloadNodeGone, so no nested lease can deadlock at
// numWorkers=2).
//
// Only POSITIVE proof returns true, on both axes, because the caller converts the op on it:
//   - KIND. A sparse reply omits is_dir, so false is no evidence either way; a positively set
//     is_dir is proof, the same measured witness removeTargetFacts' probe rests on (a flag the
//     server cannot echo back from the request).
//   - IDENTITY. The download addresses "id:<file id>" while this probe addresses the PATH, so a
//     directory answering it may be a foreign occupant standing where our FILE should land -
//     converting on that would record the stranger's node under our id. The reply must name our
//     node. decodeNode tolerates an omitted file_id, and an omission is not a match: an
//     unidentified directory leaves the op exactly where it was, queued and retrying.
//
// A lease failure, a transport error or a path-error answer all report false for the same reason:
// the op keeps its historical transient fate rather than being converted on a non-answer.
func (e *Engine) downloadNodeIsDir(ctx context.Context, op store.Op) bool {
	vid := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	var isDir bool
	err := e.withWorker(ctx, func(c syncClient) error {
		n, qerr := c.QueryNode(ctx, vid, op.Path)
		if qerr != nil {
			return qerr
		}
		isDir = n.IsDir && n.FileID != "" && n.FileID == op.FileID
		return nil
	})
	return err == nil && isDir
}

// downloadNodeGone reports whether an inbound download's node is unambiguously gone
// server-side, by listing its parent directory over a fresh worker lease (run strictly AFTER
// the download turn's own lease returned, like parentGoneConfirmed, so no nested lease can
// deadlock at numWorkers=2). The match is STRICTLY by file id (nodeGoneByID): a child merely
// occupying the leaf name under a different id is exactly the overwrite-move occupancy this
// probe exists to see through, so - unlike the remove verb's path-occupancy classifiers - it
// does not read as presence. Only positive proof returns true; a listing that still shows the
// node by id, an id-less child that could hide it, a lease failure or a probe timeout all
// report false so the caller keeps the op queued rather than dropping it on a transient blip.
func (e *Engine) downloadNodeGone(ctx context.Context, op store.Op) bool {
	vid := int64(op.ViewID) // #nosec G115 -- a view_id is a non-negative identifier
	var gone bool
	err := e.withWorker(ctx, func(c syncClient) error {
		pc, ok := c.(outboundClient)
		if !ok {
			return errNoOutbound
		}
		g, cerr := nodeGoneByID(ctx, pc, e, vid, op.Path, op.FileID)
		if cerr != nil {
			return cerr
		}
		gone = g
		return nil
	})
	return err == nil && gone
}

// localDivergedFrom reports whether the on-disk file at abs holds content that differs
// from the last-synced baseHash (the reconcile-captured Op.BaseLocalHash, or - for the
// fresh-create branch's synced occupant - the live baseline's LocalHash). It is the Phase-9d
// safety net for the inbound download path: before an inbound download overwrites the file, it
// confirms the file has not been edited since it was last synced.
//
// It re-hashes the file UNCONDITIONALLY. A (size, mtime) short-circuit like the cheap scan's
// cannot be used here: the very gap this guards is a same-size, same-mtime-second edit the cheap
// scan is blind to, so a (size, mtime) check would reproduce that exact blind spot and let the
// overwrite through. The cost is bounded - one hash of the existing file per inbound download
// onto a previously-synced path, on the download path only, not a whole-tree rehash. A source
// that vanished or became non-regular in the reconcile→apply window (errUploadSourceGone) has no
// content to lose, so it is not a divergence; the download proceeds and materialises the server
// node. A transient open/hash error is returned so the op stays queued to retry rather than
// risking either a clobber or a spurious conflict.
func (e *Engine) localDivergedFrom(abs, baseHash string) (bool, error) {
	sum, err := e.hashLocalFileMD4(abs)
	if err != nil {
		if errors.Is(err, errUploadSourceGone) {
			return false, nil
		}
		return false, err
	}
	return sum != baseHash, nil
}

// untrackedFilePresent reports whether a regular local file occupies abs (present) and whether
// it is UNTRACKED user content the fresh-create download must not clobber (untracked). A
// brand-new server node carries no BaseLocalHash, so the re-hash-against-base check cannot run,
// yet a regular file already at the target may be content the server never saw. It reuses the
// no-follow, regular-file-only open (hashLocalFileMD4): a vanished or non-regular path (symlink,
// FIFO, directory, or nothing) reports errUploadSourceGone and is not "present".
//
// The untracked decision turns on the baseline reconcile wrote for the server node: a real inbound
// download is only ever produced by the reconcile pass (or the cold-start pull), each of which
// records a StatePending baseline at the target before the op runs. So the keep-both funnel fires
// only when a present file sits under such a not-yet-synced baseline - a genuine fresh server
// create over a user file the server never saw. A present file with NO baseline at the target and
// no byid record for the node (see downloadRecordRelocated) is not a reconcile-produced create and
// overwrites directly, preserving prior behaviour.
//
// A present file under a StateSynced baseline is tracked server content - a prior download's
// output for a same-batch create+modify sequence, where the create's apply left the baseline
// synced before this op runs. Both ops of such a batch carry an empty BaseLocalHash (the
// pre-batch snapshot held no baseline), so without a further check a user edit made after the
// first apply would be replaced with no comparison at all. When that live
// baseline records the hash of the bytes the prior apply wrote (a non-empty LocalHash -
// markSynced always derives one), it is the divergence base this op's reconcile-time capture
// missed: an occupant that still matches it overwrites directly (the pinned prior behaviour)
// and liveBase carries the hash out for the caller's promote-time recheck; an occupant that
// diverged from it is an unrecorded local edit and funnels through keep-both.
//
// A StateSynced baseline with an EMPTY LocalHash is the inbound-rename bare/promoted-pending
// write anomaly: a server move that dragged an untracked/foreign on-disk file onto this
// path stamped a synced record with no local hash (a [move X a->b, modify X] batch relocates the
// user's file to b and then this fresh-create download would overwrite it). markSynced never
// leaves a file record hash-less, so the shape is either that anomaly or one of the two
// deliberate "re-verify on the next scan" markers (revertRelocatedOutbound and
// resolveVanishedUpload clear LocalHash/LocalSize/LocalMTime under StateSynced). Neither is a
// trustworthy divergence base - treat the present regular file as untracked user content and funnel it
// through keep-both rather than clobbering it under a false "still in sync" read (the never-lose
// direction, coordinated with removeBaseFor / relocatedRemoveBase / guardRenameDest).
func (e *Engine) untrackedFilePresent(abs string, op store.Op) (present, untracked bool, liveBase string, err error) {
	sum, herr := e.hashLocalFileMD4(abs)
	if herr != nil {
		if errors.Is(herr, errUploadSourceGone) {
			return false, false, "", nil
		}
		return false, false, "", herr
	}
	b, gerr := e.store.GetBaseline(op.ViewID, op.Path)
	switch {
	case isNotFound(gerr):
		// No baseline at the target path normally means this download is not a
		// reconcile-produced create over a tracked path, so a present file is overwritten
		// directly (the shared-dir-view compromise, preserved on the byid miss below).
		//
		// But the whole fresh-create protection keys on a not-yet-synced record AT THE TARGET
		// PATH, and reconcile's relocatePending EVACUATES that record the moment a later event
		// moves the node away: a [create X @a, move X a->b] batch (same-batch, or split across
		// batches when the create's drain failed transiently) leaves NO record at a while this
		// download still targets a, its rename queued behind it. Reading the miss as "not
		// reconcile-produced" then clobbers an untracked user file at a with the replacing
		// promote and no _Conflict copy - the exact loss the pending branch below prevents,
		// and in a READ-ONLY view the window is unbounded (localScan skips the view, so
		// reconcile's pendingUploads deferral can never fire either).
		// The byid index still resolves the node, so consult it: a hit at ANY path proves the
		// op IS reconcile-produced (only reconcile / the cold-start pull record a baseline for
		// the node, and only a relocation can have carried it off op.Path), which makes the
		// present regular file untracked user content -> keep-both funnel.
		relocated, rerr := e.downloadRecordRelocated(op)
		if rerr != nil {
			return true, false, "", rerr
		}
		return true, relocated, "", nil
	case errors.Is(gerr, store.ErrCorruptRecord):
		// An undecodable record cannot prove the on-disk file is tracked server content, so
		// the present file is treated as untracked user content and funnels through keep-both
		// (the never-lose direction) rather than the strict read wedging the download forever.
		// Singly reachable - a rot after the reconcile-time capture, or a
		// node tracked at another path at capture time - and healed by the resolver's /
		// markSynced's tolerant Puts once the funnel resolves.
		e.warnCorruptBaseline("fresh-create-base", op.ViewID, op.Path, gerr)
		return true, true, "", nil
	case gerr != nil:
		return true, false, "", fmt.Errorf("engine: load fresh-create baseline %d/%q: %w", op.ViewID, op.Path, gerr)
	case b.State == store.StateSynced:
		if b.LocalHash == "" {
			// Anomalous synced FILE record with no hash (the inbound-rename bare/promoted-pending
			// write): no trustworthy divergence base, so the present regular file is
			// untracked user content - funnel it through keep-both rather than clobbering it on a
			// false server-authority read (never-lose, coordinated with removeBaseFor).
			return true, true, "", nil
		}
		if sum != b.LocalHash {
			// A prior apply's synced output was edited since: unrecorded user content.
			return true, true, b.LocalHash, nil
		}
		return true, false, b.LocalHash, nil // tracked server content still in sync; a normal overwrite
	default:
		return true, true, "", nil // pending/conflict baseline over a present file: untracked user content
	}
}

// downloadRecordRelocated reports whether the download's node is still tracked SOMEWHERE in the
// view although no baseline sits at op.Path - the relocated-record shape untrackedFilePresent's
// absent-baseline branch must not read as "no reconcile-produced create here".
// Only the reconcile pass and the cold-start pull record a baseline for an inbound download's
// node, so any byid hit is positive proof the op came from one of them and that a relocation
// (relocatePending, or an applied rename) carried the record off op.Path.
//
// An id-less download can never be resolved this way and keeps the prior behaviour, as does a byid
// miss (the shared-dir-view / never-tracked compromise). A backend error propagates so the op stays
// queued rather than being resolved from an unread index.
func (e *Engine) downloadRecordRelocated(op store.Op) (bool, error) {
	if op.FileID == "" {
		return false, nil
	}
	_, err := e.store.PathByID(op.ViewID, op.FileID)
	if isNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("engine: resolve fresh-create node %d/%q: %w", op.ViewID, op.FileID, err)
	}
	return true, nil
}

// promoteRenameErr marks a promote failure that came from the RENAME itself, as opposed to any
// earlier stage of the same call.
//
// The distinction matters because the directory-occupant classification looks at what stands at
// the target, and an EARLIER failure can leave a directory standing there for an unrelated
// reason. The measured case: a parent the client cannot write makes the staging-temp create fail
// EACCES while a directory happens to occupy the target. Treating that as the clash would try to
// preserve the directory aside - a rename inside the same unwritable parent, which fails too - and
// leave the op retrying forever, which is the wedge class this whole area exists to remove.
// permissionBlockedByDirTarget dead-letters that shape, and must keep doing so.
//
// Unwrap keeps the cause visible, so the no-replace branch's fs.ErrExist test still matches.
type promoteRenameErr struct{ err error }

func (p *promoteRenameErr) Error() string { return p.err.Error() }

func (p *promoteRenameErr) Unwrap() error { return p.err }

// errPromoteTargetDiverged marks a modify download whose target file diverged from the captured
// base WHILE the transfer streamed (the post-transfer recheck in applyDownload failed). The
// promote is abandoned - the staged temp is discarded, the on-disk edit untouched - and the
// caller funnels the op through the keep-both resolver so the mid-transfer edit survives as a
// _Conflict copy.
var errPromoteTargetDiverged = errors.New("engine: download target diverged during transfer")

// funnelFreshCreateIntruder classifies the occupant that made a fresh-create download's
// no-replace promote fail with fs.ErrExist - something landed at the target between the
// pre-transfer guard (which found the path free) and the rename:
//
//   - A regular file is untracked user content that appeared during the transfer; funnel it
//     through the keep-both resolver (the file is untouched on disk, the resolver preserves it
//     as a _Conflict copy and lands the server version) and let the caller ack the download.
//   - A DIRECTORY is the file/dir clash, and it is preserved aside rather than dropped. This
//     branch used to keep the original fs.ErrExist and let the executor dead-letter the op,
//     reasoning that "file-over-dir can never apply" - true only if one refuses to move the
//     directory, which is precisely what the never-lose doctrine does instead (DESIGN §6).
//     preserveAside moves a whole subtree as readily as a file, so the occupant becomes a
//     _Conflict copy the next scan uploads under its new name, and the path is freed for the
//     server's file. Dropping the download instead left the two peers permanently divergent -
//     each holding only its own shape of the path, with the inbound apply abandoned
//     (TestIntegration_TwoClientsFileDirClash).
//   - Any other non-regular occupant keeps the original fs.ErrExist and dead-letters as before
//     (a symlink is refused sync-root-wide, so moving it aside would launder a refusal).
//   - An occupant that vanished again between the failed rename and this look is a transient
//     race: a plain error leaves the op queued to retry onto the now-free path next drain.
func (e *Engine) funnelFreshCreateIntruder(op store.Op, abs string, renameErr error) error {
	fi, lerr := os.Lstat(abs)
	switch {
	case lerr == nil && fi.Mode().IsRegular():
		return e.funnelDownloadConflict(op)
	case lerr == nil && fi.IsDir():
		return e.preserveDirOccupantAndRetry(op, abs)
	case lerr != nil:
		// Deliberately NOT %w, for the same reason.
		return fmt.Errorf("engine: download target %d/%q became occupied then free during promote (lstat: %v); retrying: %v",
			op.ViewID, op.Path, lerr, renameErr)
	default:
		return renameErr
	}
}

// promoteBlockedByDirOccupant reports whether a DIRECTORY stands at the promote target.
//
// It answers the question by looking, not by reading the errno, because the errno is the one
// thing that differs per platform here: Linux rejects a replacing rename onto a directory in
// os.Rename's userspace guard and reports fs.ErrExist (measured - not EISDIR, and not ENOTEMPTY
// even for a non-empty directory), while Windows lets MoveFileEx(REPLACE_EXISTING) fail with
// ERROR_ACCESS_DENIED → fs.ErrPermission. permissionBlockedByDirTarget already takes this
// approach for its own arm and says why.
//
// A vanished or unreadable target answers false: there is nothing to preserve, and the ordinary
// error handling should run.
// promoteFailedAtRename reports whether a promote failure came from the rename rather than from
// an earlier stage of the same call - see promoteRenameErr for why the caller must know.
func promoteFailedAtRename(err error) bool {
	var pre *promoteRenameErr
	return errors.As(err, &pre)
}

func (e *Engine) promoteBlockedByDirOccupant(abs string) bool {
	fi, err := os.Lstat(abs)
	return err == nil && fi.IsDir()
}

// preserveDirOccupantAndRetry moves the directory standing at a download's target aside as a
// keep-both copy and leaves the op queued to land on the freed path next drain.
//
// Shared by both promote policies so the outcome depends on the OCCUPANT's kind rather than on
// which promote ran - the property #20 was filed for.
func (e *Engine) preserveDirOccupantAndRetry(op store.Op, abs string) error {
	if perr := e.preserveAside(op.ViewID, op.Path, abs); perr != nil {
		return fmt.Errorf("engine: preserve the directory occupying download target %d/%q: %w",
			op.ViewID, op.Path, perr)
	}
	// Deliberately NOT wrapping the rename error: it chains to fs.ErrExist (or fs.ErrPermission
	// on Windows), which applyOp classifies as unappliable and dead-letters. The path is free
	// now, so the op must stay queued and land on the next drain.
	return fmt.Errorf("engine: download target %d/%q was occupied by a directory; preserved it as a keep-both copy and retrying",
		op.ViewID, op.Path)
}

// promoteStagedDownload runs the shared staging-promote sequence for a downloaded file
// (shared by applyDownload and the keep-both downloadNodeToPath): create a
// staging temp in abs's parent directory (name-budgeted by stagingTempPattern so a base name at
// the per-component limit - a keep-both copy, say - cannot produce an unappliable temp name),
// stream the body into it via fill, chmod/fsync/close
// the temp, atomically promote it onto abs, fsync the parent directory so the new entry
// survives a crash (fsutil.SyncDir), and stamp the mtime fill returned plus
// unixPerm via fsutil.SetMeta. The staging temp IS the atomic-write temp, so the body is
// written once rather than copied a second time through a separate buffer; fill may leave
// partial bytes in it on error, so the temp is promoted only after a clean fill and is always
// removed afterwards.
//
// replace picks the promote policy: true renames over an existing target (os.Rename, the
// tracked-overwrite download path), false refuses to clobber one (fsutil.RenameNoReplace,
// surfacing fs.ErrExist for the caller's keep-both funnel or dead-letter dispatch). recheck,
// when non-nil, runs after the temp is sealed and immediately before the rename - the last
// point a caller can re-validate the target with the smallest possible window (the modify
// path's divergence recheck); its error aborts the promote.
//
// The returned mtime is the one the FILESYSTEM holds for the promoted file, read back off the
// sealed temp (stagedOnDiskMTime) rather than assumed to be the value fill asked for: it is what
// a caller must record as a baseline's LocalMTime, and it is not always the wire value - see
// stagedOnDiskMTime for the three ways they diverge. A caller that also records the server's
// reported mtime (ServerMTime) keeps the raw wire value for that axis.
//
// promoteStagedDownload creates its staging temp through fsCreateStagingTemp and makes BOTH
// its stamps (the sealed temp's mtime, the promoted path's mtime+perm) through fsSetMeta, so a
// test can inject an ENAMETOOLONG from a mount whose per-component limit is lower than
// stagingTempPattern's budget, or model a filesystem whose timestamp
// granularity is COARSER than the whole seconds a baseline records - FAT/FAT32's two, a
// rounding network or FUSE mount - which no NTFS/ext4/APFS host this client is developed on
// can produce. The rounding lives in the filesystem, so a faithful model
// must apply to every stamp, not just the one whose value is read back.

// deriveStagedDownloadFacts fills in the content facts a tolerantly-decoded download reply
// omitted (an absent hash, an absent/zero size) from the STAGING TEMP the transfer just
// streamed into, before promoteStagedDownload renames it onto the live path.
//
// The temp is the only evidence-correct source. Deriving them from the promoted file instead
// (as both callers once did) re-reads a path that is reachable by every other writer on the
// host the instant the rename lands: an editor auto-save on the file that just appeared, say,
// landing in the promote→derive window is then hashed and recorded as BOTH LocalHash and
// ServerHash under StateSynced. Unlike an ordinary post-sync edit - which the next scan
// catches as a LocalHash mismatch - such a swallowed edit agrees with its own baseline
// forever: the cheap scan, the full re-hash and every divergence gate read agreement, so the
// edit is never uploaded and the server keeps the older content while both ends report
// synced. The temp carries a random name in the destination directory and
// is written only by fill, so its bytes are provably the ones that streamed.
//
// It runs before the temp is chmodded/fsynced/closed, so it reads through the same descriptor
// fill wrote (no re-open, no path lookup); the read leaves the offset at EOF, which is safe
// because nothing writes to the temp afterwards.
func deriveStagedDownloadFacts(tmp *os.File, node *actions.Node) error {
	if node.Hash != "" && node.Size != 0 {
		return nil
	}
	if node.Size == 0 {
		fi, serr := tmp.Stat()
		if serr != nil {
			return fmt.Errorf("stat staged download %q: %w", tmp.Name(), serr)
		}
		node.Size = fi.Size()
	}
	if node.Hash == "" {
		if _, serr := tmp.Seek(0, io.SeekStart); serr != nil {
			return fmt.Errorf("rewind staged download %q: %w", tmp.Name(), serr)
		}
		sum, herr := hashReaderMD4(tmp)
		if herr != nil {
			return fmt.Errorf("hash staged download %q: %w", tmp.Name(), herr)
		}
		node.Hash = sum
	}
	return nil
}

func (e *Engine) promoteStagedDownload(abs string, replace bool, unixPerm uint32, fill func(tmp *os.File) (int64, error), recheck func() error) (int64, error) {
	dir := filepath.Dir(abs)
	if err := fsutil.Mkdir(dir); err != nil {
		return 0, err
	}
	tmp, err := e.fsCreateStagingTemp(dir, stagingTempPattern(filepath.Base(abs)))
	if err != nil {
		return 0, fmt.Errorf("create staging file: %w", err)
	}
	tmpName := tmp.Name()
	// Released after the Remove below (defers run last-in first-out), so the scanner never
	// sees this temp both unheld and present (stalestaging.go).
	defer e.staging.hold(tmpName)()
	defer os.Remove(tmpName)

	mtime, ferr := fill(tmp)
	if ferr != nil {
		tmp.Close()
		return 0, ferr
	}
	if err := tmp.Chmod(downloadStagePerm); err != nil {
		tmp.Close()
		return 0, fmt.Errorf("chmod staging file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, fmt.Errorf("sync staging file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("close staging file: %w", err)
	}
	// Stamp the mtime on the temp and learn what the filesystem stored, BEFORE the promote: the
	// value is needed for the caller's baseline, and reading it off the live path afterwards
	// would read a path every other writer on the host can reach. It runs ahead of recheck so
	// the recheck→rename window it exists to minimise stays two syscalls wide.
	onDiskMTime, merr := e.stagedOnDiskMTime(tmpName, mtime)
	if merr != nil {
		return 0, merr
	}
	if recheck != nil {
		if err := recheck(); err != nil {
			return 0, err
		}
	}
	if replace {
		// fsutil.RenameReplace, not a bare os.Rename: on Windows a destination carrying
		// FILE_ATTRIBUTE_READONLY fails MoveFileEx(REPLACE_EXISTING) with ERROR_ACCESS_DENIED,
		// which the executor reads as transient ACL noise - but the attribute never clears
		// itself, so this promote failed on every drain forever, each retry first streaming the
		// whole body into a fresh staging temp. Reaching here means the
		// divergence gate proved the destination holds the last-agreed base, so replacing it -
		// read-only bit and all - is exactly what this op is entitled to do.
		if err := fsutil.RenameReplace(tmpName, abs); err != nil {
			return 0, fmt.Errorf("promote staging file onto %s: %w", abs, &promoteRenameErr{err})
		}
	} else if err := fsutil.RenameNoReplace(tmpName, abs); err != nil {
		// fs.ErrExist (something appeared at the target) still chains through, so the caller can
		// preserve the intruder (keep-both); the marker only records WHICH stage failed.
		return 0, fmt.Errorf("promote staging file onto %s: %w", abs, &promoteRenameErr{err})
	}
	// Through the fsSyncDir seam (not fsutil.SyncDir directly) so a test can observe - and act
	// in - the window between the rename and the caller's baseline write.
	if err := e.fsSyncDir(dir); err != nil {
		return 0, err
	}
	// Re-apply the same mtime (idempotent: the filesystem stores exactly what onDiskMTime already
	// observed) plus the op's real permission bits, over the promoted path's no-follow descriptor.
	if err := e.fsSetMeta(abs, mtime, unixPerm); err != nil {
		return 0, err
	}
	return onDiskMTime, nil
}

// funnelDownloadConflict turns an inbound download that would overwrite a locally diverged file
// into a keep-both resolution: it enqueues an OpResolveConflict for the download's view+path. The
// caller (applyDownload) returns nil so applyOp acks the download afterwards; mirroring
// funnelUploadConflict, the resolve op is durably enqueued BEFORE the download is dropped, so a
// crash in between leaves the download queued to re-detect the divergence (the local file is
// untouched) and re-enqueue. A duplicate resolve produced by such a replay is harmless: once the
// first resolve runs the canonical path matches the server and the second takes the
// false-conflict fast path. The resolve op carries the download's FileID (the server node id) so
// the resolver's server-vanished disambiguation keys on the stable id, and UnixPerm for the
// recorded baselines.
func (e *Engine) funnelDownloadConflict(op store.Op) error {
	e.log.Info("engine: inbound download would overwrite a locally diverged file; funnelling to keep-both resolver",
		"view", op.ViewID, "path", op.Path)
	if _, err := e.store.Enqueue(store.Op{
		Type:      store.OpResolveConflict,
		Direction: store.DirInbound,
		ViewID:    op.ViewID,
		Path:      op.Path,
		FileID:    op.FileID,
		UnixPerm:  op.UnixPerm,
		// Carry the download's sync id so the resolver stamps the latest server event onto
		// the resolved baseline. Without it, a second same-path download that funnels here
		// (its inherited base predates the first download's apply) would resolve as a false
		// conflict and leave the baseline recording the older download's sync id. The op's id
		// is carried as-is - no max() against the enqueue-time baseline (considered and
		// rejected): the resolver's stamp sites apply it monotonically
		// against the LIVE baseline at apply time, which subsumes any enqueue-time read (the
		// baseline can advance between enqueue and apply anyway).
		ServerSyncID: op.ServerSyncID,
	}); err != nil {
		return fmt.Errorf("engine: enqueue resolve-conflict %d/%q: %w", op.ViewID, op.Path, err)
	}
	return nil
}

// resumeDownloadClient is the inbound-resume subset of the action client: obtaining a
// download resume token and fetching offset-based chunks. *actions.Client satisfies it; the
// executor type-asserts the leased syncClient to it (mirroring outboundClient/brokenReporter)
// so the plain inbound test fakes that only need single-frame Download require no resume stubs.
type resumeDownloadClient interface {
	ResumeGetToken(ctx context.Context, req actions.ResumeRequest) (actions.ResumeToken, error)
	ResumeDownload(ctx context.Context, token, fileID string, rootNodeID, targetSyncID, offset int64, w io.Writer) (actions.Node, error)
}

// errNoResumeDownload marks a leased connection whose client cannot perform a resume
// download - a programming error (the production client always can), surfaced rather than
// panicking so a misconfigured transport leaves the op queued instead of crashing the loop.
var errNoResumeDownload = errors.New("engine: leased client does not support resume download")

// countingWriter wraps a writer and records how many bytes passed through it, so the resume
// loop can advance the offset by the bytes each chunk actually delivered (the server, not the
// client, chooses each chunk's length).
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// emptyContentMD4 is the MD4 of zero bytes, used to tell a genuinely empty fresh server
// file (size 0, hash of nothing) from a tolerantly-decoded query reply that merely omitted
// the size field.
var emptyContentMD4 = md4.ContentHash(nil)

// resumeDownloadToTemp streams a large server file into tmp using the chunked resume_download
// protocol so the body is never buffered whole (DESIGN §4), mirroring the outbound
// resumeUploadFile path. It leases one worker, requests a download resume token, then issues
// offset-based ResumeDownload chunks from offset 0 until the whole file has streamed,
// advancing the offset by the bytes each chunk actually delivered (resume-from-offset: every
// chunk is re-requested at the next offset). ResumeDownload verifies each chunk's own MD4;
// the accumulated whole-file MD4 is verified here so a chunk-assembled file is end-to-end
// checked exactly like the single-turn Download. The returned Node carries the whole-file
// facts (the per-chunk reply nodes describe only their chunk) so applyDownload records the
// same baseline either way.
//
// The event's hash/size snapshot (op.LocalHash/op.LocalSize) goes stale when the server file
// is modified AGAIN between the event and this apply: streaming op.LocalSize bytes of the
// current content and verifying them against the old hash fails every drain - a full
// download per cycle that never converges (ErrHashMismatch, or a zero-byte stall when the
// file shrank), wedging the op and the conflict chain behind it (the corrective download from
// the newer event queues behind this one) until the file is deleted server-side.
// So the apply first re-queries the node over the same worker lease and streams/
// verifies against the CURRENT server facts; the event snapshot remains only the fallback
// when the fresh facts are unavailable or incoherent - the query failed cleanly, the path is
// now a directory or a different node (this op is then genuinely superseded and its
// replacement event resolves the path), or the tolerantly-decoded reply omitted the hash or
// size (a mixed stale/fresh tuple could never verify coherently). markSynced then records
// the facts of the bytes actually streamed, not the stale event snapshot.
func (e *Engine) resumeDownloadToTemp(ctx context.Context, op store.Op, tmp io.Writer) (actions.Node, error) {
	expectHash, size := op.LocalHash, op.LocalSize
	// The fresh-facts query must address the node's CURRENT tracked path, not the
	// enqueue-time op.Path: a later move event relocates the baseline without
	// rewriting the queued op, so a modify→move→modify sequence
	// leaves op.Path stale - querying it finds nothing (or a foreign node), falls
	// back to the stale event snapshot, and the id-addressed stream then fails the
	// hash verify every drain, the exact wedge the fresh query exists to close.
	// The coherence gate below (matching FileID) still rejects a foreign node at
	// the resolved path. The download lands at op.Path - which applyDownload has
	// already rebased to the node's current tracked path when a SELF-ECHO move
	// relocated it with no queued rename to carry it; in the
	// non-self-echo case op.Path stays the stale server path and the queued inbound
	// rename moves the file there.
	queryPath := op.Path
	if op.FileID != "" {
		if cur, perr := e.store.PathByID(op.ViewID, op.FileID); perr == nil {
			queryPath = cur
		} else if !isNotFound(perr) {
			return actions.Node{}, fmt.Errorf("engine: resolve current path for %d/%q: %w", op.ViewID, op.FileID, perr)
		}
	}
	var queryMTime, mtime int64
	werr := e.withWorker(ctx, func(c syncClient) error {
		server, qerr := c.QueryNode(ctx, int64(op.ViewID), queryPath) // #nosec G115 -- a view_id is a non-negative identifier
		switch {
		case qerr != nil:
			if br, ok := c.(brokenReporter); ok && br.Broken() {
				// A query that broke the connection must propagate so WithWorker re-dials the
				// poisoned worker (matching pushConflictCopy) rather than streaming chunks over
				// a byte-misaligned socket.
				return qerr
			}
			e.log.Debug("engine: fresh node query failed; resume download falls back to the event snapshot",
				"view", op.ViewID, "path", op.Path, "err", qerr)
		case server.FileID == op.FileID && !server.IsDir && server.Hash != "" &&
			(server.Size > 0 || server.Hash == emptyContentMD4):
			expectHash, size, queryMTime = server.Hash, server.Size, server.MTime
		default:
			e.log.Debug("engine: fresh node facts incoherent for this op; resume download falls back to the event snapshot",
				"view", op.ViewID, "path", op.Path, "event_file_id", op.FileID, "fresh_file_id", server.FileID)
		}
		mt, err := e.resumeDownloadInto(ctx, c, op.ViewID, op.Path, op.FileID, expectHash, size, tmp)
		mtime = mt
		return err
	})
	if werr != nil {
		return actions.Node{}, werr
	}
	if mtime == 0 {
		mtime = queryMTime
	}
	if mtime == 0 {
		mtime = op.LocalMTime
	}
	return actions.Node{
		FileID: op.FileID,
		Hash:   expectHash,
		Size:   size,
		MTime:  mtime,
	}, nil
}

// resumeDownloadInto streams a server file (fileID, size bytes) into sink using the chunked
// resume_download protocol over the ALREADY-leased connection c, returning the mtime the chunk
// replies reported (0 if none). It requests a download resume token, then issues offset-based
// ResumeDownload chunks from offset 0 until the whole file has streamed, advancing the offset by
// the bytes each chunk actually delivered (resume-from-offset). ResumeDownload verifies each
// chunk's own MD4; the accumulated whole-file MD4 is verified here against expectHash (when
// non-empty) so a chunk-assembled file is end-to-end checked exactly like the single-turn
// Download. An empty expectHash skips the whole-file check with a Warn (parity with
// Download/ResumeDownload's "skipping MD4 verification") so a server silently dropping the
// hash is observable rather than passing as a clean transfer; the callers then derive the
// synced baseline's hash from the promoted file itself, never recording it empty. A chunk
// that delivers zero bytes before completion is a stalled server and fails the transfer
// rather than looping forever.
//
// Unlike resumeDownloadToTemp it does NOT lease a worker, so it is safe to call from inside an
// existing WithWorker callback - the keep-both resolver (downloadNodeToPath) where a nested lease
// could deadlock at numWorkers=2.
func (e *Engine) resumeDownloadInto(ctx context.Context, c syncClient, viewID uint64, relPath, fileID, expectHash string, size int64, sink io.Writer) (int64, error) {
	rc, ok := c.(resumeDownloadClient)
	if !ok {
		return 0, errNoResumeDownload
	}
	whole := md4.New()
	mw := io.MultiWriter(sink, whole)
	rootNodeID := e.viewRootNodeID(viewID)
	tok, err := rc.ResumeGetToken(ctx, actions.ResumeRequest{
		ViewID:     int64(viewID), // #nosec G115 -- a view_id is a non-negative identifier
		RootNodeID: rootNodeID,
	})
	if err != nil {
		return 0, err
	}
	var mtime, offset int64
	for offset < size {
		cw := &countingWriter{w: mw}
		n, derr := rc.ResumeDownload(ctx, tok.Token, fileID, rootNodeID, 0, offset, cw)
		if derr != nil {
			return 0, derr
		}
		if cw.n == 0 {
			return 0, fmt.Errorf("engine: resume_download for %d/%q stalled at offset %d (server delivered no bytes)",
				viewID, relPath, offset)
		}
		offset += cw.n
		if n.MTime != 0 {
			mtime = n.MTime
		}
	}
	if expectHash == "" {
		e.log.Warn("engine: resume download has no expected content hash; skipping MD4 verification",
			"view", viewID, "path", relPath, "file_id", fileID)
		return mtime, nil
	}
	got := hex.EncodeToString(whole.Sum(nil))
	if got != expectHash {
		return 0, fmt.Errorf("%w: resume_download for %d/%q computed %s, expected %s",
			actions.ErrHashMismatch, viewID, relPath, got, expectHash)
	}
	return mtime, nil
}
