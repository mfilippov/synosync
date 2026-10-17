// Package engine implements the two-way sync core: an event loop that long-polls
// subscribed views, pulls their change events, reconciles them into the durable
// queue, and applies them to local disk (the remote→local direction), plus a
// periodic local scanner that walks each view, diffs the result against the stored
// baselines, and pushes local creates/modifies/deletes back to the server (the
// local→server direction). It composes the ready building blocks - the event-stream
// actions (LongPoll/PullEvent), the catalog/transfer actions (Upload/ResumeUpload/
// Mkdir/Rename/Remove), the connection pool, and the baseline/cursor/queue store -
// behind the Transport seam, so the engine carries the reconcile and apply policy
// while the lower layers carry the wire and persistence mechanics.
//
// Remote→local (Phase 6b): download/mkdir/rename/remove applied to disk, self-echo
// suppression, per-view cursor advance, and the durable inbound queue.
//
// Local→server (Phase 6c): refreshViews captures each view's root file id; a
// throttled localScan walks the view's on-disk directory streaming MD4 over each
// file; localreconcile diffs the scan against the baselines into DirOutbound queue
// ops (new dir → OpMkdir, new/changed file → OpUpload, vanished baseline → OpRemove,
// with a single same-scan hash match collapsed to OpRename); and the outbound branch
// of the executor (outbound.go) resolves parent ids and applies each op via the
// catalog/transfer actions, recording the server-assigned file id back into the
// baseline. The server is the sole authority and the only conflict seam: a
// stale-base push rejection (ErrConflictVersion, 24579) marks the baseline
// StateConflict and is acked rather than overwriting the diverged remote.
//
// Keep-both conflict resolution (Phase 6d, conflict.go), the cold-start full-catalog
// pull (Phase 6e, coldstart.go) and the fsnotify-driven local watcher (Phase 7,
// internal/watcher) are all implemented and wired into the loop.
package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// isTerminalRefreshError reports whether a best-effort enrichment error (the root-id
// QueryNode in refreshViews) must abort the worker turn rather than being swallowed.
// A broken connection (the turn left the socket byte-misaligned), a vanished session
// (the pool must pause for re-pairing), a rejected client_version (the server refused
// this binary; the loop stops for an upgrade, which is why the
// enrichment must not swallow it as a best-effort lookup failure), and a
// cancelled/expired context are all conditions the engine loop must react to - and a broken socket must propagate so
// WithWorker re-dials it instead of returning it to the pool. Every other QueryNode
// failure (a path/not-found error, a malformed reply) is an ordinary lookup failure
// the caller treats as best-effort. A per-turn timeout (errTurnTimeout)
// must abort the turn too: the expired deadline poisoned the socket,
// so swallowing it as best-effort would release the broken worker back to the pool -
// "terminal" here means terminal for the WORKER TURN, not for Run (isTerminal still
// reads errTurnTimeout as transient and the loop retries).
func isTerminalRefreshError(err error) bool {
	return errors.Is(err, actions.ErrConnBroken) ||
		errors.Is(err, auth.ErrSessionVanished) ||
		errors.Is(err, auth.ErrClientVersion) ||
		errors.Is(err, errTurnTimeout) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// errParentUnknown marks an outbound op whose parent directory id cannot yet be
// resolved - no view root captured, a missing parent baseline, or a parent baseline
// the server has not assigned an id to. It is not a permanent failure: once the
// parent's own op has run and recorded its server id, a later drain resolves it. The
// outbound executor (Phase 6c) leaves such an op queued rather than dropping
// it.
var errParentUnknown = errors.New("engine: parent directory id unknown")

// errParentBaselineAbsent narrows errParentUnknown (which it wraps, so every existing
// transient classification still matches) to the sub-case where the parent directory has
// NO baseline at all - the exact key is missing and the fold fallback found nothing - as
// opposed to a live id-less StatePending parent merely awaiting its own op. The distinction
// matters because an absent parent baseline under a queued create means either the
// parent was relocated by a server-side rename (relocatePending re-homed the subtree's
// baselines before this op drained) or it was dropped by a sibling's dead-letter cleanup -
// both permanent dead-ends the op can never wait out, while the authoritative inbound op
// that would resolve them is serialized BEHIND it (a cyclic wedge). The outbound create
// verbs probe only on this narrower sentinel, so the ordinary pending-parent wait stays a
// plain transient retry.
var errParentBaselineAbsent = fmt.Errorf("%w: no baseline tracks the parent", errParentUnknown)

// errParentConflicted marks an outbound op whose parent directory is itself in
// StateConflict - a parent mkdir the server rejected with a stale-base conflict never
// received a server id, so the child can never resolve its parent and would otherwise
// retry forever. Unlike errParentUnknown this is terminal: the parent's
// divergence is owed to the deferred keep-both resolution (6d), so the child op is
// itself marked StateConflict and dead-lettered rather than left to wedge the queue.
var errParentConflicted = errors.New("engine: parent directory in conflict")

// errParentCorrupt marks an outbound/keep-both op whose parent directory baseline
// record no longer decodes (store.ErrCorruptRecord). Like errParentConflicted this is
// terminal: resolveParentID cannot recover the parent's server id from the corrupt
// bytes and no queued op will heal it, so retrying the child would wedge the queue
// forever. The child is dead-lettered - observable as Dropped -
// rather than retried; its on-disk data is untouched and its baseline is marked
// StateConflict so the blocked path is visible in the Unresolved count rather than
// silently swallowed by a stale StatePending record (localReconcile only re-drives
// StateSynced baselines, so a pending one would drop even a fresh local edit). The
// child recovers once the corruption is cleared - an inbound rewrite of the parent
// and child records, or a Reset/re-pair. This closes the descendant half of
// "one corrupt record must not wedge the view":
// localReconcile drops only the corrupt node's own row, but a DIRECT child's
// create/modify still resolves its parent against the corrupt record, and that lookup
// would otherwise fall through as a generic transient error and retry indefinitely.
var errParentCorrupt = errors.New("engine: parent directory baseline is corrupt")

// errParentGone marks an outbound upload/mkdir/rename whose parent directory was
// removed server-side - confirmed by the ancestor walk (ancestorConfirmsAbsent), not
// merely suspected. It is terminal: the create/move can never land under a directory
// that no longer exists, and no queued op will resurrect it (the parent is gone from
// BOTH sides - the inbound OpRemove of that directory is what deleted it). The ancestor walk
// was once given only to the remove verb (applyRemoveOut); without an
// equivalent exit the create/move verbs stayed queued forever on the never-provable
// errParentUnknown/ErrPathError, and in the "outbound op before inbound remove" drain
// order the wedged op also blocked the authoritative inbound OpRemove serialised behind
// it - so its never-lose guardRemoveDir preserve never ran either. The
// dispatcher drops the op with the standard baseline cleanup so the local content
// becomes untracked and the trailing inbound remove preserves it as a _Conflict copy.
// An id-less create whose parent BASELINE is absent
// (errParentBaselineAbsent - relocated by a server rename or dropped by a sibling's
// dead-letter) confirms the parent gone with the same walk BEFORE any server turn; a
// renamed parent reads gone there too (leaf-blind, accepted as data-safe - see
// ancestorConfirmsAbsent), which dropGoneParentBaseline's relocation-aware cleanup
// resolves so the next scan re-emits the op under the live parent.
var errParentGone = errors.New("engine: parent directory removed server-side")

// Engine drives the remote→local sync loop for one account against one local sync
// root. It is constructed with New and run with Run; it is not safe for concurrent
// use beyond the worker fan-out it manages internally.
type Engine struct {
	// progress accumulates what this run has applied, for reporting (progress.go).
	progress  progress
	store     *store.Store
	transport Transport
	// syncRoot is the absolute local directory the views are materialised under.
	syncRoot string
	// unselectedWarned latches the one Info per view explaining a
	// selection-driven skip, so a busy loop does not repeat it every cycle.
	unselectedWarned map[uint64]bool
	// staging tracks the download staging temps this run holds, to tell a leftover from an
	// interrupted run apart from a download in flight (stalestaging.go).
	staging *stagingTracker
	// views is the operator's selection and layout. Its zero value is every view,
	// each in its own subdirectory - the behaviour of an install that predates the
	// setting, so an engine constructed without SetViews keeps that exactly.
	views config.Views
	// selfSessionID is this client's session identifier, compared against an
	// event's CommitterSessionID to recognise and skip self-echo (DESIGN §6). The
	// exact wire semantics of committer_session_id still need verification against a
	// live DSM (DESIGN §13).
	selfSessionID string
	log           *slog.Logger

	// wake, when non-nil, is the Phase-7 watcher's debounced wake channel: a receive
	// signals a local filesystem change so pollOnce cancels the in-flight long-poll and
	// forces the next cycle's localScan to run at once, instead of waiting out the poll.
	// A nil wake (the default) preserves the pre-watcher behaviour exactly. It is set
	// once via SetWake before Run - or by Run itself when a watcher factory is installed
	// (see newWatcher) - and not mutated concurrently with Run.
	wake <-chan struct{}

	// pollDeferWake records that the previous long-poll was cut short by a wake before
	// the server could reply, so the NEXT poll must be allowed to run to completion.
	// Without the debt, continuous local churn - an app writing into
	// the sync root at least once per cycle - left a wake already pending at the start of
	// every poll, cancelling each one at zero milliseconds: remote deletes, renames and
	// modifies were never pulled for the whole duration of the churn, with no Stuck or
	// Warn to show it. The deferring poll still consumes the wake (the scan throttle is
	// reset, so the local push rides the next cycle's localScan) - the wake keeps its
	// latency role for outbound and simply loses its veto over inbound. Read and written
	// only from Run's goroutine in pollOnce, like the other loop bookkeeping.
	pollDeferWake bool

	// paused is the Phase-8b user-requested pause flag, flipped by Pause/Resume from the
	// IPC goroutine and read by Run's waitWhilePaused gate. While set, Run blocks at the
	// next cycle boundary (a long-poll already in flight is allowed to finish) and no new
	// cycle runs until Resume clears it. The flag itself is in-memory; durability lives a
	// layer up in the daemon, which persists the desired state
	// (store.PutPaused) and re-applies it to a freshly built engine via Pause(), so a
	// user-visible pause survives a daemon restart. atomic so it is safe to read/write
	// concurrently with Run.
	paused atomic.Bool

	// diskFull latches that the most recent apply failed with ENOSPC (the local disk
	// ran out of space). Such a failure is transient - the op is not dead-lettered, it
	// stays queued and retries once space is freed - but it is a distinct operability
	// condition the operator must see, so it is surfaced as a `status` line via
	// DiskFull(). It is set/cleared centrally in noteApplyResult on every apply path
	// (inbound switch, outbound push, keep-both resolve): an ENOSPC apply error latches it
	// and the next clean apply clears it. atomic so the status goroutine can read it while
	// Run applies.
	diskFull atomic.Bool

	// stuckMu guards opFailures: drainBatch's per-op goroutines record apply outcomes
	// concurrently, and the daemon's status goroutine reads the stuck count while Run
	// applies.
	stuckMu sync.Mutex
	// opFailures counts consecutive stuck drains per queue seq.
	// Everything the executor does not classify terminal retries forever - an EACCES'd
	// target, an unresolvable parent, a persistently failing transfer - with only a
	// per-cycle Warn, so a permanently wedged op (and the conflict chain queued behind
	// it) was invisible in `status`, unlike ENOSPC's diskFull latch. Each failed apply
	// increments the op's counter (noteOpFailure - the CULPRIT), and each drain a
	// conflicting successor is skipped behind such an op increments the successor's
	// counter too (noteOpBlocked - the VICTIM), so both the failing
	// op and the ops wedged behind it show in `status`; the victim's escalation Warn
	// names the culprit. Reaching stuckThreshold escalates with a one-time Warn and
	// counts the op in StuckOps, surfaced as the `Stuck` status line. The entry is
	// cleared once the op leaves the queue - applied and acked, dead-lettered, or
	// dropped as superseded (clearOpFailures) - so the count self-heals and the map
	// cannot outgrow the live queue. In-memory only by design: a daemon restart resets
	// the counters, which is acceptable for observability (a still-wedged op re-earns
	// its stuck mark after stuckThreshold more drains).
	opFailures map[uint64]int

	// opServerErrs tracks, per queue seq, the unmapped server result code an OUTBOUND op
	// keeps being rejected with and how many consecutive drains it has been rejected with
	// exactly that code. An unmapped code has no typed sentinel and no
	// terminal class, so such an op used to retry forever with every overlapping successor
	// serialised behind it; an unbroken run of identical codes is the determinism evidence
	// escalateDeterministicServerFailure requires before dropping it. Guarded by stuckMu
	// (the same lock as opFailures - both are per-op executor bookkeeping written from the
	// drain goroutines) and cleared alongside it in clearOpFailures. In-memory only by
	// design, like opFailures: a daemon restart re-earns the streak.
	opServerErrs map[uint64]serverErrStreak

	// opRetryAfter holds, per queue seq, the instant before which an op must NOT be
	// re-attempted: the geometric pause armed once its failures have repeated with the
	// identical server result code (opbackoff.go, two-client finding H4). Nothing used to
	// separate a failed apply from the next drain's re-apply, so a deterministically refused
	// push was measured re-sending its turn ~80 times a second for the whole five minutes the
	// escalation's wall-clock floor takes. Guarded by stuckMu like the other per-op executor
	// bookkeeping, cleared with it in clearOpFailures, and in-memory only by design.
	opRetryAfter map[uint64]time.Time

	// opWarned latches, per queue seq, which escalation Warns the op has already emitted
	// during its current failure streak. The escalation gates read
	// "counter >= stuckThreshold AND this flavour not yet logged" rather than "counter ==
	// stuckThreshold": the counter is monotonic and shared by the culprit and victim paths, so
	// an equality gate silently lost the diagnosis whenever an unrelated transient landed on
	// exactly the Nth failure, or whenever an op crossed the threshold while blocked and only
	// became the culprit afterwards. Guarded by stuckMu and cleared with the streak in
	// clearOpFailures, like opFailures/opServerErrs; in-memory only by design.
	opWarned map[uint64]opWarnLatch

	// nodeAbsentConfirms tracks, per (view, path), how many drain passes in a row a
	// path-addressed absence probe answered with the path-error sentinel.
	// The sentinel (28672) conflates "no such path" with a server-side timeout by the client's
	// own contract (actions/errors.go), so ONE of them is not corroboration - the same reason
	// the view-level twin requires absentRootConfirmations agreeing root probes. Guarded by
	// stuckMu (the same lock as the other per-op executor bookkeeping, written from the drain
	// goroutines) and in-memory only by design: a daemon restart re-earns the streak.
	nodeAbsentConfirms map[string]absenceProbeStreak

	// drainPass counts drainQueue passes, so a streak entry can tell one pass's probe from
	// the next and count at most one confirmation per pass. Guarded by stuckMu.
	drainPass uint64

	// absenceStreaksPruned is the drain pass nodeAbsentConfirms was last swept in, so the
	// over-cap eviction runs once per pass instead of once per probe (pruneAbsenceStreaksLocked).
	// Guarded by stuckMu.
	absenceStreaksPruned uint64

	// absenceSweepIdleUntil is the earliest instant the over-cap sweep could free anything: the
	// oldest surviving entry's eligibility moment. Before it, the sweep skips the walk entirely
	// rather than scanning the whole map under stuckMu to delete nothing
	// (pruneAbsenceStreaksLocked). Guarded by stuckMu.
	absenceSweepIdleUntil time.Time

	// quarantineRepaired is the store's quarantine counter (store.QuarantinedOps) as of the
	// last completed repair sweep (queuerepair.go). A quarantined queue
	// record is the one op drop that cannot resolve the baseline reconcile pre-wrote for it -
	// its view and path are exactly what failed to decode - so an advance of the counter arms
	// the sweep that re-derives the owner of every pending baseline no queued op covers. It
	// needs no lock: drainQueue runs the sweep on Run's own goroutine, after the pass's apply
	// goroutines have settled.
	quarantineRepaired uint64

	// repairDeferred is what the last repair sweep DEFERRED - pending baselines it could not
	// tell apart from legitimately owned ones because a queued op overlaps them - plus the
	// backoff on re-examining exactly that set (queuerepair.go). A
	// deferral normally drains away within a pass or two, but one caused by a
	// forever-transient op never does, and the sweep it kept armed re-streamed every
	// baseline of every view on every drain for the daemon's life. It needs no lock for the
	// same reason quarantineRepaired does not.
	repairDeferred repairDeferral

	// doomMu guards carriedDooms, written from drainBatch's per-op goroutines.
	doomMu sync.Mutex
	// carriedDooms holds the (file id, resolved output key) pairs of the dooms a drain pass
	// PROVED but failed to RECORD - the store-level failure arms of drainBatch's doomed
	// branch. Without it a doom lived only as long as the drainQueue
	// call that derived it (the in-window feeder scan and the deadOutputs set are both
	// pass-local), while the doomed rename itself is durable: the next pass re-read it with
	// its feeder long gone from the queue and applied it, hauling a same-kind untracked local
	// entity from the source onto the destination. Entries are dropped when the doom is
	// finally recorded, or when a live op of the same node at the same key supersedes it. See
	// carryDoom for the in-memory-only bound.
	carriedDooms map[string]bool

	// resume is the coalescing signal Resume sends to wake Run's waitWhilePaused gate
	// (buffered cap-1, created in New). A non-blocking send means a Resume that races an
	// already-running loop is never lost: the gate re-checks paused on every wake, so a
	// buffered signal at worst causes one spurious wakeup that immediately re-blocks.
	resume chan struct{}

	// pauseReq is the cap-1 coalescing nudge Pause sends so an IN-FLIGHT local scan aborts
	// promptly instead of running its whole walk+hash to completion first.
	// pauseAwareCtx's watcher consumes it and cancels the scan context with the errScanPaused
	// cause; a token nobody consumed is dropped by the next scan's watcher, so a pause that
	// arrived between scans cannot abort an unrelated later one.
	pauseReq chan struct{}

	// manualWake is the Phase-8b coalescing trigger TriggerSync sends on (buffered cap-1,
	// created in New): it folds into pollOnce's wake select exactly like the watcher wake,
	// so a `sync now` interrupts an in-flight long-poll (re-dialing the event connection and
	// resetting the scan throttle); a trigger fired while not blocked stays buffered and
	// forces the next cycle's scan. Non-blocking sends collapse a burst to one pending wake.
	manualWake chan struct{}

	// newWatcher, when non-nil, is the Phase-7 watcher factory Run brings the filesystem
	// watcher up through: Run calls it once with a child context (cancelled when Run
	// returns, so the watcher stops on exit) and e.syncRoot, then wires the returned
	// debounced wake channel into the wake seam above. Injecting the factory keeps the
	// engine free of a direct fsnotify dependency - the production wiring lives in the
	// daemon (Phase 8), and engine tests pass a fake channel - so only internal/watcher
	// exercises real fsnotify. A nil factory (or a wake already installed via SetWake)
	// leaves Run's behaviour exactly as before. It is set once via SetWatcher before Run.
	newWatcher func(ctx context.Context, root string) (<-chan struct{}, error)

	// limits holds the resolved resource knobs (DESIGN §11): worker count, drain
	// batch, upload thresholds, empty-scan threshold, poll timeouts and the
	// cold-start catalog cap. New initializes it to config.DefaultLimits so an engine
	// built without SetLimits behaves exactly as the historical hardcoded constants;
	// the daemon calls SetLimits with the loaded, validated config so operator
	// overrides take effect. Every consumer (loop, executor, outbound, conflict,
	// localreconcile) reads its tunable from here rather than a package constant.
	limits config.Limits

	// scanInterval is the minimum delay between local filesystem scans (localScan), so
	// the loop pushes local changes without re-walking every view on every tight cycle
	// iteration. It is derived from limits.LocalScanInterval in New/SetLimits. lastScan
	// records when the last scan ran (zero before the first), and now is the clock
	// localScan consults - a field so tests can drive the throttle deterministically
	// without sleeping.
	scanInterval time.Duration

	// turnTimeout bounds each bounded request/reply turn run over a leased worker
	// connection (see Engine.withWorker in transport.go); 0 disables the deadline.
	// It is derived from limits.OpTurnTimeout in New/SetLimits
	// and, like the other limits, must be set before Run and not concurrently with
	// it.
	turnTimeout time.Duration
	lastScan    time.Time
	now         func() time.Time

	// profileDigest is the long_poll profile digest last returned by the server, carried from one
	// event Client to the next (see profileDigester).
	profileDigest string

	// scanPass counts the local scan passes that have actually run (it is bumped in
	// localScan only after the scanInterval throttle, not on a throttled no-op). It drives
	// the Phase-9d periodic full re-hash safety net: shouldFullRehash forces a full MD4
	// re-hash (cheap-scan hints disabled) every limits.FullRehashInterval passes, so a
	// same-second / same-size modification the cheap (size, mtime) short-circuit would miss
	// is eventually caught. Driven serially from the loop, so no lock is needed.
	scanPass uint64

	// hostname is embedded in keep-both conflict-copy names (DESIGN §2). It is
	// resolved once in New via os.Hostname (falling back to "unknown"); a field so
	// resolver tests can override it for deterministic copy names, mirroring now.
	hostname string

	// afterUploadFstat is a test-only seam (nil in production): when set, applyUploadOut's
	// large-file branch invokes it after the fstat that captures liveSize but before the
	// content hash, so a test can deterministically widen the scan→apply window - e.g. grow
	// the source on disk - and assert the advertised hash still covers exactly liveSize bytes
	// rather than every byte to EOF.
	afterUploadFstat func()

	// emptyViewScans tracks, per view, how many consecutive local scans found its on-disk
	// directory empty while it still has StateSynced baselines - the signature of either a
	// genuine whole-view delete or a transient unavailability (an unmounted mount point, a
	// removed root) - and when the last COUNTED one landed. A whole-view wipe is only
	// propagated to the server once the count reaches emptyScanConfirmThreshold, so a single
	// suspicious scan never erases the remote view, yet a sustained empty state still converges.
	// The timestamp exists because consecutive SCANS are not a
	// duration (a watcher wake resets the scan throttle, so an unmount's fsnotify burst delivers
	// the whole threshold in seconds), so a scan also counts only once
	// emptyScanConfirmMinSpacing of wall clock has passed since the previous counted one. A
	// non-empty or unavailable scan drops the entry. localReconcile is driven serially from the
	// loop, so this map needs no lock.
	// emptiedDirScans is the per-DIRECTORY twin of emptyViewScans: the debounce that keeps an
	// unmounted nested mount point from being read as "the user deleted everything in here"
	// (emptieddir.go). Driven serially from the loop like its sibling, so no lock is needed.
	emptiedDirScans map[emptiedDirKey]emptyScanStreak
	// dirSeenNonEmpty remembers each directory's identity as of the last scan that saw it
	// non-empty; an emptied directory still holding that identity was emptied in place, not
	// uncovered by an unmount (emptieddir.go). Loop-serial like emptiedDirScans.
	dirSeenNonEmpty map[emptiedDirKey]fileIdent

	emptyViewScans map[uint64]emptyScanStreak

	// viewRootDev records the filesystem device id (Unix st_dev / Windows volume serial) a
	// view root last presented while healthy - i.e. observed non-empty. The empty-scan
	// debounce consults it (viewRootDeviceChanged) so a mount that dropped or swapped
	// underneath the root - which reads as an ordinary empty directory that Lstat cannot tell
	// apart from a genuine empty-out - is treated as an unavailable root rather than counted
	// toward erasing the remote view. Recorded on every non-empty scan and never
	// cleared on a transient empty, so the comparison survives the unavailable window. The id
	// is also persisted to store.View.RootDevice (on change) and reloaded lazily, so the guard
	// survives a daemon restart into an already-dropped/swapped mount rather than starting blind.
	// Driven serially from the loop, so no lock is needed.
	viewRootDev map[uint64]uint64

	// deviceMoves records the old→new view-root device transitions this process ADOPTED on
	// positive content evidence (adoptViewRootDevice's confirmed arm). It is the corroboration
	// the EMPTY branch has no way to gather on its own: an empty view can never produce content
	// evidence, so a device id that changed under it - the ordinary result of a reboot
	// renumbering a dm/LVM/btrfs/network filesystem - would otherwise suppress that view's scans
	// forever. A sibling view of the SAME filesystem (same recorded id) that
	// walked its still-tracked tree and confirmed the very same transition is proof the filesystem
	// was renumbered rather than swapped, so the empty view may adopt the new id and resume its
	// ordinary empty-scan debounce. In-memory only (no persisted field): corroboration is a fact
	// about this process's observations, and a stale one must never outlive the daemon.
	deviceMoves map[uint64]uint64

	// deviceGuard holds the per-view, cross-pass state of the view-root device guard: whether the
	// id is currently in an unreadable streak (so its Warns are emitted once per streak instead of
	// on every scan pass - a filesystem that stably refuses to report an id, e.g. a FUSE or network
	// redirector, otherwise rolls the log over with a Warn that never converges) and which file
	// last supplied content evidence (so the next pass re-checks that candidate first instead of
	// re-running the capped sweep). Driven serially from the loop, so no lock is needed.
	deviceGuard map[uint64]deviceGuardState

	// deviceID returns a path's filesystem device id, behind a seam so the empty-scan
	// device-change guard is tested without a real mount swap. New sets it to the per-OS
	// osDeviceID (statdev_unix.go / statdev_windows.go); tests override it to simulate a
	// device change or a Statfs failure.
	deviceID func(path string) (uint64, error)

	// caseFoldProbe empirically determines whether the sync-root filesystem folds case,
	// overriding the GOOS default the normpath package starts with: a
	// case-insensitive mount under Linux (exFAT/VFAT/NTFS, or an ext4 casefold dir) - or a
	// case-sensitive volume under macOS - would otherwise make the engine match paths the
	// wrong way, spuriously renaming/deleting one file of a legitimate case-pair the server
	// holds. Run calls applyCaseFold once at startup, which invokes this and feeds the
	// result to normpath.UseCaseInsensitive; a probe failure falls back to the GOOS default.
	// New sets it to probeSyncRootCaseFold; tests inject a fake to drive the agreeing /
	// disagreeing / probe-failure branches without depending on the host FS's case rules.
	caseFoldProbe func(root string) (bool, error)

	// readDir lists a directory, behind a seam so a test can count the NFD-fallback listings
	// resolveNFCComponents performs while resolving a canonical (NFC) path to its real on-disk
	// spelling. That fallback is skipped outright for a component canonically equivalent only
	// to itself (asciiSelfEquivalent), which is what the counter pins: an
	// inbound create of an ASCII-named node must not list its parent. New sets it to
	// os.ReadDir; listDir falls back to os.ReadDir when it is nil, so an Engine built
	// literally in a test needs no wiring.
	readDir func(name string) ([]os.DirEntry, error)

	// lstat is the no-follow stat noSymlinkEscapeMemo walks the path components with,
	// behind a seam so a test can make a component report as a symlink at a chosen point
	// in a scan pass - the mid-pass swap the memo must not let ride a cached proof - on a
	// host that cannot create symlinks at all (Windows without the privilege, where every
	// symlink fixture skips). New sets it to os.Lstat; lstatPath falls back to os.Lstat
	// when it is nil, so an Engine built literally in a test needs no wiring.
	lstat func(name string) (os.FileInfo, error)

	// The filesystem seams the apply path writes through. Each exists so a test can
	// inject a host rejection or a filesystem behaviour this client cannot provoke on the
	// NAME_MAX=255, whole-second, healthy mounts it is developed on: an ENAMETOOLONG from
	// a per-component limit lower than the copy-name budget, an EIO from the mount that
	// just failed a rename, a timestamp granularity coarser than the second a baseline
	// records, or an edit landing inside the window between a promote and its flush.
	//
	// They were package-level vars until now, which meant a test swapping one mutated
	// process-global state: every such test had to run alone, and none of this package
	// could use t.Parallel. New wires them; the accessors below fall back to the real
	// implementation when nil, so an Engine built literally in a test needs no wiring.
	renameNoReplace   func(oldpath, newpath string) error
	conflictCopyLstat func(name string) (os.FileInfo, error)
	syncDir           func(dir string) error
	createStagingTemp func(dir, pattern string) (*os.File, error)
	setMeta           func(path string, mtime int64, unixPerm uint32) error

	// longPath resolves a path to the spelling the host actually stores - on Windows that
	// de-aliases every 8.3 short-name component - and is a test-only seam (nil in production,
	// where longPathName falls back to the per-OS osLongPathName). It lets the short-name alias
	// gate (shortname.go) be driven deterministically on ANY host, including
	// the Windows volumes that have short-name generation switched off and the Unix-likes that
	// have no such alias at all.
	longPath func(name string) (string, error)

	// The six store seams below sit BESIDE the store rather than replacing it, and that is
	// deliberate rather than a migration someone abandoned half-way.
	//
	// They are not an abstraction layer. The engine calls dozens of store methods and gains
	// nothing from an interface listing them all - it would make every call site one
	// indirection further from the code that actually commits the transaction, in the package
	// where knowing exactly what is committed together is the whole game. What these six are
	// is targeted injection points, each for a specific failure this client cannot otherwise
	// stage: a crash landing between two writes whose ORDER is a never-lose invariant, or a
	// cost a test needs to count rather than assume. Each field's comment names the invariant
	// or the cost it exists for, and a seam without such a reason does not belong here.
	//
	// So: reach for the store directly. Add a seam only when a test must land inside a
	// specific window, and say which one.

	// scanBaselines streams a view's stored baselines, behind a seam so a test can count
	// how many times a single local-reconcile pass decodes the baseline set. New sets it to
	// store.ScanBaselinesForView; the cheap-scan path now streams the baselines exactly once
	// per pass (collectBaselines fills both the hint table and the merge-join rows from the
	// one stream), where it formerly streamed twice (loadScanHints + mergeScanAndBaselines).
	// The first return is the view-relative paths of records whose value no longer decodes,
	// skipped by the stream: consumers Warn and treat those paths as
	// tracked-but-frozen rather than failing the whole view on one corrupt record.
	scanBaselines func(viewID uint64, fn func(store.PathBaseline) error) ([]string, error)

	// scanBaselinePrefix streams only the baselines recorded strictly beneath one directory
	// (dir + "/"), behind a seam so a test can count how many records a subtree sweep decodes.
	// The three subtree helpers on the inbound apply path - relocateDescendantBaselines,
	// dropDescendantBaselines and dropPhantomDescendantBaselines - used to stream the WHOLE
	// view and filter by prefix in Go, i.e. every directory move decoded every baseline of the
	// view. The store's cursor seeks to the subtree instead, so the cost
	// is O(subtree). New sets it to store.ScanBaselinesUnderPrefix; the first return is the
	// paths under the prefix whose value no longer decodes, exactly like scanBaselines.
	scanBaselinePrefix func(viewID uint64, dir string, fn func(store.PathBaseline) error) ([]string, error)

	// rootUnavailable reports whether a view's on-disk root is in a known-unavailable state,
	// behind a seam so a test can inject the INDETERMINATE outcome - a probe that fails
	// (EACCES/EIO on the root stat) after the walk itself succeeded, which no portable
	// filesystem manipulation can stage deterministically. That outcome is the one the
	// empty-scan debounce must not count as an observation. New sets it
	// to (*Engine).viewRootUnavailable.
	rootUnavailable func(viewID uint64) (bool, error)

	// enqueueOp appends one op to the durable queue, behind a seam so a test can inject a
	// mid-batch enqueue failure and pin reconcile's crash-window ordering:
	// the op is enqueued BEFORE markPending rewrites the baseline, so a failure -
	// or a crash - between the two leaves the pre-event synced record intact. The reverse
	// order left a pending baseline with no queued op, whose replay captured no divergence
	// base and set an untouched local file aside as a spurious _Conflict copy. New sets it
	// to store.Enqueue. It serves the move and delete arms; a create/modify goes through
	// batchStore instead, which commits its baseline in the SAME transaction.
	enqueueOp func(op store.Op) (uint64, error)

	// batchStore runs a group of store writes in ONE durable transaction. Two things need it,
	// and they are worth naming separately because they are different arguments for the same
	// seam.
	//
	// ATOMICITY, which is a correctness argument.
	//
	// reconcile uses it for the create/modify arm, where op-before-baseline ordering is not
	// enough: the pending baseline that arm writes is the ONLY thing arming the fresh-create
	// never-lose gate (untrackedFilePresent reads a missing record as "not reconcile-produced"
	// and lets the download promote over the occupant with a replacing rename). A crash
	// between the two writes therefore left a queued download whose gate was disarmed, and the
	// restart drains BEFORE it re-pulls the batch (loop.go's cycle order), so nothing re-armed
	// it - the untracked user file at the path was overwritten with no _Conflict copy. A test
	// fails the whole unit through this seam to pin that atomicity.
	//
	// COST, which is a performance argument. bolt fsyncs per transaction and rewrites a b-tree
	// path plus the freelist each time, so the granularity IS the cost: cold start measured two
	// transactions per planned op at 40 ops/s and 47.6 GB of physical I/O for a 241 MB database.
	// The local reconcile's planning loop and the subtree relocate/drop sweeps a directory move
	// or delete runs eagerly inside reconcile all write through it, and there the tests pin the
	// transaction COUNT rather than only the result.
	//
	// New sets it to store.Batch.
	batchStore func(fn func(*store.Batch) error) error

	// ackOp deletes one op from the durable queue, behind a seam so a test can inject a
	// failure at the ack boundary and pin the supersession drops' crash-window ordering
	// (the enqueueOp mirror): the superseded download's stranded-baseline
	// cleanup runs BEFORE its Ack - two separate Update transactions - so a crash between
	// the two leaves the op queued with its cleanup already applied (the replayed batch
	// re-runs both idempotently), rather than acked with a stranded phantom StatePending
	// record nothing would ever clean (narrowed to the crash window). New sets it to store.Ack;
	// only the reconcile-side supersession drops route through it - the executor's drain acks stay
	// direct store.Ack calls, their crash contract being the at-least-once replay of the whole
	// apply.
	ackOp func(seq uint64) error

	// largeTreeWarned records, per view, whether the "unusually large view" high-water
	// warning has already fired so the operator sees it once rather than on every 5s scan
	// or cold-start page. A view that shrinks back below the threshold clears its entry so
	// a later regrowth warns again. Driven serially from the loop, so no lock is needed.
	largeTreeWarned map[uint64]bool

	// pathVerifyWarned records, per view, whether the "filesystem does not support path
	// verification; view uploads disabled" Warn has already fired. On
	// Windows a filesystem lacking GetFinalPathNameByHandle (some redirector/FUSE mounts)
	// makes every upload-source open fail closed (pathVerifyUnsupported) - one Warn per file
	// per scan would otherwise flood the log. The gap is a static property of the mount, so
	// the flag is never cleared in-process: a mount change that restores the capability is
	// re-probed only on daemon restart. On Unix pathVerifyUnsupported is always false, so this
	// map stays empty. Driven serially from the loop (scanView via localScan/coldStart), so no
	// lock is needed.
	pathVerifyWarned map[uint64]bool

	// pendingViewRenames maps a view id whose server-side NAME CHANGE could not yet be
	// adopted safely (viewrename.go) to the new NFC-normalized name.
	// While an entry exists the stored record keeps the OLD name - so the scan, the
	// inbound apply and the baselines stay mutually consistent - and localScan skips the
	// view, re-attempting the adoption (and re-logging the operator hint) each pass.
	// Without this deferral a rename would be read as every old-name baseline having
	// vanished and mass-delete the view server-side. Driven serially from the loop
	// (refreshViews/localScan), so no lock is needed.
	pendingViewRenames map[uint64]string

	// absentViews is the set of stored view ids that a server enumeration
	// (refreshViews → query_user_info) no longer reports - a view removed or revoked
	// server-side. While a view is marked, pollOnce's subscribe
	// list, localScan and coldStartPending all skip it, so a dead view id can no longer
	// fail the shared long_poll and spin the whole inbound loop every cycle - and the
	// drain suppresses its already-queued ops (drainBatch): they
	// stay queued without running or counting as failures, since every verb they would
	// run against the revoked view is doomed to a spurious rejection. The view's
	// local data and store records are LEFT UNTOUCHED - the view lifecycle is the
	// server's authority, not ours - and a reappearance in a later enumeration clears
	// the mark and resumes syncing (including the suppressed queue). pollOnce re-runs
	// the enumeration on a budgeted
	// steady-state timer (shouldRefreshViews) - tightened while any
	// mark exists - so a mid-run revocation is marked and a reappearance clears without
	// a daemon restart. The transition is logged once (Warn on going absent,
	// Info on returning) rather than every cycle. In-memory only: a daemon restart
	// re-derives it from the next refreshViews. Driven serially from the loop
	// (refreshViews/pollOnce/localScan/coldStartPending/drainQueue), so no lock is
	// needed.
	absentViews map[uint64]bool

	// absentQueueWarned tracks, per absent-marked view, that the drain has already
	// Warned about suppressing its queued ops, so the Warn fires
	// once per absence transition rather than once per drain. Cleared alongside the
	// absentViews mark when the view reappears, so a later revocation Warns again.
	// Loop-serial like absentViews (drainBatch schedules on the loop goroutine).
	absentQueueWarned map[uint64]bool

	// absentProbes counts, per stored view currently missing from the server enumeration, how
	// many consecutive refreshes ended in an INCONCLUSIVE view-root probe.
	// The probe gates the sticky absent mark so a paged/truncated view_list cannot suppress a
	// healthy view; this counter is what keeps the gate from becoming a wedge of its own, by
	// letting the mark through after absentProbeAttempts inconclusive answers. Reset when the
	// view reappears in an enumeration, when a probe positively finds its root, and when the
	// mark is finally applied. Loop-serial like absentViews.
	absentProbes map[uint64]int

	// absentConfirms counts, per stored view currently missing from the server enumeration,
	// how many CONSECUTIVE refreshes ended in a POSITIVE view-root probe (the path-error
	// sentinel). A single path-error is not corroboration: the codebase's own doctrine records
	// that a bare QueryNode "conflates a true server-side absence with a probe timeout"
	// (resolveServerNodeAbsence), which is exactly why every other absence prover corroborates
	// it. Acting on one would suppress a HEALTHY view - permanently, since a paged/truncated
	// view_list never re-enumerates it - on the very signal the probe gate was added to
	// defend against. Reset when the view reappears, when a probe disproves the absence, and
	// when the mark is applied. Loop-serial like absentViews.
	absentConfirms map[uint64]int

	// pollFailStreak counts consecutive SERVER-STATED rejections of the shared long_poll.
	// Long_poll was the last shared-fate stage: one view the server
	// refuses to subscribe to failed the poll for every view, and the retry re-issued the same
	// set forever. At pollFailBisectThreshold the per-view probes run (pollquarantine.go); any
	// poll that reaches the server resets it. Transport-level failures neither count nor clear
	// it - they are not evidence about the set (pollServerRejected).
	pollFailStreak int

	// pollProbeSuppress counts down the poll cycles left before the per-view probes may arm
	// again after a sweep that attributed the shared rejection to NO member.
	// Without it, a set-level or server-wide rejection - the shape the probes can never
	// attribute - re-ran the whole sweep every pollFailBisectThreshold retries for as long as it
	// lasted. Loop-serial like poisonedViews; aged in agePollQuarantines.
	pollProbeSuppress int

	// poisonedViews holds, per view quarantined from the shared long_poll subscription, how many
	// poll cycles the quarantine has served. A view lands here only after
	// its OWN single-member subscription was rejected - positive evidence about that view - and
	// leaves after pollPoisonRetryCycles so a server-side fix needs no restart. In-memory only,
	// loop-serial like absentViews.
	poisonedViews map[uint64]int

	// spinCursor latches, per view, the cursor at which syncView's pagination last hit a
	// non-empty page that could not advance the cursor. The intra-lease
	// spin-guard already stops re-pulling that page WITHIN one syncView call, but a
	// misbehaving/hostile server that keeps flagging the view via long_poll and returns the
	// same non-advancing page would otherwise make the NEXT cycle re-reconcile it - re-enqueuing
	// (and re-applying the downloads of) that page every poll forever. When the same stuck
	// cursor recurs, syncView skips the reconcile entirely (Warn + clean no-change). The latch
	// is cleared on any cursor advance or an exhausted (empty) page, so a server that recovers
	// resumes normally. In-memory only; driven serially from the loop, so no lock is needed.
	spinCursor map[uint64]int64

	// rootIDRetrySkip / rootIDRetryNext bound how often pollOnce re-runs refreshViews to
	// recover a view whose root node id the server never supplied. Without a
	// cap every poll cycle would re-issue a redundant refreshViews forever. The recovery is
	// attempted on a doubling cycle-skip backoff: rootIDRetrySkip counts cycles left to skip
	// before the next attempt, rootIDRetryNext is the interval to apply after it (1, 2, 4 …
	// capped at rootIDRetryMaxSkip). Both reset to 0 once every view has a root id, so a
	// later regression retries promptly. Driven serially from the loop, so no lock is needed.
	rootIDRetrySkip int
	rootIDRetryNext int

	// lastViewRefresh is when refreshViews last ATTEMPTED an enumeration (stamped at
	// the top of the call, success or failure), driving pollOnce's budgeted
	// steady-state re-enumeration: at most once per
	// viewRefreshInterval normally, tightened to viewRefreshAbsentInterval while any
	// absent mark exists so a revoked-and-restored view resumes promptly.
	// Attempt-stamping keeps a persistently failing enumeration at the budgeted
	// cadence instead of re-issuing the turn every poll cycle. Driven serially from
	// the loop, so no lock is needed.
	lastViewRefresh time.Time

	// coldStartCapSkip / coldStartCapNext bound how often coldStartPending re-attempts a
	// view whose cold start aborted on the ColdStartCatalogCap or the per-attempt page cap.
	// Without a backoff every cycle re-paginates the entire pre-cap
	// catalog from the server (and, on a history-replaying server, re-floods the log). Mirror
	// the rootIDRetry doubling cycle-skip backoff: coldStartCapSkip counts cycles left to skip
	// before the next attempt, coldStartCapNext is the interval applied after (1, 2, 4 …
	// capped at coldStartCapMaxSkip). Both clear when the view completes its cold start or
	// drops off the pending set. Driven serially from the loop, so no lock is needed.
	coldStartCapSkip map[uint64]int
	coldStartCapNext map[uint64]int

	// coldIngest, while a cold start streams (coldstartstream.go), plans what its walk has
	// listed since the last call. drainQueue calls it between windows, so a pass keeps taking
	// on new work while it transfers, and ends the pass when it reports stop; nil otherwise.
	// Loop goroutine only, like the rest.
	coldIngest func() (stop bool)
	// coldDrain replaces drainQueue inside a streaming cold start; nil means drainQueue. Tests
	// that assert what a cold start PLANS set it to a no-op, so the plan is still queued when
	// they look.
	coldDrain func(context.Context) error

	// lane keeps a worker free for the tree walk while one runs (walklane.go).
	lane walkLane

	// collisionNoted remembers, per view, the fold keys a durable collision trace has
	// already been recorded for (collisiontrace.go), so a collision that persists is
	// dead-lettered ONCE rather than on every 5s scan - the FIFO ring would otherwise
	// retire every other drop in minutes. A key that stops colliding (the user renamed one
	// of the two files) is forgotten, so a later recurrence is reported afresh. Driven
	// serially from the loop (localScan/coldStart), so no lock is needed.
	collisionNoted map[uint64]map[string]bool

	// collisionSeeded records the views whose collisionNoted memo has already been seeded
	// from the DURABLE trace ring. The memo alone is process-local while the condition it
	// suppresses (two on-disk names folding to one sync key) persists until a human renames
	// one of the files, so every daemon restart re-recorded a trace per still-colliding key -
	// inflating the `Dropped` count for a condition that never recurred and evicting genuine
	// drop traces from the 1000-entry FIFO. Seeded lazily on the first pass that reaches the
	// view, and only once: a trace the ring has since evicted is allowed to be re-recorded.
	collisionSeeded map[uint64]bool

	// unrepresentableNoted, unrepresentableSeeded and unrepresentableTraced bound the durable
	// trace of server names this filesystem cannot hold (unrepresentable.go): once per path,
	// seeded from the ring, capped per view. Loop-driven like collisionNoted.
	unrepresentableNoted  map[uint64]map[string]bool
	unrepresentableSeeded map[uint64]bool
	unrepresentableTraced map[uint64]int

	// syncViewMaxPages bounds how many pull_event pages a single incremental catch-up may
	// read before yielding, so one view cannot hold a worker lease indefinitely.
	syncViewMaxPages int
}

// coldStartCapMaxSkip caps the doubling cycle-skip backoff coldStartPending applies when a
// view's cold start keeps aborting on the catalog or page cap, so an over-cap view is
// re-attempted at most once per this many cycles rather than re-paginating the whole pre-cap
// catalog every cycle.
const coldStartCapMaxSkip = 32

// syncViewMaxPagesPerLease bounds how many pull_event pages one syncView call reads under a
// single worker lease before returning to continue on the next cycle from the durably-advanced
// cursor. The per-page cursor commit makes the continuation seamless, so
// this only bounds wall-clock per lease against a server stamping advancing non-empty pages
// without end (the stuck-cursor latch covers only the NON-advancing shape).
const syncViewMaxPagesPerLease = 10000

// rootIDRetryMaxSkip caps the doubling cycle-skip backoff pollOnce applies when re-running
// refreshViews to recover a still-missing view root id, so a server that never supplies one
// is re-queried at most once per this many poll cycles rather than every cycle forever.
const rootIDRetryMaxSkip = 32

// emptyScanConfirmThreshold is the default for limits.EmptyScanThreshold: the number of
// consecutive empty-while-synced scans a view must register before localReconcile
// propagates a whole-view deletion. Suppressing the first scans protects against a
// transient unavailability (e.g. an unmounted mount point that reads as an empty
// directory) wiping the entire remote view, while still letting a genuine delete-of-all
// converge after the state persists. The exact filesystem cause cannot be told apart from
// disk alone, so this is the data-preserving debounce, not a perfect classifier. The
// effective value is config-tunable (config.Limits.EmptyScanThreshold); this constant is
// the safe default and the test oracle.
const emptyScanConfirmThreshold = 3

// New constructs an Engine over the given store, transport seam, local sync root
// and self session id. A nil logger falls back to slog.Default so callers may omit
// it in tests.
func New(st *store.Store, t Transport, syncRoot, selfSessionID string, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	if selfSessionID == "" {
		// With no session id the self-echo guard (isSelfEcho) never matches, so every
		// server event - including this client's own writes echoed back - is treated as
		// remote and re-applied to disk. That is safe (the apply paths are idempotent and
		// content-guarded) but wastes work and can surface spurious churn, so flag the
		// degraded mode once at construction rather than letting it pass unnoticed.
		log.Warn("engine: empty self session id; all server events will be treated as remote (self-echo suppression disabled)")
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	limits := config.DefaultLimits()
	e := &Engine{
		store:              st,
		transport:          t,
		syncRoot:           syncRoot,
		selfSessionID:      selfSessionID,
		log:                log,
		limits:             limits,
		scanInterval:       time.Duration(limits.LocalScanInterval) * time.Second,
		turnTimeout:        time.Duration(limits.OpTurnTimeout) * time.Second,
		now:                time.Now,
		hostname:           host,
		emptyViewScans:     make(map[uint64]emptyScanStreak),
		emptiedDirScans:    make(map[emptiedDirKey]emptyScanStreak),
		dirSeenNonEmpty:    make(map[emptiedDirKey]fileIdent),
		unselectedWarned:   make(map[uint64]bool),
		staging:            newStagingTracker(time.Now()),
		opFailures:         make(map[uint64]int),
		opServerErrs:       make(map[uint64]serverErrStreak),
		opRetryAfter:       make(map[uint64]time.Time),
		opWarned:           make(map[uint64]opWarnLatch),
		nodeAbsentConfirms: make(map[string]absenceProbeStreak),
		viewRootDev:        make(map[uint64]uint64),
		deviceMoves:        make(map[uint64]uint64),
		deviceGuard:        make(map[uint64]deviceGuardState),
		deviceID:           osDeviceID,
		caseFoldProbe:      probeSyncRootCaseFold,
		readDir:            os.ReadDir,
		lstat:              os.Lstat,
		renameNoReplace:    fsutil.RenameNoReplace,
		conflictCopyLstat:  os.Lstat,
		syncDir:            fsutil.SyncDir,
		createStagingTemp:  os.CreateTemp,
		setMeta:            fsutil.SetMeta,
		scanBaselines:      st.ScanBaselinesForView,
		batchStore:         st.Batch,
		enqueueOp:          st.Enqueue,
		ackOp:              st.Ack,
		largeTreeWarned:    make(map[uint64]bool),
		pathVerifyWarned:   make(map[uint64]bool),
		resume:             make(chan struct{}, 1),
		pauseReq:           make(chan struct{}, 1),
		manualWake:         make(chan struct{}, 1),

		scanBaselinePrefix:    st.ScanBaselinesUnderPrefix,
		pendingViewRenames:    make(map[uint64]string),
		absentViews:           make(map[uint64]bool),
		absentQueueWarned:     make(map[uint64]bool),
		absentProbes:          make(map[uint64]int),
		absentConfirms:        make(map[uint64]int),
		poisonedViews:         make(map[uint64]int),
		spinCursor:            make(map[uint64]int64),
		coldStartCapSkip:      make(map[uint64]int),
		coldStartCapNext:      make(map[uint64]int),
		collisionNoted:        make(map[uint64]map[string]bool),
		collisionSeeded:       make(map[uint64]bool),
		unrepresentableNoted:  make(map[uint64]map[string]bool),
		unrepresentableSeeded: make(map[uint64]bool),
		unrepresentableTraced: make(map[uint64]int),
		syncViewMaxPages:      syncViewMaxPagesPerLease,
	}
	e.rootUnavailable = e.viewRootUnavailable
	return e
}

// SetLimits installs the operator-tuned resource knobs (DESIGN §11), back-filling
// any zero field with its built-in default so a partial set behaves as the
// historical constants. The daemon calls it once with the loaded, validated config
// before Run; it re-derives scanInterval from the new LocalScanInterval. SetLimits
// must be called before Run and not concurrently with it.
func (e *Engine) SetLimits(l config.Limits) {
	e.limits = l.WithDefaults()
	e.scanInterval = time.Duration(e.limits.LocalScanInterval) * time.Second
	e.turnTimeout = time.Duration(e.limits.OpTurnTimeout) * time.Second
}

// viewSuppressed reports whether a view must be left alone this cycle: either the
// server stopped enumerating it (reconcileAbsentViews) or the operator did not
// select it. The two have the same meaning on disk - stop polling and scanning
// the view, and touch nothing already there - so they share one gate.
//
// A store read failure does NOT suppress. Suppression stops a view's sync
// indefinitely, and the absence machinery already owns that decision behind a
// probe and a repetition budget; guessing it from a transient read error would
// silently strand a healthy view.
func (e *Engine) viewSuppressed(viewID uint64) bool {
	if e.absentViews[viewID] {
		return true
	}
	if e.views.Syncs(viewID) {
		return false
	}
	// Say so once per view. The four gates were silent about a selection-driven
	// skip, and the one message that did appear - drainBatch's - reported the
	// wrong cause ("absent from the server enumeration"), so an operator could not
	// tell a deselected view from a broken one.
	if !e.unselectedWarned[viewID] {
		e.unselectedWarned[viewID] = true
		e.log.Info("engine: view is not selected; leaving it alone (its local files are untouched)", "view", viewID)
	}
	return true
}

// SetViews installs the view selection and on-disk layout. Call it before Run:
// both decide where every path resolves, so changing them under a running engine
// would move the whole tree out from under the in-flight queue.
func (e *Engine) SetViews(v config.Views) {
	e.views = v
}

// rehashSlice is the share of the tree one local scan pass re-hashes in full, ignoring the cheap
// (size, mtime) short-circuit. That short-circuit cannot see a same-second, same-size
// modification, so every file is re-hashed once every limits.FullRehashInterval passes; 0
// disables it.
//
// The re-hash is spread, one bucket of paths per pass, rather than done all at once: the scan
// and the queue drain share the loop, and a whole-tree re-hash every N passes stopped every
// transfer for minutes on a large tree (measured on a real NAS: 2.7 minutes for 115 GB, once
// right after every start, since the pass counter starts at 0). Spread, each pass reads about
// 1/N of the bytes and the guarantee is the same.
type rehashSlice struct {
	of, idx uint64 // re-hash the paths whose bucket (of buckets) is idx; of == 0 re-hashes none
}

// covers reports whether the pass re-hashes the file with fold key key.
func (s rehashSlice) covers(key string) bool {
	switch s.of {
	case 0:
		return false
	case 1:
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return uint64(h.Sum32())%s.of == s.idx
}

// rehashSlice is this pass's share, decided once per pass in localScan before scanPass advances,
// so every view of a pass uses the same one.
func (e *Engine) rehashSlice() rehashSlice {
	interval := e.limits.FullRehashInterval
	if interval <= 0 {
		return rehashSlice{}
	}
	return rehashSlice{of: uint64(interval), idx: e.scanPass % uint64(interval)} // #nosec G115 -- checked positive
}

// largeTreeWarnBatchMultiple sets the "unusually large view" high-water mark, in
// drain-batch multiples, when no cold-start catalog cap is configured. With the default
// DrainBatch of 256 this warns once a view crosses ~5k entries - large enough to be
// noteworthy without firing on ordinary trees.
const largeTreeWarnBatchMultiple = 20

// largeTreeThreshold is the entry count past which a view is logged as unusually large
// (an early heads-up before the cold-start cap or memory pressure bites). When an operator
// has set a ColdStartCatalogCap, warn at half the cap so growth is visible before the cap
// aborts the pull; otherwise fall back to a fixed multiple of the drain batch. Under current
// validation (DrainBatch > 0, cap >= 0) this always returns a positive value - a degenerate
// cap of 1 yields a threshold of 1 (warns on any entry); the <= 0 guard in warnIfLargeTree
// is defensive only.
func (e *Engine) largeTreeThreshold() int {
	if cap := e.limits.ColdStartCatalogCap; cap > 0 {
		if hw := cap / 2; hw > 0 {
			return hw
		}
		return cap
	}
	return e.limits.DrainBatch * largeTreeWarnBatchMultiple
}

// warnIfLargeTree logs a one-shot Warn when a view's observed entry count crosses the
// large-tree high-water mark, and clears the per-view latch once it falls back below so a
// later regrowth warns again. where names the pass (e.g. "cold-start"/"local-reconcile")
// so the operator can tell which side surfaced the size. It is driven serially from the
// loop, so largeTreeWarned needs no lock.
func (e *Engine) warnIfLargeTree(viewID uint64, count int, where string) {
	hw := e.largeTreeThreshold()
	if hw <= 0 {
		return
	}
	if count >= hw {
		if !e.largeTreeWarned[viewID] {
			e.largeTreeWarned[viewID] = true
			if e.limits.ColdStartCatalogCap > 0 {
				e.log.Warn("engine: unusually large view; nearing the cold-start catalog cap",
					"view", viewID, "pass", where, "entries", count, "high_water", hw,
					"cold_start_cap", e.limits.ColdStartCatalogCap)
			} else {
				// With no cap there is no limit to near. It used to say "nearing resource
				// limits" here anyway, at ~5k entries, which on a real NAS is the first
				// thing a user sees and reads as a fault.
				msg := "engine: large view"
				if strings.HasPrefix(where, "cold-start") {
					msg = "engine: large view; its first sync lists the whole tree, transferring as it goes"
				}
				e.log.Info(msg, "view", viewID, "pass", where, "entries", count)
			}
		}
		return
	}
	delete(e.largeTreeWarned, viewID)
}

// SetWatcher installs the Phase-7 watcher factory Run uses to own the filesystem
// watcher's lifecycle: at startup Run calls factory with a context it cancels on exit
// (stopping the watcher) and e.syncRoot, and wires the returned debounced wake channel
// into the wake seam. The factory is injected rather than constructed here so the engine
// carries no direct fsnotify dependency and engine tests can pass a fake channel; the
// production factory is wired by the daemon (Phase 8) as an adapter that builds the
// watcher on the root Run supplies, e.g.
// func(ctx, root) { return watcher.New(root, log).Watch(ctx) } - note watcher.Watch(ctx)
// takes no root of its own, so the factory must honour the root argument, not capture one. A
// wake already installed via SetWake takes precedence - the factory is then not invoked -
// and a nil factory preserves today's behaviour. SetWatcher must be called before Run.
func (e *Engine) SetWatcher(factory func(ctx context.Context, root string) (<-chan struct{}, error)) {
	e.newWatcher = factory
}

// Pause requests the sync loop to stop running cycles (Phase 8b). It flips the
// in-memory paused flag so Run's waitWhilePaused gate blocks at the next cycle
// boundary - a long-poll already in flight is allowed to finish. It is safe to call
// concurrently with Run from the IPC goroutine and is idempotent. The engine does not
// persist the flag; the daemon does and re-applies it to a fresh
// engine on restart, so a pause holds across restarts until an explicit Resume.
func (e *Engine) Pause() {
	e.paused.Store(true)
	// Nudge an in-flight local scan so the pause takes effect within a bounded number of
	// walk entries instead of after a whole-tree walk+hash. The send is
	// non-blocking on a cap-1 coalescing channel; a token left behind because no scan was
	// running is drained by the next scan's watcher, which only arms while paused is set.
	select {
	case e.pauseReq <- struct{}{}:
	default:
	}
}

// errScanPaused is the cancellation CAUSE a pause-aborted local scan carries (see
// pauseAwareCtx). It never leaves localScan: the scan is read-only and simply did not happen,
// so the cycle continues to Run's pause gate instead of treating the aborted walk as a failure
// or - worse, since the abort surfaces as context.Canceled - as a terminal shutdown.
var errScanPaused = errors.New("engine: local scan cancelled by a pause request")

// pauseAwareCtx derives a child of ctx that is additionally cancelled (with the errScanPaused
// cause) when Pause is called, so the long walk+hash inside a scan stops promptly. The watcher
// goroutine exits with the returned stop function, and the stale-token drain keeps a pause that
// arrived while no scan was running - then resumed - from cancelling the next scan.
func (e *Engine) pauseAwareCtx(ctx context.Context) (context.Context, func()) {
	if !e.paused.Load() {
		select {
		case <-e.pauseReq: // drop a token no scan consumed and no pause is waiting on
		default:
		}
	}
	sctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		// An already-set pause needs no nudge: Run's gate normally blocks before the scan, so
		// this covers a pause that won the race with the scan's start (and any caller driving
		// localScan directly).
		if e.paused.Load() {
			cancel(errScanPaused)
			return
		}
		select {
		case <-e.pauseReq:
			cancel(errScanPaused)
		case <-done:
		case <-sctx.Done():
		}
	}()
	return sctx, func() {
		close(done)
		cancel(nil)
	}
}

// Resume clears the paused flag and signals Run's waitWhilePaused gate so the loop
// resumes at once rather than after the next poll wakeup. The signal send is
// non-blocking (the resume channel is coalescing, cap-1), so a Resume that races a
// loop not currently parked is never lost - the gate re-checks the flag on every
// wakeup. Safe to call concurrently with Run; idempotent.
func (e *Engine) Resume() {
	e.paused.Store(false)
	select {
	case e.resume <- struct{}{}:
	default:
	}
}

// Paused reports whether the loop is currently user-paused (Phase 8b). Safe to call
// concurrently with Run.
func (e *Engine) Paused() bool {
	return e.paused.Load()
}

// DiskFull reports whether the most recent apply failed with ENOSPC (the local disk
// is out of space) and no later apply has succeeded since. The condition is transient
// - the failing op is not dead-lettered, it stays queued and retries once space is
// freed - so this is surfaced as an operability status line rather than an error.
// Safe to call concurrently with Run.
func (e *Engine) DiskFull() bool {
	return e.diskFull.Load()
}

// StuckOps reports how many queued ops have been stuck for at least stuckThreshold
// consecutive drains without being classified terminal - either an op
// that keeps failing its own apply and never converges (an EACCES'd target, an
// unresolvable parent) or an op skipped every drain because a conflicting predecessor
// keeps failing. Surfaced as the `Stuck` line in `status`; the count
// self-clears once such an op finally applies, is dead-lettered, or its blocking
// predecessor resolves. Like DiskFull it is an operability hint, not an error. Safe to
// call concurrently with Run.
func (e *Engine) StuckOps() int {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	n := 0
	for _, failures := range e.opFailures {
		if failures >= stuckThreshold {
			n++
		}
	}
	return n
}

// TriggerSync requests an immediate sync (the `sync now` verb, Phase 8b): a
// non-blocking send on the coalescing manualWake channel, which pollOnce folds into
// its wake select exactly like a watcher wake. A trigger fired while the loop is
// blocked in a long-poll interrupts it (re-dialing the event connection and resetting
// the scan throttle); one fired while not blocked stays buffered and forces the next
// cycle's scan. Safe to call concurrently with Run; a burst collapses to one wake.
func (e *Engine) TriggerSync() {
	select {
	case e.manualWake <- struct{}{}:
	default:
	}
}

// waitWhilePaused blocks while the loop is user-paused (Phase 8b), returning when
// Resume clears the flag or ctx.Err() when the context is cancelled. It re-checks the
// flag after every resume signal so a stale buffered signal causes at most one spurious
// wakeup before re-blocking. Called at the top of Run's loop, before each cycle, so a
// long-poll already in flight finishes and pause takes effect at the cycle boundary.
func (e *Engine) waitWhilePaused(ctx context.Context) error {
	for e.paused.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.resume:
		}
	}
	return nil
}

// cursor returns the per-view sync cursor (View.LastSeenSyncID) recorded for
// viewID, or 0 when the view has no record yet - a cold start that pull_event(0)
// reads from the beginning.
func (e *Engine) cursor(viewID uint64) (int64, error) {
	v, err := e.store.GetView(viewID)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("engine: read cursor for view %d: %w", viewID, err)
	}
	return v.LastSeenSyncID, nil
}

// setCursor advances the recorded per-view cursor to syncID, preserving the rest
// of the view record. The cursor is advanced only after the events up to syncID
// have been applied (or recognised as self-echo), so it never runs ahead of what
// is on disk. A view that refreshViews has not yet upserted is an error rather than
// a silent create - the cursor belongs to a known view.
func (e *Engine) setCursor(viewID uint64, syncID int64) error {
	v, err := e.store.GetView(viewID)
	if err != nil {
		return fmt.Errorf("engine: advance cursor for view %d: %w", viewID, err)
	}
	v.LastSeenSyncID = syncID
	if err := e.store.PutView(viewID, v); err != nil {
		return fmt.Errorf("engine: write cursor for view %d: %w", viewID, err)
	}
	return nil
}

// refreshViews enumerates the account's views over a worker connection and upserts
// each into the store, preserving any cursor (and other state) already recorded. It
// records the view metadata - name, privilege and the root node id the outbound push
// addresses top-level entries under - but performs no directory scan or content
// hashing; populating the catalog from a cold start is handled by the cold-start pass
// (Phase 6e, coldstart.go).
//
// The root node id rides under query_user_info when the server reports it (decodeView reads
// it from the view reply). When it does not, the id is resolved over the same worker lease
// via QueryNode on the view ROOT PATH - the empty view-relative path (viewRootPath) SCOPED TO
// THE VIEW (QueryNode now carries view_id). The empty root path is identical for every view,
// so without the view_id scope every view missing its id would resolve to the SAME node and
// top-level creates would be misrouted into one view; the view_id disambiguates
// it the way the other view-scoped actions (list/query_node) already scope their path. This is
// NOT a lookup of the view NAME as
// a path: the empty root path is the documented
// view-relative addressing, so the resolved id is the view root, not some arbitrary node. The
// plan requires this fallback; without any resolution mechanism a view whose
// enumeration omits the root id would wedge every top-level create forever (resolveParentID →
// errParentUnknown). A QueryNode that tolerantly reports no id - OR one that fails outright -
// leaves NodeID empty WITHOUT aborting the refresh: the view is still persisted, so a fresh
// store is never left empty (which would block inbound sync too) by an enrichment that the
// unverified root addressing might fail persistently. pollOnce then re-runs
// refreshViews while any known view still lacks a NodeID (viewsMissingRootID), so a later
// enumeration can supply it rather than the op wedging with no way to recover. The
// exact addressing of the view root is still unverified against a live DSM (DESIGN §13).
func (e *Engine) refreshViews(ctx context.Context) error {
	// Stamp the steady-state re-enumeration budget on every ATTEMPT (not only success)
	// so a persistently failing enumeration is retried at the budgeted pollOnce cadence
	// (shouldRefreshViews) rather than on every poll cycle.
	e.lastViewRefresh = e.now()
	var views []actions.View
	err := e.withWorker(ctx, func(c syncClient) error {
		vs, err := c.QueryUserInfo(ctx)
		if err != nil {
			return err
		}
		// Fill any missing root node id over the same lease so the outbound push has a
		// parent to address top-level creates under. This enrichment is best-effort: a
		// QueryNode failure must NOT abort the turn, because that would leave a fresh store
		// with no persisted views and block BOTH inbound and outbound sync - and the view
		// root addressing is itself explicitly unverified (DESIGN §13), so a persistent
		// failure is plausible. On failure (or a tolerant empty reply) the view is persisted
		// with an empty NodeID; pollOnce's viewsMissingRootID retry re-runs refreshViews to
		// recover it later, while inbound sync proceeds and only top-level outbound creates
		// wait (resolveParentID → errParentUnknown, retried, not lost).
		for i := range vs {
			if vs[i].NodeID != "" {
				continue
			}
			n, qerr := c.QueryNode(ctx, vs[i].ViewID, viewRootPath)
			if qerr != nil {
				// Only an ORDINARY lookup failure is best-effort (log, persist with an empty
				// root id, retry later). A terminal or connection-level error must propagate so
				// the worker is replaced rather than released broken: returning nil here leaves a
				// byte-misaligned socket healthy in the pool (WithWorker only re-dials when fn
				// errors and the turn left it broken), and swallows a vanished session / context
				// cancellation the loop needs to react to.
				if isTerminalRefreshError(qerr) {
					return qerr
				}
				// Defence in depth on the same hazard: a turn that poisons the client must never
				// be swallowed by this best-effort branch, because returning nil releases a
				// byte-misaligned socket back to the pool. The POISONING turn now carries the
				// ErrConnBroken sentinel itself (actions.Client.doIdle wraps sentinel AND cause), so
				// isTerminalRefreshError already matches it; before that it returned a bare wrapped
				// network error and only this direct Broken() check caught it. The check
				// is kept because it keys on the client's OWN state rather than on the shape of an
				// error value: a future turn that poisons without wrapping the sentinel still
				// propagates here instead of leaking a broken worker.
				if br, ok := c.(brokenReporter); ok && br.Broken() {
					return qerr
				}
				e.log.Warn("engine: resolve view root id failed; persisting the view with an empty root id for a later retry",
					"view", vs[i].ViewID, "err", qerr)
				continue
			}
			vs[i].NodeID = n.FileID
		}
		views = vs
		return nil
	})
	if err != nil {
		return fmt.Errorf("engine: refresh views: %w", err)
	}
	for _, v := range views {
		id := uint64(v.ViewID) // #nosec G115 -- a server view_id is a non-negative identifier
		fresh := false
		rec, err := e.store.GetView(id)
		switch {
		case errors.Is(err, store.ErrNotFound):
			rec = store.View{}
			fresh = true
		case errors.Is(err, store.ErrCorruptRecord):
			// A bit-rotted view record must not abort the whole refresh: GetView is strict,
			// and the upsert loop returning any non-ErrNotFound error aborts refreshViews
			// every cycle forever - and with it reconcileAbsentViews below, re-opening the
			// spin class where a revoked HEALTHY view is never marked absent. It
			// cannot heal in-band either (every PutView sits behind the failing GetView).
			// Treat it as fresh (overwrite-heals doctrine, putBaselineTx): the
			// PutView below rewrites the corrupt record with live server facts. The lost
			// cursor (LastSeenSyncID) and InitialSyncDone mean this one view re-runs its
			// cold start - a safe-merge pass that deletes nothing - an accepted cost for
			// unwedging the whole daemon.
			e.log.Warn("engine: view record does not decode; healing it via overwrite with live server facts (the view re-runs cold start)",
				"view", id, "err", err)
			rec = store.View{}
			fresh = true
		case err != nil:
			return fmt.Errorf("engine: load view %d: %w", id, err)
		}
		// Normalize the server-supplied view name to Unicode NFC at this ingestion
		// boundary, like every other path the engine stores (Phase-9b invariant), so
		// the on-disk view directory the scanner/executor derive from rec.Name matches
		// the NFC-keyed baselines instead of diverging on a macOS NFD spelling.
		newName := normpath.Normalize(v.Name)
		switch {
		case rec.Name == newName || fresh:
			// A fresh record, an unchanged name, or a rename the server reverted:
			// nothing to adopt, and any deferred rename is moot. A PRE-EXISTING
			// record with an empty stored name is NOT short-circuited here: such a
			// view synced under the numeric-id fallback directory (viewDir), so a
			// later-supplied real name must go through the same adoption matrix -
			// adopting it blindly would re-point the scan away from the baselines'
			// directory, the mass-delete mechanism the matrix guards.
			rec.Name = newName
			delete(e.pendingViewRenames, id)
		case !e.views.Syncs(id):
			// A view the operator did not select must not have its local directory
			// moved: "deselecting a view leaves its local files alone" is the
			// contract, and the adoption matrix moves that directory. So DEFER,
			// exactly as the default branch does - do not adopt.
			//
			// Adopting the name here without moving the directory looked harmless
			// while the view stays deselected, and was not: deselection is
			// reversible (guardViewChange keys on layout identity, not selection),
			// and the matrix is the only thing that ever reconciles the stored name
			// with the on-disk directory. Once rec.Name held the new name, every
			// later refresh matched the "unchanged name" case and the rename was
			// forgotten for good - so re-selecting the view resolved it to a
			// directory nothing had written, stranding the tree at the old name and,
			// if anything happened to exist at the new one, reading every baseline
			// as locally deleted.
			//
			// localScan checks viewSuppressed before retrying a pending rename, so
			// nothing touches disk while the view stays deselected, and
			// retryPendingViewRename runs the full matrix on the first pass after it
			// is selected again.
			e.pendingViewRenames[id] = newName
		case e.adoptViewRename(id, viewRenameOldName(rec, id), newName):
			rec.Name = newName
			delete(e.pendingViewRenames, id)
		default:
			// A server-side view rename that cannot be adopted safely yet
			// (viewrename.go): keep the OLD name so the scan, the inbound apply
			// and the baselines stay mutually consistent, and record the deferral so
			// localScan suppresses the view and re-attempts the adoption each pass.
			// Overwriting the name here would point the scanner at the new directory
			// while every baseline still describes the old one - read as a mass local
			// delete and pushed to the server as one.
			e.pendingViewRenames[id] = newName
		}
		rec.Privilege = v.Privilege
		// Preserve a previously-captured root id when this enumeration could not supply
		// one (an empty NodeID never clobbers a known value), so a transient QueryNode
		// gap does not lose it.
		if v.NodeID != "" {
			rec.NodeID = v.NodeID
		}
		// Same preserve-on-gap rule for the small integer root node id: upload's
		// root_node_id and the long_poll subscribe entry both name it, and a zero is
		// rejected (12291), so a view_list that omits it must not clobber a known value.
		if v.RootNodeID != 0 {
			rec.RootNodeID = v.RootNodeID
		}
		if err := e.store.PutView(id, rec); err != nil {
			return fmt.Errorf("engine: upsert view %d: %w", id, err)
		}
	}
	return e.reconcileAbsentViews(ctx, views)
}

// reconcileAbsentViews updates the in-memory absent-view set (absentViews) from a
// fresh server enumeration: a stored view missing from the enumeration was removed or
// revoked server-side, so it is marked absent - pollOnce's subscribe list, localScan
// and coldStartPending all skip it - while its local data and store records are LEFT
// UNTOUCHED (the view lifecycle is the server's authority, not ours). A view that
// reappears in a later enumeration clears the mark and resumes syncing. The transition
// is logged once (Warn going absent, Info returning) rather than every cycle, and the
// mark is in-memory only, so a daemon restart re-derives it from the next
// refreshViews. Suppressing a dead view id keeps it out of the shared long_poll
// subscribe set, so a per-view long_poll rejection can no longer fail the whole poll
// and spin the inbound loop every cycle. The enumeration is
// treated as authoritative only when it is positively evidenced:
// QueryUserInfo rejects a contentless bare {} reply outright (errno-or-view_list, the
// List precedent) and refreshViews returns before this on any error - but an
// errno-evidenced reply WITHOUT a view_list still decodes tolerantly to a NIL slice,
// which is no evidence either way, so a nil enumeration marks (and clears) nothing
// here. Only a PRESENT view_list - a non-nil slice, possibly empty (a legitimate
// all-revoked account) - marks its omissions absent. Driven serially from the loop,
// so no lock is needed.
//
// A PRESENT view_list is still only evidence that the enumeration happened, not that it was
// COMPLETE: query_user_info is a single turn with no pagination, no total
// count and no end marker, so a DSM that pages or truncates view_list would have every omitted -
// healthy - view marked absent, and the mark is sticky (every later refresh re-reads the same
// page, so nothing clears it, in-process or across a restart). Whether the live DSM pages
// view_list is unconfirmed (DESIGN §13), so the mark is now gated on a per-view PROBE - a
// QueryNode of the view's own root - before absence becomes actionable: a probe that finds the
// root proves the omission was an enumeration artefact and the view keeps syncing. An
// inconclusive probe (any error that is not the positive path-error absence) does not suppress
// the view either, but it does not block convergence forever: after absentProbeAttempts such
// refreshes the view is marked anyway, because the alternative - never marking a genuinely
// revoked view - is the refresh spin this whole mechanism exists to stop, and the probe's
// own semantics against a revoked view are themselves unverified.
func (e *Engine) reconcileAbsentViews(ctx context.Context, views []actions.View) error {
	if views == nil {
		// Belt-and-braces under the decode-layer evidence requirement: a nil
		// enumeration carries no view_list at all (an errno-evidenced but list-less
		// reply QueryUserInfo tolerates), so it proves nothing about which views
		// exist - marking on it would suppress EVERY view's poll and scan on one
		// half-formed reply, a daemon "without errors, without sync" until restart.
		return nil
	}
	enumerated := make(map[uint64]bool, len(views))
	for _, v := range views {
		enumerated[uint64(v.ViewID)] = true // #nosec G115 -- a server view_id is a non-negative identifier
	}
	stored, err := e.store.ListViews()
	if err != nil {
		return fmt.Errorf("engine: list views for absent-view check: %w", err)
	}
	for _, sv := range stored {
		switch {
		case enumerated[sv.ViewID]:
			delete(e.absentProbes, sv.ViewID)
			delete(e.absentConfirms, sv.ViewID)
			if e.absentViews[sv.ViewID] {
				delete(e.absentViews, sv.ViewID)
				// Re-arm the drain's suppression Warn so a later revocation of the same
				// view Warns about its queued ops again.
				delete(e.absentQueueWarned, sv.ViewID)
				e.log.Info("engine: view reappeared in the server enumeration; resuming its sync", "view", sv.ViewID)
			}
		case !e.absentViews[sv.ViewID]:
			gone, perr := e.viewRootProbeConfirmsAbsent(ctx, sv.ViewID)
			switch {
			case perr != nil:
				e.absentProbes[sv.ViewID]++
				// An inconclusive answer breaks a streak of positive ones; the inconclusive
				// budget below still guarantees convergence, so an alternating server cannot
				// stall the mark forever.
				e.absentConfirms[sv.ViewID] = 0
				if e.absentProbes[sv.ViewID] < absentProbeAttempts {
					e.log.Warn("engine: view missing from the server enumeration but its root probe was inconclusive; NOT suppressing it yet",
						"view", sv.ViewID, "attempt", e.absentProbes[sv.ViewID], "attempts", absentProbeAttempts, "err", perr)
					continue
				}
				e.log.Warn("engine: view missing from the server enumeration and its root probe stayed inconclusive; suppressing its poll and scan (local data left untouched)",
					"view", sv.ViewID, "attempts", e.absentProbes[sv.ViewID], "err", perr)
			case !gone:
				// The root is still there, so the enumeration that omitted the view was
				// incomplete - a paged or truncated view_list, not a revocation. Suppressing
				// on it would stop a healthy view's sync for good.
				e.absentProbes[sv.ViewID] = 0
				e.absentConfirms[sv.ViewID] = 0
				e.log.Warn("engine: view missing from the server enumeration but its root is still present; treating the enumeration as incomplete and keeping the view in sync",
					"view", sv.ViewID)
				continue
			default:
				// The probe answered with the path-error sentinel. That is the POSITIVE
				// absence signal, but a single one is not corroboration: a bare QueryNode
				// conflates a true server-side absence with a probe timeout
				// (resolveServerNodeAbsence's own doc), and this arm bypasses the
				// absentProbeAttempts budget entirely - so one flaky turn would suppress a
				// healthy view that a paged view_list is already omitting, which is the exact
				// failure the probe gate exists to prevent. Corroborate by REPETITION across
				// refreshes: a timeout does not repeat absentRootConfirmations times in a row,
				// while a revoked view answers path-error every single refresh.
				e.absentConfirms[sv.ViewID]++
				if e.absentConfirms[sv.ViewID] < absentRootConfirmations {
					e.log.Warn("engine: view missing from the server enumeration and its root probe reports it gone, but one path-error is not corroborated; NOT suppressing it yet",
						"view", sv.ViewID, "confirmations", e.absentConfirms[sv.ViewID], "want", absentRootConfirmations)
					continue
				}
				e.log.Warn("engine: view no longer present in the server enumeration (repeated root probes confirm it is gone); suppressing its poll and scan (local data left untouched)",
					"view", sv.ViewID, "confirmations", e.absentConfirms[sv.ViewID])
			}
			e.absentViews[sv.ViewID] = true
			delete(e.absentProbes, sv.ViewID)
			delete(e.absentConfirms, sv.ViewID)
		}
	}
	return nil
}

// absentProbeAttempts bounds how many consecutive refreshes may end in an INCONCLUSIVE view-root
// probe before a view missing from the enumeration is suppressed anyway.
// The probe exists to keep a paged/truncated view_list from suppressing healthy views, but a
// probe that can neither confirm nor deny (a revoked view's QueryNode semantics are unverified -
// DESIGN §13 - and it may well fail with an access error rather than the path-error absence)
// must not leave a genuinely revoked view in the shared subscribe set forever: that is the
// refresh spin. Three refreshes at the steady-state re-enumeration cadence is well past a
// one-off turn failure.
const absentProbeAttempts = 3

// absentRootConfirmations is how many consecutive POSITIVE view-root probes (the path-error
// sentinel) must agree before a view missing from the enumeration is suppressed. A bare
// QueryNode conflates a true absence with a probe timeout - the reason every other absence
// prover in this package corroborates its verdict (absenceCorroboratedByNodeProbe,
// confirmAbsentByParentList) - and this arm is not covered by absentProbeAttempts, so acting
// on ONE path-error would let a single flaky turn permanently stop a healthy view that a
// paged view_list happens to omit. Repetition is the corroboration available here: the probe
// is the only path-addressed question the protocol answers about a view root, so it is asked
// again rather than cross-checked. Two agreeing refreshes cost one extra re-enumeration
// cycle before a genuine revocation is marked.
const absentRootConfirmations = 2

// viewRootProbeConfirmsAbsent probes a view the enumeration omitted by asking for its ROOT node
// (QueryNode on viewRootPath, the same addressing refreshViews uses to enrich a missing root id).
// It reports (true, nil) only on POSITIVE evidence the view is gone - the path-error sentinel,
// the server's "no such path" answer - and (false, nil) when the root answers with a node,
// which disproves the omission. Anything else is inconclusive and returned as an error: a
// tolerant reply carrying no id at all is no evidence either way, exactly as an errno-only
// view_list is not.
//
// A (true, nil) answer is the verdict of ONE turn, and the path-error sentinel conflates a
// real absence with a probe timeout, so the caller corroborates it across absentRootConfirmations
// refreshes before suppressing the view; this function deliberately does not carry that state.
func (e *Engine) viewRootProbeConfirmsAbsent(ctx context.Context, viewID uint64) (bool, error) {
	var node actions.Node
	err := e.withWorker(ctx, func(c syncClient) error {
		n, qerr := c.QueryNode(ctx, int64(viewID), viewRootPath) // #nosec G115 -- a stored view id came from a server view_id
		node = n
		return qerr
	})
	switch {
	case errors.Is(err, actions.ErrPathError):
		return true, nil
	case err != nil:
		return false, err
	case node.FileID == "":
		return false, fmt.Errorf("engine: view %d root probe carried no node id; its absence is inconclusive", viewID)
	default:
		return false, nil
	}
}

// viewRootPath is the view-relative path addressing a view's own root directory,
// used to resolve the view root node id via QueryNode when query_user_info omits it.
//
// It is "/", confirmed against a live DSM 7.2 / Drive Server 4.0.3. The empty
// path this previously used - flagged in DESIGN §13 as needing confirmation - is
// rejected with 28672, so the root id never resolved and every view was persisted
// with an empty root id, retried on each pass forever.
const viewRootPath = "/"

// resolveParentID returns the server file_id of the parent directory a new outbound op
// for relPath (a slash-separated, view-relative path) must be created under. A
// top-level entry's parent is the view root, taken from the view's stored NodeID; a
// nested entry's parent is the directory baseline at the parent path. A parent id that
// is not yet known - no view root captured, a missing parent baseline, or a parent
// baseline the server has not assigned an id to - yields errParentUnknown so the
// caller can retry the op after the parent's own op has recorded its id, rather than
// dropping it.
func (e *Engine) resolveParentID(viewID uint64, relPath string) (string, error) {
	parent := path.Dir(relPath)
	if parent == "." || parent == "/" || parent == "" {
		v, err := e.store.GetView(viewID)
		if isNotFound(err) {
			return "", fmt.Errorf("engine: view %d not yet refreshed: %w", viewID, errParentUnknown)
		}
		if err != nil {
			return "", fmt.Errorf("engine: resolve view root id %d: %w", viewID, err)
		}
		if v.NodeID == "" {
			return "", fmt.Errorf("engine: view %d root id unknown: %w", viewID, errParentUnknown)
		}
		return v.NodeID, nil
	}
	b, err := e.store.GetBaseline(viewID, parent)
	if isNotFound(err) {
		// On a case-insensitive filesystem the parent directory's baseline may keep a
		// different case than the op path's parent: a case-only directory rename
		// fold-matches in localReconcile (so it never re-creates the directory) without
		// rewriting the stored baseline path, so a brand-new child's op path carries the
		// on-disk case while the baseline keeps the original case. An exact lookup misses
		// and would wedge the child as errParentUnknown forever (no parent op will ever
		// run to unblock it). Retry by folded key before giving up. normpath.Key is NFC
		// identity on a case-sensitive filesystem, so this never conflates genuinely
		// distinct directories on Linux. The fold fallback also classifies a corrupt
		// fold-matching parent as terminal errParentCorrupt (not errParentUnknown), so a
		// case-variant corrupt parent dead-letters the child instead of wedging it forever
		// (mirroring the exact-key ErrCorruptRecord branch below).
		fb, ferr := e.parentBaselineByFoldedKey(viewID, parent)
		if ferr != nil {
			return "", fmt.Errorf("engine: parent baseline %d/%q unresolved: %w", viewID, parent, ferr)
		}
		b = fb
	} else if errors.Is(err, store.ErrCorruptRecord) {
		// The parent directory's baseline value no longer decodes, so its server id is
		// unrecoverable and no queued op will heal it (only an inbound overwrite/remove of
		// that record can). Surface it as terminal (errParentCorrupt) so the child is
		// dead-lettered rather than retried against the corrupt parent forever, which would
		// wedge the queue - the descendant half of the corrupt-record wedge.
		return "", fmt.Errorf("engine: parent baseline %d/%q corrupt: %w", viewID, parent, errParentCorrupt)
	} else if err != nil {
		return "", fmt.Errorf("engine: load parent baseline %d/%q: %w", viewID, parent, err)
	}
	if b.FileID == "" {
		// A parent baseline left in StateConflict by a rejected mkdir (or a read-only/crash
		// conflict) will never gain a server id: the divergence is owed to 6d. Distinguish it
		// from a parent whose op simply has not run yet (StatePending) so the child is
		// dead-lettered rather than retried forever.
		if b.State == store.StateConflict {
			return "", fmt.Errorf("engine: parent %d/%q in conflict: %w", viewID, parent, errParentConflicted)
		}
		return "", fmt.Errorf("engine: parent %d/%q has no server id: %w", viewID, parent, errParentUnknown)
	}
	return b.FileID, nil
}

// parentBaselineByFoldedKey resolves a parent directory baseline whose stored path differs
// from parent only by Unicode form / case (the case-only-rename case described in
// resolveParentID). It streams the view's baselines and returns the unique directory baseline
// whose folded key matches (nil error). A non-directory match, no match, an ambiguous match
// (more than one directory folding to the same key, which a consistent store should never
// hold), or a transient scan error yields errParentUnknown so the caller defers rather than
// mis-parenting the child.
//
// It streams via scanBaselines (not the materialising BaselinesForView) so it also sees the
// paths the stream SKIPPED as corrupt: a skipped record whose intact key
// folds to parent is the parent directory with an unreadable value - its server id is
// unrecoverable and no queued op will heal it, so it is surfaced as terminal errParentCorrupt
// (mirroring resolveParentID's exact-key ErrCorruptRecord branch and trackedFoldIndex's
// skipped-scan). BaselinesForView hid such a record, so the fold fallback used to read a
// folded corrupt parent as simply "no parent" and wedge the child on errParentUnknown forever
// (the folded-spelling half of the descendant wedge).
func (e *Engine) parentBaselineByFoldedKey(viewID uint64, parent string) (store.Baseline, error) {
	want := normpath.Key(parent)
	var match store.Baseline
	found := false
	ambiguous := false
	skipped, err := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		if pb.FileType != "dir" || normpath.Key(pb.Path) != want {
			return nil
		}
		if found {
			ambiguous = true // ambiguous: refuse rather than guess
			return errStopBaselineScan
		}
		match = pb.Baseline
		found = true
		return nil
	})
	if err != nil && !errors.Is(err, errStopBaselineScan) {
		return store.Baseline{}, errParentUnknown // transient scan error: defer, don't guess
	}
	if found && !ambiguous {
		return match, nil
	}
	if ambiguous {
		return store.Baseline{}, errParentUnknown
	}
	// No healthy fold-match. A record the stream skipped as corrupt whose path folds to the
	// parent is the parent directory with an unreadable value - terminal, not a missing parent.
	for _, p := range skipped {
		if normpath.Key(p) == want {
			return store.Baseline{}, errParentCorrupt
		}
	}
	// Genuinely no baseline tracks the parent under any spelling: surface the narrower
	// absent sentinel (still an errParentUnknown) so the outbound create verbs can probe
	// for a relocated/removed parent instead of waiting forever. The ambiguous
	// and transient-scan cases above stay the plain errParentUnknown deferral.
	return store.Baseline{}, errParentBaselineAbsent
}

// viewRootNodeID returns the view's root node id as the integer the upload verb's
// root_node_id field carries, or 0 when the store has not resolved one yet (the
// same tolerant degradation the long_poll subscribe entry applies).
func (e *Engine) viewRootNodeID(viewID uint64) int64 {
	v, err := e.store.GetView(viewID)
	if err != nil {
		return 0
	}
	return v.RootNodeID
}

// StuckOp describes one queued op that keeps failing, for the operator-facing listing.
type StuckOp struct {
	// ViewID is the view the op targets.
	ViewID uint64
	// Type is the op's type ("download", "upload", "rename", ...).
	Type string
	// Direction is the side it applies to ("inbound" to disk, "outbound" to the server).
	Direction string
	// Path is the view-relative path, and NewPath the destination for a move.
	Path    string
	NewPath string
	// Failures is how many consecutive drains it has failed.
	Failures int
	// Reason is what the last failure was, when the engine has a name for it. Empty when the
	// op is stuck without a recognised server verdict - including the common case of an op
	// merely BLOCKED behind a failing predecessor, which has no failure of its own.
	Reason string
}

// StuckDetails lists the ops behind the `Stuck` count, worst first, capped at limit.
//
// The count alone told an operator that something is wrong and nothing about what: the paths,
// and the reason each op keeps failing, were visible only in the log. The engine already holds
// the failure streaks and the last server-error code per op; the paths come from the durable
// queue, so the two are joined here rather than duplicated into the in-memory maps.
//
// Safe to call concurrently with Run: the streaks are copied under the lock and the queue is
// read afterwards. An op that drains between the two is simply absent from the listing, which
// is the right answer for it by then.
func (e *Engine) StuckDetails(limit int) ([]StuckOp, error) {
	type stuckFacts struct {
		failures int
		code     int64
		hasCode  bool
	}
	facts := make(map[uint64]stuckFacts)
	e.stuckMu.Lock()
	for seq, failures := range e.opFailures {
		if failures < stuckThreshold {
			continue
		}
		f := stuckFacts{failures: failures}
		if st, ok := e.opServerErrs[seq]; ok {
			f.code, f.hasCode = st.code, true
		}
		facts[seq] = f
	}
	e.stuckMu.Unlock()

	if len(facts) == 0 {
		return nil, nil
	}
	var out []StuckOp
	// Deliberately CROSS-view (so not scanQueuedInView): the stuck-op facts are keyed by
	// sequence across the whole queue, and the report names the view on each entry.
	err := e.store.ScanPending(func(qo store.QueuedOp) error {
		f, ok := facts[qo.Seq]
		if !ok {
			return nil
		}
		s := StuckOp{
			ViewID:    qo.ViewID,
			Type:      string(qo.Type),
			Direction: string(qo.Direction),
			Path:      qo.Path,
			NewPath:   qo.NewPath,
			Failures:  f.failures,
		}
		if f.hasCode {
			s.Reason = serverErrStreakReason(f.code)
		}
		out = append(out, s)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("engine: list stuck ops: %w", err)
	}
	// Worst first: the op that has failed most is the one to look at, and a stable tiebreak on
	// path keeps the listing from reshuffling between calls.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Failures != out[j].Failures {
			return out[i].Failures > out[j].Failures
		}
		return out[i].Path < out[j].Path
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
