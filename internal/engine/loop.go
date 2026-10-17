package engine

// This file carries the Phase-6b event loop: Run drives the remote→local cycle
// described in DESIGN §6 - long-poll the subscribed views on the dedicated event
// connection, pull each changed view's events from its per-view cursor over a worker
// connection, reconcile them into the durable queue, drain the queue to disk, and
// advance the cursor only after the batch has been applied. The loop runs until the
// context is cancelled or the pool reports a vanished session.
//
// A vanished session (transport.ErrPoolPaused, which wraps auth.ErrSessionVanished,
// or the sentinel itself) is terminal: Run returns it so the caller tears the pool
// down and re-pairs, rather than retrying silently. Any other error from a cycle is
// transient - logged, then retried after a short pause while the pool reconnects the
// dropped connection underneath.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// pollRetryDelay is how long Run pauses after a transient cycle error before
// retrying, so a persistently failing cycle does not spin while the pool reconnects.
const pollRetryDelay = time.Second

// noViewRetryDelay is how long pollOnce backs off when there are no subscribed views
// yet (a fresh account, or an initial refresh that failed), so Run re-discovers views
// on a timer instead of busy-looping on an empty view list.
const noViewRetryDelay = 5 * time.Second

// viewRefreshInterval bounds pollOnce's budgeted steady-state re-run of the view
// enumeration: the other refreshViews call sites fire only at
// startup, while the stored view list is empty, or while a view root id is missing,
// so without a periodic re-run a view granted, revoked or renamed mid-run was never
// discovered until a daemon restart - and an absent-view mark could never clear
// in-process ("clears on reappearance" was unreachable).
const viewRefreshInterval = 10 * time.Minute

// viewRefreshAbsentInterval is the tightened re-enumeration cadence applied while at
// least one view is marked absent, so a revoked-and-restored view resumes syncing
// within about a minute instead of waiting out the full steady-state interval. A
// query_user_info turn is cheap and the poll cycle is long-poll paced, so the extra
// idle turns are negligible.
const viewRefreshAbsentInterval = time.Minute

// fairnessPollTimeout caps the SERVER-SIDE long-poll timeout a fairness pass asks for - the
// one poll that deliberately ignores a pending wake so inbound is not starved by continuous
// local churn (see pollOnce).
//
// The fairness rule pays inbound with an outbound wait: the wake is consumed but the scan that
// pushes it only runs at the top of the NEXT cycle, so a local change (or an explicit
// `synosync sync`, whose manualWake folds into the same select) that arrives during a fairness
// poll waits out the poll. At the EventPollTimeout default that is five minutes - trading a
// permanent inbound starvation for a five-minute outbound one, on a sync client where the
// watcher exists to make pushes prompt. So a fairness pass waits this long and no longer: the poll
// is still a real subscribe-and-wait turn that reaches the server (the invariant the fairness fix
// rests on) and returns pending changes immediately. The server will not END it this early - it
// raises any timeout under actions.MinPollTimeout to 300 s - so a quiet fairness pass ends on this
// client-side deadline, which pollOnce treats as the expected end: no warning, and the re-dial
// the abandoned read needs. That costs one event-connection re-dial per local change, never more
// often than once per fairness pass. It applies ONLY on a fairness pass, so the idle steady state
// keeps DESIGN §2's single 300s blocked poll.
const fairnessPollTimeout = 15

// Run drives the remote→local sync loop until ctx is cancelled (Run returns
// ctx.Err()) or the session vanishes (Run returns the pause error for the caller to
// re-pair on). It first refreshes the view list, then repeatedly polls for and
// applies remote changes. Transient cycle errors are logged and retried after
// pollRetryDelay; the pause error short-circuits the loop.
func (e *Engine) Run(ctx context.Context) error {
	// Probe the sync root's real case-fold behaviour and set the process-wide normpath
	// flag from it, before any Fold-consuming goroutine starts: a
	// case-insensitive mount under Linux - or a case-sensitive volume under macOS -
	// would otherwise make the engine fold paths the wrong way and spuriously
	// rename/delete one file of a legitimate server-side case-pair. A probe failure
	// falls back to the GOOS default.
	e.applyCaseFold()
	// Bring up the filesystem watcher (Phase 7) when one is configured and no wake
	// channel was already installed via SetWake. The watcher runs under a child context
	// cancelled the moment Run returns, so it stops on exit (including a terminal error
	// that returns before ctx itself is cancelled). A startup failure is non-fatal: the
	// periodic localScan is the authoritative backstop, so the loop continues without the
	// latency optimization. e.wake is reset on exit so a closed channel is never reused.
	if e.wake == nil && e.newWatcher != nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		wake, werr := e.newWatcher(wctx, e.syncRoot)
		if werr != nil {
			e.log.Warn("engine: starting filesystem watcher failed; relying on the periodic scan", "err", werr)
		} else {
			e.wake = wake
			defer func() { e.wake = nil }()
		}
	}
	if err := e.refreshViews(ctx); err != nil {
		// A closed pool (or a vanished session) is terminal, not a transient refresh
		// failure: refreshViews leases a worker, so Acquire can surface ErrPoolClosed.
		// Logging and falling through would re-enter the loop, where pollOnce's
		// EventClient() builds a client over the pool's now-nil event session and panics.
		// isTerminal stops Run here for the caller to re-pair/tear down.
		if isTerminal(err) {
			return err
		}
		e.log.Warn("engine: initial view refresh failed", "err", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Block here while user-paused (Phase 8b): pause takes effect at the cycle
		// boundary, so a long-poll already in flight on the prior cycle has finished. A
		// cancelled context returns at once even while paused.
		if err := e.waitWhilePaused(ctx); err != nil {
			return err
		}
		if err := e.cycle(ctx); err != nil {
			if isTerminal(err) {
				return err
			}
			e.log.Warn("engine: sync cycle failed; retrying", "err", err)
			if err := sleepCtx(ctx, pollRetryDelay); err != nil {
				return err
			}
		}
	}
}

// cycle runs one iteration of the loop: it first drains any ops the durable queue
// still holds - left by a prior partial-failure drain or a crash before they applied
// - so the queue resumes even when no new view event arrives, and only then polls for
// fresh remote changes. Draining before polling is what lets a restart with queued
// work make progress without waiting for an unrelated event to wake the loop.
//
// A failure of the leading drain is logged but does NOT short-circuit the poll: a
// single op that fails every time (e.g. a download of a node deleted server-side in the
// window before it ran) must not block polling forever, or the very delete event that
// would supersede it could never be pulled - wedging the whole loop. The
// poll proceeds; the failed op stays queued for the next leading drain, and a later
// delete drops it (see reconcile.dropSupersededQueuedDownloads). A vanished session or a
// cancelled context is still terminal and returned at once.
func (e *Engine) cycle(ctx context.Context) error {
	// Run the one-time cold-start full-catalog pull for any view that has never synced
	// (a fresh pairing or a lost store) before the incremental path touches it: without
	// this, localScan would treat every existing local file as a brand-new create and
	// re-upload the whole tree, and the poll would have no baseline to reconcile against.
	// A per-view failure is logged inside coldStartPending and retried next cycle; only a
	// terminal error short-circuits the cycle here.
	if err := e.coldStartPending(ctx); err != nil {
		if isTerminal(err) {
			return err
		}
		e.log.Warn("engine: cold start pass failed; polling continues", "err", err)
	}
	// A subtree whose exclusion was lifted, or that a server move brought in from an excluded
	// path, is merged before the poll reads past it (exclude.go).
	if err := e.rescanPending(ctx); err != nil {
		if isTerminal(err) {
			return err
		}
		e.log.Warn("engine: rescan pass failed; polling continues", "err", err)
	}
	// Scan local disk for outbound changes before draining, so a newly-detected push op
	// is applied by the leading drain below rather than waiting through the next
	// long-poll (~300s at the default EventPollTimeout). A scan failure is logged, never
	// short-circuiting the poll - the same tolerance the leading drain already applies.
	if err := e.localScan(ctx); err != nil {
		if isTerminal(err) {
			return err
		}
		e.log.Warn("engine: local scan failed; polling continues", "err", err)
	}
	if err := e.drainQueue(ctx); err != nil {
		if isTerminal(err) {
			return err
		}
		e.log.Warn("engine: queue drain failed; polling continues and the op will be retried", "err", err)
	}
	return e.pollOnce(ctx)
}

// localScan walks every known view's on-disk directory and folds local creates,
// modifies, deletes and moves into outbound (DirOutbound) durable-queue ops, at most
// once per scanInterval. The emitted ops drain through the existing queue drain (the
// cycle's leading drain and syncView's trailing drain), so a local change is pushed even
// when no remote event arrives. The throttle keeps a tight loop iteration - a burst of
// changed views, or a re-poll after a long-poll deadline - from re-walking every view's
// directory each time. A reconcile failure for one view is logged and the next view is
// still scanned; only a cancelled context is terminal and returned for Run to stop on.
func (e *Engine) localScan(ctx context.Context) error {
	if !e.lastScan.IsZero() && e.now().Sub(e.lastScan) < e.scanInterval {
		return nil
	}
	views, err := e.store.ListViews()
	if err != nil {
		// Do NOT stamp lastScan here: a transient ListViews failure must not consume the
		// throttle window, or the next cycle would skip the scan for a whole scanInterval
		// even though no scan actually ran. Leaving lastScan unchanged lets the next cycle
		// retry promptly.
		return fmt.Errorf("engine: list views for local scan: %w", err)
	}
	// Build the queued-op index and the view-dir cache ONCE for the whole scan pass and share
	// them across every view's reconcile, rather than re-scanning the entire durable queue per
	// view. The scan and the queue drain run sequentially on the same goroutine (the
	// cycle calls localScan and drainQueue one after the other, never concurrently), so nothing
	// else mutates the queue while a scan is in flight - each reconcile just folds the ops it
	// enqueues back into the shared index, preserving the same cross-view visibility the
	// per-view rebuild gave.
	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		// As with ListViews above, a failure before any view is scanned must not consume the
		// throttle window - leave lastScan unchanged so the next cycle retries promptly.
		return err
	}
	// Decide ONCE per pass whether this is a periodic full-re-hash pass (Phase 9d safety net),
	// then advance the pass counter. A single per-pass decision keeps every view consistent -
	// either all views re-hash this pass or none do - rather than letting the counter tick
	// mid-pass and split the views. The cheap (size, mtime) short-circuit handles the other
	// passes; this one closes the same-second / same-size mtime-granularity race.
	rehash := e.rehashSlice()
	e.scanPass++
	// Derive a scan context a PAUSE request cancels as well. The walk is
	// bounded by ctx now, but a user pause does not cancel the run context - it flips a flag
	// Run's gate reads at the next cycle boundary - so `synosync pause` still had to wait out
	// a full-rehash walk of every view. The cause distinguishes the two: a pause-aborted scan is
	// not a failure and not terminal, it simply did not happen, and lastScan stays unstamped so
	// the scan re-runs promptly after the resume.
	sctx, stopWatch := e.pauseAwareCtx(ctx)
	defer stopWatch()
	for _, v := range views {
		if err := sctx.Err(); err != nil {
			if errors.Is(context.Cause(sctx), errScanPaused) {
				e.log.Info("engine: local scan interrupted by a pause request; it will re-run on resume", "view", v.ViewID)
				return nil
			}
			return err
		}
		if e.viewSuppressed(v.ViewID) {
			// A view removed/revoked server-side is not scanned for outbound changes: the
			// server would reject every push, and it is not ours to delete. Skip it until
			// it reappears in the enumeration.
			continue
		}
		if newName, ok := e.pendingViewRenames[v.ViewID]; ok {
			// A server-side view rename is awaiting safe adoption
			// (viewrename.go): the stored name and the on-disk layout disagree with the
			// server, so scanning would read the mismatch as local deletes and
			// mass-remove the view server-side. Skip the scan and re-attempt the
			// adoption instead - the retry re-logs the operator hint each pass and,
			// once safe, persists the new name so the NEXT pass scans cleanly (this
			// pass's shared dirCache/queued keys were built under the old name).
			e.retryPendingViewRename(v.ViewID, newName)
			continue
		}
		if !v.InitialSyncDone {
			// The cold-start full-catalog pull owns this view's first reconciliation: until it
			// has run there are no baselines, so localReconcile would diff every on-disk file
			// against nothing and enqueue an OpUpload for the entire tree. Skip it here; the
			// cycle's leading coldStartPending pass advances it.
			continue
		}
		if isReadOnlyView(v.Privilege) {
			// A read-only view's local edits cannot be pushed: the server would reject every
			// upload/mkdir/rename/remove as unauthorized, so the ops would retry forever. Skip
			// the outbound reconcile entirely rather than enqueue doomed push attempts.
			// The inbound (remote→local) path still applies the server's changes for this
			// view; only the push side is suppressed.
			continue
		}
		if _, err := e.localReconcileWith(sctx, v.ViewID, queued, dirCache, rehash); err != nil {
			if errors.Is(context.Cause(sctx), errScanPaused) {
				e.log.Info("engine: local scan interrupted by a pause request; it will re-run on resume", "view", v.ViewID)
				return nil
			}
			e.log.Warn("engine: local reconcile failed; will retry next scan", "view", v.ViewID, "err", err)
		}
	}
	// Stamp the throttle from scan COMPLETION, not its start: a scan that runs longer than
	// scanInterval would otherwise be immediately eligible again and re-walk every view's
	// directory back-to-back. Measuring from completion keeps a tight loop iteration from
	// re-scanning while still scanning at most once per interval. A per-view reconcile
	// failure above is logged-and-continued, so the pass as a whole still completed.
	e.lastScan = e.now()
	return nil
}

// viewsMissingRootID reports whether any known view still lacks a resolved root node id
// (NodeID), which top-level outbound creates need (resolveParentID falls back to it). Such a
// view cannot push top-level entries until its id is recovered, so pollOnce re-runs
// refreshViews to give a later enumeration a chance to supply it.
func viewsMissingRootID(views []store.ListView) bool {
	for _, v := range views {
		if v.NodeID == "" {
			return true
		}
	}
	return false
}

// shouldRefreshViews reports whether pollOnce's budgeted steady-state re-enumeration is
// due: the time since the last refreshViews ATTEMPT has passed
// viewRefreshInterval - tightened to viewRefreshAbsentInterval while any view is marked
// absent so a reappearance clears promptly. refreshViews stamps the budget on every
// attempt (Run's startup call, pollOnce's empty-list and missing-root-id recoveries, and
// this periodic re-run itself), so a cycle that already refreshed is never re-issued the
// turn by this check. An unset budget (no enumeration attempted yet) is due immediately.
func (e *Engine) shouldRefreshViews() bool {
	if e.lastViewRefresh.IsZero() {
		return true
	}
	interval := viewRefreshInterval
	if len(e.absentViews) > 0 {
		interval = viewRefreshAbsentInterval
	}
	return e.now().Sub(e.lastViewRefresh) >= interval
}

// isReadOnlyView reports whether a view's privilege string grants no write access, so its
// local changes must not be pushed. The server reports privileges like "rw" (read-write)
// or "ro" (read-only); a write grant carries a "w". An empty/unknown privilege is treated
// as writable (the conservative default that preserves the prior push-everything behaviour
// when the server omits the field). The exact privilege vocabulary is unconfirmed against a
// live DSM (DESIGN §13), so the test is on the presence of a write bit rather than an exact
// "ro" match.
func isReadOnlyView(privilege string) bool {
	return privilege != "" && !strings.Contains(strings.ToLower(privilege), "w")
}

// pollOnce runs one long-poll cycle: it subscribes to every known view, and for each
// view the server reports as changed pulls and applies its events. An empty change
// set (a clean long-poll timeout) is a no-op. When no views are known yet it tries to
// re-discover them and backs off for noViewRetryDelay, so the loop does not spin on an
// empty view list (a fresh account, or an initial refresh that failed).
func (e *Engine) pollOnce(ctx context.Context) error {
	views, err := e.store.ListViews()
	if err != nil {
		return fmt.Errorf("engine: list views: %w", err)
	}
	if len(views) == 0 {
		if err := e.refreshViews(ctx); err != nil {
			if isTerminal(err) {
				return err
			}
			e.log.Warn("engine: view refresh while idle failed", "err", err)
		}
		return sleepCtx(ctx, noViewRetryDelay)
	}
	// Recover a view whose root node id is still unknown: query_user_info omitted it and a
	// prior QueryNode fallback came back empty, so its NodeID was persisted empty. refreshViews
	// is otherwise only re-run while the view list is empty, so without this a top-level create
	// wedges on errParentUnknown forever. Re-run it (best-effort) so a later
	// enumeration can supply the id; a terminal error still stops. The retry is bounded by a
	// doubling cycle-skip backoff so a server that never returns a root id is not re-queried on
	// every single poll cycle forever; the backoff resets once all ids resolve.
	if viewsMissingRootID(views) {
		if e.rootIDRetrySkip > 0 {
			e.rootIDRetrySkip--
		} else {
			if err := e.refreshViews(ctx); err != nil {
				if isTerminal(err) {
					return err
				}
				e.log.Warn("engine: view refresh to recover a missing root id failed", "err", err)
			}
			// Schedule the next attempt further out (1, 2, 4 … capped) so a persistently
			// missing id is retried with diminishing frequency rather than every cycle.
			if e.rootIDRetryNext < 1 {
				e.rootIDRetryNext = 1
			}
			e.rootIDRetrySkip = e.rootIDRetryNext
			if e.rootIDRetryNext < rootIDRetryMaxSkip {
				e.rootIDRetryNext *= 2
				if e.rootIDRetryNext > rootIDRetryMaxSkip {
					e.rootIDRetryNext = rootIDRetryMaxSkip
				}
			}
		}
	} else {
		// Every known view now has a root id; clear the backoff so a future regression
		// (e.g. a freshly added view) is retried promptly rather than at the prior cadence.
		e.rootIDRetrySkip = 0
		e.rootIDRetryNext = 0
	}
	// Re-run the view enumeration on a budgeted steady-state timer.
	// Every other refreshViews call site fires only at startup, while the stored view
	// list is empty, or while a root id is missing, so in steady state a view granted,
	// revoked or renamed mid-run was never discovered until a restart - and an absent
	// mark could never clear in-process. refreshViews stamps the budget on every
	// attempt (including the call sites above), so a cycle that already refreshed does
	// not re-issue the turn. The refreshed absentViews set feeds the subscribe loop
	// below, so a revocation or reappearance takes effect this same cycle (a view
	// GRANTED mid-run joins at the next cycle's ListViews). A failure is logged and
	// the poll proceeds on the stored views; a terminal error stops Run.
	if e.shouldRefreshViews() {
		if err := e.refreshViews(ctx); err != nil {
			if isTerminal(err) {
				return err
			}
			e.log.Warn("engine: periodic view refresh failed; continuing with stored views", "err", err)
		}
	}
	// Release any quarantine that has served its cycles BEFORE the set is built, so a view whose
	// server-side cause was fixed rejoins on the very next poll.
	e.agePollQuarantines()
	subscribe := make([]actions.Subscription, 0, len(views))
	for _, v := range views {
		if _, poisoned := e.poisonedViews[v.ViewID]; poisoned {
			// This view was rejected on its OWN long_poll subscription, and the shared
			// subscription it poisoned is inbound sync for every OTHER view: keep it out until
			// its quarantine expires (pollquarantine.go). Its local data and store records are
			// untouched, exactly as for an absent view.
			continue
		}
		if e.viewSuppressed(v.ViewID) {
			// A view removed/revoked server-side (no longer in the query_user_info
			// enumeration) must not join the shared long_poll: a dead view id could fail
			// the whole poll and spin the inbound loop every cycle. Its data is left
			// untouched and it resumes if it reappears (reconcileAbsentViews logged the
			// transition).
			continue
		}
		if !v.InitialSyncDone {
			// A view that has not finished its cold-start full-catalog pull must not join the
			// incremental poll: subscribing before the baseline catalog exists would reconcile
			// events against an empty store. The cycle's leading coldStartPending pass owns it
			// until then.
			continue
		}
		subscribe = append(subscribe, actions.Subscription{
			ViewID: int64(v.ViewID), // #nosec G115 -- a view_id is a non-negative identifier
			NodeID: v.RootNodeID,
			SyncID: v.LastSeenSyncID,
		})
	}
	if len(subscribe) == 0 {
		// Every known view is still in cold start (or none completed it yet): there is nothing
		// to long-poll. Back off like the no-views case so the loop does not spin issuing empty
		// subscriptions while coldStartPending works through the backlog on the next cycle.
		return sleepCtx(ctx, noViewRetryDelay)
	}
	// Bound the long-poll read so a half-open event connection that never replies
	// cannot hang the loop (and its leading queue drain) forever. The server returns
	// within EventPollTimeout on a healthy connection; PollDeadlineSlack covers the
	// round-trip so a normal reply is never cut short.
	pollTimeout := e.limits.EventPollTimeout
	// The fairness debt is read (not yet consumed - an EventClient failure below must leave it
	// owing) before the deadline is computed, because a poll that will NOT honour the pending
	// wake asks the server for a SHORTER turn: see fairnessPollTimeout.
	honorWake := !e.pollDeferWake
	if !honorWake && pollTimeout > fairnessPollTimeout {
		pollTimeout = fairnessPollTimeout
	}
	deadline := time.Duration(pollTimeout+e.limits.PollDeadlineSlack) * time.Second
	if !honorWake {
		// No slack: the server will not answer a fairness pass before its 300 s floor, so the
		// slack that keeps a timed-out reply from being cut short has nothing to protect.
		deadline = time.Duration(pollTimeout) * time.Second
	}
	pollCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	ec, err := e.transport.EventClient()
	if err != nil {
		// The dedicated event connection is gone - the pool was closed or paused (a
		// vanished session) by a concurrent Close racing this poll. Both are terminal:
		// surfacing the error here stops Run for the caller to tear down / re-pair,
		// rather than building a client over a nil session and panicking.
		return err
	}
	// The event Client is rebuilt each poll and on every re-dial; the long_poll profile digest must
	// survive that, or each poll is answered at once (actions.Client.ProfileDigest).
	digester, _ := ec.(profileDigester)
	if digester != nil {
		digester.SetProfileDigest(e.profileDigest)
	}
	// The Phase-7 watcher wakes the loop on a local change so a push happens promptly
	// instead of waiting out the long-poll: while blocked below, a receive on e.wake
	// cancels pollCtx. The cancel is recorded in interrupted (and the wake itself in
	// woken) so the error handling can tell a wake-interrupt apart from a real
	// client-side deadline or a parent-ctx cancel. The wake channel is buffered and
	// coalescing, so a wake that fires while we are not blocked here stays pending and
	// is consumed by a later poll - never lost: the goroutine consumes a wake only via
	// this select, and any wake it consumes is honoured (the scan is forced) on BOTH the
	// error and success paths below, while a wake left in the buffer is picked up next
	// poll. stop+wg.Wait fence the goroutine so woken/interrupted are observed after it
	// has finished, and the goroutine never lingers past the poll.
	//
	// Fairness: the wake is a LATENCY optimization for the outbound
	// side, never a priority over inbound. A wake buffered while the cycle scanned and
	// drained is already pending when the next poll starts, so under continuous local
	// churn (an app writing into the sync root at least once per cycle) every poll was
	// cancelled at zero milliseconds and no poll ever reached the server's reply: remote
	// changes - including deletes and renames - were never pulled for as long as the
	// churn lasted, and the loop burned an event-connection re-dial per cycle. So a poll
	// that a wake cut short owes the next poll a completion: honorWake is false for that
	// one poll, which still CONSUMES the wake (the scan throttle is reset below, so the
	// push happens at the next cycle's localScan) but does not cancel the read. Steady
	// churn therefore alternates interrupt / complete instead of starving inbound, and
	// the fix holds whatever long_poll's cross-subscription semantics are: the completed
	// poll is a full subscribe-and-wait turn, not a replay of an abandoned one.
	//
	// honorWake was read above (it sizes the poll's own deadline); the debt is CONSUMED here,
	// after the EventClient failure arm, so a poll that never issued still owes its completion.
	e.pollDeferWake = false
	var woken, interrupted, wakeClosed atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-e.wake:
				// e.wake is nil when no watcher is installed; a receive on a nil
				// channel blocks forever, so this case is simply never selected and
				// the manualWake/deadline/stop cases drive the poll instead.
				if !ok {
					// The watcher exited (its fsnotify channels closed on an
					// internal failure) and closed the wake channel while Run's
					// ctx is still live. A closed channel reads ready forever, so
					// without this guard every poll would instantly "wake" and
					// spin (force-scan + re-dial each cycle). Record it so the
					// fence below clears the seam and the loop falls back to the
					// periodic scan; cancel like a real wake so we don't block out
					// the full long-poll deadline first. This arm cancels even on
					// a fairness pass: a closed channel is not a wake to defer,
					// and looping on it would spin this goroutine.
					wakeClosed.Store(true)
					cancel()
					return
				}
				woken.Store(true)
			case <-e.manualWake:
				// A `sync now` trigger (Phase 8b): folds into the wake path exactly
				// like a watcher wake - set woken so the success/error paths below
				// re-dial the byte-misaligned event connection and reset the scan
				// throttle. A trigger fired while not blocked here stays buffered
				// (cap-1) and is consumed by the next poll, forcing that cycle's
				// scan. It obeys the same fairness rule: a scripted `sync now` loop
				// must not starve inbound either.
				woken.Store(true)
			case <-pollCtx.Done():
				return
			case <-stop:
				return
			}
			if honorWake {
				interrupted.Store(true)
				cancel()
				return
			}
			// Fairness pass: the wake is noted (forcing the next cycle's scan) but the
			// long-poll runs on to the server's reply. Keep waiting so a further wake
			// arriving mid-poll is coalesced here rather than left to cancel the NEXT
			// poll's read the moment it starts.
		}
	}()
	changed, err := ec.LongPoll(pollCtx, subscribe, pollTimeout)
	if digester != nil {
		e.profileDigest = digester.ProfileDigest()
	}
	close(stop)
	wg.Wait()
	if err == nil {
		// The server answered, whether or not anything changed. On an idle sync this is
		// the only regular exchange there is, so without it `status` would age its
		// last-contact line to hours on a perfectly healthy client.
		e.progress.noteServerContact()
	}
	if wakeClosed.Load() {
		// Clear the seam so subsequent polls fall back to the periodic scan. Safe
		// to mutate e.wake here: stop+wg.Wait fenced the goroutine before this
		// write, and Run is single-goroutine. This is not a real local change, so
		// the scan throttle is left untouched.
		e.wake = nil
		e.log.Warn("engine: watcher exited (wake channel closed); falling back to periodic scan")
		if err != nil {
			// The in-flight read was cancelled/broken, so re-dial the
			// byte-misaligned event connection before the next poll, like the
			// wake/deadline paths.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if rerr := e.replaceEventConn(ctx, ec); rerr != nil {
				return rerr
			}
			return nil
		}
		// The poll raced the channel close and returned changes (err == nil)
		// before the cancel took effect; fall through and apply them below,
		// mirroring the real-wake success path, rather than discarding a good
		// result.
	}
	if woken.Load() {
		// A local change interrupted (or raced, or - on a fairness pass - merely
		// accompanied) the poll. Reset the scan throttle so the next cycle's localScan
		// runs immediately and pushes what the watcher signalled. This is the wake's
		// whole latency contract, and it holds on the fairness pass too: the poll that
		// ran to completion still forces the following scan.
		e.lastScan = time.Time{}
	}
	if interrupted.Load() && err != nil {
		// The wake cancelled an in-flight read, so the event socket is byte-misaligned
		// (the long-poll client reports Broken): re-dial it before the next poll - the
		// same recovery the deadline path uses - and treat the tick as a clean
		// no-change. The interrupted poll never reached the server, so it owes the next
		// poll a completion (see honorWake above); without that debt continuous churn
		// would cancel every poll here forever and inbound would never be pulled.
		// When the poll happened to return changes alongside the wake (err == nil) it
		// DID reach the reply, so no debt is owed and the changes are applied below.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.pollDeferWake = true
		e.log.Info("engine: long poll interrupted by a local change; re-dialing event connection " +
			"(the next poll runs to completion so inbound changes are not starved)")
		if rerr := e.replaceEventConn(ctx, ec); rerr != nil {
			return rerr
		}
		return nil
	}
	if err != nil {
		if ctx.Err() != nil {
			// The caller cancelled the run - terminal, surfaced as ctx.Err() by Run.
			return ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			// Our own client-side deadline fired (the parent ctx is still live): the event
			// connection is quiet or half-open. The interrupted read left the socket
			// byte-misaligned (the long-poll client is now Broken), so re-dial it before the
			// next poll - reusing it would either hang on the same dead socket until the next
			// deadline or misread a late reply to the abandoned request as the next poll's.
			// Then treat this tick as a clean no-change so the loop re-polls and
			// keeps draining the queue, rather than as a terminal context error.
			// Our deadline firing means the connection was quiet, not that the set was
			// rejected: it is not evidence for the bisect streak.
			e.clearPollFailStreak()
			if honorWake {
				// A fairness pass ending here is its expected end (fairnessPollTimeout); only a
				// full-length poll outliving its deadline says the connection went quiet.
				e.log.Warn("engine: long poll deadline elapsed; re-dialing event connection", "timeout_s", pollTimeout)
			}
			if rerr := e.replaceEventConn(ctx, ec); rerr != nil {
				return rerr
			}
			return nil
		}
		// A genuine long-poll failure that broke the socket: re-dial the event
		// connection so the next poll does not reuse the dead one. The pool only
		// re-dials on demand, so without this the loop would re-poll the same broken
		// socket forever and never recover from a dropped event connection.
		// A pause/cancel from ReplaceEvent is terminal and surfaced; any other re-dial
		// error is subordinate to the long-poll error reported below.
		if rerr := e.replaceEventConn(ctx, ec); rerr != nil {
			return rerr
		}
		// Shared-fate isolation: a persistent NON-connection-level
		// rejection of the whole subscribe set is the one shape that would otherwise fail
		// inbound sync for every view forever, since the retry re-issues the identical set.
		// Once the streak is long enough, probe each member on its own worker lease and
		// quarantine whichever view is rejected alone. Connection-level failures never reach
		// this (notePollFailure ignores them) and keep the plain retry above.
		if e.notePollFailure(err, subscribeIDs(subscribe)) {
			if _, perr := e.probePoisonedPollViews(ctx, subscribeIDs(subscribe)); perr != nil {
				return perr
			}
		}
		return fmt.Errorf("engine: long poll: %w", err)
	}
	// The poll reached the server and its subscribe set was accepted: only an UNBROKEN run of
	// rejections may arm the per-view probes.
	e.clearPollFailStreak()
	for _, id := range changed {
		if err := e.syncView(ctx, uint64(id)); err != nil { // #nosec G115 -- a view_id is a non-negative identifier
			if isTerminal(err) {
				return err
			}
			// A non-terminal syncView failure (a pull/reconcile error, e.g. a transient
			// backend read) must NOT abort the whole poll before the remaining changed
			// views are synced - a view sorted earlier in `changed` that fails every cycle
			// would otherwise starve inbound sync for every later view (mirroring
			// syncView's own trailing-drain tolerance and localScan's log-and-continue).
			// The failed view stays flagged and is retried next cycle.
			e.log.Warn("engine: sync view failed; continuing to remaining changed views",
				"view", id, "err", err)
			continue
		}
	}
	return nil
}

// syncView drains a view's whole pending event backlog from its recorded cursor under a
// single worker lease, then drains the durable queue to disk. It pulls a page of events,
// reconciles them into the durable queue, durably advances the cursor by that page, and
// repeats until a page comes back empty (the backlog is exhausted) or the cursor stops
// advancing on a non-empty page (a spin-guard).
// That intra-lease spin-guard only stops re-pulling the page WITHIN one call; a
// cross-cycle latch (spinCursor) additionally keeps a misbehaving
// server that re-flags the view every long_poll from re-reconciling the same non-advancing
// page (re-enqueuing and re-applying its downloads) every poll forever - the page is
// reconciled once and then skipped until the cursor advances again.
// Reading the full backlog per lease - rather than one page per long-poll cycle - is what
// lets a daemon that fell far behind (a long offline stretch or a large burst, where
// pull_event paginates past its ~10k cap, DESIGN §13) catch up promptly instead of one
// page per long-poll cycle (~300s at the default EventPollTimeout).
//
// The cursor is advanced as soon as each page's events are durably enqueued - not after
// the apply - because the durable queue, not the cursor, is the replay unit for the apply:
// un-applied ops stay in the queue and are retried by the next cycle's leading drain.
// Advancing the cursor first is what stops a partial-drain failure from re-pulling the
// same events and re-enqueuing duplicate (and, for renames, no-longer-applicable) ops. A
// re-pull in the narrow window between enqueue and the cursor write only re-enqueues
// idempotent ops (download/mkdir/remove, and an idempotent rename), so a duplicate is
// harmless. The per-page cursor is taken as the max of the reconcile result and the
// server-reported next cursor, so a page that produced no applicable ops still advances
// past the events it acknowledged. Committing the cursor page-by-page (a store-only write,
// no network) means a crash mid-backlog resumes from the last committed page rather than
// re-pulling from the original cursor.
//
// reconcile and setCursor are store-only, so running them inside the lease keeps the
// network turn (PullEvent) the only thing the lease serialises. drainQueue is deliberately
// NOT called inside the lease - it takes its own worker lease, and a nested lease at
// numWorkers=2 can deadlock - so the single trailing drain below applies everything the
// pagination loop enqueued, with the same tolerate-and-continue handling as before.
func (e *Engine) syncView(ctx context.Context, viewID uint64) error {
	cursor, err := e.cursor(viewID)
	if err != nil {
		return err
	}
	pages := 0
	err = e.withWorker(ctx, func(c syncClient) error {
		for {
			evs, next, perr := c.PullEvent(ctx, int64(viewID), e.viewRootNodeID(viewID), cursor) // #nosec G115 -- a view_id is a non-negative identifier
			if perr != nil {
				// Classify a decoder-cap breach (a legitimately enormous page tripping the
				// frame-byte or node/depth budget) so the wedge is diagnosable rather than a
				// bare transient.
				e.warnIfDecoderCap(perr, viewID, "pull_event")
				return fmt.Errorf("engine: pull events for view %d: %w", viewID, perr)
			}
			if len(evs) == 0 {
				// The backlog is exhausted: no events past this cursor. The server may
				// still report a next cursor strictly ahead of ours (events between the
				// two aged out / were compacted server-side); honour it so the cursor is
				// not pinned below the true server position - otherwise long_poll keeps
				// flagging the view and we re-pull the same empty page every cycle. This
				// restores the pre-pagination single-page behaviour, which advanced the
				// cursor to next even on an empty batch.
				delete(e.spinCursor, viewID) // exhausted - clear the cross-cycle spin latch
				if next > cursor {
					if serr := e.setCursor(viewID, next); serr != nil {
						return serr
					}
				}
				return nil
			}
			// Compute the cursor this page WOULD advance to WITHOUT mutating the store, so a
			// page that cannot advance is detected BEFORE reconcile re-enqueues its ops. This
			// is exactly what reconcile+the next-max below produce (reconcile's cursor is
			// max(cursor, max event sync_id), then max with next), so the advancing branch
			// commits to this same value.
			prospective := cursor
			for _, ev := range evs {
				if ev.SyncID > prospective {
					prospective = ev.SyncID
				}
			}
			if next > prospective {
				prospective = next
			}
			if prospective <= cursor {
				// A non-empty page that does not advance the cursor. The intra-lease spin-guard
				// (returning here) stops re-pulling it within this call, but a misbehaving/hostile
				// server that keeps flagging the view via long_poll and returns the same
				// non-advancing page would make the NEXT cycle re-reconcile it - re-enqueuing (and
				// re-applying the downloads of) this page every poll forever. So latch the stuck
				// cursor: reconcile the page ONCE (the first time it appears), then on any later
				// cycle that re-enters at the SAME stuck cursor, skip reconcile entirely.
				// The latch is cleared on any cursor advance or exhausted
				// page above, so a server that recovers resumes normally.
				if stuck, ok := e.spinCursor[viewID]; ok && stuck == cursor {
					e.log.Warn("engine: pull_event page still not advancing cursor; skipping re-reconcile",
						"view", viewID, "cursor", cursor, "next", next, "events", len(evs))
					return nil
				}
				if _, _, rerr := e.reconcile(viewID, evs, cursor); rerr != nil {
					return fmt.Errorf("engine: reconcile view %d: %w", viewID, rerr)
				}
				e.spinCursor[viewID] = cursor
				e.log.Warn("engine: pull_event pagination stopped early; cursor did not advance",
					"view", viewID, "cursor", cursor, "next", next, "events", len(evs))
				return nil
			}
			if _, _, rerr := e.reconcile(viewID, evs, cursor); rerr != nil {
				return fmt.Errorf("engine: reconcile view %d: %w", viewID, rerr)
			}
			delete(e.spinCursor, viewID) // made progress - clear the cross-cycle spin latch
			if serr := e.setCursor(viewID, prospective); serr != nil {
				return serr
			}
			cursor = prospective
			pages++
			if pages >= e.syncViewMaxPages {
				// The cursor was durably committed page-by-page above, so returning here bounds
				// wall-clock per lease without losing progress: the next cycle's syncView
				// resumes from the committed cursor. This guards against a server that stamps
				// advancing non-empty pages without end (the spin latch closes only
				// the NON-advancing shape).
				e.log.Warn("engine: pull_event pagination hit the per-lease page cap; "+
					"continuing next cycle from the durable cursor",
					"view", viewID, "cursor", cursor, "pages", pages)
				return nil
			}
		}
	})
	if err != nil {
		return err
	}
	if err := e.drainQueue(ctx); err != nil {
		if isTerminal(err) {
			return err
		}
		// A trailing-drain failure must NOT abort the poll before the remaining changed
		// views are pulled - the same reasoning as cycle()'s leading drain (which already
		// tolerates this): an op for THIS view that fails every cycle would otherwise
		// starve every later changed view, including the one whose delete event would
		// supersede the stuck op and let the queue converge. Log and continue; the failed
		// op stays queued for the next leading drain, and pollOnce moves on to the next
		// view.
		e.log.Warn("engine: view apply drain failed; continuing to remaining views", "view", viewID, "err", err)
	}
	return nil
}

// replaceEventConn re-dials the dedicated event connection when the long-poll turn
// just run left it byte-misaligned (the client reports Broken). The pool only re-dials
// on demand, so without this the loop would re-poll the same dead socket forever - or,
// after a client-side deadline interrupted an in-flight read, misread a late reply to
// the abandoned request. Every error the production ReplaceEvent can return
// is terminal: a pause (a vanished session), a closed pool (ErrPoolClosed), or a context
// cancellation/deadline - the pool's reconnect retries genuine transient dial failures
// internally and surfaces only these. They are all returned so Run stops, because a
// closed/paused pool yields a nil event Session and the next EventClient() would panic in
// actions.NewClient(nil, …). A defensive fallback logs anything else (a
// non-production fake) and lets the caller retry. A healthy connection, or a fake client
// that does not implement brokenReporter, is a no-op.
func (e *Engine) replaceEventConn(ctx context.Context, ec syncClient) error {
	br, ok := ec.(brokenReporter)
	if !ok || !br.Broken() {
		return nil
	}
	if rerr := e.transport.ReplaceEvent(ctx); rerr != nil {
		if isTerminal(rerr) {
			return rerr
		}
		e.log.Warn("engine: replacing event connection failed; will retry", "err", rerr)
	}
	return nil
}

// isDecoderCapError reports whether err was produced by the sproto decoder refusing an
// oversized frame: the aggregate frame-byte budget (ErrFrameTooLarge, an errors.Is-matchable
// sentinel), the per-Decode drain budget (ErrDrainTooLarge - a Binary whose
// claimed length alone exceeds what one Decode may discard) or the structural node/depth caps
// (plain errors, matched by their stable message text). A legitimately enormous List (~90k+ children) or pull_event page can trip these on an
// extreme-but-valid tree, where the engine otherwise reads the failure as an untyped transient
// - confirmAbsentByParentList stays perpetually unconvincing, a pull page never advances the
// cursor - so callers can Warn with recognizable text rather than leaving the wedge diagnosable
// only as a bare Stuck count. This drives no control flow (the op still
// retries transiently); full paginated decoding of such a reply is protocol work deferred past
// this round (DESIGN §13). The node/depth caps carry no exported sentinel, so they are matched
// on the decoder's stable "value count exceeds" / "nesting depth exceeds" message text.
func isDecoderCapError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sproto.ErrFrameTooLarge) || errors.Is(err, sproto.ErrDrainTooLarge) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "value count exceeds") || strings.Contains(msg, "nesting depth exceeds")
}

// warnIfDecoderCap logs a recognizable Warn when err came from a reply breaching a decoder cap,
// so an extreme-but-legal List/pull_event reply is diagnosable rather than
// surfacing only as a bare transient/Stuck. A non-cap error is left to the caller's ordinary
// handling. (The List consumers - confirmAbsentByParentList and its callers - do not call this
// per probe; their breach surfaces through noteOpFailure, which runs it once when the op's
// retries reach stuckThreshold, so the diagnosis rides the escalation instead of flooding the
// rotating log every drain. Paginated decoding itself stays deferred protocol work, DESIGN §13.)
func (e *Engine) warnIfDecoderCap(err error, viewID uint64, where string) {
	if isDecoderCapError(err) {
		e.log.Warn("engine: reply exceeded a decoder cap (frame-byte or node/depth budget); "+
			"the view cannot progress until the tree/page shrinks or paginated decoding lands (DESIGN §13)",
			"view", viewID, "where", where, "err", err)
	}
}

// isPause reports whether err signals a vanished session - the pool's paused state
// or the underlying auth sentinel - which Run treats as terminal.
func isPause(err error) bool {
	return errors.Is(err, transport.ErrPoolPaused) || errors.Is(err, auth.ErrSessionVanished)
}

// isTerminal reports whether err must stop the loop rather than be retried: a vanished
// session (the pool's paused state or the auth sentinel), a rejected client_version
// (transport.ErrPoolClientRejected from the connect axis, or auth.ErrClientVersion from an
// action turn of a live session - the server refused this binary; re-dialing the
// same client cannot help, so the daemon stops for a client upgrade), a closed pool,
// or a cancelled or expired context. A closed pool never reopens - and once the pool is closed or paused
// its Event() returns nil, so a retry that reached the next long poll would build a client
// over the nil event session and panic in actions.NewClient. Treating
// ErrPoolClosed as terminal here is what stops Run from retrying it (replaceEventConn and
// the worker drains surface it) instead of spinning toward that nil session; Run returns it
// for the caller to tear the pool down and re-pair, exactly as it already does for a pause.
// (EventClient now also guards the nil and returns a terminal error, so a concurrent close
// that wins the race is handled rather than panicking - this keeps the loop from relying on
// it.)
//
// The context arms match only the RUN context's own cancellation or deadline: a bounded
// worker turn aborted by its limits.OpTurnTimeout deadline surfaces as the typed transient
// errTurnTimeout (turnLimitedClient maps it before it can reach any isTerminal consumer -
// Run, cycle, syncView, coldStartPending, pollOnce), because reading that per-turn
// DeadlineExceeded as terminal here crashed the daemon on the first slow turn.
// The long-poll's own client-side deadline never reaches this classifier either -
// pollOnce handles it inline and re-dials the event connection.
//
// The client-version arm matches the auth sentinel as well as the pool's, for the same
// reason the vanished-session arm does: 20481 is a verdict about THIS
// BINARY, not about one op, and the action layer now maps it to the shared
// auth.ErrClientVersion. Received on a turn of an already-established session it used to
// arrive as an anonymous *ServerError and be consumed on the PER-OP axis - after a streak
// of identical rejections every outbound op was marked StateConflict and dead-lettered
// (one-way: localReconcile never re-derives such a push), inbound ops retried forever, and
// the "update synosync" hint was reachable only if the connection happened to drop and the
// re-dial hit the same rejection. Stopping Run here routes it to the daemon's existing
// terminal classification (terminalStopReason / runDaemonLoop's stderr hint), which already
// keys on this sentinel, and leaves the whole queue intact for the upgraded binary. The pool
// is not paused separately: the daemon closes it on the way out of the sync loop, and a
// pause would only re-label an error nobody reads after the stop.
func isTerminal(err error) bool {
	return isPause(err) || errors.Is(err, transport.ErrPoolClientRejected) ||
		errors.Is(err, auth.ErrClientVersion) ||
		errors.Is(err, transport.ErrPoolClosed) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// sleepCtx waits for d or until ctx is cancelled, returning ctx.Err() on
// cancellation and nil when the full delay elapsed.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// subscribeIDs projects a subscribe set down to its view ids, which is all the
// poll-quarantine bookkeeping needs.
func subscribeIDs(subs []actions.Subscription) []int64 {
	ids := make([]int64, 0, len(subs))
	for _, s := range subs {
		ids = append(ids, s.ViewID)
	}
	return ids
}
