package daemon

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// maxConflictEntries caps how many conflict records buildConflicts returns. The
// CLI buffers the whole IPC reply under a 1 MiB cap (ipc maxResponseBytes), so an
// unbounded report would make every `conflicts` call fail on a truncated read
// once enough conflicts accumulate (a realistic mass divergence - e.g. a restored
// backup - reaches that with a few thousand records). When the cap bites, the
// newest records by DetectedAt are kept and the report is marked Truncated with
// the full Total. A var so tests can shrink it.
var maxConflictEntries = 1000

// maxConflictReportBytes is the encoded-size budget for the ConflictReport:
// headroom under the client's 1 MiB reply cap for the JSON-RPC response
// envelope. The record cap alone does not bound the report - paths can be long -
// so buildConflicts trims the (newest-first) list until the encoded report fits.
// A var so tests can shrink it.
var maxConflictReportBytes = 896 << 10

// viewCountsCacheTTL bounds how stale a cached per-view count pair (the
// StateConflict baselines and the durable keep-both conflict records) may be
// served by buildStatus. Both are observability signals, so lagging reality by up
// to the TTL is acceptable; recomputing them on every `status` call would
// JSON-decode every baseline and every conflict record of every view (O(tree)),
// which on a very large tree pushes against the IPC handler budget.
//
// Like conflictsCacheTTL, it must EXCEED the IPC handler budget (ipc
// handlerTimeout, 8s): a refill slower than that budget is abandoned mid-pass, and
// with a shorter TTL the stamps it did land would already have expired by the time
// any retry can reach them, so every call would restart the same walk and `status`
// would never succeed at all. TestViewCountsCacheTTLExceedsHandlerBudget pins it.
const viewCountsCacheTTL = 30 * time.Second

// conflictsCacheTTL bounds how stale a cached ConflictReport may be served by
// buildConflicts. Like the per-view counts, the report costs a JSON decode of
// every conflict record of every view, and nothing invalidated it: a client
// polling `conflicts` (or several CLI calls racing) re-ran the whole scan against
// the sync data plane every time.
//
// The TTL must EXCEED the IPC handler budget (ipc handlerTimeout, 8s), which is why
// it matches viewCountsCacheTTL rather than the few seconds a burst-collapse
// alone would need. On exactly the mass-divergence tree the cache exists for, a
// whole-bucket scan outruns that budget: the triggering call is abandoned at 8s
// (the scan itself runs on to completion under scanBudget and stamps the cache),
// so the operator sees an error and retries - and with a TTL shorter than the
// budget the stamped report has already expired by the time any human retry can
// land, so the retry starts another doomed scan and `conflicts` never succeeds
// at all. That is the same "every call misses and reruns the full scan forever"
// failure the completion stamp and scanBudget both exist to prevent, just moved
// into the TTL. Serving a report up to the TTL stale is the cheap side of that
// trade: the records are durable, and the next call refreshes them.
// A var so tests can drive it.
var conflictsCacheTTL = 30 * time.Second

// scanBudget bounds ONE cache-filling store scan - scanViewCounts' per-view
// conflict+baseline walk, scanConflicts' whole-bucket walk - independently of the
// IPC handler budget of whichever call happened to trigger it.
//
// The two budgets must not be the same one. The handler budget (ipc
// handlerTimeout, 8s) exists so a slow control-plane verb cannot hold up daemon
// shutdown, and the IPC layer enforces it by ABANDONING the handler, not by
// killing its work. Running the scan under it too made a scan slower than the
// budget self-defeating: it was cancelled at 8s, cached nothing, and every
// subsequent `status`/`conflicts` call restarted and re-aborted the same scan -
// precisely the "every call misses and reruns the full scan forever" failure the
// completion-stamped cache below exists to prevent, just moved from the TTL to
// the handler budget. A scan is shared infrastructure, not per-caller work: the
// caller that started it may be gone, but the NEXT one wants its result, so it
// runs to completion under its own bound and stamps the cache. The single-flight
// locks keep at most one such scan per view in flight, and the daemon's base
// context stops them all at shutdown. A var so tests can shrink it.
var scanBudget = 2 * time.Minute

// viewCounts is one cached pair of per-view O(view) counts plus its completion
// stamp, so the TTL can be checked on the next read: the StateConflict baselines
// (see scanViewCounts) and the durable keep-both conflict records.
//
// The two counts share one record, one flight lock and one stamp because they are
// consumed together - every ipc.ViewStatus carries both - and both are full walks
// of a per-view bucket. Counting the conflict records uncached (the earlier
// shape: a bare context-less store count per view on every call) left the third
// O(view) walk of `status` with no cache, no single-flight and no scan budget at
// all, so on a mass-divergence store it blew the handler budget on every call and
// never stamped anything to recover with.
type viewCounts struct {
	unresolved int
	conflicts  int
	at         time.Time
}

// scanFailTTL bounds how long a FAILED cache-filling scan is remembered, so the failure is
// not retried by every call. It deliberately exceeds scanBudget: a scan
// that deterministically outruns its budget cannot possibly succeed on a retry started sooner
// than the budget itself, and every such retry costs another full-length bbolt read
// transaction against the sync data plane - the "one doomed scan per fresh call" cadence a CLI
// poll loop turns into a near-continuous one. It also exceeds the IPC handler budget, for the
// same reason the positive TTLs do: a shorter memory would already have expired by the time
// any retry can reach it. A var so tests can shrink it.
var scanFailTTL = 3 * time.Minute

// scanFailure is one remembered cache-filling scan failure: the error to serve to callers
// arriving within scanFailTTL, and when it was recorded. A zero value is "no failure".
type scanFailure struct {
	err error
	at  time.Time
}

// scanOutcome classifies why a cache-filling scan ended and what to say about it, so the
// diagnosis is logged at the SCAN site - the only place that knows the difference. The three
// causes are operationally distinct and used to be indistinguishable (the scan site returned a
// bare error and logged nothing, and the handler that would have surfaced it had been abandoned
// minutes earlier, so its reply went into ipc's buffered done channel and was dropped):
//
//   - the scan outran scanBudget: the tree is genuinely too large for the budget, which is an
//     operator-actionable fact and names the knob;
//   - the daemon is shutting down: expected, and not a defect;
//   - the store itself failed: a real error, distinct from either timeout.
func (d *Daemon) logScanOutcome(what string, attrs []any, scanCtx context.Context, err error) {
	base := d.scanGate()
	switch {
	case base.Err() != nil:
		d.log.Info(what+" scan cancelled by daemon shutdown", append(attrs, "err", err)...)
	case scanCtx.Err() != nil:
		d.log.Warn(what+" scan exceeded its scan budget and cached nothing; `status`/`conflicts` will keep "+
			"failing for this store until the scan fits (the budget is scanBudget)",
			append(attrs, "scan_budget", scanBudget, "retry_after", scanFailTTL, "err", err)...)
	default:
		d.log.Warn(what+" scan failed in the store layer", append(attrs, "err", err)...)
	}
}

// conflictsSnapshot is one cached ConflictReport plus its completion stamp, so
// buildConflicts can check the TTL on the next read.
type conflictsSnapshot struct {
	rep ipc.ConflictReport
	at  time.Time
	ok  bool
}

// buildStatus assembles a read-only ipc.StatusReport from the local state store
// and the live connection pool: the paired server address, the per-view sync
// state (last sync id, cold-start flag, outstanding keep-both conflict count),
// the durable pending-op count, and the pool's paused/worker state. It never
// touches the network.
//
// ctx is the IPC handler's budget: the per-view scans are O(tree), so a call whose
// client is already gone starts no refill at all. Once a refill pass HAS started it
// runs to completion (see refillViewCounts) - stopping at the next view boundary
// left the cache unable to converge on a multi-view store.
//
// The per-view counts are the only O(tree) part of the report, and they DEGRADE rather
// than fail it: a view whose count scan cannot be completed is served with its last known
// numbers (or zeros) and ViewStatus.CountsStale set. Failing the whole verb on them lost
// Paused, DiskFull, Stuck, PendingOps, DeadLettered, Server and Connection too - every
// field that needs no scan at all - and lost them PERMANENTLY on a store whose per-view
// walk cannot fit scanBudget, since the next call re-runs the same doomed scan. Observability
// that survives the tree it is meant to describe is the whole point of the cache;
// a stale count beside a live Stuck counter beats neither.
func (d *Daemon) buildStatus(ctx context.Context) (ipc.StatusReport, error) {
	views, err := d.store.ListViews()
	if err != nil {
		return ipc.StatusReport{}, fmt.Errorf("daemon: list views: %w", err)
	}

	counts := d.refillViewCounts(ctx, views)

	vs := make([]ipc.ViewStatus, 0, len(views))
	for _, lv := range views {
		c := counts[lv.ViewID]
		vs = append(vs, ipc.ViewStatus{
			ID:              lv.ViewID,
			Name:            lv.Name,
			LastSyncID:      lv.LastSeenSyncID,
			InitialSyncDone: lv.InitialSyncDone,
			Conflicts:       c.counts.conflicts,
			Unresolved:      c.counts.unresolved,
			CountsStale:     c.stale,
			LocalPath:       filepath.Join(d.cfg.SyncRoot, d.cfg.Views.Dir(lv.Name)),
			Excluded:        excludedPaths(d.cfg.Views, lv.ViewID),
			Rescanning:      lv.Rescan,
		})
	}

	pending, err := d.store.PendingLen()
	if err != nil {
		return ipc.StatusReport{}, fmt.Errorf("daemon: pending length: %w", err)
	}

	deadLettered, err := d.store.DeadLetterLen()
	if err != nil {
		return ipc.StatusReport{}, fmt.Errorf("daemon: dead-letter length: %w", err)
	}

	d.mu.Lock()
	pool := d.pool
	eng := d.engine
	wantPaused := d.wantPaused
	d.mu.Unlock()

	// Before the pool is up - the whole bring-up backoff window when the server
	// is unreachable at boot, or under an injected run seam that never set it -
	// the daemon is not connected: report Connecting with the configured worker
	// count so `status` never claims "connected" while the pool is still dialing.
	conn := ipc.ConnState{Connecting: true, NumWorkers: d.numWorkers}
	if pool != nil {
		conn = ipc.ConnState{
			Paused:      pool.Paused(),
			PauseReason: pauseReasonToken(pool.PauseReason()),
			// A mid-run outage parks the engine in the pool's reconnect loop:
			// surface it so `status` renders "reconnecting..."
			// instead of a false "connected" for the whole downtime.
			Reconnecting: pool.Reconnecting(),
			NumWorkers:   pool.NumWorkers(),
		}
	}

	// The user-paused flag comes from the live engine once it is up; before that
	// (bring-up backoff) the operator's recorded desire is the truth, so a pause
	// issued while the server is unreachable still reads back as paused. The
	// disk-full latch and the stuck-op counter have no meaning before the engine
	// is up.
	paused := wantPaused
	diskFull := false
	stuck := 0
	var stuckOps []ipc.StuckEntry
	var appliedOps, transferred int64
	var movingFor time.Duration
	var lastContact time.Time
	var listing *ipc.ListingProgress
	var recent *ipc.RecentRate
	var lastApplied time.Time
	if eng != nil {
		paused = eng.Paused()
		diskFull = eng.DiskFull()
		stuck = eng.StuckOps()
		stuckOps = d.stuckEntries(eng, vs)
		appliedOps, transferred, movingFor = eng.Progress()
		lastContact = eng.LastContact()
		listing = listingProgress(eng, vs)
		if re, ok := eng.(recentEngine); ok {
			ops, bytes, known, last := re.Recent()
			if known {
				recent = &ipc.RecentRate{OpsPerSec: ops, BytesPerSec: bytes}
			}
			lastApplied = last
		}
	}

	return ipc.StatusReport{
		Server:           serverAddr(d.cfg),
		Views:            vs,
		PendingOps:       pending,
		DeadLettered:     deadLettered,
		DiskFull:         diskFull,
		Stuck:            stuck,
		StuckOps:         stuckOps,
		AppliedOps:       appliedOps,
		TransferredBytes: transferred,
		MovingFor:        movingFor,
		LastContact:      lastContact,
		Listing:          listing,
		Recent:           recent,
		LastApplied:      lastApplied,
		ExcludedNames:    len(d.cfg.Views.ExcludedNames),
		Connection:       conn,
		Paused:           paused,
	}, nil
}

// refillViewCounts returns the per-view counts for every view in views, scanning
// (and caching) the ones whose cache entry is missing or stale.
//
// ctx (the IPC handler's budget) gates ENTERING the pass: a call whose client is
// already gone starts no new O(tree) work against the sync data plane. But once the
// pass has scanned its FIRST view it is committed and runs to completion under the
// daemon's own lifetime (scanGate) instead of the handler's clock, because stopping
// at the next view boundary made the cache unable to converge.
// Abandoning there advanced exactly ONE view's stamp per call, so the stamps landed
// sequentially (S, 2S, … N·S) while `status` needs all N fresh at once: with a
// per-view scan slower than viewCountsCacheTTL, the earliest stamps expired before
// the last view was ever reached and every `status` call failed forever - on
// exactly the multi-view, multi-million-baseline stores the cache exists for. The
// committed pass instead fills every stale view and stamps them as ONE cohort, so
// the next call is served entirely from cache.
//
// The cohort stamp is the pass's COMPLETION time for the same reason a single scan
// is stamped at its completion: a stamp older than the work
// that produced it is born expired. It makes the earliest-scanned view's count lag
// reality by up to the pass duration on top of the TTL, exactly as the whole-report
// conflictsSnapshot stamp already does - the counts are observability signals, and
// a converging stale count beats a fresh one no operator can ever read. Views
// served from cache keep their own stamp; nothing partial or unscanned is stamped,
// and a failed scan still lets the views this pass DID compute carry the cohort
// stamp (only a budget-class failure is remembered - see stickyScanFailure).
//
// A view whose counts cannot be produced does not abort the pass and does not fail the
// report: it is served stale (its last known numbers, or zeros when it has never been
// counted) and marked so, and the remaining views are still refilled. One view too large
// for scanBudget used to cost the operator the entire `status` verb, forever.
func (d *Daemon) refillViewCounts(ctx context.Context, views []store.ListView) map[uint64]servedCounts {
	out := make(map[uint64]servedCounts, len(views))
	scanned := make(map[uint64]viewCounts)
	failed := make(map[uint64]error)
	defer d.stampCohort(scanned)
	defer d.stampFailureCohort(failed)

	// gate starts as the caller's budget and becomes the daemon's lifetime the
	// moment this pass has invested a scan of its own - whether that scan SUCCEEDED or
	// not: the investment, not the outcome, is what commits the pass.
	gate := ctx
	for _, lv := range views {
		a := d.viewCountsFor(gate, lv.ViewID)
		if a.scanned {
			gate = d.scanGate()
		}
		if a.sticky {
			failed[lv.ViewID] = a.err
		}
		if a.err != nil {
			last, _ := d.lastKnownViewCounts(lv.ViewID)
			out[lv.ViewID] = servedCounts{counts: last, stale: true}
			continue
		}
		if a.fresh {
			scanned[lv.ViewID] = a.counts
		}
		out[lv.ViewID] = servedCounts{counts: a.counts}
	}
	return out
}

// servedCounts is one view's counts as buildStatus renders them: the numbers plus whether
// they are a stale stand-in because this call could not produce fresh ones.
type servedCounts struct {
	counts viewCounts
	stale  bool
}

// countsAttempt is what viewCountsFor produces for one view.
type countsAttempt struct {
	counts viewCounts
	// scanned reports that this call ran an actual store scan, successful or not, so the
	// refill pass is committed to the daemon's own lifetime from here on.
	scanned bool
	// fresh reports that counts came from a scan this call completed, so it belongs in the
	// pass's cohort stamp (a cache hit does not).
	fresh bool
	// sticky reports that this call's scan failed in the budget class and was recorded in
	// the negative cache, so the pass re-stamps it in its failure cohort.
	sticky bool
	err    error
}

// stampCohort re-stamps every view a refill pass scanned with one completion time,
// so their TTL windows expire together instead of in the staggered order the scans
// finished in (see refillViewCounts).
func (d *Daemon) stampCohort(scanned map[uint64]viewCounts) {
	if len(scanned) == 0 {
		return
	}
	at := d.now()
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	for id, c := range scanned {
		c.at = at
		d.countsCache[id] = c
	}
}

// stampFailureCohort re-stamps every budget-class scan failure a refill pass recorded
// with one completion time - the mirror of stampCohort, for the same reason.
//
// A refill pass over N views can run for N × scanBudget, while scanFailTTL is a single
// multiple of ONE view's budget. Stamped where they happen, the early views' failures
// expire before the pass that produced them has even finished, so the very next call
// restarts their doomed walks: the near-continuous doomed scanning (and its
// full-length bbolt read transactions against the sync data plane) that the negative
// cache exists to stop, back again on exactly the multi-view stores that need it. Like
// the positive cohort stamp, the pass's completion time is the honest one: a memory
// older than the work that produced it is born expired.
//
// A view that SUCCEEDED later in the pass has had its record deleted; it is not
// resurrected here.
func (d *Daemon) stampFailureCohort(failed map[uint64]error) {
	if len(failed) == 0 {
		return
	}
	at := d.now()
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	for id, err := range failed {
		if _, ok := d.countsFail[id]; !ok {
			continue
		}
		d.countsFail[id] = scanFailure{err: err, at: at}
	}
}

// viewCountsFor serves one view's counts from the TTL cache or scans them, reporting
// whether the returned value came from a fresh scan (the pass's cohort stamp covers
// exactly those).
//
// Misses are single-flighted per view behind countsFlight: the first caller runs the
// scan while concurrent misses wait on the per-view slot and then serve the freshly
// stamped result, so a polling client can never stack duplicate O(view) scans
// against the sync data plane. That wait is bounded by ctx (acquireFlight): a scan can
// run for scanBudget, far longer than the handler budget, so a waiter that expires
// meanwhile reports its own deadline and lets buildStatus degrade to stale counts
// rather than holding the abandoned handler for the whole scan. Only a budget-class
// failure is remembered (stickyScanFailure); a store-layer error is retried by the
// next call.
func (d *Daemon) viewCountsFor(ctx context.Context, viewID uint64) countsAttempt {
	if c, ok := d.cachedViewCounts(viewID); ok {
		return countsAttempt{counts: c}
	}
	// A scan that just failed is remembered for scanFailTTL: serve that error instead of
	// starting the same doomed walk again. The diagnosis was logged at
	// the scan site, so this arm stays silent.
	if err, ok := d.recentCountsFailure(viewID); ok {
		return countsAttempt{err: err}
	}
	if err := ctx.Err(); err != nil {
		return countsAttempt{err: err}
	}

	release, err := acquireFlight(ctx, d.countsFlight(viewID))
	if err != nil {
		return countsAttempt{err: err}
	}
	defer release()
	// Re-check under the flight lock: a concurrent miss that held the lock first
	// has just scanned and stamped fresh counts.
	if c, ok := d.cachedViewCounts(viewID); ok {
		return countsAttempt{counts: c}
	}
	// acquireFlight can hand over the slot in the same instant the budget expires, so
	// re-apply the gate here: a caller that queued behind another scan and expired
	// meanwhile must not start a fresh O(tree) walk for a client that is already gone.
	// Without it, a view whose scan keeps FAILING in the store layer (not remembered)
	// lets every abandoned handler take the slot in turn and serialise another full
	// scan. The cache re-check above deliberately comes first, so an expired caller
	// still serves the result the scan it waited for produced rather than reporting its
	// own deadline.
	if err := ctx.Err(); err != nil {
		return countsAttempt{err: err}
	}
	// The flight wait can outlast a concurrent scan that FAILED; its record is as good
	// a reason not to re-run as a successful one is to serve the cache.
	if err, ok := d.recentCountsFailure(viewID); ok {
		return countsAttempt{err: err}
	}

	c, sticky, err := d.scanViewCounts(viewID)
	if err != nil {
		if sticky {
			d.recordCountsFailure(viewID, err)
		}
		return countsAttempt{scanned: true, sticky: sticky, err: err}
	}
	c.at = d.now()
	d.countsMu.Lock()
	d.countsCache[viewID] = c
	delete(d.countsFail, viewID) // a success supersedes any remembered failure
	d.countsMu.Unlock()
	return countsAttempt{counts: c, scanned: true, fresh: true}
}

// lastKnownViewCounts serves a view's cached counts IGNORING the TTL - the stand-in
// buildStatus renders when a refill could not produce fresh ones. It is deliberately
// separate from cachedViewCounts: nothing may skip a scan on the strength of an expired
// entry, but an expired entry still beats reporting nothing at all once the scan has
// already failed.
func (d *Daemon) lastKnownViewCounts(viewID uint64) (viewCounts, bool) {
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	c, ok := d.countsCache[viewID]
	return c, ok
}

// scanViewCounts walks one view's conflict records and baselines and returns the
// two counts, unstamped.
//
// The unresolved count covers the baselines stuck in StateConflict: divergences the
// engine recorded (an outbound rename rejected with a stale base, a push to a
// since-downgraded read-only view, a child under a conflicted parent, or a
// dead-lettered resolve-conflict op) but could not keep-both-resolve, so they carry
// no preserved copy and never appear in the durable conflicts list. Surfacing the
// count keeps these silent divergences visible in `status`; it self-clears once the
// engine advances the baseline back to StateSynced. Both walks stream, so a large
// view is never materialised whole, and neither touches the network.
//
// Both run under scanBudget (not the caller's handler budget) so a scan the daemon
// entered always finishes and stamps the cache instead of being restarted and
// re-aborted by every later call (see scanBudget); the daemon's base context still
// stops them at shutdown. The conflict records are counted with an inline
// ScanConflicts walk rather than a context-less store counter so this scan budget
// and cancellation reach them too.
// The second return value reports whether a failure should be REMEMBERED for scanFailTTL
// (see stickyScanFailure): only a scan that outran its budget qualifies.
func (d *Daemon) scanViewCounts(viewID uint64) (viewCounts, bool, error) {
	if d.viewScanHook != nil {
		d.viewScanHook(viewID)
	}
	scanCtx, cancel := d.scanContext()
	defer cancel()

	var c viewCounts
	err := d.store.ScanConflicts(viewID, func(store.Conflict) error {
		if err := scanCtx.Err(); err != nil {
			return err
		}
		c.conflicts++
		return nil
	})
	if err != nil {
		d.logScanOutcome("daemon: conflict-count", []any{"view", viewID, "counted", c.conflicts}, scanCtx, err)
		return viewCounts{}, d.stickyScanFailure(scanCtx), fmt.Errorf("daemon: count conflicts for view %d: %w", viewID, err)
	}

	skipped, err := d.store.ScanBaselinesForView(viewID, func(pb store.PathBaseline) error {
		if err := scanCtx.Err(); err != nil {
			return err
		}
		if pb.State == store.StateConflict {
			c.unresolved++
		}
		return nil
	})
	if err != nil {
		d.logScanOutcome("daemon: unresolved-count", []any{"view", viewID, "counted", c.unresolved}, scanCtx, err)
		return viewCounts{}, d.stickyScanFailure(scanCtx), fmt.Errorf("daemon: count unresolved for view %d: %w", viewID, err)
	}
	if len(skipped) > 0 {
		// A corrupt record is skipped rather than failing the whole count; its
		// conflict state is unknowable, so the count may run low until the
		// engine rewrites the record. The cache bounds this Warn to once per TTL.
		d.log.Warn("daemon: skipping baseline records that no longer decode while counting unresolved conflicts",
			"view", viewID, "count", len(skipped))
	}
	return c, false, nil
}

// stickyScanFailure reports whether a failed cache-filling scan may be remembered for
// scanFailTTL. Only ONE of logScanOutcome's three classes qualifies: the scan that outran
// scanBudget, which is the class scanFailTTL's whole rationale is about - it "cannot possibly
// succeed on a retry started sooner than the budget itself".
//
// A store-layer error is a different animal: a one-off bbolt read failure says nothing about
// the next read, and remembering it made a transient blip suppress the counts (and, before
// buildStatus degraded, the whole `status` verb) for three minutes with no way to force a
// retry. A shutdown cancellation is not worth remembering either - the daemon is going away,
// and a restart clears the cache regardless.
func (d *Daemon) stickyScanFailure(scanCtx context.Context) bool {
	return d.scanGate().Err() == nil && scanCtx.Err() != nil
}

// cachedViewCounts serves a view's counts from the TTL cache, reporting whether a
// still-fresh entry existed.
func (d *Daemon) cachedViewCounts(viewID uint64) (viewCounts, bool) {
	now := d.now()
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	c, ok := d.countsCache[viewID]
	if !ok || now.Sub(c.at) >= viewCountsCacheTTL {
		return viewCounts{}, false
	}
	return c, true
}

// recentCountsFailure reports the remembered failure of a view's count scan while it is younger
// than scanFailTTL, so a doomed scan is not restarted by every call.
func (d *Daemon) recentCountsFailure(viewID uint64) (error, bool) {
	now := d.now()
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	f, ok := d.countsFail[viewID]
	if !ok || f.err == nil || now.Sub(f.at) >= scanFailTTL {
		return nil, false
	}
	return f.err, true
}

// recordCountsFailure remembers a failed per-view count scan for scanFailTTL.
func (d *Daemon) recordCountsFailure(viewID uint64, err error) {
	at := d.now()
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	d.countsFail[viewID] = scanFailure{err: err, at: at}
}

// recentConflictsFailure is recentCountsFailure for the whole-report conflict scan.
func (d *Daemon) recentConflictsFailure() (error, bool) {
	now := d.now()
	d.conflictsMu.Lock()
	defer d.conflictsMu.Unlock()
	if d.conflictsFail.err == nil || now.Sub(d.conflictsFail.at) >= scanFailTTL {
		return nil, false
	}
	return d.conflictsFail.err, true
}

// recordConflictsFailure remembers a failed conflict-report scan for scanFailTTL.
func (d *Daemon) recordConflictsFailure(err error) {
	at := d.now()
	d.conflictsMu.Lock()
	defer d.conflictsMu.Unlock()
	d.conflictsFail = scanFailure{err: err, at: at}
}

// countsFlight returns the per-view single-flight slot serialising the count scans,
// creating it on first use. The map only ever grows, bounded by the number of views
// the account holds.
//
// It is a cap-1 channel rather than a mutex so the wait for it can be abandoned when
// the caller's budget expires - see acquireFlight.
func (d *Daemon) countsFlight(viewID uint64) chan struct{} {
	d.countsMu.Lock()
	defer d.countsMu.Unlock()
	f, ok := d.countsFlights[viewID]
	if !ok {
		f = make(chan struct{}, 1)
		d.countsFlights[viewID] = f
	}
	return f
}

// acquireFlight takes a single-flight slot, giving up the moment ctx expires, and
// returns the release function.
//
// The abandonable wait is the point. A sync.Mutex here blocked for the WHOLE scan the
// holder was running (up to scanBudget, 2 minutes), which is far past the IPC handler
// budget (8s): the IPC layer abandoned the handler and the operator got the generic
// "handler did not finish" - never the stale-counts degrade buildStatus exists to
// serve. That undid the cohort-stamp guarantee on exactly the stores it was written for, where
// a refill pass is in flight for most of the time. An expired waiter now falls through
// to lastKnownViewCounts + CountsStale (or, for the conflict report, to a prompt error)
// instead of holding the handler.
func acquireFlight(ctx context.Context, flight chan struct{}) (release func(), err error) {
	select {
	case flight <- struct{}{}:
		return func() { <-flight }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// conflictCandidate is one conflict entry plus the global order it was streamed
// in, so buildConflicts's bounded newest-N selection can break DetectedAt ties
// exactly like the former stable descending sort did: an equal DetectedAt keeps
// the ascending view/path (collection) order.
type conflictCandidate struct {
	entry ipc.ConflictEntry
	seq   int
}

// moreKeepable reports whether c ranks ahead of o in a truncated newest-first
// report: a higher DetectedAt, or an equal DetectedAt streamed earlier (lower
// seq). seq is unique per stream, so this is a strict total order.
func (c conflictCandidate) moreKeepable(o conflictCandidate) bool {
	if c.entry.DetectedAt != o.entry.DetectedAt {
		return c.entry.DetectedAt > o.entry.DetectedAt
	}
	return c.seq < o.seq
}

// evictHeap is a min-heap over conflictCandidate whose top is always the LEAST
// keepable candidate - the record a truncated report drops first. buildConflicts
// keeps a bounded evictHeap of at most maxConflictEntries candidates, so it holds
// O(cap) records regardless of how many conflicts the bucket contains, replacing
// the former full-bucket materialisation.
type evictHeap []conflictCandidate

func (h evictHeap) Len() int { return len(h) }

// Less orders the least-keepable candidate toward the top: i sorts before j when
// j is more keepable than i.
func (h evictHeap) Less(i, j int) bool { return h[j].moreKeepable(h[i]) }
func (h evictHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *evictHeap) Push(x any)        { *h = append(*h, x.(conflictCandidate)) }
func (h *evictHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// buildConflicts assembles a read-only ipc.ConflictReport from the local state
// store: the outstanding keep-both conflict records across all views, joined
// with each view's name. It never touches the network.
//
// The report is bounded so it always fits the CLI's 1 MiB reply cap: at most
// maxConflictEntries records, trimmed further until the encoded report fits
// maxConflictReportBytes. When either bound bites, the newest records by
// DetectedAt are kept (ties keep the ascending view/path order) and the report
// carries Truncated plus the full Total. The records are STREAMED into a bounded
// newest-N heap so the whole conflicts bucket is never held
// in memory at once - under a mass divergence (1e5-1e6 records) the former
// materialise-then-sort held every record, chewing O(all) memory/CPU in the IPC
// handler window against the sync data plane.
//
// Successful reports are cached for conflictsCacheTTL and misses are
// single-flighted, exactly as viewCountsFor caches its per-view counts: the scan
// decodes every conflict record of every view, so without both a polling client
// (or a burst of CLI calls) stacked full-bucket scans against the sync data plane.
// The cached report is stamped at the scan's COMPLETION for the same reason the
// unresolved cache is - a scan slower than the TTL would otherwise be born
// expired and never serve a single hit. Only a budget-class failure is remembered
// (stickyScanFailure); a store-layer error is retried by the next call. The returned
// report shares its Conflicts slice with the cache, so callers must treat it as
// read-only (the IPC layer only marshals it).
func (d *Daemon) buildConflicts(ctx context.Context) (ipc.ConflictReport, error) {
	if rep, ok := d.cachedConflicts(); ok {
		return rep, nil
	}
	// A just-failed scan is remembered for scanFailTTL so a doomed whole-bucket walk is not
	// restarted by every call; the reason was logged at the scan site.
	if err, ok := d.recentConflictsFailure(); ok {
		return ipc.ConflictReport{}, err
	}
	// The handler's budget gates entering the scan, exactly as in viewCountsFor:
	// a call whose client is gone starts no new whole-bucket walk, but a walk that
	// has already begun finishes under scanBudget so the cache gets stamped.
	if err := ctx.Err(); err != nil {
		return ipc.ConflictReport{}, err
	}

	// The wait is bounded by ctx for the same reason viewCountsFor's is: the holder can
	// be scanning for scanBudget, and a caller that blocked on it past the handler
	// budget would be abandoned by the IPC layer with the generic "handler did not
	// finish" instead of reporting its own expiry.
	release, err := acquireFlight(ctx, d.conflictsFlight)
	if err != nil {
		return ipc.ConflictReport{}, err
	}
	defer release()
	// Re-check under the flight lock: a concurrent miss that held it first has just
	// scanned and stamped a fresh report.
	if rep, ok := d.cachedConflicts(); ok {
		return rep, nil
	}
	// Re-apply the handler gate after the wait, exactly as viewCountsFor does: the slot
	// can change hands in the instant the budget expires, and an expired caller must not
	// start another whole-bucket walk. The cache re-check above still wins, so a caller
	// that timed out while the scan it was waiting for succeeded serves that report
	// instead of an error.
	if err := ctx.Err(); err != nil {
		return ipc.ConflictReport{}, err
	}
	if err, ok := d.recentConflictsFailure(); ok {
		return ipc.ConflictReport{}, err
	}

	rep, sticky, err := d.scanConflicts()
	if err != nil {
		if sticky {
			d.recordConflictsFailure(err)
		}
		return ipc.ConflictReport{}, err
	}
	d.conflictsMu.Lock()
	d.conflictsCache = conflictsSnapshot{rep: rep, at: d.now(), ok: true}
	d.conflictsFail = scanFailure{} // a success supersedes any remembered failure
	d.conflictsMu.Unlock()
	return rep, nil
}

// cachedConflicts serves the conflict report from the TTL cache, reporting
// whether a still-fresh entry existed.
func (d *Daemon) cachedConflicts() (ipc.ConflictReport, bool) {
	now := d.now()
	d.conflictsMu.Lock()
	defer d.conflictsMu.Unlock()
	if !d.conflictsCache.ok || now.Sub(d.conflictsCache.at) >= conflictsCacheTTL {
		return ipc.ConflictReport{}, false
	}
	return d.conflictsCache.rep, true
}

// scanConflicts does buildConflicts's actual work: stream every view's conflict
// records through the bounded newest-N heap, then assemble the report under the
// byte budget. It runs under scanBudget (not the caller's handler budget) so the
// walk it started always finishes and stamps the cache; the daemon's base context
// still stops it at shutdown.
//
// The second return value reports whether a failure should be REMEMBERED for scanFailTTL
// (see stickyScanFailure): only a scan that outran its budget qualifies.
func (d *Daemon) scanConflicts() (ipc.ConflictReport, bool, error) {
	ctx, cancel := d.scanContext()
	defer cancel()

	views, err := d.store.ListViews()
	if err != nil {
		// Logged here like every other scan-site failure: the handler that would have
		// surfaced it may already have been abandoned, and its reply dropped with it.
		d.logScanOutcome("daemon: conflict-report", []any{"stage", "list views"}, ctx, err)
		return ipc.ConflictReport{}, d.stickyScanFailure(ctx), fmt.Errorf("daemon: list views: %w", err)
	}

	total := 0
	seq := 0
	h := &evictHeap{}
	for _, lv := range views {
		lv := lv
		err := d.store.ScanConflicts(lv.ViewID, func(c store.Conflict) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			total++
			cand := conflictCandidate{
				entry: ipc.ConflictEntry{
					ViewID:        lv.ViewID,
					ViewName:      lv.Name,
					CanonicalPath: c.CanonicalPath,
					CopyPath:      c.CopyPath,
					DetectedAt:    c.DetectedAt,
				},
				seq: seq,
			}
			seq++
			if maxConflictEntries <= 0 {
				return nil
			}
			if h.Len() < maxConflictEntries {
				heap.Push(h, cand)
			} else if cand.moreKeepable((*h)[0]) {
				// The heap is full and this record outranks the least-keepable one
				// currently held: replace the top and re-heapify.
				(*h)[0] = cand
				heap.Fix(h, 0)
			}
			return nil
		})
		if err != nil {
			d.logScanOutcome("daemon: conflict-report", []any{"view", lv.ViewID, "scanned", total}, ctx, err)
			return ipc.ConflictReport{}, d.stickyScanFailure(ctx), fmt.Errorf("daemon: scan conflicts for view %d: %w", lv.ViewID, err)
		}
	}

	// Extract the retained candidates newest-first (keep-priority descending).
	cands := []conflictCandidate(*h)
	sort.Slice(cands, func(i, j int) bool { return cands[i].moreKeepable(cands[j]) })

	entries, err := fitEntries(cands, total)
	if err != nil {
		d.logScanOutcome("daemon: conflict-report", []any{"stage", "encode", "scanned", total}, ctx, err)
		return ipc.ConflictReport{}, d.stickyScanFailure(ctx), fmt.Errorf("daemon: assemble conflict report: %w", err)
	}
	return ipc.ConflictReport{Conflicts: entries, Total: total, Truncated: len(entries) < total}, false, nil
}

// fitEntries takes the retained candidates newest-first and returns the longest
// prefix whose encoded report still fits maxConflictReportBytes.
//
// The size is accounted incrementally - each entry is marshalled exactly ONCE and
// charged its own length plus the comma that separates it from the previous one,
// on top of the envelope of an entry-less report - instead of marshalling the
// whole report, halving the list and marshalling again. That re-encoding walked
// the retained bytes over and over on the very reports big enough to need
// trimming, and it discarded half the list per round: a report one byte over
// budget lost 500 of its 1000 records. The arithmetic is exact for encoding/json's
// compact output (a struct element inside a slice encodes to the same bytes as it
// does standalone) and the entry-less envelope is measured with Truncated false,
// the longer of the two spellings, so the estimate can only overshoot - never
// claim a fit that isn't one. TestBuildConflictsByteBudgetTrims pins the encoded
// result against the budget.
func fitEntries(cands []conflictCandidate, total int) ([]ipc.ConflictEntry, error) {
	envelope, err := json.Marshal(ipc.ConflictReport{Conflicts: []ipc.ConflictEntry{}, Total: total})
	if err != nil {
		return nil, fmt.Errorf("daemon: encode conflict report: %w", err)
	}
	budget := maxConflictReportBytes - len(envelope)

	entries := make([]ipc.ConflictEntry, 0, len(cands))
	used := 0
	for _, c := range cands {
		enc, err := json.Marshal(c.entry)
		if err != nil {
			return nil, fmt.Errorf("daemon: encode conflict entry %d/%q: %w", c.entry.ViewID, c.entry.CanonicalPath, err)
		}
		cost := len(enc)
		if len(entries) > 0 {
			cost++ // the comma separating it from the previous entry
		}
		if used+cost > budget {
			break
		}
		used += cost
		entries = append(entries, c.entry)
	}
	return entries, nil
}

// handleStatus is the IPC handler for the read-only "status" method. It ignores
// its (absent) params and returns the live StatusReport, assembled under the
// handler's context, which gates ENTERING a cache-filling scan; a scan already under
// way finishes on its own budget so the next call is served from the cache.
func (d *Daemon) handleStatus(ctx context.Context, _ json.RawMessage) (any, error) {
	return d.buildStatus(ctx)
}

// buildConfig assembles a read-only ipc.ConfigReport: the current non-secret
// settings blob read live from the state store (the daemon
// holds bbolt's exclusive lock, so `config get` cannot read state.db itself
// while the daemon runs and calls this method instead). A never-written blob
// (an unpaired install) falls back to defaults, mirroring the CLI's own
// loadConfigFromStore fallback. It never touches the network.
func (d *Daemon) buildConfig() (ipc.ConfigReport, error) {
	cfg, err := d.store.GetConfig()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ipc.ConfigReport{Config: config.Default()}, nil
		}
		return ipc.ConfigReport{}, fmt.Errorf("daemon: load config: %w", err)
	}
	return ipc.ConfigReport{Config: cfg}, nil
}

// handleConfig is the IPC handler for the read-only "config" method. It ignores
// its (absent) params and returns the live ConfigReport. The report is a single
// bounded record read, so there is nothing for the handler context to cut short.
func (d *Daemon) handleConfig(_ context.Context, _ json.RawMessage) (any, error) {
	return d.buildConfig()
}

// maxDropEntries caps how many drop records buildDrops returns. The store already trims its
// ring to 1000, so this only bites a report whose paths are long enough to threaten the
// client's 1 MiB reply cap; the newest are kept, since a drop the operator has not seen yet is
// the one worth showing.
var maxDropEntries = 500

// buildDrops assembles the DropReport for the `drops` method.
//
// No cache, no flight control and no byte-budget trimming, unlike buildConflicts: the records
// live in ONE bucket the store already bounds to a fixed ring, so the read is a single bounded
// cursor walk rather than a scan across every view. The only thing borrowed is the newest-first
// ordering and the truncation flag.
func (d *Daemon) buildDrops() (ipc.DropReport, error) {
	records, err := d.store.ListDeadLetters()
	if err != nil {
		return ipc.DropReport{}, fmt.Errorf("daemon: list drops: %w", err)
	}
	names := make(map[uint64]string)
	if views, verr := d.store.ListViews(); verr == nil {
		for _, v := range views {
			names[v.ViewID] = v.Name
		}
	}
	// The store walks the ring by ascending sequence; the operator wants the newest first.
	rep := ipc.DropReport{Total: len(records)}
	for i := len(records) - 1; i >= 0; i-- {
		if len(rep.Drops) >= maxDropEntries {
			rep.Truncated = true
			break
		}
		r := records[i]
		rep.Drops = append(rep.Drops, ipc.DropEntry{
			ViewID:     r.ViewID,
			ViewName:   names[r.ViewID],
			Op:         string(r.Type),
			Direction:  string(r.Direction),
			Path:       r.Path,
			NewPath:    r.NewPath,
			Reason:     r.Reason,
			RecordedAt: r.RecordedAt,
		})
	}
	return rep, nil
}

// maxStuckEntries caps how many stuck ops the status report names. The point is to say WHICH
// files are wedged and why, not to reproduce the queue: a handful is enough to act on, and the
// count beside it already reports the scale.
var maxStuckEntries = 20

// stuckEntries asks the engine which ops are behind the Stuck count and resolves their view
// names for display. A failure is swallowed to nil: this is the detail line under a number that
// is itself only an operability hint, so it must never be the reason `status` fails.
func (d *Daemon) stuckEntries(eng engineControl, views []ipc.ViewStatus) []ipc.StuckEntry {
	ops, err := eng.StuckDetails(maxStuckEntries)
	if err != nil {
		d.log.Warn("daemon: could not list the stuck operations for status", "err", err)
		return nil
	}
	names := make(map[uint64]string, len(views))
	for _, v := range views {
		names[v.ID] = v.Name
	}
	out := make([]ipc.StuckEntry, 0, len(ops))
	for _, o := range ops {
		out = append(out, ipc.StuckEntry{
			ViewID:    o.ViewID,
			ViewName:  names[o.ViewID],
			Op:        o.Type,
			Direction: o.Direction,
			Path:      o.Path,
			NewPath:   o.NewPath,
			Failures:  o.Failures,
			Reason:    o.Reason,
		})
	}
	return out
}

// pauseReasonToken classifies the pool's pause sentinel into the stable ConnState
// token the CLI words for the user. The three terminal causes have three different
// fixes and `status` printed "re-login needed" for all of them: a pin mismatch is a
// changed server certificate the operator must verify before trusting, and a rejected
// client_version is fixed by upgrading, never by re-pairing.
//
// Order matters. transport.ErrPoolClientRejected deliberately does not wrap
// ErrPoolPaused, but the pause error wraps the underlying auth sentinel either way, so
// the specific causes are tested before the generic pause. An unrecognised cause maps
// to the empty token: the CLI then reports the pause without naming a fix, which is
// better than confidently naming the wrong one.
func pauseReasonToken(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, auth.ErrClientVersion), errors.Is(err, transport.ErrPoolClientRejected):
		return ipc.PauseClientRejected
	case errors.Is(err, transport.ErrPinMismatch):
		return ipc.PausePinMismatch
	case errors.Is(err, auth.ErrSessionVanished):
		return ipc.PauseSessionExpired
	default:
		return ""
	}
}

// listingProgress reports the engine's initial-sync walk, naming its view from the views the
// report already carries, or nil when no walk runs.
func listingProgress(eng engineControl, views []ipc.ViewStatus) *ipc.ListingProgress {
	viewID, dirs, entries, elapsed, ok := eng.Listing()
	if !ok {
		return nil
	}
	name := fmt.Sprintf("view %d", viewID)
	for _, v := range views {
		if v.ID == viewID {
			name = v.Name
			break
		}
	}
	return &ipc.ListingProgress{View: name, Dirs: dirs, Entries: entries, For: elapsed}
}

// excludedPaths lists the view's excluded paths for the status report.
func excludedPaths(v config.Views, viewID uint64) []string {
	var out []string
	for _, ex := range v.Excluded {
		if ex.ViewID == viewID {
			out = append(out, ex.Path)
		}
	}
	return out
}

// recentEngine is the engine's recent-rate report; an engine (or test double) without it simply
// leaves the rate out of status.
type recentEngine interface {
	Recent() (opsPerSec, bytesPerSec float64, ok bool, lastApplied time.Time)
}
