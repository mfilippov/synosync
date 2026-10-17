package engine

// Tests for the wall-clock floor: two corroboration-by-repetition mechanisms counted
// PASSES with nothing separating those passes in wall clock, so the repetition they rest on could
// be satisfied inside a single window of the very condition they exist to tell apart.
//
//   - The node-level absence streak (absencestreak.go) counted one confirmation per drain pass -
//     but loop.go runs a leading drain per cycle AND a trailing drain for every view syncView
//     changed, so two passes land seconds apart, both answered by one multi-second window of DSM
//     overload conflating 28672 ("no such path") with a server-side timeout. The conflated verdict
//     acks a remove the server never performed and drops the baseline: the user's delete is lost
//     and the live server node falls permanently outside the replica.
//   - The empty-scan debounce (localreconcile.go) counted SCANS, and a watcher wake resets the
//     scan throttle outright, so an unmount's fsnotify burst delivers the whole threshold inside
//     the seconds the mount takes to disappear - and every synced baseline of the view is pushed
//     as a server-side delete.
//
// Both now require wall clock between counted observations, the doctrine servererr.go's
// unmappedServerErrMinAge already states: the drain count alone is not a duration.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundRemoveAbsenceNeedsWallClockBetweenProbes is the motivating scenario at the drain
// level: an overloaded DSM answers 28672 to the destructive turn, to the parent listing's
// postcondition and to the corroborating probe, and the SECOND drain of the same loop iteration
// (the trailing drain syncView runs for a changed view) arrives a second later, still inside that
// window. Pre-fix its probe was pass number two and completed the corroboration, acking a remove
// the server never performed. The op must now stay queued with its baseline intact for as long as
// the probes keep landing inside the floor - and converge on the first one past it, so a genuine
// delete is delayed, never abandoned.
func TestOutboundRemoveAbsenceNeedsWallClockBetweenProbes(t *testing.T) {
	c := &pushClient{
		removeErr:          actions.ErrPathError,
		listResult:         map[string][]actions.Node{"docs": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"docs/f.txt": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	advance := stubClock(e)
	seedRemoveOpAt(t, st, "docs/f.txt")

	// Several drains, all inside one overload window: the leading drain of a cycle, the trailing
	// drain of the same cycle, the next cycle's - pass numbers pile up, seconds do not.
	for i := 0; i < 5; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			// A drain that reports no error is acceptable only when the op was not attempted at
			// all: the repeated unproved-absence code arms the geometric pause (opbackoff.go), and
			// a deferred attempt is deliberately not a batch error. What must never happen is the
			// op converging on observations one overload window produced - which the queue and
			// baseline assertions below pin either way.
			if _, paced := e.opRetryPending(1); !paced {
				t.Fatalf("drain %d: want the remove left transient (or paced); %d probes inside one overload window are not %d independent observations",
					i, i+1, absentNodeConfirmations)
			}
		}
		if n, _ := st.PendingLen(); n != 1 {
			t.Fatalf("remove op left queued = %d after drain %d, want 1 (a pass count is not a duration)", n, i)
		}
		if _, err := st.GetBaseline(7, "docs/f.txt"); err != nil {
			t.Fatalf("baseline dropped after drain %d on probes a single overload window answered: %v", i, err)
		}
		advance(time.Second)
	}

	// The overload window is over; the sentinel repeating a floor later is evidence no timeout
	// produces, so the delete converges.
	advance(absenceStreakMinAge)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drain past the floor: %v; a genuine absence must converge, not stay inconclusive forever", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued = %d after probes spanning the floor, want acked", n)
	}
	if _, err := st.GetBaseline(7, "docs/f.txt"); err == nil {
		t.Error("baseline survived a remove whose absence was corroborated across the floor")
	}
}

// TestEmptyScanDebounceNeedsWallClockBetweenScans is the mirror: the unmount burst. Three empty
// scans a few seconds apart - what a wake-driven scan does while a mount is going away, since the
// wake resets the LocalScanInterval throttle - used to reach EmptyScanThreshold and emit an
// OpRemove for every synced baseline of the view. The suppression must hold for as long as the
// scans keep landing inside the floor, and the genuine empty-out must still converge once the
// emptiness has persisted across it.
func TestEmptyScanDebounceNeedsWallClockBetweenScans(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	putSynced(t, e, viewID, "b.txt", "fb", "file", "hb", 2)
	// The view dir exists and is empty: the shape an unmounted mount point presents.

	// Far more scans than the threshold, all inside one floor: the debounce must not advance past
	// its first counted observation, and nothing may be enqueued.
	for i := 0; i < e.limits.EmptyScanThreshold+3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("burst scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("burst scan %d produced %d ops, want 0: a burst of scans inside %v is not a persisting empty state: %+v",
				i, len(ops), emptyScanConfirmMinSpacing, ops)
		}
		advance(2 * time.Second)
	}
	if got := e.emptyViewScans[viewID].count; got != 1 {
		t.Errorf("debounce = %d after a scan burst, want 1 (only the first scan of the window counts)", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Fatalf("queue not empty: an unmount's fsnotify burst propagated a whole-view deletion (%d ops)", n)
	}

	// The emptiness persists across the floor: now each scan counts, and the threshold scan
	// propagates the deletion exactly as before.
	var lastOps []store.Op
	for i := e.emptyViewScans[viewID].count; i < e.limits.EmptyScanThreshold; i++ {
		advance(emptyScanConfirmMinSpacing)
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("spaced scan %d: %v", i, err)
		}
		lastOps = ops
	}
	if len(lastOps) != 2 {
		t.Fatalf("spaced scans produced %d ops, want 2 removes (a genuine empty-out must still converge): %+v",
			len(lastOps), lastOps)
	}
	for _, want := range []string{"a.txt", "b.txt"} {
		op, ok := findOp(lastOps, want)
		if !ok || op.Type != store.OpRemove {
			t.Errorf("missing OpRemove for %q: %+v", want, lastOps)
		}
	}
}

// TestEmptyScanFloorKeepsTheRunIntact pins the shape of the suppression: a scan inside the floor
// does NOT reset the debounce, unlike the unavailable-root and probe-error branches. The emptiness
// did repeat - it is just not independent evidence yet - so a view emptied for minutes converges on
// the scans that ARE spaced, instead of being restarted by every wake-driven scan in between (which
// would make a busy sync root never converge at all).
func TestEmptyScanFloorKeepsTheRunIntact(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)

	// One counted scan, then a burst inside the floor, then a scan past it: the burst must neither
	// advance the run nor reset it.
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("first empty scan: %v", err)
	}
	for i := 0; i < 4; i++ {
		advance(time.Second)
		if _, err := e.localReconcile(context.Background(), viewID); err != nil {
			t.Fatalf("burst scan %d: %v", i, err)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 1 {
		t.Fatalf("debounce = %d after the burst, want 1 (neither advanced nor reset)", got)
	}
	advance(emptyScanConfirmMinSpacing)
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("spaced scan: %v", err)
	}
	if got := e.emptyViewScans[viewID].count; got != 2 {
		t.Errorf("debounce = %d after a spaced scan, want 2 (the run continued rather than restarting)", got)
	}

	// A recovery still resets it outright: the emptiness stopped repeating at all.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("ha"), 0o644); err != nil {
		t.Fatalf("restore a.txt: %v", err)
	}
	advance(emptyScanConfirmMinSpacing)
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("recovery scan: %v", err)
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("debounce = %d after a non-empty scan, want 0 (reset)", got)
	}
}
