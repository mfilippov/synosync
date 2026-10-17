package engine

// This file pins the probe-error fix: an empty scan whose root-availability PROBE
// fails is an indeterminate observation, not a confirmed-empty one, so it must not be left
// counted in the empty-scan debounce. The unavailable branch right below it resets the
// counter for exactly this reason - an interrupted streak is not a consecutive one - but the
// probe-error branch returned early with the counter intact, so the shape
// (empty, empty, probe-error, empty) reached the threshold and propagated a whole-view
// deletion that emptiness was never confirmed for.

import (
	"context"
	"errors"
	"testing"
)

// TestLocalReconcileProbeErrorResetsEmptyDebounce drives the exact bridging streak: two
// observed-empty scans, then a scan whose viewRootUnavailable probe fails (an EACCES/EIO
// stat of the root - injected through the rootUnavailable seam, since no portable filesystem
// manipulation makes the walk succeed while the root stat fails), then a fourth empty scan.
// With the counter reset by the failed probe the fourth scan is only the first of a new
// streak, so no OpRemove is emitted; without it the fourth scan is the threshold scan and
// erases every synced baseline from the remote view.
func TestLocalReconcileProbeErrorResetsEmptyDebounce(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	putSynced(t, e, viewID, "b.txt", "fb", "file", "hb", 2)
	// The view dir exists (newScanEngine created it) and is empty, so every scan below is an
	// ordinary observed-empty scan except the injected one. Each is spaced past the debounce's
	// wall-clock floor, which is what makes them counted observations.

	for i := 1; i < emptyScanConfirmThreshold; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("empty scan %d produced %d ops, want 0 (still debouncing): %+v", i, len(ops), ops)
		}
		advance(emptyScanConfirmMinSpacing)
	}
	if got := e.emptyViewScans[viewID].count; got != emptyScanConfirmThreshold-1 {
		t.Fatalf("emptyViewScans = %d, want %d before the failed probe", got, emptyScanConfirmThreshold-1)
	}

	probeErr := errors.New("stat view root: permission denied")
	e.rootUnavailable = func(uint64) (bool, error) { return false, probeErr }
	if _, err := e.localReconcile(context.Background(), viewID); !errors.Is(err, probeErr) {
		t.Fatalf("localReconcile with a failing probe: err = %v, want the probe error propagated", err)
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("a failed root probe left the debounce at %d, want 0 (an indeterminate scan must not bridge a streak)", got)
	}
	e.rootUnavailable = e.viewRootUnavailable
	advance(emptyScanConfirmMinSpacing)

	// The next observed-empty scan starts a fresh streak; it must not be the threshold scan.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("post-probe empty scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("an empty scan following a failed probe produced %d ops, want 0 (the probe error bridged the streak into a whole-view delete): %+v", len(ops), ops)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: %d ops enqueued", n)
	}
}

// TestLocalReconcileProbeErrorStillConverges pins the other half of the contract: resetting
// the counter must not make the debounce unreachable. Once the probe recovers, a fresh run of
// consecutive observed-empty scans still confirms a genuine whole-view delete.
func TestLocalReconcileProbeErrorStillConverges(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)

	probeErr := errors.New("stat view root: I/O error")
	e.rootUnavailable = func(uint64) (bool, error) { return false, probeErr }
	if _, err := e.localReconcile(context.Background(), viewID); !errors.Is(err, probeErr) {
		t.Fatalf("localReconcile with a failing probe: err = %v, want the probe error", err)
	}
	e.rootUnavailable = e.viewRootUnavailable

	for i := 1; i < emptyScanConfirmThreshold; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("empty scan %d produced %d ops, want 0: %+v", i, len(ops), ops)
		}
		advance(emptyScanConfirmMinSpacing)
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("threshold scan: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("threshold scan produced %d ops, want 1 remove (a genuine empty-out must still converge): %+v", len(ops), ops)
	}
}
