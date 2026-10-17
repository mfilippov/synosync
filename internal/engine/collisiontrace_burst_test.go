package engine

// Tests for the BURST bound on fold-key collision traces (collisionTraceBurstCap).
//
// The once-per-key memo bounds how often a persisting collision is re-recorded; it says nothing
// about how many DISTINCT keys one pass may write. A tree carrying more colliding keys than the
// durable ring holds records - an NFD tree merged onto an NFC one - therefore retired the whole
// ring in a single scan, evicting every genuine op-drop trace. That is exactly the harm the memo
// exists to prevent, reached in one go rather than one record at a time.
//
// These drive noteCollisionTraces directly with a synthetic collision set: the burst is a property
// of the recording rule, not of the host filesystem, and a fixture that needed hundreds of real
// NFD/NFC pairs on disk would skip on every normalizing host.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// syntheticCollisions builds n distinct fold keys, each with the two colliding spellings
// scanKeyCollisions would report (it emits only keys with ≥2 entries).
func syntheticCollisions(n int) map[string][]string {
	m := make(map[string][]string, n)
	for i := range n {
		p := fmt.Sprintf("dir/file%04d.txt", i)
		m[normpath.Key(p)] = []string{p, p}
	}
	return m
}

// TestFoldCollisionBurstIsCappedAndSummarised is the red test: a pass over a view with far more
// collisions than the cap must write the capped sample plus ONE overflow record, never one record
// per key. Before the cap this pass wrote 200 records; the durable ring holds 1000 in total and
// every other record in it is a genuine dropped op, so a handful of such views retired the lot.
func TestFoldCollisionBurstIsCappedAndSummarised(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	const keys = collisionTraceBurstCap + 168

	e.noteCollisionTraces(viewID, syntheticCollisions(keys), true)

	dls := collisionTraces(t, e.store)
	want := collisionTraceBurstCap + 1
	if len(dls) != want {
		t.Fatalf("got %d traces for %d colliding keys, want %d (the capped sample plus one overflow record)",
			len(dls), keys, want)
	}
	var overflow int
	for _, dl := range dls {
		if dl.Type != store.OpFoldCollision {
			t.Errorf("trace type = %s, want the fold-collision type", dl.Type)
		}
		if dl.Path == "" {
			overflow++
			if !strings.Contains(dl.Reason, fmt.Sprint(keys-collisionTraceBurstCap)) {
				t.Errorf("overflow reason = %q, want it to count the %d untraced keys",
					dl.Reason, keys-collisionTraceBurstCap)
			}
		}
	}
	if overflow != 1 {
		t.Errorf("overflow records = %d, want exactly 1", overflow)
	}
}

// TestFoldCollisionBurstDoesNotRepeatAcrossPasses pins that the keys the cap left untraced are
// NOTED, not merely skipped. Leaving them un-noted would put the same burst back on the very next
// 5s scan - the churn the cap exists to stop, arrived at from the other side.
func TestFoldCollisionBurstDoesNotRepeatAcrossPasses(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	collisions := syntheticCollisions(collisionTraceBurstCap + 40)

	e.noteCollisionTraces(viewID, collisions, true)
	first := len(collisionTraces(t, e.store))
	for range 4 {
		e.noteCollisionTraces(viewID, collisions, true)
	}

	if got := len(collisionTraces(t, e.store)); got != first {
		t.Fatalf("traces = %d after five passes, want %d: an over-cap collision set must not re-record every pass", got, first)
	}
}

// TestFoldCollisionOverflowWriteFailureIsNotFatal pins that a failed overflow write behaves like
// every other dead-letter write in this file: logged and dropped, never propagated into the
// reconcile pass. A trace that cannot be written must not stop the pass that keeps the colliding
// files protected - which is the only thing standing between them and the merge's silent last-wins.
func TestFoldCollisionOverflowWriteFailureIsNotFatal(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	restore := recordCollisionTrace
	t.Cleanup(func() { recordCollisionTrace = restore })
	recordCollisionTrace = func(_ *Engine, dl store.DeadLetter) (uint64, error) {
		if dl.Path == "" {
			return 0, errors.New("injected overflow write failure")
		}
		return restore(e, dl)
	}

	// Must not panic and must not propagate: noteCollisionTraces returns nothing by design.
	e.noteCollisionTraces(viewID, syntheticCollisions(collisionTraceBurstCap+9), true)

	if got := len(collisionTraces(t, e.store)); got != collisionTraceBurstCap {
		t.Fatalf("traces = %d, want the %d-record sample: only the overflow write failed",
			got, collisionTraceBurstCap)
	}
}

// TestFoldCollisionBurstSampleIsStableAcrossRestarts pins why the traced sample is taken in SORTED
// key order. Map order would pick a different subset each pass, so after a restart seedCollisionNoted
// would find the ring holding traces for keys the new sample does not contain and would write fresh
// records for the ones it does - re-recording forever, which is the ring churn seedCollisionNoted was
// added to close. A restart is modelled the way the engine sees one: the same store, a fresh memo.
func TestFoldCollisionBurstSampleIsStableAcrossRestarts(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	collisions := syntheticCollisions(collisionTraceBurstCap + 40)

	e.noteCollisionTraces(viewID, collisions, true)
	before := len(collisionTraces(t, e.store))

	// Restart: drop the process-local memo, keep the durable ring.
	e.collisionNoted = map[uint64]map[string]bool{}
	e.collisionSeeded = map[uint64]bool{}
	e.noteCollisionTraces(viewID, collisions, true)

	// The sample is re-seeded verbatim from the ring, so only the overflow summary - which names
	// no key and therefore seeds nothing - is written again.
	if got := len(collisionTraces(t, e.store)); got != before+1 {
		t.Fatalf("traces = %d after the restart, want %d (the sample re-seeds; only the keyless overflow record repeats)",
			got, before+1)
	}
}

// TestFoldCollisionOverflowWriteFailureRetriesOnTheNextPass is the other half of that contract.
// "Not fatal" must not mean "silently forgotten": the keys the cap left untraced are memoised only
// once their summary is DURABLE, exactly as an individual trace notes nothing on a failed write. If
// they were noted regardless, the next pass would find every key already known, summarise nothing
// and write nothing - so one transient store error would hide the whole overflow for the life of
// the process, which is the silent-unsyncable class H6 exists to close, reached through the cap.
func TestFoldCollisionOverflowWriteFailureRetriesOnTheNextPass(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	restore := recordCollisionTrace
	t.Cleanup(func() { recordCollisionTrace = restore })
	fail := true
	recordCollisionTrace = func(_ *Engine, dl store.DeadLetter) (uint64, error) {
		if dl.Path == "" && fail {
			return 0, errors.New("injected overflow write failure")
		}
		return restore(e, dl)
	}
	collisions := syntheticCollisions(collisionTraceBurstCap + 9)

	e.noteCollisionTraces(viewID, collisions, true)
	if got := len(collisionTraces(t, e.store)); got != collisionTraceBurstCap {
		t.Fatalf("traces = %d after the failed overflow write, want the %d-record sample only",
			got, collisionTraceBurstCap)
	}

	fail = false
	e.noteCollisionTraces(viewID, collisions, true)

	dls := collisionTraces(t, e.store)
	if len(dls) != collisionTraceBurstCap+1 {
		t.Fatalf("traces = %d after the retry, want %d: an un-noted overflow must be re-derived",
			len(dls), collisionTraceBurstCap+1)
	}
	var overflow int
	for _, dl := range dls {
		if dl.Path == "" {
			overflow++
			if !strings.Contains(dl.Reason, fmt.Sprint(9)) {
				t.Errorf("overflow reason = %q, want it to count all 9 untraced keys", dl.Reason)
			}
		}
	}
	if overflow != 1 {
		t.Errorf("overflow records = %d, want exactly 1 (the retry must not double-write)", overflow)
	}
}
