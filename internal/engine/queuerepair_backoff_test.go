package engine

// Tests for the quarantine repair backoff: the quarantine repair sweep's re-arm rule assumed a
// DEFERRED candidate - a pending baseline no queued op owns but whose path overlaps one - drains
// away within a pass or two. A baseline sitting under a forever-queued op (the accepted
// forever-transient classes: an inbound unmapped errno, errParentUnknown) never does, so the
// sweep re-ran a ListViews plus a full ScanBaselines of EVERY view on every drain for the
// lifetime of the daemon, re-Warning each time, without ever examining the candidate it was
// armed for.
//
// The fix is the opWarned convention plus a backoff: an unchanging deferral is reported once per
// streak and re-examined at a geometrically decreasing rate, while every form of new evidence -
// a fresh quarantine, a changed candidate set, coverage that actually drained - runs the sweep
// at once.

import (
	"context"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/store"
)

// repairDeferWarn is the message substring the deferral Warn is asserted on.
const repairDeferWarn = "the repair sweep stays armed and re-examines them with a backoff"

// deferralFixture seeds the forever-deferred shape: an outbound mkdir of "dir" the fake server
// never accepts (so it stays queued for good) plus a stranded pending baseline at "dir/deep.txt"
// whose own queue record has bit-rotted. The first drain quarantines the rotted record and defers
// the baseline, since the queued mkdir merely OVERLAPS it.
//
// It returns the engine (driving a hand-advanced clock, because the backoff weighs wall clock),
// the fixture, its captured log and the clock's advance hook.
func newDeferralFixture(t *testing.T) (*Engine, *repairFixture, *logCapture, func(time.Duration)) {
	t.Helper()
	f := newRepairFixture(t)
	st := f.openStore(t)
	if _, err := st.Enqueue(store.Op{
		Type:      store.OpMkdir,
		Direction: store.DirOutbound,
		ViewID:    repairViewID,
		Path:      "dir",
		IsDir:     true,
	}); err != nil {
		t.Fatalf("Enqueue covering op: %v", err)
	}
	strandedSeq := seedStrandedUpload(t, st, "dir/deep.txt", "fd", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error {
		return b.Put(queueKey(strandedSeq), []byte("{corrupt"))
	})
	writeFile(t, f.viewDir, "dir/deep.txt", []byte("the edit that must still reach the server"))

	st = f.openStore(t)
	t.Cleanup(func() { _ = st.Close() })
	capture := &logCapture{}
	e := New(st, &fakeTransport{client: &fakeClient{}}, f.root, "sess", slog.New(capture))
	advance := stubClock(e)
	// The drain that quarantines the rotted record and produces the deferral.
	_ = e.drainQueue(context.Background())
	if n := capture.countWarns(repairDeferWarn); n != 1 {
		t.Fatalf("deferral Warns after the first drain = %d, want 1", n)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 1 || ops[0].Type != store.OpMkdir {
		t.Fatalf("queued ops after the first drain = %+v, want only the covering mkdir", ops)
	}
	return e, f, capture, advance
}

// TestQuarantineRepairBacksOffOnAnUnchangingDeferral is the red test: before the fix every drain
// re-streamed every baseline of every view and re-Warned, forever, for a candidate the sweep
// cannot examine until its coverage changes. Now the same deferral observed again stretches the
// backoff and stays silent.
func TestQuarantineRepairBacksOffOnAnUnchangingDeferral(t *testing.T) {
	e, _, capture, advance := newDeferralFixture(t)
	ctx := context.Background()
	// Installed after the arming drain, so the counter sees only the sweep's own baseline
	// streams.
	sweeps := countScanBaselines(e)

	for i := range 5 {
		advance(repairDeferBackoffBase / 10)
		if err := e.repairQuarantinedBaselines(ctx); err != nil {
			t.Fatalf("sweep %d inside the backoff: %v", i, err)
		}
	}
	if *sweeps != 0 {
		t.Fatalf("baseline streams inside the backoff = %d, want 0: an unresolvable deferral must not re-scan every drain", *sweeps)
	}
	if n := capture.countWarns(repairDeferWarn); n != 1 {
		t.Fatalf("deferral Warns inside the backoff = %d, want the one per streak", n)
	}

	// Past the floor it re-examines exactly once - the deferral is still real, so it must not
	// be forgotten - and says nothing new about it.
	advance(repairDeferBackoffBase)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep past the floor: %v", err)
	}
	if *sweeps != 1 {
		t.Fatalf("baseline streams past the floor = %d, want 1", *sweeps)
	}
	if n := capture.countWarns(repairDeferWarn); n != 1 {
		t.Fatalf("deferral Warns past the floor = %d, want the one per streak", n)
	}

	// And the floor has stretched: the same wait no longer reaches the next examination.
	advance(repairDeferBackoffBase)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep inside the stretched backoff: %v", err)
	}
	if *sweeps != 1 {
		t.Fatalf("baseline streams inside the stretched backoff = %d, want 1: the backoff did not grow", *sweeps)
	}
	advance(repairDeferBackoffBase)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep past the stretched backoff: %v", err)
	}
	if *sweeps != 2 {
		t.Fatalf("baseline streams past the stretched backoff = %d, want 2", *sweeps)
	}
	if n := capture.countWarns(repairDeferWarn); n != 1 {
		t.Fatalf("deferral Warns overall = %d, want the one per streak", n)
	}
}

// TestQuarantineRepairBackoffResetsWhenTheDeferredSetChanges pins the other half: the backoff is a
// floor on re-examining the SAME evidence, so a new deferred candidate restarts it at the base and
// tells the operator once more. Without the reset a second stranded record inherited a backoff
// grown for an unrelated one.
func TestQuarantineRepairBackoffResetsWhenTheDeferredSetChanges(t *testing.T) {
	e, f, capture, advance := newDeferralFixture(t)
	ctx := context.Background()
	sweeps := countScanBaselines(e)

	// A second stranded pending baseline under the same still-queued mkdir: overlapped, owned by
	// nothing, so the next sweep defers it too.
	writeFile(t, f.viewDir, "dir/deep2.txt", []byte("v2"))
	if err := e.store.PutBaseline(repairViewID, "dir/deep2.txt", store.Baseline{
		FileID: "fd2", FileType: "file", LocalHash: "deadbeef", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}

	advance(repairDeferBackoffBase)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep past the floor: %v", err)
	}
	if *sweeps != 1 {
		t.Fatalf("baseline streams past the floor = %d, want 1", *sweeps)
	}
	if n := capture.countWarns(repairDeferWarn); n != 2 {
		t.Fatalf("deferral Warns after the set changed = %d, want a second one: this is new evidence", n)
	}

	// The backoff restarted at the base rather than continuing to grow, so one base wait reaches
	// the next examination.
	advance(repairDeferBackoffBase / 2)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep inside the restarted backoff: %v", err)
	}
	if *sweeps != 1 {
		t.Fatalf("baseline streams inside the restarted backoff = %d, want 1", *sweeps)
	}
	advance(repairDeferBackoffBase / 2)
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep past the restarted backoff: %v", err)
	}
	if *sweeps != 2 {
		t.Fatalf("baseline streams past the restarted backoff = %d, want 2: the backoff did not restart at its base", *sweeps)
	}
	if n := capture.countWarns(repairDeferWarn); n != 2 {
		t.Fatalf("deferral Warns overall = %d, want one per streak (two streaks)", n)
	}
}

// TestQuarantineRepairBackoffYieldsToDrainedCoverage is the liveness half: the backoff must never
// delay a deferral that has become resolvable. The clock does not move at all here - the covering
// op simply leaves the queue, which is exactly the ordinary temporary case the re-arm exists for.
func TestQuarantineRepairBackoffYieldsToDrainedCoverage(t *testing.T) {
	e, _, _, _ := newDeferralFixture(t)
	ctx := context.Background()

	ops, err := e.store.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if err := e.store.Ack(ops[0].Seq); err != nil {
		t.Fatalf("Ack covering op: %v", err)
	}
	if err := e.repairQuarantinedBaselines(ctx); err != nil {
		t.Fatalf("sweep after the coverage drained: %v", err)
	}
	ops, err = e.store.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after the sweep: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "dir/deep.txt" {
		t.Fatalf("queued ops after the sweep = %+v, want the re-derived push: the backoff must not outlive the coverage", ops)
	}
}

// deferralIndex builds a queued index holding one endpoint, the shape that defers a candidate
// beneath it.
func deferralIndex(endpoints ...string) queuedIndex {
	idx := queuedIndex{exact: make(map[string]bool), ancestors: make(map[string]bool)}
	for _, k := range endpoints {
		idx.add(k)
	}
	return idx
}

// TestRepairDeferralHoldsOnlyForUnresolvedCoverage is the unit-level truth table of the gate: it
// holds only while the arming quarantine, the coverage and the clock all say the previous
// deferral is still the same unexamined condition.
func TestRepairDeferralHoldsOnlyForUnresolvedCoverage(t *testing.T) {
	// Native separators: the index answers the ancestor case through filepath.Dir, exactly as the
	// executor's own conflict keys are spelled (cleanKey).
	key := filepath.Join("Photos", "dir", "deep.txt")
	covered := deferralIndex(filepath.Join("Photos", "dir"))

	cases := []struct {
		name       string
		deferral   repairDeferral
		quarantine uint64
		queued     queuedIndex
		elapsed    time.Duration
		want       bool
	}{
		{
			name:       "same coverage inside the floor",
			deferral:   repairDeferral{keys: []string{key}, quarantined: 3},
			quarantine: 3,
			queued:     covered,
			want:       true,
		},
		{
			name:       "no deferral recorded",
			deferral:   repairDeferral{},
			quarantine: 3,
			queued:     covered,
		},
		{
			name:       "a newer quarantine armed the sweep",
			deferral:   repairDeferral{keys: []string{key}, quarantined: 3},
			quarantine: 4,
			queued:     covered,
		},
		{
			name:       "the floor elapsed",
			deferral:   repairDeferral{keys: []string{key}, quarantined: 3},
			quarantine: 3,
			queued:     covered,
			elapsed:    repairDeferBackoffBase,
		},
		{
			name:       "the covering op drained",
			deferral:   repairDeferral{keys: []string{key}, quarantined: 3},
			quarantine: 3,
			queued:     deferralIndex(filepath.Join("Photos", "elsewhere")),
		},
		{
			name:       "a queued op now owns the key",
			deferral:   repairDeferral{keys: []string{key}, quarantined: 3},
			quarantine: 3,
			queued:     deferralIndex(key),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Engine{}
			advance := stubClock(e)
			d := tc.deferral
			if len(d.keys) > 0 {
				d.backoff = repairDeferBackoffBase
				d.retryAt = e.now().Add(repairDeferBackoffBase)
			}
			e.repairDeferred = d
			advance(tc.elapsed)
			if got := e.repairDeferralHolds(tc.quarantine, tc.queued); got != tc.want {
				t.Fatalf("repairDeferralHolds = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRepairDeferralStretchesAndCapsTheBackoff pins the schedule itself: geometric growth to a
// ceiling while the deferral repeats, one Warn for the whole streak, and a restart at the base
// (with a fresh Warn) the moment the candidate set changes.
func TestRepairDeferralStretchesAndCapsTheBackoff(t *testing.T) {
	capture := &logCapture{}
	e := &Engine{log: slog.New(capture)}
	stubClock(e)

	e.noteRepairDeferral(1, []string{"Photos/b", "Photos/a"})
	if got := e.repairDeferred.backoff; got != repairDeferBackoffBase {
		t.Fatalf("first backoff = %v, want the base %v", got, repairDeferBackoffBase)
	}
	if got := e.repairDeferred.keys; len(got) != 2 || got[0] != "Photos/a" {
		t.Fatalf("remembered keys = %v, want them sorted so a repeat compares equal", got)
	}

	want := repairDeferBackoffBase
	for i := range 12 {
		e.noteRepairDeferral(1, []string{"Photos/a", "Photos/b"})
		if want *= 2; want > repairDeferBackoffMax {
			want = repairDeferBackoffMax
		}
		if got := e.repairDeferred.backoff; got != want {
			t.Fatalf("backoff after repeat %d = %v, want %v", i, got, want)
		}
	}
	if got := e.repairDeferred.backoff; got != repairDeferBackoffMax {
		t.Fatalf("backoff = %v, want it capped at %v", got, repairDeferBackoffMax)
	}
	if n := capture.countWarns(repairDeferWarn); n != 1 {
		t.Fatalf("Warns for one streak = %d, want 1", n)
	}

	e.noteRepairDeferral(1, []string{"Photos/a"})
	if got := e.repairDeferred.backoff; got != repairDeferBackoffBase {
		t.Fatalf("backoff after the set changed = %v, want the base %v", got, repairDeferBackoffBase)
	}
	if n := capture.countWarns(repairDeferWarn); n != 2 {
		t.Fatalf("Warns after the set changed = %d, want a second streak reported", n)
	}

	// The same set under a NEWER quarantine is new evidence too: those candidates were deferred
	// by a different arming, and the sweep must not inherit the old schedule's silence.
	e.noteRepairDeferral(2, []string{"Photos/a"})
	if got := e.repairDeferred.backoff; got != repairDeferBackoffBase {
		t.Fatalf("backoff after a new quarantine = %v, want the base %v", got, repairDeferBackoffBase)
	}
	if n := capture.countWarns(repairDeferWarn); n != 3 {
		t.Fatalf("Warns after a new quarantine = %d, want it reported", n)
	}
}

// TestRepairDeferralCapsTheRememberedKeys pins the memory bound: a deferral larger than the cap is
// remembered as a sorted prefix, so the map cannot grow with the tree a queued directory rename
// covers.
func TestRepairDeferralCapsTheRememberedKeys(t *testing.T) {
	e := &Engine{log: slog.New(&logCapture{})}
	stubClock(e)
	keys := make([]string, repairDeferKeysCap+16)
	for i := range keys {
		keys[i] = "Photos/f" + string(rune('a'+i%26)) + strconv.Itoa(i)
	}
	e.noteRepairDeferral(1, keys)
	if got := len(e.repairDeferred.keys); got != repairDeferKeysCap {
		t.Fatalf("remembered keys = %d, want the cap %d", got, repairDeferKeysCap)
	}
	for i := 1; i < len(e.repairDeferred.keys); i++ {
		if e.repairDeferred.keys[i-1] > e.repairDeferred.keys[i] {
			t.Fatalf("remembered keys are not sorted at %d", i)
		}
	}
}
