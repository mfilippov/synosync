package engine

// Tests for the per-op retry pacing (two-client finding H4). The stand measured one
// deterministically refused `mkdir` being re-sent 23 610 times in ~5 minutes - about 80
// requests a second - because nothing at all stood between a failed apply and the next drain's
// identical turn. These tests pin the pause that now does: it grows geometrically, it caps, it
// arms only on a REPEATED identical server rejection, and it is released the moment the failure
// class changes or the op leaves the queue.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestRetryBackoffGrowsAndCaps pins the schedule itself: no pause for a first failure (a
// genuine transient must still retry on the very next drain), then a doubling run that stops at
// the cap so a wedged op is re-probed on the order of the loop's own polling cadence rather
// than never again.
func TestRetryBackoffGrowsAndCaps(t *testing.T) {
	if got := retryBackoff(1); got != 0 {
		t.Errorf("retryBackoff(1) = %v, want no pause on a first failure", got)
	}
	if got := retryBackoff(0); got != 0 {
		t.Errorf("retryBackoff(0) = %v, want no pause", got)
	}
	if got := retryBackoff(2); got != retryBackoffBase {
		t.Errorf("retryBackoff(2) = %v, want the base pause %v", got, retryBackoffBase)
	}
	prev := retryBackoff(2)
	capped := 0
	for streak := 3; streak <= 40; streak++ {
		got := retryBackoff(streak)
		switch {
		case got == retryBackoffCap:
			capped++
		case got != 2*prev:
			t.Fatalf("retryBackoff(%d) = %v, want %v (double the previous) or the cap %v",
				streak, got, 2*prev, retryBackoffCap)
		}
		if got > retryBackoffCap {
			t.Fatalf("retryBackoff(%d) = %v exceeds the cap %v", streak, got, retryBackoffCap)
		}
		prev = got
	}
	if capped == 0 {
		t.Errorf("retryBackoff never reached the cap %v within 40 identical rejections", retryBackoffCap)
	}
	// The pause must stay well under the escalation's own wall-clock floors, or the pacing -
	// not the evidence - would decide when an op dead-letters.
	if retryBackoffCap >= nameRejectionMinAge {
		t.Errorf("retryBackoffCap %v is not below nameRejectionMinAge %v", retryBackoffCap, nameRejectionMinAge)
	}
}

// TestDeterministicPushIsPacedAcrossDrains is the H4 red test at the drain level: a push the
// server refuses the same way every time is re-attempted a BOUNDED number of times per unit of
// wall clock, and the gaps between attempts GROW. Pre-fix every drain re-issued the turn, so
// the attempt count equalled the drain count exactly - the ~80/second the stand recorded.
func TestDeterministicPushIsPacedAcrossDrains(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	at := time.Unix(1_750_000_000, 0)
	e.now = func() time.Time { return at }
	seedMkdirWithChild(t, st, viewDir)

	// 200 drains, 100 ms of wall clock apart: twenty seconds of a busy loop hammering the
	// queue, which is exactly the shape a watcher wake / `synosync sync` storm produces.
	const (
		drains = 200
		step   = 100 * time.Millisecond
	)
	var attemptAt []time.Duration
	start := at
	for i := 0; i < drains; i++ {
		before := len(c.mkdirs)
		if err := e.drainQueue(context.Background()); err != nil && isTerminal(err) {
			t.Fatalf("drain %d: terminal error %v", i+1, err)
		}
		c.mu.Lock()
		grew := len(c.mkdirs) > before
		c.mu.Unlock()
		if grew {
			attemptAt = append(attemptAt, at.Sub(start))
		}
		at = at.Add(step)
	}
	// Bounded rate: with the pause doubling from 500 ms to the cap, twenty seconds admit a
	// handful of attempts, not two hundred.
	if len(attemptAt) < 2 {
		t.Fatalf("mkdir attempted %d times across %d drains; the op must keep retrying", len(attemptAt), drains)
	}
	if len(attemptAt) > 8 {
		t.Errorf("mkdir attempted %d times in %v of wall clock, want the retry paced (<= 8): %v",
			len(attemptAt), time.Duration(drains)*step, attemptAt)
	}
	// Growing gaps: each successive pause is at least as long as the previous one, and the run
	// as a whole gets strictly longer somewhere (it is a doubling schedule, not a fixed delay).
	var grew bool
	for i := 2; i < len(attemptAt); i++ {
		prev, cur := attemptAt[i-1]-attemptAt[i-2], attemptAt[i]-attemptAt[i-1]
		if cur < prev {
			t.Errorf("attempt gaps shrank: %v then %v (attempts at %v)", prev, cur, attemptAt)
		}
		if cur > prev {
			grew = true
		}
	}
	if len(attemptAt) > 2 && !grew {
		t.Errorf("attempt gaps never grew, want a geometric backoff: %v", attemptAt)
	}
	// Nothing was dropped and nothing applied: the op is merely paced, and its chain still
	// holds enqueue order behind it.
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops, want 0 (the wall clock is far below the escalation floor)", dl)
	}
}

// TestRetryPauseClearedByADifferentFailure confirms the pause is armed by the EVIDENCE, not by
// the op: once the failure class changes (a broken connection the pool re-dials underneath),
// holding the op back would only delay the retry that recovers from it, so the pause goes with
// the streak.
func TestRetryPauseClearedByADifferentFailure(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < 2; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
	}
	if _, paused := e.opRetryPending(1); !paused {
		t.Fatal("no retry pause armed after two identical rejections")
	}
	// Let the pause elapse so the next drain really attempts, and answer it with a different
	// failure class - the connection breaking under the push.
	advance(retryBackoffCap)
	c.mu.Lock()
	c.mkdirErr = actions.ErrConnBroken
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("broken-conn drain succeeded, want an error")
	}
	if _, paused := e.opRetryPending(1); paused {
		t.Error("retry pause survived a change of failure class; the recovery retry is delayed for nothing")
	}
}

// TestRetryPauseLeavesWithTheOp confirms the pause is dropped when the op leaves the queue, so
// the map cannot outgrow the live queue and a re-used seq cannot inherit someone else's pacing
// - the same discipline the streak and the stuck counters follow.
func TestRetryPauseLeavesWithTheOp(t *testing.T) {
	c := &pushClient{
		mkdirErr:  &actions.ServerError{Code: 33333},
		mkdirNode: map[string]actions.Node{"dir": {FileID: "d1"}},
	}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)
	c.uploadNode = map[string]actions.Node{"f.txt": {FileID: "f1", Hash: "h"}}

	for i := 0; i < 2; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
	}
	if _, paused := e.opRetryPending(1); !paused {
		t.Fatal("no retry pause armed after two identical rejections")
	}
	c.mu.Lock()
	c.mkdirErr = nil
	c.mu.Unlock()
	advance(retryBackoffCap) // the pause elapses; the healed push lands
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("healed drain: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending after the healed drain = %d, want 0", n)
	}
	e.stuckMu.Lock()
	left := len(e.opRetryAfter)
	e.stuckMu.Unlock()
	if left != 0 {
		t.Errorf("retry pauses left behind = %d, want 0 (the ops left the queue)", left)
	}
}

// TestPacedRetryKeepsEnqueueOrder pins the one property the deferral must not trade away: a
// paused op still blocks the successors that overlap its path, exactly as a transiently failed
// one does. Applying a later same-path op while its predecessor merely waits would break the
// serialization invariant (serialize.go) - and the deferral is a skip, not a completion.
func TestPacedRetryKeepsEnqueueOrder(t *testing.T) {
	c := &pushClient{
		mkdirErr:   &actions.ServerError{Code: 33333},
		uploadNode: map[string]actions.Node{"f.txt": {FileID: "f1", Hash: "h"}},
	}
	e, st, viewDir, _ := serverErrEngine(t, c)
	stubClock(e) // frozen: after the second rejection every later drain is a pure deferral
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < 5; i++ {
		if err := e.drainQueue(context.Background()); err != nil && isTerminal(err) {
			t.Fatalf("drain %d: terminal error %v", i+1, err)
		}
	}
	c.mu.Lock()
	uploads := len(c.uploads)
	c.mu.Unlock()
	if uploads != 0 {
		t.Errorf("child upload ran %d times while its parent mkdir was paused, want 0", uploads)
	}
	pend, err := st.PendingOpsN(10)
	if err != nil {
		t.Fatalf("PendingOpsN: %v", err)
	}
	if len(pend) != 2 || pend[0].Type != store.OpMkdir {
		t.Fatalf("queue = %+v, want the mkdir still at the head with its child behind it", pend)
	}
}

// TestTransientInboundErrnoIsPaced is the direction the H5 measurement actually came from: 32772
// was answered to a `download` of a node another client had just overwritten, i.e. to an INBOUND
// op. Escalation is (deliberately) closed to inbound work - dead-lettering it would drop a server
// change whose event is already consumed - but the PACING is not, and without it the download
// re-issued its whole turn on every drain: the same ~80-requests-a-second spin H4 measured on the
// push side, on the one code H5 measured.
//
// The op must stay queued and undropped throughout: pacing changes the rate, never the outcome.
func TestTransientInboundErrnoIsPaced(t *testing.T) {
	c := &pushClient{downloadErr: &actions.ServerError{Code: actions.CodeNodeBusy}}
	e, st, _, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Drain repeatedly without advancing past the backoff: after the first repetition the op is
	// deferred and the server stops being asked.
	for range 20 {
		_ = e.drainQueue(context.Background())
		advance(time.Millisecond)
	}
	if n := c.downloadCalls; n > 5 {
		t.Errorf("issued %d download turns across 20 drains, want a paced handful", n)
	}
	if n := c.downloadCalls; n == 0 {
		t.Error("issued no download turns at all; the first attempt must never be delayed")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("pending = %d, want the inbound op still queued (pacing must not drop it)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0: inbound work never escalates", dl)
	}

	// A change of failure class clears the streak and releases the pause, so recovery is not
	// delayed by evidence that no longer describes the failure. The clock is advanced past the
	// armed pause first: a paced op is SKIPPED without running, so the new class can only be
	// observed once the pause it was armed with has elapsed.
	advance(2 * retryBackoffCap)
	c.downloadErr = errors.New("a different, transient failure")
	before := c.downloadCalls
	_ = e.drainQueue(context.Background())
	if c.downloadCalls == before {
		t.Error("a changed failure class did not release the pause; the retry stayed deferred")
	}
	e.stuckMu.Lock()
	_, stillArmed := e.opRetryAfter[1]
	e.stuckMu.Unlock()
	if stillArmed {
		t.Error("the pause survived a change of failure class")
	}
}

// TestPacedDrainIsNotABatchError pins the contract every caller of drainQueue reads: a drain that
// only DEFERRED its ops (the pause armed by the streak has not elapsed, so nothing was attempted)
// reports success. A paced retry is the intended outcome, and reporting it as a failure would make
// loop.go log an error on every cycle for an op it deliberately held back - and would make the
// deferral indistinguishable from the transient failure it replaces for every test that asserts on
// the drain's error.
func TestPacedDrainIsNotABatchError(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	stubClock(e) // frozen: no pause can elapse
	seedMkdirWithChild(t, st, viewDir)

	// Two attempts: the first is never paced, the second arms the pause.
	for i := range 2 {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d = nil, want the rejection reported while the op is still being attempted", i+1)
		}
	}
	if _, armed := e.opRetryPending(1); !armed {
		t.Fatal("the repeated rejection armed no pause")
	}
	c.mu.Lock()
	attempts := len(c.mkdirs)
	c.mu.Unlock()

	if err := e.drainQueue(context.Background()); err != nil {
		t.Errorf("a drain that only deferred its ops = %v, want nil", err)
	}
	c.mu.Lock()
	after := len(c.mkdirs)
	c.mu.Unlock()
	if after != attempts {
		t.Errorf("mkdir attempts = %d, want the paced op left unattempted (%d)", after, attempts)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued behind the pause", n)
	}
}
