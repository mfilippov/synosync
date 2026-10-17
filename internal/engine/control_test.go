package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestPausedReflectsState confirms Paused tracks Pause/Resume: false initially, true
// after Pause, false again after Resume (Phase 8b).
func TestPausedReflectsState(t *testing.T) {
	e := New(newTestStore(t), &concTransport{client: &loopClient{}}, t.TempDir(), "s", nil)
	if e.Paused() {
		t.Fatal("Paused() = true before any Pause")
	}
	e.Pause()
	if !e.Paused() {
		t.Fatal("Paused() = false after Pause()")
	}
	e.Pause() // idempotent
	if !e.Paused() {
		t.Fatal("Paused() = false after a second Pause()")
	}
	e.Resume()
	if e.Paused() {
		t.Fatal("Paused() = true after Resume()")
	}
	e.Resume() // idempotent, non-blocking
	if e.Paused() {
		t.Fatal("Paused() = true after a second Resume()")
	}
}

// TestWaitWhilePaused confirms the gate returns at once when not paused, blocks while
// paused, and unblocks on Resume.
func TestWaitWhilePaused(t *testing.T) {
	e := New(newTestStore(t), &concTransport{client: &loopClient{}}, t.TempDir(), "s", nil)

	if err := e.waitWhilePaused(context.Background()); err != nil {
		t.Fatalf("waitWhilePaused (not paused) = %v, want nil", err)
	}

	e.Pause()
	done := make(chan error, 1)
	go func() { done <- e.waitWhilePaused(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("waitWhilePaused returned %v while paused, want it to block", err)
	case <-time.After(100 * time.Millisecond):
	}

	e.Resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitWhilePaused after Resume = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitWhilePaused did not return after Resume")
	}
}

// TestWaitWhilePausedCtxCancel confirms the gate returns ctx.Err() when the context is
// cancelled while paused, so Run exits cleanly on shutdown even while paused.
func TestWaitWhilePausedCtxCancel(t *testing.T) {
	e := New(newTestStore(t), &concTransport{client: &loopClient{}}, t.TempDir(), "s", nil)
	e.Pause()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.waitWhilePaused(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waitWhilePaused on cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitWhilePaused did not return on ctx cancel")
	}
}

// TestRunPauseStopsCyclesResumeRestarts drives the gate through Run: an engine paused
// before Run starts never reaches the long-poll, and resuming lets the loop poll. A
// cancel then stops it cleanly.
func TestRunPauseStopsCyclesResumeRestarts(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	polled := make(chan struct{}, 1)
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}},
	}
	lc.pollFn = func(int) ([]int64, error) {
		select {
		case polled <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)
	e.Pause()
	if !e.Paused() {
		t.Fatal("Paused() = false after Pause()")
	}

	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	// While paused the loop must not reach the long-poll.
	select {
	case <-polled:
		t.Fatal("loop polled while paused")
	case <-time.After(150 * time.Millisecond):
	}

	e.Resume()
	if e.Paused() {
		t.Fatal("Paused() = true after Resume()")
	}
	select {
	case <-polled:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not poll after Resume")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestTriggerSyncInterruptsLongPoll confirms a TriggerSync delivered while the loop is
// blocked in the long-poll interrupts it exactly like a watcher wake: it resets the scan
// throttle and re-dials the byte-misaligned event connection. A pre-buffered trigger
// makes the interrupt deterministic.
func TestTriggerSyncInterruptsLongPoll(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &blockingWakeClient{}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.TriggerSync() // a pending trigger the poll goroutine consumes at once
	e.lastScan = time.Unix(1700000000, 0)

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v (a trigger interrupt must not be terminal)", err)
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (TriggerSync must force the next localScan)", e.lastScan)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Errorf("ReplaceEvent calls = %d, want >= 1 (a trigger-broken event conn must be re-dialed)", replaces)
	}
}

// TestTriggerSyncCoalesces confirms TriggerSync is non-blocking and coalesces a burst to
// a single buffered wake (cap-1), so a flood of `sync now` calls never blocks the caller.
func TestTriggerSyncCoalesces(t *testing.T) {
	e := New(newTestStore(t), &concTransport{client: &loopClient{}}, t.TempDir(), "s", nil)
	e.TriggerSync()
	e.TriggerSync() // must not block; coalesces into the one buffered slot

	select {
	case <-e.manualWake:
	default:
		t.Fatal("expected one buffered trigger after TriggerSync")
	}
	select {
	case <-e.manualWake:
		t.Fatal("expected the burst to coalesce to a single buffered trigger")
	default:
	}
}
