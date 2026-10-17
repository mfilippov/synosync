package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
)

// waitFor polls cond for up to two seconds.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWalkKeepsAWorkerFree: while a walk runs, ordinary leases are held to Workers-1 so the
// walk's own lease gets a worker at once; when the walk ends the held lease goes on.
func TestWalkKeepsAWorkerFree(t *testing.T) {
	e := New(newTestStore(t), laneTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{Workers: 3})
	e.beginWalk()

	var active atomic.Int32
	hold := make(chan struct{})
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = e.withWorker(context.Background(), func(syncClient) error {
				active.Add(1)
				<-hold
				return nil
			})
		}()
	}
	waitFor(t, func() bool { return active.Load() == 2 }, "two ordinary leases")
	time.Sleep(50 * time.Millisecond)
	if n := active.Load(); n != 2 {
		t.Fatalf("%d ordinary leases ran during a walk, want 2 (Workers-1)", n)
	}

	walked := make(chan struct{})
	go func() {
		_ = e.withWalkWorker(context.Background(), func(syncClient) error { close(walked); return nil })
	}()
	select {
	case <-walked:
	case <-time.After(2 * time.Second):
		t.Fatal("the walk's lease waited behind ordinary leases")
	}

	e.endWalk()
	waitFor(t, func() bool { return active.Load() == 3 }, "the held lease after the walk ended")
	close(hold)
	wg.Wait()
}

// TestASingleWorkerIsNeverGated: with one worker there is nothing to reserve.
func TestASingleWorkerIsNeverGated(t *testing.T) {
	e := New(newTestStore(t), laneTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{Workers: 1})
	e.beginWalk()
	defer e.endWalk()
	ran := false
	if err := e.withWorker(context.Background(), func(syncClient) error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("withWorker = %v, ran=%v; want an ungated lease", err, ran)
	}
}

// TestGatedLeaseHonoursCancellation: a lease waiting at the gate returns when its context ends.
func TestGatedLeaseHonoursCancellation(t *testing.T) {
	e := New(newTestStore(t), laneTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{Workers: 2})
	e.beginWalk()
	defer e.endWalk()
	hold := make(chan struct{})
	defer close(hold)
	go func() { _ = e.withWorker(context.Background(), func(syncClient) error { <-hold; return nil }) }()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := e.withWorker(ctx, func(syncClient) error { return nil }); err == nil {
		t.Fatal("a lease past the gate's capacity ran; want it to wait and then honour the deadline")
	}
}

// laneTransport leases without bookkeeping, so concurrent leases do not race on a fake's counters.
type laneTransport struct{ client syncClient }

func (t laneTransport) EventClient() (syncClient, error) { return t.client, nil }
func (t laneTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	return fn(t.client)
}
func (t laneTransport) ReplaceEvent(context.Context) error { return nil }

// TestRecentRateMeasuresTheLastMinute: the recent rate follows the last minute, not the run.
func TestRecentRateMeasuresTheLastMinute(t *testing.T) {
	var r recentRing
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A fast first minute: 100 ops/s, then a slow one at 10 ops/s.
	var ops int64
	for s := 0; s <= 60; s += 5 {
		ops = int64(s) * 100
		r.note(t0.Add(time.Duration(s)*time.Second), ops, ops*1000)
	}
	for s := 65; s <= 120; s += 5 {
		ops += 50
		r.note(t0.Add(time.Duration(s)*time.Second), ops, ops*1000)
	}
	now := t0.Add(120 * time.Second)
	got, _, ok := r.rate(now, ops, ops*1000)
	if !ok || got < 9 || got > 11 {
		t.Errorf("recent rate = %.1f ops/s (ok=%v), want about 10 (the last minute, not the run)", got, ok)
	}
	if _, _, ok := (&recentRing{}).rate(now, 0, 0); ok {
		t.Error("an empty ring claimed a rate")
	}
	idle, _, ok := r.rate(now.Add(5*time.Minute), ops, ops*1000)
	if !ok || idle != 0 {
		t.Errorf("after five idle minutes: rate %.1f ok=%v, want 0 and known", idle, ok)
	}
}
