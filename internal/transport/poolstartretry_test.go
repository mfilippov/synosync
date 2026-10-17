package transport

// Tests for the started-flag reset after a failed bring-up. Start
// sets started before its first dial and used never to release it: a transiently failed bring-up
// left a pool that holds no sessions, refuses a second Start with "pool already started", and
// blocks Acquire forever on a free channel nothing can fill - the exact hang ErrPoolNotStarted
// was introduced to eliminate.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
)

// TestPoolTransientStartFailureIsRetryable is the red test: after a transient bring-up failure
// Acquire must report ErrPoolNotStarted instead of blocking, and a later Start must be able to
// bring the pool up for real.
func TestPoolTransientStartFailureIsRetryable(t *testing.T) {
	s := newMockServer(t)
	real := poolConnector(s)
	fail := true
	p := NewPool(func(ctx context.Context) (*Session, error) {
		if fail {
			return nil, errors.New("dial tcp: connection refused")
		}
		return real(ctx)
	}, 2)

	if err := p.Start(testContext(t)); err == nil {
		t.Fatalf("Start with a failing connector = nil, want the transient dial error")
	}
	if p.Paused() {
		t.Errorf("a transient bring-up failure paused the pool; only terminal errors may pause")
	}

	// Acquire must answer, not hang: nothing can ever land on free after a failed bring-up.
	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolNotStarted) {
			t.Fatalf("Acquire after a failed Start = %v, want ErrPoolNotStarted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire after a failed Start blocked forever; want ErrPoolNotStarted")
	}

	// And the pool is retryable: the next Start brings it up.
	fail = false
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start after a transient failure: %v", err)
	}
	defer p.Close()
	sess, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire after a successful retry: %v", err)
	}
	if got := roundTrip(t, sess, "w"); got != "w" {
		t.Errorf("worker echo = %q, want w", got)
	}
	release()
}

// TestPoolTransientStartFailureMidWorkerIsRetryable covers the other bring-up arm: the event
// connection succeeds and a WORKER dial fails, so Start closes what it opened. The pool must be
// left equally retryable, with no session leaked into the free channel.
func TestPoolTransientStartFailureMidWorkerIsRetryable(t *testing.T) {
	s := newMockServer(t)
	real := poolConnector(s)
	dials := 0
	failAfter := 1 // the event connection succeeds, the first worker fails
	p := NewPool(func(ctx context.Context) (*Session, error) {
		dials++
		if failAfter > 0 && dials > failAfter {
			return nil, errors.New("dial tcp: connection reset by peer")
		}
		return real(ctx)
	}, 2)

	if err := p.Start(testContext(t)); err == nil {
		t.Fatalf("Start with a failing worker dial = nil, want the transient error")
	}
	if _, _, err := p.Acquire(context.Background()); !errors.Is(err, ErrPoolNotStarted) {
		t.Fatalf("Acquire after a failed worker bring-up = %v, want ErrPoolNotStarted", err)
	}
	failAfter = 0
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start after a transient worker failure: %v", err)
	}
	defer p.Close()
	if p.Event() == nil {
		t.Errorf("Event() = nil after a successful retry")
	}
	for i := 0; i < p.NumWorkers(); i++ {
		sess, release, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d after retry: %v", i, err)
		}
		if got := roundTrip(t, sess, "w"); got != "w" {
			t.Errorf("worker echo = %q, want w", got)
		}
		defer release()
	}
}

// TestPoolTerminalStartFailureStillPauses pins the other half of the contract: a terminal
// bring-up failure (a vanished session) still pauses the pool for good - Acquire reports the
// pause sentinel, not "not started", and the pool is NOT retryable.
func TestPoolTerminalStartFailureStillPauses(t *testing.T) {
	p := NewPool(func(context.Context) (*Session, error) {
		return nil, auth.ErrSessionVanished
	}, 2)

	err := p.Start(testContext(t))
	if !errors.Is(err, ErrPoolPaused) {
		t.Fatalf("Start with a vanished session = %v, want ErrPoolPaused", err)
	}
	if !p.Paused() {
		t.Fatalf("Paused() = false after a terminal bring-up failure")
	}
	if _, _, err := p.Acquire(context.Background()); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Acquire after a terminal bring-up failure = %v, want ErrPoolPaused", err)
	}
	if got := p.PauseReason(); !errors.Is(got, ErrPoolPaused) {
		t.Errorf("PauseReason = %v, want ErrPoolPaused", got)
	}
}
