package netctx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// TestWatchCancelUnblocksRead verifies that cancelling ctx forces a past deadline
// onto the connection so an otherwise-blocking Read returns promptly.
func TestWatchCancelUnblocksRead(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stop := Watch(ctx, c1)
	defer stop()

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := c1.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected read to be interrupted, got nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read was not unblocked by cancellation")
	}
}

// TestWatchDeadlinePropagated verifies that a context deadline is propagated onto
// the connection so a blocking read times out on its own.
func TestWatchDeadlinePropagated(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	stop := Watch(ctx, c1)
	defer stop()

	buf := make([]byte, 1)
	_, err := c1.Read(buf)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline-exceeded read error, got %v", err)
	}
}

// TestWatchStopClearsDeadline verifies that stop() clears the deadline so the
// connection is reusable: a later read with no deadline blocks rather than
// inheriting the prior turn's expired one.
func TestWatchStopClearsDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	stop := Watch(ctx, c1)
	// Let the deadline elapse, then stop (clearing it).
	time.Sleep(20 * time.Millisecond)
	cancel()
	stop()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := c1.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("expected read to block after deadline cleared, returned %v", err)
	case <-time.After(50 * time.Millisecond):
		// Still blocked - deadline was cleared. Now write so the read completes.
	}
	go func() { _, _ = c2.Write([]byte{1}) }()
	if err := <-done; err != nil {
		t.Fatalf("read after clear failed: %v", err)
	}
}

// TestWatchNoCancel verifies the fast path when ctx can never be cancelled
// (Done() == nil): Watch returns a no-op stop that is safe to call.
func TestWatchNoCancel(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	stop := Watch(context.Background(), c1)
	stop() // must not panic or block
}

// stubDeadlineCtx is a context whose Deadline() reports a chosen time but whose Err() stays nil,
// modelling the narrow window where the socket deadline has fired (os.ErrDeadlineExceeded) before
// the runtime's context timer has set ctx.Err(). A real context.WithDeadline cannot reproduce
// this - it sets Err() the instant the deadline passes.
type stubDeadlineCtx struct {
	deadline time.Time
}

func (c *stubDeadlineCtx) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *stubDeadlineCtx) Done() <-chan struct{}       { return nil }
func (c *stubDeadlineCtx) Err() error                  { return nil }
func (c *stubDeadlineCtx) Value(any) any               { return nil }

func TestErr(t *testing.T) {
	t.Run("cancelled ctx preferred over the forced deadline", func(t *testing.T) {
		// Watch expires the socket to interrupt the turn, so the I/O error a cancelled
		// turn actually produces is os.ErrDeadlineExceeded (here wrapped, as the
		// sproto/actions layers wrap it): that artifact is what the substitution is for.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		artifact := fmt.Errorf("sproto: read header: %w", os.ErrDeadlineExceeded)
		if got := Err(ctx, artifact); !errors.Is(got, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", got)
		}
	})

	t.Run("cancelled ctx does not mask a terminal error", func(t *testing.T) {
		// A pin mismatch (or a vanished session) racing a pool close/pause must survive:
		// transport.terminalConnectErr matches the sentinel to pause the pool and demand a
		// re-pair, and would keep re-dialing under backoff if the cancellation overwrote it.
		// Only the deadline artifact above is Watch's own doing.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		sentinel := errors.New("transport: certificate pin mismatch")
		wrapped := fmt.Errorf("transport: pool worker: %w", sentinel)
		if got := Err(ctx, wrapped); !errors.Is(got, sentinel) {
			t.Fatalf("got %v, want the pin-mismatch sentinel preserved", got)
		}
		if errors.Is(Err(ctx, wrapped), context.Canceled) {
			t.Fatalf("terminal error was rewritten to context.Canceled")
		}
	})

	t.Run("expired-deadline ctx does not mask a terminal error", func(t *testing.T) {
		// Same rule on the deadline half of the substitution, including the
		// ctx.Err()-still-nil window the stub context models.
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()
		sentinel := errors.New("auth: session vanished")
		if got := Err(ctx, sentinel); !errors.Is(got, sentinel) {
			t.Fatalf("got %v, want the sentinel preserved", got)
		}
		stub := &stubDeadlineCtx{deadline: time.Now().Add(-time.Second)}
		if got := Err(stub, sentinel); !errors.Is(got, sentinel) {
			t.Fatalf("got %v, want the sentinel preserved", got)
		}
	})

	t.Run("nil error still reports ctx cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := Err(ctx, nil); !errors.Is(got, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", got)
		}
	})

	t.Run("past deadline maps timeout with ctx.Err still nil", func(t *testing.T) {
		// The kernel/runtime race the branch guards: the socket deadline (derived from
		// ctx's deadline) has fired and produced os.ErrDeadlineExceeded, but the runtime's
		// context timer has not yet marked ctx done, so ctx.Err() is still nil. A real
		// context.WithDeadline(past) reports Err()!=nil immediately and would short-circuit
		// the first branch, never reaching the mapping logic - so a stub context is used to
		// pin exactly this window.
		ctx := &stubDeadlineCtx{deadline: time.Now().Add(-time.Second)}
		got := Err(ctx, os.ErrDeadlineExceeded)
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("got %v, want context.DeadlineExceeded", got)
		}
	})

	t.Run("future deadline does not map timeout", func(t *testing.T) {
		// Same stub, but the deadline is still in the future, so an os.ErrDeadlineExceeded
		// is NOT yet attributable to the context deadline and must pass through unchanged
		// (the !time.Now().Before(dl) guard is false). If the mapping mistakenly fired it
		// would return context.DeadlineExceeded, which is not errors.Is os.ErrDeadlineExceeded.
		ctx := &stubDeadlineCtx{deadline: time.Now().Add(time.Hour)}
		want := os.ErrDeadlineExceeded
		got := Err(ctx, want)
		if !errors.Is(got, want) || errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("got %v, want os.ErrDeadlineExceeded unchanged", got)
		}
	})

	t.Run("live ctx returns err unchanged", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		want := errors.New("real network error")
		if got := Err(ctx, want); got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("live ctx with non-deadline error", func(t *testing.T) {
		want := os.ErrDeadlineExceeded
		// No deadline on ctx, so the deadline-mapping branch is not taken.
		if got := Err(context.Background(), want); !errors.Is(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}
