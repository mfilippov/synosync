package actions

// Tests for the idle-progress watchdog on the ORDINARY action turns (client.go,
// actionIdleTimeout) - the sibling of the download watchdog in idle_test.go.
//
// The download paths were bounded first because they are excluded from
// limits.op_turn_timeout_s by design. But that per-turn deadline ships DISABLED
// (config.defaultOpTurnTimeout = 0), so in the default configuration nothing bounded
// the other reply reads either: a server that accepted the request and then went
// silent (a wedged storage layer, a SIGSTOPped peer - the kernel keeps ACKing, so TCP
// never notices) blocked list / query_node / pull_event / mkdir / rename / remove /
// the upload replies forever, and with them the op goroutine, drainBatch's join and
// the whole sync cycle.
//
// long_poll is the deliberate exception and gets its own test: its reply is withheld
// server-side for the entire poll timeout by design.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

// shortActionIdle shrinks the ordinary-turn idle budget for the duration of one test
// so a stall resolves in milliseconds rather than the production 5 min. The tests
// using it are single-turn and never run in parallel, so the package-level var is not
// raced.
func shortActionIdle(t *testing.T, d time.Duration) {
	t.Helper()
	prev := actionIdleTimeout
	actionIdleTimeout = d
	t.Cleanup(func() { actionIdleTimeout = prev })
}

// silentServerClient wires a Client to a mock server that reads the request and then
// never answers, holding the connection open. The release cleanup is registered AFTER
// serveMock's so it runs first (LIFO) and the server goroutine can always be joined.
func silentServerClient(t *testing.T) *Client {
	t.Helper()
	release := make(chan struct{})
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		<-release
		return nil
	})
	t.Cleanup(func() { close(release) })
	return c
}

func TestSilentReplyFailsWithinActionIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	shortActionIdle(t, idle)
	c := silentServerClient(t)

	// The caller's context is deliberately far longer than the budget: this must be
	// the WATCHDOG's verdict, not a context deadline the operator happened to set.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	_, _, err := c.List(ctx, 1, "/")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("List err = %v, want ErrTransferStalled", err)
	}
	if elapsed > 20*idle {
		t.Fatalf("List returned after %s, want the stall detected within a few idle budgets (%s)", elapsed, idle)
	}
	if !c.Broken() {
		t.Error("Broken() = false after a stalled turn, want the connection poisoned so the pool re-dials it")
	}
	if _, _, err := c.List(ctx, 1, "/"); !errors.Is(err, ErrConnBroken) {
		t.Errorf("second List err = %v, want ErrConnBroken", err)
	}
}

// TestSilentMutatingReplyFailsWithinActionIdleBudget covers the mutating half: the
// same wedge on rename/remove kept the op goroutine - and the serialised chain of ops
// behind it - blocked forever.
func TestSilentMutatingReplyFailsWithinActionIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	shortActionIdle(t, idle)
	c := silentServerClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Remove(ctx, 1, "id:f1"); !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("Remove err = %v, want ErrTransferStalled", err)
	}
	if !c.Broken() {
		t.Error("Broken() = false after a stalled mutating turn, want the connection poisoned")
	}
}

// TestLongPollIsNotKilledByTheActionIdleBudget is the mirror requirement. long_poll
// blocks server-side for the whole poll timeout by design, so silence there is the
// healthy steady state; arming the ordinary watchdog on it would fail every quiet poll
// cycle and re-dial the event connection for nothing. Its bound is the caller's ctx
// (pollOnce's timeout + slack).
func TestLongPollIsNotKilledByTheActionIdleBudget(t *testing.T) {
	const idle = 50 * time.Millisecond
	const serverHold = 10 * idle
	shortActionIdle(t, idle)

	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		time.Sleep(serverHold)
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("changes", sproto.Array{sproto.Int(7)})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	changed, err := c.LongPoll(ctx, subs(7), 1)
	if err != nil {
		t.Fatalf("LongPoll err = %v, want nil (its reply is withheld server-side by design)", err)
	}
	if len(changed) != 1 || changed[0] != 7 {
		t.Fatalf("LongPoll changed = %v, want [7]", changed)
	}
	if c.Broken() {
		t.Error("Broken() = true after a healthy long poll")
	}
}
