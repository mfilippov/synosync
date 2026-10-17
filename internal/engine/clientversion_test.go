package engine

// Tests for the ACTION-axis consumption of a rejected client_version (20481).
// The connect axis has always read that code as a deterministic verdict about
// this BINARY - auth.ErrClientVersion → transport.ErrPoolClientRejected → the daemon stops
// with an "update synosync" hint. Received on a turn of an already-established session it
// used to arrive as an anonymous *actions.ServerError and be consumed on the PER-OP axis:
// a streak of identical rejections marked every outbound op StateConflict and dead-lettered
// it (one-way - localReconcile never re-derives such a push), while the one actionable hint
// was reachable only if the connection happened to drop first. The action layer now maps
// 20481 to the shared auth sentinel and the loop treats it as terminal, so the queue is left
// intact for the upgraded binary.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// mappedActionErr reproduces what actions.errorForCode returns for a mapped result code -
// the shared sentinel wrapped with the numeric code - so these tests exercise the exact
// error shape a real action turn hands the engine. The mapping itself is pinned on the
// actions side (TestErrorForCodeAllSentinels, TestDoErrorCodes).
func mappedActionErr(sentinel error, code int64) error {
	return fmt.Errorf("%w (code %d)", sentinel, code)
}

// TestOutboundClientVersionRejectionIsTerminalNotDeadLettered is the red test for the
// client-version rejection: the server refuses every push turn of a live session with 20481.
// Pre-fix the code was an unmapped *ServerError, so after unmappedServerErrDrains identical
// rejections the mkdir was marked StateConflict and dropped - and every later push would have
// followed, one dropped local change per streak. Now the drain surfaces a terminal error on the
// FIRST rejection: nothing is dead-lettered, both ops stay queued for the upgraded client, and the
// baseline is untouched.
func TestOutboundClientVersionRejectionIsTerminalNotDeadLettered(t *testing.T) {
	c := &pushClient{mkdirErr: mappedActionErr(auth.ErrClientVersion, actions.CodeClientVersion)}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// Drain far past both escalation gates - the drain count and the wall-clock floor -
	// so a regression that reads 20481 as an unknown deterministic rejection drops the op.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		err := e.drainQueue(context.Background())
		if err == nil {
			t.Fatalf("drain %d: succeeded, want the injected client-version rejection", i+1)
		}
		if !errors.Is(err, auth.ErrClientVersion) {
			t.Fatalf("drain %d error = %v, want it to carry auth.ErrClientVersion", i+1, err)
		}
		if !isTerminal(err) {
			t.Fatalf("drain %d: isTerminal(%v) = false; the loop would retry a verdict about this binary", i+1, err)
		}
		advance(2 * unmappedServerErrMinAge)
	}

	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on a client-version rejection, want 0 (the push is fine; the binary is not)", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued for the upgraded client", n)
	}
	b, err := st.GetBaseline(7, "dir")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateConflict {
		t.Error("baseline marked StateConflict by a client-version rejection; localReconcile never re-derives the push")
	}
}

// TestRunStopsOnActionClientVersionRejection pins the consumption end: a 20481 on an action
// turn of a live session stops Run with the shared sentinel, which is exactly what the
// daemon's terminalStopReason and cmd/synosync's stderr hint already key on ("update
// synosync"). Pre-fix the loop retried the turn forever and the hint was unreachable without
// a reconnect.
func TestRunStopsOnActionClientVersionRejection(t *testing.T) {
	st := newTestStore(t)
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
		pollFn: func(int) ([]int64, error) {
			return nil, mappedActionErr(auth.ErrClientVersion, actions.CodeClientVersion)
		},
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)

	err := e.Run(context.Background())
	if !errors.Is(err, auth.ErrClientVersion) {
		t.Fatalf("Run error = %v, want auth.ErrClientVersion", err)
	}
	// The re-pair hint must NOT fire for a rejection an upgrade fixes.
	if errors.Is(err, auth.ErrSessionVanished) || errors.Is(err, transport.ErrPoolPaused) {
		t.Errorf("Run error = %v, want it classified as a client-version rejection, not a vanished session", err)
	}
}

// TestOutboundSessionVanishedRejectionUnchanged is the 8193 regression: the sibling mapped
// code keeps its own terminal path (never per-op escalation), so the shared-sentinel change
// for 20481 did not move it.
func TestOutboundSessionVanishedRejectionUnchanged(t *testing.T) {
	c := &pushClient{mkdirErr: mappedActionErr(auth.ErrSessionVanished, actions.CodeSessionVanished)}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		err := e.drainQueue(context.Background())
		if err == nil {
			t.Fatalf("drain %d: succeeded, want the injected vanished-session rejection", i+1)
		}
		if !errors.Is(err, auth.ErrSessionVanished) || !isTerminal(err) {
			t.Fatalf("drain %d error = %v, want a terminal vanished-session error", i+1, err)
		}
		advance(2 * unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on a vanished session, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued", n)
	}
}

// TestIsTerminalClientVersionSentinels pins both spellings of the verdict: the pool's
// (connect axis) and the auth sentinel an action turn now carries. The unmapped-code class
// stays transient - that is what the per-op escalation is for.
func TestIsTerminalClientVersionSentinels(t *testing.T) {
	for _, err := range []error{
		auth.ErrClientVersion,
		mappedActionErr(auth.ErrClientVersion, actions.CodeClientVersion),
		fmt.Errorf("engine: apply mkdir: %w", mappedActionErr(auth.ErrClientVersion, actions.CodeClientVersion)),
		transport.ErrPoolClientRejected,
	} {
		if !isTerminal(err) {
			t.Errorf("isTerminal(%v) = false, want true", err)
		}
	}
	if isTerminal(&actions.ServerError{Code: 33333}) {
		t.Error("isTerminal(unmapped *ServerError) = true; an unknown code must stay transient")
	}
}

// TestIsTerminalRefreshErrorClientVersion pins the enrichment seam: refreshViews' best-effort
// root-id QueryNode must not swallow a client-version rejection as an ordinary lookup
// failure, or the verdict would never reach Run from that turn.
func TestIsTerminalRefreshErrorClientVersion(t *testing.T) {
	err := mappedActionErr(auth.ErrClientVersion, actions.CodeClientVersion)
	if !isTerminalRefreshError(err) {
		t.Errorf("isTerminalRefreshError(%v) = false, want true", err)
	}
	if isTerminalRefreshError(&actions.ServerError{Code: 33333}) {
		t.Error("isTerminalRefreshError(unmapped *ServerError) = true; an ordinary lookup failure stays best-effort")
	}
}
