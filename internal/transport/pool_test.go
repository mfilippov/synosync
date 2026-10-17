package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/backoff"
	"github.com/mfilippov/synosync/internal/sproto"
)

// zeroBackoff builds an effectively-instant backoff sequence so reconnect tests
// exercise the retry loop without real waiting.
func zeroBackoff() *backoff.Backoff {
	return &backoff.Backoff{Base: time.Nanosecond, Max: time.Nanosecond, Factor: 1}
}

// poolConnector returns a Connector that dials the mock server in verify mode
// (the captured leaf pin), so every call yields an independent, fully-upgraded
// Session.
func poolConnector(s *mockServer) Connector {
	agent := sproto.NewMap().Set("type", sproto.String("desktop"))
	return func(ctx context.Context) (*Session, error) {
		return Connect(ctx, ConnectConfig{
			Addr:  s.addr(),
			Agent: agent,
			DsID:  testDsID,
			Pin:   s.leafPin(),
		})
	}
}

// roundTrip issues one echo action over a session and returns the echoed value,
// confirming the connection is live.
func roundTrip(t *testing.T, sess *Session, msg string) string {
	t.Helper()
	if err := sess.Conn().WriteRequest(sproto.CmdAction, sproto.NewMap().Set("action", sproto.String(msg))); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	resp, err := sess.Conn().ReadResponse()
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	m, ok := resp.(*sproto.Map)
	if !ok {
		t.Fatalf("response is %T, want *Map", resp)
	}
	v, _ := m.Get("echo")
	s, _ := v.(sproto.String)
	return string(s)
}

func TestPoolNewClampsWorkers(t *testing.T) {
	if got := NewPool(nil, 0).NumWorkers(); got != minWorkers {
		t.Errorf("NumWorkers(0) = %d, want %d", got, minWorkers)
	}
	if got := NewPool(nil, 99).NumWorkers(); got != maxWorkers {
		t.Errorf("NumWorkers(99) = %d, want %d", got, maxWorkers)
	}
	if got := NewPool(nil, 3).NumWorkers(); got != 3 {
		t.Errorf("NumWorkers(3) = %d, want 3", got)
	}
}

// TestPoolStartAndEvent verifies Start brings up a usable, dedicated event
// connection.
func TestPoolStartAndEvent(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	ev := p.Event()
	if ev == nil {
		t.Fatal("Event() = nil after Start")
	}
	if got := roundTrip(t, ev, "ping"); got != "ping" {
		t.Errorf("event echo = %q, want ping", got)
	}
}

// TestPoolAcquireDistinctWorkers leases every worker and confirms they are
// distinct connections, none of which is the event connection.
func TestPoolAcquireDistinctWorkers(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 3)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	seen := make(map[*Session]bool)
	releases := make([]func(), 0, p.NumWorkers())
	for i := 0; i < p.NumWorkers(); i++ {
		sess, release, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		if sess == p.Event() {
			t.Fatal("Acquire returned the event connection")
		}
		if seen[sess] {
			t.Fatal("Acquire returned the same worker twice while both leased")
		}
		seen[sess] = true
		if got := roundTrip(t, sess, "w"); got != "w" {
			t.Errorf("worker echo = %q, want w", got)
		}
		releases = append(releases, release)
	}
	for _, r := range releases {
		r()
	}
	// After release the workers are reusable.
	sess, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	if !seen[sess] {
		t.Error("re-acquired session is not one of the original workers")
	}
	release()
}

// TestPoolReleaseIsIdempotent confirms calling release twice does not deadlock or
// over-fill the pool.
func TestPoolReleaseIsIdempotent(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	_, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
	release() // no-op
}

// TestPoolAcquireExhaustedBlocks confirms Acquire blocks once all workers are
// leased and honours context cancellation.
func TestPoolAcquireExhaustedBlocks(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		_, release, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		releases = append(releases, release)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := p.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire on exhausted pool: err = %v, want context.DeadlineExceeded", err)
	}

	// Releasing one frees a slot for a subsequent Acquire.
	releases[0]()
	sess, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire after freeing a slot: %v", err)
	}
	release()
	for _, r := range releases[1:] {
		r()
	}
	_ = sess
}

// TestPoolConcurrentAcquireNoSharing hammers Acquire from many goroutines and
// verifies no two callers ever hold the same connection simultaneously.
func TestPoolConcurrentAcquireNoSharing(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 4)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	var mu sync.Mutex
	held := make(map[*Session]bool)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess, release, err := p.Acquire(testContext(t))
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			mu.Lock()
			if held[sess] {
				t.Error("connection leased to two callers at once")
			}
			held[sess] = true
			mu.Unlock()

			// Keep the session held while doing real work on it, so a genuine
			// double-lease would overlap in the held map rather than being masked by
			// a mark-then-immediately-unmark window.
			roundTrip(t, sess, "ping")

			mu.Lock()
			delete(held, sess)
			mu.Unlock()
			release()
		}()
	}
	wg.Wait()
}

// TestPoolStartConnectError confirms a Connector failure during Start surfaces an
// error and leaves the pool unusable.
func TestPoolStartConnectError(t *testing.T) {
	sentinel := errors.New("boom")
	connect := func(ctx context.Context) (*Session, error) {
		// The very first dial (the event connection) fails, so Start bails before
		// any worker is dialed. (The event-up/worker-fail rollback path is covered
		// by TestPoolStartWorkerErrorRollback.)
		return nil, sentinel
	}
	p := NewPool(connect, 2)
	err := p.Start(testContext(t))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Start error = %v, want sentinel", err)
	}
}

// TestPoolStartWorkerErrorRollback confirms that when a worker connection fails
// after the event connection is up, the already-opened sessions are closed.
func TestPoolStartWorkerErrorRollback(t *testing.T) {
	s := newMockServer(t)
	sentinel := errors.New("worker boom")
	real := poolConnector(s)
	var opened []*Session
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		if calls <= 2 { // event conn + first worker succeed
			sess, err := real(ctx)
			if err == nil {
				opened = append(opened, sess)
			}
			return sess, err
		}
		return nil, sentinel
	}
	p := NewPool(connect, 2)
	if err := p.Start(testContext(t)); !errors.Is(err, sentinel) {
		t.Fatalf("Start error = %v, want sentinel", err)
	}
	// The two opened sessions must have been closed on rollback: a write fails.
	for i, sess := range opened {
		if err := sess.Conn().WriteRequest(sproto.CmdAction, sproto.NewMap()); err == nil {
			t.Errorf("opened session %d still writable after rollback", i)
		}
	}
}

// TestPoolStartTwice confirms a second Start is rejected.
func TestPoolStartTwice(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()
	if err := p.Start(testContext(t)); err == nil {
		t.Fatal("second Start: want error, got nil")
	}
}

// TestPoolClose confirms Close shuts everything down and unblocks waiters.
func TestPoolClose(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if p.Event() != nil {
		t.Error("Event() != nil after Close")
	}
	if _, _, err := p.Acquire(testContext(t)); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Acquire after Close: err = %v, want ErrPoolClosed", err)
	}
	// Close is idempotent.
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestPoolCloseUnblocksAcquire confirms a goroutine blocked in Acquire is
// released when the pool closes.
func TestPoolCloseUnblocksAcquire(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Drain all workers so the next Acquire blocks.
	for i := 0; i < p.NumWorkers(); i++ {
		if _, _, err := p.Acquire(testContext(t)); err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()
	// Give the goroutine a moment to block on the empty channel.
	time.Sleep(20 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolClosed) {
			t.Errorf("blocked Acquire after Close: err = %v, want ErrPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not unblock after Close")
	}
}

// TestPoolReconnectRetriesThenSucceeds confirms reconnect retries a transiently
// failing Connector under backoff and returns the session once it succeeds.
func TestPoolReconnectRetriesThenSucceeds(t *testing.T) {
	const fails = 3
	transient := errors.New("connection refused")
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		if calls <= fails {
			return nil, transient
		}
		return &Session{}, nil
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	sess, err := p.reconnect(testContext(t))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if sess == nil {
		t.Fatal("reconnect returned nil session")
	}
	if calls != fails+1 {
		t.Errorf("Connector calls = %d, want %d", calls, fails+1)
	}
	if p.Paused() {
		t.Error("pool paused after a successful reconnect")
	}
}

// TestPoolReconnectAttemptTimeoutTransient pins the error
// classification: a connect attempt that fails with the daemon connector's
// per-attempt deadline (a wrapped context.DeadlineExceeded while the CALLER's
// ctx is still live) is a transient failure - reconnect retries it under
// backoff and the pool never pauses - because pausing (or bailing) on an
// attempt timeout would turn a briefly-hung server into a re-pair signal.
func TestPoolReconnectAttemptTimeoutTransient(t *testing.T) {
	const fails = 3
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		if calls <= fails {
			// The shape the daemon connector produces when its per-attempt
			// deadline expires inside transport.Connect/auth.Reuse.
			return nil, fmt.Errorf("daemon: connect nas.example:6690: %w", context.DeadlineExceeded)
		}
		return &Session{}, nil
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	sess, err := p.reconnect(testContext(t))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if sess == nil {
		t.Fatal("reconnect returned nil session")
	}
	if calls != fails+1 {
		t.Errorf("Connector calls = %d, want %d (attempt timeouts retried)", calls, fails+1)
	}
	if p.Paused() {
		t.Error("pool paused after attempt timeouts; they must stay transient")
	}
}

// TestPoolReconnectSessionVanishedPauses confirms a vanished session from the
// Connector pauses the pool and returns ErrPoolPaused wrapping the auth sentinel,
// without further retries.
func TestPoolReconnectSessionVanishedPauses(t *testing.T) {
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		return nil, fmt.Errorf("reuse: %w", auth.ErrSessionVanished)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	_, err := p.reconnect(testContext(t))
	if !errors.Is(err, ErrPoolPaused) {
		t.Errorf("reconnect err = %v, want ErrPoolPaused", err)
	}
	if !errors.Is(err, auth.ErrSessionVanished) {
		t.Errorf("reconnect err = %v, want it to wrap auth.ErrSessionVanished", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a vanished session")
	}
	if calls != 1 {
		t.Errorf("Connector calls = %d, want 1 (no retry on vanished)", calls)
	}

	// Once paused, reconnect fails fast without dialing again.
	if _, err := p.reconnect(testContext(t)); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("reconnect while paused err = %v, want ErrPoolPaused", err)
	}
	if calls != 1 {
		t.Errorf("Connector called again while paused: calls = %d, want 1", calls)
	}
}

// TestPoolReconnectPinMismatchPauses confirms a certificate pin mismatch from the
// Connector is terminal exactly like a vanished session: the pool pauses and
// reconnect returns ErrPoolPaused wrapping ErrPinMismatch without retrying a doomed
// dial.
func TestPoolReconnectPinMismatchPauses(t *testing.T) {
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		return nil, fmt.Errorf("connect: %w", ErrPinMismatch)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	_, err := p.reconnect(testContext(t))
	if !errors.Is(err, ErrPoolPaused) {
		t.Errorf("reconnect err = %v, want ErrPoolPaused", err)
	}
	if !errors.Is(err, ErrPinMismatch) {
		t.Errorf("reconnect err = %v, want it to wrap ErrPinMismatch", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a pin mismatch")
	}
	if calls != 1 {
		t.Errorf("Connector calls = %d, want 1 (no retry on pin mismatch)", calls)
	}
}

// TestPoolStartPinMismatchPauses confirms a pin mismatch during pool bring-up is
// the same terminal pause signal as at reconnect time: Start reports ErrPoolPaused
// wrapping ErrPinMismatch rather than a plain dial error the daemon would retry.
func TestPoolStartPinMismatchPauses(t *testing.T) {
	connect := func(ctx context.Context) (*Session, error) {
		return nil, fmt.Errorf("connect: %w", ErrPinMismatch)
	}
	p := NewPool(connect, 2)
	err := p.Start(testContext(t))
	if !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Start err = %v, want ErrPoolPaused", err)
	}
	if !errors.Is(err, ErrPinMismatch) {
		t.Errorf("Start err = %v, want it to wrap ErrPinMismatch", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a pin mismatch at Start")
	}
}

// TestPoolReconnectClientVersionPauses confirms a rejected client_version (20481,
// auth.ErrClientVersion) is terminal like a vanished session, but pauses with its
// own ErrPoolClientRejected sentinel (an "update synosync" signal, distinct from
// ErrPoolPaused's "re-pair" signal) - no doomed retry of a binary the server
// refuses.
func TestPoolReconnectClientVersionPauses(t *testing.T) {
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		return nil, fmt.Errorf("reuse: %w", auth.ErrClientVersion)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	_, err := p.reconnect(testContext(t))
	if !errors.Is(err, ErrPoolClientRejected) {
		t.Errorf("reconnect err = %v, want ErrPoolClientRejected", err)
	}
	if !errors.Is(err, auth.ErrClientVersion) {
		t.Errorf("reconnect err = %v, want it to wrap auth.ErrClientVersion", err)
	}
	// It is a distinct sentinel: it must NOT read as the re-pair ErrPoolPaused, or
	// the daemon would print the wrong ("re-login") hint.
	if errors.Is(err, ErrPoolPaused) {
		t.Errorf("reconnect err = %v, must not read as ErrPoolPaused", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a rejected client version")
	}
	if calls != 1 {
		t.Errorf("Connector calls = %d, want 1 (no retry on rejected client version)", calls)
	}
}

// TestPoolStartClientVersionPauses confirms a rejected client_version during pool
// bring-up is the same terminal pause as at reconnect time: Start reports
// ErrPoolClientRejected wrapping auth.ErrClientVersion rather than a plain dial
// error the daemon would retry forever.
func TestPoolStartClientVersionPauses(t *testing.T) {
	connect := func(ctx context.Context) (*Session, error) {
		return nil, fmt.Errorf("reuse: %w", auth.ErrClientVersion)
	}
	p := NewPool(connect, 2)
	err := p.Start(testContext(t))
	if !errors.Is(err, ErrPoolClientRejected) {
		t.Errorf("Start err = %v, want ErrPoolClientRejected", err)
	}
	if !errors.Is(err, auth.ErrClientVersion) {
		t.Errorf("Start err = %v, want it to wrap auth.ErrClientVersion", err)
	}
	if errors.Is(err, ErrPoolPaused) {
		t.Errorf("Start err = %v, must not read as ErrPoolPaused", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a rejected client version at Start")
	}
}

// TestPoolPausedStateSurfacesClientRejected confirms the SPECIFIC pause sentinel
// reaches a paused-state surfacing path (Acquire / ReplaceEvent), not just the
// reconnect goroutine that detected it. A mid-run
// client_version rejection pauses the pool via one goroutine while a concurrent
// Acquire on another observes the paused state - that Acquire must surface
// ErrPoolClientRejected ("update synosync"), not the generic ErrPoolPaused
// ("re-login"), so terminalStopReason prints the right hint regardless of which
// error wins the daemon's set-once firstErr race. Before the fix every
// paused-state path returned the generic ErrPoolPaused, so the wrong hint could
// (and for a mid-run rejection usually would) win.
func TestPoolPausedStateSurfacesClientRejected(t *testing.T) {
	p := NewPool(func(context.Context) (*Session, error) { return nil, nil }, 2)
	// Enter the paused state with the client-version sentinel exactly as a mid-run
	// reconnect would (pause records pauseSentinel of the terminal Connector error).
	p.mu.Lock()
	p.pause(pauseSentinel(fmt.Errorf("reuse: %w", auth.ErrClientVersion)))
	p.mu.Unlock()

	_, _, aerr := p.Acquire(testContext(t))
	if !errors.Is(aerr, ErrPoolClientRejected) {
		t.Errorf("Acquire after client-version pause: err = %v, want ErrPoolClientRejected", aerr)
	}
	if errors.Is(aerr, ErrPoolPaused) {
		t.Errorf("Acquire err = %v, must not read as the generic ErrPoolPaused", aerr)
	}

	// The event re-dial guard surfaces the same specific reason.
	if _, rerr := p.ReplaceEvent(testContext(t)); !errors.Is(rerr, ErrPoolClientRejected) {
		t.Errorf("ReplaceEvent after client-version pause: err = %v, want ErrPoolClientRejected", rerr)
	}

	// A plain vanished-session pause still reads as the generic ErrPoolPaused, so
	// the re-pair hint is unchanged for that class.
	p2 := NewPool(func(context.Context) (*Session, error) { return nil, nil }, 2)
	p2.mu.Lock()
	p2.pause(pauseSentinel(fmt.Errorf("reuse: %w", auth.ErrSessionVanished)))
	p2.mu.Unlock()
	if _, _, err := p2.Acquire(testContext(t)); !errors.Is(err, ErrPoolPaused) || errors.Is(err, ErrPoolClientRejected) {
		t.Errorf("Acquire after vanished-session pause: err = %v, want ErrPoolPaused (not client-rejected)", err)
	}
}

// TestPoolPauseReason confirms the exported PauseReason accessor reports nil until
// the pool pauses and then the specific sentinel it paused with - the
// hook the engine's EventClient seam propagates so a client-rejected pause winning
// a race prints "update synosync" rather than the generic re-login hint.
func TestPoolPauseReason(t *testing.T) {
	p := NewPool(func(context.Context) (*Session, error) { return nil, nil }, 2)
	if r := p.PauseReason(); r != nil {
		t.Errorf("PauseReason before pause = %v, want nil", r)
	}

	p.mu.Lock()
	p.pause(pauseSentinel(fmt.Errorf("reuse: %w", auth.ErrClientVersion)))
	p.mu.Unlock()
	if r := p.PauseReason(); !errors.Is(r, ErrPoolClientRejected) {
		t.Errorf("PauseReason after client-version pause = %v, want ErrPoolClientRejected", r)
	}
	if errors.Is(p.PauseReason(), ErrPoolPaused) {
		t.Errorf("PauseReason after client-version pause = %v, must not read as ErrPoolPaused", p.PauseReason())
	}

	p2 := NewPool(func(context.Context) (*Session, error) { return nil, nil }, 2)
	p2.mu.Lock()
	p2.pause(pauseSentinel(fmt.Errorf("reuse: %w", auth.ErrSessionVanished)))
	p2.mu.Unlock()
	if r := p2.PauseReason(); !errors.Is(r, ErrPoolPaused) || errors.Is(r, ErrPoolClientRejected) {
		t.Errorf("PauseReason after vanished-session pause = %v, want ErrPoolPaused (not client-rejected)", r)
	}
}

// TestPoolReconnectCtxCancel confirms reconnect honours context cancellation
// while retrying.
func TestPoolReconnectCtxCancel(t *testing.T) {
	transient := errors.New("connection refused")
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	connect := func(c context.Context) (*Session, error) {
		calls++
		if calls >= 2 {
			cancel()
		}
		return nil, transient
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	if _, err := p.reconnect(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("reconnect err = %v, want context.Canceled", err)
	}
	if p.Paused() {
		t.Error("pool paused on ctx cancel; want only on vanished")
	}
}

// TestPoolReconnectCtxCancelFuncIsIdempotent pins the CancelFunc contract:
// reconnectCtx's cancel closes a private stop channel, so a
// second call - the ordinary `defer cancel()` plus an explicit early `cancel()` -
// used to panic on a closed channel and take the daemon down from inside a
// reconnect loop. It must be a no-op, and the returned context must stay cancelled.
func TestPoolReconnectCtxCancelFuncIsIdempotent(t *testing.T) {
	p := NewPool(nil, 2)
	rctx, cancel := p.reconnectCtx(context.Background())
	cancel()
	cancel() // must not panic
	cancel()
	if !errors.Is(rctx.Err(), context.Canceled) {
		t.Errorf("rctx.Err() = %v, want context.Canceled", rctx.Err())
	}
	// The pool-close watcher path is the other caller shape: closing the pool after
	// the cancel must not double-close anything either.
	p2 := NewPool(nil, 2)
	rctx2, cancel2 := p2.reconnectCtx(context.Background())
	p2.Close()
	<-rctx2.Done()
	cancel2()
	cancel2()
}

// TestPoolAcquireBeforeStartErrors pins that Acquire on an un-Started pool reports
// ErrPoolNotStarted instead of blocking. Nothing can ever land
// on the free channel before Start, so the pre-fix behaviour was an indefinite hang
// - with the engine's background ctx, a silent permanent stall with no diagnostic.
func TestPoolAcquireBeforeStartErrors(t *testing.T) {
	p := NewPool(func(context.Context) (*Session, error) {
		t.Error("Acquire before Start dialed a connection")
		return nil, errors.New("unexpected dial")
	}, 2)

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolNotStarted) {
			t.Fatalf("Acquire before Start = %v, want ErrPoolNotStarted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire before Start blocked; want ErrPoolNotStarted")
	}

	// Closed and paused keep their own sentinels rather than being reported as
	// "not started", so a caller racing teardown still gets the terminal answer.
	closed := NewPool(nil, 2)
	closed.Close()
	if _, _, err := closed.Acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Acquire on a closed unstarted pool = %v, want ErrPoolClosed", err)
	}
	paused := NewPool(nil, 2)
	paused.mu.Lock()
	paused.pause(ErrPoolPaused)
	paused.mu.Unlock()
	if _, _, err := paused.Acquire(context.Background()); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Acquire on a paused unstarted pool = %v, want ErrPoolPaused", err)
	}

	// After a real Start the lease path is unaffected.
	s := newMockServer(t)
	up := NewPool(poolConnector(s), 2)
	if err := up.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer up.Close()
	sess, release, err := up.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire after Start: %v", err)
	}
	if got := roundTrip(t, sess, "ping"); got != "ping" {
		t.Errorf("worker echo = %q, want ping", got)
	}
	release()
}

// TestPoolReplaceWorker confirms a dead worker is swapped for a fresh, usable
// connection that rejoins the lease pool, preserving capacity.
func TestPoolReplaceWorker(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Simulate a broken turn: replace the leased worker instead of releasing it.
	fresh, release, err := p.Replace(testContext(t), dead)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if fresh == dead {
		t.Fatal("Replace returned the dead session")
	}
	if got := roundTrip(t, fresh, "r"); got != "r" {
		t.Errorf("replaced worker echo = %q, want r", got)
	}
	release()

	// Capacity is intact: both workers are acquirable.
	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		sess, rel, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d after Replace: %v", i, err)
		}
		if got := roundTrip(t, sess, "c"); got != "c" {
			t.Errorf("worker echo = %q, want c", got)
		}
		releases = append(releases, rel)
	}
	for _, r := range releases {
		r()
	}
}

// TestPoolReplaceStaleReleaseIsNoop confirms that calling the original Acquire
// release after Replace does not re-inject the dead connection.
func TestPoolReplaceStaleReleaseIsNoop(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, staleRelease, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	fresh, release, err := p.Replace(testContext(t), dead)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	staleRelease() // must be a no-op: dead is no longer a pool member
	release()

	// Every acquirable worker must be live (the dead one must not resurface).
	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		sess, rel, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		if sess == dead {
			t.Fatal("dead session resurfaced via stale release")
		}
		if got := roundTrip(t, sess, "l"); got != "l" {
			t.Errorf("worker echo = %q, want l (dead connection handed out?)", got)
		}
		releases = append(releases, rel)
	}
	for _, r := range releases {
		r()
	}
	_ = fresh
}

// TestPoolReplaceUnknownWorker confirms replacing a session the pool does not
// manage is rejected.
func TestPoolReplaceUnknownWorker(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	if _, _, err := p.Replace(testContext(t), &Session{}); err == nil {
		t.Fatal("Replace of an unknown worker: want error, got nil")
	}
}

// TestPoolReplaceReleasedSessionRejected pins the lease precondition Replace's
// free-buffer arithmetic depends on. Membership in the worker
// set is not enough: a session that has already been released is sitting in the
// free channel, so closing it and installing a replacement would (a) leave the
// next Acquire holding a dead connection and (b) put numWorkers+1 entries through
// a numWorkers-sized buffer - the following release then blocks forever while
// holding p.mu and wedges every pool operation. Replace must reject the call with
// ErrPoolNotLeased, before dialing, and leave the pool fully usable.
func TestPoolReplaceReleasedSessionRejected(t *testing.T) {
	s := newMockServer(t)
	base := poolConnector(s)
	var mu sync.Mutex
	dials := 0
	connect := func(ctx context.Context) (*Session, error) {
		mu.Lock()
		dials++
		mu.Unlock()
		return base(ctx)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	sess, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release() // the lease ends here; sess goes back into free
	mu.Lock()
	dialsBefore := dials
	mu.Unlock()

	fresh, freshRelease, err := p.Replace(testContext(t), sess)
	if !errors.Is(err, ErrPoolNotLeased) {
		if err == nil {
			freshRelease()
		}
		t.Fatalf("Replace of a released worker: err = %v, want ErrPoolNotLeased", err)
	}
	if fresh != nil || freshRelease != nil {
		t.Error("rejected Replace returned a session/release")
	}
	mu.Lock()
	dialsAfter := dials
	mu.Unlock()
	if dialsAfter != dialsBefore {
		t.Errorf("rejected Replace dialed %d times, want 0", dialsAfter-dialsBefore)
	}

	// The pool is intact: every worker is still leasable, live, and releasable -
	// the free buffer never went over capacity, so no release blocks under p.mu.
	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		w, rel, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d after rejected Replace: %v", i, err)
		}
		if got := roundTrip(t, w, "live"); got != "live" {
			t.Errorf("worker echo = %q, want live (closed connection handed out?)", got)
		}
		releases = append(releases, rel)
	}
	for _, r := range releases {
		r()
	}
	// And a genuinely leased worker is still replaceable.
	leased, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire for Replace: %v", err)
	}
	swapped, swappedRelease, err := p.Replace(testContext(t), leased)
	if err != nil {
		t.Fatalf("Replace of a leased worker: %v", err)
	}
	if got := roundTrip(t, swapped, "swap"); got != "swap" {
		t.Errorf("replacement echo = %q, want swap", got)
	}
	swappedRelease()
}

// TestPoolReplaceReplacementAfterReleaseRejected confirms the lease bookkeeping
// follows the replacement session too: the fresh worker Replace hands back is
// leased (so it can itself be replaced when the retry's turn also drops), but once
// its release runs it is back in free and no longer Replace-able.
func TestPoolReplaceReplacementAfterReleaseRejected(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	first, firstRelease, err := p.Replace(testContext(t), dead)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	// Still leased: a second drop on the same chain replaces it again.
	second, secondRelease, err := p.Replace(testContext(t), first)
	if err != nil {
		t.Fatalf("Replace of the replacement: %v", err)
	}
	firstRelease() // stale release for a swapped-out session: a no-op
	secondRelease()

	if _, _, err := p.Replace(testContext(t), second); !errors.Is(err, ErrPoolNotLeased) {
		t.Errorf("Replace of a released replacement: err = %v, want ErrPoolNotLeased", err)
	}
	// The chain's own stale release must not have resurrected a lease either.
	if _, _, err := p.Replace(testContext(t), first); err == nil {
		t.Error("Replace of a swapped-out session: want error, got nil")
	}

	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		w, rel, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		if got := roundTrip(t, w, "chain"); got != "chain" {
			t.Errorf("worker echo = %q, want chain", got)
		}
		releases = append(releases, rel)
	}
	for _, r := range releases {
		r()
	}
}

// TestPoolReplaceNilSessionRejected reproduces the vacant-slot match: while one
// Replace is mid-re-dial its worker slot holds nil, so a nil dead session used to
// match that slot and double-install a replacement into it, leaking whichever
// session lost the race. Replace must instead fast-fail on nil input immediately,
// leaving the in-flight Replace to fill its own slot.
func TestPoolReplaceNilSessionRejected(t *testing.T) {
	s := newMockServer(t)
	base := poolConnector(s)
	gate := make(chan struct{})
	var mu sync.Mutex
	dials := 0
	connect := func(ctx context.Context) (*Session, error) {
		mu.Lock()
		dials++
		n := dials
		mu.Unlock()
		// Start opens 1 event + 2 worker connections; block every later dial (the
		// Replace re-dials) until the test releases the gate.
		if n > 3 {
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return base(ctx)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	replaceDone := make(chan error, 1)
	go func() {
		_, release, rerr := p.Replace(testContext(t), dead)
		if rerr == nil {
			release()
		}
		replaceDone <- rerr
	}()

	// Wait until the in-flight Replace has vacated the dead worker's slot (a nil
	// entry in p.workers) and is blocked dialing the replacement.
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		vacant := false
		for _, w := range p.workers {
			if w == nil {
				vacant = true
			}
		}
		p.mu.Unlock()
		if vacant {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker slot never vacated by the in-flight Replace")
		}
		time.Sleep(time.Millisecond)
	}

	if _, _, err := p.Replace(testContext(t), nil); err == nil ||
		!strings.Contains(err.Error(), "nil worker session") {
		t.Fatalf("Replace(nil) err = %v, want immediate nil-session rejection", err)
	}

	// The in-flight Replace completes normally once unblocked and capacity is intact.
	close(gate)
	if err := <-replaceDone; err != nil {
		t.Fatalf("in-flight Replace: %v", err)
	}
	var releases []func()
	for i := 0; i < p.NumWorkers(); i++ {
		sess, rel, err := p.Acquire(testContext(t))
		if err != nil {
			t.Fatalf("Acquire #%d after nil Replace: %v", i, err)
		}
		if got := roundTrip(t, sess, "n"); got != "n" {
			t.Errorf("worker echo = %q, want n", got)
		}
		releases = append(releases, rel)
	}
	for _, r := range releases {
		r()
	}
}

// TestPoolReplaceEvent confirms the event connection is re-established and the new
// session is the one Event reports.
func TestPoolReplaceEvent(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	old := p.Event()
	fresh, err := p.ReplaceEvent(testContext(t))
	if err != nil {
		t.Fatalf("ReplaceEvent: %v", err)
	}
	if fresh == old {
		t.Fatal("ReplaceEvent returned the old event session")
	}
	if p.Event() != fresh {
		t.Error("Event() does not report the replacement session")
	}
	if got := roundTrip(t, fresh, "ev"); got != "ev" {
		t.Errorf("replaced event echo = %q, want ev", got)
	}
}

// TestPoolReplaceVanishedPauses confirms Replace and ReplaceEvent surface the
// pause signal when the Connector reports a vanished session.
func TestPoolReplaceVanishedPauses(t *testing.T) {
	s := newMockServer(t)
	real := poolConnector(s)
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		if calls <= 3 { // event conn + two workers come up against the mock
			return real(ctx)
		}
		return nil, fmt.Errorf("reuse: %w", auth.ErrSessionVanished)
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, _, err := p.Replace(testContext(t), dead); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Replace err = %v, want ErrPoolPaused", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after vanished Replace")
	}
	// Subsequent Replace/ReplaceEvent fail fast with the pause signal.
	if _, _, err := p.Replace(testContext(t), dead); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Replace while paused err = %v, want ErrPoolPaused", err)
	}
	if _, err := p.ReplaceEvent(testContext(t)); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("ReplaceEvent while paused err = %v, want ErrPoolPaused", err)
	}
}

// TestPoolReplaceAfterClose confirms Replace and ReplaceEvent reject a closed pool.
func TestPoolReplaceAfterClose(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := p.Replace(testContext(t), dead); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Replace after Close err = %v, want ErrPoolClosed", err)
	}
	if _, err := p.ReplaceEvent(testContext(t)); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("ReplaceEvent after Close err = %v, want ErrPoolClosed", err)
	}
}

// TestPoolReleaseAfterClose confirms releasing a leased session after Close does
// not panic and drops the connection.
func TestPoolReleaseAfterClose(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(poolConnector(s), 2)
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, release, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	release() // must not panic or block
}

// TestPoolReleaseRacesClose stresses the realistic shutdown window: worker turns
// acquire/release concurrently with a Close. A release that observes the pool as
// open but then loses the race to Close must not panic with "send on closed
// channel". Run under -race, it pins down the makeRelease/Close serialisation.
func TestPoolReleaseRacesClose(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		s := newMockServer(t)
		p := NewPool(poolConnector(s), 4)
		if err := p.Start(testContext(t)); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					sess, release, err := p.Acquire(context.Background())
					if err != nil {
						// Pool closed: stop. Acquire returns ErrPoolClosed once Close
						// has run.
						return
					}
					_ = sess
					release() // must never panic, even as Close races us
				}
			}()
		}

		// Let the workers churn, then close underneath them.
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		wg.Wait()
	}
}

// vanishAfter wraps base so the first ok dials succeed and every later dial
// reports a vanished session, letting a test drive a started pool into the
// paused state through the normal reconnect path.
func vanishAfter(base Connector, ok int) Connector {
	calls := 0
	return func(ctx context.Context) (*Session, error) {
		calls++
		if calls > ok {
			return nil, fmt.Errorf("reuse: %w", auth.ErrSessionVanished)
		}
		return base(ctx)
	}
}

// TestPoolPausedRejectsNewWork confirms that once a vanished session pauses the
// pool, Event yields nil, Acquire fails fast with ErrPoolPaused, and a worker
// released after the pause is dropped rather than re-leased.
func TestPoolPausedRejectsNewWork(t *testing.T) {
	s := newMockServer(t)
	// 1 event + 2 worker dials succeed; the Replace dial vanishes.
	p := NewPool(vanishAfter(poolConnector(s), 3), 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire dead: %v", err)
	}
	_, releaseHeld, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire held: %v", err)
	}

	if _, _, err := p.Replace(testContext(t), dead); !errors.Is(err, ErrPoolPaused) {
		t.Fatalf("Replace err = %v, want ErrPoolPaused", err)
	}
	if !p.Paused() {
		t.Fatal("pool not paused after vanished Replace")
	}

	// Releasing a worker held across the pause must not return it to the lease
	// pool.
	releaseHeld()

	if p.Event() != nil {
		t.Error("Event() != nil after pause")
	}
	if _, _, err := p.Acquire(testContext(t)); !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Acquire after pause: err = %v, want ErrPoolPaused", err)
	}
}

// TestPoolPauseUnblocksAcquire confirms a goroutine already blocked in Acquire is
// released with ErrPoolPaused when the pool pauses.
func TestPoolPauseUnblocksAcquire(t *testing.T) {
	s := newMockServer(t)
	p := NewPool(vanishAfter(poolConnector(s), 3), 2)
	p.newBackoff = zeroBackoff
	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()

	// Drain all workers so the next Acquire blocks; keep one to drive Replace.
	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire dead: %v", err)
	}
	for i := 1; i < p.NumWorkers(); i++ {
		if _, _, err := p.Acquire(testContext(t)); err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Acquire(context.Background())
		done <- err
	}()
	// Give the goroutine a moment to block on the empty channel.
	time.Sleep(20 * time.Millisecond)

	if _, _, err := p.Replace(testContext(t), dead); !errors.Is(err, ErrPoolPaused) {
		t.Fatalf("Replace err = %v, want ErrPoolPaused", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolPaused) {
			t.Errorf("blocked Acquire after pause: err = %v, want ErrPoolPaused", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not unblock after pause")
	}
}

// TestPoolStartSessionVanishedPauses confirms a vanished session during Start is
// the same pause signal as a mid-run reconnect: Start reports ErrPoolPaused
// (wrapping the auth sentinel) and the pool is left paused.
func TestPoolStartSessionVanishedPauses(t *testing.T) {
	connect := func(ctx context.Context) (*Session, error) {
		return nil, fmt.Errorf("reuse: %w", auth.ErrSessionVanished)
	}
	p := NewPool(connect, 2)

	err := p.Start(testContext(t))
	if !errors.Is(err, ErrPoolPaused) {
		t.Errorf("Start err = %v, want ErrPoolPaused", err)
	}
	if !errors.Is(err, auth.ErrSessionVanished) {
		t.Errorf("Start err = %v, want it to wrap auth.ErrSessionVanished", err)
	}
	if !p.Paused() {
		t.Error("pool not paused after a vanished Start")
	}
}

// TestPoolCloseInterruptsReconnect confirms Close cancels a reconnect that is
// blocked in a slow dial, rather than leaving shutdown to wait on it.
func TestPoolCloseInterruptsReconnect(t *testing.T) {
	connect := func(ctx context.Context) (*Session, error) {
		// Block until the (pool-derived) dial context is cancelled.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p := NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	done := make(chan error, 1)
	go func() {
		_, err := p.reconnect(context.Background())
		done <- err
	}()
	// Let the reconnect block in the dial.
	time.Sleep(20 * time.Millisecond)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolClosed) {
			t.Errorf("reconnect after Close: err = %v, want ErrPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not interrupt the blocked reconnect")
	}
}

// TestPoolReconnectLogsPerStreak pins the streak discipline:
// a reconnect loop that fails several attempts before succeeding logs exactly
// one Warn (the streak opening) and one re-established Info (the closing
// bookend) - never one line per backoff attempt, which would flood the daemon
// log during an hours-long outage.
func TestPoolReconnectLogsPerStreak(t *testing.T) {
	const fails = 4
	calls := 0
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		if calls <= fails {
			return nil, errors.New("connection refused")
		}
		return &Session{}, nil
	}
	var buf bytes.Buffer
	p := NewPoolWithLogger(connect, 2, slog.New(slog.NewTextHandler(&buf, nil)))
	p.newBackoff = zeroBackoff

	if _, err := p.reconnect(testContext(t)); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	out := buf.String()
	if got := strings.Count(out, "re-dialing under backoff"); got != 1 {
		t.Errorf("streak Warn count = %d, want exactly 1 (per-streak, not per-attempt):\n%s", got, out)
	}
	if got := strings.Count(out, "connection re-established"); got != 1 {
		t.Errorf("re-established log count = %d, want 1:\n%s", got, out)
	}
}

// TestPoolReconnectCleanSuccessSilent confirms a re-dial that succeeds on the
// first attempt - the routine event-connection replacement after every long-poll
// deadline - logs nothing: the streak bookends fire only around actual failures.
func TestPoolReconnectCleanSuccessSilent(t *testing.T) {
	connect := func(ctx context.Context) (*Session, error) { return &Session{}, nil }
	var buf bytes.Buffer
	p := NewPoolWithLogger(connect, 2, slog.New(slog.NewTextHandler(&buf, nil)))
	p.newBackoff = zeroBackoff

	if _, err := p.reconnect(testContext(t)); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("clean re-dial logged diagnostics:\n%s", buf.String())
	}
}

// TestPoolReconnectingFlagLifecycle confirms Reconnecting is true for the whole
// in-flight re-dial loop - including while an attempt is still dialing, before
// any failure - and false again once the loop returns (the
// status surface reads this to render "reconnecting..." during a mid-run
// outage).
func TestPoolReconnectingFlagLifecycle(t *testing.T) {
	const fails = 2
	calls := 0
	sawReconnecting := true
	var p *Pool
	connect := func(ctx context.Context) (*Session, error) {
		calls++
		sawReconnecting = sawReconnecting && p.Reconnecting()
		if calls <= fails {
			return nil, errors.New("connection refused")
		}
		return &Session{}, nil
	}
	p = NewPool(connect, 2)
	p.newBackoff = zeroBackoff

	if p.Reconnecting() {
		t.Error("Reconnecting() = true before any reconnect")
	}
	if _, err := p.reconnect(testContext(t)); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if !sawReconnecting {
		t.Error("Reconnecting() = false during an in-flight re-dial attempt")
	}
	if p.Reconnecting() {
		t.Error("Reconnecting() = true after the reconnect loop returned")
	}
}

// TestPoolReplaceLogsAndReportsReconnecting exercises the reconnect-diagnostics
// surface end-to-end at the pool layer: a worker dropped mid-run and replaced
// through a transiently failing Connector reports Reconnecting for the re-dial
// window, logs the streak bookends exactly once each, and returns to a silent,
// non-reconnecting state once the replacement lands. Bring-up (Start) logs
// nothing and never reports Reconnecting.
func TestPoolReplaceLogsAndReportsReconnecting(t *testing.T) {
	s := newMockServer(t)
	base := poolConnector(s)

	var mu sync.Mutex
	failNext := 0
	sawReconnecting := false
	var p *Pool
	connect := func(ctx context.Context) (*Session, error) {
		mu.Lock()
		if p.Reconnecting() {
			sawReconnecting = true
		}
		if failNext > 0 {
			failNext--
			mu.Unlock()
			return nil, errors.New("connection refused")
		}
		mu.Unlock()
		return base(ctx)
	}

	var buf bytes.Buffer
	p = NewPoolWithLogger(connect, 2, slog.New(slog.NewTextHandler(&buf, nil)))
	p.newBackoff = zeroBackoff

	if err := p.Start(testContext(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Close()
	if sawReconnecting {
		t.Error("Reconnecting() = true during bring-up (Start is the Connecting window, not a re-dial)")
	}
	if buf.Len() != 0 {
		t.Errorf("bring-up logged reconnect diagnostics:\n%s", buf.String())
	}

	dead, _, err := p.Acquire(testContext(t))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	mu.Lock()
	failNext = 3
	mu.Unlock()

	fresh, release, err := p.Replace(testContext(t), dead)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if got := roundTrip(t, fresh, "back"); got != "back" {
		t.Errorf("replacement echo = %q, want back", got)
	}
	release()

	if !sawReconnecting {
		t.Error("Reconnecting() never observed true during the Replace re-dial")
	}
	if p.Reconnecting() {
		t.Error("Reconnecting() = true after Replace completed")
	}
	out := buf.String()
	if got := strings.Count(out, "re-dialing under backoff"); got != 1 {
		t.Errorf("streak Warn count = %d, want exactly 1:\n%s", got, out)
	}
	if got := strings.Count(out, "connection re-established"); got != 1 {
		t.Errorf("re-established log count = %d, want 1:\n%s", got, out)
	}
}
