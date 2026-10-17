package engine

// Tests for the per-turn deadline: limits.OpTurnTimeout bounds
// each bounded request/reply turn on a leased worker connection so a silent or
// protocol-violating server fails the op transiently instead of hanging the op,
// its worker and the conflict chain behind it until shutdown. Before the fix the
// discriminating drain test below hangs forever (nothing ever cancels the turn);
// with it the op fails transiently, the broken worker is re-dialed, and the queue
// converges on the next drain.
//
// The turn's own deadline expiry surfaces as the typed transient
// errTurnTimeout, NOT context.DeadlineExceeded - isTerminal read the raw context
// sentinel as fatal, so with the knob enabled the first slow turn crashed the
// daemon (drainBatch → cycle → Run → daemon.Run exits). The production-path tests
// below (cycle, Run, syncView) pin that the timeout retries instead.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// silentServerClient is a full-featured worker client that models a silent server:
// a method whose hang flag is set never replies, returning only when the caller's
// context is cancelled - exactly the wedge a per-turn deadline must convert into a
// transient failure. The aborted mid-turn read marks the connection broken, like
// the production client's poisoning discipline.
type silentServerClient struct {
	mu                 sync.Mutex
	hang               bool // Remove hangs
	hangQueryUserInfo  bool
	hangPullEvent      bool
	broken             bool
	removeCalls        int
	queryUserInfoCalls int
}

func (c *silentServerClient) setHang(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hang = v
	c.broken = false
}

func (c *silentServerClient) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken
}

// hangTurn blocks until ctx is cancelled, marks the connection broken (the aborted
// turn left the socket byte-misaligned) and returns the context's error, mirroring
// the production client's netctx mapping + poisoning discipline.
func (c *silentServerClient) hangTurn(ctx context.Context) error {
	<-ctx.Done()
	c.mu.Lock()
	c.broken = true
	c.mu.Unlock()
	return ctx.Err()
}

func (c *silentServerClient) Remove(ctx context.Context, _ int64, _ string) error {
	c.mu.Lock()
	c.removeCalls++
	hang := c.hang
	c.mu.Unlock()
	if !hang {
		return nil
	}
	return c.hangTurn(ctx)
}

func (c *silentServerClient) QueryUserInfo(ctx context.Context) ([]actions.View, error) {
	c.mu.Lock()
	c.queryUserInfoCalls++
	hang := c.hangQueryUserInfo
	c.mu.Unlock()
	if !hang {
		return nil, nil
	}
	return nil, c.hangTurn(ctx)
}

func (c *silentServerClient) queryCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queryUserInfoCalls
}

func (c *silentServerClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *silentServerClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}

func (c *silentServerClient) PullEvent(ctx context.Context, _, _, _ int64) ([]actions.Event, int64, error) {
	c.mu.Lock()
	hang := c.hangPullEvent
	c.mu.Unlock()
	if !hang {
		return nil, 0, nil
	}
	return nil, 0, c.hangTurn(ctx)
}

func (c *silentServerClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *silentServerClient) Upload(context.Context, string, string, int64, int64, []byte) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *silentServerClient) Mkdir(context.Context, string, string, int64, int64) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *silentServerClient) Rename(context.Context, int64, int64, string, string) error { return nil }

func (c *silentServerClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, false, nil
}

func (c *silentServerClient) ResumeGetToken(context.Context, actions.ResumeRequest) (actions.ResumeToken, error) {
	return actions.ResumeToken{}, nil
}

func (c *silentServerClient) ResumeUpload(context.Context, string, actions.ResumeUploadSpec, io.Reader) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *silentServerClient) ResumeDownload(context.Context, string, string, int64, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// TestDrainQueueSilentServerTurnTimeout is the discriminating test: a
// queued outbound remove drains against a server that never replies. With the
// per-turn deadline the apply fails transiently within the timeout (before the fix
// this drain blocks forever and the whole loop with it), the mid-turn abort marks
// the worker broken so the pool re-dials it (the worker is re-elected), and the op
// stays queued - once the server recovers, the next drain converges and empties
// the queue.
func TestDrainQueueSilentServerTurnTimeout(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "v", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound,
		ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cli := &silentServerClient{}
	cli.setHang(true)
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }

	e := New(st, pt, t.TempDir(), "sess", nil)
	e.turnTimeout = 30 * time.Millisecond

	// First drain: the silent turn must fail by deadline, not hang - surfaced as the
	// typed transient errTurnTimeout. The raw context sentinel must NOT leak upward:
	// isTerminal reads it as fatal, which is exactly the daemon crash this guards.
	err := e.drainQueue(context.Background())
	if !errors.Is(err, errTurnTimeout) {
		t.Fatalf("drainQueue against a silent server = %v, want errTurnTimeout", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("turn timeout leaked context.DeadlineExceeded up the chain: %v", err)
	}
	if isTerminal(err) {
		t.Fatalf("turn timeout classified terminal (would stop Run): %v", err)
	}
	if cli.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", cli.removeCalls)
	}
	// The aborted turn marked the connection broken: WithWorker must have re-dialed
	// the worker instead of returning the dead socket to the pool.
	if pool.replaced != 1 {
		t.Errorf("pool.Replace calls = %d, want 1 (broken worker re-elected)", pool.replaced)
	}
	// The failure is transient: the op stays queued, nothing was dead-lettered.
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen after timed-out drain = %d (%v), want 1", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (%v), want 0 (timeout is transient)", n, err)
	}

	// Server recovers: the next drain applies the op and the queue empties - the
	// queue was never wedged.
	cli.setHang(false)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after recovery: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen after recovery = %d (%v), want 0", n, err)
	}
	if cli.removeCalls != 2 {
		t.Errorf("remove calls = %d, want 2 (one timed out, one applied)", cli.removeCalls)
	}
	if got := e.StuckOps(); got != 0 {
		t.Errorf("StuckOps after convergence = %d, want 0", got)
	}
}

// TestCycleTurnTimeoutStaysTransient is the production-path
// reproduction: the same silent-server drain as above, but driven through cycle()
// - the path Run classifies. Before the fix drainQueue's context.DeadlineExceeded
// hit cycle's isTerminal check and cycle returned it, so Run (and with it
// daemon.Run, i.e. the whole process) exited on the first slow turn; under systemd
// Restart=on-failure that is a crash-loop for as long as the server stays slow.
// With the fix the drain failure is transient: cycle logs it, polls on, and
// returns nil; the op stays queued and the broken worker was re-dialed.
func TestCycleTurnTimeoutStaysTransient(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{
		Name: "v", Privilege: "rw", NodeID: "root7", InitialSyncDone: true,
	}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound,
		ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cli := &silentServerClient{}
	cli.setHang(true)
	pool := &fakePool{eventSess: &transport.Session{}}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }

	e := New(st, pt, t.TempDir(), "sess", nil)
	e.turnTimeout = 30 * time.Millisecond
	// Throttle the local scan: the drain and the poll are the paths under test, and
	// the view's on-disk directory does not exist in this fixture. Throttle the
	// steady-state view re-enumeration the same way - against
	// this silent server its QueryUserInfo turn would also time out and re-elect
	// the worker a second time, which is not the path under test.
	e.lastScan = e.now()
	e.lastViewRefresh = e.now()

	if err := e.cycle(context.Background()); err != nil {
		t.Fatalf("cycle with a timed-out turn = %v, want nil (transient drain failure, poll continues)", err)
	}
	if pool.replaced != 1 {
		t.Errorf("pool.Replace calls = %d, want 1 (broken worker re-elected)", pool.replaced)
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen after cycle = %d (%v), want 1 (op stays queued)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (%v), want 0 (timeout is transient)", n, err)
	}
}

// TestRunSurvivesStartupRefreshTurnTimeout pins the no-op-needed variant of the
// deadline-crash regression: the very first QueryUserInfo of Run's startup refreshViews
// times out. Before the fix Run returned the DeadlineExceeded straight from the
// isTerminal check after refreshViews (one QueryUserInfo call, daemon dead); with
// it the refresh failure is warned, the loop enters its cycle and pollOnce's
// no-views branch retries refreshViews - the second call proves Run survived. The
// only terminal condition left is our own cancel.
func TestRunSurvivesStartupRefreshTurnTimeout(t *testing.T) {
	st := newTestStore(t)
	cli := &silentServerClient{hangQueryUserInfo: true}
	pool := &fakePool{eventSess: &transport.Session{}}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }

	e := New(st, pt, t.TempDir(), "sess", nil)
	e.turnTimeout = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(ctx) }()

	deadline := time.After(10 * time.Second)
	for cli.queryCalls() < 2 {
		select {
		case err := <-errCh:
			t.Fatalf("Run = %v after %d QueryUserInfo call(s): the turn timeout was read as terminal", err, cli.queryCalls())
		case <-deadline:
			t.Fatalf("Run never retried the view refresh after a turn timeout (calls=%d)", cli.queryCalls())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled (our cancel, the only terminal condition here)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop on ctx cancel")
	}
}

// TestSyncViewPullEventTurnTimeoutStaysTransient covers the third production
// entry: a PullEvent turn inside syncView's pagination lease times out. The error
// propagates (the page was not pulled) but must classify transient - pollOnce
// returns it, cycle returns it, and Run's isTerminal check decides retry-vs-exit,
// so a terminal read here is the same daemon crash as the drain path.
func TestSyncViewPullEventTurnTimeoutStaysTransient(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{
		Name: "v", Privilege: "rw", NodeID: "root7", InitialSyncDone: true,
	}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	cli := &silentServerClient{hangPullEvent: true}
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }

	e := New(st, pt, t.TempDir(), "sess", nil)
	e.turnTimeout = 30 * time.Millisecond

	err := e.syncView(context.Background(), 7)
	if err == nil {
		t.Fatal("syncView against a silent PullEvent succeeded, want error")
	}
	if !errors.Is(err, errTurnTimeout) {
		t.Fatalf("syncView = %v, want errTurnTimeout", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("turn timeout leaked context.DeadlineExceeded up the chain: %v", err)
	}
	if isTerminal(err) {
		t.Fatalf("PullEvent turn timeout classified terminal (would stop Run): %v", err)
	}
	if pool.replaced != 1 {
		t.Errorf("pool.Replace calls = %d, want 1 (broken worker re-elected)", pool.replaced)
	}
}

// TestTurnTimeoutClassification pins mapTurnTimeout's three-way split at the
// decorator: the turn's OWN deadline (parent live) becomes the typed transient
// errTurnTimeout, while a parent deadline and a parent cancel pass through as the
// raw context sentinels - those really are the run context dying, and must stay
// terminal for the loop.
func TestTurnTimeoutClassification(t *testing.T) {
	cli := &silentServerClient{}
	cli.setHang(true)

	// Own turn deadline, parent live → typed transient.
	tc := &turnLimitedClient{inner: cli, timeout: 20 * time.Millisecond}
	err := tc.Remove(context.Background(), 1, "a")
	if !errors.Is(err, errTurnTimeout) {
		t.Fatalf("own-deadline turn = %v, want errTurnTimeout", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("own-deadline turn leaked context.DeadlineExceeded: %v", err)
	}
	if isTerminal(err) {
		t.Fatalf("own-deadline turn classified terminal: %v", err)
	}
	if !tc.Broken() {
		t.Error("aborted turn did not mark the connection broken")
	}

	// Parent's own deadline → the raw sentinel passes through, still terminal.
	cli.setHang(true)
	slow := &turnLimitedClient{inner: cli, timeout: time.Hour}
	pctx, pcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer pcancel()
	err = slow.Remove(pctx, 1, "a")
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errTurnTimeout) {
		t.Fatalf("parent-deadline turn = %v, want raw context.DeadlineExceeded", err)
	}
	if !isTerminal(err) {
		t.Fatalf("parent-deadline turn must stay terminal: %v", err)
	}

	// Parent cancel → context.Canceled passes through, still terminal.
	cli.setHang(true)
	cctx, ccancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		ccancel()
	}()
	err = slow.Remove(cctx, 1, "a")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("parent-cancel turn = %v, want context.Canceled", err)
	}
	if !isTerminal(err) {
		t.Fatalf("parent-cancel turn must stay terminal: %v", err)
	}
}

// TestIsTerminalTurnTimeoutTransient pins the classifier itself: errTurnTimeout -
// bare or wrapped the way syncView/refreshViews wrap turn errors - is transient,
// while the genuine stop conditions (vanished session, closed pool, run-ctx
// cancellation/deadline) remain terminal.
func TestIsTerminalTurnTimeoutTransient(t *testing.T) {
	if isTerminal(errTurnTimeout) {
		t.Error("isTerminal(errTurnTimeout) = true, want false")
	}
	wrapped := fmt.Errorf("engine: pull events for view 7: %w",
		&turnTimeoutError{cause: context.DeadlineExceeded})
	if isTerminal(wrapped) {
		t.Errorf("isTerminal(wrapped turn timeout) = true, want false: %v", wrapped)
	}
	if !errors.Is(wrapped, errTurnTimeout) {
		t.Errorf("wrapped turn timeout does not match errTurnTimeout: %v", wrapped)
	}
	for _, err := range []error{
		transport.ErrPoolPaused,
		transport.ErrPoolClientRejected,
		transport.ErrPoolClosed,
		auth.ErrSessionVanished,
		context.Canceled,
		context.DeadlineExceeded,
	} {
		if !isTerminal(err) {
			t.Errorf("isTerminal(%v) = false, want true", err)
		}
	}
}

// deadlineRecordingClient records, per turn, whether the context it received
// carried a deadline, so the decorator's wrap-vs-passthrough split is pinned.
type deadlineRecordingClient struct {
	silentServerClient
	mu        sync.Mutex
	deadlines map[string]bool
}

func (c *deadlineRecordingClient) record(ctx context.Context, method string) {
	_, ok := ctx.Deadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deadlines == nil {
		c.deadlines = make(map[string]bool)
	}
	c.deadlines[method] = ok
}

func (c *deadlineRecordingClient) QueryUserInfo(ctx context.Context) ([]actions.View, error) {
	c.record(ctx, "QueryUserInfo")
	return nil, nil
}

func (c *deadlineRecordingClient) QueryNode(ctx context.Context, _ int64, _ string) (actions.Node, error) {
	c.record(ctx, "QueryNode")
	return actions.Node{}, nil
}

func (c *deadlineRecordingClient) LongPoll(ctx context.Context, _ []actions.Subscription, _ int) ([]int64, error) {
	c.record(ctx, "LongPoll")
	return nil, nil
}

func (c *deadlineRecordingClient) PullEvent(ctx context.Context, _, _, _ int64) ([]actions.Event, int64, error) {
	c.record(ctx, "PullEvent")
	return nil, 0, nil
}

func (c *deadlineRecordingClient) Download(ctx context.Context, _ string, _, _ int64, _ io.Writer) (actions.Node, error) {
	c.record(ctx, "Download")
	return actions.Node{}, nil
}

func (c *deadlineRecordingClient) Upload(ctx context.Context, _, _ string, _, _ int64, _ []byte) (actions.Node, error) {
	c.record(ctx, "Upload")
	return actions.Node{}, nil
}

func (c *deadlineRecordingClient) Mkdir(ctx context.Context, _, _ string, _, _ int64) (actions.Node, error) {
	c.record(ctx, "Mkdir")
	return actions.Node{}, nil
}

func (c *deadlineRecordingClient) Rename(ctx context.Context, _, _ int64, _, _ string) error {
	c.record(ctx, "Rename")
	return nil
}

func (c *deadlineRecordingClient) Remove(ctx context.Context, _ int64, _ string) error {
	c.record(ctx, "Remove")
	return nil
}

func (c *deadlineRecordingClient) List(ctx context.Context, _ int64, _ string) ([]actions.Node, bool, error) {
	c.record(ctx, "List")
	return nil, true, nil
}

func (c *deadlineRecordingClient) ResumeGetToken(ctx context.Context, _ actions.ResumeRequest) (actions.ResumeToken, error) {
	c.record(ctx, "ResumeGetToken")
	return actions.ResumeToken{}, nil
}

func (c *deadlineRecordingClient) ResumeUpload(ctx context.Context, _ string, spec actions.ResumeUploadSpec, body io.Reader) (actions.Node, error) {
	c.record(ctx, "ResumeUpload")
	return actions.Node{}, nil
}

func (c *deadlineRecordingClient) ResumeDownload(ctx context.Context, _, _ string, _, _, _ int64, _ io.Writer) (actions.Node, error) {
	c.record(ctx, "ResumeDownload")
	return actions.Node{}, nil
}

// TestTurnLimitedClientScopesDeadlines pins which turns the decorator bounds: every
// bounded (client-controlled size) turn gets a per-call deadline, while LongPoll
// (pollOnce owns its deadline) and the server-sized download streams pass through
// unbounded so a legitimately slow large transfer is never aborted by the knob.
func TestTurnLimitedClientScopesDeadlines(t *testing.T) {
	rec := &deadlineRecordingClient{}
	tc := &turnLimitedClient{inner: rec, timeout: time.Hour}
	ctx := context.Background()

	_, _ = tc.QueryUserInfo(ctx)
	_, _ = tc.QueryNode(ctx, 1, "a")
	_, _, _ = tc.PullEvent(ctx, 1, 0, 0)
	_, _ = tc.LongPoll(ctx, nil, 30)
	_, _ = tc.Download(ctx, "id", 0, 0, io.Discard)
	_, _ = tc.Upload(ctx, "p", "n", 0, 0, nil)
	_, _ = tc.Mkdir(ctx, "p", "n", 0, 0)
	_ = tc.Rename(ctx, 1, 1, "a", "b")
	_ = tc.Remove(ctx, 1, "a")
	_, _, _ = tc.List(ctx, 1, "a")
	_, _ = tc.ResumeGetToken(ctx, actions.ResumeRequest{})
	_, _ = tc.ResumeUpload(ctx, "tok", actions.ResumeUploadSpec{}, nil)
	_, _ = tc.ResumeDownload(ctx, "tok", "id", 1, 0, 0, io.Discard)

	wantDeadline := map[string]bool{
		"QueryUserInfo":  true,
		"QueryNode":      true,
		"PullEvent":      true,
		"Upload":         true,
		"Mkdir":          true,
		"Rename":         true,
		"Remove":         true,
		"List":           true,
		"ResumeGetToken": true,
		"ResumeUpload":   true,
		"LongPoll":       false,
		"Download":       false,
		"ResumeDownload": false,
	}
	for method, want := range wantDeadline {
		got, ok := rec.deadlines[method]
		if !ok {
			t.Errorf("%s was never forwarded to the inner client", method)
			continue
		}
		if got != want {
			t.Errorf("%s ctx deadline = %v, want %v", method, got, want)
		}
	}
}

// TestTurnLimitedClientForwardsOptionalInterfaces pins that the decorator preserves
// the assert-site semantics for a client without the optional interfaces: the
// outbound/resume methods yield the same errNoOutbound/errNoResumeDownload
// sentinels the bare type assertion would have produced, and Broken() reads false.
func TestTurnLimitedClientForwardsOptionalInterfaces(t *testing.T) {
	tc := &turnLimitedClient{inner: &stubClient{}, timeout: time.Hour}
	if _, err := tc.Upload(context.Background(), "p", "n", 0, 0, nil); !errors.Is(err, errNoOutbound) {
		t.Errorf("Upload on a plain client = %v, want errNoOutbound", err)
	}
	if _, err := tc.ResumeGetToken(context.Background(), actions.ResumeRequest{}); !errors.Is(err, errNoOutbound) {
		t.Errorf("ResumeGetToken on a plain client = %v, want errNoOutbound", err)
	}
	if _, err := tc.ResumeDownload(context.Background(), "t", "id", 1, 0, 0, io.Discard); !errors.Is(err, errNoResumeDownload) {
		t.Errorf("ResumeDownload on a plain client = %v, want errNoResumeDownload", err)
	}
	if tc.Broken() {
		t.Error("Broken() on a plain client = true, want false")
	}

	broken := &brokenStubClient{}
	tcb := &turnLimitedClient{inner: broken, timeout: time.Hour}
	if !tcb.Broken() {
		t.Error("Broken() must forward the inner client's report")
	}
}

// TestWithWorkerTimeoutWiring pins the seam: a zero timeout (the default) hands fn
// the leased client untouched, a non-zero one wraps it in the decorator, and
// SetLimits derives the engine's timeout from limits.OpTurnTimeout.
func TestWithWorkerTimeoutWiring(t *testing.T) {
	st := newTestStore(t)
	inner := &fakeClient{}
	ft := &fakeTransport{client: inner}
	e := New(st, ft, t.TempDir(), "sess", nil)

	if e.turnTimeout != 0 {
		t.Fatalf("default turnTimeout = %v, want 0 (disabled)", e.turnTimeout)
	}
	err := e.withWorker(context.Background(), func(c syncClient) error {
		if c != syncClient(inner) {
			t.Errorf("zero timeout: fn got %T, want the bare leased client", c)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withWorker: %v", err)
	}

	l := config.DefaultLimits()
	l.OpTurnTimeout = 7
	e.SetLimits(l)
	if e.turnTimeout != 7*time.Second {
		t.Fatalf("SetLimits turnTimeout = %v, want 7s", e.turnTimeout)
	}
	err = e.withWorker(context.Background(), func(c syncClient) error {
		tc, ok := c.(*turnLimitedClient)
		if !ok {
			t.Fatalf("non-zero timeout: fn got %T, want *turnLimitedClient", c)
		}
		if tc.inner != syncClient(inner) || tc.timeout != 7*time.Second {
			t.Errorf("decorator = {inner:%T timeout:%v}, want the leased client under 7s", tc.inner, tc.timeout)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withWorker: %v", err)
	}
}
