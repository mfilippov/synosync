package engine

// Engine-level half of the download-stall fix. The action layer now
// bounds a download that stops delivering bytes with an idle-progress budget and
// fails it as the transient actions.ErrTransferStalled (internal/actions/idle.go).
// What this file pins is the consequence that matters: a stalled
// download must not freeze the sync cycle. Before the budget the download goroutine
// never returned, so drainBatch's wg.Wait never joined - drainQueue, and with it
// cycle(), never reached pollOnce, while the stuck-op counter stayed at zero because
// nothing ever classified the op as failed. The daemon looked healthy with a frozen
// queue and only a restart cured it.
//
// The fake below models the fixed action layer: the download blocks for a bounded
// budget and then returns the typed stall with the connection poisoned. The
// assertions are the engine's side of the contract - transient (op stays queued, not
// dead-lettered), observable (StuckOps escalates), and non-blocking (the cycle
// reaches the poll).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// stallingDownloadClient models the action layer's idle-budget behaviour: Download
// blocks for stall (a server that went silent mid-body) and then fails the turn with
// actions.ErrTransferStalled, leaving the connection broken so the pool re-dials the
// worker. Setting heal makes it deliver the content instead.
type stallingDownloadClient struct {
	mu        sync.Mutex
	stall     time.Duration
	heal      bool
	broken    bool
	calls     int
	longPolls int
	content   []byte
	node      actions.Node
}

func (c *stallingDownloadClient) setHeal(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heal = v
	c.broken = false
}

func (c *stallingDownloadClient) downloadCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *stallingDownloadClient) pollCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.longPolls
}

func (c *stallingDownloadClient) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken
}

func (c *stallingDownloadClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return nil, nil
}

func (c *stallingDownloadClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *stallingDownloadClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	c.mu.Lock()
	c.longPolls++
	c.mu.Unlock()
	return nil, nil
}

func (c *stallingDownloadClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}

func (c *stallingDownloadClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.calls++
	stall, heal := c.stall, c.heal
	c.mu.Unlock()
	if heal {
		if _, err := w.Write(c.content); err != nil {
			return actions.Node{}, err
		}
		return c.node, nil
	}
	// The peer is silent: the idle budget - not the caller's context, which carries
	// no per-turn deadline for downloads - is what ends the turn.
	time.Sleep(stall)
	c.mu.Lock()
	c.broken = true
	c.mu.Unlock()
	return actions.Node{}, fmt.Errorf("actions: read download reply: no bytes for %s: %w",
		stall, actions.ErrTransferStalled)
}

// TestStalledDownloadStaysTransientAndDoesNotWedgeTheQueue drives the drain path: a
// download that stalls fails inside a bounded time, the op stays queued (never
// dead-lettered), the broken worker is re-dialed, the stuck counter escalates and
// the queue converges once the server recovers.
func TestStalledDownloadStaysTransientAndDoesNotWedgeTheQueue(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	cli := &stallingDownloadClient{
		stall:   20 * time.Millisecond,
		content: []byte("hello"),
		node:    actions.Node{Hash: "h1", MTime: 1700000000, Size: 5},
	}
	pool := &fakePool{eventSess: &transport.Session{}}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }
	e := New(st, pt, t.TempDir(), "sess", nil)

	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	start := time.Now()
	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue succeeded, want the stalled download to fail the op")
	}
	if !errors.Is(err, actions.ErrTransferStalled) {
		t.Fatalf("drainQueue err = %v, want actions.ErrTransferStalled", err)
	}
	if isTerminal(err) {
		t.Fatalf("a stalled download was classified terminal (would stop Run): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("drainQueue took %s - the stalled download wedged the drain", elapsed)
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen after the stall = %d (%v), want 1 (op stays queued)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (%v), want 0 (a stall is transient)", n, err)
	}
	if pool.replaced != 1 {
		t.Errorf("pool.Replace calls = %d, want 1 (the poisoned worker is re-dialed)", pool.replaced)
	}

	// Repeated stalls escalate into the observable stuck-op counter - the signal that
	// stayed at zero while the drain was frozen.
	for i := 1; i < stuckThreshold; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d succeeded, want the injected stall", i+1)
		}
	}
	if got := e.StuckOps(); got != 1 {
		t.Fatalf("StuckOps after %d stalled drains = %d, want 1", stuckThreshold, got)
	}

	// The server recovers: the op applies and the counter clears - the queue was
	// never wedged, only delayed.
	cli.setHeal(true)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after recovery: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen after recovery = %d (%v), want 0", n, err)
	}
	if got := e.StuckOps(); got != 0 {
		t.Errorf("StuckOps after convergence = %d, want 0", got)
	}
}

// TestCycleReachesPollDespiteStalledDownload is the production-path half: with the
// stalled download bounded, cycle() gets past its leading drain to pollOnce. Before
// the idle budget the drain never returned, so the long poll (and every other stage
// behind it - scans, cold start, syncView backlogs) never ran again.
func TestCycleReachesPollDespiteStalledDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{
		Name: "Photos", Privilege: "rw", NodeID: "root7", InitialSyncDone: true,
	}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	cli := &stallingDownloadClient{stall: 20 * time.Millisecond}
	pool := &fakePool{eventSess: &transport.Session{}}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }
	e := New(st, pt, t.TempDir(), "sess", nil)
	// The scan and the view re-enumeration are not the paths under test (the view's
	// on-disk directory does not exist in this fixture).
	e.lastScan = e.now()
	e.lastViewRefresh = e.now()

	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.cycle(context.Background()); err != nil {
		t.Fatalf("cycle with a stalled download = %v, want nil (transient drain failure, poll continues)", err)
	}
	if got := cli.downloadCalls(); got != 1 {
		t.Errorf("Download calls = %d, want 1", got)
	}
	if got := cli.pollCalls(); got != 1 {
		t.Fatalf("LongPoll calls = %d, want 1 - the cycle never reached pollOnce", got)
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen after cycle = %d (%v), want 1 (op stays queued)", n, err)
	}
}
