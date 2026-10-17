package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// loopClient is a scripted syncClient for the event loop: QueryUserInfo returns a
// fixed view list, LongPoll is driven by a per-call pollFn the test supplies (so it
// can return changed views, an error, or cancel the run), PullEvent returns preset
// events for a view, and Download writes preset bytes.
type loopClient struct {
	mu      sync.Mutex
	views   []actions.View
	calls   int
	pollFn  func(n int) ([]int64, error)
	events  map[uint64][]actions.Event
	content map[string][]byte
	nodes   map[string]actions.Node
	// failDownloads, when >0, makes that many leading Download calls fail transiently
	// before any succeed - to exercise a deferred/failed apply that must be retried.
	failDownloads int
}

func (c *loopClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return c.views, nil
}

func (c *loopClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}

func (c *loopClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	c.mu.Lock()
	n := c.calls
	c.calls++
	c.mu.Unlock()
	return c.pollFn(n)
}

// PullEvent paginates by cursor: it returns only the view's events with a SyncID past
// `since`, so a second call once the backlog has been consumed comes back empty (the
// terminating page syncView's pagination loop needs). Events at or below the cursor are
// already acknowledged and are not replayed.
func (c *loopClient) PullEvent(_ context.Context, viewID, _, since int64) ([]actions.Event, int64, error) {
	next := since
	var evs []actions.Event
	for _, e := range c.events[uint64(viewID)] {
		if e.SyncID > since {
			evs = append(evs, e)
			if e.SyncID > next {
				next = e.SyncID
			}
		}
	}
	return evs, next, nil
}

func (c *loopClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	if c.failDownloads > 0 {
		c.failDownloads--
		c.mu.Unlock()
		return actions.Node{}, errors.New("transient download failure")
	}
	c.mu.Unlock()
	if _, err := w.Write(c.content[fileID]); err != nil {
		return actions.Node{}, err
	}
	return c.nodes[fileID], nil
}

// noViewClient is a syncClient whose account has no views: QueryUserInfo always
// returns an empty list and cancels the run once it has been called twice, so a test
// can confirm the loop re-discovers views on a backoff timer instead of busy-looping.
type noViewClient struct {
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
}

func (c *noViewClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	if n >= 2 {
		c.cancel()
	}
	return nil, nil
}
func (c *noViewClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *noViewClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *noViewClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *noViewClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// TestPollOnceRecoversMissingRootID confirms pollOnce re-runs refreshViews when a known view
// still lacks its root node id, so a view whose enumeration omitted the id (and whose QueryNode
// fallback came back empty) recovers rather than wedging top-level creates on errParentUnknown
// forever - refreshViews is otherwise only re-run while the view list is empty.
func TestPollOnceRecoversMissingRootID(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	// A view persisted with an empty NodeID (the omitted-id degenerate case). It has already
	// completed its cold start, so pollOnce exercises the incremental subscribe path.
	if err := st.PutView(7, store.View{Name: "Photos", Privilege: "rw", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		// This enumeration supplies the root id, so the recovery refresh fills it in.
		views:  []actions.View{{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) { return nil, nil }, // a clean no-change poll
	}
	e := New(st, &concTransport{client: lc}, root, "sess", nil)

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.NodeID != "root7" {
		t.Errorf("NodeID = %q, want root7 recovered by the pollOnce refresh", v.NodeID)
	}
}

// TestRunAppliesRemoteEvents drives one full cycle: the loop refreshes views,
// long-polls (view 7 changed), pulls a create event, downloads the file to disk,
// advances the cursor, and stops when the next poll cancels the context.
func TestRunAppliesRemoteEvents(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	// The view has already completed its cold start, so this exercises the incremental path.
	if err := st.PutView(7, store.View{Name: "Photos", Privilege: "rw", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos", Privilege: "rw"}},
		events: map[uint64][]actions.Event{
			7: {{SyncID: 5, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h", MTime: 1700000000, Size: 3}},
		},
		content: map[string][]byte{"f1": []byte("abc")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", MTime: 1700000000, Size: 3}},
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n == 0 {
			return []int64{7}, nil
		}
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, root, "other-session", nil)

	err := e.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}

	got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt"))
	if err != nil {
		t.Fatalf("read applied file: %v", err)
	}
	if string(got) != "abc" {
		t.Errorf("content = %q, want %q", got, "abc")
	}

	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor = %d, want 5", v.LastSeenSyncID)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d remain", n)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced", b.State)
	}
}

// TestRunSkipsSelfEcho confirms a self-committed event advances the baseline and
// cursor without touching disk or the queue.
func TestRunSkipsSelfEcho(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
		events: map[uint64][]actions.Event{
			7: {{SyncID: 9, Action: "create", FileID: "f1", Path: "mine.txt", Hash: "h", CommitterSessionID: "self"}},
		},
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n == 0 {
			return []int64{7}, nil
		}
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, root, "self", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}

	if _, err := os.Stat(filepath.Join(root, "Photos", "mine.txt")); !os.IsNotExist(err) {
		t.Errorf("self-echo wrote to disk: err=%v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 9 {
		t.Errorf("cursor = %d, want 9", v.LastSeenSyncID)
	}
	b, err := st.GetBaseline(7, "mine.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("self-echo baseline state = %q, want synced", b.State)
	}
}

// TestRunStopsOnVanishedSession confirms a paused pool (wrapping the 8193 sentinel)
// terminates Run with that signal rather than being retried silently.
func TestRunStopsOnVanishedSession(t *testing.T) {
	st := newTestStore(t)
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
		pollFn: func(int) ([]int64, error) {
			return nil, fmt.Errorf("%w: %w", transport.ErrPoolPaused, auth.ErrSessionVanished)
		},
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)

	err := e.Run(context.Background())
	if !errors.Is(err, transport.ErrPoolPaused) {
		t.Fatalf("Run error = %v, want ErrPoolPaused", err)
	}
	if !errors.Is(err, auth.ErrSessionVanished) {
		t.Errorf("Run error = %v, want it to wrap ErrSessionVanished", err)
	}
}

// TestRunStopsOnClosedPool confirms a closed pool (ErrPoolClosed, surfaced here by the
// long poll) terminates Run rather than being retried. A closed pool never reopens, and
// once closed its Event() returns nil, so a retry that reached the next poll would build a
// client over the nil event session and panic - Run must return the error instead.
// The second poll cancels the run so a regression that retried would fail cleanly with
// context.Canceled rather than hanging.
func TestRunStopsOnClosedPool(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n > 0 {
			cancel()
			return nil, context.Canceled
		}
		return nil, transport.ErrPoolClosed
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)

	err := e.Run(ctx)
	if !errors.Is(err, transport.ErrPoolClosed) {
		t.Fatalf("Run error = %v, want ErrPoolClosed (a closed pool must be terminal, not retried)", err)
	}
}

// TestRunStopsWhenRefreshHitsClosedPool confirms a closed pool surfaced by the initial
// view refresh (Acquire → ErrPoolClosed) terminates Run, rather than being logged and
// retried. refreshViews leases a worker, so a closed pool reaches Run there before any
// poll. Falling through would re-enter the loop, where pollOnce builds a client over the
// pool's now-nil event session and panics. The 2s deadline bounds a regression
// that would otherwise spin on the no-view backoff forever.
func TestRunStopsWhenRefreshHitsClosedPool(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{}, acquireErr: transport.ErrPoolClosed}
	e := New(st, ft, t.TempDir(), "s", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.Run(ctx); !errors.Is(err, transport.ErrPoolClosed) {
		t.Fatalf("Run error = %v, want ErrPoolClosed (a closed pool from view refresh must be terminal)", err)
	}
}

// TestRunRetriesTransientError confirms a non-pause cycle error is logged and
// retried (not terminal): the first poll fails transiently, the loop pauses and
// polls a second time, and only that second poll cancels the run - so observing a
// second call proves the retry actually happened rather than the error being
// treated as terminal.
func TestRunRetriesTransientError(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n == 0 {
			return nil, errors.New("transient network blip")
		}
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled after retry", err)
	}
	lc.mu.Lock()
	calls := lc.calls
	lc.mu.Unlock()
	if calls < 2 {
		t.Fatalf("LongPoll calls = %d, want >= 2 (transient error must be retried)", calls)
	}
}

// TestRunDrainsQueueOnStartup confirms the loop drains the durable queue before
// polling, so ops left by a prior crash resume even when no new view event arrives:
// the queue is pre-seeded and the very first long-poll cancels the run, yet the
// pending download is still applied.
func TestRunDrainsQueueOnStartup(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views:   []actions.View{{ViewID: 7, Name: "Photos"}},
		content: map[string][]byte{"f1": []byte("resumed")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 7}},
	}
	lc.pollFn = func(int) ([]int64, error) {
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, root, "other", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt"))
	if err != nil {
		t.Fatalf("startup-queued op was not applied: %v", err)
	}
	if string(got) != "resumed" {
		t.Errorf("content = %q, want %q", got, "resumed")
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained on startup: %d remain", n)
	}
}

// TestRunAdvancesCursorWhenApplyDeferred confirms the cursor advances as soon as
// events are enqueued - even though the first apply fails - and the deferred op is
// then retried from the durable queue (by the next cycle's leading drain) rather than
// being re-pulled and re-enqueued. The first Download fails; the cursor still reaches
// 5 and the file is eventually written.
func TestRunAdvancesCursorWhenApplyDeferred(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
		events: map[uint64][]actions.Event{
			7: {{SyncID: 5, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h", MTime: 1700000000, Size: 3}},
		},
		content:       map[string][]byte{"f1": []byte("abc")},
		nodes:         map[string]actions.Node{"f1": {Hash: "h", MTime: 1700000000, Size: 3}},
		failDownloads: 1,
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n == 0 {
			return []int64{7}, nil
		}
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, root, "other", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor = %d, want 5 (advances once events are enqueued, not after apply)", v.LastSeenSyncID)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt"))
	if err != nil {
		t.Fatalf("deferred op was not retried from the queue: %v", err)
	}
	if string(got) != "abc" {
		t.Errorf("content = %q, want %q", got, "abc")
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained after retry: %d remain", n)
	}
}

// TestRunBacksOffWithNoViews confirms the loop does not busy-spin when the account
// has no views: it re-discovers views on a backoff timer and respects cancellation.
// The client cancels after its second QueryUserInfo, so a bounded call count proves
// the loop backed off rather than spinning on the empty view list.
func TestRunBacksOffWithNoViews(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := &noViewClient{cancel: cancel}
	e := New(st, &concTransport{client: c}, t.TempDir(), "s", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	c.mu.Lock()
	calls := c.calls
	c.mu.Unlock()
	if calls != 2 {
		t.Fatalf("QueryUserInfo calls = %d, want 2 (initial refresh + one idle re-discover, then backoff)", calls)
	}
}

// TestRunRetriesOnLongPollDeadline confirms that a client-side long-poll deadline
// (the parent run context is still live) is treated as a transient no-change tick and
// re-polled, not propagated as a terminal context.DeadlineExceeded that would kill the
// loop. The first poll returns DeadlineExceeded while the run context stays alive; only
// the second poll cancels - so observing a second call proves the deadline was retried.
func TestRunRetriesOnLongPollDeadline(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
	}
	lc.pollFn = func(n int) ([]int64, error) {
		if n == 0 {
			return nil, context.DeadlineExceeded
		}
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "s", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled (long-poll deadline must not be terminal)", err)
	}
	lc.mu.Lock()
	calls := lc.calls
	lc.mu.Unlock()
	if calls < 2 {
		t.Fatalf("LongPoll calls = %d, want >= 2 (a client-side deadline must be retried)", calls)
	}
}

// TestCyclePushesLocalChange confirms one loop cycle's local scan detects a brand-new
// on-disk file, the emitted DirOutbound OpUpload drains through the queue, and the file is
// pushed to the server - all without a remote event. A single cycle exercises the leading
// scan + drain; the fake long-poll returns no change.
func TestCyclePushesLocalChange(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "new.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	c := &pushClient{uploadNode: map[string]actions.Node{"new.txt": {FileID: "srv1", Hash: "h1", MTime: 99}}}
	e := New(st, &concTransport{client: c}, root, "sess", nil)

	if err := e.cycle(context.Background()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	c.mu.Lock()
	uploads := append([]uploadCall(nil), c.uploads...)
	c.mu.Unlock()
	if len(uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (the local scan must push the new file)", len(uploads))
	}
	if uploads[0].name != "new.txt" || uploads[0].parentID != "root1" || string(uploads[0].content) != "hello world" {
		t.Errorf("upload = %+v, want name=new.txt parent=root1 content=%q", uploads[0], "hello world")
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "srv1" {
		t.Errorf("baseline = %+v, want synced with FileID srv1", b)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d remain", n)
	}
}

// TestLocalScanThrottle confirms localScan re-walks a view at most once per scanInterval:
// a first scan enqueues an op for a new file, a second scan within the interval is
// suppressed (a second new file is NOT enqueued), and only after the clock advances past
// the interval does the third scan pick it up. A controllable clock drives the throttle
// deterministically.
func TestLocalScanThrottle(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &concTransport{client: &pushClient{}}, root, "sess", nil)
	cur := time.Unix(1700000000, 0)
	e.now = func() time.Time { return cur }
	e.scanInterval = time.Hour

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("aaa"), 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("first localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("after first scan PendingLen = %d, want 1", n)
	}

	// A second new file appears, but a scan within the interval must be throttled, so no
	// new op is enqueued for it.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("bbb"), 0o644); err != nil {
		t.Fatalf("seed b.txt: %v", err)
	}
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("throttled localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("after throttled scan PendingLen = %d, want 1 (scan must be throttled)", n)
	}

	// Past the interval the scan runs again and picks up b.txt.
	cur = cur.Add(2 * e.scanInterval)
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("third localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Fatalf("after interval elapsed PendingLen = %d, want 2 (throttle must release)", n)
	}
}

// TestLocalScanThrottleStampedFromCompletion confirms the throttle is stamped from the
// scan's COMPLETION, not its start: a scan that ran while the clock advanced records the
// latest now() value, so a scan taking longer than scanInterval is not immediately
// eligible again and re-walking views back-to-back is avoided.
func TestLocalScanThrottleStampedFromCompletion(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &concTransport{client: &pushClient{}}, t.TempDir(), "sess", nil)
	base := time.Unix(1700000000, 0)
	var returned []time.Time
	calls := 0
	e.now = func() time.Time {
		calls++
		ts := base.Add(time.Duration(calls) * time.Second)
		returned = append(returned, ts)
		return ts
	}
	// A non-zero lastScan in the past so the throttle check itself calls now() first, making
	// the start vs completion stamp distinguishable across the two (or more) now() calls.
	e.lastScan = base.Add(-time.Hour)
	e.scanInterval = time.Second

	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if len(returned) < 2 {
		t.Fatalf("now() called %d times, want ≥2 (throttle check + completion stamp)", len(returned))
	}
	last := returned[len(returned)-1]
	if !e.lastScan.Equal(last) {
		t.Errorf("lastScan = %v, want %v (the final now() value - stamped at completion)", e.lastScan, last)
	}
	if e.lastScan.Equal(returned[0]) {
		t.Errorf("lastScan = %v equals the throttle-check value; it must be stamped at completion, not start", e.lastScan)
	}
}

// TestLocalScanFailureDoesNotConsumeWindow confirms a transient failure before any view is
// scanned (here ListViews on a closed store) leaves lastScan unchanged, so the next cycle
// retries promptly rather than skipping the scan for a whole scanInterval.
func TestLocalScanFailureDoesNotConsumeWindow(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &concTransport{client: &pushClient{}}, t.TempDir(), "sess", nil)
	if err := e.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := e.localScan(context.Background()); err == nil {
		t.Fatal("localScan on a closed store returned nil error")
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (a failed scan must not consume the throttle window)", e.lastScan)
	}
}

// TestLocalScanSkipsReadOnlyView confirms localScan does not generate outbound ops for a
// view the account holds read-only ("ro"): pushing them would just hit unauthorized
// rejections that retry forever. A read-write view in the same scan still pushes
// its local change.
func TestLocalScanSkipsReadOnlyView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "ReadOnly", Privilege: "ro", NodeID: "ro1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView ro: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "Writable", Privilege: "rw", NodeID: "rw1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView rw: %v", err)
	}
	root := t.TempDir()
	roDir := filepath.Join(root, "ReadOnly")
	rwDir := filepath.Join(root, "Writable")
	for _, d := range []string{roDir, rwDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed view dir %s: %v", d, err)
		}
	}
	// A new local file in each view.
	if err := os.WriteFile(filepath.Join(roDir, "a.txt"), []byte("aaa"), 0o644); err != nil {
		t.Fatalf("seed ro a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rwDir, "b.txt"), []byte("bbb"), 0o644); err != nil {
		t.Fatalf("seed rw b.txt: %v", err)
	}
	e := New(st, &concTransport{client: &pushClient{}}, root, "sess", nil)

	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (only the writable view): %+v", len(ops), ops)
	}
	if ops[0].ViewID != 8 || ops[0].Path != "b.txt" {
		t.Errorf("op = %+v, want the writable view's b.txt; the read-only view must be skipped", ops[0].Op)
	}
}

// TestRunPollsDespiteScanFailure confirms a local-scan failure for a view (here a view
// whose name escapes its subdirectory, so scanView errors) is logged and does not abort
// the cycle: the loop still reaches LongPoll. A regression that propagated the scan error
// would never poll, so the cancelling first poll proves the failure was tolerated.
func TestRunPollsDespiteScanFailure(t *testing.T) {
	st := newTestStore(t)
	// The view has already completed cold start, so localScan (not coldStartView) is what hits
	// the escaping-name scan failure under test.
	if err := st.PutView(7, store.View{Name: "../escape", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "../escape"}},
	}
	lc.pollFn = func(int) ([]int64, error) {
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "sess", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled (a scan failure must not abort polling)", err)
	}
	lc.mu.Lock()
	calls := lc.calls
	lc.mu.Unlock()
	if calls < 1 {
		t.Fatalf("LongPoll calls = %d, want >= 1 (poll must proceed despite the scan failure)", calls)
	}
}

// brokenDeadlineClient is a syncClient whose long poll fails with a client-side deadline
// and then reports the connection broken (as *actions.Client does after a read the
// deadline interrupted). The first poll returns DeadlineExceeded; the second cancels the
// run so the test terminates.
type brokenDeadlineClient struct {
	mu     sync.Mutex
	calls  int
	cancel context.CancelFunc
}

func (c *brokenDeadlineClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return []actions.View{{ViewID: 7, Name: "Photos"}}, nil
}
func (c *brokenDeadlineClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *brokenDeadlineClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	c.mu.Lock()
	n := c.calls
	c.calls++
	c.mu.Unlock()
	if n == 0 {
		return nil, context.DeadlineExceeded
	}
	c.cancel()
	return nil, context.Canceled
}
func (c *brokenDeadlineClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *brokenDeadlineClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *brokenDeadlineClient) Broken() bool { return true }

// replaceCountingTransport hands a single client to the event path and counts how many
// times the loop re-dialed the event connection, so a deadline-driven re-dial is
// observable.
type replaceCountingTransport struct {
	client   syncClient
	mu       sync.Mutex
	replaces int
}

func (t *replaceCountingTransport) EventClient() (syncClient, error) { return t.client, nil }
func (t *replaceCountingTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	return fn(t.client)
}
func (t *replaceCountingTransport) ReplaceEvent(context.Context) error {
	t.mu.Lock()
	t.replaces++
	t.mu.Unlock()
	return nil
}

// TestRunReDialsEventConnOnLongPollDeadline covers the leading-drain fix's deadline path: a
// client-side long-poll deadline interrupts an in-flight read, leaving the event socket
// byte-misaligned (the client reports Broken). The loop must re-dial the event connection before
// the next poll rather than reuse the dead one - otherwise it would hang again or misread a late
// reply to the abandoned request. Observing a ReplaceEvent call proves the deadline branch
// re-dialed.
func TestRunReDialsEventConnOnLongPollDeadline(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := &brokenDeadlineClient{cancel: cancel}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Fatalf("ReplaceEvent calls = %d, want >= 1 (a deadline-broken event conn must be re-dialed)", replaces)
	}
}

// TestRunPollsDespiteFailingLeadingDrain covers the leading-drain fix: a queued op that fails every
// time (here a download of a node that is gone server-side) must not block the loop from
// polling. The queue is pre-seeded with a perpetually-failing download; the loop must
// still reach LongPoll (where the test cancels), proving the failing leading drain did
// not wedge polling. A regression would never call LongPoll, so the context timeout is a
// safety net that fails the test instead of hanging.
func TestRunPollsDespiteFailingLeadingDrain(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "ghost.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lc := &loopClient{
		views:         []actions.View{{ViewID: 7, Name: "Photos"}},
		failDownloads: 1 << 30, // never succeeds within the test
	}
	lc.pollFn = func(int) ([]int64, error) {
		cancel()
		return nil, context.Canceled
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "other", nil)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	lc.mu.Lock()
	calls := lc.calls
	lc.mu.Unlock()
	if calls < 1 {
		t.Fatalf("LongPoll calls = %d, want >= 1 (poll must proceed despite the failing leading drain)", calls)
	}
}

// blockingWakeClient is a syncClient whose long poll blocks until its context is
// cancelled, then reports the connection broken (as *actions.Client does after a read
// the cancel interrupted). It models the event connection the Phase-7 wake cancels.
type blockingWakeClient struct {
	mu     sync.Mutex
	broken bool
}

func (c *blockingWakeClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return []actions.View{{ViewID: 7, Name: "Photos"}}, nil
}
func (c *blockingWakeClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *blockingWakeClient) LongPoll(ctx context.Context, _ []actions.Subscription, _ int) ([]int64, error) {
	<-ctx.Done()
	c.mu.Lock()
	c.broken = true
	c.mu.Unlock()
	return nil, ctx.Err()
}
func (c *blockingWakeClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *blockingWakeClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *blockingWakeClient) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken
}

// TestPollOnceWakeInterruptsLongPoll covers wake interruption: a wake delivered while the loop is
// blocked in the long-poll cancels the poll (without being mistaken for a parent-ctx
// cancel), resets the scan throttle so the next localScan runs immediately, and re-dials
// the now byte-misaligned event connection. A pre-buffered wake makes the interrupt
// deterministic - the poll goroutine consumes it as soon as it starts.
func TestPollOnceWakeInterruptsLongPoll(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	wake := make(chan struct{}, 1)
	wake <- struct{}{} // a pending wake the poll consumes at once
	c := &blockingWakeClient{}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.SetWake(wake)
	// A non-zero lastScan so the forced reset is observable.
	e.lastScan = time.Unix(1700000000, 0)

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v (a wake interrupt must not be terminal)", err)
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (a wake must force the next localScan)", e.lastScan)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Errorf("ReplaceEvent calls = %d, want >= 1 (a wake-broken event conn must be re-dialed)", replaces)
	}
}

// TestPollOnceNoWakePreservesBehaviour confirms a nil wake channel leaves the poll path
// unchanged: a clean long-poll timeout (empty change set) is a no-op that neither resets
// the scan throttle nor re-dials the event connection.
func TestPollOnceNoWakePreservesBehaviour(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		views:  []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) { return nil, nil }, // a clean no-change poll
	}
	tr := &replaceCountingTransport{client: lc}
	e := New(st, tr, t.TempDir(), "s", nil)
	last := time.Unix(1700000000, 0)
	e.lastScan = last

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if !e.lastScan.Equal(last) {
		t.Errorf("lastScan = %v, want unchanged %v (no wake must not reset the throttle)", e.lastScan, last)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces != 0 {
		t.Errorf("ReplaceEvent calls = %d, want 0 (no wake, clean poll must not re-dial)", replaces)
	}
}

// TestPollOnceClosedWakeChannelFallsBack confirms that a closed wake channel - the watcher
// exiting on an internal fsnotify failure while Run's ctx is still live - does not spin the
// loop. A closed channel reads ready forever, so without the closed-channel guard every poll
// would instantly "wake" and force-scan + re-dial each cycle. The poll must instead clear the
// seam (e.wake = nil) so the loop falls back to the periodic scan: not a real local change,
// so the scan throttle is left untouched, and the cancelled in-flight read is re-dialed once.
func TestPollOnceClosedWakeChannelFallsBack(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	wake := make(chan struct{}, 1)
	close(wake) // the watcher exited and closed its wake channel
	c := &blockingWakeClient{}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.SetWake(wake)
	last := time.Unix(1700000000, 0)
	e.lastScan = last

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v (a closed wake channel must not be terminal)", err)
	}
	if e.wake != nil {
		t.Errorf("e.wake = %v, want nil (a closed wake channel must clear the seam)", e.wake)
	}
	if !e.lastScan.Equal(last) {
		t.Errorf("lastScan = %v, want unchanged %v (a closed channel is not a real wake)", e.lastScan, last)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Errorf("ReplaceEvent calls = %d, want >= 1 (the cancelled in-flight read must be re-dialed)", replaces)
	}
}

// TestRunWakeForcesScan drives the wake path end-to-end through Run: a pre-buffered wake
// interrupts the first long-poll, which re-dials the event connection and forces a scan;
// the run then continues to a second poll where the test cancels. Observing both a
// ReplaceEvent call and a second poll proves the wake was tolerated (not terminal) and the
// loop kept making progress.
func TestRunWakeForcesScan(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &runWakeClient{cancel: cancel}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.SetWake(wake)

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	c.mu.Lock()
	calls := c.calls
	c.mu.Unlock()
	if calls < 2 {
		t.Fatalf("LongPoll calls = %d, want >= 2 (a wake interrupt must not stop the loop)", calls)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Fatalf("ReplaceEvent calls = %d, want >= 1 (the wake-interrupted event conn must be re-dialed)", replaces)
	}
}

// TestRunStartsWatcherFactory covers watcher start-up: when a watcher factory is installed (and no
// wake channel was pre-set via SetWake), Run brings the watcher up on e.syncRoot, wires its
// channel into the wake seam, and stops it on exit by cancelling the context it handed the
// factory. A pre-buffered wake interrupts the first long-poll (re-dialing the event
// connection and forcing a scan); the loop then continues to a second poll where the test
// cancels - proving the factory channel drove the wake path end-to-end.
func TestRunStartsWatcherFactory(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &runWakeClient{cancel: cancel}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)

	var (
		gotRoot    string
		watcherCtx context.Context
		factoryRun int
	)
	e.SetWatcher(func(wctx context.Context, root string) (<-chan struct{}, error) {
		factoryRun++
		watcherCtx = wctx
		gotRoot = root
		wake := make(chan struct{}, 1)
		wake <- struct{}{}
		return wake, nil
	})

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if factoryRun != 1 {
		t.Fatalf("watcher factory invoked %d times, want exactly 1", factoryRun)
	}
	if gotRoot != e.syncRoot {
		t.Errorf("watcher root = %q, want %q (Run must start the watcher on its sync root)", gotRoot, e.syncRoot)
	}
	if watcherCtx == nil || watcherCtx.Err() == nil {
		t.Errorf("watcher context not cancelled on Run exit; the watcher must be stopped on exit")
	}
	c.mu.Lock()
	calls := c.calls
	c.mu.Unlock()
	if calls < 2 {
		t.Fatalf("LongPoll calls = %d, want >= 2 (the factory wake must interrupt and the loop continue)", calls)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces < 1 {
		t.Fatalf("ReplaceEvent calls = %d, want >= 1 (the wake-interrupted event conn must be re-dialed)", replaces)
	}
	if e.wake != nil {
		t.Errorf("e.wake = %v, want nil (Run must reset the factory-installed wake on exit so a closed channel is never reused)", e.wake)
	}
}

// TestRunWatcherFactoryErrorFallsBack confirms a watcher factory that fails at startup is
// non-fatal: Run logs and proceeds exactly as today (no wake seam), so the periodic scan
// remains the authoritative backstop. The first clean long-poll cancels the run.
func TestRunWatcherFactoryErrorFallsBack(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}},
	}
	lc.pollFn = func(int) ([]int64, error) {
		cancel()
		return nil, context.Canceled
	}
	tr := &replaceCountingTransport{client: lc}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.SetWatcher(func(context.Context, string) (<-chan struct{}, error) {
		return nil, errors.New("watcher start failed")
	})

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled (a watcher start failure must not be terminal)", err)
	}
	if e.wake != nil {
		t.Errorf("e.wake = %v, want nil (a failed factory must leave the wake seam unset)", e.wake)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces != 0 {
		t.Errorf("ReplaceEvent calls = %d, want 0 (no wake means a clean poll must not re-dial)", replaces)
	}
}

// TestRunSetWakeTakesPrecedenceOverFactory confirms a wake channel installed via SetWake
// suppresses the watcher factory: Run uses the pre-built channel and never invokes the
// factory, so a caller wiring a fake channel for tests is honoured exactly.
func TestRunSetWakeTakesPrecedenceOverFactory(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &runWakeClient{cancel: cancel}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, t.TempDir(), "s", nil)

	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	e.SetWake(wake)
	factoryRun := 0
	e.SetWatcher(func(context.Context, string) (<-chan struct{}, error) {
		factoryRun++
		return nil, nil
	})

	if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if factoryRun != 0 {
		t.Fatalf("watcher factory invoked %d times, want 0 (a pre-set wake must take precedence)", factoryRun)
	}
}

// runWakeClient blocks the first long-poll until the wake cancels its context (reporting
// the connection broken afterwards, as the real client does), then cancels the run on the
// second poll so the loop terminates.
type runWakeClient struct {
	mu     sync.Mutex
	calls  int
	broken bool
	cancel context.CancelFunc
}

func (c *runWakeClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}}, nil
}
func (c *runWakeClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *runWakeClient) LongPoll(ctx context.Context, _ []actions.Subscription, _ int) ([]int64, error) {
	c.mu.Lock()
	n := c.calls
	c.calls++
	c.mu.Unlock()
	if n == 0 {
		<-ctx.Done()
		c.mu.Lock()
		c.broken = true
		c.mu.Unlock()
		return nil, ctx.Err()
	}
	c.cancel()
	return nil, context.Canceled
}
func (c *runWakeClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *runWakeClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *runWakeClient) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken
}

// pagePullClient scripts PullEvent by the cursor it is invoked with, so syncView's
// backlog-pagination loop can be exercised across several pages without a network. Each
// page (keyed by the incoming cursor) carries its events and the next cursor the server
// reports; pullErrAt injects a per-cursor error so a test can prove the per-page cursor
// commit survives a mid-backlog failure. Download writes preset bytes for the inbound
// apply. pullCalls counts how many pull_event turns ran.
type pagePullClient struct {
	mu            sync.Mutex
	pages         map[int64]catalogPage
	pullErrAt     map[int64]error
	pullCalls     int
	downloadCalls int
	content       map[string][]byte
	nodes         map[string]actions.Node
	// tr, when set, lets each PullEvent record the lease generation it ran under so a
	// test can prove the whole pagination loop holds a single worker lease (a per-page
	// re-lease would reintroduce the numWorkers=2 deadlock the design avoids).
	tr         *concTransport
	pullLeases []int
}

func (c *pagePullClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *pagePullClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *pagePullClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *pagePullClient) PullEvent(_ context.Context, _, _, since int64) ([]actions.Event, int64, error) {
	c.mu.Lock()
	c.pullCalls++
	if c.tr != nil {
		c.tr.mu.Lock()
		c.pullLeases = append(c.pullLeases, c.tr.leases)
		c.tr.mu.Unlock()
	}
	c.mu.Unlock()
	if err := c.pullErrAt[since]; err != nil {
		return nil, 0, err
	}
	p, ok := c.pages[since]
	if !ok {
		// An unscripted cursor is the end of the backlog.
		return nil, since, nil
	}
	return p.events, p.next, nil
}
func (c *pagePullClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.downloadCalls++
	c.mu.Unlock()
	if _, err := w.Write(c.content[fileID]); err != nil {
		return actions.Node{}, err
	}
	return c.nodes[fileID], nil
}

// TestSyncViewPaginatesWholeBacklog confirms one syncView call drains a view's entire
// pending backlog: several non-empty pull_event pages followed by an empty one, all under
// a single worker lease, with every event applied, the cursor at the final value, and the
// queue fully drained.
func TestSyncViewPaginatesWholeBacklog(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			0: {events: []actions.Event{createEvent("a.txt", "fa", 1, false)}, next: 1},
			1: {events: []actions.Event{createEvent("b.txt", "fb", 2, false)}, next: 2},
			2: {events: []actions.Event{createEvent("c.txt", "fc", 3, false)}, next: 3},
			3: {events: nil, next: 3},
		},
		content: map[string][]byte{
			"fa": []byte("alpha"), "fb": []byte("beta"), "fc": []byte("gamma"),
		},
		nodes: map[string]actions.Node{
			"fa": {Hash: "ha", MTime: 1700000000, Size: 5},
			"fb": {Hash: "hb", MTime: 1700000000, Size: 4},
			"fc": {Hash: "hc", MTime: 1700000000, Size: 5},
		},
	}
	tr := &concTransport{client: pc}
	pc.tr = tr
	e := New(st, tr, root, "self", nil)

	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView: %v", err)
	}

	// All three non-empty pages plus the terminating empty page are read in one syncView
	// call, proving the loop reads the whole backlog rather than one page per cycle.
	if pc.pullCalls != 4 {
		t.Errorf("pull calls = %d, want 4 (three non-empty pages + the terminating empty page)", pc.pullCalls)
	}
	// Every page was pulled under the SAME worker lease (the first one, before the trailing
	// drainQueue takes its own): a per-page re-lease would record an ascending sequence and
	// reintroduce the numWorkers=2 nested-lease deadlock the design avoids.
	for i, l := range pc.pullLeases {
		if l != 1 {
			t.Errorf("pull %d ran under lease %d, want 1 - pagination must hold a single lease (leases seen: %v)", i, l, pc.pullLeases)
		}
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 3 {
		t.Errorf("cursor = %d, want 3 (the final page)", v.LastSeenSyncID)
	}
	for name, want := range map[string]string{"a.txt": "alpha", "b.txt": "beta", "c.txt": "gamma"} {
		got, err := os.ReadFile(filepath.Join(root, "Photos", name))
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestSyncViewSpinGuardStopsOnStalledCursor confirms the pagination loop stops on a
// non-empty page whose cursor does not advance (a misbehaving/truncated catalog), rather
// than re-pulling the same page forever - the page is still reconciled once and applied.
func TestSyncViewSpinGuardStopsOnStalledCursor(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			// A non-empty page whose reported next (0) does not advance past the cursor (0).
			0: {events: []actions.Event{createEvent("a.txt", "fa", 0, false)}, next: 0},
		},
		content: map[string][]byte{"fa": []byte("alpha")},
		nodes:   map[string]actions.Node{"fa": {Hash: "ha", MTime: 1700000000, Size: 5}},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView: %v", err)
	}

	// The spin-guard must have stopped the loop: exactly one pull_event turn ran.
	if pc.pullCalls != 1 {
		t.Errorf("pull calls = %d, want 1 (spin-guard must stop after the stalled page)", pc.pullCalls)
	}
	// The page was still reconciled and applied once.
	got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt"))
	if err != nil {
		t.Fatalf("ReadFile a.txt: %v", err)
	}
	if string(got) != "alpha" {
		t.Errorf("a.txt = %q, want %q", got, "alpha")
	}
}

// TestSyncViewSpinGuardStopsOnStalledPositiveCursor confirms the spin-guard also fires
// from a non-zero starting cursor - the realistic truncated-backlog case where a daemon
// has already paginated past many pages and the server returns a non-empty page whose
// reported next cursor fails to advance past the current one.
func TestSyncViewSpinGuardStopsOnStalledPositiveCursor(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	// The view already sits at cursor 5 from earlier pagination.
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true, LastSeenSyncID: 5}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			// A non-empty page at cursor 5 whose event SyncID and reported next both stay at 5.
			5: {events: []actions.Event{createEvent("a.txt", "fa", 5, false)}, next: 5},
		},
		content: map[string][]byte{"fa": []byte("alpha")},
		nodes:   map[string]actions.Node{"fa": {Hash: "ha", MTime: 1700000000, Size: 5}},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView: %v", err)
	}

	// Exactly one pull ran: the guard stopped the loop rather than re-pulling cursor 5 forever.
	if pc.pullCalls != 1 {
		t.Errorf("pull calls = %d, want 1 (spin-guard must stop after the stalled positive-cursor page)", pc.pullCalls)
	}
	// The page was still reconciled and applied once, and the cursor stayed at 5 (not advanced
	// by the non-advancing page).
	if got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt")); err != nil || string(got) != "alpha" {
		t.Errorf("a.txt = %q (err %v), want %q", got, err, "alpha")
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor = %d, want 5 (a stalled page must not advance the cursor)", v.LastSeenSyncID)
	}
}

// TestSyncViewSpinGuardLatchSkipsReReconcileAcrossCycles confirms the cross-cycle latch:
// a misbehaving/hostile server that keeps flagging the view and returns
// the SAME non-advancing page across poll cycles must reconcile+apply it exactly ONCE, then
// skip it, rather than re-enqueuing and re-applying (re-downloading) the page every cycle
// forever. Before the fix each syncView call re-reconciled the stalled page, so the download
// re-applied on every cycle.
func TestSyncViewSpinGuardLatchSkipsReReconcileAcrossCycles(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true, LastSeenSyncID: 5}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			// A non-empty page at cursor 5 whose event SyncID and reported next both stay at 5.
			5: {events: []actions.Event{createEvent("a.txt", "fa", 5, false)}, next: 5},
		},
		content: map[string][]byte{"fa": []byte("alpha")},
		nodes:   map[string]actions.Node{"fa": {Hash: "ha", MTime: 1700000000, Size: 5}},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	// Three poll cycles all hitting the same non-advancing page.
	for i := 0; i < 3; i++ {
		if err := e.syncView(context.Background(), 7); err != nil {
			t.Fatalf("syncView cycle %d: %v", i, err)
		}
	}

	// The stalled page is reconciled and its download applied exactly ONCE across all three
	// cycles - the latch skips the reconcile on cycles 2 and 3. Without the fix the download
	// re-applies every cycle (downloadCalls == 3).
	pc.mu.Lock()
	dc := pc.downloadCalls
	pc.mu.Unlock()
	if dc != 1 {
		t.Errorf("download calls = %d, want 1 (the stalled page must reconcile+apply once, then be skipped)", dc)
	}
	if got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt")); err != nil || string(got) != "alpha" {
		t.Errorf("a.txt = %q (err %v), want %q", got, err, "alpha")
	}
	// No duplicate ops accumulate in the queue across cycles.
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0", n, err)
	}
	// The cursor never advanced past the stalled page.
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor = %d, want 5 (a stalled page must not advance the cursor)", v.LastSeenSyncID)
	}
}

// TestSyncViewSpinGuardLatchResetsWhenCursorAdvances confirms the cross-cycle latch is
// cleared once the server recovers: after a stalled page latches, a later page that DOES
// advance the cursor is reconciled and applied normally. The latch must
// not suppress legitimate progress.
func TestSyncViewSpinGuardLatchResetsWhenCursorAdvances(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			// Initially the page at cursor 0 stalls (event SyncID and next both 0).
			0: {events: []actions.Event{createEvent("a.txt", "fa", 0, false)}, next: 0},
		},
		content: map[string][]byte{"fa": []byte("alpha"), "fb": []byte("beta")},
		nodes: map[string]actions.Node{
			"fa": {Hash: "ha", MTime: 1700000000, Size: 5},
			"fb": {Hash: "hb", MTime: 1700000000, Size: 4},
		},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	// Cycle 1: the stalled page latches (one reconcile+download).
	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView cycle 1: %v", err)
	}
	// Cycle 2 (still stalled): skipped by the latch - no new download.
	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView cycle 2: %v", err)
	}
	pc.mu.Lock()
	dc := pc.downloadCalls
	pc.mu.Unlock()
	if dc != 1 {
		t.Fatalf("download calls after two stalled cycles = %d, want 1", dc)
	}

	// The server recovers: cursor 0 now advances (a real create at SyncID 1), followed by an
	// empty terminating page at cursor 1.
	pc.mu.Lock()
	pc.pages[0] = catalogPage{events: []actions.Event{createEvent("b.txt", "fb", 1, false)}, next: 1}
	pc.pages[1] = catalogPage{events: nil, next: 1}
	pc.mu.Unlock()

	// Cycle 3: the advancing page clears the latch and is reconciled+applied.
	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView cycle 3: %v", err)
	}
	pc.mu.Lock()
	dc = pc.downloadCalls
	pc.mu.Unlock()
	if dc != 2 {
		t.Errorf("download calls after recovery = %d, want 2 (the advancing page must reconcile+apply)", dc)
	}
	if got, err := os.ReadFile(filepath.Join(root, "Photos", "b.txt")); err != nil || string(got) != "beta" {
		t.Errorf("b.txt = %q (err %v), want %q", got, err, "beta")
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 1 {
		t.Errorf("cursor = %d, want 1 (the recovered page must advance the cursor)", v.LastSeenSyncID)
	}
}

// TestSyncViewDurablePerPageCursor confirms a PullEvent failure on the second page leaves
// the cursor durably advanced past the first (successfully committed) page and surfaces
// the error, so a crash/retry mid-backlog resumes from the last committed page rather than
// re-pulling the whole backlog.
func TestSyncViewDurablePerPageCursor(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	wantErr := errors.New("transient pull failure")
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			0: {events: []actions.Event{createEvent("a.txt", "fa", 1, false)}, next: 1},
		},
		// The second page (cursor 1) fails.
		pullErrAt: map[int64]error{1: wantErr},
		content:   map[string][]byte{"fa": []byte("alpha")},
		nodes:     map[string]actions.Node{"fa": {Hash: "ha", MTime: 1700000000, Size: 5}},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	err := e.syncView(context.Background(), 7)
	if !errors.Is(err, wantErr) {
		t.Fatalf("syncView error = %v, want it to wrap %v", err, wantErr)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 1 {
		t.Errorf("cursor = %d, want 1 (the first page must be durably committed before the failure)", v.LastSeenSyncID)
	}
}

// TestSyncViewEmptyPageAdvancesCursor confirms an empty event_list carrying a server next
// cursor strictly ahead of ours still advances the cursor (events between the two aged out
// / were compacted server-side). Without this the cursor stays pinned below the true server
// position and long_poll re-flags the view forever, re-pulling the same empty page.
func TestSyncViewEmptyPageAdvancesCursor(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			// No events, but the server reports a next cursor ahead of ours.
			0: {events: nil, next: 5},
		},
	}
	tr := &concTransport{client: pc}
	e := New(st, tr, root, "self", nil)

	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView: %v", err)
	}

	// One pull ran: the empty page exhausts the backlog, no further pull.
	if pc.pullCalls != 1 {
		t.Errorf("pull calls = %d, want 1 (an empty page ends the backlog)", pc.pullCalls)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor = %d, want 5 (an empty page must honour the server's advanced next cursor)", v.LastSeenSyncID)
	}
}

// digestClient is a loopClient that keeps a long_poll profile digest like actions.Client: it
// records the digest it was seeded with and learns `reply` from the poll.
type digestClient struct {
	*loopClient
	seeded, held, reply string
}

func (c *digestClient) ProfileDigest() string     { return c.held }
func (c *digestClient) SetProfileDigest(d string) { c.seeded, c.held = d, d }
func (c *digestClient) LongPoll(ctx context.Context, s []actions.Subscription, timeout int) ([]int64, error) {
	if c.reply != "" {
		c.held = c.reply
	}
	return c.loopClient.LongPoll(ctx, s, timeout)
}

// TestPollOnceCarriesTheProfileDigestAcrossEventClients: the event Client is rebuilt for every
// poll, and a poll that does not send the server's last digest back is answered at once - the
// idle daemon spun on it. The digest the first client learned must reach the second.
func TestPollOnceCarriesTheProfileDigestAcrossEventClients(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		views:  []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) { return nil, nil },
	}
	first := &digestClient{loopClient: lc, reply: "2a83c785"}
	tr := &replaceCountingTransport{client: first}
	e := New(st, tr, t.TempDir(), "s", nil)
	e.lastScan = time.Now()
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("first pollOnce: %v", err)
	}
	if first.seeded != "" {
		t.Errorf("first poll seeded digest %q, want none yet", first.seeded)
	}
	second := &digestClient{loopClient: lc}
	tr.client = second
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("second pollOnce: %v", err)
	}
	if second.seeded != "2a83c785" {
		t.Errorf("second event client was seeded with %q, want the digest the first one learned", second.seeded)
	}
}

// deadlineClient is a loopClient whose LongPoll records how far off its context's deadline is.
type deadlineClient struct {
	*loopClient
	left time.Duration
}

func (c *deadlineClient) LongPoll(ctx context.Context, s []actions.Subscription, timeout int) ([]int64, error) {
	if d, ok := ctx.Deadline(); ok {
		c.left = time.Until(d)
	}
	return c.loopClient.LongPoll(ctx, s, timeout)
}

// TestFairnessPassEndsOnItsOwnDeadline: the server raises any long_poll timeout under 300 s to
// 300 s, so a fairness pass cannot be ended by the server early. Its client-side deadline is
// therefore its whole length - fairnessPollTimeout, without the slack a full-length poll keeps
// for the server's timed-out reply - while an ordinary poll still gets that slack.
func TestFairnessPassEndsOnItsOwnDeadline(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	dc := &deadlineClient{loopClient: &loopClient{
		views:  []actions.View{{ViewID: 7, Name: "Photos", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) { return nil, nil },
	}}
	e := New(st, &replaceCountingTransport{client: dc}, t.TempDir(), "s", nil)
	e.lastScan = time.Now()

	e.pollDeferWake = true
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("fairness pollOnce: %v", err)
	}
	if want := fairnessPollTimeout * time.Second; dc.left > want || dc.left < want-5*time.Second {
		t.Errorf("fairness pass deadline %s away, want about %s", dc.left.Round(time.Second), want)
	}

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("ordinary pollOnce: %v", err)
	}
	full := time.Duration(e.limits.EventPollTimeout+e.limits.PollDeadlineSlack) * time.Second
	if dc.left > full || dc.left < full-5*time.Second {
		t.Errorf("ordinary poll deadline %s away, want about %s", dc.left.Round(time.Second), full)
	}
}
