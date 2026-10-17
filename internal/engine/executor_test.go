package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// dlClient is a concurrency-safe syncClient whose Download writes preset bytes for a
// file id and returns preset node metadata. It tracks the maximum number of Download
// calls in flight at once so a test can assert the executor serialises a path.
type dlClient struct {
	mu      sync.Mutex
	content map[string][]byte
	nodes   map[string]actions.Node
	cur     int
	max     int
	delay   time.Duration
	// dlErr, when non-nil, is returned from Download instead of writing the content,
	// letting a test drive an apply failure (e.g. an ENOSPC classification).
	dlErr error
	// dlErrOnce is dlErr scoped to the FIRST download of a node: it is returned once and then
	// forgotten. A feeder whose dead-letter dooms the op behind it leaves a compensating
	// re-request for the SAME file id, and that request must be able to land - so neither a
	// blanket dlErr nor a per-id one can express the scenario, since both kill the
	// compensation too and the test silently stops measuring what it names. The error is
	// classified terminal on its own (errRejectedPath), not by repetition, so failing once is
	// a faithful way to kill exactly one attempt.
	dlErrOnce map[string]error
	// queryErr, when non-nil, is returned from QueryNode, letting a test drive the
	// view-root/node absence probes - e.g. actions.ErrPathError for
	// a positively-gone path.
	queryErr error
	// queryNode is the node QueryNode reports when queryErr is nil (zero value by default:
	// a tolerant reply carrying no id).
	queryNode actions.Node
}

func (c *dlClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *dlClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queryNode, c.queryErr
}
func (c *dlClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *dlClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}

func (c *dlClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	c.cur--
	dlErr := c.dlErr
	if once, ok := c.dlErrOnce[fileID]; ok {
		dlErr = once
		delete(c.dlErrOnce, fileID)
	}
	c.mu.Unlock()
	if dlErr != nil {
		return actions.Node{}, dlErr
	}
	if _, err := w.Write(c.content[fileID]); err != nil {
		return actions.Node{}, err
	}
	return c.nodes[fileID], nil
}

// concTransport hands the same client to the event path and to every worker lease,
// counting leases under a lock so it is safe to call WithWorker from the executor's
// concurrent path-group goroutines.
type concTransport struct {
	client syncClient
	mu     sync.Mutex
	leases int
}

func (t *concTransport) EventClient() (syncClient, error) { return t.client, nil }

func (t *concTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	t.mu.Lock()
	t.leases++
	t.mu.Unlock()
	return fn(t.client)
}

func (t *concTransport) ReplaceEvent(context.Context) error { return nil }

func TestDrainQueueDownloadsFile(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("hello world")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", MTime: 1700000000, Size: 11}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "docs/a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(root, "Photos", "docs", "a.txt"))
	if err != nil {
		t.Fatalf("read applied file: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("file content = %q, want %q", got, "hello world")
	}

	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}

	b, err := st.GetBaseline(7, "docs/a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced", b.State)
	}
	if b.FileID != "f1" || b.ServerHash != "h1" || b.LocalHash != "h1" || b.LocalSize != 11 {
		t.Errorf("baseline facts not recorded: %+v", b)
	}

	fi, err := os.Stat(filepath.Join(root, "Photos", "docs", "a.txt"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.ModTime().Unix() != 1700000000 {
		t.Errorf("mtime = %d, want 1700000000", fi.ModTime().Unix())
	}
}

func TestDrainQueueMkdir(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "newdir", FileID: "d1", LocalMTime: 1700000000,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, err := os.Stat(filepath.Join(root, "Photos", "newdir"))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !fi.IsDir() {
		t.Error("created path is not a directory")
	}
	b, err := st.GetBaseline(7, "newdir")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileType != "dir" || b.FileID != "d1" {
		t.Errorf("dir baseline = %+v, want synced dir d1", b)
	}
}

func TestDrainQueueRename(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "old.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "old.txt")); !os.IsNotExist(err) {
		t.Errorf("old path still present: err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "new.txt"))
	if err != nil {
		t.Fatalf("read renamed file: %v", err)
	}
	if string(got) != "body" {
		t.Errorf("renamed content = %q, want %q", got, "body")
	}
	if _, err := st.GetBaseline(7, "old.txt"); err == nil {
		t.Error("baseline at old path survived the rename")
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline(new): %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f9" {
		t.Errorf("moved baseline = %+v, want synced f9", b)
	}
}

// TestApplyRenamePreservesConflictState covers this shape: when a divergent
// (StateConflict) file is moved server-side, the inbound rename relocates the local
// divergent bytes but must NOT silently advance the baseline to StateSynced - doing so
// would bury the divergence (the recorded LocalHash equals the on-disk divergent hash, so
// the next scan sees no change). The conflict state must survive the move so it stays
// surfaced.
func TestApplyRenamePreservesConflictState(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), []byte("local-divergent"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// A diverged baseline left behind by the outbound conflict path (markOutboundConflict):
	// LocalHash describes the on-disk divergent bytes, the node carries a server file id.
	if err := st.PutBaseline(7, "old.txt", store.Baseline{
		FileID: "f9", FileType: "file", State: store.StateConflict, LocalHash: "divergent",
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The bytes move with the rename.
	got, err := os.ReadFile(filepath.Join(viewDir, "new.txt"))
	if err != nil {
		t.Fatalf("read renamed file: %v", err)
	}
	if string(got) != "local-divergent" {
		t.Errorf("renamed content = %q, want %q", got, "local-divergent")
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline(new): %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("moved baseline state = %q, want %q (divergence must not be buried)", b.State, store.StateConflict)
	}
	if b.FileID != "f9" {
		t.Errorf("moved baseline file id = %q, want f9", b.FileID)
	}
}

func TestDrainQueueRemove(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "gone.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "gone.txt")); !os.IsNotExist(err) {
		t.Errorf("removed file still present: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the remove")
	}
}

// TestDrainQueueSerializesSamePath confirms two ops on the same path never run
// concurrently: grouped under one serialisation key, they apply sequentially even
// though distinct paths would parallelise.
func TestDrainQueueSerializesSamePath(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("data")},
		// The scripted hash must be the real MD4 of the delivered bytes (a faithful server -
		// actions.Download verifies exactly that): the first apply records it as the synced
		// LocalHash, and the second same-path download's live-base check
		// must see the prior output as in sync, not as a phantom local edit.
		nodes: map[string]actions.Node{"f1": {Hash: md4.ContentHash([]byte("data")), Size: 4}},
		delay: 20 * time.Millisecond,
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	for i := 0; i < 2; i++ {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 7, Path: "same.txt", FileID: "f1",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if dl.max != 1 {
		t.Errorf("max concurrent downloads on one path = %d, want 1", dl.max)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d remain", n)
	}
}

// errClient is a syncClient whose Download always fails, used to exercise the
// executor's failure path.
type errClient struct{ err error }

func (c *errClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *errClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *errClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *errClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *errClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, c.err
}

// TestDrainQueueFailureKeepsOpQueued confirms the crash-resume contract: when an
// op's apply fails, drainQueue returns the error, the op is left in the queue
// un-Acked for a later retry, and the baseline is not advanced to synced.
func TestDrainQueueFailureKeepsOpQueued(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &errClient{err: errors.New("download boom")}}, root, "sess", nil)

	// Mark the baseline pending as reconcile would, so we can confirm a failed
	// apply does not flip it to synced.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want an error from the failed download")
	}

	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 1 {
		t.Errorf("pending ops = %d, want the failed op to remain (1)", n)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateSynced {
		t.Error("baseline advanced to synced despite a failed apply")
	}
}

// TestDrainQueueStuckCounterEscalatesAndClears confirms the
// forever-transient escalation: an op that keeps failing apply without being classified
// terminal crosses stuckThreshold consecutive drains, surfaces in StuckOps (the `Stuck`
// status line) with exactly one Warn escalation, and the counter self-clears once the
// op finally applies.
func TestDrainQueueStuckCounterEscalatesAndClears(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("hello")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", MTime: 1700000000, Size: 5}},
		dlErr:   errors.New("download boom"),
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	e := New(st, &concTransport{client: dl}, root, "sess", log)

	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	const stuckMsg = "op stuck"

	// Below the threshold the op is an ordinary transient: not yet stuck, no Warn.
	for i := 0; i < stuckThreshold-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected download failure", i+1)
		}
	}
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps after %d failures = %d, want 0 (below threshold)", stuckThreshold-1, got)
	}
	if strings.Contains(logbuf.String(), stuckMsg) {
		t.Fatalf("stuck Warn fired below threshold:\n%s", logbuf.String())
	}

	// The threshold-th failure escalates: counted in StuckOps, exactly one Warn - and
	// further failures stay counted without re-warning.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("threshold drain succeeded, want the injected download failure")
	}
	if got := e.StuckOps(); got != 1 {
		t.Fatalf("StuckOps at threshold = %d, want 1", got)
	}
	for i := 0; i < 3; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatal("post-threshold drain succeeded, want the injected download failure")
		}
	}
	if got := e.StuckOps(); got != 1 {
		t.Fatalf("StuckOps past threshold = %d, want 1", got)
	}
	if n := strings.Count(logbuf.String(), stuckMsg); n != 1 {
		t.Fatalf("stuck Warn fired %d times, want exactly 1:\n%s", n, logbuf.String())
	}

	// Heal the transport: the op applies, is acked, and the stuck count self-clears.
	dl.mu.Lock()
	dl.dlErr = nil
	dl.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after heal: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending after heal = %d (err %v), want 0", n, err)
	}
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps after successful apply = %d, want 0", got)
	}
}

// TestDrainQueueStuckClearedOnDeadLetter confirms a stuck op's failure counter is
// dropped when the op leaves the queue by dead-letter rather than success, so StuckOps
// cannot keep reporting an op that no longer exists.
func TestDrainQueueStuckClearedOnDeadLetter(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server file")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 11}},
		dlErr:   errors.New("download boom"),
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if err := st.PutBaseline(7, "foo", store.Baseline{FileID: "f1", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Drive the op past the stuck threshold with the injected transfer failure.
	for i := 0; i < stuckThreshold; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected download failure", i+1)
		}
	}
	if got := e.StuckOps(); got != 1 {
		t.Fatalf("StuckOps = %d, want 1", got)
	}

	// Swap the transient transfer failure for a TERMINAL one, so the next drain dead-letters
	// the op - and the stuck counter must go with it. This used to park an untracked DIRECTORY
	// on the target instead, which is no longer terminal: a directory occupant is preserved
	// aside as a keep-both copy and the server file lands. What the test measures is that a
	// dead-letter clears the stuck count, whatever killed the op.
	dl.mu.Lock()
	dl.dlErr = errRejectedPath
	dl.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the blocked download, not wedge: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending after dead-letter = %d (err %v), want 0", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("dead-letter count = %d (err %v), want 1", n, err)
	}
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps after dead-letter = %d, want 0", got)
	}
}

// countingErrClient is a syncClient whose Download always fails and counts how many
// times it was called, used to prove a conflicting successor is skipped (its Download
// never runs) rather than applied out of order after a predecessor fails.
type countingErrClient struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (c *countingErrClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return nil, nil
}
func (c *countingErrClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *countingErrClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *countingErrClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *countingErrClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return actions.Node{}, c.err
}

// TestDrainQueueSkipsConflictingSuccessorAfterFailure confirms the executor's central
// ordering-safety guarantee: when an op fails, a later conflicting (same-path) op is
// skipped - never applied out of order - and both stay queued for the next drain. Two
// same-path downloads are enqueued; the first fails, so the second's Download must
// never run even though, run alone, it would be attempted.
func TestDrainQueueSkipsConflictingSuccessorAfterFailure(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	c := &countingErrClient{err: errors.New("download boom")}
	e := New(st, &concTransport{client: c}, root, "sess", nil)

	for _, id := range []string{"f1", "f2"} {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 7, Path: "a.txt", FileID: id,
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want an error from the failed download")
	}
	c.mu.Lock()
	calls := c.calls
	c.mu.Unlock()
	if calls != 1 {
		t.Errorf("Download calls = %d, want 1 (the conflicting successor must be skipped, not applied)", calls)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 2 {
		t.Errorf("pending ops = %d, want both the failed op and its skipped successor to remain (2)", n)
	}
}

// TestDrainQueueBlockedSuccessorEscalationNamesCulprit pins the
// observability fix: a victim op skipped every drain because a conflicting predecessor
// keeps failing surfaces in StuckOps too, and its one-time escalation Warn names the
// head-of-chain (self-failing) op - the culprit to unstick - rather than blaming the
// blameless successor. Two same-path downloads are enqueued; the first (the culprit)
// fails its own apply every drain, so the second (the victim) is skipped every drain.
// Across stuckThreshold drains both cross the threshold: the culprit warns "kept
// failing", the victim warns "blocked behind a conflicting op that keeps failing" and
// carries the culprit's seq/path.
func TestDrainQueueBlockedSuccessorEscalationNamesCulprit(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	// Both downloads carry identical, really-hashed content so the second (overwrite)
	// stays a clean pinned overwrite when the transport heals, rather than funnelling
	// into keep-both on a fake-hash mismatch.
	shared := []byte("shared bytes")
	sharedHash := md4.ContentHash(shared)
	dl := &dlClient{
		content: map[string][]byte{"f1": shared, "f2": shared},
		nodes: map[string]actions.Node{
			"f1": {Hash: sharedHash, Size: int64(len(shared))},
			"f2": {Hash: sharedHash, Size: int64(len(shared))},
		},
		dlErr: errors.New("download boom"),
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	e := New(st, &concTransport{client: dl}, root, "sess", log)

	// Two same-path downloads: f1 (seq 1, the culprit) fails its own apply every drain;
	// f2 (seq 2, the victim) conflicts on the same path and is skipped every drain.
	var culpritSeq, victimSeq uint64
	for i, id := range []string{"f1", "f2"} {
		seq, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 7, Path: "a.txt", FileID: id, UnixPerm: 644,
		})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if i == 0 {
			culpritSeq = seq
		} else {
			victimSeq = seq
		}
	}

	const (
		culpritMsg = "op stuck: kept failing"
		victimMsg  = "op stuck: blocked behind a conflicting op that keeps failing"
	)

	// Below the threshold neither op is stuck and neither has warned.
	for i := 0; i < stuckThreshold-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected download failure", i+1)
		}
	}
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps below threshold = %d, want 0", got)
	}
	if strings.Contains(logbuf.String(), culpritMsg) || strings.Contains(logbuf.String(), victimMsg) {
		t.Fatalf("stuck Warn fired below threshold:\n%s", logbuf.String())
	}

	// The threshold-th drain escalates both: the culprit (self-failing) and the victim
	// (blocked behind it) - StuckOps counts both, the wedge is now fully visible.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("threshold drain succeeded, want the injected download failure")
	}
	if got := e.StuckOps(); got != 2 {
		t.Fatalf("StuckOps at threshold = %d, want 2 (culprit + blocked victim)", got)
	}

	out := logbuf.String()
	// The culprit warns it kept failing its own apply.
	if n := strings.Count(out, culpritMsg); n != 1 {
		t.Fatalf("culprit Warn fired %d times, want exactly 1:\n%s", n, out)
	}
	// The victim warns it is blocked, exactly once, and names the culprit's seq/path so
	// an operator unsticks the failing op, not the blameless successor.
	if n := strings.Count(out, victimMsg); n != 1 {
		t.Fatalf("victim Warn fired %d times, want exactly 1:\n%s", n, out)
	}
	if want := fmt.Sprintf("blocked_by_seq=%d", culpritSeq); !strings.Contains(out, want) {
		t.Fatalf("victim Warn missing culprit seq %q:\n%s", want, out)
	}
	if !strings.Contains(out, "blocked_by_path=a.txt") {
		t.Fatalf("victim Warn missing culprit path:\n%s", out)
	}
	if want := fmt.Sprintf("seq=%d", victimSeq); !strings.Contains(out, want) {
		t.Fatalf("victim Warn missing its own seq %q:\n%s", want, out)
	}

	// The blocked victim is not permanently counted: once the wedge resolves the count
	// self-clears via clearOpFailures, exactly like the culprit. Heal the transport so the
	// culprit applies and the (now un-blocked) victim applies right behind it - both leave
	// the queue and both stuck counters go with them.
	dl.mu.Lock()
	dl.dlErr = nil
	dl.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after heal: %v", err)
	}
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps after heal = %d, want 0 (both self-cleared)", got)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending after heal = %d (err %v), want 0", n, err)
	}
}

// TestDrainQueueAppliesBacklogLargerThanBatch confirms drainQueue's bounded-batch loop
// drains a backlog larger than maxDrainBatch: every op across multiple batch iterations
// is applied and Acked, exercising the len(ops) > maxDrainBatch truncation branch and
// the multi-iteration loop that single-batch tests never reach.
func TestDrainQueueAppliesBacklogLargerThanBatch(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	const n = maxDrainBatch + 50 // spans more than one batch iteration
	for i := 0; i < n; i++ {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpMkdir, Direction: store.DirInbound,
			ViewID: 7, Path: fmt.Sprintf("d%d", i),
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	pending, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending ops = %d, want 0 (all batches drained)", pending)
	}
	for i := 0; i < n; i++ {
		if _, err := os.Stat(filepath.Join(root, "Photos", fmt.Sprintf("d%d", i))); err != nil {
			t.Errorf("dir d%d not created across batch boundary: %v", i, err)
		}
	}
}

// TestDrainQueueNotStarvedBySaturatedHead pins the head-saturation
// fix: when the head DrainBatch window is filled with permanently-failing ops, the
// drain pages its read window past them and still reaches the ops behind - before the
// fix drainQueue re-read the same head batch every pass and returned on its first
// error, so an op behind a saturated head (including one of a different, healthy
// view) was never read again: starvation forever, not the documented
// per-conflict-chain drag. Ordering still holds across window boundaries: a
// later-window op whose path overlaps a failed head op is skipped (left queued), not
// applied out of order.
func TestDrainQueueNotStarvedBySaturatedHead(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "A"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "B"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &errClient{err: errors.New("download boom")}}, root, "sess", nil)

	// Saturate the head window: a full DrainBatch of view-A downloads that all fail.
	// The first op's path doubles as the conflict anchor for the cross-window
	// ordering probe below.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "p.txt", FileID: "f0",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	for i := 1; i < maxDrainBatch; i++ {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 7, Path: fmt.Sprintf("a%d.txt", i), FileID: fmt.Sprintf("f%d", i),
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	// Window 2, behind the saturated head: a disk-only mkdir on the failed head op's
	// own path (it WOULD succeed if wrongly run - the ordering probe) and an
	// independent healthy op of view B that must apply this drain.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "p.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 8, Path: "healthy",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the saturated head's failure to surface")
	}

	if _, err := os.Stat(filepath.Join(root, "B", "healthy")); err != nil {
		t.Fatalf("healthy view-B op behind the saturated head did not apply: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "A", "p.txt")); err == nil {
		t.Fatal("conflicting later-window op applied out of order ahead of the failed head op")
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if want := maxDrainBatch + 1; n != want {
		t.Errorf("pending ops = %d, want %d (the failing head plus the skipped conflicting successor)", n, want)
	}
}

// TestBlockedKeysOverlap unit-tests the cross-window ordering index blockedKeys uses to
// carry the enqueue-order invariant past a failing drain window.
// TestDrainQueueNotStarvedBySaturatedHead exercises only the exact-path case; the
// ancestor and descendant branches of add/conflicts (the anc index and the ancestor
// walk) are the ones that keep an ancestor remove from applying ahead of a still-queued
// descendant, so pin them directly and cross-check against serialize.go's single overlap
// definition (pathsOverlap) so the second implementation cannot silently drift from it.
func TestBlockedKeysOverlap(t *testing.T) {
	sep := string(filepath.Separator)
	// k builds an anchored, cleaned key the way cleanKey does (OS separators).
	k := func(slash string) string {
		return filepath.Join(sep+"root", filepath.FromSlash(slash))
	}

	cases := []struct {
		name    string
		blocked []string
		probe   string
		want    bool
	}{
		{"equal", []string{k("a/b")}, k("a/b"), true},
		{"probe is descendant of blocked", []string{k("a")}, k("a/b/c"), true},
		{"probe is ancestor of blocked", []string{k("a/b/c")}, k("a"), true},
		{"probe is multi-level ancestor of blocked", []string{k("a/b/c/d")}, k("a/b"), true},
		{"sibling does not overlap", []string{k("a/b")}, k("a/c"), false},
		{"shared prefix without a boundary does not overlap", []string{k("a/b")}, k("a/bc"), false},
		{"common ancestor of both is not itself an overlap", []string{k("a/b")}, k("a/c"), false},
		{"unrelated does not overlap", []string{k("x/y")}, k("p/q"), false},
		{"multiple blocked keys, one ancestor hit", []string{k("m/n"), k("a/b/c")}, k("a/b"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b blockedKeys
			b.add(tc.blocked)
			if got := b.conflicts([]string{tc.probe}); got != tc.want {
				t.Errorf("conflicts(%q) with blocked %v = %v, want %v", tc.probe, tc.blocked, got, tc.want)
			}
			// Cross-check the case oracle against the single overlap definition in
			// serialize.go: if these disagree, blockedKeys has drifted from pathsOverlap.
			oracle := false
			for _, bk := range tc.blocked {
				if pathsOverlap(tc.probe, bk) {
					oracle = true
				}
			}
			if oracle != tc.want {
				t.Fatalf("pathsOverlap oracle over %v vs %q = %v, want %v", tc.blocked, tc.probe, oracle, tc.want)
			}
		})
	}

	// The empty set never conflicts (the len(b.exact)==0 fast path).
	var empty blockedKeys
	if empty.conflicts([]string{k("a/b")}) {
		t.Error("empty blockedKeys reported a conflict")
	}

	// A probe carrying several keys conflicts iff ANY of them overlaps a blocked key.
	var b blockedKeys
	b.add([]string{k("a/b/c")})
	if !b.conflicts([]string{k("x/y"), k("a")}) { // the second key is an ancestor of the blocked key
		t.Error("multi-key probe missed an ancestor overlap on its second key")
	}
	if b.conflicts([]string{k("x/y"), k("z")}) {
		t.Error("multi-key probe with no overlapping key reported a conflict")
	}
}

// TestDrainQueueUnknownOpType confirms an op with an unrecognised type is dead-lettered
// (recorded + Acked + dropped) rather than returning a plain error that would be retried
// forever and wedge the queue - the same failure mode closed for EEXIST/ENOTDIR. The
// type is currently unreachable (reconcile only enqueues known types), so the op is enqueued
// directly.
func TestDrainQueueUnknownOpType(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: "bogus", Direction: store.DirInbound, ViewID: 7, Path: "x",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The unknown-type op is dropped, not wedged: drainQueue returns nil (a dead-letter is
	// not a failure) and the queue drains.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter an unknown op type, not wedge: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("pending ops = %d, want the unknown-type op dropped (0)", n)
	}
	dn, err := st.DeadLetterLen()
	if err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	}
	if dn != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 durable trace for the unknown-type op", dn)
	}
	// The trace must be useful to an operator, not just present: reason names the
	// unknown type and the view/path identify the dropped op.
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("ListDeadLetters len = %d, want 1", len(dls))
	}
	if got := dls[0].Reason; !strings.Contains(got, "unknown op type") {
		t.Errorf("dead-letter reason = %q, want it to mention %q", got, "unknown op type")
	}
	if dls[0].ViewID != 7 || dls[0].Path != "x" {
		t.Errorf("dead-letter view/path = %d/%q, want 7/%q", dls[0].ViewID, dls[0].Path, "x")
	}
}

// TestDrainQueueUnknownOutboundOpType is the outbound mirror of
// TestDrainQueueUnknownOpType: an unrecognised DirOutbound op type is dead-lettered
// (recorded + Acked + dropped) rather than returning a plain error that would retry
// forever and wedge the queue. Unreachable in practice (localReconcile/coldStart only
// enqueue known types), so the op is enqueued directly.
func TestDrainQueueUnknownOutboundOpType(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: "bogus", Direction: store.DirOutbound, ViewID: 7, Path: "x",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter an unknown outbound op type, not wedge: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("pending ops = %d, want the unknown-type outbound op dropped (0)", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("ListDeadLetters len = %d, want 1", len(dls))
	}
	if got := dls[0].Reason; !strings.Contains(got, "unknown outbound op type") {
		t.Errorf("dead-letter reason = %q, want it to mention %q", got, "unknown outbound op type")
	}
}

// TestDrainQueueRejectsPathTraversal confirms a server-supplied path that escapes
// the view root is refused instead of writing outside the sync root.
func TestDrainQueueRejectsPathTraversal(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "../../escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The traversing path can never be applied, so it is dead-lettered (dropped) rather
	// than wedging the queue: the drain succeeds, nothing escapes, and the poison op is
	// gone so it cannot freeze every later cycle.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop a traversing path, not wedge: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape.txt")); !os.IsNotExist(err) {
		t.Errorf("traversing download escaped the sync root: err=%v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("rejected op left queued (%d): it would wedge every later cycle", n)
	}
}

// TestDrainQueueDeadLettersDownloadUnderLocalFile confirms an inbound download whose
// parent path is occupied by a local regular file (os.MkdirAll → ENOTDIR) is dead-lettered
// rather than retried forever. This is the cold-start file↔dir type-mismatch tail: the
// server holds a directory `foo` where the local tree holds a file `foo`, the cold start
// surfaces `foo` as a conflict and defers its subtree, and a later incremental event for a
// child `foo/bar` enqueues this download. The blocking local file never disappears on its
// own under the never-delete policy, so retrying would wedge the queue.
func TestDrainQueueDeadLettersDownloadUnderLocalFile(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	// A regular file occupies the parent path `foo`, so a download of `foo/bar` cannot
	// mkdir the parent.
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	blocker := filepath.Join(viewDir, "foo")
	if err := os.WriteFile(blocker, []byte("local file at foo"), 0o644); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server child")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 12}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	// reconcile would have pre-written a pending baseline for the child before the drain.
	if err := st.PutBaseline(7, "foo/bar", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "foo/bar", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The op can never apply (the local file blocks the path), so it is dead-lettered:
	// the drain succeeds and the poison op is dropped rather than wedging every later cycle.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a download under a local file, not wedge: %v", err)
	}
	n2, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n2 != 0 {
		t.Errorf("ENOTDIR op left queued (%d): it would wedge every later cycle", n2)
	}
	// The blocking local file must be untouched (never-delete policy).
	if got, err := os.ReadFile(blocker); err != nil || string(got) != "local file at foo" {
		t.Errorf("blocking local file changed: content=%q err=%v", got, err)
	}
	// The phantom pending baseline for the never-materialised child is dropped.
	if _, err := st.GetBaseline(7, "foo/bar"); !isNotFound(err) {
		t.Errorf("phantom child baseline not dropped, got err %v", err)
	}
}

// TestDownloadOverLocalDirPreservesItAndLands is the file/dir clash resolved rather than
// abandoned.
//
// A fresh inbound download (BaseLocalHash=="") whose target is occupied by an untracked local
// DIRECTORY used to be dead-lettered, on the reasoning that file-over-dir can never apply. That
// holds only if one refuses to MOVE the directory, which is exactly what the never-lose doctrine
// does instead (DESIGN §6): untracked content in the way is kept as a _Conflict copy, not treated
// as a reason to abandon the peer's change. Dropping the download left the two clients
// permanently and SILENTLY divergent -- each holding only its own shape of the path, nothing
// queued, nothing stuck, nothing in `status` to show for it. That is finding H6's shape, and
// TestIntegration_TwoClientsFileDirClash measured it against a live DSM.
//
// So: the directory is preserved aside with its whole subtree, the server's file lands on the
// canonical path, and the queue drains. Nothing is deleted -- the move is a rename, recorded in
// the conflicts list -- so the never-delete policy is intact.
func TestDownloadOverLocalDirPreservesItAndLands(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	// An untracked local directory occupies the target path `foo`; the server now holds a
	// regular file there.
	blocker := filepath.Join(viewDir, "foo")
	if err := os.MkdirAll(filepath.Join(blocker, "child"), 0o755); err != nil {
		t.Fatalf("seed blocking dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(blocker, "child", "user.txt"), []byte("user content"), 0o644); err != nil {
		t.Fatalf("seed user content: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server file")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 11}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	// reconcile would have pre-written a pending FILE baseline for the server file before the drain.
	if err := st.PutBaseline(7, "foo", store.Baseline{FileID: "f1", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The first drain preserves the occupant and leaves the op queued; the next lands the file.
	for i := 0; i < 3; i++ {
		_ = e.drainQueue(context.Background())
		if n, _ := st.PendingLen(); n == 0 {
			break
		}
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending = %d (err %v), want the download applied", n, err)
	}
	if dlCount, err := st.DeadLetterLen(); err != nil || dlCount != 0 {
		t.Fatalf("dead-letters = %d (err %v), want 0: the clash is resolved, not abandoned", dlCount, err)
	}

	// The server's file holds the canonical path.
	if got, err := os.ReadFile(blocker); err != nil || string(got) != "server file" {
		t.Fatalf("canonical path = %q (err %v), want the server file", got, err)
	}
	// The user's directory survived WHOLE under the keep-both name - nothing deleted.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "foo" {
		t.Fatalf("conflict records = %+v, want one preserving foo", recs)
	}
	preserved := filepath.Join(viewDir, filepath.Base(recs[0].CopyPath))
	if got, err := os.ReadFile(filepath.Join(preserved, "child", "user.txt")); err != nil || string(got) != "user content" {
		t.Fatalf("preserved subtree = %q (err %v), want the user content intact", got, err)
	}
	// And the baseline now describes the file that really is there, keyed to its id - the
	// original hazard (a byid entry pointing at a foreign directory, which a later
	// same-id server rename would drag across the tree) is gone by construction, because the
	// directory is no longer at that path at all.
	if p, err := st.PathByID(7, "f1"); err != nil || p != "foo" {
		t.Errorf("PathByID(f1) = %q (err %v), want the landed file at foo", p, err)
	}
}

// TestDrainQueueDeadLettersDownloadPermissionOverLocalDir is the fs.ErrPermission mirror
// of TestDrainQueueDeadLettersDownloadOverLocalDir: the same untracked local
// DIRECTORY blocks the download target, but the apply failure surfaces as a permission
// error rather than fs.ErrExist. On Windows the replacing promote itself produces that -
// os.Rename → MoveFileEx(MOVEFILE_REPLACE_EXISTING) has no dir-guard and fails with
// ERROR_ACCESS_DENIED (fs.ErrPermission) when the destination is a directory - while this
// portable repro drives the same shape on Unix by making the staging-temp create fail
// EACCES under a read-only parent while the directory occupant blocks the target. Either
// way the occupant makes the op permanently unappliable under the never-delete policy, so
// the classifier must dead-letter it rather than retry forever with every same-path
// successor wedged behind it.
func TestDrainQueueDeadLettersDownloadPermissionOverLocalDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	// An untracked local directory occupies the target path `foo`; the server now holds a
	// regular file there.
	blocker := filepath.Join(viewDir, "foo")
	if err := os.MkdirAll(filepath.Join(blocker, "child"), 0o755); err != nil {
		t.Fatalf("seed blocking dir: %v", err)
	}
	// A read-only view dir makes the staging-temp create fail EACCES, so the apply error is
	// fs.ErrPermission - not the promote's fs.ErrExist - while the dir occupant is what
	// actually dooms the op.
	if err := os.Chmod(viewDir, 0o555); err != nil {
		t.Fatalf("chmod view dir read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(viewDir, 0o755) })
	// PROBE the host rather than branch on GOOS: where the read-only bit does not stop the
	// owner from creating a file, this scenario cannot be built at all. The staging temp is
	// then created, the promote runs, and what the op meets is the ORDINARY directory occupant
	// - which is now preserved aside, not dead-lettered. Asserting a dead-letter there was this
	// test passing for a reason it does not name: the fs.ErrExist arm, never the permission one.
	// permissionBlockedByDirTarget keeps its own direct coverage in TestPermissionBlockedByDirTarget,
	// which needs no filesystem trick.
	if probe, err := os.CreateTemp(viewDir, "probe-*"); err == nil {
		name := probe.Name()
		probe.Close()
		_ = os.Remove(name)
		t.Skip("this host lets the owner create files inside a read-only directory, so the " +
			"permission-blocked shape cannot be produced here")
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server file")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 11}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if err := st.PutBaseline(7, "foo", store.Baseline{FileID: "f1", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a permission-blocked download over a local dir, not wedge: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("ErrPermission op left queued (%d): it would wedge every later cycle", n)
	}
	// The blocking local directory must be untouched (never-delete policy).
	if fi, statErr := os.Stat(blocker); statErr != nil || !fi.IsDir() {
		t.Errorf("blocking local dir changed: isdir=%v err=%v", fi != nil && fi.IsDir(), statErr)
	}
	// The phantom FILE baseline is dropped, mirroring the fs.ErrExist path.
	if _, err := st.GetBaseline(7, "foo"); !isNotFound(err) {
		t.Errorf("phantom file baseline over a local dir not dropped, got err %v", err)
	}
	if dl, err := st.DeadLetterLen(); err != nil || dl != 1 {
		t.Errorf("dead-letter count = %d (err %v), want 1", dl, err)
	}
}

// TestDrainQueueDeadLettersRenameOverLocalDir confirms an inbound rename whose
// destination path is occupied by a local untracked DIRECTORY is dead-lettered rather
// than wedging: fsutil.Rename(src, destdir) fails with EEXIST (fs.ErrExist), which the
// classifier now routes through the same dead-letter path as ENOTDIR.
func TestDrainQueueDeadLettersRenameOverLocalDir(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	// The moved node sits at src.txt on disk; an untracked directory occupies the
	// rename destination.
	if err := os.WriteFile(filepath.Join(viewDir, "src.txt"), []byte("bytes"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "dst", "child"), 0o755); err != nil {
		t.Fatalf("seed blocking dest dir: %v", err)
	}
	if err := st.PutBaseline(7, "src.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "src.txt", NewPath: "dst", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a rename onto a local dir, not wedge: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("EEXIST rename left queued (%d): it would wedge every later cycle", n)
	}
	// The blocking destination directory must be untouched (never-delete policy).
	if fi, err := os.Stat(filepath.Join(viewDir, "dst")); err != nil || !fi.IsDir() {
		t.Errorf("blocking dest dir changed: isdir=%v err=%v", fi != nil && fi.IsDir(), err)
	}
	// The source file must still be on disk (the move never ran).
	if _, err := os.Stat(filepath.Join(viewDir, "src.txt")); err != nil {
		t.Errorf("source file vanished after a rejected rename: %v", err)
	}
	if dl, err := st.DeadLetterLen(); err != nil || dl != 1 {
		t.Errorf("dead-letter count = %d (err %v), want 1", dl, err)
	}
}

// TestPermissionBlockedByDirTarget pins the classifier contract for the permission arm,
// emulated without Windows: only a file-placing op - a download, or a file
// move's destination - whose resolved on-disk target is an existing DIRECTORY reads a
// permission failure as unappliable. Every other permission shape stays transient: a
// regular-file occupant (a Windows read-only-attribute occupant / transient ACL noise on
// the parent), an absent target, and the plain mkdir/remove verbs (a mkdir's target
// existing as a directory is its success shape). The native Windows branch is exercised by
// windows-latest CI. The dir-placing verbs - a directory rename and the mkdir
// subtree relocate - are covered in dirovertarget_test.go.
func TestPermissionBlockedByDirTarget(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "dirblock"), 0o755); err != nil {
		t.Fatalf("seed dir occupant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "fileblock.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file occupant: %v", err)
	}
	// A dir occupant stored under the NFD spelling while the op carries the canonical NFC
	// path (nfcName/nfdName from normpath_boundary_test.go, guaranteed-distinct bytes): the
	// target must resolve through the same on-disk-spelling resolution the failing
	// promote/rename used, or the byte-preserving-FS shape would misread
	// as an absent target and stay transient.
	if err := os.MkdirAll(filepath.Join(viewDir, nfdName), 0o755); err != nil {
		t.Fatalf("seed NFD dir occupant: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	cases := []struct {
		name string
		op   store.Op
		want bool
	}{
		{"download onto dir occupant", store.Op{Type: store.OpDownload, ViewID: 7, Path: "dirblock"}, true},
		{"file rename onto dir occupant", store.Op{Type: store.OpRename, ViewID: 7, Path: "src.txt", NewPath: "dirblock"}, true},
		{"download onto NFD-spelled dir occupant", store.Op{Type: store.OpDownload, ViewID: 7, Path: nfcName}, true},
		// A DIR-placing rename onto a directory occupant is terminal too
		// (Unix already dead-lettered it via EEXIST); the dir-placing table lives in
		// dirovertarget_test.go.
		{"dir rename onto dir occupant", store.Op{Type: store.OpRename, IsDir: true, ViewID: 7, Path: "src", NewPath: "dirblock"}, true},
		{"file occupant stays transient", store.Op{Type: store.OpDownload, ViewID: 7, Path: "fileblock.txt"}, false},
		{"absent target stays transient", store.Op{Type: store.OpDownload, ViewID: 7, Path: "missing.txt"}, false},
		{"mkdir stays transient", store.Op{Type: store.OpMkdir, ViewID: 7, Path: "dirblock"}, false},
		{"remove stays transient", store.Op{Type: store.OpRemove, ViewID: 7, Path: "dirblock"}, false},
	}
	for _, tc := range cases {
		if got := e.permissionBlockedByDirTarget(tc.op); got != tc.want {
			t.Errorf("%s: permissionBlockedByDirTarget = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDownloadOverLocalDirThenRenameDoesNotDragDir guards the tree-corruption hazard a
// file/dir clash creates: a same-id server rename must never haul the USER's untracked
// directory across the tree, mislabelled as the server's file.
//
// The hazard used to be avoided by dropping the download and its phantom baseline, leaving the
// directory untracked at the path. Now the clash is resolved instead -- the directory is
// preserved aside as a keep-both copy and the server file lands at the path -- so the same-id
// rename moves the FILE that really is there. The hazard is gone by construction rather than by
// bookkeeping: the user's directory is not at the renamed path at all any more.
func TestDownloadOverLocalDirThenRenameDoesNotDragDir(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	blocker := filepath.Join(viewDir, "foo")
	if err := os.MkdirAll(filepath.Join(blocker, "child"), 0o755); err != nil {
		t.Fatalf("seed blocking dir: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server file")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 11}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	// Phase 1: the inbound download preserves the occupant aside and lands the server file.
	if err := st.PutBaseline(7, "foo", store.Baseline{FileID: "f1", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	for i := 0; i < 3; i++ {
		_ = e.drainQueue(context.Background())
		if n, _ := st.PendingLen(); n == 0 {
			break
		}
	}
	recs, err := st.ListConflicts(7)
	if err != nil || len(recs) != 1 {
		t.Fatalf("ListConflicts = %+v (err %v), want the occupant preserved", recs, err)
	}
	preserved := filepath.Join(viewDir, filepath.Base(recs[0].CopyPath))

	// Phase 2: the server renames the same file id.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", NewPath: "bar", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (rename): %v", err)
	}

	// The decisive assertion, unchanged in substance: the user's directory and its child are
	// wherever they were left, never hauled to `bar`.
	if fi, err := os.Stat(preserved); err != nil || !fi.IsDir() {
		t.Errorf("preserved dir gone: isdir=%v err=%v", fi != nil && fi.IsDir(), err)
	}
	if _, err := os.Stat(filepath.Join(preserved, "child")); err != nil {
		t.Errorf("preserved dir's child lost: %v", err)
	}
	// `bar` holds the server's FILE - moved there by the rename, never the user's directory
	// dragged across, which is what tree corruption would look like.
	fi, err := os.Lstat(filepath.Join(viewDir, "bar"))
	if err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("bar = %v (err %v), want the server file", fi, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "bar")); err != nil || string(got) != "server file" {
		t.Errorf("bar = %q (err %v), want the server's bytes, not the dragged directory", got, err)
	}
	// The bookkeeping half of the same hazard, and the reason this test is named for it: the
	// record at the destination must identify the SERVER's node, never the user's directory
	// mislabelled as it. The disk assertions above cannot catch a baseline that says "dir" over
	// a file that really landed, and that mislabelling is what a later scan turns into an
	// outbound remove of the live server node.
	if b, err := st.GetBaseline(7, "bar"); err != nil || b.FileID != "f1" || b.FileType == "dir" {
		t.Errorf("bar baseline = %+v (err %v), want the server node f1, not a dir record", b, err)
	}
}

// TestMkdirOverLocalFileThenRenameDoesNotDragFile is the symmetric guard to
// TestDownloadOverLocalDirThenRenameDoesNotDragDir: an untracked user FILE
// occupying a server directory's path must never be dragged across the tree or mislabelled as
// the directory node by a later rename of that node. The first drain
// does not dead-letter on the occupant: guardMkdirOccupant preserves the untracked file
// whole as a _Conflict copy (the never-lose default - no divergence base exists) and the
// directory lands on the freed path, correctly indexed as d1. The second drain's rename then
// legitimately moves the DIRECTORY - while the preserved user content stays put under its
// copy name, never relabelled as the server node.
func TestMkdirOverLocalFileThenRenameDoesNotDragFile(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	blocker := filepath.Join(viewDir, "foo")
	if err := os.WriteFile(blocker, []byte("user file"), 0o644); err != nil {
		t.Fatalf("seed blocking file: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	// Phase 1: the inbound mkdir of a server DIRECTORY over the blocking file preserves the
	// untracked file as a _Conflict copy (guardMkdirOccupant) and creates
	// the directory, so file id d1 is now correctly indexed at foo as a dir baseline.
	if err := st.PutBaseline(7, "foo", store.Baseline{FileID: "d1", FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", FileID: "d1",
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (mkdir): %v", err)
	}
	if p, err := st.PathByID(7, "d1"); err != nil || p != "foo" {
		t.Fatalf("dir node not indexed at foo after the occupant was preserved (path %q, err %v)", p, err)
	}
	if fi, err := os.Lstat(blocker); err != nil || !fi.IsDir() {
		t.Fatalf("server directory did not land at foo (isdir=%v err=%v)", fi != nil && fi.IsDir(), err)
	}
	copies := findConflictEntries(t, viewDir)
	if len(copies) != 1 {
		t.Fatalf("_Conflict entries = %v, want the untracked file preserved as exactly one copy", copies)
	}

	// Phase 2: the server renames the same DIRECTORY node (IsDir, as the move event reports
	// it). The node is genuinely tracked at foo now, so the rename moves the DIRECTORY to bar -
	// and must leave the preserved user content exactly where it is.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "foo", NewPath: "bar", FileID: "d1", IsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (rename): %v", err)
	}

	// The directory followed the server move; the preserved user file did not.
	if fi, err := os.Lstat(filepath.Join(viewDir, "bar")); err != nil || !fi.IsDir() {
		t.Errorf("renamed directory not at bar (isdir=%v err=%v)", fi != nil && fi.IsDir(), err)
	}
	if _, err := os.Lstat(blocker); !os.IsNotExist(err) {
		t.Errorf("foo still present after the rename (err %v)", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, copies[0]))
	if err != nil || string(got) != "user file" {
		t.Errorf("preserved copy = %q (err %v), want the user content untouched at its copy name", got, err)
	}
	// The user content must never end up mislabelled as the server directory node.
	if b, err := st.GetBaseline(7, "bar"); err != nil || b.FileType != "dir" || b.FileID != "d1" {
		t.Errorf("baseline at bar = %+v (err %v), want the dir node d1", b, err)
	}
}

// TestDrainQueueRejectsDotDotToViewRoot confirms a path that cleans to the view root
// or the sync root (".." under a view collapses to ".") is refused, so a remove can
// never target the view directory or the sync root itself.
func TestDrainQueueRejectsDotDotToViewRoot(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "keep"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// ".." under view "Photos" cleans to "." - the view root. A remove of it would
	// RemoveAll(viewDir) and wipe the whole view; it must be rejected instead.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "..", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop a '..' path, not wedge: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "keep")); err != nil {
		t.Errorf("view contents were removed by a '..' path: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("rejected op left queued (%d): it would wedge every later cycle", n)
	}
}

// TestDrainQueueOrdersRenameBeforeSourceDownload confirms a rename a->b is serialised
// against its SOURCE path, not only its destination: a later download at the rename's
// source must run after the move, so the move carries the old content to b and the
// download writes the new content at a. Without source serialisation the two race and
// the final state is non-deterministic.
func TestDrainQueueOrdersRenameBeforeSourceDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"fnew": []byte("new")},
		nodes:   map[string]actions.Node{"fnew": {Hash: "hn", Size: 3}},
		delay:   20 * time.Millisecond, // widen the race window the ordering must close
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Enqueue order = sync_id order: the move first, then the create at the old name.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "fnew",
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "old" {
		t.Errorf("b.txt = %q, want %q (the moved old content)", gotB, "old")
	}
	gotA, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read a.txt: %v", err)
	}
	if string(gotA) != "new" {
		t.Errorf("a.txt = %q, want %q (the newly downloaded content)", gotA, "new")
	}
}

// TestApplyRenameIdempotentOnReplay confirms a replayed rename whose source is already
// moved (and destination already in place) succeeds rather than failing forever, so a
// re-pull or crash-window duplicate does not wedge the queue.
//
// The moved node is TRACKED at the destination (the relocated StatePending record reconcile
// writes before the drain, which a prior apply's rename left behind when it crashed before the
// baseline write). That is the only shape a genuine replay can take: a node we materialised
// always carries a byid-indexed baseline. An UNTRACKED node with a present destination is the
// coldstart-gap case instead - the occupant is provably foreign, so the op dead-letters
// rather than stamping a false synced claim over it (see inbound_rename_coldstartgap_test.go).
func TestApplyRenameIdempotentOnReplay(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// Destination already present, source already gone: the move happened before.
	if err := os.WriteFile(filepath.Join(viewDir, "new.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	// ...and its baseline is the relocated pending record, so byid resolves f9 to new.txt.
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9",
		LocalHash: md4.ContentHash([]byte("body")), LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue on replayed rename: %v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("replayed rename left %d ops queued, want 0", n)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline(new): %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("moved baseline state = %q, want synced", b.State)
	}
}

// TestDrainQueueOrdersRemoveAfterDescendantDownload confirms an op on an ancestor path
// serialises against ops on its descendants, not only exact-path matches: a download to
// a/b.txt enqueued before a remove of a must complete before the remove runs, so the
// remove wipes the freshly written file along with its directory and the final state is
// deterministic. Without ancestor/descendant serialisation the two race and the slow
// download could recreate a/b.txt after the fast remove, leaving it stranded.
func TestDrainQueueOrdersRemoveAfterDescendantDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("data")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 4}},
		delay:   20 * time.Millisecond, // widen the race window the ordering must close
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Enqueue order = sync_id order: create the descendant first, then remove the
	// ancestor directory.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a/b.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "a", FileID: "d1",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "Photos", "a")); !os.IsNotExist(err) {
		t.Errorf("ancestor dir survived the remove (descendant download raced ahead): err=%v", err)
	}
}

// selectiveFailClient is a syncClient whose Download fails only for a chosen file id,
// so a test can fail one view's op while another view's op succeeds.
type selectiveFailClient struct {
	mu      sync.Mutex
	failID  string
	content map[string][]byte
	cur     int
	max     int
	delay   time.Duration
}

func (c *selectiveFailClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return nil, nil
}
func (c *selectiveFailClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *selectiveFailClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *selectiveFailClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *selectiveFailClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
	if fileID == c.failID {
		return actions.Node{}, errors.New("download boom")
	}
	if _, err := w.Write(c.content[fileID]); err != nil {
		return actions.Node{}, err
	}
	return actions.Node{Hash: "h"}, nil
}

// TestDrainQueueIndependentAcrossViews confirms the scheduler keys are scoped by view:
// the same relative path in two different views is an independent resource. The two
// downloads run concurrently, and a failure for one view's path does not skip the
// other view's identically-named path.
func TestDrainQueueIndependentAcrossViews(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "ViewA"}); err != nil {
		t.Fatalf("PutView 7: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "ViewB"}); err != nil {
		t.Fatalf("PutView 8: %v", err)
	}
	root := t.TempDir()
	dl := &selectiveFailClient{
		failID:  "bad",
		content: map[string][]byte{"good": []byte("ok")},
		delay:   20 * time.Millisecond,
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// view 7's a.txt fails; view 8's identically-named a.txt must still be applied.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "bad",
	}); err != nil {
		t.Fatalf("Enqueue view7: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 8, Path: "a.txt", FileID: "good",
	}); err != nil {
		t.Fatalf("Enqueue view8: %v", err)
	}
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the view-7 failure surfaced")
	}

	// view 8's file applied despite the same relative path failing in view 7.
	got, err := os.ReadFile(filepath.Join(root, "ViewB", "a.txt"))
	if err != nil {
		t.Fatalf("view 8 file not applied (cross-view skip): %v", err)
	}
	if string(got) != "ok" {
		t.Errorf("view 8 content = %q, want %q", got, "ok")
	}
	if dl.max != 2 {
		t.Errorf("max concurrent downloads across views = %d, want 2", dl.max)
	}
}

// TestAbsRejectsEscapingViewName confirms a server-supplied view name that escapes the
// sync root (e.g. "../evil") is refused, so a malicious name cannot redirect writes
// outside the sync root even when the relative path itself is local.
func TestAbsRejectsEscapingViewName(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "../evil"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop an escaping view name, not wedge: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "evil", "a.txt")); !os.IsNotExist(err) {
		t.Errorf("escaping view name wrote outside the sync root: err=%v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("rejected op left queued (%d): it would wedge every later cycle", n)
	}
}

// TestAbsRejectsSymlinkComponent confirms a pre-existing symlink in the view tree is
// refused, so a remote apply cannot follow it outside the sync root - a gap a purely
// lexical check (filepath.IsLocal) cannot close.
func TestAbsRejectsSymlinkComponent(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir() // a directory outside the sync root the symlink points at
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "link/escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The symlink can never be applied (the engine never creates symlinks), so the op is
	// dead-lettered: drainQueue returns nil, the op is dropped from the queue, and nothing
	// escapes through the symlink.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a symlink path, not wedge: %v", err)
	}
	if pending, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(pending) != 0 {
		t.Fatalf("queue = %+v, want empty (symlink op dead-lettered)", pending)
	}
	if _, err := os.Stat(filepath.Join(outside, "escape.txt")); !os.IsNotExist(err) {
		t.Errorf("download escaped through a symlink: err=%v", err)
	}
}

// TestAbsRejectsSymlinkInMultiSegmentViewName confirms an intermediate component of a
// multi-segment view name is symlink-walked too: a view named "Inner/Deep" materialises
// as nested directories, and a pre-existing symlink at the intermediate "Inner" must be
// refused. A naive check that treats the whole name as one opaque element would Lstat
// only the joined leaf, following "Inner" out of the sync root undetected.
func TestAbsRejectsSymlinkInMultiSegmentViewName(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Inner/Deep"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir() // a directory outside the sync root the symlink points at
	// Pre-create the nested target so a write through the symlink would actually
	// succeed (and escape) absent the symlink check - otherwise the missing dir would
	// fail the write for an unrelated reason and mask the gap.
	if err := os.MkdirAll(filepath.Join(outside, "Deep"), 0o755); err != nil {
		t.Fatalf("seed outside dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "Inner")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// As above, the symlink path is dead-lettered rather than wedging the queue.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a symlink view name, not wedge: %v", err)
	}
	if pending, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(pending) != 0 {
		t.Fatalf("queue = %+v, want empty (symlink op dead-lettered)", pending)
	}
	if _, err := os.Stat(filepath.Join(outside, "Deep", "escape.txt")); !os.IsNotExist(err) {
		t.Errorf("download escaped through the view-name symlink: err=%v", err)
	}
}

// TestRenameChainAfterDeadLetteredDownloadDoesNotMoveForeignData reproduces this shape: a
// pre-existing symlink at the path a node is first created at dead-letters that node's
// download, but the same batch then moves the node a->b->c. Because the dead-lettered
// download is acked (dropped) rather than left to block the chain, the trailing rename
// would otherwise move whatever unrelated bytes happen to sit at b onto c and stamp them
// with the moved node's identity. The feeder-doom rule must drop the dependent renames so
// the foreign data at b is never moved and no false synced baseline is recorded.
func TestRenameChainAfterDeadLetteredDownloadDoesNotMoveForeignData(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A pre-existing symlink at a.txt: the engine never creates symlinks, so f9's download
	// to a.txt is dead-lettered (and so is the on-disk move out of a.txt).
	if err := os.Symlink(outside, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	// Unrelated data at b.txt that the f9 move chain must never drag to c.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f9", Path: "a.txt", Hash: "h9", Size: 8},
		{SyncID: 2, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 3, Action: "move", FileID: "f9", FromPath: "b.txt", Path: "c.txt"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should not wedge: %v", err)
	}

	// The foreign data must still be at b.txt, untouched - never dragged onto c.txt.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil {
		t.Errorf("foreign b.txt was moved/removed: %v", err)
	} else if string(got) != "foreign" {
		t.Errorf("b.txt = %q, want %q (foreign data corrupted)", got, "foreign")
	}
	// c.txt holds f9's own SERVER bytes, fetched by the compensating download the last doomed
	// hop leaves behind (finding H2) - never the foreign bytes from b.txt, which is the drag
	// this test exists to refuse. Before that repair the node stayed missing from the replica
	// forever, which is what the chain's original assertion (c.txt absent) pinned.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err != nil || string(got) != "f9-bytes" {
		t.Errorf("c.txt = %q (err %v), want the re-requested server node %q", got, err, "f9-bytes")
	}
	// The stranded phantom is still gone: the record at c.txt is the compensation's own, written
	// by an apply that really placed the node there, not the relocated pending one the doomed
	// renames must drop.
	if b, err := st.GetBaseline(7, "c.txt"); err != nil || b.FileID != "f9" || b.State != store.StateSynced {
		t.Errorf("c.txt baseline = %+v (err %v), want the re-requested f9 recorded synced", b, err)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f9) = %q (err %v), want f9 tracked where it was actually materialised", p, err)
	}
	// The dead-lettered download and the doomed renames must all be dropped, not wedged.
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestDoomedRenameCleanupFailureMarksOpBad covers this shape: when a doomed rename's cleanup
// (dropStrandedBaseline/Ack) fails with a store-level error (ENOSPC/corruption), the goroutine
// must mark the op bad - not merely record firstErr - so a conflicting successor in the same
// batch skips and stays queued rather than applying out of order on the same path. The store
// failure is injected through the dropDoomedRename seam; the conflicting mkdir successor's
// disk effect (creating the directory) is the observable that must NOT happen.
func TestDoomedRenameCleanupFailureMarksOpBad(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A pre-existing symlink at a.txt dead-letters f9's download (the engine never creates
	// symlinks), so the rename feeding off a.txt is doomed.
	if err := os.Symlink(outside, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	// Inject a store-level failure into the doomed rename's cleanup. Only this seam fails - the
	// rest of the store works, so the mkdir successor would apply (and create c.txt) if it were
	// not correctly skipped.
	wantErr := errors.New("injected store failure")
	orig := dropDoomedRename
	dropDoomedRename = func(*Engine, store.QueuedOp) error { return wantErr }
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// op1: download f9 to a.txt → dead-lettered (symlink). op2: rename a.txt→c.txt (feeder op1,
	// doomed). op3: mkdir at c.txt - a successor conflicting with the rename's destination.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "c.txt", FileID: "fdir",
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}

	// drainQueue surfaces the injected cleanup error.
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("drainQueue err = %v, want injected %v", err, wantErr)
	}

	// The conflicting mkdir successor must NOT have applied: c.txt must not be a directory.
	if fi, err := os.Lstat(filepath.Join(viewDir, "c.txt")); err == nil && fi.IsDir() {
		t.Errorf("c.txt is a directory: the mkdir successor applied out of order behind a doomed op")
	}

	// Both the un-acked doomed rename and the skipped mkdir successor must remain queued for
	// the next drain.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	var haveMkdir, haveRename bool
	for _, p := range pending {
		switch p.Type {
		case store.OpMkdir:
			haveMkdir = true
		case store.OpRename:
			haveRename = true
		}
	}
	if !haveMkdir {
		t.Errorf("mkdir successor not queued (applied out of order?); pending=%+v", pending)
	}
	if !haveRename {
		t.Errorf("doomed rename not left queued for retry; pending=%+v", pending)
	}
}

// TestRenameAfterLostFeederDoesNotMoveForeignData covers the cross-batch case: the
// in-memory feeder/doom chain only spans one drain batch, so a dependency chain split across
// the 256-op boundary loses the dead mark - the trailing rename starts a fresh batch with no
// feeder reference. The defence is that the doom cleanup keeps the byid index honest (the
// never-materialised node is untracked), so applyRename's own foreign-source guard catches the
// rename regardless of the lost feeder. Here f9 is untracked (its producing ops were dropped in
// an earlier batch) while a foreign node sits at the rename's source; the move must be dropped
// and the foreign bytes left in place rather than dragged onto the destination.
func TestRenameAfterLostFeederDoesNotMoveForeignData(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A foreign node tracked at b.txt - the rename's source. Its bytes must never reach c.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fb", FileType: "file", ServerHash: "hb", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}
	// f9 is untracked: its create/early-move were dropped and cleaned up in an earlier batch.
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should not wedge: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil {
		t.Errorf("foreign b.txt was moved/removed: %v", err)
	} else if string(got) != "foreign" {
		t.Errorf("b.txt = %q, want %q (foreign data corrupted)", got, "foreign")
	}
	// What lands at c.txt is f9's own server copy, re-requested by the dropped rename (finding
	// H2) - the dlClient scripts no bytes for it, so an empty file. It is never the foreign
	// bytes, which is the drag this guard refuses.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err == nil && string(got) == "foreign" {
		t.Errorf("c.txt = %q: the lost-feeder rename dragged the foreign data across", got)
	}
	// The foreign node's baseline at b.txt must be untouched; f9 is tracked only where the
	// compensating download actually placed it, never over the foreign node.
	if b, err := st.GetBaseline(7, "b.txt"); err != nil {
		t.Errorf("foreign b.txt baseline lost: %v", err)
	} else if b.FileID != "fb" {
		t.Errorf("b.txt baseline = %+v, want foreign fb", b)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f9) = %q (err %v), want f9 tracked at the re-requested c.txt", p, err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestRenameDoomedByDeadFeederFromEarlierDrainWindow pins the cross-window doom carry: the doom of
// a dead-lettered feeder must survive a DrainBatch window boundary. drainQueue pages the queue
// window by window; the in-memory feeder lookup only scans the CURRENT window's scheduled ops,
// and blockedKeys carries only the keys of ops a window left QUEUED (bad) - a DEAD op's mark
// left with its window. So a boundary falling between an op that drops without placing its node
// and the rename fed by it left that rename with neither a feeder nor a block, and it fell
// through to applyRename's own guards: a same-kind untracked local file at its source passes
// them (byid miss + matching kind), so the user's file was hauled onto the destination and
// stamped with the moved node's identity as a bare synced record.
//
// Here hop 1 (x.txt->b.txt for f9) dead-letters - a DIFFERENT node's baseline sits at its source
// while f9 is untracked - and hop 2 (b.txt->c.txt for the same f9) is scheduled in the next
// window with the user's untracked b.txt on disk. It must be doomed, leaving the user's file
// exactly where it is.
func TestRenameDoomedByDeadFeederFromEarlierDrainWindow(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A foreign node tracked at the first hop's source: with f9 itself untracked, applyRename
	// drops hop 1 rather than dragging the foreign node across (the srcRecreated guard).
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fb", FileType: "file", ServerHash: "hb", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}
	// The user's own untracked file at hop 2's source. Nothing the server asked for touches it.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("user"), 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	// One op per drain window, so the two hops land in separate windows and the doom must
	// cross the boundary to reach hop 2.
	e.limits.DrainBatch = 1

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "x.txt", NewPath: "b.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue hop 1: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue hop 2: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should not wedge: %v", err)
	}

	// The user's file must still be at b.txt, byte-identical and never moved.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil {
		t.Errorf("user b.txt was moved/removed: %v", err)
	} else if string(got) != "user" {
		t.Errorf("b.txt = %q, want %q", got, "user")
	}
	// c.txt holds f9's own server copy (re-requested by the doomed hop's drop, finding H2),
	// never the user's file - the haul this guard refuses.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err == nil && string(got) == "user" {
		t.Errorf("c.txt = %q: the doomed rename hauled the user's file across", got)
	}
	// The bare-stamp anomaly stays closed: the record at c.txt is not a BARE stamp over
	// whatever the rename found, but the compensating download's own synced record.
	if b, err := st.GetBaseline(7, "c.txt"); err != nil || b.FileID != "f9" || b.LocalHash == "" {
		t.Errorf("c.txt baseline = %+v (err %v), want the compensating download's own record", b, err)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f9) = %q (err %v), want f9 tracked where it was materialised", p, err)
	}
	// The foreign node hop 1 refused to move is untouched.
	if b, err := st.GetBaseline(7, "x.txt"); err != nil {
		t.Errorf("foreign x.txt baseline lost: %v", err)
	} else if b.FileID != "fb" {
		t.Errorf("x.txt baseline = %+v, want foreign fb", b)
	}
	// Both hops left the queue with a durable trace rather than wedging it.
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, err := st.DeadLetterLen(); err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	} else if n != 2 {
		t.Errorf("DeadLetterLen = %d, want 2 (both hops dropped)", n)
	}
}

// TestRenameNotDoomedAfterLaterWindowReMaterialisesNode pins the other half of the cross-window
// doom carry: a dead output must be cleared by a LATER op of the same node that genuinely places it
// at the same path, or the cross-window doom would start dropping legitimate moves. Window 1
// drops a hop into a.txt (f9 untracked, a foreign node at its source), window 2 downloads f9
// onto a.txt for real, and window 3's rename off a.txt must run.
func TestRenameNotDoomedAfterLaterWindowReMaterialisesNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fb", FileType: "file", ServerHash: "hb", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}

	body := []byte("f9-bytes")
	dl := &dlClient{
		content: map[string][]byte{"f9": body},
		nodes:   map[string]actions.Node{"f9": {Hash: md4.ContentHash(body), Size: int64(len(body))}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	e.limits.DrainBatch = 1

	// Window 1: a doomed hop into a.txt. Window 2: the real download of f9 onto a.txt.
	// Window 3: the move a.txt->c.txt, which must NOT inherit window 1's dead mark.
	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "x.txt", NewPath: "a.txt", FileID: "f9"},
		{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %s: %v", op.Type, err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err != nil {
		t.Fatalf("c.txt missing: the legitimate move was wrongly doomed: %v", err)
	} else if !bytes.Equal(got, body) {
		t.Errorf("c.txt = %q, want %q", got, body)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt still present after the move: err=%v", err)
	}
	if b, err := st.GetBaseline(7, "c.txt"); err != nil {
		t.Errorf("c.txt baseline missing: %v", err)
	} else if b.FileID != "f9" || b.State != store.StateSynced {
		t.Errorf("c.txt baseline = %+v, want f9 synced", b)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestMoveThenRecreateSourceKeepsBaselines reproduces the move-then-recreate batch: a
// move a->b followed in the same pull by a create of a brand-new node at a. Run through
// the real reconcile+executor, the destination baseline must keep the MOVED node's
// identity (f9), not be stamped with the new node's id or left missing, and the source
// must carry the new node (fnew). Disk content for both must be correct.
func TestMoveThenRecreateSourceKeepsBaselines(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"fnew": []byte("new")},
		nodes:   map[string]actions.Node{"fnew": {Hash: "hn", Size: 3}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// One batch: move a->b (sync 1), then create a brand-new node at a (sync 2).
	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 2, Action: "create", FileID: "fnew", Path: "a.txt", Hash: "hn", Size: 3},
	}
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "old" {
		t.Errorf("b.txt = %q, want %q (the moved old content)", gotB, "old")
	}
	gotA, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read a.txt: %v", err)
	}
	if string(gotA) != "new" {
		t.Errorf("a.txt = %q, want %q (the newly created node)", gotA, "new")
	}

	bb, err := st.GetBaseline(3, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if bb.FileID != "f9" {
		t.Errorf("destination baseline FileID = %q, want f9 (the moved node, not the new one)", bb.FileID)
	}
	if bb.ServerHash != "h9" {
		t.Errorf("destination baseline ServerHash = %q, want h9 (the moved node's facts preserved, not lost)", bb.ServerHash)
	}
	if bb.State != store.StateSynced {
		t.Errorf("destination baseline state = %q, want synced", bb.State)
	}
	ba, err := st.GetBaseline(3, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline(a): %v", err)
	}
	if ba.FileID != "fnew" {
		t.Errorf("source baseline FileID = %q, want fnew (the recreated node)", ba.FileID)
	}
}

// TestMoveThenRecreateReplayDoesNotCorruptDest reproduces the dangerous replay of a
// move-then-recreate batch: after the batch has fully applied once (b holds the moved
// content, a holds the recreated node), the same events are reconciled and drained
// again - as a crash between enqueue and the cursor advance would force. The replayed
// rename must NOT drag the recreated node at a over the correct destination b; b must
// keep the moved content and identity, and a must keep the recreated node.
func TestMoveThenRecreateReplayDoesNotCorruptDest(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The recreated node's hash is the real MD4 of its content so the baseline records the
	// content's true hash after the first download: on replay the inbound download's Phase-9d
	// divergence guard re-hashes the on-disk file and finds it matches the recorded base, so it
	// does NOT funnel - matching the production state where an unmodified downloaded file hashes
	// to its baseline hash.
	newHash := md4.ContentHash([]byte("new"))
	dl := &dlClient{
		content: map[string][]byte{"fnew": []byte("new")},
		nodes:   map[string]actions.Node{"fnew": {Hash: newHash, Size: 3, MTime: 1700000000}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 2, Action: "create", FileID: "fnew", Path: "a.txt", Hash: newHash, Size: 3, MTime: 1700000000},
	}
	// Apply the batch once...
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue 1: %v", err)
	}
	// ...then replay the very same events (cursor never advanced).
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile 2 (replay): %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue 2 (replay): %v", err)
	}

	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "old" {
		t.Errorf("b.txt = %q, want %q - the replay corrupted the destination", gotB, "old")
	}
	gotA, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read a.txt: %v", err)
	}
	if string(gotA) != "new" {
		t.Errorf("a.txt = %q, want %q (the recreated node)", gotA, "new")
	}
	bb, err := st.GetBaseline(3, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if bb.FileID != "f9" || bb.ServerHash != "h9" {
		t.Errorf("destination baseline = %+v, want the moved node f9/h9 intact", bb)
	}
}

// TestDrainQueueSerializesSameDirAcrossViews confirms the scheduler keys ops by their
// resolved on-disk location, not the abstract view id: two views that share a name
// resolve to the same directory, so identically-pathed ops in them must serialise
// rather than racing on the same files.
func TestDrainQueueSerializesSameDirAcrossViews(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Shared"}); err != nil {
		t.Fatalf("PutView 7: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "Shared"}); err != nil { // same on-disk dir
		t.Fatalf("PutView 8: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("data")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 4}},
		delay:   20 * time.Millisecond, // widen the race window the serialisation must close
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	for _, view := range []uint64{7, 8} {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: view, Path: "a.txt", FileID: "f1",
		}); err != nil {
			t.Fatalf("Enqueue view %d: %v", view, err)
		}
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if dl.max != 1 {
		t.Errorf("max concurrent downloads on one shared dir = %d, want 1 (must serialise)", dl.max)
	}
}

// TestApplyOverwriteMove confirms a remote move whose destination already exists on
// disk (an overwrite move) is applied, not skipped as a replay: the moved content
// replaces the destination, the source is gone, and the destination baseline carries
// the moved node's facts. Run through the real reconcile (which relocates the source
// baseline before the drain), so srcIsMoved is false at apply time - the case that
// previously caused the move to be wrongly skipped.
func TestApplyOverwriteMove(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("moved"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(3, "b.txt", store.Baseline{FileID: "fy", FileType: "file", ServerHash: "hy", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// One overwrite move: a.txt -> b.txt, where b.txt already exists.
	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
	}
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "moved" {
		t.Errorf("b.txt = %q, want %q (the overwrite move was skipped)", gotB, "moved")
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("source a.txt survived the overwrite move: err=%v", err)
	}
	b, err := st.GetBaseline(3, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if b.FileID != "f9" || b.ServerHash != "h9" {
		t.Errorf("destination baseline = %+v, want the moved node f9/h9", b)
	}
	if b.State != store.StateSynced {
		t.Errorf("destination baseline state = %q, want synced", b.State)
	}
}

// TestConsecutiveMovesKeepBaselineMetadata reproduces a batch of two moves of the same
// node, a->b then b->c. The final baseline at c must retain the moved node's full
// facts (id, hash), not collapse to a bare id-only record as the intermediate hop's
// baseline derivation previously caused.
func TestConsecutiveMovesKeepBaselineMetadata(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", ServerMTime: 1700000000, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 2, Action: "move", FileID: "f9", FromPath: "b.txt", Path: "c.txt"},
	}
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	gotC, err := os.ReadFile(filepath.Join(viewDir, "c.txt"))
	if err != nil {
		t.Fatalf("read c.txt: %v", err)
	}
	if string(gotC) != "body" {
		t.Errorf("c.txt = %q, want %q", gotC, "body")
	}
	for _, gone := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived the move chain: err=%v", gone, err)
		}
	}
	c, err := st.GetBaseline(3, "c.txt")
	if err != nil {
		t.Fatalf("GetBaseline(c): %v", err)
	}
	if c.FileID != "f9" || c.FileType != "file" || c.ServerHash != "h9" || c.ServerMTime != 1700000000 {
		t.Errorf("final baseline = %+v, want full f9 facts preserved (not a bare id)", c)
	}
	if c.State != store.StateSynced {
		t.Errorf("final baseline state = %q, want synced", c.State)
	}
	if _, err := st.GetBaseline(3, "b.txt"); !isNotFound(err) {
		t.Errorf("intermediate baseline at b.txt survived: err=%v", err)
	}
}

// TestCreateThenDeleteDoesNotWedge reproduces a batch that creates a file and then
// deletes it before the engine can download it. The download of the already-gone node
// must not be enqueued (it would fail forever and block its trailing remove), so the
// drain completes cleanly with nothing left on disk or in the queue. The download
// client fails on any call, so a wrongly-enqueued download would surface as an error.
func TestCreateThenDeleteDoesNotWedge(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "ghost.txt", Hash: "h", Size: 3},
		{SyncID: 2, Action: "delete", FileID: "f1", Path: "ghost.txt"},
	}
	ops, _, err := e.reconcile(3, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want only the remove (the doomed download elided)", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a doomed download: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "ghost.txt")); !os.IsNotExist(err) {
		t.Errorf("ghost.txt present after create+delete: err=%v", err)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := st.GetBaseline(3, "ghost.txt"); !isNotFound(err) {
		t.Errorf("baseline survived create+delete: err=%v", err)
	}
}

// TestApplyRenameReplayPreservesDestMetadata confirms a replayed rename whose source is
// already gone does not overwrite the destination baseline (carrying full facts from
// the first apply) with a bare file-id-only record.
func TestApplyRenameReplayPreservesDestMetadata(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "old.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", ServerMTime: 1700000000, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// First apply: moves old->new, leaving the full baseline at new.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue first: %v", err)
	}

	// Replay the same rename: source is gone, destination is in place.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue replay: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue replay: %v", err)
	}

	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline(new): %v", err)
	}
	if b.FileID != "f9" || b.FileType != "file" || b.ServerHash != "h9" || b.ServerMTime != 1700000000 {
		t.Errorf("replay clobbered destination baseline: %+v, want full f9 facts preserved", b)
	}
	if b.State != store.StateSynced {
		t.Errorf("destination baseline state = %q, want synced", b.State)
	}
}

// TestApplyOverwriteMoveWithSourceRecreate covers this shape: an overwrite move (the
// destination b.txt already exists on disk) combined IN THE SAME BATCH with a recreate
// of the source. After reconcile the source baseline already names the recreated node,
// so srcRecreated is true; the destination already exists on disk, so the previous
// destExists-based test wrongly classified the first apply as a superseded replay and
// skipped the rename, leaving stale content at b. The move must run: b ends with the
// moved content and a with the freshly created node.
func TestApplyOverwriteMoveWithSourceRecreate(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("moved"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(3, "b.txt", store.Baseline{FileID: "fy", FileType: "file", ServerHash: "hy", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"fnew": []byte("new")},
		nodes:   map[string]actions.Node{"fnew": {Hash: "hn", Size: 3}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// One batch: overwrite move a->b (b already exists), then recreate a brand-new node
	// at the source path a.
	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 2, Action: "create", FileID: "fnew", Path: "a.txt", Hash: "hn", Size: 3},
	}
	if _, _, err := e.reconcile(3, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "moved" {
		t.Errorf("b.txt = %q, want %q (the overwrite move was skipped, leaving stale content)", gotB, "moved")
	}
	gotA, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read a.txt: %v", err)
	}
	if string(gotA) != "new" {
		t.Errorf("a.txt = %q, want %q (the recreated node)", gotA, "new")
	}
	bb, err := st.GetBaseline(3, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if bb.FileID != "f9" || bb.ServerHash != "h9" {
		t.Errorf("destination baseline = %+v, want the moved node f9/h9", bb)
	}
	ba, err := st.GetBaseline(3, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline(a): %v", err)
	}
	if ba.FileID != "fnew" {
		t.Errorf("source baseline FileID = %q, want fnew (the recreated node)", ba.FileID)
	}
}

// TestModifyMoveDeleteDoesNotWedge covers this shape: a batch that modifies a, moves a->b,
// then deletes b. The download seeded by the modify targets a node that the batch goes
// on to move and delete, so by apply time the server node is gone and the download
// would fail forever, blocking the conflicting rename/remove and wedging the queue.
// supersededByLaterDelete must follow the node through the move and elide the doomed
// download; the rename and remove then carry the path to its deleted end state.
func TestModifyMoveDeleteDoesNotWedge(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// a.txt exists from a prior sync; the batch modifies, moves, then deletes it.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{FileID: "f1", FileType: "file", ServerHash: "h0", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The download client fails on any call, so a wrongly-enqueued (doomed) download
	// would surface as an error rather than silently succeeding.
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 3},
		{SyncID: 2, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 3, Action: "delete", FileID: "f1", Path: "b.txt"},
	}
	ops, _, err := e.reconcile(3, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The doomed download is elided; only the rename and remove remain.
	if len(ops) != 2 || ops[0].Type != store.OpRename || ops[1].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [rename, remove] (the doomed download elided)", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a doomed download: %v", err)
	}
	for _, gone := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, gone)); !os.IsNotExist(err) {
			t.Errorf("%s present after modify+move+delete: err=%v", gone, err)
		}
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := st.GetBaseline(3, "b.txt"); !isNotFound(err) {
		t.Errorf("baseline survived modify+move+delete: err=%v", err)
	}
}

// TestAbsRejectsDotViewName covers this shape: a view name that filepath.IsLocal accepts
// but that collapses to the view root - "." or "a/.." - must be refused, since it would
// resolve the view to the sync root itself and let it clobber another view's directory.
func TestAbsRejectsDotViewName(t *testing.T) {
	for _, name := range []string{".", "a/.."} {
		st := newTestStore(t)
		if err := st.PutView(7, store.View{Name: name}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)
		if _, err := e.abs(7, "a.txt"); !errors.Is(err, errRejectedPath) {
			t.Errorf("abs with view name %q: err = %v, want errRejectedPath", name, err)
		}
	}
}

// TestReconcileDeleteDropsQueuedDownload covers the cross-batch case: a download
// enqueued by an earlier batch that has not applied (the node is being deleted
// server-side, so it keeps failing) is dropped from the durable queue when the delete
// event arrives in a later batch, so the remove no longer chains behind a doomed download
// and the queue converges instead of wedging.
func TestReconcileDeleteDropsQueuedDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// An earlier batch enqueued a download for the (now to-be-deleted) node and marked it
	// pending; the download never applied.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 3, Path: "ghost.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue prior download: %v", err)
	}
	if err := st.PutBaseline(3, "ghost.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	root := t.TempDir()
	// The download client fails on any call, proving the dropped download is never run.
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	delEvents := []actions.Event{{SyncID: 9, Action: "delete", FileID: "f1", Path: "ghost.txt"}}
	ops, cursor, err := e.reconcile(3, delEvents, 4)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 9 {
		t.Errorf("cursor = %d, want 9", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want only the remove", ops)
	}
	// The queue now holds only the remove - the doomed download was dropped.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpRemove {
		t.Fatalf("queue = %+v, want only the remove (doomed download dropped)", pending)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged after a cross-batch delete: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "ghost.txt")); !os.IsNotExist(err) {
		t.Errorf("ghost.txt present after delete: err=%v", err)
	}
}

// TestReconcileAncestorDeleteDropsLargeBacklog covers this shape: a single ancestor-
// directory delete can supersede a whole backlog of queued downloads. The drop must page
// through them (bounded memory) and still ack every one - more than dropPageSize - so the
// queue converges. The download client fails on any call, proving none of the superseded
// downloads is ever run.
func TestReconcileAncestorDeleteDropsLargeBacklog(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Enqueue a backlog larger than one page, all under the dir/ subtree the delete removes.
	const n = dropPageSize*2 + 7
	for i := 0; i < n; i++ {
		if _, err := st.Enqueue(store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 3, Path: fmt.Sprintf("dir/f%d.txt", i), FileID: fmt.Sprintf("f%d", i),
		}); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	delEvents := []actions.Event{{SyncID: 9, Action: "delete", FileID: "fd", Path: "dir"}}
	ops, _, err := e.reconcile(3, delEvents, 4)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want only the remove", ops)
	}
	// Every superseded download was paged out; only the remove remains.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpRemove {
		t.Fatalf("queue has %d ops, want only the remove (whole backlog dropped)", len(pending))
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged: %v", err)
	}
	if got, _ := st.PendingLen(); got != 0 {
		t.Errorf("queue not drained: %d ops remain", got)
	}
}

// TestReconcileMoveThenDeleteDropsQueuedDownload covers the moved-path variant:
// a download queued by an earlier batch (never applied, the node being deleted
// server-side) where a later batch moves the node before deleting it. The queue still
// records the pre-move path, so a bare comparison of the delete against that path misses
// it; dropSupersededQueuedDownloads must follow the node through the move. The queued
// download is doomed and would wedge the queue behind its own rename/remove, and the
// rename of the never-materialised source must not wedge either.
func TestReconcileMoveThenDeleteDropsQueuedDownload(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// An earlier batch enqueued a download for node f1 at "a.txt" and marked it pending;
	// the download never applied, so nothing is on disk at a.txt.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 3, Path: "a.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue prior download: %v", err)
	}
	if err := st.PutBaseline(3, "a.txt", store.Baseline{FileID: "f1", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	root := t.TempDir()
	// The download client fails on any call, proving the dropped download is never run.
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	// This batch moves the node a.txt->b.txt, then deletes b.txt.
	events := []actions.Event{
		{SyncID: 7, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 8, Action: "delete", FileID: "f1", Path: "b.txt"},
	}
	ops, cursor, err := e.reconcile(3, events, 4)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 8 {
		t.Errorf("cursor = %d, want 8", cursor)
	}
	if len(ops) != 2 || ops[0].Type != store.OpRename || ops[1].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [rename, remove] (the doomed download dropped)", ops)
	}
	// The prior download was dropped, so the queue holds only the rename and remove.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 2 || pending[0].Type != store.OpRename || pending[1].Type != store.OpRemove {
		t.Fatalf("queue = %+v, want [rename, remove] (doomed download dropped)", pending)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a moved-then-deleted queued download: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	for _, gone := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(root, "Photos", gone)); !os.IsNotExist(err) {
			t.Errorf("%s present after move+delete: err=%v", gone, err)
		}
	}
	if _, err := st.GetBaseline(3, "b.txt"); !isNotFound(err) {
		t.Errorf("baseline survived move+delete: err=%v", err)
	}
}

// TestReconcileMoveToEscapingDestKeepsSourceBaseline covers this shape: a move whose
// destination escapes the view root must be skipped BEFORE the source baseline is
// relocated. The old code relocated the source baseline to the invalid destination and
// then dead-lettered the op at apply time, stranding the source node untracked. The fix
// leaves the source baseline intact.
func TestReconcileMoveToEscapingDestKeepsSourceBaseline(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	src := store.Baseline{FileID: "f9", FileType: "file", ServerHash: "h9", ServerMTime: 1700000000, State: store.StateSynced}
	if err := st.PutBaseline(3, "a.txt", src); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)

	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "../evil"}}
	ops, cursor, err := e.reconcile(3, move, 4)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("ops = %+v, want none (escaping move skipped)", ops)
	}
	if cursor != 5 {
		t.Errorf("cursor = %d, want 5 (the unappliable event is still acknowledged)", cursor)
	}
	got, err := st.GetBaseline(3, "a.txt")
	if err != nil {
		t.Fatalf("source baseline was stranded: %v", err)
	}
	if got != src {
		t.Errorf("source baseline mutated: %+v, want %+v", got, src)
	}
	if _, err := st.GetBaseline(3, "../evil"); !isNotFound(err) {
		t.Errorf("an invalid baseline was written at the escaping destination: err=%v", err)
	}
}

// TestApplyRenameNoSourceNoDestDropsOp covers the missing-source/missing-destination
// case: a move whose source was never materialised on disk (a create that predates the
// event-log window) and whose destination is therefore also absent must NOT be falsely
// acked with a synced baseline - that would record a node we do not have and risk a
// later spurious delete. The op is dropped and no synced baseline is written.
func TestApplyRenameNoSourceNoDestDropsOp(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	// Neither source nor destination exists on disk, and no baseline is tracked.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop the unappliable rename, not wedge: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The rename recorded nothing of its own; what tracks new.txt is the compensating download
	// the drop enqueued (finding H2) - an apply that really placed the node there, so its synced
	// record is earned rather than the false claim this test pins against.
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil || b.FileID != "f9" || b.LocalHash == "" {
		t.Errorf("new.txt baseline = %+v (err %v), want the compensating download's own record", b, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "Photos", "new.txt")); err != nil {
		t.Errorf("new.txt not materialised by the compensation: %v", err)
	}
}

// TestApplyRenameOverwritesForeignDestBaseline covers a successful overwrite move whose
// destination held a different (foreign) node's baseline and whose source had no
// baseline: after the on-disk move places our node at the destination, the destination
// baseline must identify our moved node, not the overwritten one it replaced.
func TestApplyRenameOverwritesForeignDestBaseline(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("moved"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	// The destination carries a foreign node's baseline; the source has none.
	if err := st.PutBaseline(7, "b.txt", store.Baseline{FileID: "fy", FileType: "file", ServerHash: "hy", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "moved" {
		t.Errorf("b.txt = %q, want %q (the move overwrote the destination)", gotB, "moved")
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if b.FileID != "f9" {
		t.Errorf("destination baseline FileID = %q, want f9 (our moved node, not the overwritten fy)", b.FileID)
	}
	// Not synced: this apply moved content byid could not attribute to f9 (the source carried
	// no baseline), so the record names our node but carries no evidence it ever agreed with
	// the server. StateConflict is what applyRename records for that bare shape - it keeps
	// every never-lose gate armed and surfaces the path as Unresolved, instead of claiming an
	// agreement no transfer established and letting the next scan push those bytes over the
	// server's node.
	if b.State != store.StateConflict {
		t.Errorf("destination baseline state = %q, want conflict (a bare record is not agreement)", b.State)
	}
}

// TestApplyChainRenameMissingSourceDoesNotRelocateForeignNode covers this shape: a
// cold-start a->b->c move chain for a node (f9) that was never materialised on disk
// (its create predates the event window), where an unrelated node already occupies the
// intermediate path b. The first hop (a->b) finds its source absent and the destination
// holding the foreign node, so it is swallowed; the second hop (b->c) must NOT drag that
// foreign node onto c and mislabel it f9 - it has to recognise that f9 is tracked nowhere
// and drop the rename, leaving the foreign node where it sits.
func TestApplyChainRenameMissingSourceDoesNotRelocateForeignNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// b holds an unrelated node (fy) on disk and in the baseline; a and c are absent and
	// f9 is tracked nowhere.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("unrelated"), 0o644); err != nil {
		t.Fatalf("seed b: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{FileID: "fy", FileType: "file", ServerHash: "hy", State: store.StateSynced}); err != nil {
		t.Fatalf("seed b baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The foreign node stays at b; c never receives it.
	gotB, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt: %v", err)
	}
	if string(gotB) != "unrelated" {
		t.Errorf("b.txt = %q, want %q (the foreign node must not be moved)", gotB, "unrelated")
	}
	// c.txt receives f9's own server copy from the dropped hop's compensation (finding H2) -
	// never the unrelated node's bytes, which is the relocation this test refuses.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err == nil && string(got) == "unrelated" {
		t.Errorf("c.txt = %q: the unrelated node was wrongly relocated to it", got)
	}
	if b, err := st.GetBaseline(7, "c.txt"); err != nil || b.FileID != "f9" || b.LocalHash == "" {
		t.Errorf("c.txt baseline = %+v (err %v), want the compensating download's own record", b, err)
	}
	bbase, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b): %v", err)
	}
	if bbase.FileID != "fy" {
		t.Errorf("b baseline FileID = %q, want fy (the foreign node, undisturbed)", bbase.FileID)
	}
}

// TestReconcileSkipsInvalidMoveInSupersession covers this shape: a batch that creates a
// node, then carries a non-containable (escaping) move of it, then deletes it. The
// invalid move must not re-base the supersession scan - if it did, the later valid delete
// would no longer be seen as removing the created node and the doomed download would be
// enqueued, fail forever, and wedge its trailing remove. With the invalid event ignored,
// the download is elided and only the remove survives.
func TestReconcileSkipsInvalidMoveInSupersession(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(3, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	// Any download attempt errors, so a wrongly-kept doomed download would wedge the drain.
	e := New(st, &concTransport{client: &errClient{err: errors.New("node already deleted")}}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h", Size: 3},
		{SyncID: 2, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "../escape"},
		{SyncID: 3, Action: "delete", FileID: "f1", Path: "a.txt"},
	}
	ops, _, err := e.reconcile(3, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want only the remove (doomed download elided despite the invalid move)", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged because an invalid move hid the superseding delete: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestApplyMkdirAppliesMTime covers this shape: a directory create records the server
// mtime in its baseline, so it must also apply that mtime to the directory on disk - a
// baseline that claims an mtime the directory does not carry would read as a local
// modification to a later scan.
func TestApplyMkdirAppliesMTime(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	const mtime = int64(1700000000)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "sub", FileID: "d1", LocalMTime: mtime,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	fi, err := os.Stat(filepath.Join(root, "Photos", "sub"))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := fi.ModTime().Unix(); got != mtime {
		t.Errorf("directory mtime = %d, want %d (the recorded baseline mtime applied to disk)", got, mtime)
	}
	b, err := st.GetBaseline(7, "sub")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalMTime != mtime || b.State != store.StateSynced {
		t.Errorf("baseline = %+v, want LocalMTime=%d synced", b, mtime)
	}
}

// TestDeadLetteredDownloadDropsStrandedBaseline covers this shape: a download whose path
// passes reconcile's lexical containability check but hits a pre-existing symlink along
// it is dead-lettered at apply time. reconcile already wrote a pending baseline for the
// node, so the dead-letter must drop it - otherwise the byid index keeps pointing at a
// node that never reached disk, and a later move of the same file id would resolve
// through it (nodeTracked==true), skip applyRename's foreign-source guard, and drag
// unrelated content onto the destination.
func TestDeadLetteredDownloadDropsStrandedBaseline(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("body")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 4}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Reconcile a create at link/escape.txt: the path is lexically containable, so reconcile
	// enqueues a download and writes a pending baseline for f1.
	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "link/escape.txt", Hash: "h1", Size: 4},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := st.GetBaseline(7, "link/escape.txt"); err != nil {
		t.Fatalf("reconcile should have written a pending baseline: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink path, not wedge: %v", err)
	}
	// The op is dropped, nothing escaped through the symlink, and the stranded pending
	// baseline (and its byid entry) are gone.
	if _, err := os.Stat(filepath.Join(outside, "escape.txt")); !os.IsNotExist(err) {
		t.Errorf("download escaped through a symlink: err=%v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := st.GetBaseline(7, "link/escape.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("stranded baseline not dropped: err=%v", err)
	}
	if _, err := st.PathByID(7, "f1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f1) = %v, want ErrNotFound (stranded byid entry not dropped)", err)
	}
}

// TestReconcileSkipsNULPath covers this shape: a server path carrying an embedded NUL
// byte passes filepath.IsLocal on Unix yet is an invalid OS path (os.Lstat returns
// EINVAL, not IsNotExist), so an op built from it would neither apply nor dead-letter -
// it would fail every drain and wedge the queue. reconcile must reject it as
// non-containable: no op, no baseline, but the cursor still advances so it is not
// re-pulled forever.
func TestReconcileSkipsNULPath(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)
	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "f1", Path: "a\x00b.txt", Hash: "h1", Size: 4},
	}
	ops, cursor, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none (NUL path skipped)", ops)
	}
	if cursor != 5 {
		t.Errorf("cursor = %d, want 5 (the unappliable event is still acknowledged)", cursor)
	}
	if _, err := st.GetBaseline(7, "a\x00b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a baseline was written for a NUL path: err=%v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not empty: %d ops, want 0", n)
	}
}

// TestDrainQueueDeadLettersNULPath covers the NUL-path case at the executor seam: an op that
// somehow reached the durable queue with a NUL-byte path (e.g. enqueued before the
// reconcile-time guard existed) must be dead-lettered by abs() rather than failing
// os.Lstat with EINVAL every drain and wedging the queue.
func TestDrainQueueDeadLettersNULPath(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a\x00b.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a NUL path, not wedge: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain (NUL path wedged it)", n)
	}
}

// TestRejectedRemoveDropsPendingBaseline covers this shape: a remove whose path is
// dead-lettered (a pre-existing symlink along it) is acked so it cannot wedge the queue,
// but its pending baseline must be dropped too. Leaving it pending would strand it forever
// - the op is gone from the queue, so nothing remains to "retry" it.
func TestRejectedRemoveDropsPendingBaseline(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	// A delete event flipped this node's baseline to pending before the symlink made its
	// path unappliable; the remove op can never run.
	if err := st.PutBaseline(7, "link/gone.txt", store.Baseline{FileID: "f2", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "link/gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink remove, not wedge: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := st.GetBaseline(7, "link/gone.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("rejected remove left its baseline pending forever: err=%v", err)
	}
	if _, err := st.PathByID(7, "f2"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f2) = %v, want ErrNotFound (stranded byid entry not dropped)", err)
	}
}

// TestRejectedRemoveCleansRelocatedNode covers the relocated-node case: a rejected
// rename (move a.txt -> link/b.txt, refused at the symlink) re-tracks the moved file id at its
// still-reachable source a.txt. A later server delete names the node at link/b.txt (the server's
// path), so dead-lettering it by op.Path alone would miss the node entirely - the on-disk file at
// a.txt and its pending baseline would survive the server delete forever. The delete must be
// honoured at the reachable path: the file is removed and its baseline dropped, resolved by the
// stable file id without any content comparison.
func TestRejectedRemoveCleansRelocatedNode(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// First batch: the rejected move re-tracks f9 at the reachable source a.txt (pending).
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (move): %v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Fatalf("after rejected move PathByID(f9) = %q, %v; want a.txt", path, err)
	}

	// Second batch: the server deletes the node, naming it at its server path link/b.txt.
	del := []actions.Event{{SyncID: 6, Action: "delete", FileID: "f9", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, del, 5); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink remove, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The server delete must take effect at the reachable path: the file is gone.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived a server delete: err=%v", err)
	}
	// And no record of the node is left behind.
	if _, err := st.GetBaseline(7, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("relocated remove left its baseline behind: err=%v", err)
	}
	if _, err := st.PathByID(7, "f9"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f9) = %v, want ErrNotFound (stranded byid entry not dropped)", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("the remove escaped through a symlink: err=%v", err)
	}
}

// TestRejectedModifyKeepsReachableSourceTracked covers the move/modify/delete-in-three-batches
// case: after a rejected move (a.txt -> link/b.txt, refused at the symlink) re-tracks f9 at its
// still-reachable source a.txt, a later modify event names the node at its server path link/b.txt.
// Folding that modify must not relocate f9's baseline to the unappliable link/b.txt - doing so
// would let the modify's own dead-lettered download strand the node, leaving a.txt untracked so a
// final server delete (also at link/b.txt) could never resolve and remove it. The node must stay
// tracked at the reachable a.txt across the rejected modify, so the delete honours it there.
func TestRejectedModifyKeepsReachableSourceTracked(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Batch 1: the rejected move re-tracks f9 at the reachable source a.txt (pending).
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (move): %v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Fatalf("after rejected move PathByID(f9) = %q, %v; want a.txt", path, err)
	}

	// Batch 2: a modify names the node at its (unappliable) server path link/b.txt. Folding it
	// must keep f9 tracked at the reachable a.txt rather than relocating it to link/b.txt.
	mod := []actions.Event{{SyncID: 6, Action: "modify", FileID: "f9", Path: "link/b.txt", Hash: "h10"}}
	if _, _, err := e.reconcile(7, mod, 5); err != nil {
		t.Fatalf("reconcile modify: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink download, not wedge: %v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Fatalf("after rejected modify PathByID(f9) = %q, %v; want a.txt (reachable source preserved)", path, err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Fatalf("a.txt should still be on disk after the rejected modify: %v", err)
	}

	// Batch 3: the server deletes the node at its server path link/b.txt. It must resolve f9 to
	// the reachable a.txt and remove the file there.
	del := []actions.Event{{SyncID: 7, Action: "delete", FileID: "f9", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, del, 6); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink remove, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived a server delete after move+modify: err=%v", err)
	}
	if _, err := st.PathByID(7, "f9"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f9) = %v, want ErrNotFound (node not fully resolved)", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("an op escaped through the symlink: err=%v", err)
	}
}

// TestRejectedRenameKeepsSourceFileTracked covers this shape: a move whose destination path
// is dead-lettered (a pre-existing symlink along it) is rejected before the on-disk move
// runs, so the source file is still in place. reconcile relocated the moved node's baseline
// to the (unappliable) destination ahead of the drain; dropping that record would orphan
// the still-present source file. Instead the baseline must move back to the source, leaving
// the real on-disk node tracked where it actually is.
func TestRejectedRenameKeepsSourceFileTracked(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Reconcile relocates f9's baseline to link/b.txt (lexically containable), then the
	// executor rejects the move at the symlink.
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink move, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The source file must be untouched and still on disk.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Errorf("source file lost: %v", err)
	} else if string(got) != "body" {
		t.Errorf("a.txt = %q, want %q", got, "body")
	}
	// Nothing escaped through the symlink.
	if _, err := os.Stat(filepath.Join(outside, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("the move escaped through a symlink: err=%v", err)
	}
	// The node must stay tracked at the source, not be orphaned. It is re-tracked as
	// pending (not synced): a same-batch modify could have advanced the relocated facts
	// past what the untouched source holds, so the executor cannot claim synced without a
	// Phase-6c scan.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("source baseline was orphaned: %v", err)
	}
	if b.FileID != "f9" || b.State != store.StatePending {
		t.Errorf("restored source baseline = %+v, want pending f9", b)
	}
	if _, err := st.GetBaseline(7, "link/b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a baseline survived at the rejected destination: err=%v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil {
		t.Errorf("f9 untracked after a rejected rename: %v", err)
	} else if path != "a.txt" {
		t.Errorf("PathByID(f9) = %q, want a.txt", path)
	}
}

// TestSecondRejectedRenameKeepsReachableSource covers the double-rejected-rename case: a
// first move (a.txt -> link/b.txt, refused at the symlink) re-tracks f9 at its reachable
// source a.txt, where the real file still sits. The server (which believes the move
// succeeded) then moves link/b.txt -> c.txt; this rename's SOURCE is the symlinked, unappliable
// link/b.txt, so it is dead-lettered. The cleanup must not delete f9's baseline at a.txt -
// PathByID resolves f9 to a.txt while op.Path is the stale server source link/b.txt, so checking
// only op.Path would orphan the still-present file. The node must stay tracked at the reachable
// a.txt.
func TestSecondRejectedRenameKeepsReachableSource(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Batch 1: the rejected move re-tracks f9 at the reachable source a.txt (pending).
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (move): %v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Fatalf("after rejected move PathByID(f9) = %q, %v; want a.txt", path, err)
	}

	// Batch 2: the server moves the node on from its server path link/b.txt to c.txt. The
	// source link/b.txt is the symlinked, unappliable path, so the rename is dead-lettered.
	move2 := []actions.Event{{SyncID: 6, Action: "move", FileID: "f9", FromPath: "link/b.txt", Path: "c.txt"}}
	if _, _, err := e.reconcile(7, move2, 5); err != nil {
		t.Fatalf("reconcile move2: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink-source move, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The real file must survive at the reachable source and stay tracked there.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Errorf("source file orphaned/lost after a second rejected rename: %v", err)
	} else if string(got) != "body" {
		t.Errorf("a.txt = %q, want %q", got, "body")
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("source baseline was dropped, orphaning the on-disk file: %v", err)
	}
	if b.FileID != "f9" || b.State != store.StatePending {
		t.Errorf("source baseline = %+v, want pending f9", b)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Errorf("PathByID(f9) = %q, %v; want a.txt (node must stay tracked)", path, err)
	}
	if _, err := os.Stat(filepath.Join(outside, "c.txt")); !os.IsNotExist(err) {
		t.Errorf("the move escaped through a symlink: err=%v", err)
	}
}

// TestRejectedRenameThenModifyDoesNotMarkStaleSourceSynced covers this shape: a node is
// moved INTO a symlinked (rejected) path and then modified there in the same batch. Both
// ops are dead-lettered at the symlink, but the modify advances the relocated baseline's
// facts (hash) past what the untouched source still holds. The rejected-rename cleanup must
// not stamp those newer facts onto the stale source bytes as synced - it re-tracks the
// source as pending so a Phase-6c scan verifies the content instead of trusting it.
func TestRejectedRenameThenModifyDoesNotMarkStaleSourceSynced(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h_old", LocalHash: "h_old", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	// The modify carries a fresh hash for the same node at the (symlinked) destination.
	dl := &dlClient{content: map[string][]byte{}, nodes: map[string]actions.Node{}}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"},
		{SyncID: 6, Action: "modify", FileID: "f9", Path: "link/b.txt", Hash: "h_new", Size: 3},
	}
	if _, _, err := e.reconcile(7, events, 4); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink ops, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The source file is untouched (still the pre-move bytes), and nothing escaped.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Errorf("source file lost: %v", err)
	} else if string(got) != "old" {
		t.Errorf("a.txt = %q, want %q", got, "old")
	}
	if _, err := os.Stat(filepath.Join(outside, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("the move escaped through a symlink: err=%v", err)
	}
	// The source stays tracked but must NOT be marked synced with the modify's hash:
	// stamping h_new onto the stale "old" bytes would read as truth.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("source baseline orphaned: %v", err)
	}
	if b.FileID != "f9" {
		t.Errorf("source baseline FileID = %q, want f9", b.FileID)
	}
	if b.State == store.StateSynced {
		t.Errorf("source baseline marked synced; the modify's facts would falsely describe the stale source bytes")
	}
}

// TestRejectedRenameThenSuccessfulModifyRemovesStaleSource covers the successful-
// recovery case. A move a.txt->link/b.txt is rejected at the symlink, so resolveRejectedRename
// re-tracks the node at its still-reachable source a.txt (file still on disk there). Later the
// symlink is replaced by a real directory and a modify for the same node downloads successfully
// to link/b.txt. markSynced re-points the baseline to link/b.txt; the original file left at
// a.txt must be removed too, or it survives untracked and a subsequent server delete (targeting
// only link/b.txt) can never reach it.
func TestRejectedRenameThenSuccessfulModifyRemovesStaleSource(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	linkPath := filepath.Join(viewDir, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h_old", LocalHash: "h_old", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("modified")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h_new", Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Batch 1: the move is rejected at the symlink; the node is re-tracked at a.txt.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"},
	}, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (rejected move): %v", err)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "a.txt" {
		t.Fatalf("after rejected move PathByID(f9) = %q, %v; want a.txt", p, err)
	}

	// The symlink is replaced by a real directory, so link/b.txt becomes appliable.
	if err := os.Remove(linkPath); err != nil {
		t.Fatalf("remove symlink: %v", err)
	}
	if err := os.MkdirAll(linkPath, 0o755); err != nil {
		t.Fatalf("mkdir real link dir: %v", err)
	}

	// Batch 2: the modify downloads successfully to link/b.txt.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "modify", FileID: "f9", Path: "link/b.txt", Hash: "h_new", Size: 8},
	}, 5); err != nil {
		t.Fatalf("reconcile modify: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (modify): %v", err)
	}

	// The download landed at link/b.txt and the baseline is synced there.
	if got, err := os.ReadFile(filepath.Join(viewDir, "link", "b.txt")); err != nil {
		t.Fatalf("read link/b.txt: %v", err)
	} else if string(got) != "modified" {
		t.Errorf("link/b.txt = %q, want %q", got, "modified")
	}
	if b, err := st.GetBaseline(7, "link/b.txt"); err != nil {
		t.Fatalf("destination baseline missing: %v", err)
	} else if b.FileID != "f9" || b.State != store.StateSynced {
		t.Errorf("link/b.txt baseline = %+v, want synced f9", b)
	}
	// The stale original at a.txt must be gone - both on disk and as a baseline.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("stale source a.txt survived the successful relocation: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Errorf("stale source baseline at a.txt survived the relocation")
	}
}

// TestRejectedCreateThenMoveModifyKeepsServerSyncID covers this shape: in one batch a node
// is created at a dead-lettered path, moved to a reachable one, and then modified there. The
// rejected create's cleanup drops the pending baseline reconcile pre-wrote - which by then
// sits at the modify's path (byid follows the relocation) - but the modify's own download
// then rebuilds it. That rebuild must retain the event's ServerSyncID rather than resetting
// it to zero.
func TestRejectedCreateThenMoveModifyKeepsServerSyncID(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("modified")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h3", Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	events := []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "link/a.txt", Hash: "h1", Size: 4},
		{SyncID: 2, Action: "move", FileID: "f1", FromPath: "link/a.txt", Path: "b.txt"},
		{SyncID: 3, Action: "modify", FileID: "f1", Path: "b.txt", Hash: "h3", Size: 8},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil {
		t.Fatalf("read b.txt: %v", err)
	} else if string(got) != "modified" {
		t.Errorf("b.txt = %q, want %q", got, "modified")
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "f1" || b.State != store.StateSynced {
		t.Errorf("b.txt baseline = %+v, want synced f1", b)
	}
	if b.ServerSyncID != 3 {
		t.Errorf("b.txt ServerSyncID = %d, want 3 (rebuild lost the event sync id)", b.ServerSyncID)
	}
}

// TestDropStrandedBaselinePreservesSyncedNode exercises dropStrandedBaseline's protective
// guard: a doom-cleanup must never drop a baseline that is already synced - that is proof a
// real apply placed the node, so it is authoritative. Dropping it would silently untrack a
// real on-disk node and risk a spurious server-side delete in 6c.
func TestDropStrandedBaselinePreservesSyncedNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)

	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}
	if err := e.dropStrandedBaseline(7, "f1"); err != nil {
		t.Fatalf("dropStrandedBaseline: %v", err)
	}
	if b, err := st.GetBaseline(7, "kept.txt"); err != nil {
		t.Errorf("synced baseline was dropped by doom-cleanup: %v", err)
	} else if b.FileID != "f1" || b.State != store.StateSynced {
		t.Errorf("synced baseline mutated: %+v", b)
	}
}

// TestDropRejectedRemoveBaselinePreservesRealNodes exercises the two protective guards in
// dropRejectedRemoveBaseline: dropping a rejected remove's pending baseline must skip a
// baseline that has since become synced, or one that now carries a different file id (a
// same-batch delete-then-create that took the path). Dropping either would lose a real node.
func TestDropRejectedRemoveBaselinePreservesRealNodes(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)

	// Guard 1: the baseline at the path is now synced (a later create took it).
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{
		FileID: "new", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}
	if err := e.dropRejectedRemoveBaseline(context.Background(), store.Op{ViewID: 7, Path: "gone.txt", FileID: "old"}); err != nil {
		t.Fatalf("dropRejectedRemoveBaseline (synced): %v", err)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("synced baseline dropped by a rejected remove cleanup: %v", err)
	}

	// Guard 2: a different file id occupies the path (delete-then-create, still pending).
	if err := st.PutBaseline(7, "swap.txt", store.Baseline{
		FileID: "created", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
	}
	if err := e.dropRejectedRemoveBaseline(context.Background(), store.Op{ViewID: 7, Path: "swap.txt", FileID: "deleted"}); err != nil {
		t.Fatalf("dropRejectedRemoveBaseline (foreign id): %v", err)
	}
	if b, err := st.GetBaseline(7, "swap.txt"); err != nil {
		t.Errorf("pending baseline of a recreated node dropped: %v", err)
	} else if b.FileID != "created" {
		t.Errorf("baseline FileID = %q, want created", b.FileID)
	}
}

// TestResolveRejectedRenameDropsPhantomWhenSourceGone covers resolveRejectedRename's drop
// branch: when a rename is dead-lettered at its destination and the source file is no longer
// on disk, the relocated baseline reconcile left at the destination is a genuine phantom (no
// real node anywhere) and must be dropped along with its byid index entry - otherwise a later
// move of the same file id would resolve through the stale index and drag unrelated bytes.
func TestResolveRejectedRenameDropsPhantomWhenSourceGone(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// reconcile relocated f9's baseline to the (rejected) destination; it is pending and indexed
	// in byid. The source "a.txt" was never materialised on disk.
	if err := st.PutBaseline(7, "dest.txt", store.Baseline{
		FileID: "f9", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	op := store.Op{Type: store.OpRename, ViewID: 7, Path: "a.txt", NewPath: "dest.txt", FileID: "f9"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}
	if _, err := st.GetBaseline(7, "dest.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("phantom baseline survived at the rejected destination: err=%v", err)
	}
	if _, err := st.PathByID(7, "f9"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f9) = %v, want ErrNotFound (stranded byid entry not dropped)", err)
	}
}

// TestResolveRejectedRenameSkipsForeignSourceNode covers this shape: a rejected rename's source
// path (op.Path) is now occupied on disk by a *different* tracked node, while the moved file id
// is indexed at another, still-present path. resolveRejectedRename must NOT re-track the moved
// node on top of op.Path: doing so would overwrite the foreign node's baseline and, through the
// byid index, drop the moved node's real indexed record - binding the moved file id to unrelated
// bytes. The moved node must be re-tracked at its own indexed path and the foreign node left
// untouched.
func TestResolveRejectedRenameSkipsForeignSourceNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// op.Path (the stale rename source) holds a different, tracked node on disk.
	if err := os.WriteFile(filepath.Join(viewDir, "src.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "src.txt", store.Baseline{
		FileID: "fForeign", FileType: "file", ServerHash: "hF", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}
	// The moved node f9 is indexed (byid) at its still-present reachable path kept.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "kept.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// A rejected rename of f9 whose stale source is src.txt (the foreign node's path).
	op := store.Op{Type: store.OpRename, ViewID: 7, Path: "src.txt", NewPath: "dest.txt", FileID: "f9"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}

	// The foreign node at src.txt must be untouched.
	if b, err := st.GetBaseline(7, "src.txt"); err != nil {
		t.Errorf("foreign baseline at src.txt was clobbered: %v", err)
	} else if b.FileID != "fForeign" {
		t.Errorf("src.txt baseline FileID = %q, want fForeign", b.FileID)
	}
	if path, err := st.PathByID(7, "fForeign"); err != nil || path != "src.txt" {
		t.Errorf("PathByID(fForeign) = %q, %v; want src.txt (foreign index dropped)", path, err)
	}
	// The moved node must stay tracked at its own indexed path, re-tracked pending.
	if path, err := st.PathByID(7, "f9"); err != nil || path != "kept.txt" {
		t.Errorf("PathByID(f9) = %q, %v; want kept.txt", path, err)
	}
	if b, err := st.GetBaseline(7, "kept.txt"); err != nil {
		t.Errorf("moved node baseline lost at kept.txt: %v", err)
	} else if b.FileID != "f9" || b.State != store.StatePending {
		t.Errorf("kept.txt baseline = %+v, want pending f9", b)
	}
	if string(mustRead(t, filepath.Join(viewDir, "src.txt"))) != "foreign" {
		t.Errorf("src.txt content changed")
	}
}

// TestResolveRejectedRenameMultiHopRestoresDescendants covers the multi-hop variant of the
// rejected directory-move recovery. A batch holds two moves of the same directory - a->link/b
// then link/b->c - both folded by reconcile before the drain, so the root and its descendants
// are indexed at the FINAL hop c, not at this op's immediate destination link/b. When the first
// move (a->link/b) is dead-lettered at the symlink, resolveRejectedRename must reverse the
// descendant baselines from their CURRENT prefix (c) back to the source a, not from op.NewPath
// (link/b) - which holds nothing, so reversing from it would strand the descendants at c while
// their files sit at a. A later child delete would then ack a no-op at the empty destination and
// leave the real source file behind.
func TestResolveRejectedRenameMultiHopRestoresDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The on-disk move never ran (the first hop was rejected), so the subtree's files are still
	// at the source a.
	if err := os.MkdirAll(filepath.Join(viewDir, "a", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	// reconcile folded a->link/b then link/b->c, leaving root + descendants indexed (pending) at
	// the final hop c.
	for path, b := range map[string]store.Baseline{
		"c":           {FileID: "fd", FileType: "dir", State: store.StatePending},
		"c/x.txt":     {FileID: "fx", FileType: "file", State: store.StatePending},
		"c/sub":       {FileID: "fs", FileType: "dir", State: store.StatePending},
		"c/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StatePending},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// The dead-lettered FIRST move: a -> link/b (its destination is behind the symlink).
	op := store.Op{Type: store.OpRename, ViewID: 7, Path: "a", NewPath: "link/b", FileID: "fd"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}

	// Nothing must be left under the final-hop prefix c.
	for _, dst := range []string{"c", "c/x.txt", "c/sub", "c/sub/y.txt"} {
		if _, err := st.GetBaseline(7, dst); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("baseline stranded at multi-hop destination %q: err=%v", dst, err)
		}
	}
	// Root and every descendant must be back at the source a, resolvable by file id.
	for path, id := range map[string]string{
		"a": "fd", "a/x.txt": "fx", "a/sub": "fs", "a/sub/y.txt": "fy",
	} {
		b, err := st.GetBaseline(7, path)
		if err != nil {
			t.Errorf("baseline lost at %q: %v", path, err)
			continue
		}
		if b.FileID != id {
			t.Errorf("baseline %q FileID = %q, want %q", path, b.FileID, id)
		}
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}
}

// TestResolveRejectedRenameSkipsEmptyIDSourceNode covers this shape: the foreign node occupying
// op.Path (the stale rename source) carries an *empty* file id - a real local-only/unindexed
// baseline (e.g. a create event the server gave no id), not a free path. resolveRejectedRename
// must still refuse to re-track the moved node on top of it; it must fall through to the moved
// node's own indexed path. Treating an empty id as "free" would overwrite the local-only node.
func TestResolveRejectedRenameSkipsEmptyIDSourceNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// op.Path holds a different, tracked node whose baseline carries no server file id.
	if err := os.WriteFile(filepath.Join(viewDir, "src.txt"), []byte("local"), 0o644); err != nil {
		t.Fatalf("seed local-only file: %v", err)
	}
	if err := st.PutBaseline(7, "src.txt", store.Baseline{
		FileType: "file", LocalHash: "hL", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed local-only baseline: %v", err)
	}
	// The moved node f9 is indexed at its still-present reachable path kept.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "kept.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	op := store.Op{Type: store.OpRename, ViewID: 7, Path: "src.txt", NewPath: "dest.txt", FileID: "f9"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}

	// The local-only node at src.txt must be untouched.
	if b, err := st.GetBaseline(7, "src.txt"); err != nil {
		t.Errorf("local-only baseline at src.txt was clobbered: %v", err)
	} else if b.FileID != "" || b.LocalHash != "hL" {
		t.Errorf("src.txt baseline = %+v, want the untouched local-only record", b)
	}
	if string(mustRead(t, filepath.Join(viewDir, "src.txt"))) != "local" {
		t.Errorf("src.txt content changed")
	}
	// The moved node stays tracked at its own indexed path.
	if path, err := st.PathByID(7, "f9"); err != nil || path != "kept.txt" {
		t.Errorf("PathByID(f9) = %q, %v; want kept.txt", path, err)
	}
	if b, err := st.GetBaseline(7, "kept.txt"); err != nil {
		t.Errorf("moved node baseline lost at kept.txt: %v", err)
	} else if b.FileID != "f9" || b.State != store.StatePending {
		t.Errorf("kept.txt baseline = %+v, want pending f9", b)
	}
}

// TestRemoveHonoursRelocatedNodeWhenPathBecomesValid covers this shape: a rejected rename
// (move a.txt -> link/b.txt, refused at a symlink) re-tracks f9 at its reachable source a.txt.
// The symlink is then removed, so the server delete's path link/b.txt becomes a valid (empty)
// location. applyRemove takes its success path there - a no-op, since nothing is at link/b.txt -
// and must still honour the delete at the node's relocated, byid-indexed path a.txt: remove the
// file and drop its baseline. Resolving the delete only by op.Path would ack it while the node
// survives locally after a server delete.
func TestRemoveHonoursRelocatedNodeWhenPathBecomesValid(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	linkPath := filepath.Join(viewDir, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: "h9", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Batch 1: the rejected move re-tracks f9 at the reachable source a.txt (pending).
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "f9", FromPath: "a.txt", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (move): %v", err)
	}
	if path, err := st.PathByID(7, "f9"); err != nil || path != "a.txt" {
		t.Fatalf("after rejected move PathByID(f9) = %q, %v; want a.txt", path, err)
	}

	// The symlink is removed, so link/b.txt is now a valid (empty) on-disk path: the delete's
	// own path applies cleanly as a no-op, no longer rejected.
	if err := os.Remove(linkPath); err != nil {
		t.Fatalf("remove symlink: %v", err)
	}

	// Batch 2: the server deletes the node at its server path link/b.txt.
	del := []actions.Event{{SyncID: 6, Action: "delete", FileID: "f9", Path: "link/b.txt"}}
	if _, _, err := e.reconcile(7, del, 5); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (delete): %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// The server delete must take effect at the reachable relocated path.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived a server delete (remove ignored the relocated node): err=%v", err)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("relocated remove left its baseline behind: err=%v", err)
	}
	if _, err := st.PathByID(7, "f9"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("PathByID(f9) = %v, want ErrNotFound (stranded byid entry not dropped)", err)
	}
}

// TestRemoveHonorsSyncedRelocatedNode confirms a server delete removes a node that a
// rejected move left synced at a path the delete does not name. The node was moved
// a->b on the server but the move was refused locally (it stays synced at a); the
// delete arrives keyed to the server's path b. removeRelocatedNode resolves the node by
// file id to a and must honour the delete there even though the baseline is synced -
// otherwise the file and baseline survive the server delete forever.
func TestRemoveHonorsSyncedRelocatedNode(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "b.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("server-deleted node survived at its relocated path: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !isNotFound(err) {
		t.Errorf("baseline survived the delete: err=%v", err)
	}
	if _, err := st.PathByID(7, "f9"); !isNotFound(err) {
		t.Errorf("byid entry survived the delete: err=%v", err)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("queue not drained: %d ops remain", len(pending))
	}
}

// TestDrainQueueRenameRelocatesDirDescendants confirms a directory move relocates its
// descendant baselines (and byid entries) to the new prefix, matching the on-disk
// subtree move os.Rename performs in one call. Without this the descendants' persisted
// state is left under the old prefix, diverging from disk.
func TestDrainQueueRenameRelocatesDirDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "d", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed file x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed file y: %v", err)
	}
	// Baselines for the directory and its subtree.
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/x.txt", store.Baseline{FileID: "fx", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed x baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/sub", store.Baseline{FileID: "fs", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed sub baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/sub/y.txt", store.Baseline{FileID: "fy", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed y baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "d", NewPath: "e", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Disk: the whole subtree moved under the new prefix.
	if _, err := os.Stat(filepath.Join(viewDir, "e", "x.txt")); err != nil {
		t.Errorf("descendant x not under new prefix: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "e", "sub", "y.txt")); err != nil {
		t.Errorf("descendant y not under new prefix: %v", err)
	}
	// Baselines: descendants relocated, none left under the old prefix.
	for _, old := range []string{"d", "d/x.txt", "d/sub", "d/sub/y.txt"} {
		if _, err := st.GetBaseline(7, old); !isNotFound(err) {
			t.Errorf("baseline survived under old prefix %q: err=%v", old, err)
		}
	}
	for path, id := range map[string]string{"e/x.txt": "fx", "e/sub": "fs", "e/sub/y.txt": "fy"} {
		b, err := st.GetBaseline(7, path)
		if err != nil {
			t.Errorf("GetBaseline(%q): %v", path, err)
			continue
		}
		if b.FileID != id {
			t.Errorf("baseline %q FileID = %q, want %q", path, b.FileID, id)
		}
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}
}

// TestMultiHopDirMoveRelocatesDescendants drives a same-batch a->b->c directory move
// chain. reconcile relocates the directory root baseline straight to the final
// destination c, so the first hop (a->b) describes the node at neither of its own
// endpoints - yet it must still recognise the node as a directory (via the baseline at
// the tracked path) and relocate the descendants under each hop's prefix. Without that
// the descendants would be left indexed under the original prefix a.
func TestMultiHopDirMoveRelocatesDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "a", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	for path, b := range map[string]store.Baseline{
		"a":           {FileID: "fa", FileType: "dir", State: store.StateSynced},
		"a/x.txt":     {FileID: "fx", FileType: "file", State: store.StateSynced},
		"a/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"a/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// A same-batch two-hop move of the directory root; reconcile relocates the root
	// baseline a->b->c and enqueues both rename ops.
	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "b"},
		{SyncID: 2, Action: "move", FileID: "fa", IsDir: true, FromPath: "b", Path: "c"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Disk: the whole subtree lives under the final prefix c.
	if _, err := os.Stat(filepath.Join(viewDir, "c", "x.txt")); err != nil {
		t.Errorf("descendant x not under final prefix: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "c", "sub", "y.txt")); err != nil {
		t.Errorf("descendant y not under final prefix: %v", err)
	}
	// Baselines: nothing left under the old prefixes a/ or b/.
	for _, old := range []string{"a", "a/x.txt", "a/sub", "a/sub/y.txt", "b", "b/x.txt", "b/sub", "b/sub/y.txt"} {
		if _, err := st.GetBaseline(7, old); !isNotFound(err) {
			t.Errorf("baseline survived under stale prefix %q: err=%v", old, err)
		}
	}
	for path, id := range map[string]string{"c": "fa", "c/x.txt": "fx", "c/sub": "fs", "c/sub/y.txt": "fy"} {
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}
}

// TestDirMoveReplayRelocatesDescendants confirms descendant relocation is crash-replay
// safe. It reconstructs the state left by a crash after os.Rename moved the directory on
// disk but before the descendant baselines were relocated: the source is gone on disk and
// the descendants are still indexed under the old prefix. On replay the rename is a
// swallowed no-op (source absent, destination present), yet the descendants must still be
// re-homed - relocating only when this apply performs the rename would leave them stranded
// forever.
func TestDirMoveReplayRelocatesDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// Disk already reflects the completed os.Rename(a, b): the subtree lives under b.
	if err := os.MkdirAll(filepath.Join(viewDir, "b", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	// Baselines as the crash left them: the root relocated to b but still pending (its
	// synced write never ran), descendants still under the old prefix a/.
	for path, b := range map[string]store.Baseline{
		"b":           {FileID: "fa", FileType: "dir", State: store.StatePending},
		"a/x.txt":     {FileID: "fx", FileType: "file", State: store.StateSynced},
		"a/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"a/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a", NewPath: "b", FileID: "fa",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Baselines repaired: descendants under b/, none left under a/.
	for _, old := range []string{"a/x.txt", "a/sub", "a/sub/y.txt"} {
		if _, err := st.GetBaseline(7, old); !isNotFound(err) {
			t.Errorf("baseline survived under old prefix %q: err=%v", old, err)
		}
	}
	for path, id := range map[string]string{"b/x.txt": "fx", "b/sub": "fs", "b/sub/y.txt": "fy"} {
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}
	if b, err := st.GetBaseline(7, "b"); err != nil || b.State != store.StateSynced {
		t.Errorf("root baseline = %+v, %v, want synced", b, err)
	}
}

// TestDirMoveThenSourceRecreateKeepsMovedChild drives the data-loss scenario a directory
// move followed by a re-create of the source path: directory a (holding child a/x.txt) is
// moved a->b, then a fresh directory a and a fresh file a/x.txt are created. The moved
// child must survive at b/x.txt with its original bytes while the new file lands at a/x.txt
// with its own bytes. Before the fix, reconcile overwrote the moved child's a/x.txt baseline
// with the new node, then the executor's prefix scan dragged the new node onto b/x.txt and
// the new node's download deleted the moved child's file - losing it.
func TestDirMoveThenSourceRecreateKeepsMovedChild(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// On disk: directory a with the moved child already materialised.
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x.txt"), []byte("old-child"), 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	// Baselines for the directory and its synced child.
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/x.txt", store.Baseline{FileID: "fx", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"fx2": []byte("new-file")},
		nodes:   map[string]actions.Node{"fx2": {Hash: "hn", MTime: 1700000000, Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// move a->b, re-create directory a, then create a fresh file a/x.txt (a new id).
	events := []actions.Event{
		{SyncID: 1, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "b"},
		{SyncID: 2, Action: "create", FileID: "fa2", IsDir: true, Path: "a"},
		{SyncID: 3, Action: "create", FileID: "fx2", Path: "a/x.txt", Hash: "hn", Size: 8, MTime: 1700000000},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The moved child kept its bytes at the new prefix.
	if got := mustRead(t, filepath.Join(viewDir, "b", "x.txt")); string(got) != "old-child" {
		t.Errorf("moved child b/x.txt = %q, want %q (lost or overwritten)", got, "old-child")
	}
	// The freshly created file landed at the recreated source path.
	if got := mustRead(t, filepath.Join(viewDir, "a", "x.txt")); string(got) != "new-file" {
		t.Errorf("new file a/x.txt = %q, want %q", got, "new-file")
	}
	// Baselines track both nodes at their correct, distinct paths.
	if p, err := st.PathByID(7, "fx"); err != nil || p != "b/x.txt" {
		t.Errorf("moved child PathByID(fx) = %q, %v, want b/x.txt", p, err)
	}
	if p, err := st.PathByID(7, "fx2"); err != nil || p != "a/x.txt" {
		t.Errorf("new file PathByID(fx2) = %q, %v, want a/x.txt", p, err)
	}
	if b, err := st.GetBaseline(7, "b/x.txt"); err != nil || b.FileID != "fx" {
		t.Errorf("b/x.txt baseline = %+v, %v, want id fx", b, err)
	}
	if b, err := st.GetBaseline(7, "a/x.txt"); err != nil || b.FileID != "fx2" {
		t.Errorf("a/x.txt baseline = %+v, %v, want id fx2", b, err)
	}
}

// TestRelocateDescendantBaselinesNoOpMove confirms a directory move whose source and
// destination are the same (or canonically equivalent) leaves the subtree's baselines
// intact. Before the guard each descendant was written back to its own path and then
// deleted, wiping the whole subtree's tracking.
func TestRelocateDescendantBaselinesNoOpMove(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for path, b := range map[string]store.Baseline{
		"d":         {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"d/x.txt":   {FileID: "fx", FileType: "file", State: store.StateSynced},
		"d/sub":     {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"d/sub/y.t": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)

	for _, endpoints := range [][2]string{{"d", "d"}, {"d", "d/."}, {"d/.", "d"}} {
		if err := e.relocateDescendantBaselines(7, endpoints[0], endpoints[1]); err != nil {
			t.Fatalf("relocateDescendantBaselines(%q,%q): %v", endpoints[0], endpoints[1], err)
		}
		for path, id := range map[string]string{"d/x.txt": "fx", "d/sub": "fs", "d/sub/y.t": "fy"} {
			b, err := st.GetBaseline(7, path)
			if err != nil {
				t.Fatalf("descendant %q wiped by no-op move %v: %v", path, endpoints, err)
			}
			if b.FileID != id {
				t.Errorf("descendant %q FileID = %q, want %q", path, b.FileID, id)
			}
		}
	}
}

// TestRelocateDescendantBaselinesEmptyIDNoClobber confirms the descendant-collision guard
// treats two empty file ids as distinct nodes: a local-only descendant must not overwrite a
// different local-only node already recorded at the destination. The store leaves empty-id
// baselines unindexed, so equal empty ids cannot establish identity.
func TestRelocateDescendantBaselinesEmptyIDNoClobber(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// A moved directory with a local-only (empty-id) descendant, and a DIFFERENT local-only
	// node already sitting at the destination path.
	for path, b := range map[string]store.Baseline{
		"d":       {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"d/x.txt": {FileType: "file", LocalHash: "moved", LocalSize: 1, State: store.StateSynced},
		"e/x.txt": {FileType: "file", LocalHash: "occupant", LocalSize: 2, State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, t.TempDir(), "sess", nil)
	if err := e.relocateDescendantBaselines(7, "d", "e"); err != nil {
		t.Fatalf("relocateDescendantBaselines: %v", err)
	}
	// The destination occupant must be untouched...
	occ, err := st.GetBaseline(7, "e/x.txt")
	if err != nil || occ.LocalHash != "occupant" {
		t.Errorf("destination occupant clobbered: e/x.txt = %+v, err=%v", occ, err)
	}
	// ...and the local-only descendant left where it is rather than deleted.
	mv, err := st.GetBaseline(7, "d/x.txt")
	if err != nil || mv.LocalHash != "moved" {
		t.Errorf("local-only descendant lost: d/x.txt = %+v, err=%v", mv, err)
	}
}

// TestRejectedDirMoveRestoresDescendantBaselines covers this shape: a directory move whose
// destination is rejected at a pre-existing symlink. reconcile's relocatePending rebased the
// whole subtree's baselines to the (rejected) destination prefix before the drain ran, but the
// on-disk move never happened - the files are still under the source. The rejection cleanup must
// carry the descendant baselines back to the source too, not just the directory root: otherwise a
// descendant's baseline points at the empty destination while its file sits at the source, and a
// later child delete (which the server names under the destination) resolves through the stranded
// index, targets the empty destination, and leaves the real source file behind.
func TestRejectedDirMoveRestoresDescendantBaselines(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "d", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "link")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	for path, b := range map[string]store.Baseline{
		"d":           {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"d/x.txt":     {FileID: "fx", FileType: "file", State: store.StateSynced},
		"d/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"d/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Reconcile relocates the subtree's baselines to link/e/* (lexically containable), then the
	// executor rejects the move at the symlink and the cleanup must reverse the relocation.
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "fd", IsDir: true, FromPath: "d", Path: "link/e"}}
	if _, _, err := e.reconcile(7, move, 4); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter the symlink move, not wedge: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// Nothing must be tracked under the rejected destination prefix.
	for _, dst := range []string{"link/e", "link/e/x.txt", "link/e/sub", "link/e/sub/y.txt"} {
		if _, err := st.GetBaseline(7, dst); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("baseline stranded at rejected destination %q: err=%v", dst, err)
		}
	}
	// Every descendant baseline must be back at the source, resolvable by file id to the path
	// where its file actually sits on disk.
	for path, id := range map[string]string{"d/x.txt": "fx", "d/sub": "fs", "d/sub/y.txt": "fy"} {
		b, err := st.GetBaseline(7, path)
		if err != nil {
			t.Errorf("descendant baseline lost at %q: %v", path, err)
			continue
		}
		if b.FileID != id {
			t.Errorf("baseline %q FileID = %q, want %q", path, b.FileID, id)
		}
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v, want %q", id, p, err, path)
		}
	}

	// Concretely: a later server delete of the child (which the server names under the moved
	// prefix link/e) must now reach the real file at the source and remove it, rather than acking
	// a no-op at the empty destination and leaving the file behind.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "link/e/x.txt", FileID: "fx",
	}); err != nil {
		t.Fatalf("Enqueue delete: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (delete): %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "d", "x.txt")); !os.IsNotExist(err) {
		t.Errorf("server delete left the real source file behind: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "d/x.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("deleted child baseline survived: err=%v", err)
	}
}

// TestResolveRejectedRenameDropsPhantomDirDescendants covers the multi-hop variant where the
// source candidate is no longer free: a directory move a->link/b->c is folded by reconcile to
// the final hop c (root + descendants), then a is recreated as a different node, then the first
// move (a->link/b) is dead-lettered at the symlink. resolveRejectedRename cannot re-track the
// moved directory at a (occupied by the recreated node) and the destination prefix c does not
// exist on disk, so the moved node is a genuine phantom. Dropping only the root would leave the
// whole c/* subtree stranded - byid entries under a directory that does not exist. The phantom
// drop must remove the descendant baselines too, while leaving the recreated node at a untouched.
func TestResolveRejectedRenameDropsPhantomDirDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// a exists on disk again - recreated as a different node. The final hop c never existed.
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed recreated dir: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{
		FileID: "fNew", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed recreated baseline: %v", err)
	}
	// reconcile folded a->link/b then link/b->c, leaving the moved root + descendants indexed
	// (pending) at the final hop c.
	for path, b := range map[string]store.Baseline{
		"c":           {FileID: "fd", FileType: "dir", State: store.StatePending},
		"c/x.txt":     {FileID: "fx", FileType: "file", State: store.StatePending},
		"c/sub":       {FileID: "fs", FileType: "dir", State: store.StatePending},
		"c/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StatePending},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	op := store.Op{Type: store.OpRename, ViewID: 7, Path: "a", NewPath: "link/b", FileID: "fd"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}

	// The whole moved subtree must be gone - root and every descendant baseline + byid entry.
	for _, dst := range []string{"c", "c/x.txt", "c/sub", "c/sub/y.txt"} {
		if _, err := st.GetBaseline(7, dst); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("phantom baseline survived at %q: err=%v", dst, err)
		}
	}
	for _, id := range []string{"fd", "fx", "fs", "fy"} {
		if _, err := st.PathByID(7, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("stranded byid entry for %q not dropped: err=%v", id, err)
		}
	}
	// The recreated node at a must be untouched.
	if b, err := st.GetBaseline(7, "a"); err != nil {
		t.Errorf("recreated baseline at a lost: %v", err)
	} else if b.FileID != "fNew" {
		t.Errorf("a baseline FileID = %q, want fNew", b.FileID)
	}
	if p, err := st.PathByID(7, "fNew"); err != nil || p != "a" {
		t.Errorf("PathByID(fNew) = %q, %v; want a", p, err)
	}
}

// TestRejectedDirMoveThenSuccessfulCreateRelocatesSubtree covers the directory variant of the
// rejected-move recovery: a directory move a->link/b is dead-lettered at a symlink and re-tracked
// at its still-reachable source a (with its subtree); once the symlink is replaced by a real
// directory, a later create/modify of the directory at link/b must relocate the WHOLE subtree -
// on disk and in the baselines - rather than create an empty link/b and reindex only the root,
// which would strand the subtree (and its descendant baselines) at a where a later parent delete
// could never reach them.
func TestRejectedDirMoveThenSuccessfulCreateRelocatesSubtree(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "a", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	linkPath := filepath.Join(viewDir, "link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}
	// The directory subtree is tracked (synced) at the source a.
	for path, b := range map[string]store.Baseline{
		"a":           {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"a/x.txt":     {FileID: "fx", FileType: "file", State: store.StateSynced},
		"a/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"a/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// Batch 1: the directory move is rejected at the symlink; the subtree is re-tracked at a.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "fd", FromPath: "a", Path: "link/b"},
	}, 4); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (rejected move): %v", err)
	}
	if p, err := st.PathByID(7, "fd"); err != nil || p != "a" {
		t.Fatalf("after rejected move PathByID(fd) = %q, %v; want a", p, err)
	}

	// The symlink is replaced by a real directory, so link/b becomes appliable.
	if err := os.Remove(linkPath); err != nil {
		t.Fatalf("remove symlink: %v", err)
	}
	if err := os.MkdirAll(linkPath, 0o755); err != nil {
		t.Fatalf("mkdir real link dir: %v", err)
	}

	// Batch 2: a directory modify reported at link/b. reconcile keeps the root baseline at the
	// reachable source a; the OpMkdir applies at link/b and must move the whole subtree across.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "modify", FileID: "fd", Path: "link/b", IsDir: true},
	}, 5); err != nil {
		t.Fatalf("reconcile modify: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (modify): %v", err)
	}

	// The subtree must have moved on disk to link/b, source a gone.
	if got := mustRead(t, filepath.Join(viewDir, "link", "b", "x.txt")); string(got) != "xx" {
		t.Errorf("link/b/x.txt = %q, want xx", got)
	}
	if got := mustRead(t, filepath.Join(viewDir, "link", "b", "sub", "y.txt")); string(got) != "yy" {
		t.Errorf("link/b/sub/y.txt = %q, want yy", got)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("stale source dir a survived the relocation: err=%v", err)
	}
	// Root and every descendant baseline must be at link/b, resolvable by file id; nothing left at a.
	for path, id := range map[string]string{
		"link/b": "fd", "link/b/x.txt": "fx", "link/b/sub": "fs", "link/b/sub/y.txt": "fy",
	} {
		b, err := st.GetBaseline(7, path)
		if err != nil {
			t.Errorf("baseline missing at %q: %v", path, err)
			continue
		}
		if b.FileID != id {
			t.Errorf("baseline %q FileID = %q, want %q", path, b.FileID, id)
		}
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v; want %q", id, p, err, path)
		}
	}
	if b, err := st.GetBaseline(7, "link/b"); err != nil {
		t.Errorf("root baseline missing at link/b: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("link/b baseline state = %v, want synced", b.State)
	}
	for _, stale := range []string{"a", "a/x.txt", "a/sub", "a/sub/y.txt"} {
		if _, err := st.GetBaseline(7, stale); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("stale baseline survived at %q: err=%v", stale, err)
		}
	}
}

// rejected-move directory recovery must be crash/error replay-safe. relocateTrackedDirForMkdir
// renames the subtree on disk before it carries the descendant baselines across, so a crash (or
// a SetMeta failure) in that window leaves the disk subtree already at op.Path while the
// baselines still point at the old prefix. A replay then finds the old path gone; it must finish
// the descendant-baseline relocation rather than fall back to a plain mkdir that reindexes only
// the root and strands every descendant baseline at the old prefix forever.
func TestRelocateTrackedDirForMkdirReplaySafeAfterPartialApply(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// Post-crash disk state: the prior apply already renamed a -> link/b, so the subtree is on
	// disk at link/b and nothing remains at a.
	if err := os.MkdirAll(filepath.Join(viewDir, "link", "b", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "link", "b", "x.txt"), []byte("xx"), 0o644); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "link", "b", "sub", "y.txt"), []byte("yy"), 0o644); err != nil {
		t.Fatalf("seed y: %v", err)
	}
	// But the baselines still point at the old prefix a - the crash hit between the disk rename
	// and the baseline relocation.
	for path, b := range map[string]store.Baseline{
		"a":           {FileID: "fd", FileType: "dir", State: store.StateSynced},
		"a/x.txt":     {FileID: "fx", FileType: "file", State: store.StateSynced},
		"a/sub":       {FileID: "fs", FileType: "dir", State: store.StateSynced},
		"a/sub/y.txt": {FileID: "fy", FileType: "file", State: store.StateSynced},
	} {
		if err := st.PutBaseline(7, path, b); err != nil {
			t.Fatalf("seed baseline %q: %v", path, err)
		}
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// The OpMkdir for the directory at its new path link/b is replayed from the durable queue.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7,
		Path: "link/b", FileID: "fd", ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Root and every descendant baseline must be at link/b, resolvable by file id; nothing left at a.
	for path, id := range map[string]string{
		"link/b": "fd", "link/b/x.txt": "fx", "link/b/sub": "fs", "link/b/sub/y.txt": "fy",
	} {
		b, err := st.GetBaseline(7, path)
		if err != nil {
			t.Errorf("baseline missing at %q: %v", path, err)
			continue
		}
		if b.FileID != id {
			t.Errorf("baseline %q FileID = %q, want %q", path, b.FileID, id)
		}
		if p, err := st.PathByID(7, id); err != nil || p != path {
			t.Errorf("PathByID(%q) = %q, %v; want %q", id, p, err, path)
		}
	}
	for _, stale := range []string{"a", "a/x.txt", "a/sub", "a/sub/y.txt"} {
		if _, err := st.GetBaseline(7, stale); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("stale baseline survived at %q: err=%v", stale, err)
		}
	}
	// The disk subtree must remain intact at link/b.
	if got := mustRead(t, filepath.Join(viewDir, "link", "b", "x.txt")); string(got) != "xx" {
		t.Errorf("link/b/x.txt = %q, want xx", got)
	}
	if got := mustRead(t, filepath.Join(viewDir, "link", "b", "sub", "y.txt")); string(got) != "yy" {
		t.Errorf("link/b/sub/y.txt = %q, want yy", got)
	}
}

// mustRead reads a file or fails the test.
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// TestRenameNotDoomedWhileProducerStillQueued pins the other clearing rule of the
// cross-window doom: a dead output must also be cleared by a later window's op that WILL place
// the node there once it succeeds - one that merely failed transiently and stays queued - not
// only by one that already did.
//
// Leaving the mark in that case dooms the rename PERMANENTLY (durable dead-letter trace plus
// the relocated baseline dropped) over a producer that is simply retrying, turning a
// self-healing wait into an unrecoverable path divergence. Ordering is still enforced by
// blockedKeys: the rename must skip behind the queued producer, not run out of order.
//
// Window 1 drops a hop into a.txt (f9 untracked, a foreign node at its source). Window 2 is the
// real download of f9 onto a.txt, failing transiently. Window 3's rename off a.txt must stay
// queued for the next drain.
func TestRenameNotDoomedWhileProducerStillQueued(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fb", FileType: "file", ServerHash: "hb", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}

	dl := &dlClient{dlErr: errors.New("server hiccup")} // the download fails transiently
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	e.limits.DrainBatch = 1 // one op per window, so each hop crosses a boundary

	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "x.txt", NewPath: "a.txt", FileID: "f9"},
		{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %s: %v", op.Type, err)
		}
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue returned nil, want the transient download failure surfaced")
	}

	// Only the first hop left the queue; the download and the rename behind it stay queued.
	if n, err := st.DeadLetterLen(); err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	} else if n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 (the rename must not be doomed by a still-queued producer)", n)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d ops, want 2 (download + rename): %+v", len(pending), pending)
	}
	if pending[0].Type != store.OpDownload || pending[1].Type != store.OpRename {
		t.Errorf("pending order = %v/%v, want download then rename", pending[0].Type, pending[1].Type)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "c.txt")); !os.IsNotExist(err) {
		t.Errorf("c.txt exists: the rename ran ahead of its producer; err=%v", err)
	}
}
