package engine

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// poolOfN is a Transport that models a pool of n workers: at most n leases at once, each
// recorded, so a test can see how many listings a walk really kept in flight. Unlike
// fakeTransport it is safe for concurrent use.
type poolOfN struct {
	slots    chan struct{}
	client   syncClient
	inflight atomic.Int32
	peak     atomic.Int32
}

func newPoolOfN(n int, c syncClient) *poolOfN {
	return &poolOfN{slots: make(chan struct{}, n), client: c}
}

func (p *poolOfN) EventClient() (syncClient, error)   { return p.client, nil }
func (p *poolOfN) ReplaceEvent(context.Context) error { return nil }
func (p *poolOfN) WithWorker(ctx context.Context, fn func(syncClient) error) error {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.slots }()
	n := p.inflight.Add(1)
	defer p.inflight.Add(-1)
	for old := p.peak.Load(); n > old && !p.peak.CompareAndSwap(old, n); old = p.peak.Load() {
	}
	return fn(p.client)
}

// wideTree serves a root with `fan` directories, each holding `files` files, and pauses each
// listing briefly so concurrent ones overlap. onList, when set, runs inside every listing.
type wideTree struct {
	mu     sync.Mutex
	fan    int
	files  int
	onList func(p string)
}

func (w *wideTree) List(_ context.Context, _ int64, p string) ([]actions.Node, bool, error) {
	time.Sleep(2 * time.Millisecond)
	if w.onList != nil {
		w.onList(p)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var nodes []actions.Node
	if p == "/" {
		for i := 0; i < w.fan; i++ {
			nodes = append(nodes, actions.Node{Name: fmt.Sprintf("d%02d", i), IsDir: true, FileID: fmt.Sprintf("dir%d", i)})
		}
		return nodes, true, nil
	}
	for i := 0; i < w.files; i++ {
		nodes = append(nodes, actions.Node{Name: fmt.Sprintf("f%d", i), FileID: p + fmt.Sprint(i), Hash: "h", Size: 1})
	}
	return nodes, true, nil
}

func (w *wideTree) MaxSyncID(context.Context, int64, int64) (int64, error) { return 42, nil }
func (w *wideTree) QueryUserInfo(context.Context) ([]actions.View, error)  { return nil, nil }
func (w *wideTree) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (w *wideTree) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (w *wideTree) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (w *wideTree) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// TestColdStartWalkListsAcrossTheWorkers pins the three things the per-listing lease buys a
// large first sync: listings overlap up to the pool size (not one at a time, not past it),
// status sees the walk's progress and a fresh server contact WHILE it runs, and the catalog
// is complete and the progress cleared once it ends.
func TestColdStartWalkListsAcrossTheWorkers(t *testing.T) {
	tree := &wideTree{fan: 12, files: 5}
	pool := newPoolOfN(3, tree)
	e := New(newTestStore(t), pool, t.TempDir(), "sess", discardLogger())

	var midDirs, midEntries int64
	var midActive bool
	var midContact time.Time
	var once sync.Once
	tree.onList = func(p string) {
		if p != "d10" {
			return
		}
		once.Do(func() {
			_, midDirs, midEntries, _, midActive = e.Listing()
			midContact = e.LastContact()
		})
	}

	started := time.Now()
	catalog, cursor, err := e.walkServerCatalog(context.Background(), 7)
	if err != nil {
		t.Fatalf("walkServerCatalog: %v", err)
	}

	if want := 12 + 12*5; len(catalog) != want {
		t.Errorf("catalog holds %d nodes, want %d", len(catalog), want)
	}
	if cursor != 42 {
		t.Errorf("cursor = %d, want the log head read before the walk", cursor)
	}
	if p := pool.peak.Load(); p < 2 || p > 3 {
		t.Errorf("peak concurrent leases = %d, want listings overlapping up to the pool size (2..3)", p)
	}
	if !midActive || midDirs < 1 || midEntries < 12 {
		t.Errorf("mid-walk Listing() = active %v, %d dirs, %d entries; want the walk visible in progress",
			midActive, midDirs, midEntries)
	}
	if midContact.Before(started) {
		t.Errorf("mid-walk LastContact = %v, want a contact stamped by a listing during the walk", midContact)
	}
	if _, _, _, _, active := e.Listing(); active {
		t.Error("Listing() still reports a walk after it ended")
	}
}
