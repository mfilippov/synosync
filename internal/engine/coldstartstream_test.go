package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// gatedListClient holds the listing of one directory until something else has happened, so a
// test can say "the walk is still in flight" and mean it.
type gatedListClient struct {
	*catalogClient
	gatedDir string
	gate     <-chan struct{}
	// downloaded is closed by the first Download.
	downloaded chan struct{}
	once       sync.Once
}

func (c *gatedListClient) List(ctx context.Context, viewID int64, p string) ([]actions.Node, bool, error) {
	if p == c.gatedDir {
		select {
		case <-c.gate:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, false, errors.New("test: the gated listing waited for a transfer that never came")
		}
	}
	return c.catalogClient.List(ctx, viewID, p)
}

func (c *gatedListClient) Download(ctx context.Context, fileID string, a, b int64, w io.Writer) (actions.Node, error) {
	n, err := c.catalogClient.Download(ctx, fileID, a, b, w)
	c.once.Do(func() { close(c.downloaded) })
	return n, err
}

func coldStreamFixture(t *testing.T) (*store.Store, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "V"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	return st, root
}

// TestColdStartTransfersWhileTheWalkLists is the property the streaming cold start exists
// for: a file in a directory the walk has already listed is downloaded while the walk is
// still listing the rest. The listing of "deep" is held until a download has happened, so a
// cold start that transfers only after the walk ends never gets there.
func TestColdStartTransfersWhileTheWalkLists(t *testing.T) {
	st, root := coldStreamFixture(t)
	first, second := []byte("first file"), []byte("second file")
	inner := &catalogClient{
		tree: map[string][]actions.Node{
			"": {
				{Name: "a.txt", FileID: "f1", Size: int64(len(first)), Hash: md4.ContentHash(first), MTime: 50},
				{Name: "deep", FileID: "d1", IsDir: true},
			},
			"deep": {
				{Name: "b.txt", FileID: "f2", Size: int64(len(second)), Hash: md4.ContentHash(second), MTime: 50},
			},
		},
		content: map[string][]byte{"f1": first, "f2": second},
	}
	downloaded := make(chan struct{})
	c := &gatedListClient{catalogClient: inner, gatedDir: "deep", gate: downloaded, downloaded: downloaded}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "V", "a.txt")); err != nil || string(got) != string(first) {
		t.Fatalf("a.txt = %q, %v; want it downloaded during the walk", got, err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Fatal("InitialSyncDone = false after a completed walk")
	}
	// What the walk listed after the gate opened was planned too, and is the ordinary cycle's
	// to finish if this cold start's last pass did not.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "V", "deep", "b.txt")); err != nil || string(got) != string(second) {
		t.Fatalf("deep/b.txt = %q, %v; want it downloaded", got, err)
	}
}

// TestColdStartPauseInterruptsTheWalk: a pause stops a walk in flight without failing it. The
// view stays pending, so the cold start runs again after the resume, and nothing is reported
// as an error.
func TestColdStartPauseInterruptsTheWalk(t *testing.T) {
	st, root := coldStreamFixture(t)
	inner := &catalogClient{tree: map[string][]actions.Node{
		"":     {{Name: "deep", FileID: "d1", IsDir: true}},
		"deep": {},
	}}
	never := make(chan struct{})
	c := &gatedListClient{catalogClient: inner, gatedDir: "deep", gate: never, downloaded: make(chan struct{})}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	go func() {
		time.Sleep(50 * time.Millisecond)
		e.Pause()
	}()

	err := e.coldStartView(context.Background(), listViewOf(t, st, 7))
	if !errors.Is(err, errColdStartPaused) {
		t.Fatalf("coldStartView = %v, want errColdStartPaused", err)
	}
	if listViewOf(t, st, 7).InitialSyncDone {
		t.Fatal("InitialSyncDone = true after an interrupted walk")
	}
}

// TestColdPlannerDecidesALocalOnlySubtreeAtOnce: a local directory the server does not hold as
// a directory is never listed, so its whole subtree is planned when its parent is, parents
// first. That includes a local directory under a server FILE of the same name, which the
// keep-both arm moves aside whole - its children are planned as local-only, as before.
func TestColdPlannerDecidesALocalOnlySubtreeAtOnce(t *testing.T) {
	st, root := coldStreamFixture(t)
	e := New(st, &catalogTransport{client: &catalogClient{}}, root, "sess", nil)
	server := map[string]actions.Event{
		"clash": serverFile("clash", "c1", "h1", 3),
	}
	local := []scanEntry{
		localDir("new"),
		localDir("new/inner"),
		localFile("new/inner/f.txt", "m1", 1),
		localFile("new/g.txt", "m2", 1),
		localDir("clash"),
		localFile("clash/k.txt", "m3", 1),
	}
	ops, err := e.coldStartReconcile(7, server, local, map[string]bool{}, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	type planned struct {
		typ  store.OpType
		path string
	}
	var got []planned
	for _, op := range ops {
		got = append(got, planned{op.Type, op.Path})
	}
	want := []planned{
		{store.OpResolveConflict, "clash"},
		{store.OpUpload, "clash/k.txt"},
		{store.OpMkdir, "new"},
		{store.OpUpload, "new/g.txt"},
		{store.OpMkdir, "new/inner"},
		{store.OpUpload, "new/inner/f.txt"},
	}
	if len(got) != len(want) {
		t.Fatalf("planned %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("planned %+v, want %+v", got, want)
		}
	}
}

// TestColdPlannerKeepsTheFirstReportOfADirectoryMetTwice: a directory that moves while the walk
// is in flight is listed under both parents, with the same file ids all the way down. Its first
// report is planned; its second subtree is protected, not planned a second time.
func TestColdPlannerKeepsTheFirstReportOfADirectoryMetTwice(t *testing.T) {
	st, root := coldStreamFixture(t)
	e := New(st, &catalogTransport{client: &catalogClient{}}, root, "sess", nil)
	pl, err := e.newColdPlanner(7, nil, map[string]bool{}, false)
	if err != nil {
		t.Fatalf("newColdPlanner: %v", err)
	}
	var ops []store.Op
	pl.ops = &ops
	for _, l := range []coldListing{
		{rel: "", children: []actions.Event{serverDir("a", "da"), serverDir("b", "db")}},
		{rel: "a", children: []actions.Event{serverDir("a/m", "dm")}},
		{rel: "b", children: []actions.Event{serverDir("b/m", "dm")}},
		{rel: "a/m", children: []actions.Event{serverFile("a/m/x", "fx", "h", 1)}},
		{rel: "b/m", children: []actions.Event{serverFile("b/m/x", "fx", "h", 1)}},
	} {
		if err := pl.planDir(l); err != nil {
			t.Fatalf("planDir %q: %v", l.rel, err)
		}
	}
	if err := pl.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	var paths []string
	for _, op := range ops {
		paths = append(paths, op.Path)
	}
	want := []string{"a", "b", "a/m", "a/m/x"}
	if len(paths) != len(want) {
		t.Fatalf("planned %q, want %q", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("planned %q, want %q", paths, want)
		}
	}
}

// TestColdStartCompletesWithoutDrainingTheBacklog: once the walk has ended and every listing is
// planned, the cold start writes its cursor and completes, and what is still queued drains in
// the ordinary cycle. Draining the whole backlog first kept the cursor unwritten - and the poll
// gated - for as long as the transfers took.
func TestColdStartCompletesWithoutDrainingTheBacklog(t *testing.T) {
	st, root := coldStreamFixture(t)
	var nodes []actions.Node
	content := map[string][]byte{}
	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		body := []byte("body of " + name)
		content[name] = body
		nodes = append(nodes, actions.Node{Name: name, FileID: name, Size: int64(len(body)), Hash: md4.ContentHash(body), MTime: 50})
	}
	c := &catalogClient{tree: map[string][]actions.Node{"": nodes}, content: content}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)
	// One op per window, so the pass reaches a window boundary - where it hears that the walk
	// is over - long before the queue is empty.
	e.SetLimits(config.Limits{DrainBatch: 1})

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Fatal("InitialSyncDone = false after a completed walk")
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n == 0 {
		t.Fatal("queue drained inside the cold start; want the backlog left to the ordinary cycle")
	}
}

// TestColdFeedReportsTheEndWithTheLastListings pins the hang the one-slot wake channel allowed:
// the walk's last push and its finish collapse into ONE token, the loop consumes it, and the take
// that follows is the last chance to learn the walk is over - there is no further signal to wait
// for. Withholding done until a later, empty take left the loop waiting on wake forever.
func TestColdFeedReportsTheEndWithTheLastListings(t *testing.T) {
	f := newColdFeed()
	f.push(coldListing{rel: "last"})
	f.finish(nil)
	<-f.wake // the single token both nudges collapsed into
	dirs, done, err := f.take()
	if len(dirs) != 1 || !done || err != nil {
		t.Fatalf("take = %d listing(s), done=%v, err=%v; want the last listing WITH done", len(dirs), done, err)
	}
}
