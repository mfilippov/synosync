package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// catalogPage is one scripted pull_event reply: the events returned at a given cursor
// plus the next cursor the server reports. bareEmpty marks an EMPTY page that came back
// as an unevidenced bare {} (no errno, no event_list) - the shape the
// cold-start first-page rule rejects; a page with events, or an empty page left
// bareEmpty=false, models the evidenced (errno / present-but-empty event_list) reply.
type catalogPage struct {
	events    []actions.Event
	next      int64
	bareEmpty bool
}

// catalogClient is a syncClient whose PullEvent is scripted by cursor, so the cold-start
// catalog reader can be exercised across multiple pages without a network. Every other
// method is a no-op - the cold-start walk only drives List and MaxSyncID.
type catalogClient struct {
	pages     map[int64]catalogPage
	pullErr   error
	pullCalls int
	// content serves Download by file id for the cursor-clamp replay tests,
	// where the incremental syncView that follows the cold start drains the op the
	// replayed event produced. Left nil elsewhere: Download then writes nothing, exactly as
	// before.
	content map[string][]byte
	// nodes serves QueryNode by view-relative path for the blocked-subtree replay tests,
	// where the keep-both resolver the cold start plans for the
	// type-mismatched root must reach its terminal errConflictServerDir dead-letter. An
	// unscripted path keeps the former zero-node reply.
	nodes map[string]actions.Node
	// tree scripts `list` by view-relative directory path ("" is the view root), which is
	// what the cold-start walk reads. A path that is not scripted answers the way an
	// errno-only reply does - no children AND no evidence - so a test that forgets a
	// subtree fails loudly instead of quietly cold-starting against half a tree.
	tree map[string][]actions.Node
	// failList makes a directory's listing FAIL, by view-relative path. It models the shape a
	// walk cannot read on its own: an errno that conflates a vanished directory with a
	// server-side timeout.
	failList map[string]error
	// reList, when a path is present in it, answers every listing of that path AFTER the
	// first. It is how a scenario models the race the corroboration exists for: the parent
	// names a directory when the walk descends, and no longer names it when asked again.
	reList map[string][]actions.Node
	// listed counts how often each path has been listed, so reList can take effect.
	listed map[string]int
	// listCalls counts the `list` turns the cold-start walk made, the walk's counterpart
	// to pullCalls: it is how a scenario proves a cold start ran once and did not repeat.
	listCalls int
	// logHead scripts get_max_sync_id, the probe cold start uses to corroborate an empty
	// page. Zero (the default) reports "cannot tell", which is what keeps every scenario
	// written before the corroboration on its original, uncorroborated path.
	logHead int64
	// logHeadErr fails the probe, modelling a server that answers the pull but not the
	// head; the empty page then keeps its historical reading.
	logHeadErr error
	// mu guards the listing counters: the walk lists on its own goroutine while the drain
	// applies.
	mu sync.Mutex
}

// List makes catalogClient satisfy treeLister, the seam the cold-start walk reads through.
func (c *catalogClient) List(_ context.Context, _ int64, p string) ([]actions.Node, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls++
	if p == "/" {
		p = ""
	}
	if err := c.failList[p]; err != nil {
		return nil, false, err
	}
	if c.listed == nil {
		c.listed = map[string]int{}
	}
	c.listed[p]++
	if again, ok := c.reList[p]; ok && c.listed[p] > 1 {
		return again, true, nil
	}
	nodes, ok := c.tree[p]
	if !ok {
		return nil, false, nil
	}
	return nodes, true, nil
}

// treeFromCatalog turns a flat set of view-relative server nodes into the per-directory
// listings `list` would report for them, synthesising the directories along each path.
//
// It lets a scenario keep saying WHAT the server holds without also spelling out how a
// walk reaches it. A scenario that needs to say something about the walk itself - an
// unevidenced directory, a listing that disagrees with its parent - scripts tree directly.
func treeFromCatalog(evs []actions.Event) map[string][]actions.Node {
	tree := map[string][]actions.Node{"": {}}
	seen := map[string]bool{}
	var ensure func(rel string)
	ensure = func(rel string) {
		if rel == "" || seen[rel] {
			return
		}
		seen[rel] = true
		parent, name := splitRel(rel)
		ensure(parent)
		tree[parent] = append(tree[parent], actions.Node{Name: name, IsDir: true})
		if _, ok := tree[rel]; !ok {
			tree[rel] = []actions.Node{}
		}
	}
	for _, ev := range evs {
		parent, name := splitRel(ev.Path)
		ensure(parent)
		if ev.IsDir {
			ensure(ev.Path)
			continue
		}
		tree[parent] = append(tree[parent], actions.Node{
			Name: name, FileID: ev.FileID, Size: ev.Size, Hash: ev.Hash, MTime: ev.MTime,
		})
	}
	return tree
}

// splitRel splits a view-relative path into its parent and leaf.
func splitRel(rel string) (parent, name string) {
	i := strings.LastIndex(rel, "/")
	if i < 0 {
		return "", rel
	}
	return rel[:i], rel[i+1:]
}

// MaxSyncID makes catalogClient satisfy logHeadProbe.
func (c *catalogClient) MaxSyncID(context.Context, int64, int64) (int64, error) {
	return c.logHead, c.logHeadErr
}

func (c *catalogClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return nil, nil
}
func (c *catalogClient) QueryNode(_ context.Context, _ int64, path string) (actions.Node, error) {
	return c.nodes[path], nil
}
func (c *catalogClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *catalogClient) PullEvent(ctx context.Context, viewID, _, since int64) ([]actions.Event, int64, error) {
	evs, next, _, err := c.PullEventEvidenced(ctx, viewID, 0, since)
	return evs, next, err
}

// PullEventEvidenced makes catalogClient the evidence-reporting variant the incremental poll
// drives. A page with events, or an empty page not marked bareEmpty,
// reports evidenced; an unscripted cursor (beyond the catalog) is an evidenced empty page;
// only a page explicitly marked bareEmpty reports the unevidenced bare {} shape.
func (c *catalogClient) PullEventEvidenced(_ context.Context, _, _, since int64) ([]actions.Event, int64, bool, error) {
	c.pullCalls++
	if c.pullErr != nil {
		return nil, 0, false, c.pullErr
	}
	p, ok := c.pages[since]
	if !ok {
		// An unscripted cursor is treated as the end of the catalog: an evidenced empty page.
		return nil, since, true, nil
	}
	evidenced := len(p.events) > 0 || !p.bareEmpty
	return p.events, p.next, evidenced, nil
}
func (c *catalogClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	body := c.content[fileID]
	if _, err := w.Write(body); err != nil {
		return actions.Node{}, err
	}
	return actions.Node{FileID: fileID, Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: 60}, nil
}

// catalogTransport hands one syncClient to the engine for worker leases, recording how
// many leases were taken so a test can confirm the whole catalog pull runs under a single
// lease. A preset workerErr models an Acquire failure (the pool closed).
type catalogTransport struct {
	client    syncClient
	workerErr error
	leases    int
	mu        sync.Mutex // the walk leases concurrently with the drain
}

func (t *catalogTransport) EventClient() (syncClient, error) { return t.client, nil }
func (t *catalogTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	t.mu.Lock()
	t.leases++
	t.mu.Unlock()
	if t.workerErr != nil {
		return t.workerErr
	}
	return fn(t.client)
}
func (t *catalogTransport) ReplaceEvent(context.Context) error { return nil }

func createEvent(path, fileID string, syncID int64, isDir bool) actions.Event {
	return actions.Event{Action: "create", Path: path, FileID: fileID, SyncID: syncID, IsDir: isDir}
}

// coldStartClampV1 is the content the clamp-replay fixtures hold locally AND server-side while
// the cold start runs, so the catalog's create adopts (equal hash, no transfer) and the only
// thing that can move the file afterwards is the concurrent event replayed from the clamped
// cursor.
var coldStartClampV1 = []byte("v1 content")

// TestColdStartConcurrentModifyIsNotLost covers the same scenario as the delete/move variants
// below - another client changed a node before this one walked it - for a CHANGE IN CONTENT.
// A walk has nothing to fold: the listing reports the node as it is now, and that is what the
// merge meets.
//
// The invariant is unchanged by any of that: because the client never agreed to the local bytes
// as a baseline, a divergence between them and the server's content funnels into keep-both
// rather than overwriting either side.
func TestColdStartConcurrentModifyIsNotLost(t *testing.T) {
	v2 := []byte("v2 from the other client")
	_, st, _ := seedColdStartAgainstTree(t, map[string][]actions.Node{"": {
		{Name: "same.txt", FileID: "s1", Hash: md4.ContentHash(v2), Size: int64(len(v2)), MTime: 60},
	}}, map[string][]byte{"s1": v2})

	b, err := st.GetBaseline(7, "same.txt")
	if err != nil {
		t.Fatalf("GetBaseline same.txt: %v", err)
	}
	if b.State != store.StateConflict {
		t.Fatalf("same.txt baseline state = %q, want conflict (local bytes diverge from the folded server facts)", b.State)
	}
	// The divergence is owned by a queued resolver, never dropped.
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n == 0 {
		t.Error("no op queued for the diverged path; the concurrent modify was lost")
	}
}

// TestColdStartViewEvidencedEmptyFirstPageUploadsLocal is the contrast: an EVIDENCED empty first
// page is a genuinely empty server, so the same local file is planned as a fresh OpUpload and the
// view is marked InitialSyncDone - the pre-fix behaviour, preserved for the legitimate case.
func TestColdStartViewEvidencedEmptyFirstPageUploadsLocal(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "only.txt"), []byte("local only"), 0o644); err != nil {
		t.Fatalf("write only.txt: %v", err)
	}
	// The walk's version of the same claim: the view root lists, and lists NOTHING.
	// Distinct from an unscripted path, which answers unevidenced and stops the walk.
	c := &catalogClient{tree: map[string][]actions.Node{"": {}}}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "only.txt" {
		t.Fatalf("ops = %+v, want a single outbound upload of only.txt", ops)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Errorf("InitialSyncDone = false, want true after an evidenced empty cold start")
	}
}

// TestColdStartPendingUnevidencedListingIsNonTerminal: a view whose root answers without
// evidence must not stop Run. coldStartPending logs it and returns nil, leaving the view
// pending for the next cycle.
//
// The fixture matters. An unevidenced listing is an errno-only reply - no children array at
// all - which is NOT the same as a listing that says "empty". catalogClient answers an
// unscripted path exactly that way, so the root is left unscripted deliberately; a tree with an
// empty root would be the opposite case and would cold-start cleanly.
func TestColdStartPendingUnevidencedListingIsNonTerminal(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "V"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	c := &catalogClient{logHead: 5} // no tree at all: the root lists unevidenced
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("coldStartPending = %v, want nil (an unevidenced listing is non-terminal)", err)
	}
	if listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = true, want false (nothing was proven about the tree)")
	}
	// And nothing was planned from a tree the client never saw.
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0", n, err)
	}
}

// serverFile / serverDir / localFile / localDir build the catalog-event and scan-entry
// fixtures the cold-start merge cases consume.
func serverFile(path, fileID, hash string, size int64) actions.Event {
	return actions.Event{Action: "create", Path: path, FileID: fileID, Hash: hash, Size: size, MTime: 50, SyncID: 1}
}

func serverDir(path, fileID string) actions.Event {
	return actions.Event{Action: "create", Path: path, FileID: fileID, IsDir: true, MTime: 50, SyncID: 1}
}

func localFile(path, md4 string, size int64) scanEntry {
	return scanEntry{relPath: path, size: size, mtime: 100, unixPerm: 644, md4: md4}
}

func localDir(path string) scanEntry {
	return scanEntry{relPath: path, isDir: true, mtime: 100, unixPerm: 755}
}

func catalogMap(evs ...actions.Event) map[string]actions.Event {
	m := make(map[string]actions.Event, len(evs))
	for _, ev := range evs {
		m[ev.Path] = ev
	}
	return m
}

// TestColdStartReconcilePolicy exercises one case per safe-merge branch on an in-memory
// store: the op (if any) it enqueues and the baseline state it records.
func TestColdStartReconcilePolicy(t *testing.T) {
	type want struct {
		op    store.OpType        // "" => no op enqueued
		dir   store.Direction     // direction of the enqueued op
		state store.BaselineState // expected baseline state at the path
	}
	cases := []struct {
		name   string
		server map[string]actions.Event
		local  []scanEntry
		path   string
		want   want
	}{
		{
			name:   "dir both adopt",
			server: catalogMap(serverDir("d", "s1")),
			local:  []scanEntry{localDir("d")},
			path:   "d",
			want:   want{state: store.StateSynced},
		},
		{
			name:   "server-only dir mkdir",
			server: catalogMap(serverDir("d", "s1")),
			path:   "d",
			want:   want{op: store.OpMkdir, dir: store.DirInbound, state: store.StatePending},
		},
		{
			name:  "local-only dir mkdir",
			local: []scanEntry{localDir("d")},
			path:  "d",
			want:  want{op: store.OpMkdir, dir: store.DirOutbound, state: store.StatePending},
		},
		{
			name:   "server-only file download",
			server: catalogMap(serverFile("a.txt", "s1", "h1", 3)),
			path:   "a.txt",
			want:   want{op: store.OpDownload, dir: store.DirInbound, state: store.StatePending},
		},
		{
			name:   "equal hash adopt",
			server: catalogMap(serverFile("a.txt", "s1", "h1", 3)),
			local:  []scanEntry{localFile("a.txt", "h1", 3)},
			path:   "a.txt",
			want:   want{state: store.StateSynced},
		},
		{
			name:   "divergent file conflict",
			server: catalogMap(serverFile("a.txt", "s1", "h1", 3)),
			local:  []scanEntry{localFile("a.txt", "h2", 5)},
			path:   "a.txt",
			want:   want{op: store.OpResolveConflict, dir: store.DirOutbound, state: store.StateConflict},
		},
		{
			name:  "local-only file upload",
			local: []scanEntry{localFile("a.txt", "h1", 3)},
			path:  "a.txt",
			want:  want{op: store.OpUpload, dir: store.DirOutbound, state: store.StatePending},
		},
		{
			// A server DIRECTORY over a local file is an inbound mkdir, not a keep-both
			// resolve: the resolver settles file CONTENT and cannot settle a directory, so it
			// only ever dead-lettered here. The mkdir's occupant guard is what preserves the
			// local file, as a keep-both copy, when it runs.
			name:   "server directory over a local file plans the mkdir",
			server: catalogMap(serverDir("x", "s1")),
			local:  []scanEntry{localFile("x", "h1", 3)},
			path:   "x",
			want:   want{op: store.OpMkdir, dir: store.DirInbound, state: store.StatePending},
		},
		{
			// The mirror image still resolves: preserveAside moves a whole directory just as
			// it moves a file, so keep-both converges without a special case.
			name:   "server file over a local directory still resolves",
			server: catalogMap(serverFile("x", "s1", "h1", 3)),
			local:  []scanEntry{localDir("x")},
			path:   "x",
			want:   want{op: store.OpResolveConflict, dir: store.DirOutbound, state: store.StateConflict},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, st := newReconcileEngine(t, "self")
			ops, err := e.coldStartReconcile(1, tc.server, tc.local, nil, false)
			if err != nil {
				t.Fatalf("coldStartReconcile: %v", err)
			}
			if tc.want.op == "" {
				if len(ops) != 0 {
					t.Fatalf("enqueued %d ops, want 0: %+v", len(ops), ops)
				}
			} else {
				if len(ops) != 1 {
					t.Fatalf("enqueued %d ops, want 1: %+v", len(ops), ops)
				}
				if ops[0].Type != tc.want.op || ops[0].Direction != tc.want.dir {
					t.Errorf("op = %s/%s, want %s/%s", ops[0].Type, ops[0].Direction, tc.want.op, tc.want.dir)
				}
				if ops[0].Path != tc.path {
					t.Errorf("op path = %q, want %q", ops[0].Path, tc.path)
				}
			}
			b, err := st.GetBaseline(1, tc.path)
			if err != nil {
				t.Fatalf("GetBaseline %q: %v", tc.path, err)
			}
			if b.State != tc.want.state {
				t.Errorf("baseline state = %q, want %q", b.State, tc.want.state)
			}
		})
	}
}

// TestColdStartReconcileNoDeletions confirms a baseline-less local-or-server-only path is
// never removed: cold start only ever creates/adopts, so a local file with no server node
// becomes an upload, never a remove, and the merge enqueues nothing destructive.
func TestColdStartReconcileNoDeletions(t *testing.T) {
	e, _ := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(serverFile("server.txt", "s1", "h1", 3)),
		[]scanEntry{localFile("local.txt", "h2", 5)},
		nil,
		false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			t.Errorf("cold start must never enqueue a remove, got %+v", op)
		}
	}
	if len(ops) != 2 {
		t.Fatalf("enqueued %d ops, want 2 (one download, one upload): %+v", len(ops), ops)
	}
}

// TestColdStartReconcileSkipsQueuedPath confirms the partial-failure retry is idempotent: a
// path the durable queue already references (an op left by a prior cold-start attempt that has
// not yet drained) is skipped, so the re-fold of the catalog does not enqueue a duplicate op.
func TestColdStartReconcileSkipsQueuedPath(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// Simulate an OpUpload left queued by a previous cold-start attempt for a local-only file.
	if _, err := st.Enqueue(store.Op{
		Type:      store.OpUpload,
		Direction: store.DirOutbound,
		ViewID:    1,
		Path:      "already.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ops, err := e.coldStartReconcile(1,
		nil,
		[]scanEntry{
			localFile("already.txt", "h1", 3), // already queued -> must be skipped
			localFile("fresh.txt", "h2", 4),   // not queued -> a fresh upload
		},
		nil,
		false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (only the un-queued path): %+v", len(ops), ops)
	}
	if ops[0].Path != "fresh.txt" || ops[0].Type != store.OpUpload {
		t.Errorf("op = %+v, want upload of fresh.txt", ops[0])
	}
	// The already-queued path must not have a second op nor have its baseline rewritten.
	if _, err := st.GetBaseline(1, "already.txt"); !isNotFound(err) {
		t.Errorf("already-queued path should not be re-baselined, got err %v", err)
	}
}

// TestColdStartReconcileSkipsNonContainableServerPath confirms a server create whose path
// escapes the sync root is skipped up front - no baseline written, no op enqueued - matching
// the incremental reconcile's containment screen, while a sibling containable path proceeds.
func TestColdStartReconcileSkipsNonContainableServerPath(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(
			serverFile("../escape.txt", "s1", "h1", 3), // non-containable -> skipped
			serverFile("ok.txt", "s2", "h2", 4),        // containable -> inbound download
		),
		nil,
		nil,
		false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (only the containable path): %+v", len(ops), ops)
	}
	if ops[0].Path != "ok.txt" || ops[0].Type != store.OpDownload {
		t.Errorf("op = %+v, want download of ok.txt", ops[0])
	}
	// The escaping path must never have a baseline written.
	if _, err := st.GetBaseline(1, "../escape.txt"); !isNotFound(err) {
		t.Errorf("non-containable path should not be baselined, got err %v", err)
	}
}

// TestColdStartReconcilePlansTheMkdirOverATypeMismatch: where the server holds a directory and
// disk holds a regular file, the inbound mkdir is planned for that path and the subtree beneath
// it is planned NORMALLY behind it.
//
// The subtree used to be deferred, because the parent was surfaced as an OpResolveConflict that
// could never settle a server directory: a child download would then meet a local FILE where it
// needed a directory (ENOTDIR) and wedge the queue. The mkdir removes the premise - its
// occupant guard sets the local file aside as a keep-both copy and frees the path - and the
// executor's ancestor/descendant serialisation lands the children behind it.
func TestColdStartReconcilePlansTheMkdirOverATypeMismatch(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(
			serverDir("foo", "s1"),               // server: directory at foo
			serverFile("foo/bar", "s2", "h1", 3), // server-only child under foo
			serverDir("foo/sub", "s3"),           // server-only child dir under foo
			serverFile("ok.txt", "s4", "h2", 4),  // unrelated server-only file
		),
		[]scanEntry{localFile("foo", "hX", 9)}, // local: a regular file at foo (type mismatch)
		nil,
		false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	// The mkdir for foo, both of its children, and the unrelated download.
	want := map[string]store.OpType{
		"foo":     store.OpMkdir,
		"foo/bar": store.OpDownload,
		"foo/sub": store.OpMkdir,
		"ok.txt":  store.OpDownload,
	}
	if len(ops) != len(want) {
		t.Fatalf("enqueued %d ops, want %d (mkdir foo + its subtree + ok.txt): %+v", len(ops), len(want), ops)
	}
	for _, op := range ops {
		if w, ok := want[op.Path]; !ok || op.Type != w {
			t.Errorf("op %+v: want type %q at that path", op, w)
		}
		if op.Direction != store.DirInbound {
			t.Errorf("op %+v: want inbound", op)
		}
	}
	// The subtree is planned, so its baselines exist - that is the whole difference.
	for _, p := range []string{"foo/bar", "foo/sub"} {
		if _, err := st.GetBaseline(1, p); err != nil {
			t.Errorf("GetBaseline %q: %v, want the child planned behind the mkdir", p, err)
		}
	}
}

// TestColdStartReconcilePlansTheMkdirOnAReadOnlyView is the arm that is easiest to get wrong.
// An inbound mkdir is not a push, so a read-only view gets it too - the read-only rule
// suppresses sending the local side, not receiving the server's. Skipping it there left the
// view with a collision nothing resolved and nothing an operator could see.
func TestColdStartReconcilePlansTheMkdirOnAReadOnlyView(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(
			serverDir("foo", "s1"),               // server: directory at foo
			serverFile("foo/bar", "s2", "h1", 3), // server-only child under foo
		),
		[]scanEntry{localFile("foo", "hX", 9)}, // local: a regular file at foo (type mismatch)
		nil,
		true) // read-only view
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("enqueued %d ops, want 2 (mkdir foo + download foo/bar, both inbound): %+v", len(ops), ops)
	}
	for _, op := range ops {
		if op.Direction != store.DirInbound {
			t.Errorf("op %+v on a read-only view must be inbound", op)
		}
	}
	if _, err := st.GetBaseline(1, "foo/bar"); err != nil {
		t.Errorf("GetBaseline foo/bar: %v, want the child planned behind the mkdir", err)
	}
}

// TestColdStartReconcileProtectsUnreadableLocalPath confirms a path the scanner observed on
// disk but could not surface (unreadable/EACCES, or under an unreadable subtree) is left
// untouched even when the server holds a node at the same path: it must not be downloaded over,
// mirroring localReconcile's pathProtected. Without this guard the locally-unreadable file's
// content would be silently overwritten by the server version with no keep-both.
func TestColdStartReconcileProtectsUnreadableLocalPath(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(
			serverFile("locked.txt", "s1", "h1", 3), // present on server, unreadable locally
			serverFile("ok.txt", "s2", "h2", 4),     // server-only, readable -> download
		),
		nil, // scan surfaced no entries (locked.txt could not be read)
		map[string]bool{"locked.txt": true},
		false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (only the readable server-only path): %+v", len(ops), ops)
	}
	if ops[0].Path != "ok.txt" || ops[0].Type != store.OpDownload {
		t.Errorf("op = %+v, want download of ok.txt", ops[0])
	}
	// The protected path must not be downloaded over nor have a baseline written.
	if _, err := st.GetBaseline(1, "locked.txt"); !isNotFound(err) {
		t.Errorf("unreadable local path should not be baselined/clobbered, got err %v", err)
	}
}

// TestColdStartReconcileReadOnly checks a read-only view runs only inbound downloads and
// adoptions: a local-only file and a divergent file are left untouched (no op, no baseline),
// while a server-only file still downloads and a matching file is still adopted.
func TestColdStartReconcileReadOnly(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	server := catalogMap(
		serverFile("pull.txt", "s1", "h1", 3),  // server-only -> download
		serverFile("same.txt", "s2", "h2", 4),  // matching -> adopt
		serverFile("clash.txt", "s3", "h3", 5), // divergent -> skipped on read-only
	)
	local := []scanEntry{
		localFile("same.txt", "h2", 4),  // matches server
		localFile("clash.txt", "hX", 9), // diverges from server
		localFile("only.txt", "h9", 1),  // local-only -> skipped on read-only
	}
	ops, err := e.coldStartReconcile(1, server, local, nil, true)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (only the inbound download): %+v", len(ops), ops)
	}
	if ops[0].Type != store.OpDownload || ops[0].Path != "pull.txt" {
		t.Errorf("op = %+v, want download of pull.txt", ops[0])
	}
	// The matching file is adopted as synced.
	if b, err := st.GetBaseline(1, "same.txt"); err != nil {
		t.Fatalf("GetBaseline same.txt: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("same.txt state = %q, want synced", b.State)
	}
	// The divergent and local-only paths are left untouched: no baseline written.
	if _, err := st.GetBaseline(1, "clash.txt"); !isNotFound(err) {
		t.Errorf("clash.txt should have no baseline on read-only view, got err %v", err)
	}
	if _, err := st.GetBaseline(1, "only.txt"); !isNotFound(err) {
		t.Errorf("only.txt should have no baseline on read-only view, got err %v", err)
	}
}

// TestColdStartReconcileParentBeforeChild checks directory creates are enqueued ahead of the
// files and subdirectories nested under them, so resolveParentID finds each parent first.
func TestColdStartReconcileParentBeforeChild(t *testing.T) {
	e, _ := newReconcileEngine(t, "self")
	// Server-only nested tree (inbound) plus a local-only nested tree (outbound), supplied to
	// the merge out of order to prove the enqueue is sorted parent-before-child.
	server := catalogMap(
		serverFile("in/sub/leaf.txt", "s3", "h1", 3),
		serverDir("in/sub", "s2"),
		serverDir("in", "s1"),
	)
	local := []scanEntry{
		localFile("out/sub/leaf.txt", "h2", 4),
		localDir("out/sub"),
		localDir("out"),
	}
	ops, err := e.coldStartReconcile(1, server, local, nil, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	pos := make(map[string]int, len(ops))
	for i, op := range ops {
		key := op.Path
		pos[key] = i
	}
	checks := [][2]string{
		{"in", "in/sub"},
		{"in/sub", "in/sub/leaf.txt"},
		{"out", "out/sub"},
		{"out/sub", "out/sub/leaf.txt"},
	}
	for _, c := range checks {
		parent, child := c[0], c[1]
		if pos[parent] >= pos[child] {
			t.Errorf("parent %q (pos %d) must be enqueued before child %q (pos %d)", parent, pos[parent], child, pos[child])
		}
	}
}

// TestColdStartReconcileCaseFoldMerge proves the merge-join folds a server node and a local
// file that differ only in case into a single row on a case-insensitive filesystem, rather
// than producing a redundant download+upload pair: a server "Foo.txt" and a local "foo.txt"
// with equal content adopt as one synced baseline under the server's case, and a divergent
// pair keeps both - exactly the behaviour the former serverByKey/local maps gave.
func TestColdStartReconcileCaseFoldMerge(t *testing.T) {
	t.Run("equal content adopts under server case", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, st := newReconcileEngine(t, "self")
		ops, err := e.coldStartReconcile(1,
			catalogMap(serverFile("Foo.txt", "s1", "h1", 3)),
			[]scanEntry{localFile("foo.txt", "h1", 3)}, // same content, divergent case
			nil,
			false)
		if err != nil {
			t.Fatalf("coldStartReconcile: %v", err)
		}
		if len(ops) != 0 {
			t.Fatalf("enqueued %d ops, want 0 (case-only match adopts, no transfer): %+v", len(ops), ops)
		}
		// The baseline is recorded under the server's canonical case, not the local case.
		if b, err := st.GetBaseline(1, "Foo.txt"); err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		} else if b.State != store.StateSynced {
			t.Errorf("Foo.txt state = %q, want synced", b.State)
		}
		if _, err := st.GetBaseline(1, "foo.txt"); !isNotFound(err) {
			t.Errorf("a second baseline at the local case foo.txt must not be written, got err %v", err)
		}
	})
	t.Run("divergent content keeps both", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, st := newReconcileEngine(t, "self")
		ops, err := e.coldStartReconcile(1,
			catalogMap(serverFile("Foo.txt", "s1", "h1", 3)),
			[]scanEntry{localFile("foo.txt", "h2", 5)}, // divergent content, divergent case
			nil,
			false)
		if err != nil {
			t.Fatalf("coldStartReconcile: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpResolveConflict {
			t.Fatalf("ops = %+v, want a single keep-both conflict", ops)
		}
		if ops[0].Path != "Foo.txt" {
			t.Errorf("conflict op path = %q, want the server case Foo.txt", ops[0].Path)
		}
		if b, err := st.GetBaseline(1, "Foo.txt"); err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		} else if b.State != store.StateConflict {
			t.Errorf("Foo.txt state = %q, want conflict", b.State)
		}
	})
}

// TestColdStartReconcilePlansAWholeSubtreeOnAReadOnlyView is the ordering property under the
// mismatch: the merge join walks rows in ascending path order, so the parent's mkdir is planned
// before any of its children and the executor's ancestor/descendant serialisation keeps them
// behind it. Nothing is deferred, and a read-only view is no exception - inbound work is not a
// push. An unrelated path outside the subtree is unaffected either way.
func TestColdStartReconcilePlansAWholeSubtreeOnAReadOnlyView(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(
			serverDir("d", "s1"),                     // server: directory at d
			serverFile("d/child.txt", "s2", "h1", 3), // server-only child under the blocked d
			serverDir("d/sub", "s3"),                 // server-only child dir under d
			serverFile("keep.txt", "s4", "h2", 4),    // unrelated server-only -> still downloads
		),
		[]scanEntry{localFile("d", "hX", 9)}, // local: a regular file at d (type mismatch)
		nil,
		true) // read-only view
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	want := map[string]store.OpType{
		"d":           store.OpMkdir,
		"d/child.txt": store.OpDownload,
		"d/sub":       store.OpMkdir,
		"keep.txt":    store.OpDownload,
	}
	if len(ops) != len(want) {
		t.Fatalf("enqueued %d ops, want %d: %+v", len(ops), len(want), ops)
	}
	// The parent's mkdir must be planned before anything beneath it.
	var mkdirAt, childAt = -1, -1
	for i, op := range ops {
		if w, ok := want[op.Path]; !ok || op.Type != w {
			t.Errorf("op %+v: want type %q at that path", op, w)
		}
		if op.Direction != store.DirInbound {
			t.Errorf("op %+v on a read-only view must be inbound", op)
		}
		switch op.Path {
		case "d":
			mkdirAt = i
		case "d/child.txt":
			childAt = i
		}
	}
	if mkdirAt < 0 || childAt < 0 || mkdirAt > childAt {
		t.Errorf("mkdir of d at %d, child at %d: the parent must be planned first", mkdirAt, childAt)
	}
	for _, p := range []string{"d", "d/child.txt", "d/sub"} {
		if _, err := st.GetBaseline(1, p); err != nil {
			t.Errorf("GetBaseline %q: %v, want the subtree planned", p, err)
		}
	}
}

// listViewOf reads back the single stored view as a store.ListView, the form coldStartView
// and coldStartPending consume.
func listViewOf(t *testing.T, st *store.Store, viewID uint64) store.ListView {
	t.Helper()
	v, err := st.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView %d: %v", viewID, err)
	}
	return store.ListView{ViewID: viewID, View: v}
}

// TestColdStartViewMixed drives the full per-view cold start over a scripted catalog and a
// real on-disk scan, confirming a mixed set folds into the expected ops/baselines, the cursor
// advances to the catalog max, and the view is marked InitialSyncDone. The queue is only
// planned here (coldStartView does not drain), so the assertions are on the enqueued ops and
// the recorded baselines, not on applied disk state.
func TestColdStartViewMixed(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	sameContent := []byte("same content")
	if err := os.WriteFile(filepath.Join(viewDir, "same.txt"), sameContent, 0o644); err != nil {
		t.Fatalf("write same.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "clash.txt"), []byte("local clash"), 0o644); err != nil {
		t.Fatalf("write clash.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "only.txt"), []byte("local only"), 0o644); err != nil {
		t.Fatalf("write only.txt: %v", err)
	}
	sameHash := md4.ContentHash(sameContent)
	serverClashHash := md4.ContentHash([]byte("server clash")) // differs from the local clash.txt
	c := &catalogClient{logHead: 10, tree: treeFromCatalog([]actions.Event{
		serverFile("pull.txt", "p1", "ph", 4),              // server-only -> download
		serverFile("same.txt", "s1", sameHash, 12),         // matching -> adopt synced
		serverFile("clash.txt", "c1", serverClashHash, 12), // divergent -> keep-both,
	})}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	gotOps := make(map[string]store.Op, len(ops))
	for _, op := range ops {
		gotOps[op.Path] = op.Op
	}
	if len(ops) != 3 {
		t.Fatalf("enqueued %d ops, want 3 (download, conflict, upload): %+v", len(ops), ops)
	}
	if op, ok := gotOps["pull.txt"]; !ok || op.Type != store.OpDownload || op.Direction != store.DirInbound {
		t.Errorf("pull.txt op = %+v, want inbound download", op)
	}
	if op, ok := gotOps["clash.txt"]; !ok || op.Type != store.OpResolveConflict {
		t.Errorf("clash.txt op = %+v, want resolve-conflict", op)
	}
	if op, ok := gotOps["only.txt"]; !ok || op.Type != store.OpUpload || op.Direction != store.DirOutbound {
		t.Errorf("only.txt op = %+v, want outbound upload", op)
	}

	wantState := map[string]store.BaselineState{
		"pull.txt":  store.StatePending,
		"same.txt":  store.StateSynced,
		"clash.txt": store.StateConflict,
		"only.txt":  store.StatePending,
	}
	for p, want := range wantState {
		b, err := st.GetBaseline(7, p)
		if err != nil {
			t.Fatalf("GetBaseline %q: %v", p, err)
		}
		if b.State != want {
			t.Errorf("%s baseline state = %q, want %q", p, b.State, want)
		}
	}

	v := listViewOf(t, st, 7).View
	if !v.InitialSyncDone {
		t.Errorf("InitialSyncDone = false, want true after cold start")
	}
	// The cursor is the log head read BEFORE the walk, not a maximum derived from the
	// catalog: everything the walk saw is at least that new, so replaying forward from it
	// re-asserts what was recorded and delivers whatever changed mid-walk.
	if v.LastSeenSyncID != 10 {
		t.Errorf("cursor = %d, want 10 (the log head read before the walk)", v.LastSeenSyncID)
	}
}

// TestColdStartViewReadOnly confirms a read-only view's cold start runs only the inbound
// download and adoption, never an outbound upload or keep-both - pushing to a read-only view
// would only draw a server rejection.
func TestColdStartViewReadOnly(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "ro", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "only.txt"), []byte("local only"), 0o644); err != nil {
		t.Fatalf("write only.txt: %v", err)
	}
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("pull.txt", "p1", "ph", 4),
	})}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Path != "pull.txt" {
		t.Fatalf("ops = %+v, want a single inbound download of pull.txt", ops)
	}
	if _, err := st.GetBaseline(7, "only.txt"); !isNotFound(err) {
		t.Errorf("only.txt must not be pushed on a read-only view, got baseline err %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Errorf("InitialSyncDone = false, want true")
	}
}

// TestColdStartPendingSkipsDoneView confirms coldStartPending never re-pulls a view whose
// cold start has already completed (InitialSyncDone true), so the expensive full-catalog pull
// runs once and is not repeated every cycle.
func TestColdStartPendingSkipsDoneView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("a.txt", "s1", "h", 3),
	})}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)

	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("coldStartPending: %v", err)
	}
	if c.pullCalls != 0 {
		t.Errorf("pullCalls = %d, want 0 (a done view must not be re-pulled)", c.pullCalls)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("PendingLen = %d, want 0 (no ops for a done view)", n)
	}
}

// TestColdStartPendingSkipsAbsentView confirms coldStartPending excludes a view marked absent
// (removed/revoked server-side) even when its cold start has NOT completed, mirroring the
// pollOnce/localScan skips: its view id is gone from the server, so the full-catalog pull would
// fail every cycle and re-spin. It must run no pull and leave InitialSyncDone false so the view
// resumes cleanly if it reappears.
func TestColdStartPendingSkipsAbsentView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// A non-empty page would make pullCalls > 0 if the cold start ran, so pullCalls==0 is
	// positive proof the absent view was skipped rather than just producing no ops.
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("a.txt", "s1", "h", 3),
	})}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)
	e.absentViews[7] = true // view 7 removed server-side

	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("coldStartPending: %v", err)
	}
	if c.pullCalls != 0 {
		t.Errorf("pullCalls = %d, want 0 (an absent view must not run its cold-start pull)", c.pullCalls)
	}
	if listViewOf(t, st, 7).InitialSyncDone {
		t.Errorf("InitialSyncDone = true, want false (absent view must stay pending for retry on reappearance)")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("PendingLen = %d, want 0 (no ops for an absent view)", n)
	}
}

// TestColdStartPendingRunsOnce confirms coldStartPending runs the cold start for a not-yet-done
// view and marks it done, so a second pass skips it (the full-catalog pull is not repeated).
func TestColdStartPendingRunsOnce(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// A second view added later (not yet done) must also be picked up on the same pass.
	if err := st.PutView(8, store.View{Name: "W", Privilege: "rw", NodeID: "root8"}); err != nil {
		t.Fatalf("PutView 8: %v", err)
	}
	c := &catalogClient{tree: map[string][]actions.Node{"": {}}}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)

	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("first coldStartPending: %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone || !listViewOf(t, st, 8).InitialSyncDone {
		t.Fatalf("both views should be marked done after the first pass")
	}
	firstCalls := c.listCalls
	if firstCalls == 0 {
		t.Fatalf("listCalls = 0, want > 0 (cold start must walk the tree)")
	}

	// A second pass: both views are now done, so no further pull happens.
	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("second coldStartPending: %v", err)
	}
	if c.listCalls != firstCalls {
		t.Errorf("listCalls = %d, want unchanged at %d (cold start must not repeat)", c.listCalls, firstCalls)
	}
}

// TestLocalScanSkipsColdStartPendingView confirms localScan never folds a view's on-disk files
// into outbound ops before its cold start has run: doing so would re-upload the whole tree as
// brand-new creates. A view with InitialSyncDone false and a local file enqueues nothing.
func TestLocalScanSkipsColdStartPendingView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "new.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("write new.txt: %v", err)
	}
	e := New(st, &catalogTransport{client: &catalogClient{}}, root, "sess", nil)

	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("PendingLen = %d, want 0 (a cold-start-pending view must be skipped by localScan)", n)
	}
}

// TestPollOnceSkipsColdStartPendingView confirms pollOnce does not subscribe a view whose cold
// start has not finished: with the only view still pending it backs off without long-polling,
// so an event can never reconcile against an empty baseline catalog.
func TestPollOnceSkipsColdStartPendingView(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		views:  []actions.View{{ViewID: 7, Name: "V", Privilege: "rw", NodeID: "root"}},
		pollFn: func(int) ([]int64, error) { return nil, nil },
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "sess", nil)

	// A short deadline bounds the no-subscribe backoff (noViewRetryDelay) so the test does not
	// wait the full interval; pollOnce returns the deadline error after the backoff begins.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := e.pollOnce(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pollOnce err = %v, want DeadlineExceeded from the no-subscribe backoff", err)
	}
	lc.mu.Lock()
	calls := lc.calls
	lc.mu.Unlock()
	if calls != 0 {
		t.Errorf("LongPoll calls = %d, want 0 (a cold-start-pending view must not be subscribed)", calls)
	}
}

// TestColdStartSameIDTwoSpellingsSingleDownload drives coldStartView over a catalog reporting
// ONE file id under two case spellings across two pages - the re-report form the
// catalogKeyCollisions same-id exemption was written for - on a case-SENSITIVE filesystem,
// where the two spellings share neither a path key nor a fold key. Before the id-dedupe
// the merge planned two OpDownloads of one node onto two paths, each
// markSynced evicting the other's baseline via the byid drop-moved-orphan and the next scan
// re-uploading the orphan as a duplicate server node. Now exactly one download is planned, at// the FIRST-reported spelling, writing exactly one baseline; the rename to the other spelling// is in the log above the cursor, so the incremental poll replays it.
func TestColdStartSameIDTwoSpellingsSingleDownload(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "V"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	// The walk reports the node at "A.txt", then again at "a.txt" (a case-only rename
	// mid-walk). The first report is planned; the rename is above the cursor, for the poll.
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("A.txt", "f1", "h1", 3),
		serverFile("a.txt", "f1", "h2", 4),
	})}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("enqueued %d ops, want 1 (one node must plan one download): %+v", len(ops), ops)
	}
	op := ops[0].Op
	if op.Type != store.OpDownload || op.Path != "A.txt" || op.FileID != "f1" || op.LocalHash != "h1" {
		t.Errorf("op = %+v, want a download of A.txt (first-reported spelling) carrying hash h1", op)
	}
	if b, err := st.GetBaseline(7, "A.txt"); err != nil {
		t.Fatalf("GetBaseline A.txt: %v", err)
	} else if b.State != store.StatePending || b.ServerHash != "h1" || b.FileID != "f1" {
		t.Errorf("A.txt baseline = %+v, want pending with server hash h1 and id f1", b)
	}
	// The later spelling must leave no trace: no baseline and no second op.
	if _, err := st.GetBaseline(7, "a.txt"); !isNotFound(err) {
		t.Errorf("a.txt must have no baseline, got err %v", err)
	}
}

// seedColdStartAgainstTree runs a cold start over a local same.txt holding v1, against
// whatever the server's tree is said to hold.
//
// It models "another client changed this node" the way a WALK meets it: not as an extra
// event arriving late in a log, but as a listing that already reports the outcome. The three
// scenarios below are the three outcomes - the node changed, the node is gone, the node moved
// - and what each must do to the local file is the same as it always was.
func seedColdStartAgainstTree(t *testing.T, tree map[string][]actions.Node, content map[string][]byte) (*Engine, *store.Store, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "same.txt"), coldStartClampV1, 0o644); err != nil {
		t.Fatalf("write same.txt: %v", err)
	}
	c := &catalogClient{tree: tree, content: content}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	return e, st, viewDir
}

// TestColdStartConcurrentDeleteKeepsLocalCopy covers a node another client deletes
// while the multi-page cold start is reading. The fold REPLAYS the log, so the
// deletion retracts the node from the reconstructed tree and the path becomes
// local-only - which the safe merge pushes back rather than removing (cold start
// never deletes, DESIGN §6).
//
// That is a deliberate trade: the alternative - skipping deletes and clamping the
// cursor so the incremental pass applies them - made every fresh pairing replay the
// view's ENTIRE deletion history, because pull_event(0) returns the raw log and the
// oldest deletion in it sets the clamp. Resurrecting a file deleted inside the pull
// window costs an extra copy on the server; replaying from the beginning of history
// cost the client its ability to converge at all. Neither loses local data.
func TestColdStartConcurrentDeleteKeepsLocalCopy(t *testing.T) {
	// The node is simply not in the listing. A walk cannot tell "deleted" from "never
	// existed", and it does not have to: cold start never deletes, so the path is local-only
	// and the safe merge pushes it back.
	_, st, viewDir := seedColdStartAgainstTree(t, map[string][]actions.Node{"": {}}, nil)

	if _, err := os.Lstat(filepath.Join(viewDir, "same.txt")); err != nil {
		t.Fatalf("same.txt must survive a cold start: %v", err)
	}
	b, err := st.GetBaseline(7, "same.txt")
	if err != nil {
		t.Fatalf("GetBaseline same.txt: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("same.txt baseline = %q, want pending (a queued push owns it)", b.State)
	}
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n == 0 {
		t.Error("no op queued for the local-only path")
	}
}

// TestColdStartConcurrentMoveFollowsTheNode is the move variant: the relocation is
// folded, so the node is reconstructed at its NEW path and the old one retracted.
func TestColdStartConcurrentMoveFollowsTheNode(t *testing.T) {
	// The relocated node is listed at its NEW path and nowhere else, which is all a walk
	// ever sees of a move.
	_, st, _ := seedColdStartAgainstTree(t, map[string][]actions.Node{"": {
		{Name: "moved.txt", FileID: "s1", Hash: md4.ContentHash(coldStartClampV1),
			Size: int64(len(coldStartClampV1)), MTime: 50},
	}}, nil)

	if _, err := st.GetBaseline(7, "moved.txt"); err != nil {
		t.Errorf("GetBaseline moved.txt: %v, want the relocated node to be planned", err)
	}
}

// TestColdStartRunsUnderAPerTurnTimeout pins the wrapper's method set against the walk's
// hard requirements.
//
// withWorker installs turnLimitedClient around every leased client whenever
// limits.op_turn_timeout_s is set, and inner is a NAMED FIELD, so nothing is promoted: a
// verb the wrapper does not declare is simply absent from its method set. The cold-start
// walk asserts BOTH treeLister and logHeadProbe and refuses to degrade - there is no head
// to guess - so a missing forwarder does not slow a cold start down, it stops it before the
// first list turn. And a view that never completes its cold start is excluded from the
// long-poll subscription and from localReconcile alike, so the whole view goes silent in
// both directions for as long as the knob is set.
//
// The engine tests otherwise run with turnTimeout == 0, where the raw client is passed
// through unwrapped, which is exactly why this needs its own scenario.
func TestColdStartRunsUnderAPerTurnTimeout(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &catalogClient{logHead: 42, tree: treeFromCatalog([]actions.Event{
		createEvent("a.txt", "f1", 1, false),
	})}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{OpTurnTimeout: 30})

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView under a per-turn timeout: %v", err)
	}
	v := listViewOf(t, st, 7).View
	if !v.InitialSyncDone {
		t.Error("InitialSyncDone = false, want true")
	}
	if v.LastSeenSyncID != 42 {
		t.Errorf("cursor = %d, want 42 (the head read through the wrapper)", v.LastSeenSyncID)
	}
}

// TestColdStartWalkSkipsADirectoryItsParentNoLongerReports and its sibling below are the two
// halves of the corroboration rule.
//
// A directory removed server-side after its parent was enumerated answers `list` with a path
// error, and on a view large enough to take minutes to walk that is a routine race. Skipping it
// is safe only because the PARENT is asked again and no longer names it - that is the positive
// evidence. The errno alone is not: it is the union of 28672/32769/40961, and 28672 conflates a
// path error with a TIMEOUT.
func TestColdStartWalkSkipsADirectoryItsParentNoLongerReports(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &catalogClient{
		logHead: 9,
		tree: map[string][]actions.Node{
			// The walk's first look at the root names "gone", so it descends.
			"": {
				{Name: "gone", FileID: "g1", IsDir: true, MTime: 50},
				{Name: "kept.txt", FileID: "k1", Hash: "h1", Size: 3, MTime: 50},
			},
		},
		// Listing it fails the way a removed directory does.
		failList: map[string]error{"gone": actions.ErrPathError},
		// The corroborating re-list no longer names it: it really was removed.
		reList: map[string][]actions.Node{
			"": {{Name: "kept.txt", FileID: "k1", Hash: "h1", Size: 3, MTime: 50}},
		},
	}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	// The rest of the tree is planned, and the view completes: a race must not cost a cold start.
	if _, err := st.GetBaseline(7, "kept.txt"); err != nil {
		t.Errorf("GetBaseline kept.txt: %v, want the surviving sibling planned", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = false, want true (a corroborated removal is not a failure)")
	}
}

// TestColdStartWalkRefusesToSkipWhenTheParentStillReportsTheDirectory is the half that matters
// for data: an errno the parent contradicts is a server that failed to answer, not a directory
// that went away. Skipping there would leave the subtree's files undownloaded while the view is
// marked synced - a silent, permanent hole, since the incremental poll reads FORWARD from the
// cursor and never goes back for what the walk failed to record.
func TestColdStartWalkRefusesToSkipWhenTheParentStillReportsTheDirectory(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &catalogClient{
		logHead: 9,
		tree: map[string][]actions.Node{
			"": {
				{Name: "slow", FileID: "s1", IsDir: true, MTime: 50},
				{Name: "kept.txt", FileID: "k1", Hash: "h1", Size: 3, MTime: 50},
			},
		},
		failList: map[string]error{"slow": actions.ErrPathError},
	}
	e := New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil)

	err := e.coldStartView(context.Background(), listViewOf(t, st, 7))
	if err == nil {
		t.Fatal("coldStartView succeeded; a directory the parent still reports must not be skipped")
	}
	if !strings.Contains(err.Error(), "not proof of absence") {
		t.Errorf("error = %v, want it to say the errno is not proof", err)
	}
	if listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = true; an unproven skip must leave the view pending, not synced")
	}
}

// TestColdStartCatalogCapAdmitsExactlyTheCap and its sibling pin the cap's BOUNDARY, which the
// walk changed: it is checked per node now, before the node is appended, because checking once
// per directory let a single wide directory overshoot by its whole breadth - the measured stand
// serves 10000 children in one listing, so a cap of 1000 would be passed tenfold on the very
// knob an operator sets to bound memory.
func TestColdStartCatalogCapAdmitsExactlyTheCap(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &catalogTransport{client: manyCreates(3)}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{ColdStartCatalogCap: 3})

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView with cap == node count: %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = false; a catalog exactly at the cap must be admitted")
	}
}

func TestColdStartCatalogCapRefusesOneOverTheCap(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e, _ := captureEngine(t, st, &catalogTransport{client: manyCreates(4)})
	e.SetLimits(config.Limits{ColdStartCatalogCap: 3})

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); !errors.Is(err, errColdStartCatalogCap) {
		t.Fatalf("coldStartView error = %v, want %v one node over the cap", err, errColdStartCatalogCap)
	}
}

// TestColdStartCatalogCapUnlimited pins the zero value: 0 means unlimited, and a walk must not
// treat it as a budget of none.
func TestColdStartCatalogCapUnlimited(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &catalogTransport{client: manyCreates(25)}, t.TempDir(), "sess", nil)
	e.SetLimits(config.Limits{ColdStartCatalogCap: 0})

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView with an unlimited cap: %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Error("InitialSyncDone = false, want true")
	}
}

// TestColdStartEnqueueSpansMultipleBatches drives the planned ops past coldStartEnqueueBatch,
// which nothing else does: every other scenario plans a handful, so a defect in the second
// chunk - a slicing error at the boundary, state that does not survive a commit, an adoption
// buffer that is flushed but not cleared - would ship without a failing test.
func TestColdStartEnqueueSpansMultipleBatches(t *testing.T) {
	const n = coldStartEnqueueBatch*2 + 7
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	evs := make([]actions.Event, 0, n)
	for i := 0; i < n; i++ {
		evs = append(evs, createEvent(fmt.Sprintf("f%05d.txt", i), fmt.Sprintf("id%05d", i), int64(i+1), false))
	}
	c := &catalogClient{logHead: 9, tree: treeFromCatalog(evs)}
	e := planOnly(New(st, &catalogTransport{client: c}, t.TempDir(), "sess", nil))

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	got, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if got != n {
		t.Fatalf("PendingLen = %d, want %d (every op across every chunk)", got, n)
	}
	// Spot-check the boundaries: the last op of the first chunk, the first of the second, and
	// the tail that does not fill a chunk.
	for _, i := range []int{0, coldStartEnqueueBatch - 1, coldStartEnqueueBatch, n - 1} {
		p := fmt.Sprintf("f%05d.txt", i)
		if _, err := st.GetBaseline(7, p); err != nil {
			t.Errorf("GetBaseline %q: %v, want a baseline for every planned op", p, err)
		}
	}
}

// TestColdStartAdoptionSpansMultipleBatches is the same boundary for the OTHER volume path. A
// re-pair over an intact tree adopts every file it finds and enqueues nothing, so it exercises
// the adoption buffer and none of the queue batching.
func TestColdStartAdoptionSpansMultipleBatches(t *testing.T) {
	const n = coldStartEnqueueBatch*2 + 5
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	evs := make([]actions.Event, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("f%05d.txt", i)
		body := []byte(fmt.Sprintf("content %d\n", i))
		if err := os.WriteFile(filepath.Join(viewDir, name), body, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		evs = append(evs, actions.Event{
			Action: "modify", Path: name, FileID: fmt.Sprintf("id%05d", i),
			Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: 50,
		})
	}
	c := &catalogClient{logHead: 9, tree: treeFromCatalog(evs)}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	// Nothing to transfer: every path matched on both sides.
	if n2, err := st.PendingLen(); err != nil || n2 != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0 (an adoption transfers nothing)", n2, err)
	}
	for _, i := range []int{0, coldStartEnqueueBatch - 1, coldStartEnqueueBatch, n - 1} {
		p := fmt.Sprintf("f%05d.txt", i)
		b, err := st.GetBaseline(7, p)
		if err != nil {
			t.Fatalf("GetBaseline %q: %v, want every adoption committed", p, err)
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline %q state = %q, want synced", p, b.State)
		}
	}
}
