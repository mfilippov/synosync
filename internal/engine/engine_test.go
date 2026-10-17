package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// fakeClient is a scripted syncClient: each method returns its preset result and
// records that it was called, so engine tests assert behaviour without a network.
type fakeClient struct {
	views    []actions.View
	viewsErr error
	calls    int
	// node and nodeErr script QueryNode - refreshViews issues it as the root-id fallback
	// (on the empty view root path) when query_user_info omits a view's root id.
	node      actions.Node
	nodeErr   error
	nodeCalls int
	nodePaths []string
	nodeViews []int64
	// broken makes Broken() report a poisoned connection, modelling the first mid-I/O
	// failure that returns a wrapped network error (not the ErrConnBroken sentinel) yet
	// still leaves the client byte-misaligned.
	broken bool
	// longPollViews records the view-id set LongPoll was last subscribed with, so a test
	// can assert an absent view is excluded from the subscription.
	longPollViews []int64
}

// Broken reports the modelled poisoned-connection state so refreshViews and the outbound
// recovery probes can be exercised against a worker whose turn left the socket misaligned.
func (f *fakeClient) Broken() bool { return f.broken }

func (f *fakeClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	f.calls++
	return f.views, f.viewsErr
}

func (f *fakeClient) QueryNode(_ context.Context, viewID int64, path string) (actions.Node, error) {
	f.nodeCalls++
	f.nodePaths = append(f.nodePaths, path)
	f.nodeViews = append(f.nodeViews, viewID)
	return f.node, f.nodeErr
}

func (f *fakeClient) LongPoll(_ context.Context, views []actions.Subscription, _ int) ([]int64, error) {
	f.longPollViews = f.longPollViews[:0]
	for _, v := range views {
		f.longPollViews = append(f.longPollViews, v.ViewID)
	}
	return nil, nil
}

func (f *fakeClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}

func (f *fakeClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// fakeTransport hands the same fakeClient to both the event path and worker leases
// and records how many workers were leased, so refreshViews can be exercised
// without a pool.
type fakeTransport struct {
	client     *fakeClient
	workerErr  error
	workers    int
	released   bool
	acquireErr error
	eventErr   error
}

func (t *fakeTransport) EventClient() (syncClient, error) { return t.client, t.eventErr }

func (t *fakeTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	if t.acquireErr != nil {
		return t.acquireErr
	}
	t.workers++
	if t.workerErr != nil {
		return t.workerErr
	}
	err := fn(t.client)
	t.released = true
	return err
}

func (t *fakeTransport) ReplaceEvent(context.Context) error { return nil }

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestNewDefaultsLogger(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", "sess", nil)
	if e.log == nil {
		t.Fatal("New left a nil logger")
	}
	if e.syncRoot != "/root" || e.selfSessionID != "sess" {
		t.Errorf("New stored syncRoot=%q selfSessionID=%q", e.syncRoot, e.selfSessionID)
	}
}

func TestRefreshViewsUpsertsMetadata(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{views: []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw"},
		{ViewID: 9, Name: "Docs", Privilege: "ro"},
	}}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if ft.workers != 1 {
		t.Errorf("leased %d workers, want 1", ft.workers)
	}
	if !ft.released {
		t.Error("worker lease was not released")
	}

	v7, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v7.Name != "Photos" || v7.Privilege != "rw" {
		t.Errorf("view 7 = %+v, want Photos/rw", v7)
	}
	v9, err := st.GetView(9)
	if err != nil {
		t.Fatalf("GetView(9): %v", err)
	}
	if v9.Name != "Docs" || v9.Privilege != "ro" {
		t.Errorf("view 9 = %+v, want Docs/ro", v9)
	}
}

// TestRefreshViewsNormalizesNameToNFC confirms a server view name in Unicode NFD
// (the macOS spelling) is folded to NFC at ingestion, like every other path the
// engine stores (Phase-9b invariant), so the on-disk view directory the
// scanner/executor derive from it matches the NFC-keyed baselines.
func TestRefreshViewsNormalizesNameToNFC(t *testing.T) {
	st := newTestStore(t)
	// "é" as a combining sequence (NFD: e + U+0301) - distinct bytes from the
	// precomposed NFC form (U+00E9) the engine must store.
	const nfd = "Cafe\u0301" // NFD: base e + combining acute
	const nfc = "Caf\u00e9"  // NFC: precomposed é
	ft := &fakeTransport{client: &fakeClient{views: []actions.View{
		{ViewID: 7, Name: nfd, Privilege: "rw"},
	}}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v.Name != nfc {
		t.Errorf("stored view name = %q (% x), want NFC %q (% x)", v.Name, v.Name, nfc, nfc)
	}
}

// TestRefreshViewsPreservesCursor confirms a re-enumeration overwrites metadata but
// keeps the per-view cursor an earlier sync had advanced. The stored name differs
// from the server's, so this also exercises the rename guard's nothing-materialised
// branch (no directory under either name, no baselines → adopt directly); the sync
// root is a real temp dir so the guard's directory probes run cleanly.
func TestRefreshViewsPreservesCursor(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "old", Privilege: "ro", LastSeenSyncID: 42}); err != nil {
		t.Fatalf("seed PutView: %v", err)
	}
	ft := &fakeTransport{client: &fakeClient{views: []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw"},
	}}}
	e := New(st, ft, t.TempDir(), "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v.Name != "Photos" || v.Privilege != "rw" {
		t.Errorf("metadata not refreshed: %+v", v)
	}
	if v.LastSeenSyncID != 42 {
		t.Errorf("cursor clobbered: LastSeenSyncID = %d, want 42", v.LastSeenSyncID)
	}
}

// TestRefreshViewsCapturesRootID confirms a root id carried directly in
// query_user_info is persisted into View.NodeID and no QueryNode fallback is issued.
func TestRefreshViewsCapturesRootID(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{views: []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "root7"},
	}}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if ft.client.nodeCalls != 0 {
		t.Errorf("QueryNode called %d times, want 0 when the root id is already present", ft.client.nodeCalls)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v.NodeID != "root7" {
		t.Errorf("view 7 NodeID = %q, want root7", v.NodeID)
	}
}

// TestRefreshViewsResolvesOmittedRootIDViaQueryNode confirms a view whose enumeration omits
// the root id IS resolved via a fallback QueryNode on the view ROOT PATH (the empty
// view-relative path, viewRootPath) - not the view name. Without this resolution every
// top-level create would wedge forever (resolveParentID → errParentUnknown), with no
// recovery since views are not periodically re-enumerated. The plan requires it.
func TestRefreshViewsResolvesOmittedRootIDViaQueryNode(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
		node:  actions.Node{FileID: "resolved7"},
	}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if ft.client.nodeCalls != 1 {
		t.Fatalf("QueryNode called %d times, want 1 root-path resolution", ft.client.nodeCalls)
	}
	if len(ft.client.nodePaths) != 1 || ft.client.nodePaths[0] != viewRootPath {
		t.Fatalf("QueryNode queried %v, want the empty view root path", ft.client.nodePaths)
	}
	// The root resolution must be scoped to the specific view: the empty root path
	// is identical for every view, so without view_id all views missing a root id would resolve
	// to the same node and misroute top-level creates.
	if len(ft.client.nodeViews) != 1 || ft.client.nodeViews[0] != 7 {
		t.Fatalf("QueryNode view_ids = %v, want [7] (root resolution scoped to its view)", ft.client.nodeViews)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v.NodeID != "resolved7" {
		t.Errorf("view 7 NodeID = %q, want resolved7 from the root-path QueryNode", v.NodeID)
	}
}

// TestRefreshViewsPersistsDespiteQueryNodeFailure confirms a QueryNode root-id enrichment
// failure does NOT abort refreshViews: the view is still persisted (with an empty NodeID), so a
// fresh store is never left empty - which would block inbound sync too - when the unverified
// root addressing (DESIGN §13) fails. pollOnce's viewsMissingRootID retry recovers the id later.
func TestRefreshViewsPersistsDespiteQueryNodeFailure(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{
		// query_user_info omits the root id, so refreshViews issues the QueryNode fallback - which
		// fails outright here.
		views:   []actions.View{{ViewID: 7, Name: "Photos"}},
		nodeErr: errors.New("query_node failed"),
	}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews must not abort on a QueryNode enrichment failure: %v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("view 7 not persisted despite the QueryNode failure: %v", err)
	}
	if v.Name != "Photos" {
		t.Errorf("view 7 Name = %q, want Photos", v.Name)
	}
	if v.NodeID != "" {
		t.Errorf("view 7 NodeID = %q, want empty (enrichment failed, deferred to retry)", v.NodeID)
	}
}

// TestRefreshViewsPropagatesTerminalQueryNodeError confirms the root-id enrichment is
// best-effort only for ORDINARY lookup failures: a terminal/connection-level QueryNode
// error (here ErrConnBroken) must propagate out of refreshViews rather than being
// swallowed, so the leased worker is re-dialed instead of returned to the pool
// byte-misaligned.
func TestRefreshViewsPropagatesTerminalQueryNodeError(t *testing.T) {
	st := newTestStore(t)
	ft := &fakeTransport{client: &fakeClient{
		views:   []actions.View{{ViewID: 7, Name: "Photos"}},
		nodeErr: actions.ErrConnBroken,
	}}
	e := New(st, ft, "/root", "sess", nil)

	err := e.refreshViews(context.Background())
	if err == nil {
		t.Fatal("refreshViews swallowed a broken-connection QueryNode error; want it propagated")
	}
	if !errors.Is(err, actions.ErrConnBroken) {
		t.Errorf("refreshViews error = %v, want it to wrap ErrConnBroken", err)
	}
}

// TestRefreshViewsPropagatesBrokenWorkerWrappedError confirms the broken-state check catches a
// poisoned worker even when the QueryNode error is NOT the ErrConnBroken sentinel. The first
// mid-I/O failure on a turn returns a wrapped network error (EOF/reset) while still setting the
// client broken; isTerminalRefreshError keys on the sentinel and so misses it, which would let
// the best-effort branch swallow the error and release a byte-misaligned socket to the pool.
// Checking Broken() directly propagates it so WithWorker re-dials the worker.
func TestRefreshViewsPropagatesBrokenWorkerWrappedError(t *testing.T) {
	st := newTestStore(t)
	wrapped := fmt.Errorf("actions: read query_node reply: %w", io.ErrUnexpectedEOF)
	ft := &fakeTransport{client: &fakeClient{
		views:   []actions.View{{ViewID: 7, Name: "Photos"}},
		nodeErr: wrapped, // a wrapped network error, NOT ErrConnBroken
		broken:  true,    // ...but the turn poisoned the connection
	}}
	e := New(st, ft, "/root", "sess", nil)

	err := e.refreshViews(context.Background())
	if err == nil {
		t.Fatal("refreshViews swallowed a broken-worker QueryNode error; want it propagated")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("refreshViews error = %v, want it to wrap the underlying I/O error", err)
	}
}

// TestRefreshViewsPreservesRootID confirms a re-enumeration that cannot supply a root
// id (empty NodeID in query_user_info) leaves a previously-captured one intact rather
// than zeroing it.
func TestRefreshViewsPreservesRootID(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "old", NodeID: "root7", LastSeenSyncID: 5}); err != nil {
		t.Fatalf("seed PutView: %v", err)
	}
	ft := &fakeTransport{client: &fakeClient{
		// query_user_info omits the root id (empty NodeID), so this enumeration cannot supply one.
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
	}}
	e := New(st, ft, "/root", "sess", nil)

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView(7): %v", err)
	}
	if v.NodeID != "root7" {
		t.Errorf("view 7 NodeID = %q, want the preserved root7", v.NodeID)
	}
	if v.LastSeenSyncID != 5 {
		t.Errorf("cursor clobbered: LastSeenSyncID = %d, want 5", v.LastSeenSyncID)
	}
}

// TestPollOnceMissingRootIDBacksOff confirms pollOnce does not re-run refreshViews on
// every poll cycle when a view's root id never resolves: the recovery attempt is bounded
// by a doubling cycle-skip backoff, so a server that never supplies a root id is re-queried
// far fewer times than the number of cycles.
func TestPollOnceMissingRootIDBacksOff(t *testing.T) {
	st := newTestStore(t)
	// A view that has finished cold start (so pollOnce reaches the long-poll path instead of
	// sleeping) but whose root id is still unknown.
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("seed PutView: %v", err)
	}
	fc := &fakeClient{
		// query_user_info keeps omitting the root id, so refreshViews never resolves it.
		views: []actions.View{{ViewID: 7, Name: "Photos"}},
	}
	e := New(st, &fakeTransport{client: fc}, "/root", "sess", nil)

	const cycles = 8
	for i := 0; i < cycles; i++ {
		if err := e.pollOnce(context.Background()); err != nil {
			t.Fatalf("pollOnce[%d]: %v", i, err)
		}
	}
	// Cycles 0,2,6 trigger refreshViews (skip backoff 1,2,4 between them); without the cap
	// every cycle would call it. Assert it ran fewer times than cycles and at least once.
	if fc.calls >= cycles {
		t.Errorf("QueryUserInfo calls = %d over %d cycles; want the backoff to skip cycles", fc.calls, cycles)
	}
	if fc.calls < 1 {
		t.Errorf("QueryUserInfo calls = %d; want at least one recovery attempt", fc.calls)
	}
	if fc.calls != 3 {
		t.Errorf("QueryUserInfo calls = %d; want 3 (cycles 0,2,6 on the doubling backoff)", fc.calls)
	}
}

// TestResolveParentID covers top-level (view root) and nested (parent baseline)
// resolution and the unknown-parent errors that leave an outbound op retryable.
func TestResolveParentID(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", "sess", nil)

	// View root unknown (no record at all) → errParentUnknown for a top-level entry.
	if _, err := e.resolveParentID(7, "a.txt"); !errors.Is(err, errParentUnknown) {
		t.Errorf("resolveParentID with no view = %v, want errParentUnknown", err)
	}

	// View present but root id not captured yet → still errParentUnknown.
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if _, err := e.resolveParentID(7, "a.txt"); !errors.Is(err, errParentUnknown) {
		t.Errorf("resolveParentID with empty root id = %v, want errParentUnknown", err)
	}

	// Root id captured → a top-level entry resolves to it.
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root7"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	got, err := e.resolveParentID(7, "a.txt")
	if err != nil {
		t.Fatalf("resolveParentID(top-level): %v", err)
	}
	if got != "root7" {
		t.Errorf("top-level parent = %q, want root7", got)
	}

	// Nested entry whose parent directory baseline carries a server id resolves to it.
	if err := st.PutBaseline(7, "docs", store.Baseline{FileID: "dir1", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	got, err = e.resolveParentID(7, "docs/a.txt")
	if err != nil {
		t.Fatalf("resolveParentID(nested): %v", err)
	}
	if got != "dir1" {
		t.Errorf("nested parent = %q, want dir1", got)
	}

	// Nested entry whose parent baseline is missing → errParentUnknown (retry later).
	if _, err := e.resolveParentID(7, "missing/a.txt"); !errors.Is(err, errParentUnknown) {
		t.Errorf("resolveParentID with missing parent = %v, want errParentUnknown", err)
	}

	// Nested entry whose parent baseline has no server id yet → errParentUnknown.
	if err := st.PutBaseline(7, "fresh", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := e.resolveParentID(7, "fresh/a.txt"); !errors.Is(err, errParentUnknown) {
		t.Errorf("resolveParentID with idless parent = %v, want errParentUnknown", err)
	}
}

func TestRefreshViewsPropagatesError(t *testing.T) {
	st := newTestStore(t)
	wantErr := errors.New("boom")
	ft := &fakeTransport{client: &fakeClient{viewsErr: wantErr}}
	e := New(st, ft, "/root", "sess", nil)

	err := e.refreshViews(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("refreshViews error = %v, want wrap of %v", err, wantErr)
	}
}

// TestCursorRoundTrip exercises the cursor helpers: a missing view reads as 0, and
// setCursor advances an existing view's cursor while leaving its metadata intact.
func TestCursorRoundTrip(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", "sess", nil)

	got, err := e.cursor(5)
	if err != nil {
		t.Fatalf("cursor(missing): %v", err)
	}
	if got != 0 {
		t.Errorf("cursor of missing view = %d, want 0", got)
	}

	if err := st.PutView(5, store.View{Name: "View5", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := e.setCursor(5, 123); err != nil {
		t.Fatalf("setCursor: %v", err)
	}
	got, err = e.cursor(5)
	if err != nil {
		t.Fatalf("cursor(5): %v", err)
	}
	if got != 123 {
		t.Errorf("cursor(5) = %d, want 123", got)
	}
	v, err := st.GetView(5)
	if err != nil {
		t.Fatalf("GetView(5): %v", err)
	}
	if v.Name != "View5" {
		t.Errorf("setCursor clobbered metadata: %+v", v)
	}
}

// TestSetCursorMissingView confirms advancing the cursor of a view that was never
// upserted is an error rather than a silent create.
func TestSetCursorMissingView(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, "/root", "sess", nil)
	if err := e.setCursor(404, 1); err == nil {
		t.Fatal("setCursor on a missing view succeeded, want error")
	}
}

// fakePool is a sessionPool that hands out nil sessions plus a release closure it
// records, so poolTransport's lease/release wiring is testable without a live,
// TLS-handshaking transport.Pool.
type fakePool struct {
	acquireErr    error
	acquired      int
	released      int
	replaced      int
	replaceErr    error
	eventReplaced int
	eventSess     *transport.Session
	paused        bool
	pauseReason   error
}

func (p *fakePool) Event() *transport.Session { return p.eventSess }

func (p *fakePool) Paused() bool { return p.paused }

func (p *fakePool) PauseReason() error {
	if !p.paused {
		return nil
	}
	return p.pauseReason
}

func (p *fakePool) Acquire(context.Context) (*transport.Session, func(), error) {
	if p.acquireErr != nil {
		return nil, nil, p.acquireErr
	}
	p.acquired++
	return nil, func() { p.released++ }, nil
}

func (p *fakePool) Replace(context.Context, *transport.Session) (*transport.Session, func(), error) {
	if p.replaceErr != nil {
		return nil, nil, p.replaceErr
	}
	p.replaced++
	return nil, func() { p.released++ }, nil
}

func (p *fakePool) ReplaceEvent(context.Context) (*transport.Session, error) {
	p.eventReplaced++
	return nil, nil
}

// stubClient is a no-op syncClient used to confirm poolTransport builds a client
// per connection and routes the call through.
type stubClient struct{ fakeClient }

// brokenStubClient is a stubClient that reports itself broken, so poolTransport's
// replace-on-broken path can be exercised without a live connection.
type brokenStubClient struct{ stubClient }

func (*brokenStubClient) Broken() bool { return true }

func TestPoolTransportEventClient(t *testing.T) {
	pool := &fakePool{eventSess: &transport.Session{}}
	want := &stubClient{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return want }
	got, err := pt.EventClient()
	if err != nil {
		t.Fatalf("EventClient() error = %v, want nil", err)
	}
	if got != want {
		t.Errorf("EventClient() = %v, want the built client", got)
	}
}

func TestPoolTransportEventClientNilSession(t *testing.T) {
	// A closed pool hands out a nil event session: EventClient must surface a terminal
	// error rather than building a client over nil and panicking.
	closed := &fakePool{eventSess: nil}
	pt := newPoolTransport(closed, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient {
		t.Fatal("newClient must not be called for a nil event session")
		return nil
	}
	if _, err := pt.EventClient(); !errors.Is(err, transport.ErrPoolClosed) {
		t.Errorf("EventClient() on closed pool error = %v, want ErrPoolClosed", err)
	}

	// A paused pool also hands out nil, but the caller should re-pair: surface the pause.
	// With no recorded reason the seam falls back to ErrPoolPaused (old behaviour).
	paused := &fakePool{eventSess: nil, paused: true}
	pt2 := newPoolTransport(paused, "tok", nil)
	if _, err := pt2.EventClient(); !errors.Is(err, transport.ErrPoolPaused) {
		t.Errorf("EventClient() on paused pool error = %v, want ErrPoolPaused", err)
	}
}

// TestPoolTransportEventClientPauseReason confirms the seam propagates the pool's
// own pause reason rather than hardcoding ErrPoolPaused: a client-rejected
// pause (20481) surfaces as ErrPoolClientRejected - an "update synosync" signal -
// while a vanished-session pause (8193) surfaces as ErrPoolPaused (re-login).
func TestPoolTransportEventClientPauseReason(t *testing.T) {
	rejected := &fakePool{eventSess: nil, paused: true, pauseReason: transport.ErrPoolClientRejected}
	pt := newPoolTransport(rejected, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient {
		t.Fatal("newClient must not be called for a nil event session")
		return nil
	}
	if _, err := pt.EventClient(); !errors.Is(err, transport.ErrPoolClientRejected) {
		t.Errorf("EventClient() on client-rejected pause error = %v, want ErrPoolClientRejected", err)
	}

	vanished := &fakePool{eventSess: nil, paused: true, pauseReason: transport.ErrPoolPaused}
	pt2 := newPoolTransport(vanished, "tok", nil)
	if _, err := pt2.EventClient(); !errors.Is(err, transport.ErrPoolPaused) ||
		errors.Is(err, transport.ErrPoolClientRejected) {
		t.Errorf("EventClient() on vanished-session pause error = %v, want ErrPoolPaused (not client-rejected)", err)
	}
}

func TestPoolTransportWithWorkerLeases(t *testing.T) {
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	built := &stubClient{}
	pt.newClient = func(*transport.Session) syncClient { return built }

	var gotClient syncClient
	err := pt.WithWorker(context.Background(), func(c syncClient) error {
		gotClient = c
		return nil
	})
	if err != nil {
		t.Fatalf("WithWorker: %v", err)
	}
	if gotClient != built {
		t.Error("WithWorker did not pass the built client to fn")
	}
	if pool.acquired != 1 || pool.released != 1 {
		t.Errorf("acquired=%d released=%d, want 1/1", pool.acquired, pool.released)
	}
}

// TestPoolTransportWithWorkerReleasesOnError confirms the lease is returned even
// when fn fails, so a failed turn never leaks a worker connection.
func TestPoolTransportWithWorkerReleasesOnError(t *testing.T) {
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &stubClient{} }

	wantErr := errors.New("turn failed")
	err := pt.WithWorker(context.Background(), func(syncClient) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithWorker error = %v, want %v", err, wantErr)
	}
	if pool.released != 1 {
		t.Errorf("released=%d, want 1 (lease must be returned on error)", pool.released)
	}
}

// TestPoolTransportWithWorkerAcquireError confirms an Acquire failure (pool closed
// or paused) is surfaced without invoking fn or leasing anything.
func TestPoolTransportWithWorkerAcquireError(t *testing.T) {
	wantErr := errors.New("pool closed")
	pool := &fakePool{acquireErr: wantErr}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &stubClient{} }

	called := false
	err := pt.WithWorker(context.Background(), func(syncClient) error {
		called = true
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithWorker error = %v, want %v", err, wantErr)
	}
	if called {
		t.Error("fn ran despite an Acquire error")
	}
	if pool.released != 0 {
		t.Errorf("released=%d, want 0", pool.released)
	}
}

// TestPoolTransportWithWorkerReplacesBrokenConnection confirms that when a turn fails
// AND leaves the connection broken, the dead session is re-dialed through Replace
// rather than returned to the pool - otherwise the next Acquire would reuse the dead
// socket and stall sync. The original lease's release must not also run (Replace owns
// it); the replacement's release does.
func TestPoolTransportWithWorkerReplacesBrokenConnection(t *testing.T) {
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &brokenStubClient{} }

	wantErr := errors.New("socket dropped mid-turn")
	err := pt.WithWorker(context.Background(), func(syncClient) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithWorker error = %v, want %v (the turn error survives a replace)", err, wantErr)
	}
	if pool.replaced != 1 {
		t.Errorf("replaced=%d, want 1 (a broken connection must be re-dialed)", pool.replaced)
	}
	if pool.released != 1 {
		t.Errorf("released=%d, want 1 (only the replacement is released, not the dead lease)", pool.released)
	}
}

// TestPoolTransportWithWorkerReplacesBrokenOnNilError pins the unconditional broken
// check: a callback that swallows a probe's mid-I/O failure and
// returns nil must NOT hand the byte-misaligned socket back to the pool as healthy -
// the next lease builds a FRESH client (the broken flag is per-Client) over the
// desynced stream and decodes the abandoned reply's tail as its own values, worst
// case reading a stray nested map as a clean empty List that
// confirmAbsentByParentList takes as positive proof of absence. The broken session
// is re-dialed through Replace even though fn reported success, and fn's nil result
// survives.
func TestPoolTransportWithWorkerReplacesBrokenOnNilError(t *testing.T) {
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &brokenStubClient{} }

	if err := pt.WithWorker(context.Background(), func(syncClient) error { return nil }); err != nil {
		t.Fatalf("WithWorker = %v, want nil (fn's clean result survives the replace)", err)
	}
	if pool.replaced != 1 {
		t.Errorf("replaced=%d, want 1 (a broken connection must be re-dialed even after a nil fn error)", pool.replaced)
	}
	if pool.released != 1 {
		t.Errorf("released=%d, want 1 (only the replacement is released, not the dead lease)", pool.released)
	}
}

// TestPoolTransportWithWorkerNilErrorReplaceErrorIsSurfaced confirms the nil-fn-error
// replace path surfaces a Replace failure (the pool paused on a vanished session,
// closed, or ctx cancelled) instead of the callback's nil, so the engine loop reacts
// to the terminal condition rather than reading the turn as clean.
func TestPoolTransportWithWorkerNilErrorReplaceErrorIsSurfaced(t *testing.T) {
	wantErr := errors.New("pool paused")
	pool := &fakePool{replaceErr: wantErr}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &brokenStubClient{} }

	if err := pt.WithWorker(context.Background(), func(syncClient) error { return nil }); !errors.Is(err, wantErr) {
		t.Fatalf("WithWorker = %v, want the Replace error %v", err, wantErr)
	}
	if pool.released != 0 {
		t.Errorf("released=%d, want 0 (Replace took over the dead lease and itself failed)", pool.released)
	}
}

// TestPoolTransportWithWorkerReplaceErrorIsSurfaced confirms a Replace failure (the
// pool paused on a vanished session, closed, or ctx cancelled) is returned in place of
// the turn error so the engine loop reacts to the terminal condition.
func TestPoolTransportWithWorkerReplaceErrorIsSurfaced(t *testing.T) {
	wantErr := errors.New("pool paused")
	pool := &fakePool{replaceErr: wantErr}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return &brokenStubClient{} }

	err := pt.WithWorker(context.Background(), func(syncClient) error { return errors.New("turn failed") })
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithWorker error = %v, want the Replace error %v", err, wantErr)
	}
	if pool.released != 0 {
		t.Errorf("released=%d, want 0 (Replace took over the dead lease and itself failed)", pool.released)
	}
}

// TestNewPoolTransportThreadsLogger confirms the exported constructor installs the
// engine's logger on the transport (so the production actions.NewClientWithLogger
// factory builds clients whose degradation warnings land in the daemon's rotating
// handler) and that a nil logger falls back to slog.Default().
func TestNewPoolTransportThreadsLogger(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pt, ok := NewPoolTransport(nil, "tok", log).(*poolTransport)
	if !ok {
		t.Fatal("NewPoolTransport did not return a *poolTransport")
	}
	if pt.log != log {
		t.Error("NewPoolTransport did not install the supplied logger")
	}
	pt2, _ := NewPoolTransport(nil, "tok", nil).(*poolTransport)
	if pt2.log != slog.Default() {
		t.Error("NewPoolTransport(nil logger) did not fall back to slog.Default()")
	}
}

// TestPoolTransportReplaceEvent confirms ReplaceEvent delegates to the pool so a dead
// event connection is re-dialed.
func TestPoolTransportReplaceEvent(t *testing.T) {
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	if err := pt.ReplaceEvent(context.Background()); err != nil {
		t.Fatalf("ReplaceEvent: %v", err)
	}
	if pool.eventReplaced != 1 {
		t.Errorf("eventReplaced=%d, want 1", pool.eventReplaced)
	}
}

// Ensure *actions.Client structurally satisfies syncClient - the production
// implementation the fakes stand in for.
var _ syncClient = (*actions.Client)(nil)

// Ensure *actions.Client also reports its broken state through brokenReporter, the
// signal poolTransport uses to decide between releasing and re-dialing a connection.
var _ brokenReporter = (*actions.Client)(nil)
