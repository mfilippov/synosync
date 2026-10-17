package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// seedStatusStore populates a store with two views, conflicts, and queued ops so
// buildStatus has non-trivial data to assemble.
func seedStatusStore(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.PutView(1, store.View{Name: "alpha", LastSeenSyncID: 42, InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 1: %v", err)
	}
	if err := st.PutView(2, store.View{Name: "beta", LastSeenSyncID: 7}); err != nil {
		t.Fatalf("PutView 2: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict a: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "b.txt", CopyPath: "b_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict b: %v", err)
	}
	// Two StateConflict baselines in view 1 (an outbound divergence with no keep-both
	// copy) plus a StateSynced one that must NOT be counted, and none in view 2.
	if err := st.PutBaseline(1, "c.txt", store.Baseline{FileID: "fc", FileType: "file", State: store.StateConflict}); err != nil {
		t.Fatalf("PutBaseline c: %v", err)
	}
	if err := st.PutBaseline(1, "d.txt", store.Baseline{FileID: "fd", FileType: "file", State: store.StateConflict}); err != nil {
		t.Fatalf("PutBaseline d: %v", err)
	}
	if err := st.PutBaseline(1, "e.txt", store.Baseline{FileID: "fe", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline e: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.Enqueue(store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 1, Path: "x"}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
}

// newStatusDaemon builds a Daemon wired to st with the given config, without
// running it; tests then call buildStatus or exercise the registered handler.
func newStatusDaemon(t *testing.T, st *store.Store, cfg config.Config) (*Daemon, net.Listener) {
	t.Helper()
	l, sockPath := listenTestSocket(t)
	d, err := New(Deps{
		Store:    st,
		Config:   cfg,
		SockPath: sockPath,
		LockPath: filepath.Join(t.TempDir(), "daemon.lock"),
		// New requires a Connector (or a run seam); it is never dialed in these tests.
		Connector: func(context.Context) (*transport.Session, error) { return nil, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d, l
}

func TestBuildStatusAssemblesReport(t *testing.T) {
	st := openTestStore(t)
	seedStatusStore(t, st)

	cfg := config.Config{Server: config.Server{Host: "syno.example", Port: 6690}}
	d, _ := newStatusDaemon(t, st, cfg)
	d.setPool(fakePool{paused: true, workers: 4})

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}

	if rep.Server != "syno.example:6690" {
		t.Errorf("Server: got %q, want syno.example:6690", rep.Server)
	}
	if rep.PendingOps != 3 {
		t.Errorf("PendingOps: got %d, want 3", rep.PendingOps)
	}
	if !rep.Connection.Paused || rep.Connection.NumWorkers != 4 || rep.Connection.Connecting {
		t.Errorf("Connection: got %+v, want {Connecting:false Paused:true NumWorkers:4}", rep.Connection)
	}
	if len(rep.Views) != 2 {
		t.Fatalf("Views: got %d, want 2", len(rep.Views))
	}
	v1 := rep.Views[0]
	if v1.ID != 1 || v1.Name != "alpha" || v1.LastSyncID != 42 || !v1.InitialSyncDone || v1.Conflicts != 2 || v1.Unresolved != 2 {
		t.Errorf("view 1: got %+v", v1)
	}
	v2 := rep.Views[1]
	if v2.ID != 2 || v2.Name != "beta" || v2.LastSyncID != 7 || v2.InitialSyncDone || v2.Conflicts != 0 || v2.Unresolved != 0 {
		t.Errorf("view 2: got %+v", v2)
	}
}

// TestBuildStatusSurfacesDeadLettersAndDiskFull confirms the durable dead-letter
// count and the engine's disk-full flag reach the StatusReport.
func TestBuildStatusSurfacesDeadLettersAndDiskFull(t *testing.T) {
	st := openTestStore(t)
	// The bare RecordDeadLetter is reserved for the one drop class that never had a queued op
	// (a fold-key collision); seed the bucket through the op-shaped entry point instead -
	// acking an absent queue seq is a no-op.
	if _, err := st.RecordDeadLetterAndAck(store.DeadLetter{ViewID: 1, Path: "x", Reason: "r"}, 0, nil, nil); err != nil {
		t.Fatalf("RecordDeadLetterAndAck: %v", err)
	}
	if _, err := st.RecordDeadLetterAndAck(store.DeadLetter{ViewID: 1, Path: "y", Reason: "r"}, 0, nil, nil); err != nil {
		t.Fatalf("RecordDeadLetterAndAck: %v", err)
	}

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	eng := &fakeEngine{}
	eng.diskFull.Store(true)
	d.setEngine(eng)

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.DeadLettered != 2 {
		t.Errorf("DeadLettered: got %d, want 2", rep.DeadLettered)
	}
	if !rep.DiskFull {
		t.Errorf("DiskFull: got false, want true")
	}
}

// TestBuildStatusSurfacesStuckOps confirms the engine's stuck-op count reaches the
// StatusReport once the engine is up, and reads zero before it (the
// counter has no meaning during bring-up backoff).
func TestBuildStatusSurfacesStuckOps(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (no engine): %v", err)
	}
	if rep.Stuck != 0 {
		t.Errorf("Stuck before engine up: got %d, want 0", rep.Stuck)
	}

	eng := &fakeEngine{}
	eng.stuck.Store(4)
	d.setEngine(eng)

	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Stuck != 4 {
		t.Errorf("Stuck: got %d, want 4", rep.Stuck)
	}
}

func TestBuildStatusNoPool(t *testing.T) {
	st := openTestStore(t)
	cfg := config.Config{
		Server:   config.Server{Host: "h", Port: 6690},
		SyncRoot: "/tmp/root",
	}
	d, _ := newStatusDaemon(t, st, cfg)
	// numWorkers defaults to defaultNumWorkers when Deps leaves it unset.
	d.numWorkers = 3

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Connection.Paused {
		t.Errorf("Connection.Paused: got true, want false with no pool")
	}
	// With no pool the daemon is still dialing (bring-up backoff): the report
	// must say so rather than let the CLI render a false "connected".
	if !rep.Connection.Connecting {
		t.Errorf("Connection.Connecting: got false, want true with no pool")
	}
	if rep.Connection.NumWorkers != 3 {
		t.Errorf("Connection.NumWorkers: got %d, want 3 (configured)", rep.Connection.NumWorkers)
	}
	if len(rep.Views) != 0 {
		t.Errorf("Views: got %d, want 0", len(rep.Views))
	}
}

// TestBuildStatusConnectingClearsOncePoolUp confirms the Connecting state is the
// pool-presence transition: true through the bring-up window, false as soon as
// the run seam records the live pool.
func TestBuildStatusConnectingClearsOncePoolUp(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (no pool): %v", err)
	}
	if !rep.Connection.Connecting {
		t.Fatalf("Connection.Connecting before setPool: got false, want true")
	}

	d.setPool(fakePool{workers: 2})
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (pool up): %v", err)
	}
	if rep.Connection.Connecting {
		t.Errorf("Connection.Connecting after setPool: got true, want false")
	}
	if rep.Connection.NumWorkers != 2 {
		t.Errorf("Connection.NumWorkers: got %d, want 2 (live pool)", rep.Connection.NumWorkers)
	}
}

// TestBuildStatusSurfacesReconnecting confirms a mid-run re-dial (a dropped
// connection being re-established under backoff after a successful bring-up)
// reaches the report as Connection.Reconnecting - before
// the fix the report read pool-up/not-paused and `status` claimed "connected"
// for the whole downtime - and that a healthy pool reports it false.
func TestBuildStatusSurfacesReconnecting(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	d.setPool(fakePool{reconnecting: true, workers: 3})
	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (reconnecting): %v", err)
	}
	if !rep.Connection.Reconnecting {
		t.Error("Connection.Reconnecting: got false, want true while the pool re-dials")
	}
	if rep.Connection.Connecting || rep.Connection.Paused {
		t.Errorf("Connection: got %+v, want only Reconnecting set", rep.Connection)
	}

	d.setPool(fakePool{workers: 3})
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (healthy): %v", err)
	}
	if rep.Connection.Reconnecting {
		t.Error("Connection.Reconnecting: got true for a healthy pool, want false")
	}
}

// TestCountUnresolvedTTLCache confirms the per-view unresolved count is served
// from the TTL cache between scans - a repeated `status` inside the TTL does not
// re-decode the view's baselines - and refreshes once the TTL expires (the
// scan is O(view baselines) and must not run on every status call).
func TestCountUnresolvedTTLCache(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if err := st.PutBaseline(1, p, store.Baseline{FileID: "f" + p, FileType: "file", State: store.StateConflict}); err != nil {
			t.Fatalf("PutBaseline %s: %v", p, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	base := time.Unix(1_750_000_000, 0)
	now := base
	d.now = func() time.Time { return now }

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Views[0].Unresolved != 2 {
		t.Fatalf("Unresolved: got %d, want 2", rep.Views[0].Unresolved)
	}

	// A third StateConflict baseline appears; inside the TTL the cached count is
	// served, so the report intentionally lags.
	if err := st.PutBaseline(1, "c.txt", store.Baseline{FileID: "fc", FileType: "file", State: store.StateConflict}); err != nil {
		t.Fatalf("PutBaseline c: %v", err)
	}
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (cached): %v", err)
	}
	if rep.Views[0].Unresolved != 2 {
		t.Errorf("Unresolved inside TTL: got %d, want 2 (cached)", rep.Views[0].Unresolved)
	}

	// Past the TTL the count is recomputed and picks up the new divergence.
	now = base.Add(viewCountsCacheTTL + time.Second)
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus (expired): %v", err)
	}
	if rep.Views[0].Unresolved != 3 {
		t.Errorf("Unresolved after TTL: got %d, want 3 (fresh)", rep.Views[0].Unresolved)
	}
}

// TestCountUnresolvedScanStampedAtCompletion pins the stamp-at-completion fix:
// the cache record is stamped with the scan's COMPLETION time, not its start, so
// a scan that itself takes longer than viewCountsCacheTTL still yields a fresh
// entry and the next `status` is served from cache. Start-stamped, such an entry
// was born already expired, so every call re-ran the full O(view-baselines) scan
// and pushed against the 10 s IPC deadline - on exactly the trees the cache
// exists for.
func TestCountUnresolvedScanStampedAtCompletion(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for _, p := range []string{"a.txt", "b.txt"} {
		if err := st.PutBaseline(1, p, store.Baseline{FileID: "f" + p, FileType: "file", State: store.StateConflict}); err != nil {
			t.Fatalf("PutBaseline %s: %v", p, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	base := time.Unix(1_750_000_000, 0)
	late := base.Add(viewCountsCacheTTL + 10*time.Second)
	first := true
	// The first clock reading is the pre-scan cache check; every later reading -
	// including the completion stamp - sees the clock TTL+10s further on, i.e.
	// the scan itself outlived the TTL.
	d.now = func() time.Time {
		if first {
			first = false
			return base
		}
		return late
	}

	a := d.viewCountsFor(context.Background(), 1)
	if a.err != nil {
		t.Fatalf("viewCountsFor: %v", a.err)
	}
	if a.counts.unresolved != 2 {
		t.Fatalf("count: got %d, want 2", a.counts.unresolved)
	}

	// A third divergence appears; a cache hit intentionally lags it by up to the TTL.
	if err := st.PutBaseline(1, "c.txt", store.Baseline{FileID: "fc", FileType: "file", State: store.StateConflict}); err != nil {
		t.Fatalf("PutBaseline c: %v", err)
	}

	a = d.viewCountsFor(context.Background(), 1)
	if a.err != nil {
		t.Fatalf("viewCountsFor (second): %v", a.err)
	}
	if a.counts.unresolved != 2 {
		t.Errorf("count right after a TTL-long scan: got %d, want 2 (served from the completion-stamped cache, not a rescan)", a.counts.unresolved)
	}
}

// corruptBaselineRecord overwrites the files-bucket value for view_id\0path with
// undecodable bytes via a raw bbolt handle at path (the store's own db is
// unexported), simulating latent record corruption. The store must be closed.
func corruptBaselineRecord(t *testing.T, path string, viewID uint64, relPath string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	key := make([]byte, 8, 9+len(relPath))
	binary.BigEndian.PutUint64(key, viewID)
	key = append(key, 0x00)
	key = append(key, relPath...)
	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("files")).Put(key, []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt baseline record: %v", err)
	}
}

// TestCountUnresolvedSingleFlight confirms concurrent cache misses for one view
// share a single scan: the first caller scans while the
// rest block on the per-view flight lock and serve its freshly stamped result.
// Without the single-flight, a polling client could stack up to
// maxInFlightConns duplicate O(view-baselines) scans competing with the sync
// data plane. The scan count is observed through the skip-Warn a corrupt
// baseline record triggers exactly once per scan.
func TestCountUnresolvedSingleFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Enough records that a scan is not instantaneous, widening the window the
	// concurrent misses overlap in.
	const baselines = 400
	for i := 0; i < baselines; i++ {
		p := fmt.Sprintf("f%04d.txt", i)
		if err := st.PutBaseline(1, p, store.Baseline{FileID: "id" + p, FileType: "file", State: store.StateConflict}); err != nil {
			t.Fatalf("PutBaseline %s: %v", p, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	corruptBaselineRecord(t, path, 1, "f0000.txt")

	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	var logBuf bytes.Buffer
	d, err := New(Deps{
		Store:     st2,
		Config:    config.Config{Server: config.Server{Host: "h", Port: 6690}},
		SockPath:  tempSockPath(t),
		LockPath:  filepath.Join(t.TempDir(), "daemon.lock"),
		Connector: func(context.Context) (*transport.Session, error) { return nil, nil },
		Logger:    slog.New(slog.NewTextHandler(&logBuf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	counts := make([]int, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			a := d.viewCountsFor(context.Background(), 1)
			counts[i], errs[i] = a.counts.unresolved, a.err
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("viewCountsFor[%d]: %v", i, errs[i])
		}
		if counts[i] != baselines-1 { // the corrupt record is skipped
			t.Errorf("count[%d]: got %d, want %d", i, counts[i], baselines-1)
		}
	}
	if warns := bytes.Count(logBuf.Bytes(), []byte("no longer decode")); warns != 1 {
		t.Errorf("scan ran %d times for %d concurrent misses, want exactly 1 (single-flight)", warns, callers)
	}
}

func TestBuildStatusStoreError(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	// Closing the store makes every read fail; buildStatus must surface the error
	// rather than return a partial/empty report.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := d.buildStatus(context.Background()); err == nil {
		t.Fatal("buildStatus on closed store: got nil error, want failure")
	}
}

// corruptConflictRecord overwrites the conflict value for view_id\0canonicalPath
// with undecodable bytes via a raw bbolt handle at path (the store's own db is
// unexported), simulating latent record corruption. The store must be closed.
func corruptConflictRecord(t *testing.T, path string, viewID uint64, canonicalPath string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	key := make([]byte, 8, 9+len(canonicalPath))
	binary.BigEndian.PutUint64(key, viewID)
	key = append(key, 0x00)
	key = append(key, canonicalPath...)
	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("conflicts")).Put(key, []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt conflict record: %v", err)
	}
}

// TestBuildStatusAndConflictsToleratesCorruptConflict confirms one bit-rotted
// conflicts-bucket record no longer breaks the two verbs an operator would reach
// for to diagnose it: both buildStatus and buildConflicts still assemble their
// report, surfacing the healthy conflict around the corrupt one. Before
// the ListConflicts skip-and-Warn fix both calls errored forever on one record.
func TestBuildStatusAndConflictsToleratesCorruptConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(1, store.View{Name: "alpha", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// "bad.txt" sorts before "good.txt", so the corrupt record is the first the
	// cursor hits - the case that used to abort the scan immediately.
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "bad.txt", CopyPath: "bad_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict bad: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "good.txt", CopyPath: "good_Conflict.txt"}); err != nil {
		t.Fatalf("PutConflict good: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptConflictRecord(t, path, 1, "bad.txt")

	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	d, _ := newStatusDaemon(t, st2, config.Config{Server: config.Server{Host: "h", Port: 6690}})

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus over corrupt conflict: %v", err)
	}
	if len(rep.Views) != 1 || rep.Views[0].Conflicts != 1 {
		t.Fatalf("buildStatus: got %+v, want one view with one healthy conflict", rep.Views)
	}

	cr, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts over corrupt conflict: %v", err)
	}
	if len(cr.Conflicts) != 1 || cr.Conflicts[0].CanonicalPath != "good.txt" {
		t.Fatalf("buildConflicts: got %+v, want just good.txt", cr.Conflicts)
	}
}

func TestStatusHandlerRoundTrip(t *testing.T) {
	st := openTestStore(t)
	seedStatusStore(t, st)

	cfg := config.Config{Server: config.Server{Host: "syno.example", Port: 6690}}
	d, l := newStatusDaemon(t, st, cfg)
	d.setPool(fakePool{workers: 2})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = d.server.Serve(ctx, l)
	}()

	c, err := ipc.Dial(d.sockPath)
	if err != nil {
		t.Fatalf("ipc.Dial: %v", err)
	}
	defer c.Close()

	var got ipc.StatusReport
	if err := c.Call("status", nil, &got); err != nil {
		t.Fatalf("Call status: %v", err)
	}
	if got.Server != "syno.example:6690" {
		t.Errorf("Server: got %q", got.Server)
	}
	if got.PendingOps != 3 {
		t.Errorf("PendingOps: got %d, want 3", got.PendingOps)
	}
	if got.Connection.NumWorkers != 2 {
		t.Errorf("NumWorkers: got %d, want 2", got.Connection.NumWorkers)
	}
	if len(got.Views) != 2 || got.Views[0].Conflicts != 2 {
		t.Errorf("Views: got %+v", got.Views)
	}

	cancel()
	<-served
}

// TestBuildConfigReturnsStoredBlob confirms the read-only `config` IPC method
// serves the settings blob live from the state store the
// daemon holds open, end-to-end through the registered handler.
func TestBuildConfigReturnsStoredBlob(t *testing.T) {
	st := openTestStore(t)
	want := config.Default()
	want.SyncRoot = "/srv/sync"
	want.Server = config.Server{Host: "syno.example", Port: 6690, User: "alice"}
	want.LogLevel = "debug"
	if err := st.PutConfig(want); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}

	d, _ := newStatusDaemon(t, st, want)
	rep, err := d.buildConfig()
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if rep.Config.SyncRoot != "/srv/sync" || rep.Config.Server.Host != "syno.example" ||
		rep.Config.Server.User != "alice" || rep.Config.LogLevel != "debug" {
		t.Errorf("buildConfig: got %+v", rep.Config)
	}

	// The handler must be registered and return the same report.
	res, err := d.handleConfig(context.Background(), nil)
	if err != nil {
		t.Fatalf("handleConfig: %v", err)
	}
	got, ok := res.(ipc.ConfigReport)
	if !ok {
		t.Fatalf("handleConfig: got %T, want ipc.ConfigReport", res)
	}
	if got.Config.Server.Host != "syno.example" {
		t.Errorf("handleConfig: got %+v", got.Config)
	}
}

// TestBuildConfigUnconfiguredDefaults confirms a never-written settings blob (an
// unpaired install) falls back to defaults rather than erroring, mirroring the
// CLI's loadConfigFromStore.
func TestBuildConfigUnconfiguredDefaults(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	rep, err := d.buildConfig()
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	def := config.Default()
	if rep.Config.LogLevel != def.LogLevel || rep.Config.Limits.Workers != def.Limits.Workers {
		t.Errorf("buildConfig: got %+v, want defaults", rep.Config)
	}
}

// TestPauseReasonToken pins the classification `status` words for the user. The three
// terminal pause causes have three different remedies and the CLI printed one hint for
// all of them, so a changed server certificate was reported as a login problem and a
// rejected client version sent the operator to re-pair, which cannot fix it.
//
// Every case wraps the sentinel, as the pool's pause error does, so the test exercises
// the errors.Is chain rather than sentinel identity.
func TestPauseReasonToken(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"not paused", nil, ""},
		{"vanished session", fmt.Errorf("pool paused: %w", auth.ErrSessionVanished), ipc.PauseSessionExpired},
		{"pin mismatch", fmt.Errorf("pool paused: %w", transport.ErrPinMismatch), ipc.PausePinMismatch},
		{"client rejected", fmt.Errorf("pool paused: %w", auth.ErrClientVersion), ipc.PauseClientRejected},
		{"client rejected sentinel", transport.ErrPoolClientRejected, ipc.PauseClientRejected},
		{"generic pause", transport.ErrPoolPaused, ""},
		{"unknown", errors.New("something else"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pauseReasonToken(tc.err); got != tc.want {
				t.Fatalf("pauseReasonToken(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestBuildStatusCarriesPauseReason confirms the token actually reaches the report: the
// classifier is useless if the status builder drops it.
func TestBuildStatusCarriesPauseReason(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{Server: config.Server{Host: "h", Port: 6690}})
	d.setPool(fakePool{paused: true, workers: 2, pauseReason: fmt.Errorf("paused: %w", auth.ErrClientVersion)})

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if !rep.Connection.Paused {
		t.Fatal("report does not show the pool as paused")
	}
	if rep.Connection.PauseReason != ipc.PauseClientRejected {
		t.Fatalf("PauseReason = %q, want %q", rep.Connection.PauseReason, ipc.PauseClientRejected)
	}
}
