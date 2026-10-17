package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/engine"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
)

// fakeEngine is an engineControl stand-in that records the verbs it received.
type fakeEngine struct {
	// stuckOps and stuckErr script the detail listing behind the Stuck count.
	stuckOps     []engine.StuckOp
	stuckErr     error
	pauseCount   atomic.Int32
	resumeCount  atomic.Int32
	triggerCount atomic.Int32
	paused       atomic.Bool
	diskFull     atomic.Bool
	stuck        atomic.Int32
}

// blockingPauseEngine makes the pause application window deterministic so the
// pause/resume serialization test does not depend on scheduler timing.
type blockingPauseEngine struct {
	fakeEngine
	pauseStarted chan struct{}
	releasePause chan struct{}
	startOnce    sync.Once
}

func (e *blockingPauseEngine) StuckDetails(int) ([]engine.StuckOp, error) { return nil, nil }

func (e *blockingPauseEngine) Pause() {
	e.startOnce.Do(func() { close(e.pauseStarted) })
	<-e.releasePause
	e.fakeEngine.Pause()
}

func (e *fakeEngine) Pause()  { e.pauseCount.Add(1); e.paused.Store(true) }
func (e *fakeEngine) Resume() { e.resumeCount.Add(1); e.paused.Store(false) }
func (e *fakeEngine) Paused() bool {
	return e.paused.Load()
}
func (e *fakeEngine) TriggerSync()   { e.triggerCount.Add(1) }
func (e *fakeEngine) DiskFull() bool { return e.diskFull.Load() }
func (e *fakeEngine) StuckOps() int  { return int(e.stuck.Load()) }

func (e *fakeEngine) StuckDetails(int) ([]engine.StuckOp, error) { return e.stuckOps, e.stuckErr }

func TestControlHandlersDispatchToEngine(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	eng := &fakeEngine{}
	d.setEngine(eng)

	if _, err := d.handlePause(context.Background(), nil); err != nil {
		t.Fatalf("handlePause: %v", err)
	}
	if eng.pauseCount.Load() != 1 {
		t.Errorf("pause: got %d calls, want 1", eng.pauseCount.Load())
	}

	if _, err := d.handleResume(context.Background(), nil); err != nil {
		t.Fatalf("handleResume: %v", err)
	}
	if eng.resumeCount.Load() != 1 {
		t.Errorf("resume: got %d calls, want 1", eng.resumeCount.Load())
	}

	if _, err := d.handleSync(context.Background(), nil); err != nil {
		t.Fatalf("handleSync: %v", err)
	}
	if eng.triggerCount.Load() != 1 {
		t.Errorf("sync: got %d calls, want 1", eng.triggerCount.Load())
	}
}

// TestConcurrentPauseResumeStayConsistent reproduces the handler-to-handler
// race: an older pause is held while a newer resume arrives. The newer command
// must wait for the serialized state transition and win everywhere - desired
// memory state, persisted state and the live engine.
func TestConcurrentPauseResumeStayConsistent(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	eng := &blockingPauseEngine{
		pauseStarted: make(chan struct{}),
		releasePause: make(chan struct{}),
	}
	d.setEngine(eng)

	pauseDone := make(chan struct{})
	go func() {
		_, _ = d.handlePause(context.Background(), nil)
		close(pauseDone)
	}()
	select {
	case <-eng.pauseStarted:
	case <-time.After(time.Second):
		t.Fatal("pause did not reach engine")
	}

	resumeDone := make(chan struct{})
	go func() {
		_, _ = d.handleResume(context.Background(), nil)
		close(resumeDone)
	}()
	select {
	case <-resumeDone:
		t.Fatal("resume overtook an in-flight pause transition")
	case <-time.After(50 * time.Millisecond):
	}

	close(eng.releasePause)
	select {
	case <-pauseDone:
	case <-time.After(time.Second):
		t.Fatal("pause did not finish")
	}
	select {
	case <-resumeDone:
	case <-time.After(time.Second):
		t.Fatal("resume did not finish")
	}

	if eng.Paused() {
		t.Fatal("engine is paused, want newer resume to win")
	}
	d.mu.Lock()
	wantPaused := d.wantPaused
	d.mu.Unlock()
	if wantPaused {
		t.Fatal("wantPaused is true, want newer resume to win")
	}
	paused, err := st.GetPaused()
	if err != nil {
		t.Fatalf("GetPaused: %v", err)
	}
	if paused {
		t.Fatal("persisted pause state is true, want newer resume to win")
	}
}

func TestControlHandlersNoEngineNoOp(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	// With no engine set yet, the control verbs must still ack without panicking.
	for name, h := range map[string]func(j []byte) (any, error){
		"pause":  func(j []byte) (any, error) { return d.handlePause(context.Background(), j) },
		"resume": func(j []byte) (any, error) { return d.handleResume(context.Background(), j) },
		"sync":   func(j []byte) (any, error) { return d.handleSync(context.Background(), j) },
	} {
		res, err := h(nil)
		if err != nil {
			t.Fatalf("%s with no engine: %v", name, err)
		}
		ack, ok := res.(ipc.ControlAck)
		if !ok || !ack.OK {
			t.Errorf("%s: got %#v, want ControlAck{OK:true}", name, res)
		}
	}
}

// TestPauseBeforeEngineAppliedOnSetEngine reproduces a lost-pause
// defect: a pause issued while the engine is still in bring-up backoff must not
// be lost - the desired state is recorded, reported by status, and applied to
// the engine the moment setEngine runs.
func TestPauseBeforeEngineAppliedOnSetEngine(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	res, err := d.handlePause(context.Background(), nil)
	if err != nil {
		t.Fatalf("handlePause with no engine: %v", err)
	}
	if ack, ok := res.(ipc.ControlAck); !ok || !ack.OK {
		t.Fatalf("handlePause: got %#v, want ControlAck{OK:true}", res)
	}

	// Status reflects the desired state even before the engine is up.
	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if !rep.Paused {
		t.Error("Paused before engine up: got false, want true after pause")
	}

	eng := &fakeEngine{}
	d.setEngine(eng)
	if eng.pauseCount.Load() == 0 {
		t.Error("setEngine after pause: engine never received Pause()")
	}
	if !eng.Paused() {
		t.Error("engine built after pause: got unpaused, want paused")
	}
}

// TestResumeBeforeEngineLeavesEngineUnpaused confirms the symmetric path: a
// resume issued before the engine is up clears the desired state, so the engine
// starts unpaused even after an earlier pause.
func TestResumeBeforeEngineLeavesEngineUnpaused(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	if _, err := d.handlePause(context.Background(), nil); err != nil {
		t.Fatalf("handlePause: %v", err)
	}
	if _, err := d.handleResume(context.Background(), nil); err != nil {
		t.Fatalf("handleResume: %v", err)
	}

	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Paused {
		t.Error("Paused after pause+resume before engine: got true, want false")
	}

	eng := &fakeEngine{}
	d.setEngine(eng)
	if eng.pauseCount.Load() != 0 {
		t.Errorf("setEngine after resume: engine got %d Pause() calls, want 0", eng.pauseCount.Load())
	}
	if eng.Paused() {
		t.Error("engine built after resume: got paused, want unpaused")
	}
}

// TestPausePersistsAcrossRestart confirms the persisted half of that fix: a pause
// recorded by one daemon is seeded from the store into a freshly constructed one
// ("paused until explicit resume" survives a daemon restart), and an explicit
// resume clears the persisted state for the next restart too.
func TestPausePersistsAcrossRestart(t *testing.T) {
	st := openTestStore(t)
	d1, _ := newStatusDaemon(t, st, config.Config{})
	if _, err := d1.handlePause(context.Background(), nil); err != nil {
		t.Fatalf("handlePause: %v", err)
	}

	// "Restart": a new daemon over the same store.
	d2, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d2.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus after restart: %v", err)
	}
	if !rep.Paused {
		t.Error("Paused after restart: got false, want true (persisted pause)")
	}
	eng := &fakeEngine{}
	d2.setEngine(eng)
	if !eng.Paused() {
		t.Error("engine after restart: got unpaused, want paused (persisted pause)")
	}

	if _, err := d2.handleResume(context.Background(), nil); err != nil {
		t.Fatalf("handleResume: %v", err)
	}
	d3, _ := newStatusDaemon(t, st, config.Config{})
	rep, err = d3.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus after second restart: %v", err)
	}
	if rep.Paused {
		t.Error("Paused after resume+restart: got true, want false")
	}
}

func TestBuildStatusReflectsEnginePaused(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})

	// No engine yet: not paused.
	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if rep.Paused {
		t.Errorf("Paused: got true with no engine, want false")
	}

	eng := &fakeEngine{}
	eng.paused.Store(true)
	d.setEngine(eng)
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus: %v", err)
	}
	if !rep.Paused {
		t.Errorf("Paused: got false, want true when engine paused")
	}
}

func TestBuildConflicts(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView 1: %v", err)
	}
	if err := st.PutView(2, store.View{Name: "beta"}); err != nil {
		t.Fatalf("PutView 2: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt", DetectedAt: 100}); err != nil {
		t.Fatalf("PutConflict a: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "b.txt", CopyPath: "b_Conflict.txt", DetectedAt: 200}); err != nil {
		t.Fatalf("PutConflict b: %v", err)
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if len(rep.Conflicts) != 2 {
		t.Fatalf("Conflicts: got %d, want 2", len(rep.Conflicts))
	}
	c0 := rep.Conflicts[0]
	if c0.ViewID != 1 || c0.ViewName != "alpha" || c0.CanonicalPath != "b.txt" ||
		c0.CopyPath != "b_Conflict.txt" || c0.DetectedAt != 200 {
		t.Errorf("conflict 0: got %+v", c0)
	}
	c1 := rep.Conflicts[1]
	if c1.CanonicalPath != "a.txt" || c1.DetectedAt != 100 {
		t.Errorf("conflict 1: got %+v", c1)
	}
	if rep.Total != 2 || rep.Truncated {
		t.Errorf("Total/Truncated: got %d/%v, want 2/false", rep.Total, rep.Truncated)
	}
}

func TestBuildConflictsEmpty(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if len(rep.Conflicts) != 0 {
		t.Errorf("Conflicts: got %d, want 0", len(rep.Conflicts))
	}
	if rep.Total != 0 || rep.Truncated {
		t.Errorf("Total/Truncated: got %d/%v, want 0/false", rep.Total, rep.Truncated)
	}
}

// TestBuildConflictsTruncatesOverCap confirms buildConflicts caps the report at
// maxConflictEntries, keeping the newest records by DetectedAt across views and
// reporting the full Total with Truncated set (an unbounded
// report outgrows the CLI's 1 MiB reply cap and makes every `conflicts` call fail).
func TestBuildConflictsTruncatesOverCap(t *testing.T) {
	oldCap := maxConflictEntries
	maxConflictEntries = 3
	t.Cleanup(func() { maxConflictEntries = oldCap })

	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView 1: %v", err)
	}
	if err := st.PutView(2, store.View{Name: "beta"}); err != nil {
		t.Fatalf("PutView 2: %v", err)
	}
	seed := []struct {
		viewID uint64
		path   string
		at     int64
	}{
		{1, "a.txt", 100},
		{1, "b.txt", 400},
		{1, "c.txt", 200},
		{2, "d.txt", 500},
		{2, "e.txt", 300},
	}
	for _, s := range seed {
		c := store.Conflict{CanonicalPath: s.path, CopyPath: s.path + ".copy", DetectedAt: s.at}
		if err := st.PutConflict(s.viewID, c); err != nil {
			t.Fatalf("PutConflict %s: %v", s.path, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != 5 || !rep.Truncated {
		t.Errorf("Total/Truncated: got %d/%v, want 5/true", rep.Total, rep.Truncated)
	}
	if len(rep.Conflicts) != 3 {
		t.Fatalf("Conflicts: got %d, want 3", len(rep.Conflicts))
	}
	// The newest three survive, ordered newest first.
	for i, want := range []struct {
		path string
		at   int64
	}{{"d.txt", 500}, {"b.txt", 400}, {"e.txt", 300}} {
		got := rep.Conflicts[i]
		if got.CanonicalPath != want.path || got.DetectedAt != want.at {
			t.Errorf("conflict %d: got %q@%d, want %q@%d", i, got.CanonicalPath, got.DetectedAt, want.path, want.at)
		}
	}
}

// TestBuildConflictsByteBudgetTrims confirms the secondary byte budget: even
// under the record cap, long paths must not push the encoded report past the
// CLI's 1 MiB reply cap - buildConflicts trims the newest-first list until the
// JSON fits. Runs against the production maxConflictEntries/maxConflictReportBytes.
func TestBuildConflictsByteBudgetTrims(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// 20 records × ~64 KiB ≈ 1.25 MiB encoded: over the byte budget while far
	// under the 1000-record cap. The canonical path stays small enough for the
	// bbolt key limit; the bulk rides in the copy path (value only).
	const n = 20
	longCanonical := strings.Repeat("c", 8<<10)
	longCopy := strings.Repeat("k", 56<<10)
	for i := 0; i < n; i++ {
		c := store.Conflict{
			CanonicalPath: fmt.Sprintf("%s-%02d", longCanonical, i),
			CopyPath:      longCopy,
			DetectedAt:    int64(i),
		}
		if err := st.PutConflict(1, c); err != nil {
			t.Fatalf("PutConflict %d: %v", i, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != n || !rep.Truncated {
		t.Errorf("Total/Truncated: got %d/%v, want %d/true", rep.Total, rep.Truncated, n)
	}
	if len(rep.Conflicts) == 0 || len(rep.Conflicts) >= n {
		t.Fatalf("Conflicts: got %d, want trimmed to 0 < len < %d", len(rep.Conflicts), n)
	}
	enc, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(enc) > maxConflictReportBytes {
		t.Errorf("encoded report: %d bytes, want <= %d", len(enc), maxConflictReportBytes)
	}
	if len(enc) >= 1<<20 {
		t.Errorf("encoded report: %d bytes, want < 1 MiB (the IPC reply cap)", len(enc))
	}
	// The trimmed list keeps the newest records, newest first.
	for i, c := range rep.Conflicts {
		want := int64(n - 1 - i)
		if c.DetectedAt != want {
			t.Errorf("conflict %d: DetectedAt got %d, want %d", i, c.DetectedAt, want)
		}
	}
}

// TestBuildConflictsStreamsBoundedOverCap confirms the streaming newest-N
// selection keeps only the newest maxConflictEntries records
// even when the bucket holds far more, without materialising the whole bucket:
// with a cap of 5 and 60 records spread across views, Total is the full count,
// Truncated is set, exactly the cap survives, and they are the newest by
// DetectedAt in descending order - the heap must have evicted every older record.
func TestBuildConflictsStreamsBoundedOverCap(t *testing.T) {
	oldCap := maxConflictEntries
	maxConflictEntries = 5
	t.Cleanup(func() { maxConflictEntries = oldCap })

	st := openTestStore(t)
	for v := uint64(1); v <= 3; v++ {
		if err := st.PutView(v, store.View{Name: fmt.Sprintf("view%d", v)}); err != nil {
			t.Fatalf("PutView %d: %v", v, err)
		}
	}
	// 60 records, DetectedAt = a shuffled-but-deterministic spread so the newest
	// are scattered across views (not conveniently last), forcing real eviction.
	const n = 60
	for i := 0; i < n; i++ {
		view := uint64(i%3) + 1
		at := int64((i * 37) % n) // a permutation of 0..59
		c := store.Conflict{
			CanonicalPath: fmt.Sprintf("p-%02d.txt", i),
			CopyPath:      fmt.Sprintf("p-%02d.copy", i),
			DetectedAt:    at,
		}
		if err := st.PutConflict(view, c); err != nil {
			t.Fatalf("PutConflict %d: %v", i, err)
		}
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != n || !rep.Truncated {
		t.Errorf("Total/Truncated: got %d/%v, want %d/true", rep.Total, rep.Truncated, n)
	}
	if len(rep.Conflicts) != maxConflictEntries {
		t.Fatalf("Conflicts: got %d, want %d", len(rep.Conflicts), maxConflictEntries)
	}
	// The newest five DetectedAt values (a permutation of 0..59) are 59..55,
	// newest first.
	for i, c := range rep.Conflicts {
		want := int64(n - 1 - i)
		if c.DetectedAt != want {
			t.Errorf("conflict %d: DetectedAt got %d, want %d (list: %+v)", i, c.DetectedAt, want, rep.Conflicts)
		}
	}
}

// TestBuildConflictsTieBreakKeepsCollectionOrder pins that DetectedAt ties keep
// the ascending view/path order the records were streamed in - the stable
// behaviour the former stable sort provided, now reproduced by the bounded heap's
// seq tie-break. With a cap of 2 and three records all at the same DetectedAt,
// the two earliest-collected (lowest view id, then lowest path) survive.
func TestBuildConflictsTieBreakKeepsCollectionOrder(t *testing.T) {
	oldCap := maxConflictEntries
	maxConflictEntries = 2
	t.Cleanup(func() { maxConflictEntries = oldCap })

	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView 1: %v", err)
	}
	if err := st.PutView(2, store.View{Name: "beta"}); err != nil {
		t.Fatalf("PutView 2: %v", err)
	}
	// Collection order (ListViews asc view, ScanConflicts asc path):
	// (view1,a)@50, (view1,b)@50, (view2,a)@50 - all tied.
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", DetectedAt: 50}); err != nil {
		t.Fatalf("PutConflict 1a: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "b.txt", DetectedAt: 50}); err != nil {
		t.Fatalf("PutConflict 1b: %v", err)
	}
	if err := st.PutConflict(2, store.Conflict{CanonicalPath: "a.txt", DetectedAt: 50}); err != nil {
		t.Fatalf("PutConflict 2a: %v", err)
	}

	d, _ := newStatusDaemon(t, st, config.Config{})
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts: %v", err)
	}
	if rep.Total != 3 || !rep.Truncated || len(rep.Conflicts) != 2 {
		t.Fatalf("report: Total=%d Truncated=%v len=%d, want 3/true/2", rep.Total, rep.Truncated, len(rep.Conflicts))
	}
	// The two earliest-collected ties survive, in collection order.
	c0, c1 := rep.Conflicts[0], rep.Conflicts[1]
	if c0.ViewID != 1 || c0.CanonicalPath != "a.txt" || c1.ViewID != 1 || c1.CanonicalPath != "b.txt" {
		t.Errorf("tie order: got [%d/%s, %d/%s], want [1/a.txt, 1/b.txt]",
			c0.ViewID, c0.CanonicalPath, c1.ViewID, c1.CanonicalPath)
	}
}

func TestBuildConflictsStoreError(t *testing.T) {
	st := openTestStore(t)
	d, _ := newStatusDaemon(t, st, config.Config{})
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := d.buildConflicts(context.Background()); err == nil {
		t.Fatal("buildConflicts on closed store: got nil error, want failure")
	}
}

func TestConflictsHandlerRoundTrip(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(1, store.View{Name: "alpha"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutConflict(1, store.Conflict{CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt", DetectedAt: 7}); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}

	d, l := newStatusDaemon(t, st, config.Config{})
	eng := &fakeEngine{}
	d.setEngine(eng)

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

	var got ipc.ConflictReport
	if err := c.Call("conflicts", nil, &got); err != nil {
		t.Fatalf("Call conflicts: %v", err)
	}
	if len(got.Conflicts) != 1 || got.Conflicts[0].CanonicalPath != "a.txt" {
		t.Errorf("Conflicts: got %+v", got.Conflicts)
	}

	// A control verb over the wire acks and reaches the engine. The server handles
	// one request per connection, so dial a fresh client.
	c2, err := ipc.Dial(d.sockPath)
	if err != nil {
		t.Fatalf("ipc.Dial (pause): %v", err)
	}
	defer c2.Close()
	var ack ipc.ControlAck
	if err := c2.Call("pause", nil, &ack); err != nil {
		t.Fatalf("Call pause: %v", err)
	}
	if !ack.OK {
		t.Errorf("pause ack: got %+v, want OK", ack)
	}
	if eng.pauseCount.Load() != 1 {
		t.Errorf("pause: engine got %d calls, want 1", eng.pauseCount.Load())
	}

	cancel()
	<-served
}
