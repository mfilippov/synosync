package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
)

// logCapture is a minimal slog.Handler that records every emitted record so a test can
// assert a particular Warn fired (level + message substring). The engine is driven serially
// in these tests, but the mutex keeps it safe regardless.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }

// countWarns reports how many captured records are Warn-level and contain substr in their
// message.
func (h *logCapture) countWarns(substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

// countRecords reports how many captured records (at any level) contain substr in their
// message, for assertions on Info-level lines such as the pause-interrupted scan.
func (h *logCapture) countRecords(substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

func captureEngine(t *testing.T, st *store.Store, tr Transport) (*Engine, *logCapture) {
	t.Helper()
	cap := &logCapture{}
	// A real, empty sync root: the cold start scans it while the walk lists, so a path the
	// test process cannot read ("/root" for a non-root CI user) fails the scan before the
	// walk can reach the cap these tests are about.
	e := New(st, tr, t.TempDir(), "sess", slog.New(cap))
	return e, cap
}

// manyCreates builds a single-page catalog of n create events under cursor 0 so a cold-start
// pull accumulates exactly n catalog entries before the next (empty) page ends it.
func manyCreates(n int) *catalogClient {
	evs := make([]actions.Event, 0, n)
	for i := 0; i < n; i++ {
		evs = append(evs, createEvent(string(rune('a'+i%26))+string(rune('0'+i/26)), "f", int64(i+1), false))
	}
	return &catalogClient{tree: treeFromCatalog(evs), logHead: 10}
}

// TestColdStartViewCapAbortsRetryable confirms an oversized view degrades gracefully end to
// end: coldStartView surfaces the cap sentinel, the view stays InitialSyncDone=false (so the
// next cycle retries), and coldStartPending logs-and-continues rather than stopping Run.
func TestColdStartViewCapAbortsRetryable(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e, _ := captureEngine(t, st, &catalogTransport{client: manyCreates(10)})
	e.SetLimits(config.Limits{ColdStartCatalogCap: 2})

	err := e.coldStartView(context.Background(), listViewOf(t, st, 7))
	if !errors.Is(err, errColdStartCatalogCap) {
		t.Fatalf("coldStartView error = %v, want %v", err, errColdStartCatalogCap)
	}
	v := listViewOf(t, st, 7).View
	if v.InitialSyncDone {
		t.Errorf("InitialSyncDone = true, want false (cold start must be retryable)")
	}

	// coldStartPending must treat the cap as non-terminal: it logs the failure and returns
	// nil so Run keeps going.
	if perr := e.coldStartPending(context.Background()); perr != nil {
		t.Fatalf("coldStartPending = %v, want nil (cap is non-terminal)", perr)
	}
}

// TestWarnIfLargeTree exercises the high-water warning latch directly: it fires once on
// crossing the threshold, stays silent while still large, and re-arms after the view shrinks
// back below.
func TestWarnIfLargeTree(t *testing.T) {
	st := newTestStore(t)
	e, cap := captureEngine(t, st, &catalogTransport{client: &catalogClient{}})
	// A cap of 10 puts the high-water mark at cap/2 = 5.
	e.SetLimits(config.Limits{ColdStartCatalogCap: 10})
	if got := e.largeTreeThreshold(); got != 5 {
		t.Fatalf("largeTreeThreshold = %d, want 5 (cap/2)", got)
	}

	e.warnIfLargeTree(7, 4, "test") // below threshold: silent
	if n := cap.countWarns("unusually large view"); n != 0 {
		t.Fatalf("warns after below-threshold = %d, want 0", n)
	}
	e.warnIfLargeTree(7, 6, "test") // crosses: one warn
	e.warnIfLargeTree(7, 7, "test") // still large: latched, no repeat
	if n := cap.countWarns("unusually large view"); n != 1 {
		t.Fatalf("warns while latched = %d, want 1", n)
	}
	e.warnIfLargeTree(7, 2, "test") // shrinks: clears the latch
	e.warnIfLargeTree(7, 8, "test") // regrows: warns again
	if n := cap.countWarns("unusually large view"); n != 2 {
		t.Fatalf("warns after re-arm = %d, want 2", n)
	}
}

// TestLargeTreeThresholdNoCap confirms that without a cold-start cap the high-water mark falls
// back to a drain-batch multiple of the configured DrainBatch.
func TestLargeTreeThresholdNoCap(t *testing.T) {
	st := newTestStore(t)
	e, _ := captureEngine(t, st, &catalogTransport{client: &catalogClient{}})
	e.SetLimits(config.Limits{DrainBatch: 10, ColdStartCatalogCap: 0})
	if got, want := e.largeTreeThreshold(), 10*largeTreeWarnBatchMultiple; got != want {
		t.Errorf("largeTreeThreshold no-cap = %d, want %d", got, want)
	}
}

// TestWarnIfLargeTreeWithoutACapIsNotAWarning: with no cold-start cap there is no limit to
// near, so crossing the high-water mark is an Info line, not a Warn. It used to warn of
// "resource limits" at ~5k entries, the first line a user saw on any real NAS.
func TestWarnIfLargeTreeWithoutACapIsNotAWarning(t *testing.T) {
	st := newTestStore(t)
	e, cap := captureEngine(t, st, &catalogTransport{client: &catalogClient{}})
	e.SetLimits(config.Limits{DrainBatch: 10, ColdStartCatalogCap: 0})
	e.warnIfLargeTree(7, e.largeTreeThreshold()+1, "test")
	if n := cap.countWarns("large view"); n != 0 {
		t.Errorf("warns = %d, want none without a cap", n)
	}
}
