package engine

import (
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// seedQueue enqueues ops and returns the engine holding them.
func seedQueue(t *testing.T, ops ...store.Op) *Engine {
	t.Helper()
	st := newTestStore(t)
	for _, op := range ops {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	return New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
}

// TestScanQueuedInViewFiltersByView: every caller wanted one view's ops and each wrote the
// same guard. One place to get it wrong is better than ten.
func TestScanQueuedInViewFiltersByView(t *testing.T) {
	e := seedQueue(t,
		store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "a.txt"},
		store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 2, Path: "b.txt"},
		store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "c.txt"},
	)
	var seen []string
	if err := e.scanQueuedInView(1, "test", func(qo store.QueuedOp) error {
		seen = append(seen, qo.Path)
		return nil
	}); err != nil {
		t.Fatalf("scanQueuedInView: %v", err)
	}
	if len(seen) != 2 || seen[0] != "a.txt" || seen[1] != "c.txt" {
		t.Fatalf("saw %v, want only view 1's ops", seen)
	}
}

// TestScanQueuedInViewStopsWithoutError is the rule worth centralising. errStopScan is a
// control signal: a caller that only needs to know whether a match EXISTS stops at the
// first one. Reported as an error it would look like an unreadable queue - and several of
// these scans guard a destructive apply, which refuses on exactly that signal. So a stop
// must read as success, and a real failure must not.
func TestScanQueuedInViewStopsWithoutError(t *testing.T) {
	e := seedQueue(t,
		store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "a.txt"},
		store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "b.txt"},
	)
	n := 0
	if err := e.scanQueuedInView(1, "test", func(store.QueuedOp) error {
		n++
		return errStopScan
	}); err != nil {
		t.Fatalf("an early stop must not be reported as a failure: %v", err)
	}
	if n != 1 {
		t.Fatalf("callback ran %d times, want the scan to stop at the first op", n)
	}
}

// TestScanQueuedInViewWrapsRealFailures: the other half of the same rule. A genuine error
// has to survive, and carry what was being looked for.
func TestScanQueuedInViewWrapsRealFailures(t *testing.T) {
	e := seedQueue(t, store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "a.txt"})
	boom := errors.New("record will not decode")
	err := e.scanQueuedInView(1, "widgets", func(store.QueuedOp) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the callback's failure to survive", err)
	}
	if got := err.Error(); !strings.Contains(got, "widgets") {
		t.Fatalf("err = %q, want it to name what was being scanned", got)
	}
}
