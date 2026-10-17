package engine

import (
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestStuckDetailsNamesThePathsAndReasons pins the listing behind the Stuck count. The number
// alone said something was wedged and nothing about what; the paths and the reason each op
// keeps failing lived only in the log.
func TestStuckDetailsNamesThePathsAndReasons(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())

	seqA, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	seqB, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7, Path: "b.txt", NewPath: "c.txt",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// A third op that is queued but NOT stuck: it must not appear.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "fine.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	e.stuckMu.Lock()
	e.opFailures[seqA] = stuckThreshold
	e.opFailures[seqB] = stuckThreshold + 5
	e.opServerErrs[seqB] = serverErrStreak{code: actions.CodeNameCaseCollision}
	e.stuckMu.Unlock()

	got, err := e.StuckDetails(0)
	if err != nil {
		t.Fatalf("StuckDetails: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("listed %d ops, want the two stuck ones: %+v", len(got), got)
	}
	// Worst first.
	if got[0].Path != "b.txt" {
		t.Errorf("first entry = %q, want the op with more failures", got[0].Path)
	}
	if got[0].NewPath != "c.txt" {
		t.Errorf("a move did not name its destination: %+v", got[0])
	}
	if got[0].Reason == "" {
		t.Error("an op with a recognised server verdict reported no reason")
	}
	// The op with no server verdict of its own - most often one merely blocked behind a
	// failing predecessor - reports no reason rather than an invented one.
	if got[1].Path != "a.txt" || got[1].Reason != "" {
		t.Errorf("second entry = %+v, want a.txt with no reason", got[1])
	}
}

// TestStuckDetailsCapsTheListing pins that the cap keeps the worst, since the count beside it
// already reports the scale.
func TestStuckDetailsCapsTheListing(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())

	for i, p := range []string{"a", "b", "c"} {
		seq, err := st.Enqueue(store.Op{Type: store.OpUpload, ViewID: 7, Path: p})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		e.stuckMu.Lock()
		e.opFailures[seq] = stuckThreshold + i
		e.stuckMu.Unlock()
	}

	got, err := e.StuckDetails(2)
	if err != nil {
		t.Fatalf("StuckDetails: %v", err)
	}
	if len(got) != 2 || got[0].Path != "c" || got[1].Path != "b" {
		t.Fatalf("listing = %+v, want the two worst", got)
	}
}

// TestStuckDetailsIsEmptyWithoutStuckOps pins that a healthy engine does the cheap thing: no
// queue scan at all when nothing has reached the threshold.
func TestStuckDetailsIsEmptyWithoutStuckOps(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())
	if _, err := st.Enqueue(store.Op{Type: store.OpUpload, ViewID: 7, Path: "a.txt"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := e.StuckDetails(0)
	if err != nil {
		t.Fatalf("StuckDetails: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("listing = %+v, want empty", got)
	}
}
