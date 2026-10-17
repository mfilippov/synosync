package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// TestProgressCountsOnlyContentTransfers pins what the byte figure means. An operator reads
// it as throughput, so a metadata turn must not inflate it: a mkdir carries no content, and a
// rename and a remove move none either - their LocalSize describes the node, not a transfer.
func TestProgressCountsOnlyContentTransfers(t *testing.T) {
	cases := []struct {
		op   store.Op
		want int64
	}{
		{store.Op{Type: store.OpDownload, LocalSize: 4096}, 4096},
		{store.Op{Type: store.OpUpload, LocalSize: 1024}, 1024},
		{store.Op{Type: store.OpMkdir, LocalSize: 4096}, 0},
		{store.Op{Type: store.OpRename, LocalSize: 999}, 0},
		{store.Op{Type: store.OpRemove, LocalSize: 999}, 0},
	}
	for _, tc := range cases {
		if got := transferredBytes(tc.op); got != tc.want {
			t.Errorf("transferredBytes(%s, size %d) = %d, want %d",
				tc.op.Type, tc.op.LocalSize, got, tc.want)
		}
	}
}

// TestProgressWindowStartsAtTheFirstOp: the window exists to make a RATE meaningful, so it
// must not include the time spent connecting, walking the tree and planning - on a large view
// that is minutes, and folding it in would report a transfer rate several times below the real
// one at exactly the moment an operator is deciding whether the sync is working.
func TestProgressWindowStartsAtTheFirstOp(t *testing.T) {
	var p progress

	ops, bytes, window := p.snapshot()
	if ops != 0 || bytes != 0 || window != 0 {
		t.Fatalf("fresh snapshot = (%d, %d, %v), want all zero", ops, bytes, window)
	}

	p.record(100)
	time.Sleep(5 * time.Millisecond)
	p.record(0)
	p.record(50)

	ops, bytes, window = p.snapshot()
	if ops != 3 {
		t.Errorf("ops = %d, want 3 (every applied op, transferring or not)", ops)
	}
	if bytes != 150 {
		t.Errorf("bytes = %d, want 150", bytes)
	}
	if window <= 0 {
		t.Errorf("window = %v, want a positive window once something has been applied", window)
	}
}

// TestProgressWindowDoesNotRestart guards the CompareAndSwap: a later op must not move the
// start, or a long sync would report the rate of only its most recent moment.
func TestProgressWindowDoesNotRestart(t *testing.T) {
	var p progress
	p.record(1)
	first := p.startedUnixNano.Load()
	time.Sleep(2 * time.Millisecond)
	p.record(1)
	if got := p.startedUnixNano.Load(); got != first {
		t.Errorf("window start moved from %d to %d; it must be fixed at the first op", first, got)
	}
}

// TestEngineProgressIsZeroBeforeAnythingApplies: `status` renders the line only when
// something has moved, and it decides that from these values.
func TestEngineProgressIsZeroBeforeAnythingApplies(t *testing.T) {
	e := New(newTestStore(t), nil, t.TempDir(), "sess", nil)
	ops, bytes, window := e.Progress()
	if ops != 0 || bytes != 0 || window != 0 {
		t.Errorf("Progress() = (%d, %d, %v), want all zero on a fresh engine", ops, bytes, window)
	}
}

// TestLastContactStampsOnlyOnASuccessfulTurn covers what the status line means. It is
// the answer to "has the server said anything to us lately", which the connection state
// cannot give: a socket reads as connected whether or not anything comes back over it.
// So a turn that COMPLETES stamps it, a turn that fails leaves the previous time
// standing - the line ageing is the whole signal - and a fresh engine reports the zero
// time so `status` omits the line rather than printing "never" during bring-up.
func TestLastContactStampsOnlyOnASuccessfulTurn(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)

	if !e.LastContact().IsZero() {
		t.Fatalf("fresh engine LastContact = %v, want the zero time", e.LastContact())
	}

	if err := e.withWorker(context.Background(), func(syncClient) error { return nil }); err != nil {
		t.Fatalf("withWorker: %v", err)
	}
	first := e.LastContact()
	if first.IsZero() {
		t.Fatal("a completed turn did not stamp the contact time")
	}

	boom := errors.New("turn failed")
	if err := e.withWorker(context.Background(), func(syncClient) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("withWorker error = %v, want the injected failure", err)
	}
	if got := e.LastContact(); !got.Equal(first) {
		t.Fatalf("a failed turn moved the contact time from %v to %v; the line must age instead", first, got)
	}
}
