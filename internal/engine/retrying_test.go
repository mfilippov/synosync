package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestFailureReasonClassifiesWhatAnOperatorActsOn pins the coarse classes `status` names for a
// failed op. The incident behind issue #6 was a rename whose guard probe hit a worker socket the
// server had reset: the chain carried ErrConnBroken under two layers of wrapping.
func TestFailureReasonClassifiesWhatAnOperatorActsOn(t *testing.T) {
	connReset := fmt.Errorf("engine: apply outbound rename: destination could not be proved free: %w",
		fmt.Errorf("actions: write query_node request: %w: %w", actions.ErrConnBroken,
			&net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}))
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"wrapped broken connection", connReset, "the connection to the server was lost"},
		{"bare net error", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			"the connection to the server was lost"},
		{"dns", fmt.Errorf("dial: %w", &net.DNSError{Err: "no such host", Name: "nas"}),
			"the server's name could not be resolved"},
		{"turn timeout", &turnTimeoutError{cause: context.DeadlineExceeded}, "the server took too long to answer"},
		{"stalled", fmt.Errorf("x: %w", actions.ErrTransferStalled), "the transfer stalled"},
		{"busy", actions.ErrNodeBusy, "the server is still settling this item"},
		{"parent", errParentUnknown, "its parent folder is not on the server yet"},
		{"unexpected", actions.ErrUnexpectedReply, "the server sent an unexpected reply"},
		{"unknown", errors.New("something else"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := failureReason(tc.err); got != tc.want {
				t.Errorf("failureReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestRetryingOpsIsTheBandBelowTheStuckThreshold pins that a failed op is reported from its
// FIRST failure, with its reason, and that the Retrying and Stuck bands never overlap: before
// issue #6 an op below stuckThreshold read in `status` exactly like work that is moving.
func TestRetryingOpsIsTheBandBelowTheStuckThreshold(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())

	enqueue := func(op store.Op) store.QueuedOp {
		t.Helper()
		seq, err := st.Enqueue(op)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		return store.QueuedOp{Seq: seq, Op: op}
	}
	failing := enqueue(store.Op{Type: store.OpRename, Direction: store.DirOutbound, ViewID: 1,
		Path: "Settings/VPN/a.conf", NewPath: "Settings/VPN/b.conf"})
	blocked := enqueue(store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1,
		Path: "Settings/VPN/b.conf"})
	stuck := enqueue(store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "stuck.txt"})
	enqueue(store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 1, Path: "fine.txt"})

	if n := e.RetryingOps(); n != 0 {
		t.Fatalf("RetryingOps on a healthy queue = %d, want 0", n)
	}

	e.noteOpFailure(failing, fmt.Errorf("probe: %w", actions.ErrConnBroken))
	e.noteOpFailure(failing, fmt.Errorf("probe: %w", actions.ErrConnBroken))
	e.noteOpBlocked(blocked, &chainCulprit{seq: failing.Seq, typ: failing.Type, path: failing.Path})
	for range stuckThreshold {
		e.noteOpFailure(stuck, errors.New("unclassified"))
	}

	if n := e.RetryingOps(); n != 2 {
		t.Errorf("RetryingOps = %d, want 2 (the failing op and the one blocked behind it)", n)
	}
	if n := e.StuckOps(); n != 1 {
		t.Errorf("StuckOps = %d, want 1 (the bands must not overlap)", n)
	}

	got, err := e.RetryingDetails(0)
	if err != nil {
		t.Fatalf("RetryingDetails: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("RetryingDetails = %+v, want two entries", got)
	}
	if got[0].Path != failing.Path || got[0].NewPath != failing.NewPath || got[0].Failures != 2 {
		t.Errorf("first entry = %+v, want the rename with 2 failures (worst first)", got[0])
	}
	if got[0].Reason != "the connection to the server was lost" {
		t.Errorf("rename reason = %q, want the dropped connection named", got[0].Reason)
	}
	if got[1].Reason != reasonBlocked {
		t.Errorf("blocked reason = %q, want %q", got[1].Reason, reasonBlocked)
	}

	stuckList, err := e.StuckDetails(0)
	if err != nil {
		t.Fatalf("StuckDetails: %v", err)
	}
	if len(stuckList) != 1 || stuckList[0].Path != "stuck.txt" || stuckList[0].Reason != "" {
		t.Errorf("StuckDetails = %+v, want only stuck.txt with no reason (unclassified error)", stuckList)
	}

	// The op converges: it leaves both the count and the reason map.
	e.clearOpFailures(failing.Seq)
	if n := e.RetryingOps(); n != 1 {
		t.Errorf("RetryingOps after the rename applied = %d, want 1", n)
	}
	e.stuckMu.Lock()
	_, kept := e.opLastReason[failing.Seq]
	e.stuckMu.Unlock()
	if kept {
		t.Error("clearOpFailures left the op's reason behind")
	}
}

// pollProbeClient reports the engine's PollingSince as seen from INSIDE the long poll.
type pollProbeClient struct {
	*loopClient
	e    *Engine
	seen time.Time
}

func (c *pollProbeClient) LongPoll(ctx context.Context, subs []actions.Subscription, timeout int) ([]int64, error) {
	c.seen = c.e.PollingSince()
	return c.loopClient.LongPoll(ctx, subs, timeout)
}

// TestPollingSinceBracketsTheLongPoll pins that `status` can tell a held poll - the normal state
// of an idle sync, with no reply to stamp for up to EventPollTimeout - from a silent server.
func TestPollingSinceBracketsTheLongPoll(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "home", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		views:  []actions.View{{ViewID: 7, Name: "home", Privilege: "rw", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) { return nil, nil },
	}
	probe := &pollProbeClient{loopClient: lc}
	e := New(st, &concTransport{client: probe}, t.TempDir(), "sess", nil)
	probe.e = e

	if !e.PollingSince().IsZero() {
		t.Fatal("PollingSince before any poll is not zero")
	}
	before := time.Now()
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if probe.seen.IsZero() || probe.seen.Before(before) {
		t.Errorf("PollingSince inside the poll = %v, want a time at or after %v", probe.seen, before)
	}
	if !e.PollingSince().IsZero() {
		t.Error("PollingSince after the poll returned is not zero")
	}
}

// replaceCountTransport counts ReplaceEvent calls and can fail them.
type replaceCountTransport struct {
	fakeTransport
	replaceErr error
}

func (t *replaceCountTransport) ReplaceEvent(context.Context) error { return t.replaceErr }

// TestReplaceEventConnStampsContactOnlyOnSuccess pins that a completed re-dial counts as the
// server answering: after an outage the age otherwise kept counting from before it for as long
// as the first poll on the new connection held (issue #6).
func TestReplaceEventConnStampsContactOnlyOnSuccess(t *testing.T) {
	st := newTestStore(t)
	tr := &replaceCountTransport{fakeTransport: fakeTransport{client: &fakeClient{}}, replaceErr: errors.New("dial failed")}
	e := New(st, tr, t.TempDir(), "sess", discardLogger())
	dead := &fakeClient{broken: true}

	if err := e.replaceEventConn(context.Background(), dead); err != nil {
		t.Fatalf("replaceEventConn (failed dial): %v", err)
	}
	if !e.LastContact().IsZero() {
		t.Fatal("a failed re-dial stamped the contact time")
	}

	tr.replaceErr = nil
	if err := e.replaceEventConn(context.Background(), dead); err != nil {
		t.Fatalf("replaceEventConn: %v", err)
	}
	if e.LastContact().IsZero() {
		t.Error("a completed re-dial did not stamp the contact time")
	}

	// A healthy connection is not re-dialed, so nothing is stamped on its behalf.
	e2 := New(st, &replaceCountTransport{fakeTransport: fakeTransport{client: &fakeClient{}}}, t.TempDir(), "sess", discardLogger())
	if err := e2.replaceEventConn(context.Background(), &fakeClient{}); err != nil {
		t.Fatalf("replaceEventConn (healthy): %v", err)
	}
	if !e2.LastContact().IsZero() {
		t.Error("a no-op replace on a healthy connection stamped the contact time")
	}
}
