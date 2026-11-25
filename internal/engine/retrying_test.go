package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
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
		// A bare EOF is also how a large upload whose local file shrank mid-stream ends.
		{"bare eof", fmt.Errorf("actions: resume_upload body: %w", io.EOF), ""},
		{"closed", fmt.Errorf("x: %w", net.ErrClosed), "the connection to the server was lost"},
		// syscall.Errno and the deadline sentinels satisfy net.Error: none of these may be
		// reported as a lost connection (the first review of #7 caught exactly that).
		{"local open", fmt.Errorf("apply: %w", &fs.PathError{Op: "open", Path: "x", Err: syscall.EACCES}),
			"a local file or folder could not be read or written"},
		{"local rename", &os.LinkError{Op: "rename", Old: "a", New: "b", Err: syscall.ENOENT},
			"a local file or folder could not be read or written"},
		{"bare errno", fmt.Errorf("x: %w", syscall.EACCES), ""},
		{"ctx deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), "the operation timed out"},

		{"cancelled", fmt.Errorf("x: %w", context.Canceled), ""},
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

// replaceErrTransport scripts ReplaceEvent's result.
type replaceErrTransport struct {
	fakeTransport
	replaceErr error
}

func (t *replaceErrTransport) ReplaceEvent(context.Context) error { return t.replaceErr }

// TestReplaceEventConnStampsContactOnlyOnSuccess pins that a completed re-dial counts as the
// server answering: after an outage the age otherwise kept counting from before it for as long
// as the first poll on the new connection held (issue #6).
func TestReplaceEventConnStampsContactOnlyOnSuccess(t *testing.T) {
	st := newTestStore(t)
	tr := &replaceErrTransport{fakeTransport: fakeTransport{client: &fakeClient{}}, replaceErr: errors.New("dial failed")}
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
	e2 := New(st, &replaceErrTransport{fakeTransport: fakeTransport{client: &fakeClient{}}}, t.TempDir(), "sess", discardLogger())
	if err := e2.replaceEventConn(context.Background(), &fakeClient{}); err != nil {
		t.Fatalf("replaceEventConn (healthy): %v", err)
	}
	if !e2.LastContact().IsZero() {
		t.Error("a no-op replace on a healthy connection stamped the contact time")
	}

	// A terminal re-dial failure (a closed pool) is returned and stamps nothing.
	e3 := New(st, &replaceErrTransport{fakeTransport: fakeTransport{client: &fakeClient{}}, replaceErr: transport.ErrPoolClosed},
		t.TempDir(), "sess", discardLogger())
	if err := e3.replaceEventConn(context.Background(), dead); !errors.Is(err, transport.ErrPoolClosed) {
		t.Fatalf("replaceEventConn (closed pool) = %v, want ErrPoolClosed", err)
	}
	if !e3.LastContact().IsZero() {
		t.Error("a terminal re-dial failure stamped the contact time")
	}
}

// TestFailingDetailsReasonPrecedenceAndBounds pins which reason wins and where the bands meet:
// a server code streak beats a classified transient, a LATER block beats a stale code, and an
// op at stuckThreshold-1 is Retrying while one at stuckThreshold is Stuck.
func TestFailingDetailsReasonPrecedenceAndBounds(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())
	enqueue := func(path string) uint64 {
		t.Helper()
		seq, err := st.Enqueue(store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: path})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		return seq
	}
	coded, blocked, edge, over := enqueue("coded"), enqueue("blocked"), enqueue("edge"), enqueue("over")
	codeReason := serverErrStreakReason(actions.CodeNameCaseCollision)

	e.stuckMu.Lock()
	e.opFailures[coded] = 3
	e.opServerErrs[coded] = serverErrStreak{code: actions.CodeNameCaseCollision}
	e.setOpLastReason(coded, "the connection to the server was lost")
	e.opFailures[blocked] = 4
	e.opServerErrs[blocked] = serverErrStreak{code: actions.CodeNameCaseCollision}
	e.setOpLastReason(blocked, reasonBlocked)
	e.opFailures[edge] = stuckThreshold - 1
	e.opFailures[over] = stuckThreshold
	e.stuckMu.Unlock()

	retrying, err := e.RetryingDetails(0)
	if err != nil {
		t.Fatalf("RetryingDetails: %v", err)
	}
	byPath := map[string]StuckOp{}
	for _, s := range retrying {
		byPath[s.Path] = s
	}
	if len(retrying) != 3 {
		t.Fatalf("RetryingDetails = %+v, want coded, blocked and edge", retrying)
	}
	if _, ok := byPath["over"]; ok {
		t.Error("an op AT stuckThreshold was listed as Retrying")
	}
	if got := byPath["coded"].Reason; got != codeReason {
		t.Errorf("coded reason = %q, want the server code %q", got, codeReason)
	}
	if got := byPath["blocked"].Reason; got != reasonBlocked {
		t.Errorf("blocked reason = %q, want %q over the stale code", got, reasonBlocked)
	}
	stuck, err := e.StuckDetails(0)
	if err != nil || len(stuck) != 1 || stuck[0].Path != "over" {
		t.Errorf("StuckDetails = %+v (err %v), want only the op at stuckThreshold", stuck, err)
	}
}

// TestPollAnsweredClassifiesPollOutcomes pins which poll endings count against the server.
func TestPollAnsweredClassifiesPollOutcomes(t *testing.T) {
	deadline := fmt.Errorf("poll: %w", context.DeadlineExceeded)
	broken := fmt.Errorf("poll: %w", actions.ErrConnBroken)
	cases := []struct {
		name                            string
		err                             error
		honorWake, interrupted, cancels bool
		want                            string // "answered", "miss" or "none"
	}{
		{"answered", nil, true, false, false, "answered"},
		{"answered alongside a wake", nil, true, true, false, "answered"},
		{"full poll timed out", deadline, true, false, false, "miss"},
		{"broken read", broken, true, false, false, "miss"},
		{"fairness pass held its planned window", deadline, false, false, false, "answered"},
		{"fairness pass broken", broken, false, false, false, "miss"},
		{"cut short by a wake", broken, true, true, false, "none"},
		{"run cancelled", broken, true, false, true, "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := "none"
			if a := pollAnswered(tc.err, tc.honorWake, tc.interrupted, tc.cancels); a != nil {
				got = map[bool]string{true: "answered", false: "miss"}[*a]
			}
			if got != tc.want {
				t.Errorf("pollAnswered = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestPollMissesCountsUnansweredPollsThroughPollOnce drives the streak through the real poll:
// a server that keeps failing the poll raises it, one answer resets it.
func TestPollMissesCountsUnansweredPollsThroughPollOnce(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "home", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	fail := true
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "home", Privilege: "rw", NodeID: "root7"}},
		pollFn: func(int) ([]int64, error) {
			if fail {
				return nil, fmt.Errorf("poll: %w", actions.ErrConnBroken)
			}
			return nil, nil
		},
	}
	e := New(st, &concTransport{client: lc}, t.TempDir(), "sess", discardLogger())

	for i := range 2 {
		if err := e.pollOnce(context.Background()); err == nil {
			t.Fatalf("poll %d succeeded, want the injected failure", i+1)
		}
	}
	if got := e.PollMisses(); got != 2 {
		t.Fatalf("PollMisses after two failed polls = %d, want 2", got)
	}
	fail = false
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if got := e.PollMisses(); got != 0 {
		t.Errorf("PollMisses after an answered poll = %d, want 0", got)
	}
}

// brokenPollClient breaks every long poll and then reports the socket broken, as a server that
// resets the poll does - so pollOnce re-dials the event connection after each failure.
type brokenPollClient struct {
	*loopClient
}

func (c *brokenPollClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, fmt.Errorf("poll: %w", actions.ErrConnBroken)
}

func (c *brokenPollClient) Broken() bool { return true }

// TestPollMissesSurviveTheRedialContactStamp pins the interaction the second review of #7 found:
// every failed poll is followed by a successful re-dial, which stamps contact, so the last-reply
// age reads "just now" - the miss streak must still grow, since it is all status has left.
func TestPollMissesSurviveTheRedialContactStamp(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "home", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{views: []actions.View{{ViewID: 7, Name: "home", Privilege: "rw", NodeID: "root7"}}}
	e := New(st, &concTransport{client: &brokenPollClient{loopClient: lc}}, t.TempDir(), "sess", discardLogger())

	for i := range 3 {
		if err := e.pollOnce(context.Background()); err == nil {
			t.Fatalf("poll %d succeeded, want the injected failure", i+1)
		}
	}
	if e.LastContact().IsZero() {
		t.Fatal("the re-dial after a broken poll did not stamp contact (precondition of this test)")
	}
	if got := e.PollMisses(); got != 3 {
		t.Errorf("PollMisses = %d, want 3 despite the re-dial stamping contact", got)
	}
	if !e.PollingSince().IsZero() {
		t.Error("PollingSince is set between polls")
	}
}

// TestUnclassifiedFirstFailureIsLoggedOncePerOpAndCapped pins the log line behind `status`'s
// "see synosync log": an op whose failure has no class gets one Info line on its FIRST failure
// (below stuckThreshold the log otherwise carried only the first error of each drain pass), a
// classified failure gets none since `status` names it, and one pass logs at most
// maxUnclassifiedLogsPerPass of them.
func TestUnclassifiedFirstFailureIsLoggedOncePerOpAndCapped(t *testing.T) {
	st := newTestStore(t)
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", log)
	const msg = "op failed; it stays queued"

	op := func(seq uint64, path string) store.QueuedOp {
		return store.QueuedOp{Seq: seq, Op: store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: path}}
	}
	e.beginDrainPass()
	e.noteOpFailure(op(1, "odd.bin"), errors.New("something unclassified"))
	e.noteOpFailure(op(1, "odd.bin"), errors.New("something unclassified"))
	e.noteOpFailure(op(2, "net.bin"), fmt.Errorf("x: %w", actions.ErrConnBroken))
	if n := strings.Count(buf.String(), msg); n != 1 {
		t.Fatalf("logged %d first-failure lines, want 1 (the unclassified op, once):\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "odd.bin") || !strings.Contains(buf.String(), "something unclassified") {
		t.Errorf("the line does not name the op and its error:\n%s", buf.String())
	}

	buf.Reset()
	e.beginDrainPass()
	for i := range maxUnclassifiedLogsPerPass + 5 {
		e.noteOpFailure(op(uint64(100+i), fmt.Sprintf("f%d", i)), errors.New("unclassified"))
	}
	if n := strings.Count(buf.String(), msg); n != maxUnclassifiedLogsPerPass {
		t.Errorf("one pass logged %d lines, want the cap %d", n, maxUnclassifiedLogsPerPass)
	}
	buf.Reset()
	e.beginDrainPass()
	e.noteOpFailure(op(999, "next.bin"), errors.New("unclassified"))
	if n := strings.Count(buf.String(), msg); n != 1 {
		t.Errorf("the next pass logged %d lines, want 1 (the cap is per pass)", n)
	}
}

// TestUnclassifiedFailureLogLatchesPerStreak pins the two gaps the review of #8 found: the line
// is owed to the LAST reason status shows, so an op whose first failure was classified or only
// blocked still gets it once it fails unclassified; and a cancelled drain (a shutdown) logs
// nothing.
func TestUnclassifiedFailureLogLatchesPerStreak(t *testing.T) {
	st := newTestStore(t)
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", log)
	const msg = "op failed; it stays queued"
	op := func(seq uint64) store.QueuedOp {
		return store.QueuedOp{Seq: seq, Op: store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 1, Path: "p"}}
	}
	e.beginDrainPass()

	e.noteOpFailure(op(1), fmt.Errorf("x: %w", actions.ErrConnBroken)) // classified first
	e.noteOpFailure(op(1), errors.New("odd"))                          // then unclassified
	e.noteOpFailure(op(1), errors.New("odd"))                          // latched: no second line
	e.noteOpBlocked(op(2), nil)                                        // first only blocked
	e.noteOpFailure(op(2), errors.New("odd"))
	e.noteOpFailure(op(3), fmt.Errorf("drain: %w", context.Canceled)) // shutdown
	if n := strings.Count(buf.String(), msg); n != 2 {
		t.Fatalf("logged %d lines, want 2 (ops 1 and 2 once each, none for the cancel):\n%s", n, buf.String())
	}

	// A new streak re-earns the line.
	buf.Reset()
	e.clearOpFailures(1)
	e.noteOpFailure(op(1), errors.New("odd"))
	if n := strings.Count(buf.String(), msg); n != 1 {
		t.Errorf("a new streak logged %d lines, want 1", n)
	}
}

// TestUnclassifiedFailureOverTheCapIsLoggedOnALaterPass pins that the cap defers, not drops: an
// op it suppressed is not latched, so a later pass still logs it.
func TestUnclassifiedFailureOverTheCapIsLoggedOnALaterPass(t *testing.T) {
	st := newTestStore(t)
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", log)
	op := func(seq uint64) store.QueuedOp {
		return store.QueuedOp{Seq: seq, Op: store.Op{Type: store.OpUpload, ViewID: 1, Path: fmt.Sprintf("f%d", seq)}}
	}
	e.beginDrainPass()
	for i := range maxUnclassifiedLogsPerPass + 1 {
		e.noteOpFailure(op(uint64(i+1)), errors.New("odd"))
	}
	last := fmt.Sprintf("path=f%d ", maxUnclassifiedLogsPerPass+1)
	if strings.Contains(buf.String(), last) {
		t.Fatalf("the op over the cap was logged in the same pass:\n%s", buf.String())
	}
	e.beginDrainPass()
	e.noteOpFailure(op(uint64(maxUnclassifiedLogsPerPass+1)), errors.New("odd"))
	if !strings.Contains(buf.String(), last) {
		t.Errorf("the op over the cap was never logged on the next pass:\n%s", buf.String())
	}
}
