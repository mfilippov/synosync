package engine

// Tests for shared-fate isolation of the long_poll subscription.
// One view the server refuses to subscribe to failed the WHOLE poll for every view, and the
// retry re-issued the identical subscribe set forever: inbound sync stopped for the entire
// account with ~one Warn per second and no exit but a restart (the only quarantine, absentViews,
// is fed exclusively by the query_user_info enumeration, which keeps listing the view).

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/store"
)

// poisonClient rejects any long_poll whose subscribe set contains the poisoned view and accepts
// every other set, modelling an errno-form per-member rejection. It records each subscribe set so
// a test can assert what was probed and what the shared poll ended up subscribing to.
type poisonClient struct {
	poison  int64
	reject  error
	sets    [][]int64
	changed []int64
}

func (c *poisonClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *poisonClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *poisonClient) LongPoll(_ context.Context, subscribe []actions.Subscription, _ int) ([]int64, error) {
	ids := make([]int64, 0, len(subscribe))
	for _, s := range subscribe {
		ids = append(ids, s.ViewID)
	}
	c.sets = append(c.sets, ids)
	for _, id := range ids {
		if id == c.poison {
			return nil, c.reject
		}
	}
	return c.changed, nil
}
func (c *poisonClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *poisonClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// pollQuarantineEngine wires a two-view store to the poison client.
func pollQuarantineEngine(t *testing.T, c *poisonClient) (*Engine, *logCapture) {
	t.Helper()
	st := newTestStore(t)
	for _, v := range []struct {
		id   uint64
		name string
	}{{7, "Photos"}, {9, "Docs"}} {
		if err := st.PutView(v.id, store.View{Name: v.name, NodeID: "r", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	cap := &logCapture{}
	e := New(st, &concTransport{client: c}, t.TempDir(), "sess", slog.New(cap))
	// Views are already enumerated in the store; keep the refresh out of the way so the test
	// drives long_poll alone.
	e.lastViewRefresh = e.now()
	return e, cap
}

// TestPollQuarantinesPoisonedView is the red test: the shared poll keeps being rejected because
// of view 9, and after the threshold the engine must identify and quarantine exactly that view so
// view 7 keeps being polled.
func TestPollQuarantinesPoisonedView(t *testing.T) {
	c := &poisonClient{poison: 9, reject: &actions.ServerError{Code: 21005}}
	e, cap := pollQuarantineEngine(t, c)

	for i := 0; i < pollFailBisectThreshold; i++ {
		if err := e.pollOnce(context.Background()); err == nil {
			t.Fatalf("pollOnce #%d = nil, want the shared rejection", i)
		}
	}
	if _, poisoned := e.poisonedViews[9]; !poisoned {
		t.Fatalf("view 9 not quarantined after %d rejected shared polls; sets=%v", pollFailBisectThreshold, c.sets)
	}
	if _, poisoned := e.poisonedViews[7]; poisoned {
		t.Errorf("the healthy view was quarantined too")
	}
	if n := cap.countWarns("quarantining it from the shared"); n != 1 {
		t.Errorf("quarantine warns = %d, want exactly 1", n)
	}

	// The next poll subscribes to the healthy view alone and succeeds.
	c.sets = nil
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce after the quarantine = %v, want the healthy view to poll cleanly", err)
	}
	if len(c.sets) == 0 {
		t.Fatalf("no long_poll issued after the quarantine")
	}
	last := c.sets[len(c.sets)-1]
	if len(last) != 1 || last[0] != 7 {
		t.Errorf("subscribe set after the quarantine = %v, want [7]", last)
	}
}

// TestPollQuarantineExpiresAndRejoins pins the exit: the quarantine is temporary, so a
// server-side fix takes effect without a daemon restart.
func TestPollQuarantineExpiresAndRejoins(t *testing.T) {
	c := &poisonClient{poison: 9, reject: &actions.ServerError{Code: 21005}}
	e, cap := pollQuarantineEngine(t, c)
	e.poisonedViews[9] = 0

	for i := 0; i < pollPoisonRetryCycles-1; i++ {
		if err := e.pollOnce(context.Background()); err != nil {
			t.Fatalf("pollOnce #%d while quarantined = %v, want clean polls of the healthy view", i, err)
		}
		if _, poisoned := e.poisonedViews[9]; !poisoned {
			t.Fatalf("the quarantine expired after %d cycles, want %d", i+1, pollPoisonRetryCycles)
		}
	}
	// The server-side cause is fixed before the quarantine expires: the view rejoins and polls.
	c.poison = 0
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce at expiry: %v", err)
	}
	if _, poisoned := e.poisonedViews[9]; poisoned {
		t.Fatalf("the quarantine did not expire after %d cycles", pollPoisonRetryCycles)
	}
	if n := cap.countRecords("quarantine expired"); n != 1 {
		t.Errorf("expiry log lines = %d, want 1", n)
	}
	last := c.sets[len(c.sets)-1]
	if len(last) != 2 {
		t.Errorf("subscribe set after expiry = %v, want both views", last)
	}
}

// TestConnectionLevelPollFailureIsNotBisected pins the preserved path: a failure the SERVER never
// stated is not evidence about any member, so it never arms the probes and never quarantines
// anything. The raw-socket case is the one that matters in production: actions.Client.do returns a
// mid-turn I/O failure as a plain wrapped net error and NOT as ErrConnBroken (that sentinel only
// guards a SUBSEQUENT turn on the poisoned connection), so an exclusion list built on the sentinel
// armed the whole sweep - a worker lease per view, against a server that is simply unreachable -
// on every ordinary network outage.
func TestConnectionLevelPollFailureIsNotBisected(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"raw socket error", errors.New("actions: read long_poll reply: connection reset by peer")},
		{"poisoned connection", actions.ErrConnBroken},
		{"malformed reply", actions.ErrUnexpectedReply},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &poisonClient{poison: 9, reject: tc.err}
			e, cap := pollQuarantineEngine(t, c)

			for i := 0; i < 3*pollFailBisectThreshold; i++ {
				if err := e.pollOnce(context.Background()); err == nil {
					t.Fatalf("pollOnce #%d = nil, want the injected failure", i)
				}
			}
			if len(e.poisonedViews) != 0 {
				t.Errorf("a failure the server never stated quarantined %v; want nothing", e.poisonedViews)
			}
			if n := cap.countWarns("probing each subscribed view"); n != 0 {
				t.Errorf("per-view probes ran for a non-server failure (%d probe warns)", n)
			}
			if e.pollFailStreak != 0 {
				t.Errorf("pollFailStreak = %d, want 0 (only server-stated rejections count)", e.pollFailStreak)
			}
		})
	}
}

// TestUnattributablePollRejectionSuppressesFurtherProbes pins the probe rate limit: once a sweep
// has proved the rejection is not attributable to any single member, re-running the whole sweep
// every pollFailBisectThreshold retries costs one worker turn per view and 2+N Warn lines every
// few seconds for as long as the condition lasts - more load and more log volume than the spin
// the quarantine exists to stop. The suppression is temporary: it ages out with the poll cycles,
// so a genuinely poisoned member is still isolated later.
func TestUnattributablePollRejectionSuppressesFurtherProbes(t *testing.T) {
	c := &poisonClient{poison: -1, reject: nil}
	e, cap := pollQuarantineEngine(t, c)
	e.transport = &concTransport{client: &sharedOnlyRejectClient{poisonClient: c}}

	poll := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if err := e.pollOnce(context.Background()); err == nil {
				t.Fatalf("pollOnce = nil, want the shared rejection")
			}
		}
	}
	poll(pollFailBisectThreshold)
	if n := cap.countWarns("probing each subscribed view"); n != 1 {
		t.Fatalf("probe sweeps = %d, want the first one", n)
	}
	// Every cycle of the suppression window keeps rejecting, and none of them re-probes.
	poll(pollProbeSuppressCycles - 1)
	if n := cap.countWarns("probing each subscribed view"); n != 1 {
		t.Errorf("probe sweeps during the suppression window = %d, want 1", n)
	}
	// The window has aged out: the still-failing set is probed once more.
	poll(1)
	if n := cap.countWarns("probing each subscribed view"); n != 2 {
		t.Errorf("probe sweeps after the suppression aged out = %d, want 2", n)
	}
}

// TestPollFailureNotAttributableToOneViewQuarantinesNothing pins the conservative rule: when
// every single-member probe is accepted, the rejection was not about one member, so nothing is
// quarantined and the ordinary retry continues.
func TestPollFailureNotAttributableToOneViewQuarantinesNothing(t *testing.T) {
	// setRejecter rejects only MULTI-view sets: every single-view probe passes.
	c := &poisonClient{poison: -1, reject: nil}
	e, cap := pollQuarantineEngine(t, c)
	rejectShared := &sharedOnlyRejectClient{poisonClient: c}
	e.transport = &concTransport{client: rejectShared}

	for i := 0; i < pollFailBisectThreshold; i++ {
		if err := e.pollOnce(context.Background()); err == nil {
			t.Fatalf("pollOnce #%d = nil, want the shared rejection", i)
		}
	}
	if len(e.poisonedViews) != 0 {
		t.Errorf("quarantined %v although every single-view probe passed", e.poisonedViews)
	}
	if n := cap.countWarns("not attributable to one member"); n != 1 {
		t.Errorf("inconclusive-probe warns = %d, want 1", n)
	}
}

// probeFailClient rejects every MULTI-view set with a server errno (so the bisect arms) and
// fails every SINGLE-view probe with probeErr, modelling a probe that produces no verdict about
// the view it addressed.
type probeFailClient struct {
	*poisonClient
	probeErr error
	probes   int
}

func (c *probeFailClient) LongPoll(_ context.Context, subscribe []actions.Subscription, _ int) ([]int64, error) {
	c.sets = append(c.sets, subscriptionIDs(subscribe))
	if len(subscribe) > 1 {
		return nil, &actions.ServerError{Code: 21005}
	}
	c.probes++
	return nil, c.probeErr
}

// probeFailEngine wires the two-view quarantine engine to a probe that fails with probeErr.
func probeFailEngine(t *testing.T, probeErr error) (*Engine, *probeFailClient, *logCapture) {
	t.Helper()
	base := &poisonClient{poison: -1}
	e, cap := pollQuarantineEngine(t, base)
	c := &probeFailClient{poisonClient: base, probeErr: probeErr}
	e.transport = &concTransport{client: c}
	return e, c, cap
}

// TestPollProbeFailureIsNoVerdict pins the evidence rule for the probes themselves: a probe that
// fails for a reason the SERVER did not state - an ordinary socket error, which actions returns
// as a plain wrapped I/O error and NOT as ErrConnBroken - says nothing about the view it
// addressed. Quarantining on it would let one server- or network-wide outage evict every view
// from the shared subscription at once and stop inbound sync account-wide for
// pollPoisonRetryCycles on non-evidence.
func TestPollProbeFailureIsNoVerdict(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"raw socket error", errors.New("actions: read long_poll reply: connection reset by peer")},
		{"poisoned connection", actions.ErrConnBroken},
		{"malformed reply", actions.ErrUnexpectedReply},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, c, cap := probeFailEngine(t, tc.err)
			for i := 0; i < pollFailBisectThreshold; i++ {
				if err := e.pollOnce(context.Background()); err == nil {
					t.Fatalf("pollOnce #%d = nil, want the shared rejection", i)
				}
			}
			if c.probes == 0 {
				t.Fatalf("the per-view probes never ran; sets=%v", c.sets)
			}
			if len(e.poisonedViews) != 0 {
				t.Errorf("quarantined %v on a probe failure the server never stated", e.poisonedViews)
			}
			if n := cap.countWarns("no verdict for this view"); n == 0 {
				t.Errorf("the inconclusive probe was not reported as a non-verdict")
			}
		})
	}
}

// TestPollProbeBudgetExpiryIsNotTerminal is the red test for the probe's own budget: LongPoll is
// deliberately excluded from the per-turn deadline mapping, so an expiry of pollProbeBudget
// surfaced as a raw context.DeadlineExceeded - which isTerminal reads as the RUN context's own
// expiry, stopping Run and exiting the daemon. The probes only arm against a server that is
// already failing long_poll, i.e. exactly when a 30s probe is likely to time out.
func TestPollProbeBudgetExpiryIsNotTerminal(t *testing.T) {
	e, _, cap := probeFailEngine(t, context.DeadlineExceeded)
	var last error
	for i := 0; i < pollFailBisectThreshold; i++ {
		last = e.pollOnce(context.Background())
		if last == nil {
			t.Fatalf("pollOnce #%d = nil, want the shared rejection", i)
		}
	}
	if isTerminal(last) {
		t.Fatalf("pollOnce = %v, classified terminal: a probe's own budget must not stop the daemon", last)
	}
	if len(e.poisonedViews) != 0 {
		t.Errorf("quarantined %v on the probe's own timeout", e.poisonedViews)
	}
	if n := cap.countWarns("no verdict for this view"); n == 0 {
		t.Errorf("the timed-out probe was not reported as a non-verdict")
	}
}

// TestPollProbeTerminalErrorStopsTheLoop pins the other side: a probe that surfaces a genuinely
// terminal condition (a vanished session) aborts the bisect and is returned so Run stops for the
// caller to re-pair, rather than being logged as one view's verdict.
func TestPollProbeTerminalErrorStopsTheLoop(t *testing.T) {
	e, _, _ := probeFailEngine(t, auth.ErrSessionVanished)
	var last error
	for i := 0; i < pollFailBisectThreshold; i++ {
		last = e.pollOnce(context.Background())
	}
	if !errors.Is(last, auth.ErrSessionVanished) {
		t.Fatalf("pollOnce = %v, want the vanished session surfaced", last)
	}
	if !isTerminal(last) {
		t.Fatalf("pollOnce = %v, want a terminal classification", last)
	}
	if len(e.poisonedViews) != 0 {
		t.Errorf("quarantined %v on a terminal probe error", e.poisonedViews)
	}
}

// sharedOnlyRejectClient accepts every single-view subscription and rejects any set with more
// than one member, modelling a rejection that is about the SET (or the server), not a member.
type sharedOnlyRejectClient struct {
	*poisonClient
}

func (c *sharedOnlyRejectClient) LongPoll(_ context.Context, subscribe []actions.Subscription, _ int) ([]int64, error) {
	c.sets = append(c.sets, subscriptionIDs(subscribe))
	if len(subscribe) > 1 {
		return nil, &actions.ServerError{Code: 21005}
	}
	return nil, nil
}

// subscriptionIDs projects a subscribe set to its view ids for the assertions.
func subscriptionIDs(subs []actions.Subscription) []int64 {
	ids := make([]int64, 0, len(subs))
	for _, s := range subs {
		ids = append(ids, s.ViewID)
	}
	return ids
}
