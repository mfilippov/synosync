package engine

// Fairness of the long-poll against the local-change wake.
//
// pollOnce's wake goroutine used to cancel the in-flight long-poll for every wake it
// consumed. The wake channel is buffered and coalescing, so under continuous local churn
// (an application writing into the sync root at least once per cycle - a browser profile,
// a log, a build directory surviving watcher/filter.go) a wake was ALREADY pending when
// each poll started: every poll died at zero milliseconds, no poll ever reached the
// server's reply, and remote changes - including deletes and renames - were never pulled
// for as long as the churn lasted. Nothing surfaced it: no Stuck, no Warn, one Info line
// per cycle plus a burned TLS handshake for the re-dialed event connection.
//
// The fix keeps the wake as a latency optimization for the outbound side but drops its
// veto over inbound: a poll a wake cut short owes the next poll a completion, during
// which the wake is still consumed (so the scan throttle is reset and the push rides the
// next cycle's localScan) but does not cancel the read. These tests pin the alternation
// for both the watcher wake and TriggerSync, and pin that an idle poll is still woken at
// once.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// churnPollClient models the event connection while the server holds a pending change:
// LongPoll waits out a synthetic round-trip before reporting the changed view, so a wake
// that cancels the poll context first wins the race exactly as it does against a real
// network RTT. A cancelled poll reports the connection broken, as *actions.Client does
// after an interrupted read, and records the context error it observed so a test can tell
// a wake cancel (context.Canceled) apart from the client-side deadline
// (context.DeadlineExceeded) without depending on wall-clock timing. The embedded
// loopClient supplies the event pull and the download for the inbound apply.
type churnPollClient struct {
	*loopClient
	rtt     time.Duration
	changed []int64

	cmu      sync.Mutex
	polls    int
	broken   bool
	lastErr  error
	timeouts []int
}

func (c *churnPollClient) LongPoll(ctx context.Context, _ []actions.Subscription, timeout int) ([]int64, error) {
	c.cmu.Lock()
	c.polls++
	c.timeouts = append(c.timeouts, timeout)
	c.cmu.Unlock()
	select {
	case <-ctx.Done():
		c.cmu.Lock()
		c.broken = true
		c.lastErr = ctx.Err()
		c.cmu.Unlock()
		return nil, ctx.Err()
	case <-time.After(c.rtt):
		return c.changed, nil
	}
}

func (c *churnPollClient) Broken() bool {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.broken
}

func (c *churnPollClient) pollCount() int {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.polls
}

func (c *churnPollClient) observedErr() error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.lastErr
}

func (c *churnPollClient) askedTimeouts() []int {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return append([]int(nil), c.timeouts...)
}

// churnFixture builds an engine whose single view has one pending server-side create and
// whose event connection needs a round-trip to report it.
func churnFixture(t *testing.T) (*Engine, *churnPollClient, *replaceCountingTransport, string) {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{
		NodeID: "root7", Name: "Photos", Privilege: "rw", InitialSyncDone: true,
	}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	lc := &loopClient{
		views: []actions.View{{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "root7"}},
		events: map[uint64][]actions.Event{
			7: {{SyncID: 5, Action: "create", FileID: "f1", Path: "a.txt", Hash: "h", MTime: 1700000000, Size: 3}},
		},
		content: map[string][]byte{"f1": []byte("abc")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", MTime: 1700000000, Size: 3}},
	}
	c := &churnPollClient{loopClient: lc, rtt: 200 * time.Millisecond, changed: []int64{7}}
	tr := &replaceCountingTransport{client: c}
	e := New(st, tr, root, "other-session", nil)
	return e, c, tr, root
}

// TestPollOnceDeferredWakeLetsLongPollReachTheServer is the red test for wake starvation: a wake
// pending at the start of every poll must not starve inbound sync. The first poll is cut
// short by the churn (nothing is pulled), but the second - with churn still pending - runs
// to the server's reply and applies the remote create. Before the fix the second poll was
// cancelled at zero milliseconds too and the file never appeared, for as many cycles as
// the churn lasted.
func TestPollOnceDeferredWakeLetsLongPollReachTheServer(t *testing.T) {
	e, c, tr, root := churnFixture(t)
	wake := make(chan struct{}, 1)
	e.SetWake(wake)
	applied := filepath.Join(root, "Photos", "a.txt")

	// Cycle 1: a local change is already pending when the poll starts, so the wake wins
	// the race against the server's round-trip and the poll is cut short.
	wake <- struct{}{}
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (interrupted cycle): %v (a wake interrupt must not be terminal)", err)
	}
	if !errors.Is(c.observedErr(), context.Canceled) {
		t.Fatalf("long poll ended with %v, want context.Canceled (the wake must cut the first poll short)",
			c.observedErr())
	}
	if !e.pollDeferWake {
		t.Fatalf("pollDeferWake = false, want true (a wake-interrupted poll owes the next one a completion)")
	}
	if _, err := os.Stat(applied); !os.IsNotExist(err) {
		t.Fatalf("Stat(%s) err = %v, want not-exist (the interrupted poll pulled nothing)", applied, err)
	}

	// Cycle 2: the churn continues - another wake is pending before the poll even starts -
	// but this poll is owed a completion, so it reaches the server and the inbound create
	// is applied. A non-zero lastScan makes the wake's surviving latency contract
	// observable: the fairness pass still consumes the wake and forces the next scan.
	e.lastScan = time.Unix(1700000000, 0)
	wake <- struct{}{}
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (fairness cycle): %v", err)
	}
	got, err := os.ReadFile(applied)
	if err != nil {
		t.Fatalf("read applied file: %v (the deferred poll must reach the server and apply inbound)", err)
	}
	if string(got) != "abc" {
		t.Errorf("content = %q, want %q", got, "abc")
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (the wake consumed by the fairness pass still forces the scan)",
			e.lastScan)
	}
	if e.pollDeferWake {
		t.Errorf("pollDeferWake = true, want false (one completed poll pays the debt off)")
	}
	if n := c.pollCount(); n != 2 {
		t.Errorf("LongPoll calls = %d, want 2", n)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces != 1 {
		t.Errorf("ReplaceEvent calls = %d, want 1 (only the interrupted poll misaligns the socket)", replaces)
	}
	// The debt is paid, so a wake pending at the next poll interrupts it again: steady
	// churn alternates interrupt / complete rather than starving inbound.
	wake <- struct{}{}
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (third cycle): %v", err)
	}
	if !e.pollDeferWake {
		t.Errorf("pollDeferWake = false after the third cycle, want true (the wake is honoured again)")
	}
}

// TestPollOnceIdleWakeStillInterruptsPromptly pins the watcher's latency contract: with no
// fairness debt outstanding - the ordinary idle case - a single wake still cancels the
// in-flight long-poll at once instead of waiting out EventPollTimeout. The observed
// context error distinguishes the two without a wall-clock assertion: a honoured wake ends
// the poll with context.Canceled, an unhonoured one with the client-side deadline.
func TestPollOnceIdleWakeStillInterruptsPromptly(t *testing.T) {
	e, c, tr, _ := churnFixture(t)
	// A round-trip far longer than any plausible scheduling delay, so an unhonoured wake
	// would surface as the client-side deadline rather than a reply.
	c.rtt = time.Hour
	e.limits.EventPollTimeout = 1
	e.limits.PollDeadlineSlack = 0
	wake := make(chan struct{}, 1)
	e.SetWake(wake)
	e.lastScan = time.Unix(1700000000, 0)

	wake <- struct{}{}
	start := time.Now()
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if !errors.Is(c.observedErr(), context.Canceled) {
		t.Fatalf("long poll ended with %v, want context.Canceled (an idle poll must be woken at once)",
			c.observedErr())
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Errorf("pollOnce took %v, want well under the 1s poll deadline (the wake must not be delayed)", elapsed)
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (a wake forces the next localScan)", e.lastScan)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces != 1 {
		t.Errorf("ReplaceEvent calls = %d, want 1 (the wake-broken event conn is re-dialed)", replaces)
	}
}

// TestPollOnceManualWakeGetsTheSameFairness covers the TriggerSync path: a scripted
// `sync now` loop (or a burst of them) must not starve inbound either, since manualWake
// folds into the very same select. The first trigger cuts the poll short; the second,
// pending before the next poll starts, is consumed without cancelling it and the inbound
// create lands.
func TestPollOnceManualWakeGetsTheSameFairness(t *testing.T) {
	e, c, _, root := churnFixture(t)
	applied := filepath.Join(root, "Photos", "a.txt")

	e.TriggerSync()
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (interrupted cycle): %v", err)
	}
	if !errors.Is(c.observedErr(), context.Canceled) {
		t.Fatalf("long poll ended with %v, want context.Canceled (a trigger must cut the first poll short)",
			c.observedErr())
	}
	if !e.pollDeferWake {
		t.Fatalf("pollDeferWake = false, want true (a trigger-interrupted poll owes a completion)")
	}

	e.lastScan = time.Unix(1700000000, 0)
	e.TriggerSync()
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (fairness cycle): %v", err)
	}
	got, err := os.ReadFile(applied)
	if err != nil {
		t.Fatalf("read applied file: %v (a `sync now` loop must not starve inbound)", err)
	}
	if string(got) != "abc" {
		t.Errorf("content = %q, want %q", got, "abc")
	}
	if !e.lastScan.IsZero() {
		t.Errorf("lastScan = %v, want zero (the consumed trigger still forces the next scan)", e.lastScan)
	}
}

// TestFairnessPassAsksForABoundedServerTimeout pins the OTHER side of the fairness trade: the
// pass that ignores a pending wake must not make outbound wait out EventPollTimeout. The wake is
// consumed but the push only rides the NEXT cycle's localScan, so at the 300s default a saved
// file - or an explicit `synosync sync`, whose manualWake folds into the same select - could sit
// unpushed for five minutes. The deferring poll therefore asks the server for a short turn: still
// a complete subscribe-and-wait turn (the invariant the fairness fix rests on), just a bounded one.
func TestFairnessPassAsksForABoundedServerTimeout(t *testing.T) {
	e, c, _, _ := churnFixture(t)
	// A round-trip long enough that the poll ends on its own deadline rather than a reply, so
	// the asked-for timeout is what bounds the pass.
	c.rtt = time.Hour
	e.limits.PollDeadlineSlack = 0
	wake := make(chan struct{}, 1)
	e.SetWake(wake)

	// Cycle 1: the wake is honoured, so the full configured timeout is asked for.
	wake <- struct{}{}
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (interrupted cycle): %v", err)
	}
	if !e.pollDeferWake {
		t.Fatalf("pollDeferWake = false, want true")
	}
	if got := c.askedTimeouts(); len(got) != 1 || got[0] != e.limits.EventPollTimeout {
		t.Fatalf("honoured-wake poll asked for %v, want [%d] (the configured timeout is untouched)",
			got, e.limits.EventPollTimeout)
	}

	// Cycle 2: the fairness pass. The server now replies promptly (so the test does not sit out
	// the bounded turn), and the turn it was ASKED for is the capped one, not the configured 300s.
	c.rtt = 10 * time.Millisecond
	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce (fairness cycle): %v", err)
	}
	got := c.askedTimeouts()
	if len(got) != 2 || got[1] != fairnessPollTimeout {
		t.Fatalf("fairness poll asked for %v, want a second entry of %d (bounded, not the configured %d)",
			got, fairnessPollTimeout, e.limits.EventPollTimeout)
	}
	if e.pollDeferWake {
		t.Errorf("pollDeferWake = true after the bounded completion; the debt must be paid off")
	}
}

// TestPollOnceClosedWakeChannelCancelsOnAFairnessPass guards the closed-channel arm on the
// deferring pass: a closed wake channel reads ready forever, so it must still cancel and
// clear the seam rather than spin the poll goroutine on a channel that can never be
// drained (the fairness pass otherwise loops after consuming a wake).
func TestPollOnceClosedWakeChannelCancelsOnAFairnessPass(t *testing.T) {
	e, c, tr, _ := churnFixture(t)
	c.rtt = time.Hour
	e.limits.EventPollTimeout = 1
	e.limits.PollDeadlineSlack = 0
	wake := make(chan struct{})
	close(wake)
	e.SetWake(wake)
	// Enter the poll owing a completion, so the closed channel is seen on a fairness pass.
	e.pollDeferWake = true
	last := time.Unix(1700000000, 0)
	e.lastScan = last

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v (a closed wake channel must not be terminal)", err)
	}
	if e.wake != nil {
		t.Errorf("e.wake = %v, want nil (a closed wake channel must clear the seam)", e.wake)
	}
	if !errors.Is(c.observedErr(), context.Canceled) {
		t.Errorf("long poll ended with %v, want context.Canceled (the closed channel must cancel, not spin)",
			c.observedErr())
	}
	if !e.lastScan.Equal(last) {
		t.Errorf("lastScan = %v, want unchanged %v (a closed channel is not a real wake)", e.lastScan, last)
	}
	tr.mu.Lock()
	replaces := tr.replaces
	tr.mu.Unlock()
	if replaces != 1 {
		t.Errorf("ReplaceEvent calls = %d, want 1 (the cancelled in-flight read is re-dialed)", replaces)
	}
}
