package engine

// Tests for the terminal classification of DETERMINISTIC server rejections:
// an outbound push the server refuses with the SAME unmapped result code on every
// drain used to retry forever, wedging every overlapping successor behind it through
// blockedKeys. It now dead-letters after unmappedServerErrDrains identical rejections, while
// alternating or one-off unmapped codes stay transient exactly as before.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// serverErrEngine wires an outbound engine (store, pushClient over concTransport, seeded
// view directory) with a Warn-level log buffer the escalation assertions read.
func serverErrEngine(t *testing.T, c *pushClient) (*Engine, *store.Store, string, *bytes.Buffer) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return New(st, &concTransport{client: c}, root, "sess", log), st, viewDir, &logbuf
}

// stubClock replaces e.now with a hand-advanced clock and returns the advance function, so a
// test can decide how much WALL CLOCK a rejection streak has spanned independently of how fast
// it drains (unmappedServerErrMinAge).
func stubClock(e *Engine) func(time.Duration) {
	at := time.Unix(1_750_000_000, 0)
	e.now = func() time.Time { return at }
	return func(d time.Duration) { at = at.Add(d) }
}

// seedMkdirWithChild seeds a directory holding one file, and queues the outbound mkdir for
// the directory plus the child upload that serialises behind it (the wedge shape: the child
// overlaps the mkdir's conflict key, so a permanently failing mkdir blocks it forever).
func seedMkdirWithChild(t *testing.T, st *store.Store, viewDir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "f.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "dir", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "dir", LocalMTime: 1,
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "dir/f.txt",
		LocalHash: "h", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
}

// TestOutboundDeterministicServerErrnoDeadLetters is the red test for a deterministic server errno:
// the server stably refuses `mkdir dir` with an unmapped errno (a share quota / ACL / naming
// policy). Pre-fix the mkdir retried every drain forever and its child upload stayed serialised
// behind it with no exit. Now: identical rejections accumulate as determinism evidence, the
// op is dead-lettered with the CODE in its durable trace once the streak is proven, and the
// child drains behind it (the parent's baseline is StateConflict, so it resolves terminally
// through errParentConflicted instead of wedging on an id-less pending parent).
func TestOutboundDeterministicServerErrnoDeadLetters(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, logbuf := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// Every drain below the streak leaves BOTH ops queued: the mkdir fails, the child is
	// skipped behind it. Nothing is dropped - an unknown code is not yet proof of permanence.
	for i := 0; i < unmappedServerErrDrains-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		// Age the streak past unmappedServerErrMinAge as well, so this test exercises the
		// drain-count rule alone; the wall-clock floor has its own test below.
		advance(unmappedServerErrMinAge)
		if n, _ := st.PendingLen(); n != 2 {
			t.Fatalf("drain %d: pending = %d, want both ops still queued", i+1, n)
		}
		if dl, _ := st.DeadLetterLen(); dl != 0 {
			t.Fatalf("drain %d: dead-lettered %d ops before the streak was proven", i+1, dl)
		}
	}

	// The streak-completing drain escalates: the mkdir is dropped, and the child - no longer
	// blocked by a failing predecessor - resolves terminally under the conflicted parent.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("escalation drain: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending after escalation = %d, want the whole chain drained", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 2 {
		t.Fatalf("dead letters = %d, want the mkdir and its blocked child", len(dls))
	}
	var mkdirTrace string
	for _, dl := range dls {
		if dl.Path == "dir" {
			mkdirTrace = dl.Reason
		}
	}
	if !strings.Contains(mkdirTrace, "33333") {
		t.Errorf("mkdir dead-letter trace = %q, want it to carry the server error code", mkdirTrace)
	}

	// The parent baseline records the divergence rather than vanishing, so the drop stays
	// visible in the Unresolved count instead of being silently forgotten.
	b, err := st.GetBaseline(7, "dir")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("parent baseline = %+v, want StateConflict", b)
	}
	// The escalation is observable, and the stuck escalation named the code first.
	logs := logbuf.String()
	if !strings.Contains(logs, "deterministic") || !strings.Contains(logs, "33333") {
		t.Errorf("escalation Warn missing from the log:\n%s", logs)
	}
	if got := e.StuckOps(); got != 0 {
		t.Errorf("StuckOps after the chain drained = %d, want 0", got)
	}
}

// TestOutboundDeterministicServerErrnoNeedsWallClockEvidence pins the second half of the
// determinism proof. drainQueue runs once per cycle PLUS once per changed view (syncView's
// trailing drain), so an account with remote activity can reach unmappedServerErrDrains within
// seconds - nothing like the "minutes of identical rejections" the drop rests on. Because the
// escalation is one-way for the op (its baseline goes StateConflict, which localReconcile then
// leaves alone, so no later scan re-derives the push), a server-side blip under load must not
// be enough to abandon a healthy local change permanently.
func TestOutboundDeterministicServerErrnoNeedsWallClockEvidence(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	stubClock(e) // the clock never advances: every drain lands in the same instant
	seedMkdirWithChild(t, st, viewDir)

	// Twice the drain count, all within one instant: the count rule is satisfied many times
	// over and the op must still be queued, unmarked and undropped.
	//
	// A drain here either fails the apply or DEFERS it: once the rejection has repeated, the
	// retry pause armed for the op holds it back (opbackoff.go), and with a frozen clock that
	// pause never elapses. Both outcomes leave the op queued, which is what this test is
	// about; the attempt count below pins the pacing itself.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		if err := e.drainQueue(context.Background()); err != nil && isTerminal(err) {
			t.Fatalf("drain %d: terminal error %v", i+1, err)
		}
	}
	// The pacing half of finding H4: 40 drains inside one instant must not mean 40 pushes.
	// Pre-fix nothing at all stood between a failed apply and the next drain's identical turn,
	// and the stand measured ~80 attempts a second for five minutes.
	c.mu.Lock()
	attempts := len(c.mkdirs)
	c.mu.Unlock()
	if attempts > 2 {
		t.Errorf("mkdir attempted %d times across %d drains in one instant, want the retry paced (<= 2)",
			attempts, 2*unmappedServerErrDrains)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dead-lettered %d ops on a streak with no wall-clock evidence, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Fatalf("pending = %d, want both ops still queued and retrying", n)
	}
	b, err := st.GetBaseline(7, "dir")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateConflict {
		t.Errorf("parent baseline marked StateConflict without wall-clock evidence; the push is now unrecoverable")
	}
}

// TestInboundServerErrnoNeverEscalates pins the direction rule, which is purely structural in
// applyOp (the escalation wraps only the DirOutbound branch) and so would be lost by a
// one-line move. Dead-lettering an inbound op would drop a server change this client has
// already consumed the event for, and no later drain re-derives it - the exact asymmetry with
// a push, which leaves the local file on disk for the next scan.
func TestInboundServerErrnoNeverEscalates(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	c := &dlClient{dlErr: &actions.ServerError{Code: 33333}}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	advance := stubClock(e)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "f.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Far past BOTH escalation rules: twice the drain count and many times the wall-clock floor.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dead-lettered %d inbound ops; an inbound op has no re-derivation and must keep retrying", dl)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("pending = %d, want the inbound download still queued", n)
	}
}

// TestOutboundStuckWarnCarriesServerErrno confirms the stuck escalation (which fires at
// stuckThreshold, well before the terminal streak) now names the server's own rejection
// code: an anonymous Stuck counter gives an operator nothing to act on.
func TestOutboundStuckWarnCarriesServerErrno(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 41111}}
	e, st, viewDir, logbuf := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < stuckThreshold; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		// Step past the retry pause the repeat rejection armed (opbackoff.go), so every drain
		// here really attempts the push: the stuck counter only advances on an ATTEMPT.
		advance(retryBackoffCap)
	}
	logs := logbuf.String()
	if !strings.Contains(logs, "op stuck") {
		t.Fatalf("no stuck Warn at the threshold:\n%s", logs)
	}
	if !strings.Contains(logs, "server_errno=41111") {
		t.Errorf("stuck Warn does not name the server code:\n%s", logs)
	}
	// Still far from the terminal streak: nothing dropped yet.
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops at the stuck threshold, want 0", dl)
	}
}

// TestOutboundAlternatingServerErrnosStayTransient is the conservatism guard: determinism
// is the ONLY evidence the escalation rests on, so codes that change from drain to drain
// never prove permanence - the op keeps retrying (observably, via Stuck) forever, exactly
// as before the fix.
func TestOutboundAlternatingServerErrnosStayTransient(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < 3*unmappedServerErrDrains; i++ {
		c.mu.Lock()
		if i%2 == 0 {
			c.mkdirErr = &actions.ServerError{Code: 44444}
		} else {
			c.mkdirErr = &actions.ServerError{Code: 33333}
		}
		c.mu.Unlock()
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued (alternating codes are not proof)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on alternating codes, want 0", dl)
	}
}

// TestOutboundServerErrnoStreakResetByOtherFailure confirms an unrelated transient in the
// middle of a run of identical codes restarts the evidence: the streak must be UNBROKEN, or
// a server hiccup interleaved with connection noise could be mistaken for determinism.
func TestOutboundServerErrnoStreakResetByOtherFailure(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < unmappedServerErrDrains-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(retryBackoffCap) // step past the paced retry so each drain really attempts
	}
	// One broken connection (a different failure class) in the middle of the run.
	c.mu.Lock()
	c.mkdirErr = actions.ErrConnBroken
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("broken-conn drain succeeded, want an error")
	}
	c.mu.Lock()
	c.mkdirErr = &actions.ServerError{Code: 33333}
	c.mu.Unlock()
	// The old streak is gone, so this drain (which would have been well past the threshold)
	// must not drop anything.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("post-reset drain succeeded, want the injected server rejection")
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops after the streak was broken, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued", n)
	}
}

// TestOutboundServerErrnoStreakClearedOnSuccess confirms a push that finally lands clears
// its streak, so a later, unrelated run of the same code starts counting from scratch
// instead of inheriting a stale near-terminal count.
func TestOutboundServerErrnoStreakClearedOnSuccess(t *testing.T) {
	c := &pushClient{
		mkdirErr:  &actions.ServerError{Code: 33333},
		mkdirNode: map[string]actions.Node{"dir": {FileID: "d1"}},
	}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)
	c.uploadNode = map[string]actions.Node{"f.txt": {FileID: "f1", Hash: "h"}}

	for i := 0; i < unmappedServerErrDrains-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(retryBackoffCap) // step past the paced retry so each drain really attempts
	}
	c.mu.Lock()
	c.mkdirErr = nil
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("healed drain: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending after the healed drain = %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on a successful push, want 0", dl)
	}
	e.stuckMu.Lock()
	streaks := len(e.opServerErrs)
	e.stuckMu.Unlock()
	if streaks != 0 {
		t.Errorf("rejection streaks left behind = %d, want 0 (the ops left the queue)", streaks)
	}
}

// TestEscalationStoreFailureLatchesDiskFull pins the escalation's store steps to the same
// disk-full discipline every sibling dead-letter arm in executor.go follows: a
// store-level failure while marking the conflict can be ENOSPC on a full disk, and returning it
// raw left the op retrying silently with `status` never naming the condition - on exactly the
// path made honest everywhere else.
func TestEscalationStoreFailureLatchesDiskFull(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	orig := markEscalatedConflict
	t.Cleanup(func() { markEscalatedConflict = orig })
	markEscalatedConflict = func(*Engine, store.Op) error {
		return fmt.Errorf("engine: mark outbound conflict: %w", hostNativeDiskFullErr)
	}

	for i := 0; i < unmappedServerErrDrains; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if !e.DiskFull() {
		t.Errorf("DiskFull() false after the escalation's store step failed with %v, want it latched",
			hostNativeDiskFullErr)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops although the conflict marking failed, want 0 (no trace a retry could double-count)", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued for retry", n)
	}

	// The terminal exit is the ONE path that used to leave escalateDeterministicServerFailure
	// without arming the pause, and it is reached only once the evidence is already complete. With
	// the store unwritable the op stays queued, so every following drain re-issued the whole doomed
	// turn at full drain cadence - the ~80 requests a second H4 measured, re-opened by the very
	// failure that stops the drop. Pace it on the way out too.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatalf("extra drain: succeeded, want the injected server rejection")
	}
	e.stuckMu.Lock()
	paced := len(e.opRetryAfter)
	e.stuckMu.Unlock()
	if paced == 0 {
		t.Error("no retry pause armed after the escalation's store step failed; the doomed push re-issues on every drain")
	}
}

// TestNoteServerErrStreak table-drives the evidence accounting directly: identical codes
// accumulate, a different code restarts at one (and re-stamps the streak's start), and a
// non-server failure clears the entry.
func TestNoteServerErrStreak(t *testing.T) {
	clock := time.Unix(1_750_000_000, 0)
	e := &Engine{opServerErrs: make(map[uint64]serverErrStreak), now: func() time.Time { return clock }}
	steps := []struct {
		err       error
		wantCode  int64
		wantCount int
		wantAge   time.Duration
	}{
		{&actions.ServerError{Code: 7}, 7, 1, 0},
		{errors.New("wrapped: " + (&actions.ServerError{Code: 7}).Error()), 0, 0, 0}, // message-only: no code
		{&actions.ServerError{Code: 7}, 7, 1, 0},
		{&actions.ServerError{Code: 7}, 7, 2, time.Minute},
		{&actions.ServerError{Code: 9}, 9, 1, 0}, // a new code re-stamps `since`
		{&actions.ServerError{Code: 9}, 9, 2, time.Minute},
		{actions.ErrConnBroken, 0, 0, 0},
		{&actions.ServerError{Code: 9}, 9, 1, 0},
	}
	for i, s := range steps {
		code, count, age := e.noteServerErr(1, s.err)
		if code != s.wantCode || count != s.wantCount || age != s.wantAge {
			t.Errorf("step %d: noteServerErr = (%d, %d, %v), want (%d, %d, %v)",
				i, code, count, age, s.wantCode, s.wantCount, s.wantAge)
		}
		clock = clock.Add(time.Minute)
	}
}

// TestServerErrStreakReason pins how each streak code is rendered for the operator. Two of
// them are synthetic negatives standing in for a non-answer, and a bare "error code -1" is
// nothing anyone can act on; 24580 is a REAL code whose meaning was measured on a live DSM
// (a create whose name differs from a sibling only by case - docs/INTEGRATION.md, settled
// 02.08.2026), so it keeps the generic prefix - the trace must still carry the number, and
// existing log-line matches must still hold - and appends the cause.
func TestServerErrStreakReason(t *testing.T) {
	cases := []struct {
		name string
		code int64
		want []string
	}{
		{"generic unmapped code", 33333, []string{"error code 33333"}},
		{"resume never completed", resumeNoCompletionCode, []string{"never reported the resumed upload as complete"}},
		{"resolve without a file id", resolveNoServerIDCode, []string{"without a file id"}},
		{"directory subtree unenumerable", removeDirUnverifiedCode, []string{"could not be enumerated"}},
		{"upload target absence unproved", uploadAbsenceUnprovedCode, []string{"too few times to prove the node gone"}},
		{"case collision", actions.CodeNameCaseCollision, []string{"error code 24580", "only by case"}},
		{"type collision", actions.CodeNameTypeCollision, []string{"error code 24581", "different type"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := serverErrStreakReason(tc.code)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("serverErrStreakReason(%d) = %q, want it to contain %q", tc.code, got, want)
				}
			}
		})
	}
}

// TestNameRejectionDeadLettersOnTheShortConfirmation is the end-to-end half of finding H4 for
// both MEASURED name refusals. Pre-fix each spent the full unknown-code window - twenty drains
// AND five minutes - re-sending a turn whose verdict is about the NAME and therefore identical
// every time (the stand recorded 23 610 attempts of one `mkdir`), holding the blocked child
// behind it for the duration. Now three agreeing rejections spanning nameRejectionMinAge are
// enough, the durable trace an operator reads in `status` names the CAUSE rather than just the
// number, and the neighbour the doomed op was blocking drains in the SAME pass.
func TestNameRejectionDeadLettersOnTheShortConfirmation(t *testing.T) {
	cases := []struct {
		name string
		code int64
		want []string
	}{
		{"case collision", actions.CodeNameCaseCollision, []string{"24580", "only by case"}},
		{"type collision", actions.CodeNameTypeCollision, []string{"24581", "different type"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{
				mkdirErr:   &actions.ServerError{Code: tc.code},
				uploadNode: map[string]actions.Node{"f.txt": {FileID: "f1", Hash: "h"}},
			}
			e, st, viewDir, logbuf := serverErrEngine(t, c)
			advance := stubClock(e)
			seedMkdirWithChild(t, st, viewDir)

			for i := 0; i < nameRejectionDrains-1; i++ {
				if err := e.drainQueue(context.Background()); err == nil {
					t.Fatalf("drain %d: succeeded, want the injected %d rejection", i+1, tc.code)
				}
				if dl, _ := st.DeadLetterLen(); dl != 0 {
					t.Fatalf("drain %d: dropped %d ops before the repetition was confirmed", i+1, dl)
				}
				// Enough wall clock to clear the paced retry AND, across the run, the
				// name-refusal floor - but nowhere near the five-minute unknown-code floor,
				// which is the point: the drop must not wait for that one.
				advance(nameRejectionMinAge)
			}
			if err := e.drainQueue(context.Background()); err != nil {
				t.Fatalf("escalation drain: %v", err)
			}

			dls, err := st.ListDeadLetters()
			if err != nil {
				t.Fatalf("ListDeadLetters: %v", err)
			}
			var trace string
			for _, dl := range dls {
				if dl.Path == "dir" {
					trace = dl.Reason
				}
			}
			if trace == "" {
				t.Fatalf("no dead-letter trace for the refused mkdir (got %d traces)", len(dls))
			}
			for _, want := range tc.want {
				if !strings.Contains(trace, want) {
					t.Errorf("dead-letter trace = %q, want it to contain %q", trace, want)
				}
			}
			if logs := logbuf.String(); !strings.Contains(logs, tc.want[len(tc.want)-1]) {
				t.Errorf("escalation Warn does not name the cause:\n%s", logs)
			}
			// The queue keeps moving in the very pass the doom is recorded: the child upload
			// that was serialised behind the mkdir is no longer blocked by a failing
			// predecessor, so it resolves (terminally, under the now-conflicted parent) rather
			// than waiting for another drain.
			if n, _ := st.PendingLen(); n != 0 {
				t.Errorf("pending after the escalation drain = %d, want the blocked neighbour to drain too", n)
			}
		})
	}
}

// TestNameRejectionNeedsRepetitionAndWallClock pins the corroboration the shortened window
// still demands. A name refusal is deterministic by measurement, not by a single reply: one
// rejection is no evidence, and neither are three landing inside a second - nothing spaces two
// drain passes in wall clock (drainQueue runs once per cycle plus once per changed view), which
// is why every corroboration-by-repetition streak in this engine carries a clock floor as well
// as a count.
func TestNameRejectionNeedsRepetitionAndWallClock(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: actions.CodeNameTypeCollision}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// One rejection: nothing dropped.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("first drain succeeded, want the injected 24581 rejection")
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dropped %d ops on a single rejection, want 0", dl)
	}
	// Enough drains to satisfy the COUNT, all within a second and a half of wall clock (the
	// paced retry's own spacing). The floor must still hold the drop back.
	for i := 0; i < 2*nameRejectionDrains; i++ {
		if err := e.drainQueue(context.Background()); err != nil && isTerminal(err) {
			t.Fatalf("drain %d: terminal error %v", i+1, err)
		}
		advance(retryBackoff(nameRejectionDrains))
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dropped %d ops with the count met but no wall-clock evidence, want 0", dl)
	}
	// Past the floor, the same streak escalates.
	advance(nameRejectionMinAge)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("escalation drain: %v", err)
	}
	if dl, _ := st.DeadLetterLen(); dl == 0 {
		t.Error("nothing dropped once both the count and the wall-clock floor were satisfied")
	}
}

// TestNonNameServerErrnoKeepsTheLongWindow is the containment guard for the shortened window:
// it applies ONLY to the two codes whose permanence was measured, so an UNKNOWN code must still
// serve out the conservative unmappedServerErr* window.
//
// The driven code must be one escalateDeterministicServerFailure actually carries as far as
// escalationThresholds: 32772 returns early at serverTransientRejection, so a test written on it
// asserted "nothing was dropped" for a reason that has nothing to do with the mapping under test
// and would have stayed green if serverNameRejection were widened to a third code. 32772's own
// exemption is pinned by TestTransientServerErrnoNeverEscalates.
func TestNonNameServerErrnoKeepsTheLongWindow(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: 33333}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	for i := 0; i < 2*nameRejectionDrains; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected 33333 rejection", i+1)
		}
		advance(nameRejectionMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dropped %d ops on a code with no measured permanence, want 0 (it keeps retrying)", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued and retrying", n)
	}
}

// TestTransientServerErrnoNeverEscalates is the red test for finding H5: 32772 is the one code
// whose TRANSIENCE was measured (a download of a just-overwritten node answers it once and the
// identical request serves the file a second later), so no run of it is evidence of permanence.
// Before the mapping an outbound op that kept drawing it accumulated the identical-code streak
// like any unknown rejection and was dead-lettered - abandoning a healthy local change over the
// server settling a node. It must keep retrying past BOTH the count and the wall-clock floor of
// the conservative window, and the op must stay queued.
func TestTransientServerErrnoNeverEscalates(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: actions.CodeNodeBusy}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// Twice the unknown-code window on both axes: any escalation at all would have fired.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected 32772 rejection", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dropped %d ops on a code measured to clear on its own, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued and retrying", n)
	}
	// Still paced, though: the streak is what arms the geometric pause, so a transient code
	// costs one attempt per backoff rather than one per drain (finding H4's ~80/second).
	if !serverTransientRejection(actions.CodeNodeBusy) {
		t.Error("serverTransientRejection(32772) = false, want true")
	}
	for _, code := range []int64{33333, actions.CodeNameCaseCollision, actions.CodeNameTypeCollision,
		resumeNoCompletionCode, resolveNoServerIDCode} {
		if serverTransientRejection(code) {
			t.Errorf("serverTransientRejection(%d) = true, want false: only 32772 was measured transient", code)
		}
	}
}

// TestTransientServerErrnoIsPaced pins the other half: the op is not escalated, but it IS held
// back between attempts. Without the pause a code the server clears in a second would be
// re-issued - whole body and all - on every drain of the outage.
func TestTransientServerErrnoIsPaced(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: actions.CodeNodeBusy}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// Drain repeatedly WITHOUT advancing the clock past the backoff: after the first couple of
	// refusals the op is deferred, so the server stops being asked.
	for i := 0; i < 20; i++ {
		_ = e.drainQueue(context.Background())
		advance(time.Millisecond)
	}
	if n := len(c.mkdirs); n > 5 {
		t.Errorf("issued %d mkdir turns across 20 drains, want a paced handful", n)
	}
}

// TestEscalationThresholds table-drives the window selection itself, so the mapping cannot be
// widened by accident: only the two measured name codes take the short confirmation, and the
// synthetic non-answer codes keep the conservative one.
func TestEscalationThresholds(t *testing.T) {
	cases := []struct {
		name  string
		code  int64
		short bool
	}{
		{"case collision", actions.CodeNameCaseCollision, true},
		{"type collision", actions.CodeNameTypeCollision, true},
		{"unknown code", 33333, false},
		{"transient churn code", 32772, false},
		{"stale cursor", actions.CodeStaleCursor, false},
		{"resume non-answer", resumeNoCompletionCode, false},
		{"resolve non-answer", resolveNoServerIDCode, false},
		{"remove-dir non-answer", removeDirUnverifiedCode, false},
		{"upload absence non-answer", uploadAbsenceUnprovedCode, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drains, age := escalationThresholds(tc.code)
			wantDrains, wantAge := unmappedServerErrDrains, unmappedServerErrMinAge
			if tc.short {
				wantDrains, wantAge = nameRejectionDrains, nameRejectionMinAge
			}
			if drains != wantDrains || age != wantAge {
				t.Errorf("escalationThresholds(%d) = (%d, %v), want (%d, %v)",
					tc.code, drains, age, wantDrains, wantAge)
			}
		})
	}
}

// TestUploadAbsenceUnprovedIsPacedButNeverEscalated pins the pacing the H1 upload guard's
// path-error arm needs. That arm answers "the node's absence is not proved yet"
// (errUploadTargetAbsenceUnproved), which is a MAPPED sentinel and not an unmapped errno - so
// ServerErrorCode answered false, noteServerErr deleted the streak AND the armed pause, and the
// op re-opened its source and re-issued the probe on every drain for the whole corroboration
// window: absentNodeConfirmations passes spaced by absenceStreakMinAge, i.e. minutes of exactly
// the ~80-attempts-a-second spin finding H4 measured and this same round set out to stop.
//
// It is folded in under a synthetic code and marked TRANSIENT: paced like a repeat rejection,
// never dead-lettered, because the streak is this client gathering its own evidence rather than
// the server refusing anything - dropping the push for it would discard the user's file.
func TestUploadAbsenceUnprovedIsPacedButNeverEscalated(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	stubClock(e)
	err := fmt.Errorf("guard: %w", errUploadTargetAbsenceUnproved)

	if !serverTransientRejection(uploadAbsenceUnprovedCode) {
		t.Fatal("serverTransientRejection(uploadAbsenceUnprovedCode) = false, want true: a push must " +
			"not be dropped for its own precondition still gathering evidence")
	}
	qo := store.QueuedOp{Seq: 41, Op: store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", FileID: "srv1",
	}}
	// Well past both floors of the conservative window on the count axis; the wall clock is
	// stubbed and never advances, but a transient code must not escalate on either.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		if got := e.escalateDeterministicServerFailure(qo, err); !errors.Is(got, errUploadTargetAbsenceUnproved) {
			t.Fatalf("escalate %d = %v, want the guard error passed through unchanged", i+1, got)
		}
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0: the absence corroborates itself, so the push must keep waiting", dl)
	}
	if code, count, _ := e.noteServerErr(qo.Seq, err); code != uploadAbsenceUnprovedCode || count < 2 {
		t.Errorf("noteServerErr = (%d, %d), want the synthetic code with an unbroken streak", code, count)
	}
	if _, armed := e.opRetryPending(qo.Seq); !armed {
		t.Error("no retry pause armed: the arm re-probes every drain, which is the H4 spin")
	}
	// Any other failure class still clears the streak and the pause with it.
	if code, count, _ := e.noteServerErr(qo.Seq, errors.New("connection reset")); code != 0 || count != 0 {
		t.Errorf("noteServerErr(other) = (%d, %d), want the streak cleared", code, count)
	}
	if _, armed := e.opRetryPending(qo.Seq); armed {
		t.Error("pause survived a change of failure class; the next attempt must not be held back")
	}
}

// TestNoteServerErrRemoveDirUnverifiedExcludesTransport pins the one exclusion the synthetic
// removeDirUnverifiedCode needs.
//
// That code is NOT a serverTransientRejection, so an unbroken streak of it reaches the terminal
// exit: the directory delete is dead-lettered and markOutboundConflict leaves a StateConflict
// record at a path with no local file, which localReconcile never drives again. The evidence that
// exit rests on is "the server keeps giving the identical non-answer" - and a subtree walk
// defeated by a broken socket, a stalled turn or an expired per-turn deadline is not the server
// answering at all. Folding it in let a server that keeps every lease but times out its `list`
// turns for unmappedServerErrMinAge supply the evidence for dropping the user's deletion, the one
// shape every other transport failure is explicitly protected from by the !ok branch.
func TestNoteServerErrRemoveDirUnverifiedExcludesTransport(t *testing.T) {
	clock := time.Unix(1_750_000_000, 0)
	e := &Engine{
		opServerErrs: make(map[uint64]serverErrStreak),
		opRetryAfter: make(map[uint64]time.Time),
		now:          func() time.Time { return clock },
	}
	// A server-stated non-answer (an unlisted reply) accumulates as before.
	unlisted := fmt.Errorf("%w: listing of 7/%q enumerated nothing", errRemoveDirUnverified, "d")
	if code, count, _ := e.noteServerErr(1, unlisted); code != removeDirUnverifiedCode || count != 1 {
		t.Fatalf("noteServerErr(unlisted) = (%d, %d), want (%d, 1)", code, count, removeDirUnverifiedCode)
	}
	clock = clock.Add(time.Minute)
	if code, count, _ := e.noteServerErr(1, unlisted); code != removeDirUnverifiedCode || count != 2 {
		t.Fatalf("noteServerErr(unlisted x2) = (%d, %d), want (%d, 2)", code, count, removeDirUnverifiedCode)
	}
	// A walk the CONNECTION defeated clears the streak instead of extending it.
	transport := []error{
		fmt.Errorf("%w: listing 7/%q: %w", errRemoveDirUnverified, "d", actions.ErrConnBroken),
		fmt.Errorf("%w: listing 7/%q: %w", errRemoveDirUnverified, "d", actions.ErrTransferStalled),
		fmt.Errorf("%w: listing 7/%q: %w", errRemoveDirUnverified, "d", errTurnTimeout),
		fmt.Errorf("%w: listing 7/%q: %w", errRemoveDirUnverified, "d", context.DeadlineExceeded),
		fmt.Errorf("%w: listing 7/%q: %w", errRemoveDirUnverified, "d", context.Canceled),
	}
	for _, terr := range transport {
		// Re-seed the streak each time, so the clearing is what the assertion observes.
		e.noteServerErr(1, unlisted)
		code, count, _ := e.noteServerErr(1, terr)
		if code != 0 || count != 0 {
			t.Errorf("noteServerErr(%v) = (%d, %d), want the streak cleared", terr, code, count)
		}
		e.stuckMu.Lock()
		_, paced := e.opRetryAfter[1]
		e.stuckMu.Unlock()
		if paced {
			t.Errorf("noteServerErr(%v) left a retry pause armed; a transport failure must be retried, not paced", terr)
		}
	}
}

// TestParentNotADirectoryTakesTheShortenedWindow pins the third measured refusal.
//
// 24584 is what every push BENEATH a clashing path draws when the peer made that path a FILE:
// the parent the push names is that file. Left in the unknown-code window those descendants held
// the queue for five minutes, and blockedKeys holds every path-overlapping op behind them -
// including the inbound download of the clashing path, which is the only thing that resolves the
// clash. TestIntegration_TwoClientsFileDirClash timed out at three minutes on exactly that.
func TestParentNotADirectoryTakesTheShortenedWindow(t *testing.T) {
	c := &pushClient{mkdirErr: &actions.ServerError{Code: actions.CodeParentNotADirectory}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedMkdirWithChild(t, st, viewDir)

	// Drain until the op is dropped, and count how long it took. The unknown-code window is the
	// thing being ruled out, so bound the loop by IT: a 24584 still serving out the conservative
	// window would run to the bound and fail.
	drains := 0
	for dl, _ := st.DeadLetterLen(); dl == 0; dl, _ = st.DeadLetterLen() {
		if drains >= unmappedServerErrDrains {
			t.Fatalf("24584 survived %d drains and %v: it is serving out the unknown-code "+
				"window, and a wedged descendant blocks the inbound apply that resolves the "+
				"clash for its whole duration", drains, unmappedServerErrMinAge)
		}
		if err := e.drainQueue(context.Background()); err != nil && isTerminal(err) {
			t.Fatalf("drain %d: terminal error %v", drains+1, err)
		}
		drains++
		advance(nameRejectionMinAge)
	}
	if drains > nameRejectionDrains+1 {
		t.Errorf("dropped after %d drains, want the shortened window of %d", drains, nameRejectionDrains)
	}
}

// TestParentNotADirectoryKeepsItsNumber guards the half of the mapping that is easy to lose.
//
// The engine's escalation is driven by an identical-CODE streak, so a code that reaches it as a
// bare sentinel drops out of the streak entirely and silently reverts to forever-retry. 24584
// must therefore answer ServerErrorCode AND match its sentinel, exactly as 24580/24581 do.
func TestParentNotADirectoryKeepsItsNumber(t *testing.T) {
	err := error(&actions.ServerError{Code: actions.CodeParentNotADirectory})
	code, ok := actions.ServerErrorCode(err)
	if !ok || code != actions.CodeParentNotADirectory {
		t.Errorf("ServerErrorCode = (%d, %v), want (%d, true)", code, ok, actions.CodeParentNotADirectory)
	}
	if !errors.Is(err, actions.ErrParentNotADirectory) {
		t.Error("24584 does not match ErrParentNotADirectory")
	}
	if !serverStructuralRejection(actions.CodeParentNotADirectory) {
		t.Error("serverStructuralRejection(24584) = false, want true")
	}
	// It is a refusal, not a transient condition: it must be allowed to reach a terminal exit.
	if serverTransientRejection(actions.CodeParentNotADirectory) {
		t.Error("serverTransientRejection(24584) = true, want false")
	}
}

// TestUploadNoFileIDIsPacedButNeverEscalated pins the pacing for the last arm of the H4 class.
//
// A placing turn (upload or mkdir) that SUCCEEDS while naming no node leaves the apply unable to
// advance the baseline - an empty id drops the node from the byid index and every later
// modify/move/remove misresolves - so the op stays queued. It used to do so with a bare
// fmt.Errorf: no *actions.ServerError, no synthetic code, so noteServerErr took its !ok branch and
// cleared BOTH the streak and the retry pause on every drain. For the upload arm each retry
// re-streams the WHOLE file body, because guardUploadTarget reads a server hash equal to ours as
// its own lost reply and proceeds - an unbounded full-body re-upload once per drain, forever.
//
// TRANSIENT like the two absence codes: the server did the work, only the client's bookkeeping is
// incomplete, so dropping the op would abandon a file the server already holds while the next scan
// re-derives the same push anyway. Measured 27.08.2026 (TestIntegration_FileIDPresence): this DSM
// names the node on every surface, so the arm is unreachable against it - pacing is what keeps it
// from becoming a spin the day a server does omit the id.
func TestUploadNoFileIDIsPacedButNeverEscalated(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	stubClock(e)
	err := fmt.Errorf("apply: %w", errUploadNoFileID)

	if !serverTransientRejection(uploadNoFileIDCode) {
		t.Fatal("serverTransientRejection(uploadNoFileIDCode) = false, want true: the server accepted " +
			"the create, so dropping the op abandons a file it already holds")
	}
	qo := store.QueuedOp{Seq: 57, Op: store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "fresh.txt",
	}}
	// Well past both floors of the conservative window on the count axis; a transient code must
	// not escalate on either.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		if got := e.escalateDeterministicServerFailure(qo, err); !errors.Is(got, errUploadNoFileID) {
			t.Fatalf("escalate %d = %v, want the apply error passed through unchanged", i+1, got)
		}
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0: the push landed, so it must not be dropped", dl)
	}
	if code, count, _ := e.noteServerErr(qo.Seq, err); code != uploadNoFileIDCode || count < 2 {
		t.Errorf("noteServerErr = (%d, %d), want the synthetic code with an unbroken streak", code, count)
	}
	if _, armed := e.opRetryPending(qo.Seq); !armed {
		t.Error("no retry pause armed: the arm re-streams the whole file body every drain, which is " +
			"the H4 spin with a transfer attached")
	}
	// The operator-facing rendering must name the cause, not just the number.
	if got := serverErrStreakReason(uploadNoFileIDCode); got == "" || !strings.Contains(got, "named no node") {
		t.Errorf("serverErrStreakReason(uploadNoFileIDCode) = %q, want it to name the cause", got)
	}
	// Any other failure class still clears the streak and the pause with it.
	if code, count, _ := e.noteServerErr(qo.Seq, errors.New("connection reset")); code != 0 || count != 0 {
		t.Errorf("noteServerErr(other) = (%d, %d), want the streak cleared", code, count)
	}
}
