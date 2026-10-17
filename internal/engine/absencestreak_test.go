package engine

// Tests for the absence streak: a listing-derived absence used to become actionable on ONE
// corroborating QueryNode answering the path-error sentinel - and errno 28672 conflates "no such
// path" with a server-side operation timeout by this client's own contract, so a briefly
// overloaded DSM produced that verdict for a perfectly healthy node. The corroboration now
// requires absentNodeConfirmations agreeing probes across drain passes, the same doctrine the
// view-level twin (absentRootConfirmations) already applied.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// newAbsenceStreakEngine builds the bare Engine the streak unit tests drive, with a hand-advanced
// clock: the corroboration weighs wall clock as well as passes, so a test must
// say how much time separated two probes rather than inheriting whatever the machine ran at.
func newAbsenceStreakEngine() (*Engine, func(time.Duration)) {
	e := &Engine{nodeAbsentConfirms: make(map[string]absenceProbeStreak)}
	return e, stubClock(e)
}

// absenceSettlePasses bounds the drain passes a corroborated absence can now cost: the sentinel
// must repeat across absentNodeConfirmations passes, and the ancestor walk pays that price once
// for the ancestor's own path and once for the node's - plus one pass of slack for the ops
// serialised behind it.
const absenceSettlePasses = 2*absentNodeConfirmations + 1

// drainSettled runs drain passes until the queue empties, bounded by absenceSettlePasses, and
// returns the LAST pass's error. A test that asserted "this converges in one drain" keeps
// asserting exactly that outcome - over the passes the corroboration now needs instead of one.
// Passes stop the moment the queue is empty, so per-turn call counts stay pinnable.
//
// The corroboration counts WALL CLOCK between agreeing probes as well as
// passes (absenceStreakMinAge): two drains of one loop iteration land seconds apart, inside a single
// window of server overload, so a pass count alone was never the duration the evidence rests on.
// Production passes that can legitimately corroborate are therefore minutes apart, and the helper
// models that by freezing the engine's clock and advancing it past the floor between passes. A test
// that settles in ONE pass never sees the clock move, so the pinned timestamps of the resolver tests
// (pinClock) stay exactly as they were.
func drainSettled(t *testing.T, e *Engine, st *store.Store) error {
	t.Helper()
	ctx := context.Background()
	base := e.now
	at := base()
	e.now = func() time.Time { return at }
	defer func() { e.now = base }()
	var err error
	for i := 0; i < absenceSettlePasses; i++ {
		err = e.drainQueue(ctx)
		n, perr := st.PendingLen()
		if perr != nil {
			t.Fatalf("PendingLen: %v", perr)
		}
		if n == 0 {
			return err
		}
		at = at.Add(absenceStreakMinAge)
	}
	return err
}

// TestOutboundRemoveAbsenceNeedsTwoAgreeingProbes is the red test: the destructive turn, the
// parent listing's postcondition and the corroborating probe all answer 28672 - the shape a
// briefly overloaded DSM produces for a file it never deleted. One agreeing probe used to ack the
// remove and drop the baseline, so the user's delete was silently lost AND the still-present
// server node fell permanently outside the replica. The first probe must now leave the op queued.
func TestOutboundRemoveAbsenceNeedsTwoAgreeingProbes(t *testing.T) {
	c := &pushClient{
		removeErr:          actions.ErrPathError,
		listResult:         map[string][]actions.Node{"docs": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"docs/f.txt": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	advance := stubClock(e)
	seedRemoveOpAt(t, st, "docs/f.txt")

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the remove left transient after ONE agreeing probe (28672 also reports a timeout)")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op left queued = %d, want 1 (one conflated sentinel is not corroboration)", n)
	}
	if _, err := st.GetBaseline(7, "docs/f.txt"); err != nil {
		t.Errorf("baseline dropped on a single path-error probe: %v", err)
	}

	// The second drain asks again, a floor's worth of wall clock later (a drain of the SAME
	// overload window is covered by TestOutboundRemoveAbsenceNeedsWallClockBetweenProbes). A
	// timeout does not repeat across minutes; a deleted node answers the sentinel every single
	// time - so the verdict is now corroborated and the remove converges.
	advance(absenceStreakMinAge)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (second pass): %v; a genuine absence must converge, not stay inconclusive forever", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued = %d after two agreeing probes, want acked", n)
	}
	if _, err := st.GetBaseline(7, "docs/f.txt"); err == nil {
		t.Error("baseline survived a remove whose absence two probes corroborated")
	}
}

// TestOutboundRemoveAbsenceStreakResetsOnAnsweringProbe pins the reset rule: the evidence is an
// UNBROKEN run of sentinels. A probe that finds the node alive in between refutes the absence
// outright and clears the streak, so the NEXT sentinel starts over rather than completing a
// verdict the server never gave twice in a row.
func TestOutboundRemoveAbsenceStreakResetsOnAnsweringProbe(t *testing.T) {
	c := &pushClient{
		removeErr:          actions.ErrPathError,
		listResult:         map[string][]actions.Node{"docs": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"docs/f.txt": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	advance := stubClock(e)
	seedRemoveOpAt(t, st, "docs/f.txt")

	// Pass 1: one sentinel - inconclusive.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue (pass 1): want the remove left transient")
	}
	// Pass 2: the node answers at its own path. The listing that omitted it was a page, and the
	// run of sentinels is broken.
	advance(absenceStreakMinAge)
	c.queryNodeErrByPath = nil
	c.queryNode = map[string]actions.Node{"docs/f.txt": {Name: "f.txt", FileID: "fa"}}
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue (pass 2): want the remove left transient; the node answered at its path")
	}
	// Pass 3: the sentinel returns, but it is the FIRST of a new run - not the second of the old.
	advance(absenceStreakMinAge)
	c.queryNode = nil
	c.queryNodeErrByPath = map[string]error{"docs/f.txt": actions.ErrPathError}
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue (pass 3): a sentinel following a refutation must restart the streak, not complete it")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op left queued = %d, want 1", n)
	}
	if _, err := st.GetBaseline(7, "docs/f.txt"); err != nil {
		t.Errorf("baseline dropped although a probe had found the node alive: %v", err)
	}
	// Pass 4 completes the new run.
	advance(absenceStreakMinAge)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (pass 4): %v; two agreeing probes must still converge", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued = %d after the new run agreed twice", n)
	}
}

// TestAbsenceStreakCountsOncePerDrainPass pins the unit the corroboration counts in. Two probes
// issued inside ONE apply are one server condition observed twice, not the repetition the verdict
// rests on, so only the first of a pass counts. It also pins the reset and the staleness rule.
func TestAbsenceStreakCountsOncePerDrainPass(t *testing.T) {
	e, advance := newAbsenceStreakEngine()

	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Fatalf("first probe = %d, want 1", got)
	}
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Errorf("second probe in the SAME pass = %d, want 1 (one condition observed twice)", got)
	}
	if got := e.noteAbsentProbe(7, "b.txt"); got != 1 {
		t.Errorf("a different path = %d, want its own streak at 1", got)
	}
	if got := e.noteAbsentProbe(9, "a.txt"); got != 1 {
		t.Errorf("the same path in a different view = %d, want its own streak at 1", got)
	}

	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != absentNodeConfirmations {
		t.Errorf("probe in the next pass = %d, want %d", got, absentNodeConfirmations)
	}

	e.clearAbsentProbe(7, "a.txt")
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Errorf("probe after a reset = %d, want the streak restarted at 1", got)
	}

	// A run goes stale by TIME, not by the passes the daemon churned through meanwhile: a probe
	// long after the last counted one is a new blip, not a repetition. Burning passes cannot do
	// this any more, and must not - a stuck op paces its own probes, so a bound in passes let
	// the op's backoff decide whether its evidence counted.
	e.beginDrainPass()
	advance(absenceStreakMaxSpan + time.Second)
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Errorf("probe after a silence longer than a run may span = %d, want a fresh streak", got)
	}
}

// TestAbsenceStreakNeedsWallClockBetweenProbes is the wall-clock-floor red test at the unit
// level: passes are not spaced in wall clock (loop.go runs a leading drain and a trailing drain per
// changed view inside ONE cycle iteration), so two probes issued a second apart - both answered by
// the same multi-second window of DSM overload - used to satisfy the corroboration in full. A probe
// inside absenceStreakMinAge must therefore report the streak unchanged while keeping the run alive,
// and the first probe past the floor must complete it: the sentinel repeating minutes apart is what
// no timeout produces.
func TestAbsenceStreakNeedsWallClockBetweenProbes(t *testing.T) {
	e, advance := newAbsenceStreakEngine()

	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Fatalf("first probe = %d, want 1", got)
	}

	// The trailing drain of the very same loop iteration: a new pass, one second later.
	e.beginDrainPass()
	advance(time.Second)
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Errorf("probe one second later in a new pass = %d, want 1 (one overload window observed twice)", got)
	}
	// However many passes the cycle churns through, time is the evidence - not the count.
	for i := 0; i < 10; i++ {
		e.beginDrainPass()
		advance(time.Second)
		if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
			t.Fatalf("probe %d inside the floor = %d, want 1 (a pass count is not a duration)", i, got)
		}
	}

	// Past the floor the sentinel has demonstrably repeated across time, and the run - kept alive
	// by the probes inside the floor rather than aged out - completes.
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != absentNodeConfirmations {
		t.Errorf("probe past the floor = %d, want the verdict %d (a genuine absence must still converge)",
			got, absentNodeConfirmations)
	}
}

// TestAbsenceStreakVerdictExpires pins the age bound on a REACHED verdict. The entry is
// deliberately not cleared when it converges (the ancestor walk re-reads its rungs), but a
// settled absence describes the node as it was: while the run still stands another client can
// re-create the path, and answering a later probe from the old evidence would let ONE conflated
// 28672 authorise a destructive apply - the hole the corroboration exists to close. markSynced
// refutes it only when THIS client records agreement there, which a failed inbound download never
// reaches. The walk still pays nothing: a verdict stays usable for the passes around it.
func TestAbsenceStreakVerdictExpires(t *testing.T) {
	e, advance := newAbsenceStreakEngine()
	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Fatalf("first probe = %d, want 1", got)
	}
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != absentNodeConfirmations {
		t.Fatalf("second probe = %d, want the verdict %d", got, absentNodeConfirmations)
	}

	// Still authoritative for the passes the ancestor walk re-reads it in.
	for i := uint64(0); i < absenceVerdictMaxAge; i++ {
		e.beginDrainPass()
	}
	if got := e.noteAbsentProbe(7, "a.txt"); got != absentNodeConfirmations {
		t.Errorf("probe %d passes after the verdict = %d, want it still settled: the walk re-reads its rungs",
			absenceVerdictMaxAge, got)
	}

	// One pass further and the verdict is history, not evidence about the node now.
	for i := uint64(0); i <= absenceVerdictMaxAge; i++ {
		e.beginDrainPass()
	}
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Errorf("probe %d passes after the verdict = %d, want a fresh run: one conflated 28672 must not ride a stale verdict",
			absenceVerdictMaxAge+1, got)
	}
}

// TestAbsenceStreakEvictsStaleEntriesOverCap pins the bound: a path whose op left the queue right
// after its first probe leaves an entry nothing else prunes, so crossing absenceStreakCap evicts
// the entries a run can no longer be continued from. Live entries survive the sweep, so the bound
// never costs a verdict in flight.
func TestAbsenceStreakEvictsStaleEntriesOverCap(t *testing.T) {
	e, advance := newAbsenceStreakEngine()
	e.beginDrainPass()
	for i := 0; i <= absenceStreakCap; i++ {
		e.noteAbsentProbe(7, "stale/"+strconv.Itoa(i))
	}
	advance(absenceStreakMaxSpan + time.Second)
	e.beginDrainPass()
	// A fresh entry in the current pass, then enough more to cross the cap again.
	e.noteAbsentProbe(7, "live.txt")
	for i := 0; i <= absenceStreakCap; i++ {
		e.noteAbsentProbe(7, "fresh/"+strconv.Itoa(i))
	}
	e.stuckMu.Lock()
	_, staleKept := e.nodeAbsentConfirms[absenceStreakKey(7, "stale/0")]
	_, liveKept := e.nodeAbsentConfirms[absenceStreakKey(7, "live.txt")]
	e.stuckMu.Unlock()
	if staleKept {
		t.Errorf("a streak entry past %v survived the eviction", absenceStreakMaxSpan)
	}
	if !liveKept {
		t.Errorf("an entry probed in the current pass was evicted; a verdict in flight would restart")
	}
}

// TestAbsenceStreakSweepSpokenInPassesWouldDropABackingOffRun is the second half of the same bug,
// and the one a fix to noteAbsentProbe alone leaves behind.
//
// Crossing absenceStreakCap takes thousands of distinct probed paths - a large subtree gone
// server-side - which is exactly the situation whose ops are backing off. If the sweep evicts by
// drain passes while the run is continued by wall clock, the sweep throws away precisely the
// entries the wall-clock bound exists to keep, and the verdict becomes unreachable again with no
// trace of why.
func TestAbsenceStreakSweepSpokenInPassesWouldDropABackingOffRun(t *testing.T) {
	e, advance := newAbsenceStreakEngine()
	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "backingoff.txt"); got != 1 {
		t.Fatalf("first probe = %d, want 1", got)
	}

	// The op backs off: its next probe is hundreds of passes away, but only seconds of wall clock.
	for i := 0; i < 1024; i++ {
		e.beginDrainPass()
	}
	advance(2 * time.Second)

	// Meanwhile a vanished subtree floods the map past the cap, triggering a sweep.
	for i := 0; i <= absenceStreakCap; i++ {
		e.noteAbsentProbe(7, "gone/"+strconv.Itoa(i))
	}

	e.stuckMu.Lock()
	_, kept := e.nodeAbsentConfirms[absenceStreakKey(7, "backingoff.txt")]
	e.stuckMu.Unlock()
	if !kept {
		t.Fatal("a run whose op is backing off was swept away by the cap; " +
			"the sweep must age entries by the same clock that continues a run")
	}

	// And it must still be able to finish: eviction is the only thing that could have stopped it.
	advance(absenceStreakMinAge)
	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "backingoff.txt"); got != absentNodeConfirmations {
		t.Fatalf("probe past the floor = %d, want the verdict %d", got, absentNodeConfirmations)
	}
}

// TestAbsenceProbeInconclusiveErrorNamesTheRule pins the shape of the inconclusive answer the
// first agreeing probe now produces: a plain error the callers already treat as "leave the op
// queued", naming the rule so an operator reading a Warn can tell it from a transport failure.
func TestAbsenceProbeInconclusiveErrorNamesTheRule(t *testing.T) {
	e, _ := newAbsenceStreakEngine()
	pc := &pushClient{
		listResult:         map[string][]actions.Node{"": {{Name: "sibling.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"a.txt": actions.ErrPathError},
	}
	gone, _, err := resolveServerNodeAbsence(context.Background(), pc, e, 7, "a.txt", "fa")
	if err == nil || gone {
		t.Fatalf("resolveServerNodeAbsence = (%v, %v), want an inconclusive error on the first agreeing probe", gone, err)
	}
	if !strings.Contains(err.Error(), "inconclusive") {
		t.Errorf("error = %v, want it to name the inconclusive corroboration", err)
	}
	if errors.Is(err, actions.ErrPathError) {
		t.Errorf("error = %v, want a plain transient rather than the sentinel itself", err)
	}
}

// TestAbsenceProbeNilMemoKeepsSingleProbeVerdict pins the documented seam: a caller with no memory
// to corroborate by repetition falls back to the single probe's verdict, exactly as a client that
// does not implement nodeProber skips the corroboration entirely. Every production caller passes
// the Engine.
func TestAbsenceProbeNilMemoKeepsSingleProbeVerdict(t *testing.T) {
	pc := &pushClient{
		listResult:         map[string][]actions.Node{"": {{Name: "sibling.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"a.txt": actions.ErrPathError},
	}
	gone, _, err := resolveServerNodeAbsence(context.Background(), pc, nil, 7, "a.txt", "fa")
	if err != nil || !gone {
		t.Fatalf("resolveServerNodeAbsence with a nil memo = (%v, %v), want the single probe's verdict", gone, err)
	}
}

// seedRemoveOpAt seeds a synced-then-deleted node at p as the drain sees it: a StatePending
// baseline (planRemove's flip) whose byid entry still names p - a genuine local delete, so the
// PathByID cross-check cannot rescue the op and the absence verdict alone decides its fate.
func seedRemoveOpAt(t *testing.T, st *store.Store, p string) {
	t.Helper()
	if err := st.PutBaseline(7, p, store.Baseline{FileID: "fa", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: p, FileID: "fa",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestAbsenceStreakClearedByAFailedProbe pins the other half of the reset rule: the streak is
// broken by ANY answer that is not the sentinel, a failure of the probe itself included. Narrowing
// the reset to "the node answered" would let a flaky link's inconclusive pass sit harmlessly
// between two sentinels and still count them as an unbroken run - a destructive verdict built from
// two observations the server never made back to back.
func TestAbsenceStreakClearedByAFailedProbe(t *testing.T) {
	ctx := context.Background()
	const p = "docs/f.txt"
	e, advance := newAbsenceStreakEngine()
	c := &pushClient{queryNodeErrByPath: map[string]error{p: actions.ErrPathError}}

	// Pass 1: one sentinel - counted, but not yet a verdict.
	e.beginDrainPass()
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); gone || err == nil {
		t.Fatalf("pass 1 = (%v, %v), want inconclusive: one sentinel is not corroboration", gone, err)
	}

	// Pass 2: the probe fails on transport. That is no verdict at all, so the run breaks.
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	c.queryNodeErrByPath = map[string]error{p: errors.New("dial tcp: connection reset")}
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); gone || err == nil {
		t.Fatalf("pass 2 = (%v, %v), want inconclusive: a failed probe proves nothing", gone, err)
	}
	if st, tracked := e.nodeAbsentConfirms[absenceStreakKey(7, p)]; tracked {
		t.Errorf("streak after a failed probe = %+v, want it cleared: the run of sentinels is broken", st)
	}

	// Pass 3: the sentinel returns as the FIRST of a new run, not the second of the old one.
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	c.queryNodeErrByPath = map[string]error{p: actions.ErrPathError}
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); gone || err == nil {
		t.Fatalf("pass 3 = (%v, %v), want inconclusive: a sentinel after a failed probe restarts the streak", gone, err)
	}

	// Pass 4 completes the new run.
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); !gone || err != nil {
		t.Fatalf("pass 4 = (%v, %v), want a corroborated absence: two agreeing probes must still converge", gone, err)
	}
}

// TestAbsenceStreakClearedByRecordedAgreement pins the reset a VERDICT cannot do for itself. A
// streak that reached its threshold is deliberately left in place - the ancestor walk needs every
// rung settled in the same pass, so clearing at the verdict would make it alternate forever - and
// the entry then stays authoritative for as long as the run may span. Within that window another
// client can re-create the very path whose absence just converged, and the next destructive op's
// single conflated 28672 would ride the old evidence straight to a verdict: exactly the "one
// timeout acks a delete the server never performed" hole the corroboration exists to close.
// markSynced is the refutation that cannot alternate with the walk: recording agreement at a path
// is proof this client just materialised a node there.
func TestAbsenceStreakClearedByRecordedAgreement(t *testing.T) {
	ctx := context.Background()
	const p = "docs/f.txt"
	c := &pushClient{queryNodeErrByPath: map[string]error{p: actions.ErrPathError}}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	advance := stubClock(e)

	// Two agreeing probes settle the absence, and the entry survives the verdict by design.
	e.beginDrainPass()
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); gone || err == nil {
		t.Fatalf("pass 1 = (%v, %v), want inconclusive", gone, err)
	}
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); !gone || err != nil {
		t.Fatalf("pass 2 = (%v, %v), want the corroborated absence", gone, err)
	}

	// The path is re-created and this client records the agreement for it.
	if err := e.markSynced(7, p, func(b *store.Baseline) {
		b.FileID = "fresh"
		b.FileType = "file"
		b.LocalHash = "h1"
		b.ServerHash = "h1"
	}); err != nil {
		t.Fatalf("markSynced: %v", err)
	}
	if st, tracked := e.nodeAbsentConfirms[absenceStreakKey(7, p)]; tracked {
		t.Fatalf("streak after recorded agreement = %+v, want it cleared: the node demonstrably occupies the path", st)
	}

	// A later sentinel is therefore the FIRST of a new run, not the second of the settled one.
	e.beginDrainPass()
	advance(absenceStreakMinAge)
	if gone, err := absenceCorroboratedByNodeProbe(ctx, c, e, 7, p, "fa"); gone || err == nil {
		t.Fatalf("post-agreement probe = (%v, %v), want inconclusive: one conflated 28672 must not ride the old streak", gone, err)
	}
}

// TestAbsenceStreakSurvivesAnOpBackingOff is the shape a real daemon produces, and the one a
// bound measured in drain passes could not survive.
//
// A stuck op backs off exponentially, so its probes grow further apart - while the daemon's
// pass counter races ahead on everything else it drains. Measured against a live stand, one
// op's consecutive probes of a single path landed at passes 488, 742, 1251, 2273, 4271, 6772 …
// 24320: gaps that double, because the gap IS the backoff. Every one exceeded a 64-pass
// ceiling, so the run restarted at 1 on each probe and the verdict was unreachable.
//
// An op's retry schedule is not evidence about the node, and must not decide whether the node's
// absence can be proved.
func TestAbsenceStreakSurvivesAnOpBackingOff(t *testing.T) {
	e, advance := newAbsenceStreakEngine()

	e.beginDrainPass()
	if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
		t.Fatalf("first probe = %d, want 1", got)
	}

	// Each probe lands many hundreds of passes after the last, and the gaps double.
	gap := uint64(256)
	for i := 0; i < 5; i++ {
		for j := uint64(0); j < gap; j++ {
			e.beginDrainPass()
		}
		advance(2 * time.Second)
		if got := e.noteAbsentProbe(7, "a.txt"); got != 1 {
			t.Fatalf("probe %d inside the floor = %d, want 1", i, got)
		}
		gap *= 2
	}

	// The sentinel has repeated for well over the floor. The run must complete.
	for j := uint64(0); j < gap; j++ {
		e.beginDrainPass()
	}
	advance(absenceStreakMinAge)
	if got := e.noteAbsentProbe(7, "a.txt"); got != absentNodeConfirmations {
		t.Fatalf("probe past the floor after a backing-off op = %d, want the verdict %d "+
			"(a genuine absence must converge however the op paced its retries)",
			got, absentNodeConfirmations)
	}
}

// TestAbsenceStreakSweepSkipsAWalkThatCannotFreeAnything pins the cost bound the wall-clock
// eviction rule made necessary.
//
// Over the cap with every entry still live, the sweep can free nothing - and a pass-based rule
// reached that state only briefly, while a ten-minute span can hold it for ten minutes. Walking
// the whole map under stuckMu every pass to delete nothing is what the once-per-pass guard was
// added to prevent, and the flood that crosses the cap is exactly when the lock is busiest.
//
// The mark must also never DELAY an eviction: it is computed over the survivors, so once they
// age out the sweep must free them on the next pass.
func TestAbsenceStreakSweepSkipsAWalkThatCannotFreeAnything(t *testing.T) {
	e, advance := newAbsenceStreakEngine()
	e.beginDrainPass()
	for i := 0; i <= absenceStreakCap; i++ {
		e.noteAbsentProbe(7, "live/"+strconv.Itoa(i))
	}

	e.stuckMu.Lock()
	idle, size := e.absenceSweepIdleUntil, len(e.nodeAbsentConfirms)
	e.stuckMu.Unlock()
	if idle.IsZero() {
		t.Fatal("an over-cap sweep that freed nothing left no idle mark; it will re-walk every pass")
	}
	if size <= absenceStreakCap {
		t.Fatalf("map size = %d, want the live entries kept over the cap %d", size, absenceStreakCap)
	}

	// A later pass inside the mark must not walk - and must not evict.
	e.beginDrainPass()
	advance(time.Second)
	e.noteAbsentProbe(7, "live/0")
	e.stuckMu.Lock()
	after := len(e.nodeAbsentConfirms)
	e.stuckMu.Unlock()
	if after < size {
		t.Fatalf("entries were evicted inside the idle mark: %d -> %d", size, after)
	}

	// Past the span the survivors are genuinely evictable, and the mark must not hold the sweep
	// back: one probe in a new pass has to free them.
	advance(absenceStreakMaxSpan + time.Second)
	e.beginDrainPass()
	e.noteAbsentProbe(7, "trigger.txt")
	e.stuckMu.Lock()
	freed := len(e.nodeAbsentConfirms)
	e.stuckMu.Unlock()
	if freed > absenceStreakCap {
		t.Fatalf("map still holds %d entries after the span elapsed; the idle mark delayed an eviction", freed)
	}
}
