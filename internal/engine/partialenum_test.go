package engine

// Tests for partial enumeration as (non-)authoritative evidence, for views and
// for nodes. Neither `query_user_info`'s view_list nor `list`'s file_list carries any
// completeness evidence - no total, no page cursor, no end marker - so "not in the reply" is
// indistinguishable from "past the slice the server chose to send". Both consumers used to read
// the omission as positive proof: a healthy view became STICKY absent (nothing ever clears it,
// in-process or across a restart), and a live node's absence became actionable (an acked remove
// the server never performed, a dead-lettered live download).

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestAbsentViewNeedsProbeConfirmation is the red test for partial view enumeration: an enumeration
// that omits a stored view must not suppress it while the view's own root still answers. Once the
// root probe positively reports the view gone, the suppression proceeds exactly as before.
func TestAbsentViewNeedsProbeConfirmation(t *testing.T) {
	st := newTestStore(t)
	for _, v := range []struct {
		id   uint64
		name string
		node string
	}{{7, "Photos", "r7"}, {9, "Docs", "r9"}} {
		if err := st.PutView(v.id, store.View{Name: v.name, NodeID: v.node, InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	// The enumeration is a PAGE: it lists view 7 only, though view 9 is alive and well.
	fc := &fakeClient{
		views: []actions.View{{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r7"}},
		node:  actions.Node{FileID: "r9"}, // the omitted view's root probe finds it
	}
	cap := &logCapture{}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", slog.New(cap))

	for i := 0; i < absentProbeAttempts+2; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews #%d: %v", i, err)
		}
		if e.absentViews[9] {
			t.Fatalf("view 9 suppressed on pass %d although its root probe found it (partial enumeration read as authoritative)", i)
		}
	}
	if n := cap.countWarns("treating the enumeration as incomplete"); n == 0 {
		t.Errorf("no Warn naming the incomplete enumeration; records=%d", len(cap.records))
	}
	if fc.nodeCalls == 0 {
		t.Errorf("the omitted view was never probed")
	}

	// The view is genuinely revoked now: the root probe reports the path gone. ONE path-error
	// is not enough - the sentinel conflates a real absence with a probe timeout, and this arm
	// is outside the absentProbeAttempts budget, so a single flaky turn would permanently stop
	// a healthy view the enumeration is merely paging past.
	fc.nodeErr = actions.ErrPathError
	for i := 1; i < absentRootConfirmations; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews (confirmation %d): %v", i, err)
		}
		if e.absentViews[9] {
			t.Fatalf("view 9 suppressed after %d path-error probe(s); want %d agreeing ones", i, absentRootConfirmations)
		}
	}
	if n := cap.countWarns("one path-error is not corroborated"); n != absentRootConfirmations-1 {
		t.Errorf("uncorroborated-absence warns = %d, want %d", n, absentRootConfirmations-1)
	}
	// The next agreeing probe corroborates it and the mark lands with its own Warn.
	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews (revoked): %v", err)
	}
	if !e.absentViews[9] {
		t.Fatalf("view 9 not suppressed although %d root probes confirm it is gone", absentRootConfirmations)
	}
	if n := cap.countWarns("root probes confirm it is gone"); n != 1 {
		t.Errorf("confirmed-absence warns = %d, want 1", n)
	}
	if _, ok := e.absentConfirms[9]; ok {
		t.Errorf("the confirmation counter survived the mark it produced")
	}
}

// TestAbsentViewConfirmationStreakResets pins the other half of the corroboration: a
// non-agreeing probe between two path-errors breaks the streak, so a server flapping between
// "gone" and "still here" never accumulates a suppression it did not earn.
func TestAbsentViewConfirmationStreakResets(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(9, store.View{Name: "Docs", NodeID: "r9", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	fc := &fakeClient{views: []actions.View{}, nodeErr: actions.ErrPathError}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", slog.New(&logCapture{}))

	for i := 0; i < 3*absentRootConfirmations; i++ {
		// Alternate: a path-error, then a probe that finds the root.
		if i%2 == 0 {
			fc.nodeErr = actions.ErrPathError
			fc.node = actions.Node{}
		} else {
			fc.nodeErr = nil
			fc.node = actions.Node{FileID: "r9"}
		}
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews #%d: %v", i, err)
		}
		if e.absentViews[9] {
			t.Fatalf("view 9 suppressed on pass %d although a probe found its root in between", i)
		}
	}
}

// TestAbsentViewInconclusiveProbeConvergesAnyway pins the liveness bound on the probe gate: a
// probe that can neither confirm nor deny (the revoked-view QueryNode semantics are unverified)
// must not leave a genuinely revoked view in the shared subscribe set forever, so the mark lands
// after absentProbeAttempts inconclusive answers.
func TestAbsentViewInconclusiveProbeConvergesAnyway(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(9, store.View{Name: "Docs", NodeID: "r9", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// A probe failure that is NOT the positive path-error absence: inconclusive.
	fc := &fakeClient{views: []actions.View{}, nodeErr: errors.New("query_node: server busy")}
	cap := &logCapture{}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", slog.New(cap))

	for i := 1; i < absentProbeAttempts; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews #%d: %v", i, err)
		}
		if e.absentViews[9] {
			t.Fatalf("view 9 suppressed on inconclusive probe %d, want the gate to hold until attempt %d", i, absentProbeAttempts)
		}
	}
	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews (final attempt): %v", err)
	}
	if !e.absentViews[9] {
		t.Fatalf("view 9 never suppressed after %d inconclusive probes (the inconclusive-probe spin re-opened)", absentProbeAttempts)
	}
	if n := cap.countWarns("root probe stayed inconclusive"); n != 1 {
		t.Errorf("inconclusive-convergence warns = %d, want 1", n)
	}
	if _, ok := e.absentProbes[9]; ok {
		t.Errorf("the probe counter survived the mark it produced")
	}
}

// TestCompleteEnumerationCostsNoProbes pins the happy path: when every stored view is enumerated
// nothing is probed at all, so the corroboration cannot cost a turn per refresh.
func TestCompleteEnumerationCostsNoProbes(t *testing.T) {
	st := newTestStore(t)
	for _, v := range []struct {
		id   uint64
		name string
	}{{7, "Photos"}, {9, "Docs"}} {
		if err := st.PutView(v.id, store.View{Name: v.name, NodeID: "r", InitialSyncDone: true}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	fc := &fakeClient{views: []actions.View{
		{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "r"},
		{ViewID: 9, Name: "Docs", Privilege: "rw", NodeID: "r"},
	}}
	e := New(st, &fakeTransport{client: fc}, t.TempDir(), "sess", nil)
	for i := 0; i < 3; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews #%d: %v", i, err)
		}
	}
	if fc.nodeCalls != 0 {
		t.Errorf("QueryNode calls on a complete enumeration = %d, want 0 (probe only before a sticky verdict)", fc.nodeCalls)
	}
	if len(e.absentViews) != 0 {
		t.Errorf("absentViews = %v, want none", e.absentViews)
	}
}

// TestAbsenceProversRequireNodeProbeCorroboration is the red test for the node-listing mirror: a
// parent listing that does not contain the node is not proof of absence, because the listing may be
// a page. A direct probe of the node's own path - which no pagination can hide - decides.
// (The nil absenceMemo argument is the documented seam: these subtests exercise ONE probe's
// classification, not the repetition rule layered on top - which
// absencestreak_test.go covers, through the drain passes it counts in.)
func TestAbsenceProversRequireNodeProbeCorroboration(t *testing.T) {
	ctx := context.Background()
	// The listing enumerates the parent (listed=true) but its page omits our node, which is in
	// fact alive at the very path we are checking.
	partial := func() *pushClient {
		return &pushClient{
			listResult: map[string][]actions.Node{
				"": {{FileID: "other", Name: "sibling.txt"}},
			},
			queryNode: map[string]actions.Node{"a.txt": {FileID: "fa", Name: "a.txt"}},
		}
	}

	t.Run("resolveServerNodeAbsence", func(t *testing.T) {
		pc := partial()
		gone, moved, err := resolveServerNodeAbsence(ctx, pc, nil, 7, "a.txt", "fa")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gone || moved {
			t.Errorf("gone=%v moved=%v, want both false: the node is still at the path the page omitted", gone, moved)
		}
		if len(pc.queryNodeCalls) == 0 {
			t.Errorf("absence was decided without probing the node's own path")
		}
	})
	t.Run("confirmAbsentByParentList", func(t *testing.T) {
		gone, err := confirmAbsentByParentList(ctx, partial(), nil, 7, "a.txt", "fa")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gone {
			t.Errorf("gone = true for a node the probe finds alive at its path")
		}
	})
	t.Run("nodeGoneByID", func(t *testing.T) {
		gone, err := nodeGoneByID(ctx, partial(), nil, 7, "a.txt", "fa")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gone {
			t.Errorf("gone = true for a node the probe finds alive at its path")
		}
	})
}

// TestAbsenceProversKeepPositiveOutcomeWhenProbeConfirms pins that the corroboration does not
// weaken the positive outcome the provers exist for: a genuinely deleted node - omitted from the
// listing AND reported gone by the probe - is still positively absent.
func TestAbsenceProversKeepPositiveOutcomeWhenProbeConfirms(t *testing.T) {
	ctx := context.Background()
	deleted := func() *pushClient {
		return &pushClient{
			listResult: map[string][]actions.Node{
				"": {{FileID: "other", Name: "sibling.txt"}},
			},
			queryNodeErrByPath: map[string]error{"a.txt": actions.ErrPathError},
		}
	}
	if gone, _, err := resolveServerNodeAbsence(ctx, deleted(), nil, 7, "a.txt", "fa"); err != nil || !gone {
		t.Errorf("resolveServerNodeAbsence = (%v, %v), want gone with no error", gone, err)
	}
	if gone, err := nodeGoneByID(ctx, deleted(), nil, 7, "a.txt", "fa"); err != nil || !gone {
		t.Errorf("nodeGoneByID = (%v, %v), want gone with no error", gone, err)
	}

	// A node the probe reports under a DIFFERENT id is not ours, so the listing's verdict stands
	// (the leaf-occupancy rules above already decided what that means for each caller).
	foreign := &pushClient{
		listResult: map[string][]actions.Node{"": {{FileID: "other", Name: "sibling.txt"}}},
		queryNode:  map[string]actions.Node{"a.txt": {FileID: "someone-else", Name: "a.txt"}},
	}
	if gone, err := nodeGoneByID(ctx, foreign, nil, 7, "a.txt", "fa"); err != nil || !gone {
		t.Errorf("nodeGoneByID with a foreign occupant = (%v, %v), want gone with no error", gone, err)
	}

	// A probe that answers with a node carrying NO id at all is not "some other node": nothing
	// says it is not ours, so it corroborates nothing. Reading a tolerant `file: {}` as proof
	// would ack an unperformed remove or dead-letter a live download on the bare-{} shape the
	// evidence invariant forbids - the rule viewRootProbeConfirmsAbsent and the id-less-child
	// arms of these very provers already apply.
	idless := func() *pushClient {
		return &pushClient{
			listResult: map[string][]actions.Node{"": {{FileID: "other", Name: "sibling.txt"}}},
			queryNode:  map[string]actions.Node{"a.txt": {Name: "a.txt"}},
		}
	}
	if gone, _, err := resolveServerNodeAbsence(ctx, idless(), nil, 7, "a.txt", "fa"); err == nil || gone {
		t.Errorf("resolveServerNodeAbsence = (%v, %v), want an inconclusive error on an id-less probe reply", gone, err)
	}
	if gone, err := confirmAbsentByParentList(ctx, idless(), nil, 7, "a.txt", "fa"); err == nil || gone {
		t.Errorf("confirmAbsentByParentList = (%v, %v), want an inconclusive error on an id-less probe reply", gone, err)
	}
	if gone, err := nodeGoneByID(ctx, idless(), nil, 7, "a.txt", "fa"); err == nil || gone {
		t.Errorf("nodeGoneByID = (%v, %v), want an inconclusive error on an id-less probe reply", gone, err)
	}

	// An inconclusive probe leaves the absence unproven: the op stays queued rather than acting
	// on a verdict nothing corroborates.
	flaky := &pushClient{
		listResult:         map[string][]actions.Node{"": {{FileID: "other", Name: "sibling.txt"}}},
		queryNodeErrByPath: map[string]error{"a.txt": errors.New("query_node: server busy")},
	}
	gone, _, err := resolveServerNodeAbsence(ctx, flaky, nil, 7, "a.txt", "fa")
	if err == nil {
		t.Errorf("resolveServerNodeAbsence = (%v, nil), want an inconclusive error on a failed probe", gone)
	} else if !strings.Contains(err.Error(), "inconclusive") {
		t.Errorf("error = %v, want it to name the inconclusive corroboration", err)
	}
	if gone {
		t.Errorf("gone = true on an inconclusive probe")
	}
}
