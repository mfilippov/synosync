package engine

// Tests for absence corroboration on every prover: the ancestor-walk arc and every id-less caller
// of the absence provers used to bypass the absence corroboration entirely, so a paged or truncated
// GRANDPARENT listing became positive proof that a whole subtree is gone. The consumers of that
// verdict ack a delete the server may never have performed (a silently lost local delete plus a
// node permanently outside the replica) or drop a deferred remote create. Both arcs now demand a
// path-addressed QueryNode, where ANY answering node refutes the absence.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// pagedGrandparentRemoveClient scripts the coincidence that made a delete look proven: the destructive turn and
// the parent listing both come back 28672 (which conflates "no such path" with a server-side
// timeout), while the GRANDPARENT lists cleanly - but its page omits the parent, exactly as a
// delete would.
func pagedGrandparentRemoveClient() *pushClient {
	return &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"docs/sub": actions.ErrPathError},
		listResult: map[string][]actions.Node{
			"docs": {{Name: "other", FileID: "fo"}},
		},
	}
}

// seedRemoveOp seeds a synced-then-deleted node as the drain sees it: a StatePending baseline
// (planRemove's flip) whose byid entry still names op.Path - a genuine local delete, so the
// PathByID cross-check cannot rescue the op and the absence verdict alone decides its fate.
func seedRemoveOp(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.PutBaseline(7, "docs/sub/f.txt", store.Baseline{FileID: "fa", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "docs/sub/f.txt", FileID: "fa",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestAncestorWalkAbsenceNeedsAncestorProbe is the red test for the ancestor-walk half of id-less
// absence corroboration: the walk itself is id-less, so before the fix its verdict rested on the
// grandparent listing alone. A directory still answering at its own path proves that listing was a
// page, so the child's absence is not proven and the outbound remove must stay queued rather than
// being acked away.
func TestAncestorWalkAbsenceNeedsAncestorProbe(t *testing.T) {
	c := pagedGrandparentRemoveClient()
	// The leaf probe is inconclusive (the overloaded server answers 28672 to it too), but the
	// PARENT answers: "docs/sub" is alive, so the listing of "docs" that omitted it was partial.
	c.queryNodeErrByPath = map[string]error{"docs/sub/f.txt": actions.ErrPathError}
	c.queryNode = map[string]actions.Node{"docs/sub": {Name: "sub", FileID: "fd", IsDir: true}}

	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedRemoveOp(t, st)

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the remove left transient; the absence rests on a paged grandparent listing")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op left queued = %d, want 1 (an uncorroborated absence must not ack it)", n)
	}
	if _, err := st.GetBaseline(7, "docs/sub/f.txt"); err != nil {
		t.Errorf("baseline dropped on an uncorroborated absence: %v", err)
	}
	var probed bool
	for _, p := range c.queryNodeCalls {
		if p == "docs/sub" {
			probed = true
		}
	}
	if !probed {
		t.Errorf("the ancestor's own path was never probed; QueryNode calls = %q", c.queryNodeCalls)
	}
}

// TestAncestorWalkAbsenceNeedsNodeProbe is the red test for the second half: even when the walk
// positively confirms the parent gone, the verdict is still listing-derived one level up, and the
// arc returned it WITHOUT ever probing the node's own path - the very probe the absence
// corroboration introduced and DESIGN §13 claims every prover runs. A node still answering at p
// refutes it.
func TestAncestorWalkAbsenceNeedsNodeProbe(t *testing.T) {
	c := pagedGrandparentRemoveClient()
	// The parent probe agrees with the listing (gone), but the node itself answers at its own
	// path: whatever happened one level up, f.txt is still there and must not be acked away.
	c.queryNodeErrByPath = map[string]error{"docs/sub": actions.ErrPathError}
	c.queryNode = map[string]actions.Node{"docs/sub/f.txt": {Name: "f.txt", FileID: "fa"}}

	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedRemoveOp(t, st)

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the remove left transient; the node still answers at its own path")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op left queued = %d, want 1", n)
	}
	if _, err := st.GetBaseline(7, "docs/sub/f.txt"); err != nil {
		t.Errorf("baseline dropped although the node answered at its own path: %v", err)
	}
	if len(c.removes) != 1 {
		t.Errorf("removes = %q, want exactly the one failed attempt", c.removes)
	}
}

// TestAncestorWalkAbsenceStillConvergesWhenGenuine is the regression guard: a genuinely deleted
// subtree - omitted from the grandparent listing AND reported gone by both path probes - still
// converges in a single drain (acked, baseline dropped), exactly as before the fix. The
// corroboration must cost liveness nothing on the outcome the walk exists for.
func TestAncestorWalkAbsenceStillConvergesWhenGenuine(t *testing.T) {
	c := pagedGrandparentRemoveClient()
	c.queryNodeErrByPath = map[string]error{
		"docs/sub":       actions.ErrPathError,
		"docs/sub/f.txt": actions.ErrPathError,
	}

	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedRemoveOp(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (a genuinely deleted subtree must still converge within the corroboration passes)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued = %d, want acked via the corroborated ancestor walk", n)
	}
	if _, err := st.GetBaseline(7, "docs/sub/f.txt"); err == nil {
		t.Error("baseline survived a remove whose absence both probes corroborated")
	}
}

// TestAncestorWalkAbsenceInconclusiveProbeStaysQueued pins the third outcome: a probe that fails
// on transport is no verdict at all - neither the ancestor's nor the node's. Both arms leave the
// op queued rather than deciding a destructive ack on an unread answer.
func TestAncestorWalkAbsenceInconclusiveProbeStaysQueued(t *testing.T) {
	for _, tc := range []struct {
		name  string
		probe map[string]error
	}{
		{"ancestor probe fails", map[string]error{
			"docs/sub/f.txt": actions.ErrPathError,
			"docs/sub":       errors.New("query_node: server busy"),
		}},
		{"node probe fails", map[string]error{
			"docs/sub":       actions.ErrPathError,
			"docs/sub/f.txt": errors.New("query_node: server busy"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := pagedGrandparentRemoveClient()
			c.queryNodeErrByPath = tc.probe

			e, st, _ := outboundEngine(t, c, "Photos", "root1")
			seedRemoveOp(t, st)

			if err := e.drainQueue(context.Background()); err == nil {
				t.Fatal("drainQueue: want the remove left transient on an inconclusive probe")
			}
			if n, _ := st.PendingLen(); n != 1 {
				t.Errorf("remove op left queued = %d, want 1", n)
			}
			if _, err := st.GetBaseline(7, "docs/sub/f.txt"); err != nil {
				t.Errorf("baseline dropped on an inconclusive probe: %v", err)
			}
		})
	}
}

// TestIdlessAbsenceProbeCorroborates covers the prover directly on the id-less axis: the caller
// has no file id (the ancestor walk never had one; a fresh create has none yet), which used to
// SKIP the corroboration outright. The id-less question is occupancy, not identity - any node
// answering for the path refutes the absence, and a bare id-less reply is the strongest form of
// that refutation, not a reason to trust the listing.
func TestIdlessAbsenceProbeCorroborates(t *testing.T) {
	ctx := context.Background()
	listing := map[string][]actions.Node{"docs": {{Name: "other", FileID: "fo"}}}

	for _, tc := range []struct {
		name     string
		node     map[string]actions.Node
		nodeErr  map[string]error
		wantGone bool
		wantErr  bool
	}{
		{
			name:     "any node refutes",
			node:     map[string]actions.Node{"docs/sub": {Name: "sub", FileID: "fd", IsDir: true}},
			wantGone: false,
		},
		{
			name:     "an id-less reply refutes too",
			node:     map[string]actions.Node{"docs/sub": {}},
			wantGone: false,
		},
		{
			name:     "path error corroborates",
			nodeErr:  map[string]error{"docs/sub": actions.ErrPathError},
			wantGone: true,
		},
		{
			name:    "a failed probe is inconclusive",
			nodeErr: map[string]error{"docs/sub": errors.New("query_node: server busy")},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc := &pushClient{listResult: listing, queryNode: tc.node, queryNodeErrByPath: tc.nodeErr}
			gone, err := confirmAbsentByParentList(ctx, pc, nil, 7, "docs/sub", "")
			switch {
			case tc.wantErr && err == nil:
				t.Fatalf("confirmAbsentByParentList = (%v, nil), want an inconclusive error", gone)
			case tc.wantErr:
				if !strings.Contains(err.Error(), "inconclusive") {
					t.Errorf("error = %v, want it to name the inconclusive corroboration", err)
				}
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case gone != tc.wantGone:
				t.Errorf("gone = %v, want %v", gone, tc.wantGone)
			}
			if len(pc.queryNodeCalls) == 0 {
				t.Errorf("the id-less verdict was decided without probing the path")
			}
		})
	}
}

// TestVanishedUploadIdlessAbsenceNeedsCorroboration is the red test for the guard's second
// consumer: a fresh create (no server file id) whose local source vanished. Its remote-create
// download was deferred into this upload's keep-both funnel and the reconcile cursor has already
// moved past the event, so an unproven "gone" drops the create for good - the file never
// materialises locally. The walk's grandparent listing is the only evidence behind that verdict,
// and the parent answering at its own path refutes it.
func TestVanishedUploadIdlessAbsenceNeedsCorroboration(t *testing.T) {
	op := store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "docs/sub/f.txt"}

	t.Run("paged listing does not drop the deferred create", func(t *testing.T) {
		c := &pushClient{
			queryNodeErrByPath: map[string]error{"docs/sub/f.txt": actions.ErrPathError},
			listErrByPath:      map[string]error{"docs/sub": actions.ErrPathError},
			listResult:         map[string][]actions.Node{"docs": {{Name: "other", FileID: "fo"}}},
			queryNode:          map[string]actions.Node{"docs/sub": {Name: "sub", FileID: "fd", IsDir: true}},
		}
		e, _, _ := outboundEngine(t, c, "Photos", "root1")
		diverged, err := e.serverDivergedForVanishedUpload(context.Background(), op)
		if err == nil {
			t.Fatalf("serverDivergedForVanishedUpload = (%v, nil), want the op left queued on an uncorroborated absence", diverged)
		}
		if diverged {
			t.Errorf("diverged = true on an inconclusive probe; want the op simply requeued")
		}
	})

	t.Run("corroborated absence still resolves as a local delete", func(t *testing.T) {
		c := &pushClient{
			queryNodeErrByPath: map[string]error{
				"docs/sub/f.txt": actions.ErrPathError,
				"docs/sub":       actions.ErrPathError,
			},
			listErrByPath: map[string]error{"docs/sub": actions.ErrPathError},
			listResult:    map[string][]actions.Node{"docs": {{Name: "other", FileID: "fo"}}},
		}
		e, _, _ := outboundEngine(t, c, "Photos", "root1")
		advance := stubClock(e)
		// The corroboration counts agreeing probes across drain passes that are
		// also separated in wall clock, and this test drives the prover
		// directly rather than through drainQueue, so it advances both itself. A genuine absence
		// answers the sentinel every pass, so it converges.
		var (
			diverged bool
			err      error
		)
		for i := 0; i < absenceSettlePasses; i++ {
			e.beginDrainPass()
			if diverged, err = e.serverDivergedForVanishedUpload(context.Background(), op); err == nil {
				break
			}
			advance(absenceStreakMinAge)
		}
		if err != nil {
			t.Fatalf("serverDivergedForVanishedUpload: %v (a corroborated absence must still converge)", err)
		}
		if diverged {
			t.Errorf("diverged = true although nothing is on the server; want a plain local delete")
		}
	})
}
