package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundRenameOntoOccupiedDestinationKeepsBoth pins the destination precondition.
//
// rename was the one mutating verb with no precondition of its own: upload probes its target
// (H1) and a directory remove enumerates its subtree (H7), but a move went straight to the
// wire. Nothing in the protocol lets the client say "only if the destination is free", and the
// server's behaviour when it is not is unmeasured, so fakeDSM defaults to the destructive
// reading - the occupant is destroyed and the move lands on top of it. A client with no guard
// therefore loses another user's file here, through a verb that never reports a conflict.
//
// The move must instead funnel into keep-both at the destination: the server's node wins the
// canonical path, and the locally moved file survives as a copy.
func TestOutboundRenameOntoOccupiedDestinationKeepsBoth(t *testing.T) {
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	// The peer's file already stands on the path this client is about to move onto.
	d.seedFile(7, "taken.txt", []byte("the peer's file"), 1000)
	peerID := d.fileIDAt(7, "taken.txt")

	n := newDSMEngine(t, d, "sess-a", false)
	n.write(t, "Photos", "mine.txt", "my file")
	n.settle(t, 2)

	movedID := d.fileIDAt(7, "mine.txt")
	if movedID == "" {
		t.Fatalf("the local file never reached the server; server holds %v", d.paths(7))
	}

	// The user moves it onto the peer's path.
	if err := n.rename(t, "Photos", "mine.txt", "taken.txt"); err != nil {
		t.Fatalf("local move: %v", err)
	}
	n.settle(t, 4)

	// The peer's node is untouched and still holds its own content.
	if got, ok := d.content(7, "taken.txt"); !ok || string(got) != "the peer's file" {
		t.Fatalf("server taken.txt = %q present=%v, want the peer's own content: the move destroyed it",
			got, ok)
	}
	if id := d.fileIDAt(7, "taken.txt"); id != peerID {
		t.Fatalf("taken.txt now holds node %q, want the peer's %q", id, peerID)
	}
	// The user's bytes survive somewhere in the view.
	if !viewHoldsContent(t, n, "Photos", "my file") {
		t.Fatalf("the locally moved file was lost; view holds %v", viewTree(t, n, "Photos"))
	}
}

// TestOutboundRenameInconclusiveDestinationProbeDoesNotMove pins the refusal direction: a probe
// that answers neither a node nor "the path is free" leaves the move queued rather than sending
// it. The op is paced, never dropped - the guard is this client's own evidence gathering, not a
// server verdict (renameTargetUnprovedCode is a serverTransientRejection).
func TestOutboundRenameInconclusiveDestinationProbeDoesNotMove(t *testing.T) {
	c := &pushClient{queryNodeErr: errors.New("probe blew up")}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileID: "f9", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the unproved-destination refusal")
	}

	if len(c.renames) != 0 {
		t.Errorf("rename was sent %v times despite an unproved destination, want 0", len(c.renames))
	}
	if pending, _ := st.PendingLen(); pending != 1 {
		t.Errorf("queue holds %d ops, want the move still queued", pending)
	}
}

// TestOutboundRenameUnsettledPathErrorIsPaced pins that a refusal no probe can explain is
// counted and paced. Before it had a code of its own, a bare ErrPathError carried no
// *actions.ServerError, so noteServerErr cleared both the streak and the retry pause every
// drain and the op re-issued the turn forever, holding every path-overlapping op behind it.
func TestOutboundRenameUnsettledPathErrorIsPaced(t *testing.T) {
	c := &pushClient{
		renameErr:          actions.ErrPathError,
		queryNodeErrByPath: renameDestFree("new.txt"),
		// Both parents and the source node answer alive, so no arm can settle the refusal.
		queryNode:  map[string]actions.Node{"old.txt": {FileID: "f9"}},
		listResult: map[string][]actions.Node{"/": {{FileID: "f9", Name: "old.txt", Path: "old.txt"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileID: "f9", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Two drains: the first failure of a class always retries immediately by design, and the
	// geometric pause is armed from the second identical one (opbackoff.go).
	for i := 0; i < 2; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drainQueue %d succeeded, want the unsettled path error", i)
		}
		e.stuckMu.Lock()
		e.opRetryAfter = map[uint64]time.Time{}
		e.stuckMu.Unlock()
	}
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the unsettled path error")
	}

	// The op is now on a counted streak with a retry pause, which is what stops the spin.
	e.stuckMu.Lock()
	streaks := len(e.opServerErrs)
	paused := len(e.opRetryAfter)
	var code int64
	for _, s := range e.opServerErrs {
		code = s.code
	}
	e.stuckMu.Unlock()
	if streaks != 1 || code != renameUnsettledCode {
		t.Errorf("server-error streaks = %d code = %d, want one streak at renameUnsettledCode (%d)",
			streaks, code, renameUnsettledCode)
	}
	if paused != 1 {
		t.Errorf("retry pauses = %d, want the op paced", paused)
	}
	if pending, _ := st.PendingLen(); pending != 1 {
		t.Errorf("queue holds %d ops, want the move still queued", pending)
	}
}

// TestOutboundRenameLostReplyAdoptionRequiresIDMatch pins that the postcondition adopts the
// destination as our completed move ONLY on an id match. The waiver for an id-less op would
// have adopted any node standing there - the silent-divergence shape from the other side.
func TestOutboundRenameLostReplyAdoptionRequiresIDMatch(t *testing.T) {
	c := &pushClient{
		renameErr: actions.ErrPathError,
		// The destination is free at guard time, then a foreign node answers the
		// postcondition probe - the shape an id-less adoption would have swallowed.
		queryNodeHook: func(p string, call int) (actions.Node, error, bool) {
			if p == "new.txt" && call == 1 {
				return actions.Node{}, actions.ErrPathError, true
			}
			if p == "new.txt" {
				return actions.Node{FileID: "foreign"}, nil, true
			}
			return actions.Node{}, nil, false
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileID: "f9", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue succeeded: a foreign node at the destination was adopted as our move")
	}
	b, gerr := st.GetBaseline(7, "new.txt")
	if gerr != nil {
		t.Fatalf("GetBaseline: %v", gerr)
	}
	if b.State == store.StateSynced {
		t.Error("destination baseline was marked synced against a foreign node")
	}
}

// rename moves a view-relative path on disk, the way a user would.
func (n *dsmEngine) rename(t *testing.T, viewName, from, to string) error {
	t.Helper()
	dir := n.viewDir(viewName)
	return os.Rename(
		filepath.Join(dir, filepath.FromSlash(from)),
		filepath.Join(dir, filepath.FromSlash(to)),
	)
}
