package engine

// Two places where reconcile established node identity on weaker evidence than the doctrine
// asks for. Both share a root: the store keeps ONE path per file id, so a record written on a
// bad identity guess does not just add a wrong row - it evicts the right one.

import (
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestRelocatePendingIgnoresIDLessMove pins that two empty file ids are not an identity.
//
// The gate compared the record's id to the event's, so an id-less move event matched a
// LOCAL-ONLY baseline - the one kind of record that carries no id - and relocated it. For a
// directory that was destructive in a way the next scan completed: relocateDescendantBaselines
// carried children holding real server ids under the new prefix while their files stayed put,
// so the scan diffed the moved records as vanished and pushed outbound removes of live server
// nodes.
//
// The store already refuses this inference on its own side (putBaselineTx: an empty file_id is
// not a real identity); this pins that reconcile agrees.
func TestRelocatePendingIgnoresIDLessMove(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", discardLogger())

	// A local-only directory record: tracked on disk, never pushed, so it carries no id.
	if err := st.PutBaseline(7, "dir", store.Baseline{
		FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// A child of it that HAS reached the server.
	if err := st.PutBaseline(7, "dir/child.txt", store.Baseline{
		FileID: "srv-child", FileType: "file", ServerHash: "h", LocalHash: "h",
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}

	// An id-less move event naming the same source path.
	ev := actions.Event{SyncID: 5, Action: "move", FromPath: "dir", Path: "moved", IsDir: true}
	if err := e.relocatePending(st, 7, ev); err != nil {
		t.Fatalf("relocatePending: %v", err)
	}

	if _, err := st.GetBaseline(7, "dir"); err != nil {
		t.Errorf("the local-only record was relocated on an id-less move: %v", err)
	}
	if _, err := st.GetBaseline(7, "dir/child.txt"); err != nil {
		t.Errorf("the child's record was dragged under the new prefix while its file stayed put: %v", err)
	}
	if _, err := st.GetBaseline(7, "moved"); err == nil {
		t.Error("a record was planted at the move destination on no identity evidence")
	}
}

// TestSelfEchoOfAMovedNodeDoesNotEvictItsRecord pins the id-first resolve on the echo side.
//
// The path lookup could not see that the echoed node is tracked elsewhere, and it routinely is:
// push a modify of N at p, rename p to q locally and push that too, and the modify's echo
// arrives after the rename already relocated N's record to q. Rebuilding a record at p then
// planted a phantom where nothing exists on disk and - because the store keeps one path per file
// id - evicted the live record at q, leaving the node untracked at its real location.
func TestSelfEchoOfAMovedNodeDoesNotEvictItsRecord(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "self", discardLogger())

	// The node's live record, already relocated to its new path by the local rename.
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileID: "n1", FileType: "file", ServerHash: "h1", LocalHash: "h1",
		ServerSyncID: 10, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}

	// The echo of our own earlier modify, still naming the OLD path.
	ev := actions.Event{
		SyncID: 11, Action: "modify", FileID: "n1", Path: "old.txt",
		Hash: "h1", MTime: 200, CommitterSessionID: "self",
	}
	b, ok, err := e.selfEchoCreateBaseline(7, ev)
	if err != nil {
		t.Fatalf("selfEchoCreateBaseline: %v", err)
	}
	if ok {
		t.Fatalf("the echo produced a record %+v for a path the node no longer occupies", b)
	}

	if _, err := st.GetBaseline(7, "new.txt"); err != nil {
		t.Errorf("the node's live record at its real path was lost: %v", err)
	}
	if p, err := st.PathByID(7, "n1"); err != nil || p != "new.txt" {
		t.Errorf("byid resolves n1 to %q (err %v), want new.txt", p, err)
	}
}

// TestSelfEchoAtTheNodesOwnPathStillApplies is the negative side: the id-first check must not
// swallow the ordinary echo, where the node is tracked at exactly the path the echo names.
func TestSelfEchoAtTheNodesOwnPathStillApplies(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "self", discardLogger())

	ev := actions.Event{
		SyncID: 4, Action: "create", FileID: "n2", Path: "fresh.txt",
		Hash: "h2", Size: 3, MTime: 100, CommitterSessionID: "self",
	}
	b, ok, err := e.selfEchoCreateBaseline(7, ev)
	if err != nil {
		t.Fatalf("selfEchoCreateBaseline: %v", err)
	}
	if !ok {
		t.Fatal("an echo for an untracked path was skipped; nothing else records our own create")
	}
	if b.FileID != "n2" || b.State != store.StateSynced {
		t.Errorf("baseline = %+v, want the echoed node recorded synced", b)
	}
}
