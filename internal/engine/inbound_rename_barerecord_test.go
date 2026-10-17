package engine

import (
	"context"
	"testing"
	"time"
)

// TestInboundRenameOfUntrackedSameKindSourceDoesNotPushOverServerNode pins the outbound half
// of the bare-record problem.
//
// applyRename states the invariant "an entity byid does not resolve to op.FileID is the user's
// local content", then lets a SAME-KIND occupant fall through to the overwrite-move path
// anyway. The user's file is dragged to the destination and recorded under the moved node's id
// with NEITHER hash.
//
// The INBOUND consequence of that record is pinned in inbound_rename_dataloss_test.go: a later
// modify or remove funnels the file into a _Conflict copy. The OUTBOUND consequence was not,
// and it is the one that reaches the other replica: localReconcile drives StateSynced records,
// sees "" != the on-disk hash and plans a push; guardUploadTarget then finds an empty
// ServerHash on both sides, degrades to node identity with only a Warn, and lets the user's
// unrelated bytes overwrite the server's node. compare_mtime accepts any not-older write, so
// nothing downstream refuses it either, and the node's real content is gone from both replicas.
//
// Only a stateful server model can assert this: the claim is about what the SERVER still holds.
//
// The scan is driven explicitly after the rename rather than through a whole cycle, because a
// cycle would scan BEFORE the move arrived and push the untracked file as an ordinary
// local-only create, which is a different story.
func TestInboundRenameOfUntrackedSameKindSourceDoesNotPushOverServerNode(t *testing.T) {
	ctx := context.Background()
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	d.seedFile(7, "a.txt", []byte("server X"), 1000)
	other := &dsmClient{dsm: d, session: "sess-other"}

	n := newDSMEngine(t, d, "sess-a", false)
	// The engine never materialised the server's node (its download was dropped), and the
	// user's own file stands at the path it would have landed on.
	n.write(t, "Photos", "a.txt", "user bytes")

	// The server moves its node aside.
	if err := other.Rename(ctx, 7, 0, "a.txt", "b.txt"); err != nil {
		t.Fatalf("other client Rename: %v", err)
	}
	evs, _, err := d.pullEvent(7, 0)
	if err != nil {
		t.Fatalf("pullEvent: %v", err)
	}
	if _, _, err := n.e.reconcile(7, evs, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := n.e.drainQueue(ctx); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The next scan is where a record claiming agreement would turn into a push.
	n.e.lastScan = time.Time{}
	if err := n.e.localScan(ctx); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if err := n.e.drainQueue(ctx); err != nil {
		t.Fatalf("drainQueue after scan: %v", err)
	}

	got, ok := d.content(7, "b.txt")
	if !ok || string(got) != "server X" {
		t.Fatalf("server node = %q present=%v, want its own content %q: the user's unrelated file was pushed over it",
			got, ok, "server X")
	}
	// The user's bytes are still here too - the fix records the relocation rather than
	// discarding it, it just refuses to call it agreement.
	if !viewHoldsContent(t, n, "Photos", "user bytes") {
		t.Fatalf("the user's file vanished; view now holds %v", viewTree(t, n, "Photos"))
	}
}
