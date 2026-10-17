//go:build integration

package integration

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_RemoveDirectorySemantics measures what `remove` does to a directory
// that is NOT empty.
//
// The client's outbound-delete model USED to assume the verb is single-node - the harness
// deleted trees child-first (removeTreeRemote) on the reading that "the server rejects
// removing a non-empty directory" - while the engine's never-lose gates can only reason
// about the descendants IT knows about, the ones in its own baseline. Since the server
// deletes recursively, a client that deletes a directory it last saw with two files also
// deletes the third file a PEER added meanwhile, which no gate on this side can see.
// TestIntegration_TwoClientsDeleteDirMirror shows exactly that outcome; this test isolates
// the server behaviour behind it, with no engine involved, and is what
// verifyServerSubtreeAgrees exists to defend against.
func TestIntegration_RemoveDirectorySemantics(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	dir := sc.remoteMkdirAll(t, "removedir")
	body := []byte("remove-dir: a child the deleter never enumerated\n")
	child := serverPut(t, sc.client, sc.view.RootNodeID, dir.FileID, "child.txt", body)
	t.Logf("directory %s holds child %s", dir.FileID, child.FileID)

	ctx, cancel := actionCtx()
	err := sc.client.Remove(ctx, sc.view.ViewID, sc.viewRel("removedir"))
	cancel()

	dirGone := func() bool {
		_, ok := serverQuery(t, sc.client, sc.view.ViewID, sc.viewRel("removedir"))
		return !ok
	}
	childGone := func() bool {
		_, ok := serverQuery(t, sc.client, sc.view.ViewID, sc.viewRel("removedir/child.txt"))
		return !ok
	}

	// The measurement is no longer exploratory: the recursive semantics are now a load-bearing
	// INVARIANT of the client (docs/INVARIANTS.md; verifyServerSubtreeAgrees enumerates the subtree on the
	// server before every directory delete, and the unit fakes model a recursive `remove`). So the
	// answer is asserted rather than logged. A DSM release that restored a non-recursive `remove`
	// must fail this test loudly - otherwise the fakes would model a server that no longer exists
	// and the precondition would be validated against fiction, which is "a fake must never be
	// kinder than the measured server" inverted.
	gone, chGone := dirGone(), childGone()
	switch {
	case err == nil && gone && chGone:
		t.Logf("MEASURED (as expected): `remove` of a NON-EMPTY directory SUCCEEDS and deletes the child " +
			"with it - the verb is RECURSIVE. A client deleting a directory therefore destroys every node " +
			"a peer added inside it since that client last looked, and no client-side never-lose gate can " +
			"see those nodes (see TestIntegration_TwoClientsDeleteDirMirror)")
	case err == nil && gone && !chGone:
		t.Fatalf("integration: `remove` deleted the directory but LEFT the child addressable - an orphan; " +
			"the client's subtree precondition assumes the child goes with the directory")
	case err != nil && errors.Is(err, actions.ErrPathError):
		t.Fatalf("integration: `remove` of a non-empty directory was REFUSED with a path error (40961). " +
			"The server is no longer recursive, so verifyServerSubtreeAgrees' whole premise - and every " +
			"unit fake built on it - is stale; re-derive the semantics before trusting the preconditions")
	case err != nil:
		t.Fatalf("integration: `remove` of a non-empty directory was REFUSED with %v; the recursive "+
			"semantics the client depends on no longer hold", err)
	default:
		t.Fatalf("integration: remove returned %v; dirGone=%v childGone=%v - the recursive semantics "+
			"the client depends on could not be confirmed", err, gone, chGone)
	}
}
