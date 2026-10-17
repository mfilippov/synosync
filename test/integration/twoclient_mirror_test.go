//go:build integration

package integration

import (
	"testing"
)

// TestIntegration_TwoClientsEditVsDeleteMirror is delete-vs-edit with the two roles
// SWAPPED, and the swap matters: in `DeleteVsEdit` the destructive apply lands on the
// client that stayed online (its inbound remove meets a locally edited file, and the
// never-lose gate preserves it). Here the destructive apply is OUTBOUND - the returning
// client pushes a delete of a node the peer has since MODIFIED, and the only thing
// standing between the peer's edit and oblivion is whatever precondition the outbound
// remove carries.
//
// The peer's edit is the only copy of those bytes anywhere, so it must survive.
func TestIntegration_TwoClientsEditVsDeleteMirror(t *testing.T) {
	a, b := twoClients(t)

	const rel = "doomed-by-b.txt"
	base := []byte("edit-vs-delete: agreed baseline\n")
	seedSynced(t, a, b, rel, base)

	// B goes offline and deletes the file there; A, still online, edits it.
	b.eng.Pause()
	b.removeLocal(t, rel)

	editA := []byte("edit-vs-delete: client A's edit, made while B was deleting the file\n")
	a.writeLocal(t, rel, editA)
	a.waitServerHash(t, rel, editA)

	b.eng.Resume()

	waitConverged(t, a, b)
	requireNoLoss(t, a, b, map[string][]byte{
		"client A's edit of a file the returning peer deleted": editA,
	})
	t.Logf("converged tree: %s", describeTree(a.localTree(t, "")))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsRenameVsDelete races a rename against a delete of the same
// file. The rename PRESERVES the bytes and the delete destroys them, so the two orders
// are not symmetric and both are worth driving:
//
//   - the online client renames while the returning one deletes: the returning client's
//     outbound remove addresses a path the server no longer has (the node moved), which
//     is the "moved, not gone" exit the absence classifier exists for. Deleting the
//     renamed node instead would destroy content the peer explicitly kept.
//   - the returning client renames while the online one deletes (the /Reverse subtest):
//     the delete arrives for a node the returning client has re-homed.
//
// A delete is a legitimate outcome of either order - the point is that the two replicas
// must AGREE on it, and neither may be left holding a file the other deleted forever.
// Agreement IS the assertion here, and waitConverged is what makes it: it returns only once
// sameTree holds on both sides and fails with both trees rendered otherwise. Nothing further can
// be asserted about the content without picking a winner the race does not have - which is why
// this scenario deliberately does not call requireNoLoss, unlike its rename-vs-rename sibling
// where the bytes survive by construction.
func TestIntegration_TwoClientsRenameVsDelete(t *testing.T) {
	t.Run("OnlineRenamesReturningDeletes", func(t *testing.T) {
		a, b := twoClients(t)

		body := []byte("rename-vs-delete: the contested body\n")
		seedSynced(t, a, b, "subject.txt", body)

		b.eng.Pause()
		b.removeLocal(t, "subject.txt")

		a.renameLocal(t, "subject.txt", "kept.txt")
		waitForSync(t, a.eng, twoClientTimeout, func() bool {
			_, ok := a.queryRel(t, "kept.txt")
			return ok
		})

		b.eng.Resume()
		treeA, _ := waitConverged(t, a, b)
		// waitConverged already proved the two trees are byte-identical; a length re-check here
		// could never fail (sameTree compares lengths first) and read as an assertion that was
		// not one.
		t.Logf("converged tree: %s", describeTree(treeA))
		reportHealth(t, "client A", a)
		reportHealth(t, "client B", b)
	})

	t.Run("Reverse", func(t *testing.T) {
		a, b := twoClients(t)

		body := []byte("rename-vs-delete reverse: the contested body\n")
		seedSynced(t, a, b, "subject.txt", body)

		b.eng.Pause()
		b.renameLocal(t, "subject.txt", "kept-by-b.txt")

		a.removeLocal(t, "subject.txt")
		waitForSync(t, a.eng, twoClientTimeout, func() bool {
			_, ok := a.queryRel(t, "subject.txt")
			return !ok
		})

		b.eng.Resume()
		treeA, _ := waitConverged(t, a, b)
		// waitConverged already proved the two trees are byte-identical; a length re-check here
		// could never fail (sameTree compares lengths first) and read as an assertion that was
		// not one.
		t.Logf("converged tree: %s", describeTree(treeA))
		reportHealth(t, "client A", a)
		reportHealth(t, "client B", b)
	})
}

// TestIntegration_TwoClientsDeleteDirMirror is the subtree form of the same swap: the
// returning client deletes a whole directory it last saw empty-ish, while the online
// peer has added a file inside it. The outbound remove must not take the peer's new
// file with it.
func TestIntegration_TwoClientsDeleteDirMirror(t *testing.T) {
	a, b := twoClients(t)

	kept := []byte("delete-dir-mirror: the file that existed before\n")
	seedSynced(t, a, b, "box/kept.txt", kept)

	b.eng.Pause()
	b.removeLocal(t, "box")

	fresh := []byte("delete-dir-mirror: client A added this while B was deleting the directory\n")
	a.writeLocal(t, "box/fresh.txt", fresh)
	a.waitServerHash(t, "box/fresh.txt", fresh)

	b.eng.Resume()

	waitConverged(t, a, b)
	requireNoLoss(t, a, b, map[string][]byte{
		"client A's new file inside a directory the returning peer deleted": fresh,
	})
	t.Logf("converged tree: %s", describeTree(a.localTree(t, "")))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}
