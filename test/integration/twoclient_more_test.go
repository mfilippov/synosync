//go:build integration

package integration

import (
	"bytes"

	"os"

	"testing"
	"time"
)

// bigTwoClientTimeout bounds the large-file two-client race, which moves megabytes
// through the resume path on both clients before it can converge.
const bigTwoClientTimeout = 240 * time.Second

// clashTimeout bounds the file-vs-directory clash, which cannot resolve until the blocking push
// is dead-lettered. That used to take the unmapped-code window (unmappedServerErrMinAge, 5
// minutes), which is what the old eight-minute budget was sized for; since finding H4 typed 24581
// as a MEASURED name refusal it takes the shortened nameRejectionDrains/nameRejectionMinAge
// confirmation (3 drains / 10 seconds) instead. The budget is still generous by two orders of
// magnitude - the remaining cost is convergence, not the window - so a scenario that takes
// minutes now signals a regression rather than a wait.
const clashTimeout = 3 * time.Minute

// reportHealth logs the operability counters both engines expose after a scenario -
// stuck ops and dead letters. A scenario that converges but leaves an op retrying
// forever, or silently drops one to a dead letter, has not really succeeded; this is
// what makes that visible without failing a scenario whose convergence is the point.
// It never fails the test itself: it runs from a t.Cleanup registered by twoClients, and its
// whole job is to make a failure diagnosable - a Fatalf from a cleanup goroutine, or a panic,
// replaces the diagnosis with a second, unrelated failure. The nil-engine guard is the same rule:
// scenario.stop() clears sc.eng to model a killed daemon, so a scenario that fails between stop()
// and start() (TestIntegration_TwoClientsRestartWithPendingWork has four Fatalf-capable calls in
// exactly that window) would otherwise crash the whole test binary on sc.eng.StuckOps().
func reportHealth(t *testing.T, who string, sc *scenario) {
	t.Helper()
	stuck := -1
	if sc.eng != nil {
		stuck = sc.eng.StuckOps()
	}
	pending, err := sc.st.PendingLen()
	if err != nil {
		t.Logf("integration: %s pending queue length: %v", who, err)
		pending = -1
	}
	dls, err := sc.st.ListDeadLetters()
	if err != nil {
		t.Logf("integration: %s dead letters: %v", who, err)
	}
	t.Logf("%s: stuck=%d (-1: engine stopped) pending=%d dead-letters=%d", who, stuck, pending, len(dls))
	for _, dl := range dls {
		t.Logf("%s: dead-letter %s %s %q: %s", who, dl.Type, dl.Direction, dl.Path, dl.Reason)
	}
	if ops, err := sc.st.PendingOpsN(16); err == nil {
		for _, op := range ops {
			t.Logf("%s: PENDING seq=%d %s %s %q new=%q dir=%v fid=%q", who,
				op.Seq, op.Type, op.Direction, op.Path, op.NewPath, op.IsDir, op.FileID)
		}
	}
}

// requireNoLoss fails unless every named body still exists somewhere in both clients'
// trees. It is the never-lose assertion in its weakest, least brittle form: it does
// not care WHERE the bytes ended up (canonical path, _Conflict copy, renamed sibling),
// only that no user content was destroyed.
func requireNoLoss(t *testing.T, a, b *scenario, bodies map[string][]byte) {
	t.Helper()
	treeA, treeB := a.localTree(t, ""), b.localTree(t, "")
	for what, body := range bodies {
		requireSurvives(t, "client A", treeA, body, what)
		requireSurvives(t, "client B", treeB, body, what)
	}
}

// TestIntegration_TwoClientsRenameVsRename races two renames of the same file to
// different names. Neither side loses content by construction (a rename moves bytes
// rather than replacing them), so this is a convergence test: the two clients must
// end up with the same tree, and the file's content must exist - once is ideal, twice
// is churn, zero is loss.
func TestIntegration_TwoClientsRenameVsRename(t *testing.T) {
	a, b := twoClients(t)

	const src = "subject.txt"
	body := []byte("rename-vs-rename: the only content in this scenario\n")
	seedSynced(t, a, b, src, body)

	b.eng.Pause()
	a.renameLocal(t, src, "from-a.txt")
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, ok := a.queryRel(t, "from-a.txt")
		return ok
	})
	b.renameLocal(t, src, "from-b.txt")
	b.eng.Resume()

	treeA, treeB := waitConverged(t, a, b)
	requireSurvives(t, "client A", treeA, body, "the renamed file's content")
	requireSurvives(t, "client B", treeB, body, "the renamed file's content")
	t.Logf("converged tree: %s", describeTree(treeA))
	if len(treeA) > 1 {
		t.Logf("NOTE: the double rename left %d copies (churn, not loss)", len(treeA))
	}
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsDeleteDirVsCreateInside deletes a whole directory on one
// client while the other, offline, creates a NEW file inside it. The delete is
// destructive against content the deleting client never saw, so DESIGN §6 requires the
// new file to survive - this is the subtree form of delete-vs-edit.
func TestIntegration_TwoClientsDeleteDirVsCreateInside(t *testing.T) {
	a, b := twoClients(t)

	kept := []byte("delete-dir-vs-create: the file that existed before\n")
	seedSynced(t, a, b, "tree/kept.txt", kept)

	b.eng.Pause()
	fresh := []byte("delete-dir-vs-create: created inside the doomed directory, must survive\n")
	b.writeLocal(t, "tree/fresh.txt", fresh)

	a.removeLocal(t, "tree")
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, ok := a.queryRel(t, "tree")
		return !ok
	})

	b.eng.Resume()
	waitConverged(t, a, b)
	requireNoLoss(t, a, b, map[string][]byte{"the file created inside a concurrently deleted directory": fresh})
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsFileDirClash has one client create a FILE at a path where
// the other creates a DIRECTORY. The two shapes cannot coexist, so one side's inbound
// apply is blocked by an occupant of the wrong kind - the executor's
// existsBlocksForever / dirnotempty classification decides whether that op retries,
// dead-letters, or wedges the queue. Whatever it decides, neither body may be lost and
// neither queue may wedge.
func TestIntegration_TwoClientsFileDirClash(t *testing.T) {
	a, b := twoClients(t)

	fileBody := []byte("file-dir-clash: client A made this a FILE\n")
	innerBody := []byte("file-dir-clash: client B made it a DIRECTORY holding this\n")

	b.eng.Pause()
	a.writeLocal(t, "thing", fileBody)
	a.waitServerHash(t, "thing", fileBody)
	b.writeLocal(t, "thing/inner.txt", innerBody)
	b.eng.Resume()

	// Convergence is not guaranteed to be quick here (one side must give way), so
	// report rather than assert it, and hold the hard line at "nothing was lost".
	//
	// The budget is still longer than every other scenario's: client B's outbound mkdir over the
	// peer's FILE draws errno 24581 and blocks the inbound download of that same path until it is
	// dead-lettered. Since finding H4 that takes the shortened name-refusal confirmation
	// (nameRejectionDrains drains AND nameRejectionMinAge of wall clock, ~10 seconds) rather than
	// the five-minute unknown-code window, so what is being measured here is the healing, not the
	// wait.
	waitBoth(t, a, b, clashTimeout, func() bool {
		_, okA := holdsContent(a.localTree(t, ""), innerBody)
		_, okB := holdsContent(b.localTree(t, ""), fileBody)
		return okA && okB
	})
	requireNoLoss(t, a, b, map[string][]byte{
		"client A's file at the clashing path":          fileBody,
		"client B's file inside the clashing directory": innerBody,
	})
	t.Logf("client A tree: %s", describeTree(a.localTree(t, "")))
	t.Logf("client B tree: %s", describeTree(b.localTree(t, "")))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsMoveDirVsEditInside renames a directory on one client while
// the other, offline, edits a file inside it under the OLD path. The edit must survive
// the relocation - this is the ancestor-rename form of rename-vs-edit, and it exercises
// the baseline relocation (relocatePending) that outbound_upload.go's relocation
// redirect exists for.
func TestIntegration_TwoClientsMoveDirVsEditInside(t *testing.T) {
	a, b := twoClients(t)

	base := []byte("move-dir-vs-edit: agreed baseline\n")
	seedSynced(t, a, b, "box/doc.txt", base)

	b.eng.Pause()
	edit := []byte("move-dir-vs-edit: client B's edit under the pre-move path\n")
	b.writeLocal(t, "box/doc.txt", edit)

	a.renameLocal(t, "box", "crate")
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, oldOK := a.queryRel(t, "box")
		_, newOK := a.queryRel(t, "crate/doc.txt")
		return !oldOK && newOK
	})

	b.eng.Resume()
	waitConverged(t, a, b)
	requireNoLoss(t, a, b, map[string][]byte{"client B's edit under a concurrently moved directory": edit})
	t.Logf("converged tree: %s", describeTree(a.localTree(t, "")))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsOlderMTimeEdit is the concurrent-edit race with the mtime
// comparison pointed the OTHER way: client B's edit carries an OLDER timestamp than the
// version client A pushed (a restore from backup, a file copied with -p, a clock skew).
// The server rejects that upload with 24579, which is the exact rejection the keep-both
// funnel is built on - so this is the branch where the protection actually engages, and
// both bodies must survive on both clients.
func TestIntegration_TwoClientsOlderMTimeEdit(t *testing.T) {
	a, b := twoClients(t)

	const rel = "restored.txt"
	base := []byte("older-mtime: agreed baseline\n")
	seedSynced(t, a, b, rel, base)

	b.eng.Pause()
	editA := []byte("older-mtime: client A's edit, pushed with a current timestamp\n")
	editB := []byte("older-mtime: client B's OLDER edit, restored from a backup\n")

	a.writeLocal(t, rel, editA)
	a.waitServerHash(t, rel, editA)

	b.writeLocal(t, rel, editB)
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(b.localAbs(t, rel), old, old); err != nil {
		t.Fatalf("integration: backdate client B's file: %v", err)
	}
	b.eng.Resume()

	waitConverged(t, a, b)
	requireNoLoss(t, a, b, map[string][]byte{
		"client A's newer edit":     editA,
		"client B's backdated edit": editB,
	})
	t.Logf("converged tree: %s", describeTree(a.localTree(t, "")))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsDeleteVsDelete has both clients delete the same file while
// neither can see the other. There is nothing to preserve - the interesting question is
// whether the second, now-redundant delete converges quietly or leaves an op retrying
// against a node that is already gone.
func TestIntegration_TwoClientsDeleteVsDelete(t *testing.T) {
	a, b := twoClients(t)

	const rel = "doomed.txt"
	body := []byte("delete-vs-delete: agreed baseline\n")
	seedSynced(t, a, b, rel, body)

	b.eng.Pause()
	a.removeLocal(t, rel)
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, ok := a.queryRel(t, rel)
		return !ok
	})
	b.removeLocal(t, rel)
	b.eng.Resume()

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		return !a.localExists(t, rel) && !b.localExists(t, rel)
	})
	// Both queues must drain: a redundant delete that keeps retrying is a wedge, and it
	// would block every later op serialised on that path.
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		return pendingLen(t, a) == 0 && pendingLen(t, b) == 0
	})
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
	if n := b.eng.StuckOps(); n != 0 {
		t.Fatalf("client B left %d stuck op(s) after a redundant delete", n)
	}
}

// TestIntegration_TwoClientsLargeFileConcurrentEdit is the concurrent-edit race above
// the upload resume threshold, so both pushes travel the resume_upload path rather than
// the single-frame one. The conflict handling is shared, but the resume path reaches it
// through a different reply shape, so the outcome is worth measuring separately.
func TestIntegration_TwoClientsLargeFileConcurrentEdit(t *testing.T) {
	a, b := twoClients(t)

	const rel = "big.bin"
	base := bigBody(5<<20, 'a')
	a.writeLocal(t, rel, base)
	waitBoth(t, a, b, bigTwoClientTimeout, func() bool {
		ga, oka := a.readLocal(t, rel)
		gb, okb := b.readLocal(t, rel)
		return oka && okb && bytes.Equal(ga, base) && bytes.Equal(gb, base)
	})

	b.eng.Pause()
	editA := bigBody(5<<20, 'b')
	editB := bigBody(5<<20, 'c')
	a.writeLocal(t, rel, editA)
	a.waitServerHash(t, rel, editA)
	b.writeLocal(t, rel, editB)
	b.eng.Resume()

	waitBoth(t, a, b, bigTwoClientTimeout, func() bool {
		return sameTree(a.localTree(t, ""), b.localTree(t, ""))
	})
	requireNoLoss(t, a, b, map[string][]byte{
		"client A's large-file edit": editA,
		"client B's large-file edit": editB,
	})
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// bigBody builds a deterministic body of n bytes that is not uniformly one byte (so a
// truncated or mis-offset transfer cannot accidentally compare equal) and differs per
// seed character.
func bigBody(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = seed + byte(i%17)
	}
	return out
}

// TestIntegration_TwoClientsCaseClash has the two clients create names that differ only
// in case. The stand is measured to accept a case-only RENAME but to refuse two
// fold-equal SIBLINGS with errno 24580 (docs/INTEGRATION.md), and a Windows client's
// own filesystem cannot hold both either - so this is where a fold collision meets a
// server that will not take it. Neither body may be lost and neither queue may wedge;
// the shape of the resolution is recorded rather than asserted.
func TestIntegration_TwoClientsCaseClash(t *testing.T) {
	a, b := twoClients(t)

	upper := []byte("case-clash: client A wrote Notes.txt\n")
	lower := []byte("case-clash: client B wrote notes.txt\n")

	b.eng.Pause()
	a.writeLocal(t, "Notes.txt", upper)
	a.waitServerHash(t, "Notes.txt", upper)
	b.writeLocal(t, "notes.txt", lower)
	b.eng.Resume()

	// Give both loops room to reach a steady state, then record it.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		a.eng.TriggerSync()
		b.eng.TriggerSync()
		time.Sleep(500 * time.Millisecond)
	}
	treeA, treeB := a.localTree(t, ""), b.localTree(t, "")
	t.Logf("client A tree: %s", describeTree(treeA))
	t.Logf("client B tree: %s", describeTree(treeB))
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)

	// The hard line: client B's own body must still exist on client B. Losing the peer's
	// body to a server that refuses the name is a documented dead-letter; losing your OWN
	// local file to it is not.
	requireSurvives(t, "client B", treeB, lower, "its own case-clashing file")
	requireSurvives(t, "client A", treeA, upper, "its own case-clashing file")
	if _, ok := holdsContent(treeB, upper); !ok {
		t.Logf("NOTE: client B never received the peer's fold-equal sibling (expected: the host FS cannot hold both)")
	}
}
