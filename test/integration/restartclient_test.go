//go:build integration

package integration

import (
	"bytes"
	"testing"
)

// TestIntegration_TwoClientsRestartWithPendingWork stops one client's daemon outright
// - not Pause, a real shutdown - makes changes on BOTH sides while it is down, and
// brings a fresh engine up on the same state.db.
//
// The difference from the paused scenarios is what survives: a stop keeps the durable
// queue and the baselines and discards every in-memory streak, cache, wake and
// deferral. That is the state a real machine is in after a reboot or a `synosync
// daemon` restart, and it is where an op enqueued but not applied has to be replayed
// from the store rather than re-derived from memory.
//
// Both sides' changes are on DISJOINT paths, so there is no conflict to arbitrate:
// the only correct outcome is that everything survives and the two replicas converge.
func TestIntegration_TwoClientsRestartWithPendingWork(t *testing.T) {
	a, b := twoClients(t)

	base1 := []byte("restart: agreed file one\n")
	base2 := []byte("restart: agreed file two\n")
	seedSynced(t, a, b, "keep/one.txt", base1)
	seedSynced(t, a, b, "keep/two.txt", base2)

	// The daemon on client B goes away.
	b.stop()

	// While it is down: A changes its own paths, B's disk changes under it.
	editA := []byte("restart: client A edited one.txt while B's daemon was down\n")
	newA := []byte("restart: client A created this while B's daemon was down\n")
	a.writeLocal(t, "keep/one.txt", editA)
	a.writeLocal(t, "keep/from-a.txt", newA)
	a.waitServerHash(t, "keep/one.txt", editA)
	a.waitServerHash(t, "keep/from-a.txt", newA)

	editB := []byte("restart: client B's disk changed two.txt while its daemon was down\n")
	newB := []byte("restart: client B's disk gained this while its daemon was down\n")
	b.writeLocal(t, "keep/two.txt", editB)
	b.writeLocal(t, "keep/from-b.txt", newB)

	// A fresh engine over the SAME store - the restart.
	b.start(t)

	want := map[string][]byte{
		"keep/one.txt":    editA,
		"keep/two.txt":    editB,
		"keep/from-a.txt": newA,
		"keep/from-b.txt": newB,
	}
	waitBoth(t, a, b, bulkTimeout, func() bool {
		return holdsAll(a.localTree(t, ""), want) && holdsAll(b.localTree(t, ""), want)
	})
	requireHoldsAll(t, "client A", a.localTree(t, ""), want)
	requireHoldsAll(t, "client B", b.localTree(t, ""), want)

	// And the restarted client must settle, not keep re-deriving work.
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		return pendingLen(t, b) == 0
	})
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
	if n := b.eng.StuckOps(); n != 0 {
		t.Fatalf("the restarted client left %d stuck op(s)", n)
	}
	if got, ok := b.readLocal(t, "keep/one.txt"); !ok || !bytes.Equal(got, editA) {
		t.Fatalf("the restarted client never picked up the peer's edit made while it was down")
	}
}
