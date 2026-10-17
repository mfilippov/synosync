//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// bulkTimeout bounds the bulk scenarios, which move dozens of files through both
// clients rather than one.
const bulkTimeout = 180 * time.Second

// TestIntegration_TwoClientsDisjointCreates has both clients create their own files in
// the SAME directory at the same time, with neither paused. Nothing here is a conflict:
// every path belongs to exactly one writer. It is the throughput-and-scheduling case -
// two peers writing a shared directory concurrently - and the only correct outcome is
// that both clients end up holding every file.
func TestIntegration_TwoClientsDisjointCreates(t *testing.T) {
	a, b := twoClients(t)

	const n = 15
	want := map[string][]byte{}
	for i := range n {
		relA := fmt.Sprintf("shared/a-%02d.txt", i)
		relB := fmt.Sprintf("shared/b-%02d.txt", i)
		bodyA := []byte(fmt.Sprintf("disjoint: client A file %02d\n", i))
		bodyB := []byte(fmt.Sprintf("disjoint: client B file %02d\n", i))
		a.writeLocal(t, relA, bodyA)
		b.writeLocal(t, relB, bodyB)
		want[relA] = bodyA
		want[relB] = bodyB
	}

	waitBoth(t, a, b, bulkTimeout, func() bool {
		return holdsAll(a.localTree(t, ""), want) && holdsAll(b.localTree(t, ""), want)
	})

	requireHoldsAll(t, "client A", a.localTree(t, ""), want)
	requireHoldsAll(t, "client B", b.localTree(t, ""), want)
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsBulkOfflineChanges is the everyday two-machine shape: one
// client works offline for a while and comes back. Both sides change DISJOINT paths -
// no path is touched by both - so a correct sync merges the two change sets with no
// conflict at all. Every distinct body must be present on both clients afterwards, and
// every path both sides agreed to delete must be gone.
func TestIntegration_TwoClientsBulkOfflineChanges(t *testing.T) {
	a, b := twoClients(t)

	// Seed a small tree both clients agree on.
	seeded := map[string][]byte{}
	for i := range 8 {
		rel := fmt.Sprintf("bulk/file-%02d.txt", i)
		body := []byte(fmt.Sprintf("bulk: seeded file %02d\n", i))
		a.writeLocal(t, rel, body)
		seeded[rel] = body
	}
	waitBoth(t, a, b, bulkTimeout, func() bool {
		return holdsAll(a.localTree(t, ""), seeded) && holdsAll(b.localTree(t, ""), seeded)
	})

	b.eng.Pause()

	// Client A's change set: edit two, delete one, rename one, create two.
	editedA0 := []byte("bulk: client A edited file-00\n")
	editedA1 := []byte("bulk: client A edited file-01\n")
	a.writeLocal(t, "bulk/file-00.txt", editedA0)
	a.writeLocal(t, "bulk/file-01.txt", editedA1)
	a.removeLocal(t, "bulk/file-02.txt")
	a.renameLocal(t, "bulk/file-03.txt", "bulk/renamed-by-a.txt")
	newA := []byte("bulk: client A created this while B was away\n")
	a.writeLocal(t, "bulk/new-from-a.txt", newA)
	waitForSync(t, a.eng, bulkTimeout, func() bool {
		_, gone := a.queryRel(t, "bulk/file-02.txt")
		_, renamed := a.queryRel(t, "bulk/renamed-by-a.txt")
		n, created := a.queryRel(t, "bulk/new-from-a.txt")
		return !gone && renamed && created && !n.IsDir
	})

	// Client B's change set, made offline, over paths client A did not touch.
	editedB4 := []byte("bulk: client B edited file-04\n")
	editedB5 := []byte("bulk: client B edited file-05\n")
	b.writeLocal(t, "bulk/file-04.txt", editedB4)
	b.writeLocal(t, "bulk/file-05.txt", editedB5)
	b.removeLocal(t, "bulk/file-06.txt")
	newB := []byte("bulk: client B created this while offline\n")
	b.writeLocal(t, "bulk/new-from-b.txt", newB)

	b.eng.Resume()

	want := map[string][]byte{
		"bulk/file-00.txt":      editedA0,
		"bulk/file-01.txt":      editedA1,
		"bulk/renamed-by-a.txt": seeded["bulk/file-03.txt"],
		"bulk/new-from-a.txt":   newA,
		"bulk/file-04.txt":      editedB4,
		"bulk/file-05.txt":      editedB5,
		"bulk/new-from-b.txt":   newB,
		"bulk/file-07.txt":      seeded["bulk/file-07.txt"],
	}
	waitBoth(t, a, b, bulkTimeout, func() bool {
		return holdsAll(a.localTree(t, ""), want) && holdsAll(b.localTree(t, ""), want) &&
			sameTree(a.localTree(t, ""), b.localTree(t, ""))
	})

	requireHoldsAll(t, "client A", a.localTree(t, ""), want)
	requireHoldsAll(t, "client B", b.localTree(t, ""), want)
	for _, deleted := range []string{"bulk/file-02.txt", "bulk/file-06.txt"} {
		if a.localExists(t, deleted) || b.localExists(t, deleted) {
			t.Fatalf("the agreed deletion of %q did not propagate to both clients", deleted)
		}
	}
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// holdsAll reports whether tree holds every wanted path with exactly the wanted bytes.
func holdsAll(tree map[string][]byte, want map[string][]byte) bool {
	for rel, body := range want {
		got, ok := tree[rel]
		if !ok || !bytes.Equal(got, body) {
			return false
		}
	}
	return true
}

// requireHoldsAll fails naming the first path a tree is missing or holds wrongly.
func requireHoldsAll(t *testing.T, who string, tree map[string][]byte, want map[string][]byte) {
	t.Helper()
	for rel, body := range want {
		got, ok := tree[rel]
		if !ok {
			t.Fatalf("%s is missing %q: tree holds %s", who, rel, describeTree(tree))
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("%s holds the wrong bytes at %q (%d, want %d): tree holds %s",
				who, rel, len(got), len(body), describeTree(tree))
		}
	}
}
