//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// TestIntegration_TwoClientsRecreateSamePath drives create → delete → re-create of the
// same path in quick succession on one client. The stand's event log COMPACTS per node
// (docs/INTEGRATION.md), so what the peer receives is not the sequence that happened:
// the first node's rows collapse to a delete while the second node's collapse to a
// modify, and the two arrive interleaved by sync_id. The peer must end up holding the
// FINAL content - an ordering slip here shows up as the file missing, not as an error.
func TestIntegration_TwoClientsRecreateSamePath(t *testing.T) {
	a, b := twoClients(t)

	const rel = "phoenix.txt"
	first := []byte("recreate: the first incarnation\n")
	second := []byte("recreate: the SECOND incarnation, which must survive\n")

	a.writeLocal(t, rel, first)
	a.waitServerHash(t, rel, first)
	a.removeLocal(t, rel)
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, ok := a.queryRel(t, rel)
		return !ok
	})
	a.writeLocal(t, rel, second)
	a.waitServerHash(t, rel, second)

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		got, ok := b.readLocal(t, rel)
		return ok && bytes.Equal(got, second)
	})
	assertLocalContent(t, b, rel, second)
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsEmptyDirs syncs directories that never hold a file. An
// empty directory carries no content to compare, so every hash-based gate degenerates
// on it, and a delete of one has nothing to preserve - the shapes where a sync client
// most often either loses the directory or resurrects it forever.
func TestIntegration_TwoClientsEmptyDirs(t *testing.T) {
	a, b := twoClients(t)

	a.mkdirLocal(t, "hollow/inner")
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		return b.localIsDir(t, "hollow/inner")
	})
	if !b.localIsDir(t, "hollow/inner") {
		t.Fatalf("client B never materialised the empty directory")
	}

	// Deleting the leaf must propagate, and must NOT come back.
	a.removeLocal(t, "hollow/inner")
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		return !b.localExists(t, "hollow/inner")
	})
	// Hold still for a while: a resurrection loop shows up as the directory reappearing.
	settleBoth(t, a, b, 15)
	if b.localExists(t, "hollow/inner") || a.localExists(t, "hollow/inner") {
		t.Fatalf("the deleted empty directory came back (A=%v B=%v)",
			a.localExists(t, "hollow/inner"), b.localExists(t, "hollow/inner"))
	}
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsFileBecomesDir replaces a synced FILE with a DIRECTORY of
// the same name on one client - an ordinary thing for a user to do, and a shape that
// forces the peer to apply a delete and a create at one path in the right order.
func TestIntegration_TwoClientsFileBecomesDir(t *testing.T) {
	a, b := twoClients(t)

	const rel = "shape"
	body := []byte("file-becomes-dir: the file that was here first\n")
	seedSynced(t, a, b, rel, body)

	a.removeLocal(t, rel)
	a.mkdirLocal(t, rel)
	inner := []byte("file-becomes-dir: now it is a directory holding this\n")
	a.writeLocal(t, rel+"/inner.txt", inner)

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		got, ok := b.readLocal(t, rel+"/inner.txt")
		return ok && bytes.Equal(got, inner) && b.localIsDir(t, rel)
	})
	if !b.localIsDir(t, rel) {
		t.Fatalf("client B still holds %q as a file", rel)
	}
	assertLocalContent(t, b, rel+"/inner.txt", inner)
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsRenameChain renames the same file twice in quick
// succession. The peer sees two move rows for one node and must land it at the FINAL
// path - applying them out of order, or dropping the second because the first has not
// been applied yet, leaves the file at a path nobody expects.
func TestIntegration_TwoClientsRenameChain(t *testing.T) {
	a, b := twoClients(t)

	body := []byte("rename-chain: one body, three names\n")
	seedSynced(t, a, b, "one.txt", body)

	a.renameLocal(t, "one.txt", "two.txt")
	a.renameLocal(t, "two.txt", "three.txt")

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		got, ok := b.readLocal(t, "three.txt")
		return ok && bytes.Equal(got, body) &&
			!b.localExists(t, "one.txt") && !b.localExists(t, "two.txt")
	})
	assertLocalContent(t, b, "three.txt", body)
	if b.localExists(t, "one.txt") || b.localExists(t, "two.txt") {
		t.Fatalf("client B kept an intermediate name: %s", describeTree(b.localTree(t, "")))
	}
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// TestIntegration_TwoClientsUnicodeNames round-trips names that are equal under Unicode
// normalisation but different byte sequences - the macOS/Windows/Linux disagreement the
// normpath package exists for. Two outcomes are acceptable, and the choice is recorded in
// DESIGN §6: either the two spellings converge to ONE file (a normalising host coalesces
// them), or the host holds both - NTFS and ext4 do - and the client cannot sync the second,
// because the store keys a node by its NFC fold key and both names fold to one. What is NOT
// acceptable is the measured pre-fix behaviour (finding H6): the extra file sitting there
// looking synchronised while it is excluded from every pass and would never be backed up.
// So a surviving divergence must be DECLARED - a durable trace naming the collision, which
// `status` counts under Dropped. Neither outcome may churn (each client re-spelling the
// name would rewrite it forever).
func TestIntegration_TwoClientsUnicodeNames(t *testing.T) {
	a, b := twoClients(t)

	// "é" as a single code point (NFC) and as "e" + combining acute (NFD).
	const nfc = "café.txt"
	const nfd = "café.txt"
	body := []byte("unicode: one file, two spellings\n")

	a.writeLocal(t, nfc, body)
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		_, ok := b.readLocal(t, nfc)
		return ok
	})

	// Now the other client writes the OTHER spelling of the same name.
	b.writeLocal(t, nfd, body)
	settleBoth(t, a, b, 20)

	treeA, treeB := a.localTree(t, ""), b.localTree(t, "")
	t.Logf("client A tree: %s", describeTree(treeA))
	t.Logf("client B tree: %s", describeTree(treeB))
	// What the SERVER ended up holding decides whether the extra local spelling is
	// merely a duplicate or a file that is silently not backed up at all.
	ctx, cancel := actionCtx()
	children, listed, err := a.client.List(ctx, a.view.ViewID, a.viewRel(""))
	cancel()
	if err != nil || !listed {
		t.Logf("server listing unavailable (listed=%v err=%v)", listed, err)
	} else {
		names := make([]string, 0, len(children))
		for _, ch := range children {
			names = append(names, fmt.Sprintf("%q", ch.Name))
		}
		t.Logf("server holds %d entr(ies): %v", len(children), names)
	}
	if len(treeA) != len(treeB) {
		// A fold-key collision: the host kept both spellings and only one can ever own the
		// key. Tolerated, but only when the client SAYS so - an undeclared extra file is
		// the data-loss shape of H6 (a file the user believes is backed up and is not).
		requireCollisionDeclared(t, "client A", a, len(treeA) > len(treeB))
		requireCollisionDeclared(t, "client B", b, len(treeB) > len(treeA))
	}
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
}

// requireCollisionDeclared fails unless the client holding the extra spelling recorded a
// durable trace naming the fold-key collision. It is the acceptance form of finding H6: the
// divergence itself is allowed to survive (both files stay on disk, untouched, which is
// strictly safer than syncing one and dropping the other), but it must be observable.
func requireCollisionDeclared(t *testing.T, who string, sc *scenario, holdsExtra bool) {
	t.Helper()
	if !holdsExtra {
		return
	}
	dls, err := sc.st.ListDeadLetters()
	if err != nil {
		t.Fatalf("integration: %s dead letters: %v", who, err)
	}
	for _, dl := range dls {
		// Matched by TYPE, not by a substring of the operator-facing reason. store.OpFoldCollision
		// is a value no QUEUE op ever carries - the path-keyed queue cannot represent two names
		// sharing one key - which is exactly why the engine's own memo seeding keys on it
		// (collisiontrace.go). Prose is not a discriminator: rewording the reason would silently
		// turn this acceptance check into one that can never fire.
		if dl.Type == store.OpFoldCollision {
			return
		}
	}
	t.Fatalf("%s holds an extra spelling that no durable trace declares (finding H6); dead letters: %+v",
		who, dls)
}

// settleBoth nudges both engines for the given number of seconds and returns, so a
// scenario can observe a state that is supposed to STOP changing.
func settleBoth(t *testing.T, a, b *scenario, seconds int) {
	t.Helper()
	for range seconds * 2 {
		a.eng.TriggerSync()
		b.eng.TriggerSync()
		time.Sleep(500 * time.Millisecond)
	}
}
