//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// twoClientTimeout bounds one convergence step of a two-client scenario. It is
// larger than the single-client budgets because a change has to travel local→server
// on one client AND server→local on the other, each through its own long-poll cycle.
const twoClientTimeout = 120 * time.Second

// twoClients brings up TWO independent sync clients - separate devices, sessions,
// sync roots and state.db files - on the SAME view and the SAME scratch base, both
// seeded at the event log's current tail (no cold start). It is the fixture for the
// scenarios below, which are the ones a single-client harness structurally cannot
// reach: two peers that both act while neither has seen the other's change yet.
//
// The returned pair is (a, b); a is the ordinary scenario (its client and teardown
// own the scratch base), b is the peer.
func twoClients(t *testing.T) (a, b *scenario) {
	t.Helper()
	a = setupScenario(t)
	b = a.spawnPeer(t)
	// Always dump both clients' operability counters when the scenario ends. A
	// two-client failure is usually a DROPPED or wedged op, and the dead-letter trace
	// naming it is the difference between "did not converge" and a diagnosis; a t.Cleanup
	// registered here runs before the engines are torn down (LIFO) and covers the
	// failure paths a trailing reportHealth call never reaches.
	t.Cleanup(func() {
		if t.Failed() {
			reportHealth(t, "client A", a)
			reportHealth(t, "client B", b)
		}
	})
	return a, b
}

// spawnPeer starts a second full sync client against the scenario's view and scratch
// base: its own paired device (ensurePeerPaired), its own temp sync root and store,
// its own engine over its own pool. Its session id differs from the first engine's,
// so each peer sees the other's commits as ordinary remote events rather than
// self-echoes (DESIGN §6) - which is exactly what a second real installation is.
//
// The peer skips cold start at the log's current tail like setupScenario does, so it
// starts EMPTY and learns the scratch tree from the events the first client commits
// after this point. Spawn it BEFORE placing any content, or the peer's cursor starts
// past that content and it never sees it.
//
// The returned scenario shares the view, scratch base and mutation client with its
// parent (the parent's t.Cleanup owns the server-side teardown); everything else -
// local paths, store, baselines, engine - is the peer's own.
func (sc *scenario) spawnPeer(t *testing.T) *scenario {
	t.Helper()
	pp := ensurePeerPaired(t, sc.h)

	syncRoot := tempSyncRoot(t)
	paths := tempPaths(t)
	conf := sc.h.baseConfig(syncRoot)
	conf.DeviceUUID = pp.deviceUUID
	conf.Server.DsID = pp.dsID

	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("integration: open peer store %s: %v", paths.StateDB(), err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.PutConfig(conf); err != nil {
		t.Fatalf("integration: put peer config: %v", err)
	}
	if err := st.PutView(sc.viewID, store.View{
		NodeID:     sc.view.NodeID,
		RootNodeID: sc.view.RootNodeID,
		Name:       sc.view.Name,
		Privilege:  sc.view.Privilege,
	}); err != nil {
		t.Fatalf("integration: seed peer view %d: %v", sc.viewID, err)
	}

	// The peer ALIASES the scenario's server-mutation client rather than dialling its own:
	// both commit under the same mutator pairing, so a second session would only cost the NAS
	// another connection. The alias is why closeClient is deliberately left nil here - the
	// session belongs to sc, and a redialClient on the peer must replace its own pointer
	// without closing sc's. It is also the reason a redial on ONE side does not heal the
	// other's pointer: a scenario that breaks the shared client must redial on the side it
	// then uses.
	peer := &scenario{
		h: sc.h, conf: conf, st: st, client: sc.client,
		connector: pp.connector, session: pp.session,
		deviceUUID: pp.deviceUUID, dsID: pp.dsID,
		view: sc.view, viewID: sc.viewID, base: sc.base,
		label: "B",
	}
	peer.skipColdStart(t)
	peer.start(t)
	return peer
}

// pollEvery is how often the two-client waits re-evaluate their predicate. It is a
// full second rather than the single-client harness's 250 ms because these predicates
// READ the replicas' files, and on Windows an open reader blocks the engine's
// staging-temp promote with ERROR_SHARING_VIOLATION ("used by another process") -
// measured: a 250 ms content poll over a tree of eight files starved the promote long
// enough to fail a 3-minute budget, then the same scenario passed in 1.4 s once the
// polling stopped. That is the harness fighting the engine, not a defect under test.
const pollEvery = time.Second

// waitBoth polls predicate until it holds or timeout elapses, nudging BOTH engines
// each round. A two-client convergence needs both loops to run: the change has to be
// pushed by one and pulled by the other, and TriggerSync on one alone leaves the
// other waiting out its long poll.
func waitBoth(t *testing.T, a, b *scenario, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		a.eng.TriggerSync()
		b.eng.TriggerSync()
		if predicate() {
			return
		}
		if time.Now().After(deadline) {
			// Always render both trees: every two-client condition is about what the two
			// replicas hold, and a bare "condition not met" costs a whole re-run to diagnose.
			t.Fatalf("integration: two-client condition not met within %s\n  client A: %s\n  client B: %s",
				timeout, describeTree(a.localTree(t, "")), describeTree(b.localTree(t, "")))
		}
		time.Sleep(pollEvery)
	}
}

// localTree reads every regular file under a scratch-relative directory into a map of
// scratch-relative slash path → contents. A missing directory reads as an empty tree.
// It is how the scenarios below assert "nothing was lost" without depending on the
// name the keep-both resolver happens to choose for a preserved copy.
func (sc *scenario) localTree(t *testing.T, rel string) map[string][]byte {
	t.Helper()
	root := sc.localAbs(t, rel)
	out := map[string][]byte{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		// Skip the engine's own staging temps (".<base>.dl-<digits>"). They are
		// in-flight state, not replica content - a download streaming into one when the
		// snapshot is taken would otherwise read as a file the peer is missing, and the
		// scenario would "fail" on the engine doing its job. watcher/filter.go drops the
		// same names for the same reason.
		if name := d.Name(); strings.HasPrefix(name, ".") && strings.Contains(name, ".dl-") {
			return nil
		}
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		key, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		out[filepath.ToSlash(key)] = body
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("integration: walk local tree %q: %v", rel, err)
	}
	return out
}

// holdsContent reports the name under which tree holds exactly want, if any.
func holdsContent(tree map[string][]byte, want []byte) (string, bool) {
	for name, body := range tree {
		if bytes.Equal(body, want) {
			return name, true
		}
	}
	return "", false
}

// describeTree renders a tree as "name(len)" pairs in a stable order, for failure
// messages: the interesting fact about a lost or duplicated file is its size, and a
// keep-both copy's name is generated, so the listing is what makes a failure readable.
func describeTree(tree map[string][]byte) string {
	names := make([]string, 0, len(tree))
	for n := range tree {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s(%dB)", n, len(tree[n])))
	}
	if len(parts) == 0 {
		return "<empty>"
	}
	return strings.Join(parts, " ")
}

// sameTree reports whether two trees hold the same names with the same bytes.
func sameTree(x, y map[string][]byte) bool {
	if len(x) != len(y) {
		return false
	}
	for name, body := range x {
		other, ok := y[name]
		if !ok || !bytes.Equal(body, other) {
			return false
		}
	}
	return true
}

// pendingLen reports the client's queued-op count, failing the test if the store cannot answer.
// The bare `n, _ := st.PendingLen()` this replaces reads a backend failure as the SUCCESS value
// (0), which is the difference between "the queue drained" and "the queue could not be read" -
// and inside a wait predicate it makes the wait return on its very first poll.
func pendingLen(t *testing.T, sc *scenario) int {
	t.Helper()
	n, err := sc.st.PendingLen()
	if err != nil {
		t.Fatalf("integration: %s pending queue length: %v", sc.label, err)
	}
	return n
}

// requireSurvives fails unless tree holds want somewhere, naming what it did hold.
func requireSurvives(t *testing.T, who string, tree map[string][]byte, want []byte, what string) {
	t.Helper()
	if _, ok := holdsContent(tree, want); !ok {
		t.Fatalf("%s lost %s (%d bytes): tree holds %s", who, what, len(want), describeTree(tree))
	}
}

// convergedSettlePolls is how many CONSECUTIVE agreeing polls waitConverged demands before it
// calls the two replicas converged. One is not enough, and the cheapest counter-example is the
// one every rename/delete race passes through: sameTree short-circuits on equal lengths, so two
// EMPTY trees agree trivially, and mid-propagation both sides are routinely empty for a moment -
// after the delete has applied on both and before the surviving node has been downloaded back.
// A single-poll check returns success there, on a state that is not the outcome, and every
// scenario whose only assertion is convergence would then pass on transient agreement.
const convergedSettlePolls = 3

// waitConverged waits until both clients hold the same tree under the scratch base for
// convergedSettlePolls consecutive polls and reports it, failing on timeout with both sides
// rendered. Any disagreement restarts the count.
func waitConverged(t *testing.T, a, b *scenario) (treeA, treeB map[string][]byte) {
	t.Helper()
	deadline := time.Now().Add(twoClientTimeout)
	agreed := 0
	for {
		a.eng.TriggerSync()
		b.eng.TriggerSync()
		ta, tb := a.localTree(t, ""), b.localTree(t, "")
		if sameTree(ta, tb) {
			agreed++
			if agreed >= convergedSettlePolls {
				return ta, tb
			}
			time.Sleep(pollEvery)
			continue
		}
		agreed = 0
		if time.Now().After(deadline) {
			// Render BOTH sides: a convergence failure is only diagnosable from what each
			// client actually holds, and the generic waitBoth message says nothing.
			t.Fatalf("integration: the two clients did not converge within %s\n  client A: %s\n  client B: %s",
				twoClientTimeout, describeTree(ta), describeTree(tb))
		}
		time.Sleep(pollEvery)
	}
}

// seedSynced writes a file on client a and waits until BOTH clients hold it, so a
// scenario starts from a state where both sides agree on the same baseline. That
// agreement is what makes the divergence the scenario then creates a genuine
// concurrent edit rather than a first-sight create.
func seedSynced(t *testing.T, a, b *scenario, rel string, content []byte) {
	t.Helper()
	a.writeLocal(t, rel, content)
	waitBoth(t, a, b, twoClientTimeout, func() bool {
		ga, oka := a.readLocal(t, rel)
		gb, okb := b.readLocal(t, rel)
		return oka && okb && bytes.Equal(ga, content) && bytes.Equal(gb, content)
	})
}

// TestIntegration_TwoClientsConcurrentEdit is the canonical two-writer race: both
// clients hold the same synced file, then each edits it while the other's change is
// still invisible (client b is paused, which is what an offline laptop looks like to
// the server). On resume the never-lose doctrine (DESIGN §6) must funnel the loser
// into a keep-both copy rather than clobber it, so BOTH edits survive on BOTH clients
// and the two trees converge.
func TestIntegration_TwoClientsConcurrentEdit(t *testing.T) {
	a, b := twoClients(t)

	const rel = "doc.txt"
	base := []byte("two-client: agreed baseline both sides start from\n")
	seedSynced(t, a, b, rel, base)

	// b goes offline; both sides now edit the same path from the same baseline.
	b.eng.Pause()
	editA := []byte("two-client: edit made on client A while B was offline\n")
	editB := []byte("two-client: edit made on client B while it was offline, must not be clobbered\n")

	a.writeLocal(t, rel, editA)
	a.waitServerHash(t, rel, editA)
	b.writeLocal(t, rel, editB)

	b.eng.Resume()

	treeA, treeB := waitConverged(t, a, b)
	requireSurvives(t, "client A", treeA, editA, "its own edit")
	requireSurvives(t, "client A", treeA, editB, "the peer's concurrent edit")
	requireSurvives(t, "client B", treeB, editA, "the peer's concurrent edit")
	requireSurvives(t, "client B", treeB, editB, "its own edit")

	// The canonical path holds ONE of the two edits on both clients (whichever the
	// resolver picked), never the stale baseline.
	canonical, ok := treeA[rel]
	if !ok {
		t.Fatalf("client A no longer has the canonical path %q: tree holds %s", rel, describeTree(treeA))
	}
	if bytes.Equal(canonical, base) {
		t.Fatalf("client A's canonical %q still holds the STALE baseline after both sides edited it", rel)
	}
}

// TestIntegration_TwoClientsDeleteVsEdit races a delete against an edit of the same
// file. Client a deletes the agreed file; client b, offline, edits it. The delete is
// a destructive apply against content that no longer matches the last agreed
// baseline, so DESIGN §6 requires b to PRESERVE the edit rather than apply the
// deletion - the edit is the only copy of that content anywhere.
func TestIntegration_TwoClientsDeleteVsEdit(t *testing.T) {
	a, b := twoClients(t)

	const rel = "notes.txt"
	base := []byte("delete-vs-edit: agreed baseline\n")
	seedSynced(t, a, b, rel, base)

	b.eng.Pause()
	editB := []byte("delete-vs-edit: client B's edit, the only copy of these bytes anywhere\n")

	a.removeLocal(t, rel)
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, ok := a.queryRel(t, rel)
		return !ok
	})
	b.writeLocal(t, rel, editB)

	b.eng.Resume()

	treeA, treeB := waitConverged(t, a, b)
	requireSurvives(t, "client B", treeB, editB, "its own edit, against a concurrent delete")
	requireSurvives(t, "client A", treeA, editB, "the peer's edit of a file A deleted")
}

// TestIntegration_TwoClientsRenameVsEdit races a rename against an edit. Client a
// renames the agreed file; client b, offline, edits it under the OLD name. The
// content b wrote must survive the rename being applied on top of it, and both
// clients must end up with the same tree - one file, holding b's bytes, under a's
// new name.
func TestIntegration_TwoClientsRenameVsEdit(t *testing.T) {
	a, b := twoClients(t)

	const (
		from = "moving.txt"
		to   = "moved.txt"
	)
	base := []byte("rename-vs-edit: agreed baseline\n")
	seedSynced(t, a, b, from, base)

	b.eng.Pause()
	editB := []byte("rename-vs-edit: client B's edit under the pre-rename name\n")

	a.renameLocal(t, from, to)
	waitForSync(t, a.eng, twoClientTimeout, func() bool {
		_, oldOK := a.queryRel(t, from)
		_, newOK := a.queryRel(t, to)
		return !oldOK && newOK
	})
	b.writeLocal(t, from, editB)

	b.eng.Resume()

	treeA, treeB := waitConverged(t, a, b)
	requireSurvives(t, "client B", treeB, editB, "its own edit, against a concurrent rename")
	requireSurvives(t, "client A", treeA, editB, "the peer's edit of a file A renamed")
}

// TestIntegration_TwoClientsSameNameCreate has both clients create the SAME new path
// with different content while neither can see the other. Unlike the edit races there
// is no shared baseline to diverge from: each side's file is untracked, which is the
// case the never-lose doctrine treats most conservatively (an untracked local file is
// never overwritten). Both bodies must survive on both clients.
func TestIntegration_TwoClientsSameNameCreate(t *testing.T) {
	a, b := twoClients(t)

	const rel = "fresh.txt"
	fromA := []byte("same-name-create: body written on client A\n")
	fromB := []byte("same-name-create: body written on client B\n")

	b.eng.Pause()
	a.writeLocal(t, rel, fromA)
	a.waitServerHash(t, rel, fromA)
	b.writeLocal(t, rel, fromB)
	b.eng.Resume()

	treeA, treeB := waitConverged(t, a, b)
	requireSurvives(t, "client A", treeA, fromA, "its own create")
	requireSurvives(t, "client A", treeA, fromB, "the peer's concurrent create")
	requireSurvives(t, "client B", treeB, fromA, "the peer's concurrent create")
	requireSurvives(t, "client B", treeB, fromB, "its own create")
}
