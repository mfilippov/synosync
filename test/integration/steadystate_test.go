//go:build integration

package integration

import (
	"os"
	"testing"
	"time"
)

// TestIntegration_TwoClientsSteadyState checks the property every two-way sync must
// have and no single-client scenario can observe: once two clients agree, they STOP.
//
// A ping-pong loop is the classic failure - client A writes a file, client B downloads
// it but records a different local mtime (the wire value is out of range, or coarser
// than the filesystem's), B's next scan reads that as a local modification and pushes
// it back, A downloads it in turn, and the pair churns forever. It costs no
// correctness assertion anywhere: every content check still passes while the two
// clients rewrite the same bytes indefinitely.
//
// The probe seeds the shapes most likely to round-trip badly - an ordinary file, an
// EMPTY file, a file with a deliberately old mtime, and one inside a subdirectory -
// waits for both clients to hold them, then watches the server's event cursor for the
// view. In a steady state it must stop advancing.
func TestIntegration_TwoClientsSteadyState(t *testing.T) {
	a, b := twoClients(t)

	ordinary := []byte("steady-state: an ordinary file\n")
	nested := []byte("steady-state: a file inside a subdirectory\n")

	seedSynced(t, a, b, "plain.txt", ordinary)
	seedSynced(t, a, b, "nest/inner.txt", nested)

	// An EMPTY file: zero-length bodies are the shape a size-or-hash comparison is most
	// likely to mishandle.
	a.writeLocal(t, "empty.txt", nil)

	// A file whose mtime is far in the past: the download side re-applies the wire mtime,
	// and a value the local filesystem cannot represent exactly is what makes a
	// cheap-scan hint miss forever.
	a.writeLocal(t, "old.txt", []byte("steady-state: an old file\n"))
	old := time.Date(1985, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(a.localAbs(t, "old.txt"), old, old); err != nil {
		t.Fatalf("integration: backdate old.txt: %v", err)
	}

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		for _, rel := range []string{"empty.txt", "old.txt"} {
			if _, ok := b.readLocal(t, rel); !ok {
				return false
			}
		}
		return sameTree(a.localTree(t, ""), b.localTree(t, ""))
	})
	t.Logf("converged tree: %s", describeTree(a.localTree(t, "")))

	// Now watch for churn - with an instrument scoped to the two clients under test.
	//
	// This used to read the view's global event cursor and require it to hold still. That is
	// the right SIGNAL (a pair rewriting each other's files commits on every round) but the
	// wrong SCOPE: MaxSyncID covers the whole view, not this scenario's scratch subtree, and
	// the scenario runs moments after TwoClientsRapidChurn. Anything still committing on that
	// view - a neighbouring scenario's residue, or the DSM's own indexer catching up - advances
	// the cursor and fails this test with a message blaming a client ping-pong. It failed
	// exactly once that way, and could not be diagnosed afterwards.
	//
	// The engines' own applied-op counters answer the question directly: a ping-pong is applies,
	// on both sides, forever. They are process-local and monotonic, so nothing outside these two
	// clients can move them. The view cursor is still read, but as CORROBORATION rather than as
	// the verdict - a cursor that moves while neither client applied anything is somebody else's
	// traffic, and saying so is more useful than failing on it.
	applied := func() (int64, int64) {
		aOps, _, _ := a.eng.Progress()
		bOps, _, _ := b.eng.Progress()
		return aOps, bOps
	}
	settle := func() int64 {
		ctx, cancel := actionCtx()
		defer cancel()
		id, err := a.client.MaxSyncID(ctx, a.view.ViewID, a.view.RootNodeID)
		if err != nil {
			t.Logf("integration: could not read the view cursor (diagnostic only): %v", err)
			return -1
		}
		return id
	}

	// Wait for the pair to go quiet before measuring. The engines sync the whole view, so a
	// neighbouring scenario's last commits (a scrub of the scratch base, a server-to-local run)
	// can still reach them for a while after this scenario converged; a fixed 10 s pause let that
	// residue land inside the window and fail the test (it passed alone, every time). A real
	// ping-pong never goes quiet, so it still fails - here, with the same diagnosis.
	const quiet, settleCap = 20 * time.Second, 3 * time.Minute
	lastA, lastB := applied()
	stableSince, settleStart := time.Now(), time.Now()
	for time.Since(stableSince) < quiet {
		if time.Since(settleStart) > settleCap {
			curA, curB := applied()
			t.Fatalf("the two clients never went quiet for %s within %s: A at %d, B at %d ops "+
				"(a ping-pong loop rewrites the same bytes forever)", quiet, settleCap, curA, curB)
		}
		time.Sleep(2 * time.Second)
		if curA, curB := applied(); curA != lastA || curB != lastB {
			lastA, lastB, stableSince = curA, curB, time.Now()
		}
	}
	startA, startB := applied()
	startCursor := settle()
	const watch = 45 * time.Second
	time.Sleep(watch)
	endA, endB := applied()
	endCursor := settle()

	// Log the evidence BEFORE asserting on it. The one failure this test has ever produced was
	// undiagnosable because its numbers lived only inside the Fatalf, and the run that caught it
	// was captured through a filter that kept only the verdict lines.
	t.Logf("over %s of idle: client A applied %d -> %d ops, client B %d -> %d, view cursor %d -> %d",
		watch, startA, endA, startB, endB, startCursor, endCursor)
	t.Logf("client A tree: %s", describeTree(a.localTree(t, "")))
	t.Logf("client B tree: %s", describeTree(b.localTree(t, "")))

	if endA != startA || endB != startB {
		t.Fatalf("the two idle clients kept applying work: A %d -> %d, B %d -> %d over %s "+
			"with no user change (a ping-pong loop rewrites the same bytes forever)",
			startA, endA, startB, endB, watch)
	}
	if startCursor >= 0 && endCursor != startCursor {
		// Not a failure: neither client applied anything, so whatever moved the cursor is not
		// this pair. Worth saying out loud, because it is the shape that produced that one
		// unexplained failure.
		t.Logf("note: the view cursor advanced %d -> %d while neither client applied an op - "+
			"that is traffic from outside this scenario, not a ping-pong",
			startCursor, endCursor)
	}

	// A steady state must also be a QUIET one: no op left retrying on either side.
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
	if n := a.eng.StuckOps() + b.eng.StuckOps(); n != 0 {
		t.Fatalf("the settled pair left %d stuck op(s)", n)
	}
	if na := pendingLen(t, a); na != 0 {
		t.Fatalf("client A settled with %d queued op(s)", na)
	}
	if nb := pendingLen(t, b); nb != 0 {
		t.Fatalf("client B settled with %d queued op(s)", nb)
	}
}
