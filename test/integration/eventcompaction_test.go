//go:build integration

package integration

import (
	"strings"
	"testing"
)

// TestIntegration_EventLogCompaction characterises how much HISTORY a live DSM's event log
// keeps per node - the premise under cold start.
//
// docs/INVARIANTS.md records pull_event(sync_id=0) as "the RAW LOG, not a snapshot", which is what
// makes the cold-start fold a REPLAY (create/modify assert, delete retracts, move relocates)
// rather than a filter. The capture in TestIntegration_EventShapes showed something the
// wording does not cover: after a node was created, modified, moved and removed, only the
// move and delete rows were in the page - its create and modify rows were gone. So the log
// is compacted per node, not strictly append-only, and the question is by what rule.
//
// Three lifecycles are driven in one pass and read back with a single pull, each on its own
// path so the rows are attributable:
//
//	touched  - create, then modify        (does the create row survive a later modify?)
//	erased   - create, then delete        (does the create row survive the delete?)
//	relocated - create, then move         (does the create row survive the move?)
//
// Everything is recorded; the assertion is only on what a REPLAY needs to converge, which is
// the property cold start actually depends on: whichever rows survive must still describe
// the node's final state - a surviving row for a live node, a delete for a dead one. A log
// that dropped the create of a node that still exists, leaving nothing in its place, would
// make a cold start miss the file entirely, and that is the failure this pins.
func TestIntegration_EventLogCompaction(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	since, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}

	touched := sc.remotePut(t, "compact/touched.txt", []byte("v1\n"))
	sc.remotePut(t, "compact/touched.txt", []byte("v2 longer\n"))

	erased := sc.remotePut(t, "compact/erased.txt", []byte("doomed\n"))
	serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel("compact/erased.txt"))

	relocated := sc.remotePut(t, "compact/relocated.txt", []byte("moving\n"))
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("compact/relocated.txt"), sc.viewRel("compact/arrived.txt"))

	events, next, evidenced, err := sc.client.PullEventEvidenced(ctx, sc.view.ViewID, sc.view.RootNodeID, since)
	if err != nil {
		t.Fatalf("integration: pull_event since %d: %v", since, err)
	}
	if !evidenced {
		t.Fatalf("integration: pull_event since %d carried no evidence", since)
	}
	t.Logf("pull_event since=%d: %d events, next_sync_id=%d", since, len(events), next)

	// Group the page's rows by the node they describe, so "how many rows survive per node"
	// is read off ids rather than paths (a move changes the path, not the id).
	rows := map[string][]string{}
	for _, ev := range events {
		if !strings.Contains(ev.Path, "compact") && !strings.Contains(ev.FromPath, "compact") {
			continue
		}
		rows[ev.FileID] = append(rows[ev.FileID],
			ev.Action+"@"+ev.Path)
		t.Logf("event: sync_id=%d action=%q path=%q from_path=%q file_id=%q size=%d hash=%q",
			ev.SyncID, ev.Action, ev.Path, ev.FromPath, ev.FileID, ev.Size, ev.Hash)
	}

	for _, tc := range []struct {
		label   string
		node    string
		alive   bool
		wantEnd string // the path the node must end up described at, when alive
	}{
		{label: "create+modify", node: touched.FileID, alive: true, wantEnd: sc.viewRel("compact/touched.txt")},
		{label: "create+delete", node: erased.FileID, alive: false},
		{label: "create+move", node: relocated.FileID, alive: true, wantEnd: sc.viewRel("compact/arrived.txt")},
	} {
		got := rows[tc.node]
		t.Logf("VERDICT %s: node %s has %d row(s) in the log: %v", tc.label, tc.node, len(got), got)
		if len(got) == 0 {
			t.Errorf("integration: %s left NO row for node %s in the log - a cold start replaying "+
				"this page never learns the node exists", tc.label, tc.node)
			continue
		}
		last := got[len(got)-1]
		if tc.alive {
			if strings.HasPrefix(last, "delete@") {
				t.Errorf("integration: %s: the last surviving row for a LIVE node is %q", tc.label, last)
			}
			if !strings.HasSuffix(last, "@"+tc.wantEnd) && !strings.HasSuffix(last, "@/"+tc.wantEnd) {
				t.Errorf("integration: %s: the last surviving row is %q, want it to describe %q - "+
					"a replay would place the node at the wrong path", tc.label, last, tc.wantEnd)
			}
		} else if !strings.HasPrefix(last, "delete@") {
			t.Errorf("integration: %s: the last surviving row for a DELETED node is %q, not a delete - "+
				"a replay would resurrect it", tc.label, last)
		}
	}
}
