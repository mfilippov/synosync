//go:build integration

package integration

import (
	"strings"
	"testing"
)

// TestIntegration_EventShapes captures what a live DSM actually puts in an event for each
// change kind. Several engine paths are written to survive a SPARSE event - one that names
// a node without identifying it - and how much of that machinery is reachable depends
// entirely on which fields the server really sends:
//
//   - preBatchDeletePath (reconcile.go) exists ONLY for an ID-LESS delete: an id-carrying
//     delete resolves through the batch snapshot's byID map and never walks paths at all.
//     Its walk terminates on a "create" claiming the path - an arm that cannot fire against
//     a server which reports creates as "modify" (settled 02.08.2026), so an id-less delete
//     of an in-batch-created node would be re-based through an older, unrelated move. If
//     DSM always carries file_id on a delete, that whole path is unreachable here and the
//     concern is bounded to servers that do not.
//   - rescueUncontainableSource needs the event's KIND (KindReported) and its SOURCE to
//     rewrite a move off an unrepresentable path; a move missing either is skipped whole
//     (accepted residuals, DESIGN §13).
//   - the engine reads a move's endpoints as path=SOURCE, destination=NEW - the inversion
//     pinned in docs/INVARIANTS.md - so a move event that carries no FromPath at all would mean the
//     decode has nothing to rebase from.
//
// The scenario therefore drives one node through create → modify → move → delete with the
// MUTATOR session (so nothing is a self-echo) and records every decoded field of the
// resulting events. It asserts only what the client genuinely depends on; everything else
// is logged, because the point is to learn the shape, not to presume it.
func TestIntegration_EventShapes(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	since, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}

	// create → modify → move → delete, all committed by the mutator session.
	created := sc.remotePut(t, "events/node.txt", []byte("first\n"))
	sc.remotePut(t, "events/node.txt", []byte("second, longer\n"))
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("events/node.txt"), sc.viewRel("events/moved.txt"))
	serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel("events/moved.txt"))

	// Two more move SHAPES, because the residuals below are about move events that
	// arrive sparse and a single file rename is one shape out of several: a DIRECTORY
	// rename (whose kind is what rescueUncontainableSource needs) and a move ACROSS
	// directories (whose source path is what the rebase needs).
	sc.remotePut(t, "events/dir/inside.txt", []byte("subtree content\n"))
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("events/dir"), sc.viewRel("events/dir-renamed"))
	sc.remoteMkdirAll(t, "events/elsewhere")
	sc.remotePut(t, "events/travels.txt", []byte("crossing directories\n"))
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("events/travels.txt"), sc.viewRel("events/elsewhere/travels.txt"))

	events, next, evidenced, err := sc.client.PullEventEvidenced(ctx, sc.view.ViewID, sc.view.RootNodeID, since)
	if err != nil {
		t.Fatalf("integration: pull_event since %d: %v", since, err)
	}
	if !evidenced {
		t.Fatalf("integration: pull_event since %d carried no evidence", since)
	}
	t.Logf("pull_event since=%d: %d events, next_sync_id=%d (node file_id=%q)",
		since, len(events), next, created.FileID)

	var sawDelete, sawMove bool
	for _, ev := range events {
		if !strings.Contains(ev.Path, "events") && !strings.Contains(ev.FromPath, "events") {
			continue
		}
		t.Logf("event: sync_id=%d action=%q path=%q from_path=%q file_id=%q from_file_id=%q "+
			"is_dir=%v kind_reported=%v size=%d hash=%q committer=%q",
			ev.SyncID, ev.Action, ev.Path, ev.FromPath, ev.FileID, ev.FromFileID,
			ev.IsDir, ev.KindReported, ev.Size, ev.Hash, ev.CommitterSessionID)

		switch ev.Action {
		case "delete":
			sawDelete = true
			if ev.FileID == "" {
				t.Errorf("integration: the DELETE event for %q carries NO file_id - it is the "+
					"id-less shape preBatchDeletePath exists for, and that function's create-arm "+
					"terminator cannot fire on a server which reports creates as \"modify\". "+
					"Record it in DESIGN §13: an id-less delete of an in-batch-created node "+
					"re-bases through an older unrelated move.", ev.Path)
			}
		case "move":
			sawMove = true
			if ev.FromPath == "" {
				t.Errorf("integration: the MOVE event to %q carries no source path - the engine "+
					"reads path=SOURCE / destination=NEW, so a source-less move is skipped whole "+
					"(DESIGN §13)", ev.Path)
			}
			if !ev.KindReported {
				t.Errorf("integration: the MOVE event for %q does not report the node's KIND - "+
					"rescueUncontainableSource cannot materialise it (DESIGN §13)", ev.Path)
			}
		}
	}
	if !sawMove {
		t.Errorf("integration: no move event for the renamed node in the page since sync_id %d - "+
			"a rename that surfaces as something else (delete+create) is itself the finding", since)
	}
	if !sawDelete {
		t.Errorf("integration: no delete event for the removed node in the page since sync_id %d", since)
	}

	// Does the server ever emit an UNPOSITIONED event? Cold start's cursor clamp works
	// only on events carrying a POSITIVE SyncID - a sparse one gives no resume point, and
	// clamping to 0 would make every such cold start replay the view's whole history
	// (accepted residual). Nobody had checked whether this DSM emits
	// them at all, so the whole log is swept here rather than just this test's own rows.
	full, _, _, err := sc.client.PullEventEvidenced(ctx, sc.view.ViewID, sc.view.RootNodeID, 0)
	if err != nil {
		t.Fatalf("integration: pull_event from 0: %v", err)
	}
	unpositioned := 0
	for _, ev := range full {
		if ev.SyncID <= 0 {
			unpositioned++
			if unpositioned <= 3 {
				t.Logf("UNPOSITIONED event: action=%q path=%q file_id=%q sync_id=%d",
					ev.Action, ev.Path, ev.FileID, ev.SyncID)
			}
		}
	}
	t.Logf("VERDICT: %d of %d events on the full page carry a non-positive sync_id", unpositioned, len(full))
	if unpositioned > 0 {
		t.Errorf("integration: the server emits UNPOSITIONED events (%d of %d) - the cold-start "+
			"cursor clamp cannot cover them, so record the shape in DESIGN §13 and decide whether "+
			"the accepted residual is still acceptable at this frequency", unpositioned, len(full))
	}
}
