//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_RecursiveRemoveTombstones measures whether a RECURSIVE server-side
// `remove` reports its descendants in the event log, or only the directory it names.
//
// The answer decides a live gap in the inbound half of the engine. `applyRemove`
// (inbound_remove.go) states the premise directly - "a server delete event names only
// the directory, never its descendants" - and drops the descendant baselines itself on
// that basis. The same premise makes the REFUSAL path lossy: when an inbound directory
// delete is refused because a successor node stands at the path (the H3 identity gate,
// or the positional gate), `repairSupersededRecord` repairs only the directory's own
// record and deliberately keeps the descendant records. If the server sends no
// per-descendant rows, those children keep a StateSynced record over local files the
// server already destroyed: `localReconcile` drives only StateSynced records and finds
// disk == LocalHash, so it never diffs them, the server never names them again, and cold
// start does not re-run. The files then look synced in both replicas while existing in
// one - no _Conflict copy, no dead letter, no Unresolved entry, which is the
// silent-unsyncable class H6 exists to close.
//
// If the server DOES emit a row per descendant, each child converges through its own
// ordinary inbound delete and the gap closes on its own; `dropDescendantBaselines`
// becomes belt-and-braces rather than the only repair.
//
// The scenario drives the server directly, with no engine, so it measures the protocol
// and nothing else. It asserts only that the capture really happened (the directory's
// own row must arrive); which shape the log takes is LOGGED, because the point is to
// learn the answer, not to presume it. Record the verdict in docs/INTEGRATION.md, and
// pin it with an assertion here, once the client is aligned to it.
func TestIntegration_RecursiveRemoveTombstones(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	// A directory holding two files, a nested directory, and a file inside that - so the
	// capture separates "the server reports files" from "the server reports directories",
	// and covers a descendant more than one level below the deleted node.
	dir := sc.remoteMkdirAll(t, "rectomb")
	fileA := sc.remotePut(t, "rectomb/a.txt", []byte("first child\n"))
	fileB := sc.remotePut(t, "rectomb/b.txt", []byte("second child\n"))
	sub := sc.remoteMkdirAll(t, "rectomb/sub")
	fileC := sc.remotePut(t, "rectomb/sub/c.txt", []byte("grandchild\n"))

	// The nodes the delete must account for, deepest first so the log is easy to read.
	type target struct {
		label  string
		rel    string
		fileID string
		isDir  bool
	}
	descendants := []target{
		{"file (grandchild)", "rectomb/sub/c.txt", fileC.FileID, false},
		{"directory (nested)", "rectomb/sub", sub.FileID, true},
		{"file (child)", "rectomb/b.txt", fileB.FileID, false},
		{"file (child)", "rectomb/a.txt", fileA.FileID, false},
	}
	t.Logf("built the tree: dir=%q sub=%q a=%q b=%q c=%q",
		dir.FileID, sub.FileID, fileA.FileID, fileB.FileID, fileC.FileID)

	// Take the cursor AFTER the tree is built, so the page below holds the deletion and
	// not the creates. The log COMPACTS per node and a tombstone lands at a NEW, higher
	// sync_id than the create it absorbs (INVARIANTS.md), so a delete row can never hide
	// behind this cursor. get_max_sync_id LAGS the log, so the real tail is taken from
	// pull_event's own next_sync_id, exactly as skipColdStart resolves it.
	since := sc.logTail(t)
	t.Logf("cursor before the recursive remove: sync_id=%d", since)

	ctx, cancel := actionCtx()
	if err := sc.client.Remove(ctx, sc.view.ViewID, sc.viewRel("rectomb")); err != nil {
		cancel()
		t.Fatalf("integration: recursive remove of %q: %v", sc.viewRel("rectomb"), err)
	}
	cancel()

	// The directory's own row is the proof the capture worked. Poll for it rather than
	// reading once: a log that lands the row a moment later would otherwise be recorded
	// as "the server reports nothing", which is the very verdict under test.
	dirRel := sc.viewRel("rectomb")
	var events []actions.Event
	deadline := time.Now().Add(20 * time.Second)
	for {
		events = sc.drainEvents(t, since)
		if findDeleteRow(events, dirRel, dir.FileID) != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("integration: no delete row for the removed directory %q within 20s of %d rows "+
				"since sync_id %d - the capture itself did not work (cursor or paging), so no verdict "+
				"about the descendants can be read from it", dirRel, len(events), since)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Let the log settle and read it again before concluding anything about the
	// descendants. The poll above stops at the FIRST page that carries the directory's
	// own row, so a server that emits the children's tombstones a moment later would be
	// recorded as "reports nothing" purely because the read was early - and "0 rows" is
	// exactly the verdict that makes the client's refusal path lossy. An absence is only
	// evidence once it has been given time to be contradicted.
	time.Sleep(settleAfterRemove)
	settled := sc.drainEvents(t, since)
	if len(settled) > len(events) {
		t.Logf("the log grew from %d to %d rows during the %s settle window",
			len(events), len(settled), settleAfterRemove)
	}
	events = settled

	for _, ev := range events {
		if !underPath(ev, dirRel) {
			continue
		}
		t.Logf("event: sync_id=%d action=%q path=%q from_path=%q file_id=%q is_dir=%v kind_reported=%v",
			ev.SyncID, ev.Action, ev.Path, ev.FromPath, ev.FileID, ev.IsDir, ev.KindReported)
	}

	reported := 0
	for _, d := range descendants {
		rel := sc.viewRel(d.rel)
		row := findDeleteRow(events, rel, d.fileID)
		if row == nil {
			t.Logf("NO delete row for the %s %q (file_id=%q)", d.label, rel, d.fileID)
			continue
		}
		reported++
		t.Logf("delete row PRESENT for the %s %q: sync_id=%d file_id=%q (matched by %s)",
			d.label, rel, row.SyncID, row.FileID, matchKind(row, rel, d.fileID))
	}

	switch {
	case reported == len(descendants):
		t.Logf("MEASURED: a recursive `remove` emits a delete row for EVERY descendant "+
			"(%d of %d). Each child therefore converges through its own inbound delete, so the "+
			"refused-delete repair (repairSupersededRecord) leaving descendant records behind is "+
			"self-healing, and applyRemove's premise that \"a delete event names only the "+
			"directory\" is WRONG in the safe direction - dropDescendantBaselines is belt-and-"+
			"braces, not the only repair. Align the comments in inbound_remove.go and record the "+
			"answer in docs/INTEGRATION.md.", reported, len(descendants))
	case reported == 0:
		t.Logf("MEASURED: a recursive `remove` reports ONLY the directory it names - 0 of %d "+
			"descendants get a row. applyRemove's premise holds, and the refusal path is "+
			"therefore lossy exactly as the review found: when an inbound directory delete is "+
			"refused by the successor gate, repairSupersededRecord repairs only the directory's "+
			"record, so every formerly-synced descendant keeps a StateSynced record over a local "+
			"file the server already destroyed - invisible to localReconcile (disk == LocalHash), "+
			"never re-described by the server, and never surfaced as Unresolved. That is the H6 "+
			"silent-unsyncable class, and the repair needs to account for the subtree.",
			len(descendants))
	default:
		t.Logf("MEASURED: a recursive `remove` reports SOME descendants (%d of %d) - see the "+
			"per-node lines above for which. A partial rule is the worst case for the client: the "+
			"repair cannot assume either premise, so record exactly which kinds and depths are "+
			"reported in docs/INTEGRATION.md before changing repairSupersededRecord.",
			reported, len(descendants))
	}
}

// settleAfterRemove is how long the log is given to produce anything further after the
// directory's own tombstone has arrived. It buys the descendant-absence verdict its
// evidence: without it the measurement would rest on a single read that stops at the
// first page carrying the row it waited for.
const settleAfterRemove = 10 * time.Second

// logTail resolves the event log's true tail: get_max_sync_id LAGS the log (measured
// 31.07.2026), so its answer is only a floor, and pull_event's own next_sync_id is the
// real position. Mirrors skipColdStart's resolution.
func (sc *scenario) logTail(t *testing.T) int64 {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()

	tail, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}
	if _, next, err := sc.client.PullEvent(ctx, sc.view.ViewID, sc.view.RootNodeID, tail); err != nil {
		t.Fatalf("integration: pull_event to resolve the log tail: %v", err)
	} else if next > tail {
		tail = next
	}
	return tail
}

// maxDrainPages bounds how many pull_event pages one drain reads. `pull_event` pages by a
// FIXED-WIDTH sync_id window (2000) over a log the compaction leaves holed, so an empty
// page proves its own window and not the log (INVARIANTS.md) - a drain must step over a
// hole rather than stop at it, and therefore needs its own bound.
const maxDrainPages = 16

// drainEvents reads the event log forward from since, following next_sync_id across
// pages, and returns every row it collected. It stops when the cursor stops advancing or
// the page budget is spent.
func (sc *scenario) drainEvents(t *testing.T, since int64) []actions.Event {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()

	var all []actions.Event
	cursor := since
	for page := 0; page < maxDrainPages; page++ {
		events, next, err := sc.client.PullEvent(ctx, sc.view.ViewID, sc.view.RootNodeID, cursor)
		if err != nil {
			t.Fatalf("integration: pull_event since %d: %v", cursor, err)
		}
		all = append(all, events...)
		if next <= cursor {
			break
		}
		cursor = next
	}
	return all
}

// underPath reports whether an event names a path at or below rel, on either endpoint (a
// move reports its source at the top level and its destination separately).
func underPath(ev actions.Event, rel string) bool {
	for _, p := range []string{ev.Path, ev.FromPath} {
		p = strings.TrimLeft(p, "/")
		if p == rel || strings.HasPrefix(p, rel+"/") {
			return true
		}
	}
	return false
}

// findDeleteRow returns the delete event for a node, matched by its view-relative path OR
// by its file id. Both are tried because a tombstone that carries an id but a stale or
// empty path - or the reverse - still reports the node, and reading only one field would
// under-count what the server actually said.
func findDeleteRow(events []actions.Event, rel, fileID string) *actions.Event {
	for i := range events {
		ev := &events[i]
		if ev.Action != "delete" {
			continue
		}
		if strings.TrimLeft(ev.Path, "/") == rel {
			return ev
		}
		if fileID != "" && ev.FileID == fileID {
			return ev
		}
	}
	return nil
}

// matchKind names which field identified a delete row, so the log says whether the server
// reported the path, the id, or both.
func matchKind(ev *actions.Event, rel, fileID string) string {
	byPath := strings.TrimLeft(ev.Path, "/") == rel
	byID := fileID != "" && ev.FileID == fileID
	switch {
	case byPath && byID:
		return "path and file_id"
	case byPath:
		return "path"
	default:
		return "file_id"
	}
}
