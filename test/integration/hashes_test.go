//go:build integration

package integration

import (
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
)

// TestIntegration_NodeHashes settles a residual left open
// (DESIGN §13, docs/INTEGRATION.md): does a live DSM report a per-file content
// hash on `query_node`, and does it carry one on the `pull_event` log a cold
// start replays?
//
// Three client verdicts are provable in that one form only:
//
//   - cold start's "same content on both sides" gate (coldStartContentMatch),
//     which degrades to size+kind with one Warn per pass when no hash rides
//     along;
//   - the keep-both resolver's false-conflict fast path, which deliberately does
//     NOT degrade - a hash-less query_node Warns and resolves as a real
//     keep-both, costing one _Conflict copy plus two transfers per funnelled
//     resolve;
//   - the lost-reply upload probe, which falls back to identity plus a positive
//     size.
//
// So the answer decides whether those degrade arms are dead insurance or the
// everyday path. This scenario asks the server directly and pins the answer: a
// hash must be present AND equal to the MD4 of the bytes we uploaded (the wire
// value folded to lowercase hex at decode). A failure here is not a client bug -
// it is the measurement changing, and it must be recorded in DESIGN §13 before
// the assertion is relaxed.
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_NodeHashes(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	// A cursor taken BEFORE the mutations, so the event page below starts at our
	// own changes instead of replaying the view's whole history. get_max_sync_id
	// LAGS the log (measured 31.07.2026), which only ever yields extra older
	// events here - harmless, since the events are matched by path.
	since, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}

	content := []byte("hash capture: the quick brown fox jumps over the lazy dog\n")
	wantHash := md4.ContentHash(content)
	node := sc.remotePut(t, "hashes/file.txt", content)

	// 1. The upload reply itself.
	t.Logf("upload reply: file_id=%q size=%d hash=%q", node.FileID, node.Size, node.Hash)

	// 2. query_node of the freshly uploaded file.
	got, ok := sc.queryRel(t, "hashes/file.txt")
	if !ok {
		t.Fatalf("integration: query_node %q reports the just-uploaded file absent", sc.viewRel("hashes/file.txt"))
	}
	t.Logf("query_node(file): file_id=%q size=%d is_dir=%v hash=%q", got.FileID, got.Size, got.IsDir, got.Hash)
	if got.Hash == "" {
		t.Errorf("integration: query_node of %q carried NO content hash - the resolver's "+
			"false-conflict fast path cannot prove equality on this server, so every funnelled "+
			"resolve costs a _Conflict copy plus two transfers. Record the measurement in "+
			"DESIGN §13 / docs/INTEGRATION.md before relaxing this assertion.",
			sc.viewRel("hashes/file.txt"))
	} else if got.Hash != wantHash {
		t.Errorf("integration: query_node hash %q != MD4 of the uploaded bytes %q - the server's "+
			"hash is not the content hash the client compares against (or is not MD4)", got.Hash, wantHash)
	}
	if got.Size != int64(len(content)) {
		t.Errorf("integration: query_node size %d, want %d", got.Size, len(content))
	}

	// 3. query_node of a DIRECTORY: a hash here would be meaningless, and the
	//    client treats a directory's hash as absent. Recorded, not asserted as
	//    absent, so a server that reports one is visible rather than silently
	//    compared.
	dir, ok := sc.queryRel(t, "hashes")
	if !ok {
		t.Fatalf("integration: query_node %q reports the scratch directory absent", sc.viewRel("hashes"))
	}
	t.Logf("query_node(dir): file_id=%q is_dir=%v size=%d hash=%q", dir.FileID, dir.IsDir, dir.Size, dir.Hash)
	if !dir.IsDir {
		t.Errorf("integration: query_node of %q reports is_dir=false", sc.viewRel("hashes"))
	}

	// 4. The event log a cold start replays. A create's event must carry the same
	//    hash, or coldStartContentMatch has nothing to compare and falls back to
	//    size+kind.
	events, next, evidenced, err := sc.client.PullEventEvidenced(ctx, sc.view.ViewID, sc.view.RootNodeID, since)
	if err != nil {
		t.Fatalf("integration: pull_event since %d: %v", since, err)
	}
	if !evidenced {
		t.Fatalf("integration: pull_event since %d carried no evidence (neither errno nor an event array) - "+
			"cold start reads such a reply as inconclusive", since)
	}
	t.Logf("pull_event since=%d: %d events, next_sync_id=%d", since, len(events), next)

	rel := sc.viewRel("hashes/file.txt")
	var found bool
	for _, ev := range events {
		// Every event in the page is recorded, not just the matching one: the
		// page is the exact shape a cold start replays, and which ACTION a fresh
		// single-frame upload surfaces as (create? modify? both?) is part of the
		// measurement - the engine's replay asserts on create and modify alike,
		// so a server that reports only one of them must be on the record.
		t.Logf("event: sync_id=%d action=%q path=%q is_dir=%v kind_reported=%v size=%d hash=%q",
			ev.SyncID, ev.Action, ev.Path, ev.IsDir, ev.KindReported, ev.Size, ev.Hash)
		if strings.TrimLeft(ev.Path, "/") != rel {
			continue
		}
		found = true
		if ev.Action != "create" && ev.Action != "modify" {
			continue
		}
		if ev.Hash == "" {
			t.Errorf("integration: %s event for %q carried NO content hash - cold start's "+
				"content match degrades to size+kind (one Warn per pass) on this server. "+
				"Record it in DESIGN §13 before relaxing this assertion.", ev.Action, rel)
		} else if ev.Hash != wantHash {
			t.Errorf("integration: %s event hash %q != MD4 of the uploaded bytes %q",
				ev.Action, ev.Hash, wantHash)
		}
		if ev.Size != int64(len(content)) {
			t.Errorf("integration: %s event size %d, want %d", ev.Action, ev.Size, len(content))
		}
	}
	if !found {
		t.Errorf("integration: no event for %q in the page since sync_id %d (%d events) - "+
			"the cursor may lag further than the page covers; re-run with SYNOSYNC_IT_LOG=debug",
			rel, since, len(events))
	}
}
