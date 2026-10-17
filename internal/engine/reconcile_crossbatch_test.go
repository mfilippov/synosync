package engine

// These tests pin the fix for a server-side [move a->b] and [modify b]
// arriving in SEPARATE reconcile batches with no successful drain in between. The
// pre-batch snapshot only spans one reconcile call, so in batch 2 the live baseline at b is
// already StatePending (relocatePending flipped it in batch 1) and the snapshot captures no
// synced base; queuedDownloadBases used to inherit bases only from queued OpDownloads, and
// the queued OpRename carried nothing. The download then ran with an empty base, took the
// fresh-create branch, saw the rename-advanced StateSynced baseline as "normal overwrite"
// and clobbered a cheap-scan-missed local edit the rename had just carried to b - an edit
// that was never uploaded, so the loss was unrecoverable. The fix threads the moved node's
// pre-batch synced base onto the rename op (Op.MovedBaseSynced/Op.MovedBaseLocalHash) and
// teaches queuedDownloadBases to inherit it by file id, so the cross-batch download funnels
// into keep-both exactly like the same-batch interleaving.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// runCrossBatchMoveModify drives the cross-batch [move a->b] + [modify b] interleaving
// against a locally-edited source the cheap scan missed. failFirstDrain selects the two
// production shapes that separate the batches: two PullEvent pages reconciled back-to-back
// inside one syncView lease (false - no drain runs between them), or two poll cycles whose
// leading drain fails transiently (true - the rename op survives the failed drain in the
// queue, which is exactly where the modify's reconcile must find its preserved base).
func runCrossBatchMoveModify(t *testing.T, failFirstDrain bool) {
	t.Helper()
	ts := time.Date(2026, 7, 15, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("original-content-v0")
	localBytes := []byte("local-edit-not-yet-pushed")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"b.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: the unrecorded local edit at a.txt. Baseline: still the last-synced facts
	// (the cheap scan missed the same-size/mtime edit, so no outbound upload is queued).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Batch 1: the move alone. The queued rename must preserve the moved node's pre-batch
	// synced base - the only carrier a later batch's download can inherit it from.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpRename {
		t.Fatalf("batch1 ops = %+v, want one rename", ops1)
	}
	if !ops1[0].MovedBaseSynced || ops1[0].MovedBaseLocalHash != origHash {
		t.Errorf("rename moved base = {MovedBaseSynced:%v MovedBaseLocalHash:%q}, want {true %q} (pre-batch synced record)",
			ops1[0].MovedBaseSynced, ops1[0].MovedBaseLocalHash, origHash)
	}

	if failFirstDrain {
		// The leading drain between the two poll cycles fails transiently: the view
		// directory is briefly unwritable, so the rename's os.Rename hits EACCES and the
		// op stays queued (cycle tolerates a failing drain and still polls the next batch).
		if err := os.Chmod(viewDir, 0o555); err != nil {
			t.Fatalf("chmod view dir read-only: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(viewDir, 0o755) })
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drainQueue on a read-only view dir succeeded, want a transient failure")
		}
		if n, _ := st.PendingLen(); n != 1 {
			t.Fatalf("queue after failed drain = %d ops, want the rename still queued", n)
		}
		if err := os.Chmod(viewDir, 0o755); err != nil {
			t.Fatalf("restore view dir perms: %v", err)
		}
	}

	// Batch 2: the modify at the destination. The live baseline is StatePending (or the
	// snapshot sees it mid-flight), so the base can only come from the queued rename.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "modify", FileID: "srv1", Path: "b.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpDownload {
		t.Fatalf("batch2 ops = %+v, want one download", ops2)
	}
	// The unit-level pin: the cross-batch download must inherit the pre-move synced base
	// from the queued rename. Without it the fresh-create branch overwrites the edit.
	if ops2[0].BaseLocalHash != origHash {
		t.Errorf("download BaseLocalHash = %q, want the inherited pre-batch synced hash %q",
			ops2[0].BaseLocalHash, origHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path holds the server bytes (server wins)...
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server bytes %q", got, err, serverBytes)
	}
	// ...and the local edit the rename carried to b.txt survives as the _Conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved local edit %q", got, err, localBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("source a.txt survived the move: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestReconcileCrossBatchMoveModifyFunnelsLocalEdit is the cross-batch regression
// in its two-pages shape: [move a->b] and [modify b] arrive as two PullEvent pages of one
// syncView backlog, reconciled back-to-back inside one lease with the single trailing drain
// after both. The download must inherit the moved node's pre-batch synced base from the
// queued rename and funnel the local edit into keep-both, not overwrite it.
func TestReconcileCrossBatchMoveModifyFunnelsLocalEdit(t *testing.T) {
	runCrossBatchMoveModify(t, false)
}

// TestReconcileCrossBatchMoveModifyFailedDrainFunnelsLocalEdit is the same interleaving in
// its two-poll-cycles shape: the drain between the batches runs but fails transiently (the
// rename op survives it in the queue), then the second cycle reconciles the modify and the
// next drain applies both. The outcome must be identical to the two-pages shape.
func TestReconcileCrossBatchMoveModifyFailedDrainFunnelsLocalEdit(t *testing.T) {
	requireReadOnlyDir(t)
	runCrossBatchMoveModify(t, true)
}
