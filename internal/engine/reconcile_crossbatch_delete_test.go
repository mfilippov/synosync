package engine

// These tests pin the cross-batch remove-gate fix: a server-side [move a->b] (or [modify f])
// and a [delete] of the same node arriving in SEPARATE reconcile batches with no successful
// drain in between. The pre-batch snapshot only spans one reconcile call, so in
// batch 2 the live baseline is already StatePending (relocatePending/markUpsertPending flipped
// it in batch 1) and the snapshot captures no synced base. The remove-gate used to inherit no
// cross-batch base at all (the inheritance was wired only to the download-gate), so
// BaseWasSynced stayed false and the apply-time divergence guard fell into removeBaseFor's
// preserve-whole default - resurrecting a perfectly clean deleted file as a spurious _Conflict
// copy the next scan re-uploaded to the server. The fix teaches queuedInboundBases to serve the
// remove-gate too (inheriting from a queued OpRename's MovedBaseSynced base and from a queued
// OpDownload's BaseLocalHash), and reorders the base capture BEFORE dropSupersededQueuedDownloads
// so a [modify f] download superseded by [delete f] still hands its base to the remove-gate
// before it is acked away.
//
// The *Idless* and *SparseDirMove* variants pin a follow-up: a SPARSE
// id-less delete's inheritance lookup degrades to the path key, which an id-keyed-only seed
// missed (same resurrection through a different key scheme - recordInheritedBase now registers
// every base under both keys), and the inherited base's KIND must come from the pre-batch
// baseline (MovedBaseIsDir), not the sparse event's IsDir, or a file that locally replaced a
// tracked directory is destroyed with no _Conflict copy.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// assertViewDirEmpty fails if any entry survives under viewDir - a leftover _Conflict copy is
// exactly the resurrection the fix prevents.
func assertViewDirEmpty(t *testing.T, viewDir string) {
	t.Helper()
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, en := range entries {
			names[i] = en.Name()
		}
		t.Errorf("view dir not empty after clean delete: %v (a _Conflict copy resurrected the file?)", names)
	}
}

// runCrossBatchMoveDelete drives the [move a->b] (batch 1) + [delete b] (batch 2) interleaving on
// a perfectly clean synced file. failFirstDrain selects the two production shapes that separate
// the batches: two PullEvent pages reconciled back-to-back inside one syncView lease (false - no
// drain runs between them), or two poll cycles whose leading drain fails transiently (true - the
// rename op survives the failed drain in the queue, which is exactly where the delete's reconcile
// must find its inherited base). deleteFileID selects the delete event's id: "srv1" is the
// id-carrying shape, "" the sparse id-less shape whose inheritance lookup
// degrades to the path key.
func runCrossBatchMoveDelete(t *testing.T, failFirstDrain bool, deleteFileID string) {
	t.Helper()
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Batch 1: the move alone. The queued rename must preserve the moved node's pre-batch
	// synced base - the only carrier a later batch's delete can inherit it from.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpRename {
		t.Fatalf("batch1 ops = %+v, want one rename", ops1)
	}
	if !ops1[0].MovedBaseSynced || ops1[0].MovedBaseLocalHash != cleanHash {
		t.Errorf("rename moved base = {MovedBaseSynced:%v MovedBaseLocalHash:%q}, want {true %q}",
			ops1[0].MovedBaseSynced, ops1[0].MovedBaseLocalHash, cleanHash)
	}

	if failFirstDrain {
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

	// Batch 2: the delete of the destination. The live baseline at b is StatePending, so the
	// base can only come from the queued rename.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "delete", FileID: deleteFileID, Path: "b.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRemove {
		t.Fatalf("batch2 ops = %+v, want one remove", ops2)
	}
	// The unit-level pin: the cross-batch remove must inherit the pre-move synced base from the
	// queued rename. Without it the apply-time guard resurrects the clean file as a _Conflict copy.
	if !ops2[0].BaseWasSynced || ops2[0].BaseLocalHash != cleanHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (inherited from the queued rename)",
			ops2[0].BaseWasSynced, ops2[0].BaseLocalHash, cleanHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	for _, name := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s survived the cross-batch move+delete: %v", name, err)
		}
	}
	assertViewDirEmpty(t, viewDir)
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (clean cross-batch delete must not resurrect)", recs)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline survived the delete: err=%v, want ErrNotFound", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestReconcileCrossBatchMoveDeleteCleanRemoval is the two-pages shape: [move a->b] and [delete b]
// arrive as two PullEvent pages of one syncView backlog, reconciled back-to-back inside one lease
// with the single trailing drain after both.
func TestReconcileCrossBatchMoveDeleteCleanRemoval(t *testing.T) {
	runCrossBatchMoveDelete(t, false, "srv1")
}

// TestReconcileCrossBatchMoveIdlessDeleteCleanRemoval is the sparse-delete shape: the
// batch-2 delete arrives SPARSE (no file id), the form the code already models same-batch via
// preBatchDeletePath. Its inheritance lookup degrades to the path key, so an id-keyed-only
// queuedInboundBases seed missed the queued rename's base entirely - BaseWasSynced stayed
// false and the apply-time guard resurrected the perfectly clean deleted file as a spurious
// _Conflict copy the next scan re-uploaded. The dual-key registration (recordInheritedBase)
// closes it.
func TestReconcileCrossBatchMoveIdlessDeleteCleanRemoval(t *testing.T) {
	runCrossBatchMoveDelete(t, false, "")
}

// TestReconcileCrossBatchMoveDeleteFailedDrainCleanRemoval is the two-poll-cycles shape: the drain
// between the batches runs but fails transiently (the rename survives it in the queue), then the
// second cycle reconciles the delete and the next drain applies both.
func TestReconcileCrossBatchMoveDeleteFailedDrainCleanRemoval(t *testing.T) {
	requireReadOnlyDir(t)
	runCrossBatchMoveDelete(t, true, "srv1")
}

// runCrossBatchModifyDelete drives the [modify f] (batch 1) + [delete f] (batch 2) variant.
// Batch 1 queues an OpDownload carrying f's pre-batch synced base and flips the live baseline
// StatePending; batch 2's delete supersedes and drops that download. The base capture must run
// BEFORE dropSupersededQueuedDownloads acks the doomed download, so the remove-gate still
// inherits f's base and deletes the untouched local file cleanly rather than resurrecting it
// as a _Conflict copy. deleteFileID selects the delete event's id shape, as in
// runCrossBatchMoveDelete.
func runCrossBatchModifyDelete(t *testing.T, deleteFileID string) {
	t.Helper()
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)
	serverBytes := []byte("remote-modify-version")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "f.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "f.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Batch 1: the remote modify. The queued download must preserve f's pre-batch synced base.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "f.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpDownload || ops1[0].BaseLocalHash != cleanHash {
		t.Fatalf("batch1 ops = %+v, want one download with base %q", ops1, cleanHash)
	}

	// Batch 2: the delete. It supersedes the queued download, but the remove must still inherit
	// f's base captured before the drop.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "delete", FileID: deleteFileID, Path: "f.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRemove {
		t.Fatalf("batch2 ops = %+v, want one remove", ops2)
	}
	if !ops2[0].BaseWasSynced || ops2[0].BaseLocalHash != cleanHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (inherited from the superseded download before its ack)",
			ops2[0].BaseWasSynced, ops2[0].BaseLocalHash, cleanHash)
	}
	// The doomed download must have been dropped: only the remove remains in the queue.
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("queue = %d ops, want just the remove (download superseded)", n)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "f.txt")); !os.IsNotExist(err) {
		t.Errorf("f.txt survived the modify+delete: %v", err)
	}
	assertViewDirEmpty(t, viewDir)
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (clean modify+delete must not resurrect)", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

func TestReconcileCrossBatchModifyDeleteCleanRemoval(t *testing.T) {
	runCrossBatchModifyDelete(t, "srv1")
}

// TestReconcileCrossBatchModifyIdlessDeleteCleanRemoval is the sparse variant of the modify
// shape: the id-less delete's inheritance lookup uses the path key, which
// the queued download's base must be registered under too - and the inheritance must still be
// seeded BEFORE dropSupersededQueuedDownloads acks the doomed download away (deleteSupersedes'
// path-equality fallback drops it for the id-less delete just the same).
func TestReconcileCrossBatchModifyIdlessDeleteCleanRemoval(t *testing.T) {
	runCrossBatchModifyDelete(t, "")
}

// runCrossBatchMoveDeleteDivergent is the direction regression: the same
// [move a->b] + [delete b] interleaving, but a.txt holds a genuine cheap-scan-missed local edit
// (baseline still records the last-synced hash). The inherited base must be the REAL last-synced
// hash so the apply-time guard detects the on-disk divergence and preserves the edit as a
// _Conflict copy - the fix must not over-suppress a legitimately diverged file. deleteFileID
// selects the delete event's id shape, as in runCrossBatchMoveDelete.
func runCrossBatchMoveDeleteDivergent(t *testing.T, deleteFileID string) {
	t.Helper()
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	origBytes := []byte("original-synced-content")
	localBytes := []byte("local-edit-not-yet-pushed")
	origHash := md4.ContentHash(origBytes)

	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: the unrecorded local edit at a.txt. Baseline: still the last-synced facts (the
	// cheap scan missed the same-size/mtime edit, so no outbound upload is queued).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "delete", FileID: deleteFileID, Path: "b.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if !ops2[0].BaseWasSynced || ops2[0].BaseLocalHash != origHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (the REAL last-synced hash)",
			ops2[0].BaseWasSynced, ops2[0].BaseLocalHash, origHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The server's delete is honoured on the canonical path, but the diverged local edit
	// survives as the _Conflict copy (data preserved).
	if _, err := os.Stat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt survived the delete: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved local edit %q", got, err, localBytes)
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

func TestReconcileCrossBatchMoveDeleteDivergentPreserved(t *testing.T) {
	runCrossBatchMoveDeleteDivergent(t, "srv1")
}

// TestReconcileCrossBatchMoveIdlessDeleteDivergentPreserved is the id-less direction
// regression: a base inherited through the PATH key must still be the
// real last-synced hash, so a genuinely diverged local edit keeps funnelling into a _Conflict
// copy - the dual-key registration must not turn divergence detection off for sparse deletes.
func TestReconcileCrossBatchMoveIdlessDeleteDivergentPreserved(t *testing.T) {
	runCrossBatchMoveDeleteDivergent(t, "")
}

// TestReconcileCrossBatchSparseDirMoveDeletePreservesReplacingFile pins the kind half of the
// sparse follow-up: the server moves a tracked DIRECTORY with a SPARSE move event (IsDir
// false - the tolerant decode's default), the user has locally replaced that directory with a
// regular file, and a cross-batch id-less delete follows. The inherited base's kind must come
// from the pre-batch BASELINE (MovedBaseIsDir), not from the event: an event-sourced kind
// inherits isDir=false, the synced dir's structurally-empty hash then reads as "synced file
// without hash → server authority", and the replacing file is destroyed with NO _Conflict
// copy - the exact replaced-directory data-loss the direct capture already guards, bypassed through
// the inheritance path.
func TestReconcileCrossBatchSparseDirMoveDeletePreservesReplacingFile(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	replacingBytes := []byte("file-that-locally-replaced-the-tracked-dir")

	copyRel := conflictCopyName("e", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// Tracked synced DIRECTORY at d - but on disk the user has replaced it with a regular
	// file (rm -rf d && echo data > d). The cheap scan has not run yet, so no outbound op
	// records the replacement; the baseline still describes the synced directory.
	if err := os.WriteFile(filepath.Join(viewDir, "d"), replacingBytes, 0o644); err != nil {
		t.Fatalf("seed replacing file: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "srv1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}

	// Batch 1: the server moves the directory d->e, but the event is SPARSE - IsDir is
	// false even though the node is a directory. The queued rename must capture the base's
	// kind from the baseline, not the event.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "d", Path: "e"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpRename {
		t.Fatalf("batch1 ops = %+v, want one rename", ops1)
	}
	if !ops1[0].MovedBaseSynced || !ops1[0].MovedBaseIsDir {
		t.Errorf("rename moved base = {MovedBaseSynced:%v MovedBaseIsDir:%v}, want {true true} (kind from the BASELINE, not the sparse event)",
			ops1[0].MovedBaseSynced, ops1[0].MovedBaseIsDir)
	}

	// Batch 2: the sparse id-less delete of the destination.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "delete", Path: "e"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRemove {
		t.Fatalf("batch2 ops = %+v, want one remove", ops2)
	}
	if !ops2[0].BaseWasSynced || !ops2[0].BaseIsDir {
		t.Errorf("remove base = {BaseWasSynced:%v BaseIsDir:%v}, want {true true} (dir kind inherited from the queued rename)",
			ops2[0].BaseWasSynced, ops2[0].BaseIsDir)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The server's delete is honoured on the canonical path, but the file that locally
	// replaced the tracked directory survives as the _Conflict copy (never-lose).
	for _, name := range []string{"d", "e"} {
		if _, err := os.Stat(filepath.Join(viewDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s survived the cross-batch dir move+delete: %v", name, err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(replacingBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved replacing file %q", got, err, replacingBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "e" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one e -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}
