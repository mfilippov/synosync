package engine

// These tests pin the pre-batch snapshot fix: the reconcile capture gates (download-base,
// rename-dest, remove-base) must read a PRE-BATCH snapshot of the baselines, not the live
// store, because an earlier event in the same batch already rewrites the baseline through
// markPending before a later event's gate runs. The motivating HIGH interleaving is
// [move a->b, modify b]: the move relocates the baseline to b and flips it StatePending,
// so a live download-base read at the modify captures nothing and applyDownload clobbers
// a cheap-scan-missed local edit the rename just carried to b - an edit that was never
// uploaded, so the loss is unrecoverable. The sibling LOW interleaving is
// [move a->b, delete b]: the live remove-base read sees the relocated StatePending record,
// captures BaseWasSynced=false, and guardRemoveDivergence resurrects a perfectly clean
// deleted file as a spurious _Conflict copy.

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

// TestReconcileMoveModifyBatchFunnelsLocalEdit is the pre-batch snapshot regression: a
// single batch of move a.txt->b.txt, modify b.txt (same node) against a locally-edited
// source the cheap scan missed. The download-base gate must capture the node's PRE-BATCH
// synced hash (the move has already flipped the live baseline StatePending by the time the
// modify's gate runs), so the download funnels the divergence into keep-both - the edit
// survives as a _Conflict copy - instead of overwriting the moved file with server bytes.
func TestReconcileMoveModifyBatchFunnelsLocalEdit(t *testing.T) {
	ts := time.Date(2026, 7, 14, 15, 30, 4, 0, time.UTC)
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

	// One batch: move FIRST, then modify at the destination - the interleaving where no
	// earlier same-batch download captured the base and the live baseline is already pending.
	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 7, Action: "modify", FileID: "srv1", Path: "b.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 2 || ops[0].Type != store.OpRename || ops[1].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want [rename, download]", ops)
	}
	// The unit-level pin: the download must carry the node's pre-batch synced hash even
	// though the move already flipped the live baseline StatePending.
	if ops[1].BaseLocalHash != origHash {
		t.Errorf("download BaseLocalHash = %q, want the pre-batch synced hash %q", ops[1].BaseLocalHash, origHash)
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

// TestReconcileMoveDeleteBatchCleanRemoval is the sibling LOW regression of the same root
// cause: a single batch of move a.txt->b.txt, delete b.txt (same node) on a perfectly clean
// synced file. The remove-base gate must capture the node's PRE-BATCH StateSynced record
// (id-first - the move already relocated it and flipped it StatePending), so the apply-time
// divergence guard reads on-disk == last-synced and honours the delete cleanly, instead of
// treating the relocated pending record as untracked and resurrecting the deleted file as a
// spurious _Conflict copy.
func TestReconcileMoveDeleteBatchCleanRemoval(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 14, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 7, Action: "delete", FileID: "srv1", Path: "b.txt"},
	}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 2 || ops[0].Type != store.OpRename || ops[1].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [rename, remove]", ops)
	}
	// The unit-level pin: the remove must carry the node's pre-batch synced base so the
	// clean file deletes without a keep-both detour.
	if !ops[1].BaseWasSynced || ops[1].BaseLocalHash != cleanHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (pre-batch synced record)",
			ops[1].BaseWasSynced, ops[1].BaseLocalHash, cleanHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Clean delete: both paths gone, no _Conflict resurrection anywhere in the view dir.
	for _, name := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s survived the move+delete batch: %v", name, err)
		}
	}
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
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (clean delete must not resurrect)", recs)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline survived the delete: err=%v, want ErrNotFound", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestReconcileMoveSparseDeleteBatchCleanRemoval is the sparse-delete tail of the same
// root cause: the batch's delete arrives SPARSE (no file id), so the remove-base gate cannot
// resolve the moved node through the snapshot's byID map, and the delete's path names the
// node's post-move slot while the pre-batch synced record lives under the source spelling.
// The gate must map the path back through the batch's earlier moves (preBatchDeletePath) so
// the clean file deletes cleanly - without the mapping BaseWasSynced stays false and the
// apply-time guard resurrects the deleted file as a spurious _Conflict copy.
func TestReconcileMoveSparseDeleteBatchCleanRemoval(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 15, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 7, Action: "delete", Path: "b.txt"}, // sparse: no file id
	}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 2 || ops[0].Type != store.OpRename || ops[1].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [rename, remove]", ops)
	}
	// The unit-level pin: the sparse remove must still carry the node's pre-batch synced
	// base, found under the move's source spelling.
	if !ops[1].BaseWasSynced || ops[1].BaseLocalHash != cleanHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (resolved through the batch's move)",
			ops[1].BaseWasSynced, ops[1].BaseLocalHash, cleanHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	for _, name := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s survived the move+sparse-delete batch: %v", name, err)
		}
	}
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
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (clean sparse delete must not resurrect)", recs)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline survived the delete: err=%v, want ErrNotFound", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestReconcileSelfEchoMoveSparseDeleteCleanRemoval covers the snapshot side of the same
// fix: a SELF-ECHO move is skipped by snapshotBatchBaselines entirely (it enqueues no ops
// and runs no gates), so a sparse delete following it in one batch relies on the delete's
// own capture of the resolved pre-batch path. The on-disk state mirrors a self-echo: the
// file already sits at the destination (this client moved it), while the pre-batch baseline
// still records the source spelling.
func TestReconcileSelfEchoMoveSparseDeleteCleanRemoval(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 15, 15, 30, 4, 0, time.UTC))

	// The move already happened locally: disk holds b.txt, the pre-batch baseline a.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// outboundEngine wires selfSessionID "sess", so the move is a self-echo; the delete
	// (another client's) is sparse.
	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "sess"},
		{SyncID: 7, Action: "delete", Path: "b.txt"},
	}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want [remove] (the self-echo move enqueues nothing)", ops)
	}
	if !ops[0].BaseWasSynced || ops[0].BaseLocalHash != cleanHash {
		t.Errorf("remove base = {BaseWasSynced:%v BaseLocalHash:%q}, want {true %q} (captured at the resolved pre-batch path)",
			ops[0].BaseWasSynced, ops[0].BaseLocalHash, cleanHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt survived the sparse delete: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestReconcileMoveReplayKeepsIdempotentDestBranch is the snapshot regression for the
// rename-dest gate's idempotent-replay branch: when a crash-replayed batch re-reconciles a
// move whose first apply already landed the node at the destination, the pre-batch snapshot
// (which now IS the post-apply state) still shows the destination holding this very node, so
// the gate must take the "no guard needed" branch - not DestUntracked, which would set the
// node's own file aside as a spurious conflict copy on replay.
func TestReconcileMoveReplayKeepsIdempotentDestBranch(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	// The state after the first apply of move a.txt->b.txt: the node tracked synced at b.txt.
	if err := st.PutBaseline(1, "b.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "h1", LocalHash: "h1", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt"},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("ops = %+v, want one rename", ops)
	}
	if ops[0].DestUntracked || ops[0].DestBaseLocalHash != "" {
		t.Errorf("replay rename = {DestUntracked:%v DestBaseLocalHash:%q}, want the idempotent branch (false, empty)",
			ops[0].DestUntracked, ops[0].DestBaseLocalHash)
	}
}
