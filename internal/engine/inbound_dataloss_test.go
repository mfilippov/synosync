package engine

// These tests pin the never-lose doctrine (DESIGN §6, Law #1): the inbound apply paths that
// destroy local content - applyRename's destination overwrite, applyRemove, and a fresh-create
// applyDownload - must not silently clobber a locally-diverged or untracked file. Each reproduces
// the silent clobber and confirms the keep-both outcome: the divergent local content survives as a
// _Conflict copy, a durable store.Conflict is recorded, and the server wins the canonical path.
// They go through the real reconcile so the reconcile-time divergence capture runs, mirroring the
// inbound download divergence suite (inbound_divergence_test.go).

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

// TestInboundRenameOverwriteFunnelsUntrackedDest is the cluster-A rename regression: a server move
// onto a path where the user concurrently created an UNTRACKED file must not clobber it. The move's
// content lands on the destination (server authority) while the user's file is preserved as a
// _Conflict copy.
func TestInboundRenameOverwriteFunnelsUntrackedDest(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	movedBytes := []byte("the-moved-node-content")
	userBytes := []byte("user-created-untracked-at-dest")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// a.txt is the tracked node being moved; b.txt is an untracked user file at the destination.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "ha", LocalHash: md4.ContentHash(movedBytes),
		LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The moved node won the destination path...
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	// ...and the user's untracked file survives as the _Conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved user file %q", got, err, userBytes)
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

// TestInboundRenameEmptyFileIDFunnelsPendingDest is a regression test: a sparse/empty-id server
// move event (ev.FileID == "") onto a path holding a not-yet-uploaded local file (a StatePending
// baseline whose own FileID is also "") must NOT take the "destination already holds THIS node"
// idempotent-replay branch - the "" == "" file-id comparison would wrongly match and skip the
// keep-both guard, letting applyRename clobber the user's file. The guard must fire (DestUntracked
// captured), so the move's content lands on the destination while the user's pending file survives
// as a _Conflict copy. Defense-in-depth: the trigger depends on the server emitting an empty-id
// move, but the gate is correct regardless.
func TestInboundRenameEmptyFileIDFunnelsPendingDest(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	movedBytes := []byte("the-moved-node-content")
	userBytes := []byte("user-pending-upload-empty-id-dest")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	// b.txt holds the user's locally-created file; its baseline is StatePending and, like the
	// move event, carries an empty FileID (a not-yet-uploaded node the server never assigned an id).
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	// The source baseline shares the empty FileID so the move is classified as our own node.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "", FileType: "file", LocalHash: md4.ContentHash(movedBytes),
		LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "", FileType: "file", LocalHash: md4.ContentHash(userBytes),
		LocalSize: int64(len(userBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "", FromPath: "a.txt", Path: "b.txt"}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The empty-id move must be captured as an untracked destination, not an idempotent self-replay.
	if len(ops) != 1 || ops[0].Type != store.OpRename || !ops[0].DestUntracked {
		t.Fatalf("reconcile ops = %+v, want one OpRename with DestUntracked=true", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved pending file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
}

// TestInboundRenameOverwriteFunnelsEditedDest is the cluster-A rename regression for a TRACKED but
// locally-edited destination: the cheap scan missed a same-size/mtime edit, so b.txt's baseline
// still records the old synced hash. A server move onto b.txt must preserve the local edit, not
// clobber it.
func TestInboundRenameOverwriteFunnelsEditedDest(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	movedBytes := []byte("the-moved-node-content")
	destSyncedBytes := []byte("dest-as-last-synced")
	destEditBytes := []byte("dest-locally-edited!")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	// b.txt on disk holds the unrecorded local edit; its baseline still records the synced hash.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), destEditBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(movedBytes),
		LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fb", FileType: "file", LocalHash: md4.ContentHash(destSyncedBytes),
		LocalSize: int64(len(destSyncedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(destEditBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved local edit %q", got, err, destEditBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" {
		t.Fatalf("conflict records = %+v, want one b.txt", recs)
	}
}

// TestInboundRenameOverwriteSyncedDestNoConflict confirms the guard does NOT fire on the common
// case: a server move onto a tracked destination whose on-disk content still matches its last sync
// is a server-authoritative overwrite, no keep-both copy.
func TestInboundRenameOverwriteSyncedDestNoConflict(t *testing.T) {
	movedBytes := []byte("the-moved-node-content")
	destBytes := []byte("dest-in-sync")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), destBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(movedBytes), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fb", FileType: "file", LocalHash: md4.ContentHash(destBytes), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (in-sync overwrite is authoritative)", recs)
	}
	// The destination baseline must be re-pointed at the moved node and the source baseline dropped,
	// not merely left without a conflict row - a stale FileID at the destination would still pass the
	// conflict-count assertion above.
	db, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline b.txt: %v", err)
	}
	if db.FileID != "fa" || db.State != store.StateSynced {
		t.Errorf("dest baseline = {FileID:%q State:%v}, want {fa StateSynced}", db.FileID, db.State)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("source baseline a.txt survived the move: err=%v, want ErrNotFound", err)
	}
}

// TestInboundRenameOverwriteFunnelsPendingUploadDest is the HIGH #1 regression: a server move onto a
// path whose baseline is StatePending with a queued outbound upload (the user created/edited the file
// locally; its baseline's LocalHash records the still-unpushed ON-DISK content) must not silently
// clobber it. A hash-vs-base guard would read on-disk == LocalHash as "no divergence" and overwrite
// the user's file; the StateSynced gate on the reconcile-time base capture treats a non-synced foreign
// destination as untracked so the file is preserved whole as a _Conflict copy.
func TestInboundRenameOverwriteFunnelsPendingUploadDest(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	movedBytes := []byte("the-moved-node-content")
	userBytes := []byte("user-pending-upload-at-dest")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	// b.txt holds the user's locally-created file; its baseline is StatePending and its LocalHash
	// records that same on-disk content (an upload queued but not yet pushed).
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(movedBytes),
		LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fb", FileType: "file", LocalHash: md4.ContentHash(userBytes),
		LocalSize: int64(len(userBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved pending file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
}

// TestInboundRenameOverwriteFunnelsConflictDest is the HIGH #1 regression for a StateConflict
// destination: a path left diverged by an earlier keep-both that could not resolve still records its
// divergent ON-DISK content in LocalHash. A server move onto it must preserve the file, not clobber it
// - the StateSynced gate treats the non-synced foreign baseline as untracked.
func TestInboundRenameOverwriteFunnelsConflictDest(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	movedBytes := []byte("the-moved-node-content")
	userBytes := []byte("user-conflicted-content-at-dest")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(movedBytes),
		LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fb", FileType: "file", LocalHash: md4.ContentHash(userBytes),
		LocalSize: int64(len(userBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved conflicted file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
}

// TestInboundRemoveFunnelsLocalDivergence is the cluster-A remove regression: a server delete of a
// file the user locally edited (a cheap-scan-missed edit, so the baseline still records the old
// hash) must preserve the edit as a _Conflict copy rather than os.Remove it.
func TestInboundRemoveFunnelsLocalDivergence(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("original-synced-content")
	editBytes := []byte("local-edit-the-scan-missed")
	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), editBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(origBytes),
		LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path is gone (server delete honoured) but the edit survives as the copy.
	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(editBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s", recs, copyRel)
	}
	if _, err := st.GetBaseline(7, "x.txt"); err == nil {
		t.Errorf("x.txt baseline survived the remove")
	}
}

// TestInboundRemoveInSyncDeletes confirms the remove guard does NOT fire when the on-disk file
// still matches its last-synced hash: the server's delete is honoured normally.
func TestInboundRemoveInSyncDeletes(t *testing.T) {
	origBytes := []byte("original-synced-content")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(origBytes), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived an in-sync delete: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (in-sync delete is authoritative)", recs)
	}
}

// TestInboundRemoveSyncedNoHashPreserves covers removeBaseFor's synced-file-but-no-recorded-hash
// branch: a StateSynced FILE baseline with an empty LocalHash is anomalous - markSynced
// always derives a hash, so the only producers are the inbound-rename bare/promoted-pending writes
// that relocate an untracked/foreign on-disk file onto a path. There is no trustworthy divergence
// base, so a server delete arriving for it must PRESERVE the on-disk file whole as a _Conflict copy
// (never-lose) rather than honour the delete under a false server-authority read. Before this guard
// existed, this returned ok=false and the user's file was deleted with no copy.
func TestInboundRemoveSyncedNoHashPreserves(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("on-disk-content-no-baseline-hash")
	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// StateSynced FILE record with an empty LocalHash - the anomalous inbound-rename bare-write state.
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: "", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// A StateSynced reconcile-time baseline is captured as BaseWasSynced=true with an empty
	// BaseLocalHash; removeBaseFor then returns an empty base (preserve whole) at apply time.
	if len(ops) != 1 || ops[0].Type != store.OpRemove || !ops[0].BaseWasSynced || ops[0].BaseLocalHash != "" {
		t.Fatalf("reconcile ops = %+v, want one OpRemove with BaseWasSynced=true and empty BaseLocalHash", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path is gone (server delete applied to the now-vacated path) but the file
	// survives whole as the _Conflict copy.
	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s (never-lose preserve)", recs, copyRel)
	}
}

// TestInboundRemovePendingUploadFunnelsDivergence is the removeBaseFor StateSynced-gate regression.
// The user locally created/edited x.txt; its baseline is StatePending with LocalHash
// recording the still-unpushed ON-DISK content (a queued upload). A server delete then arrives.
// flipPending keeps the baseline StatePending and keeps that LocalHash, so without the gate
// removeBaseFor would return it as the divergence base - and guardRemoveDivergence would read
// on-disk == base as "no divergence" and os.Remove the user's unpushed file. The StateSynced gate
// treats the non-synced baseline as untracked so the whole file is preserved as a _Conflict copy.
func TestInboundRemovePendingUploadFunnelsDivergence(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-pending-upload-not-yet-pushed")
	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// StatePending baseline whose LocalHash is the current on-disk (unpushed) content.
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(userBytes),
		LocalSize: int64(len(userBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// A StatePending (queued-upload) reconcile-time baseline must NOT be captured as a
	// trustworthy divergence base, so the unpushed file is preserved whole.
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].BaseWasSynced {
		t.Fatalf("reconcile ops = %+v, want one OpRemove with BaseWasSynced=false", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved pending file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s", recs, copyRel)
	}
}

// TestInboundRemoveConflictBaselineFunnelsDivergence drives the StateConflict case through reconcile
// so it pins the capture mapping (a non-synced baseline yields BaseWasSynced=false), not just the
// downstream preserve. A path left diverged by an earlier unresolvable keep-both records its
// divergent ON-DISK content in LocalHash under a StateConflict baseline; a server delete arriving for
// it must leave BaseWasSynced unset on the produced op so guardRemoveDivergence preserves the file
// whole rather than reading on-disk == LocalHash as "no divergence" and deleting it.
func TestInboundRemoveConflictBaselineFunnelsDivergence(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-conflicted-content-still-on-disk")
	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(userBytes),
		LocalSize: int64(len(userBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// A StateConflict reconcile-time baseline must NOT be captured as a trustworthy
	// divergence base - guardRemoveDivergence then preserves the on-disk file whole.
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].BaseWasSynced {
		t.Fatalf("reconcile ops = %+v, want one OpRemove with BaseWasSynced=false", ops)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved conflicted file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s", recs, copyRel)
	}
}

// TestInboundRemoveDirFunnelsDivergentSubtree is the cluster-A directory regression: a server delete
// of a directory whose subtree holds a locally-edited file must not recursively delete it. The whole
// directory is set aside as a _Conflict copy so no edited descendant is lost.
func TestInboundRemoveDirFunnelsDivergentSubtree(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("child-original-synced")
	editBytes := []byte("child-locally-edited!")
	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "f.txt"), editBytes, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash(origBytes),
		LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The original directory is gone, but its edited child survives under the _Conflict directory.
	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName, "f.txt")); err != nil || string(got) != string(editBytes) {
		t.Fatalf("preserved child = %q (err %v), want the local edit %q", got, err, editBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" {
		t.Fatalf("conflict records = %+v, want one d", recs)
	}
	// The whole subtree moved aside, so neither the directory baseline nor its descendant
	// baseline may linger under the old prefix (a lingering descendant would diff as a
	// vanished file next scan and emit a spurious outbound remove).
	if _, err := st.GetBaseline(7, "d"); err == nil {
		t.Errorf("dir baseline d survived the preserve-aside remove")
	}
	if _, err := st.GetBaseline(7, "d/f.txt"); err == nil {
		t.Errorf("descendant baseline d/f.txt survived the preserve-aside remove")
	}
}

// TestInboundDownloadFreshCreateFunnelsUntracked is the cluster-A fresh-create regression: a brand-
// new server node (no BaseLocalHash) whose target path already holds an untracked local file must
// preserve that file via keep-both, not clobber it. The resolver downloads the server version onto
// the canonical path and keeps the user file as a _Conflict copy.
func TestInboundDownloadFreshCreateFunnelsUntracked(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-untracked-local-file")
	serverBytes := []byte("brand-new-server-content")
	serverHash := md4.ContentHash(serverBytes)
	userHash := md4.ContentHash(userBytes)
	copyRel := conflictCopyName("new.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"new.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: userHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// An untracked user file already sits at the path the server is creating.
	if err := os.WriteFile(filepath.Join(viewDir, "new.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "new.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "new.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("new.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want preserved user file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "new.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one new.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundDownloadFreshCreateNoLocalFileOverwrites confirms the fresh-create guard does NOT fire
// when no local file occupies the target: the server create is materialised directly.
func TestInboundDownloadFreshCreateNoLocalFileOverwrites(t *testing.T) {
	serverBytes := []byte("brand-new-server-content")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "new.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "new.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("new.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (no keep-both)", len(c.uploads))
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
}

// TestInboundDownloadCreateModifyBatchMidTransferEditFunnels is the create+modify regression,
// mid-transfer window: a download with an EMPTY BaseLocalHash (no pre-batch baseline existed when
// its batch was captured) drains onto a path where an earlier apply of the same node already left
// a synced baseline recording the bytes it wrote, and a user edit lands WHILE the download
// streams. The old fresh-create branch saw a synced occupant, picked "normal overwrite" and
// installed no recheck - the edit was replaced with no comparison at all and, the promoted content
// equalling the baseline, no later scan could see the loss. The live synced hash must serve as the
// divergence base for the promote-time recheck so the edit funnels through keep-both.
//
// The between-applies state is seeded directly (mirroring the pre-check-window sibling below):
// a same-batch [create f, modify f] enqueues only ONE download - the
// modify's enqueue drops the create's as superseded - so the historical two-download reconcile
// no longer produces this shape; it still arises through the carried-rename interleaving
// ([create f@a, foreign move a->b, modify f@b], where the queued rename exempts the first
// download from the supersession), whose end-to-end flow is pinned in reconcile_supersede_test.go.
func TestInboundDownloadCreateModifyBatchMidTransferEditFunnels(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	editedBytes := []byte("local-edit-landed-mid-second-transfer")
	serverBytes := []byte("fresh-server-content")
	serverHash := md4.ContentHash(serverBytes)
	editedHash := md4.ContentHash(editedBytes)
	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: editedHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The state exactly between the two applies: the earlier apply's output is on disk and its
	// markSynced baseline records the promoted bytes' hash.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), serverBytes, 0o644); err != nil {
		t.Fatalf("seed first apply's output: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash, ServerSyncID: 5,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}
	// The trailing download as its batch produced it: no pre-batch baseline existed, so no
	// BaseLocalHash.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 556,
		ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The edit lands during the transfer - after the pre-check found the on-disk output still
	// matching the live synced baseline, and before the promote.
	c.downloadHook = func() {
		if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), editedBytes, 0o644); err != nil {
			t.Errorf("write mid-transfer edit: %v", err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The decisive assertion: the mid-transfer edit survives as the _Conflict copy. Before the
	// fix the second promote replaced it and no copy ever existed.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(editedBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one a.txt -> %s", recs, copyRel)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != serverHash || b.ServerSyncID != 6 {
		t.Errorf("baseline = %+v, want synced with server hash %s and ServerSyncID 6", b, serverHash)
	}
}

// TestInboundDownloadCreateModifyBatchEditAfterFirstApplyFunnels is the create+modify
// regression, pre-check window: the state exactly between the two applies of a create+modify
// batch - download #1's apply left a synced baseline recording the bytes it wrote, the user then
// edited the file, and download #2 (BaseLocalHash empty, as the pre-batch snapshot captured) is
// about to drain. The old fresh-create branch classified the synced occupant as a normal
// overwrite with no hash comparison; the live synced hash must serve as the divergence base so
// the pre-check routes the edit into keep-both before any transfer.
func TestInboundDownloadCreateModifyBatchEditAfterFirstApplyFunnels(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	editedBytes := []byte("local-edit-after-first-apply")
	serverBytes := []byte("fresh-server-content")
	serverHash := md4.ContentHash(serverBytes)
	editedHash := md4.ContentHash(editedBytes)
	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: editedHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The edited file sits on disk; the baseline is what download #1's markSynced wrote (synced,
	// LocalHash = the promoted bytes' hash).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), editedBytes, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash, ServerSyncID: 5,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}
	// Download #2 as the batch produced it: no pre-batch baseline existed, so no BaseLocalHash.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 556,
		ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The decisive assertion: the edit survives as the _Conflict copy. Before the fix the synced
	// occupant was overwritten directly and no copy ever existed.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(editedBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one a.txt -> %s", recs, copyRel)
	}
	// The divergence was caught by the pre-check, before any transfer: the only Download call is
	// the resolver's canonical fetch.
	if c.downloadCalls != 1 {
		t.Errorf("Download calls = %d, want 1 (funnelled before the transfer)", c.downloadCalls)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != serverHash || b.ServerSyncID != 6 {
		t.Errorf("baseline = %+v, want synced with server hash %s and ServerSyncID 6", b, serverHash)
	}
}

// TestInboundDownloadCreateModifyBatchNoEditOverwrites pins the no-edit complement of the
// create+modify fix: the same create+modify batch with NO local edit between the applies must
// keep the direct overwrite - the second download's occupant still matches the live synced hash,
// so no keep-both round-trip, no conflict copy and no spurious funnel.
func TestInboundDownloadCreateModifyBatchNoEditOverwrites(t *testing.T) {
	serverBytes := []byte("fresh-server-content")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 6, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 556},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("a.txt = %q (err %v), want %q", got, err, serverBytes)
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0 (no keep-both resolve)", len(c.queryNodeCalls))
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (no keep-both)", len(c.uploads))
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want only a.txt", entries)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != serverHash || b.ServerSyncID != 6 {
		t.Errorf("baseline = %+v, want synced with server hash %s and ServerSyncID 6", b, serverHash)
	}
}

// TestInboundRemoveUntrackedFilePreserved is the cluster-A untracked-file remove regression: a server
// delete of a path with NO baseline but an on-disk regular file (a user file the server never saw)
// must preserve that file as a keep-both copy rather than os.RemoveAll it. Exercises the untracked
// branch of removeBaseFor (empty base, preserve whole).
func TestInboundRemoveUntrackedFilePreserved(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("untracked-user-file-content")
	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// A user file with no baseline sits at the path the server is deleting.
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "x.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "x.txt")); !os.IsNotExist(err) {
		t.Errorf("x.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want preserved untracked file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" {
		t.Fatalf("conflict records = %+v, want one x.txt", recs)
	}
}

// TestInboundRemoveDirUntrackedDescendantPreserved confirms a server delete of a directory whose
// subtree holds an untracked descendant (no baseline) preserves the directory whole rather than
// recursively deleting it. Exercises the untracked-descendant branch of subtreeHasDivergence.
func TestInboundRemoveDirUntrackedDescendantPreserved(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	syncedBytes := []byte("synced-child")
	untrackedBytes := []byte("untracked-child-the-server-never-saw")
	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "synced.txt"), syncedBytes, 0o644); err != nil {
		t.Fatalf("seed synced child: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "untracked.txt"), untrackedBytes, 0o644); err != nil {
		t.Fatalf("seed untracked child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/synced.txt", store.Baseline{
		FileID: "fs", FileType: "file", LocalHash: md4.ContentHash(syncedBytes),
		LocalSize: int64(len(syncedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced child baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName, "untracked.txt")); err != nil || string(got) != string(untrackedBytes) {
		t.Fatalf("preserved untracked child = %q (err %v), want %q", got, err, untrackedBytes)
	}
}

// TestInboundRemoveDirPendingDescendantPreserved is the directory analog of the removeBaseFor
// StateSynced gate: a server delete of a directory whose subtree holds a descendant in a NON-synced
// state (a queued-but-unpushed local edit, StatePending) whose on-disk content equals its own
// unpushed LocalHash. Without gating the subtree walk on StateSynced the hash-vs-LocalHash check
// reads "in sync" and the whole directory - including the unpushed edit - is deleted. With the gate
// the non-synced descendant is treated as divergence and the directory is preserved aside whole.
func TestInboundRemoveDirPendingDescendantPreserved(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	pendingBytes := []byte("unpushed-local-edit-not-yet-on-server")
	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "edited.txt"), pendingBytes, 0o644); err != nil {
		t.Fatalf("seed pending child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// A StatePending descendant: a local edit reconciled into a queued outbound upload, so its
	// LocalHash is the unpushed on-disk content itself - not a trustworthy last-synced base.
	if err := st.PutBaseline(7, "d/edited.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash(pendingBytes),
		LocalSize: int64(len(pendingBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending child baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName, "edited.txt")); err != nil || string(got) != string(pendingBytes) {
		t.Fatalf("preserved pending child = %q (err %v), want %q", got, err, pendingBytes)
	}
}

// TestInboundRemoveDirAllInSyncDeletes confirms the directory remove guard does NOT fire when every
// descendant is in sync with its baseline: the server's recursive delete is honoured. Negative for
// subtreeHasDivergence (no divergence found → normal delete).
func TestInboundRemoveDirAllInSyncDeletes(t *testing.T) {
	childBytes := []byte("in-sync-child-content")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "f.txt"), childBytes, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash(childBytes),
		LocalSize: int64(len(childBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("in-sync directory survived the delete: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (in-sync subtree deletes normally)", recs)
	}
}

// TestInboundRemoveDirDropsDescendantBaselines reproduces the descendant-baseline wedge's root
// cause: a server delete event names only the directory, and applyRemove's clean path dropped
// just the directory's own baseline - every tracked descendant's survived. The next scan then
// diffed each surviving descendant as a locally-vanished file and emitted an outbound OpRemove
// whose absence confirmation (listing the already-deleted server parent) could never conclude,
// wedging the queue permanently. The fix drops the descendant baselines in the same apply, so
// the scan after the delete produces no ops at all.
func TestInboundRemoveDirDropsDescendantBaselines(t *testing.T) {
	childBytes := []byte("in-sync-child")
	nestedBytes := []byte("in-sync-nested-child")
	keepBytes := []byte("unrelated-keeper")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC))

	// d/f.txt and d/sub/g.txt are tracked, fully in-sync descendants. keep.txt is an unrelated
	// in-sync file that keeps the post-delete scan non-empty: the empty-view debounce would
	// otherwise suppress exactly the spurious removes this test asserts are no longer produced.
	if err := os.MkdirAll(filepath.Join(viewDir, "d", "sub"), 0o755); err != nil {
		t.Fatalf("seed dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "f.txt"), childBytes, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "sub", "g.txt"), nestedBytes, 0o644); err != nil {
		t.Fatalf("seed nested child: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keepBytes, 0o644); err != nil {
		t.Fatalf("seed keeper: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/sub", store.Baseline{FileID: "fsub", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed subdir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash(childBytes),
		LocalSize: int64(len(childBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/sub/g.txt", store.Baseline{
		FileID: "fg", FileType: "file", LocalHash: md4.ContentHash(nestedBytes),
		LocalSize: int64(len(nestedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed nested child baseline: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileID: "fk", FileType: "file", LocalHash: md4.ContentHash(keepBytes),
		LocalSize: int64(len(keepBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keeper baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("directory survived the delete: %v", err)
	}
	for _, p := range []string{"d", "d/f.txt", "d/sub", "d/sub/g.txt"} {
		if _, err := st.GetBaseline(7, p); err == nil {
			t.Errorf("baseline %q survived the directory delete", p)
		}
	}
	if _, err := st.GetBaseline(7, "keep.txt"); err != nil {
		t.Errorf("unrelated baseline dropped by the descendant sweep: %v", err)
	}

	// The next scan must see nothing to push: before the fix the surviving descendant
	// baselines diffed as vanished files here and produced outbound OpRemove ops that could
	// never converge against the deleted server parent.
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("next scan emitted %d op(s) %+v, want none", len(ops), ops)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not empty after the post-delete scan: %d", n)
	}
}

// TestInboundRemoveDirUnreadableSubtreePreserved is the fail-safe regression for the walk-error
// branch of subtreeHasDivergence: a server delete of a directory whose subtree contains an
// UNREADABLE subdirectory (chmod 000) cannot be verified in-sync, so the guard conservatively
// treats it as divergence and preserves the whole tree rather than risk a silent recursive delete.
func TestInboundRemoveDirUnreadableSubtreePreserved(t *testing.T) {
	requireUnreadableDir(t)
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "d", "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// Make the subdirectory unreadable so WalkDir surfaces a permission error for it.
	locked := filepath.Join(viewDir, "d", "sub")
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	// The subtree is moved aside under the _Conflict prefix; restore perms there so TempDir cleanup
	// can remove it.
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(viewDir, copyName, "sub"), 0o755) })

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d survived as the canonical path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, copyName, "sub")); err != nil {
		t.Fatalf("unreadable subtree was not preserved aside: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" {
		t.Fatalf("conflict records = %+v, want one d", recs)
	}
}

// TestInboundRemoveDirUnhashableChildPreserved is the fail-safe regression for the hash-error branch
// of subtreeHasDivergence: a tracked child file with a recorded local hash that cannot be hashed
// (chmod 000 → EACCES, not source-gone) cannot be confirmed in-sync, so the guard conservatively
// preserves the whole directory rather than recursively deleting unverifiable content.
func TestInboundRemoveDirUnhashableChildPreserved(t *testing.T) {
	requireUnreadableFile(t)
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	childBytes := []byte("tracked-but-unreadable-child")
	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	child := filepath.Join(viewDir, "d", "f.txt")
	if err := os.WriteFile(child, childBytes, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash(childBytes),
		LocalSize: int64(len(childBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	// The child is tracked with a local hash, but unreadable: hashLocalFileMD4 fails with EACCES
	// (not source-gone), so the subtree cannot be confirmed in-sync.
	if err := os.Chmod(child, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(viewDir, copyName, "f.txt"), 0o644) })

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d survived as the canonical path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, copyName, "f.txt")); err != nil {
		t.Fatalf("unhashable child was not preserved aside: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" {
		t.Fatalf("conflict records = %+v, want one d", recs)
	}
}

// findViewFileWithContent returns the view-relative path of the first regular file under dir
// whose content equals want, or "" when no file holds it. The evacuated-record tests below use
// it because the rename queued behind the funneled download may carry the preserved user file
// across to the move's destination before the resolver names its _Conflict copy: what matters
// is that the bytes survive on disk with a durable conflict record, not which leaf holds them.
func findViewFileWithContent(t *testing.T, dir string, want []byte) string {
	t.Helper()
	found := ""
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || found != "" {
			return err
		}
		got, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if string(got) == string(want) {
			rel, rerr := filepath.Rel(dir, p)
			if rerr != nil {
				return rerr
			}
			found = filepath.ToSlash(rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk view dir: %v", err)
	}
	return found
}

// TestInboundFreshCreateEvacuatedRecordFunnelsUntracked is the evacuated-record regression: a
// same-batch [create X @a, move X a->b]. reconcile enqueues the create's download at a and then
// relocatePending EVACUATES the StatePending record it just wrote from a to b (the move's
// destination), leaving NO baseline at the download's own target. untrackedFilePresent's
// absent-baseline branch read that as "not a reconcile-produced create" and overwrote directly:
// an untracked user file at a was destroyed by the replacing promote with no _Conflict copy and
// no record, and the rename then carried the server bytes to b. The byid index still resolves
// the node (the record moved, it did not vanish), which is positive proof the op IS
// reconcile-produced - so the present file is untracked user content and funnels through
// keep-both, preserving it.
func TestInboundFreshCreateEvacuatedRecordFunnelsUntracked(t *testing.T) {
	ts := time.Date(2026, 7, 24, 9, 15, 2, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-untracked-file-at-the-create-path")
	serverBytes := []byte("brand-new-server-node-content")
	serverHash := md4.ContentHash(serverBytes)
	userHash := md4.ContentHash(userBytes)
	serverNode := actions.Node{FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}

	c := &pushClient{
		queryNode:       map[string]actions.Node{"a.txt": serverNode, "b.txt": serverNode},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": serverNode},
		uploadNode: map[string]actions.Node{
			filepath.Base(conflictCopyName("a.txt", host, ts)): {FileID: "copyf", Hash: userHash, MTime: 777},
			filepath.Base(conflictCopyName("b.txt", host, ts)): {FileID: "copyf", Hash: userHash, MTime: 777},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The user created a file at the path the server is about to create its node on.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed untracked user file: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 6, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The record the fresh-create protection keys on has been carried off the download's target.
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Fatalf("baseline still at a.txt; the evacuation this test models did not happen")
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The user's bytes survive. The funneled download acks, the rename queued behind it carries
	// the still-untouched user file across to the move's destination, and the resolver - which
	// follows the node's byid relocation to b.txt - preserves it there as the _Conflict copy and
	// lands the server version on the canonical path.
	if at := findViewFileWithContent(t, viewDir, userBytes); at == "" {
		t.Fatalf("untracked user file destroyed by the fresh-create promote (no copy on disk)")
	}
	copyRel := conflictCopyName("b.txt", host, ts)
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy %s = %q (err %v), want the user file %q", copyRel, got, err, userBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want the server node %q", got, err, serverBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived the move: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
	dl, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dl) != 0 {
		t.Errorf("dead letters = %+v, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundFreshCreateEvacuatedRecordCrossBatchFunnelsUntracked is the cross-batch half of the
// evacuated-record regression: the create's download fails its first drain transiently and stays
// queued, and the move arrives in a LATER batch - relocatePending then evacuates the pending
// record from under an op that is already in the queue. The gap is the same (no baseline at the
// download's target, byid still resolving the node), so the retry must funnel rather than clobber.
func TestInboundFreshCreateEvacuatedRecordCrossBatchFunnelsUntracked(t *testing.T) {
	ts := time.Date(2026, 7, 24, 9, 15, 2, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-untracked-file-written-before-the-drain")
	serverBytes := []byte("brand-new-server-node-content")
	serverHash := md4.ContentHash(serverBytes)
	userHash := md4.ContentHash(userBytes)
	serverNode := actions.Node{FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}

	c := &pushClient{
		queryNode:       map[string]actions.Node{"a.txt": serverNode, "b.txt": serverNode},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": serverNode},
		uploadNode: map[string]actions.Node{
			filepath.Base(conflictCopyName("b.txt", host, ts)): {FileID: "copyf", Hash: userHash, MTime: 777},
		},
		downloadErr: errors.New("transient server hiccup"),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}, 0); err != nil {
		t.Fatalf("reconcile create: %v", err)
	}
	// The first drain fails transiently; the download stays queued for a later retry.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatalf("drainQueue: want the scripted transient download failure, got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("pending ops after the failed drain = %d, want the download still queued", n)
	}
	// The user then writes at the create's path, and the server moves the node away.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed untracked user file: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}, 5); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	c.downloadErr = nil
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if at := findViewFileWithContent(t, viewDir, userBytes); at == "" {
		t.Fatalf("untracked user file destroyed by the fresh-create promote (no copy on disk)")
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" {
		t.Fatalf("conflict records = %+v, want one b.txt", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundFreshCreateEvacuatedRecordReadOnlyViewPreserves is the read-only-view half of the
// evacuated-record regression, where the exposure window is UNBOUNDED: localScan skips a
// read-only view, so no outbound upload is ever queued for the user's file and reconcile's
// pendingUploads deferral can never fire - the untracked file sits at the create's path until
// the download drains. The keep-both resolver cannot push a copy in a read-only view (it
// dead-letters terminally), but the file must still never be destroyed: the funnel leaves it
// untouched on disk instead of replacing it with the server node.
func TestInboundFreshCreateEvacuatedRecordReadOnlyViewPreserves(t *testing.T) {
	ts := time.Date(2026, 7, 24, 9, 15, 2, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-file-in-a-read-only-view")
	serverBytes := []byte("brand-new-server-node-content")
	serverHash := md4.ContentHash(serverBytes)
	serverNode := actions.Node{FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}

	c := &pushClient{
		queryNode:       map[string]actions.Node{"a.txt": serverNode, "b.txt": serverNode},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": serverNode},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", Privilege: "ro"}); err != nil {
		t.Fatalf("PutView read-only: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed untracked user file: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 6, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if at := findViewFileWithContent(t, viewDir, userBytes); at == "" {
		t.Fatalf("untracked user file destroyed in a read-only view (no copy on disk)")
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %+v, want 0 in a read-only view", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundFreshCreateUntrackedNodeOverwrites pins the behaviour the byid consultation must NOT
// change: a download whose node is tracked NOWHERE (no baseline at the target and no byid entry -
// a directly-seeded op, the shared-dir-view compromise) still overwrites a present local file
// directly, with no keep-both round trip.
func TestInboundFreshCreateUntrackedNodeOverwrites(t *testing.T) {
	userBytes := []byte("pre-existing-local-file")
	serverBytes := []byte("server-node-content")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 24, 9, 15, 2, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("a.txt = %q (err %v), want the server content %q", got, err, serverBytes)
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %v, want 0 (no keep-both resolve)", c.queryNodeCalls)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
}
