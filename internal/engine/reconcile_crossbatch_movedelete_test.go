package engine

// This file pins the fix for the three-batch interleaving
// [modify f@a] + [move a->b] + [delete b], where the delete's supersession drop used to
// destroy the moved node's baseline as a "phantom" while the node's real file was still
// sitting at the un-renamed source.
//
// dropSupersededQueuedDownloads acks the doomed download (correct - its node is gone
// server-side) and then calls dropStrandedBaseline to clear the record reconcile pre-wrote.
// That helper resolves the node through byid - which the batch-2 rename already relocated to
// b - and Lstats THAT path. The on-disk file is still at a (the rename is queued, not
// applied), so the probe reads "no file of the node's kind here" and drops the record. The
// queued rename then finds nodeTracked == false and dead-letters on the foreign-looking
// source, stranding the user's clean file untracked at the old path for the next scan to push
// back to the server as a duplicate create.
//
// The fix consults the queue before deciding a record is a phantom: a live queued inbound
// OpRename for the same node is the materialisation the record is waiting for, so the record
// is not stranded and must be preserved.

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

// seedModifyMoveDelete drives the three batches and returns the store/view dir for assertions.
func seedModifyMoveDelete(t *testing.T, deleteFileID string) (*Engine, *store.Store, string) {
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

	// Batch 1: a server modify. Queues an OpDownload carrying the pre-batch synced base and
	// flips the live record StatePending.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 3, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: md4.ContentHash([]byte("v2")), Size: 2},
	}, 0); err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	// Batch 2: the move. Relocates the (pending) record to b.txt and queues the rename.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
	}, 3); err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	// Batch 3: the delete of the destination. Supersedes the queued download.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "delete", FileID: deleteFileID, Path: "b.txt"},
	}, 5); err != nil {
		t.Fatalf("reconcile batch3: %v", err)
	}
	return e, st, viewDir
}

// TestSupersededDownloadKeepsBaselineCarriedByQueuedRename pins the store half of the fix: the
// superseded download's drop must not take the record a still-queued rename owns.
func TestSupersededDownloadKeepsBaselineCarriedByQueuedRename(t *testing.T) {
	_, st, _ := seedModifyMoveDelete(t, "srv1")

	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("b.txt baseline dropped as a phantom while its rename is still queued: %v", err)
	}
	if b.FileID != "srv1" {
		t.Errorf("b.txt baseline = %+v, want the moved node srv1", b)
	}
	if p, perr := st.PathByID(7, "srv1"); perr != nil || p != "b.txt" {
		t.Errorf("PathByID(srv1) = %q, %v; want b.txt indexed", p, perr)
	}
}

// TestCrossBatchModifyMoveDeleteCleanRemoval is the end-to-end half: the clean file must be
// deleted outright, with no _Conflict copy and no leftover baseline.
func TestCrossBatchModifyMoveDeleteCleanRemoval(t *testing.T) {
	e, st, viewDir := seedModifyMoveDelete(t, "srv1")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertViewDirEmpty(t, viewDir)
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (a clean cross-batch delete must not resurrect the file)", recs)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline survived: err=%v, want ErrNotFound", err)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a.txt baseline survived: err=%v, want ErrNotFound", err)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0", n)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestCrossBatchModifyMoveIdlessDeleteCleanRemoval is the sparse id-less delete shape.
func TestCrossBatchModifyMoveIdlessDeleteCleanRemoval(t *testing.T) {
	e, st, viewDir := seedModifyMoveDelete(t, "")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertViewDirEmpty(t, viewDir)
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (clean delete resurrected as a _Conflict copy)", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}
