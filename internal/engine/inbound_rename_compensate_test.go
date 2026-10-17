package engine

// These tests pin the two-client finding H2 fix (measured on two live clients,
// 14.08.2026): an inbound OpRename whose moved node is absent from disk used to be dropped outright, on
// the promise that "a subsequent modify event (or the 6c full-catalog pull) will materialise the
// node". Two live clients proved the promise never arrives - nothing touches that node again, the
// cold start does not re-run (InitialSyncDone is set) and the server's event log compacts the move
// row away - so the node stayed missing from that replica FOREVER while `status` read clean:
// queue empty, no stuck ops, one dead-letter.
//
// The drop now carries a compensating inbound op (OpDownload, OpMkdir for a directory) at the
// move's destination with the same file id, enqueued in the SAME transaction as the trace and the
// Ack, plus the StatePending baseline that arms the compensation's own never-lose gate.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDroppedRenameCompensatesWithDownload is the core regression: a move of a node this client
// never materialised (source and destination both absent on disk, byid miss) dead-letters - and
// the node then ARRIVES at the destination, because the drop enqueued a download for it. Before
// the fix the drain ended with an empty queue, one dead-letter and no b.txt anywhere, forever.
func TestDroppedRenameCompensatesWithDownload(t *testing.T) {
	body := []byte("the node the move pointed at")
	hash := md4.ContentHash(body)
	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": body},
		downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: hash, Size: int64(len(body)), MTime: 555}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt", ServerSyncID: 42,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// One drain pass: the rename drops, its compensation is appended at the queue tail and the
	// same pass's paging loop applies it.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The drop is still observable - the operator must see the rename did not apply.
	dl, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dl) != 1 || dl[0].Type != store.OpRename {
		t.Fatalf("dead letters = %+v, want one dropped rename", dl)
	}
	if !strings.Contains(dl[0].Reason, "re-requested") || !strings.Contains(dl[0].Reason, "b.txt") {
		t.Errorf("dead-letter reason = %q, want it to name the re-requested node at b.txt", dl[0].Reason)
	}

	// THE FIX: the node materialised at the move's destination.
	got, err := os.ReadFile(filepath.Join(viewDir, "b.txt"))
	if err != nil || string(got) != string(body) {
		t.Fatalf("b.txt = %q (err %v), want the re-requested node %q", got, err, body)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.FileID != "fmov" || b.State != store.StateSynced {
		t.Fatalf("b.txt baseline = %+v (err %v), want synced fmov", b, err)
	}
	if b.ServerSyncID != 42 {
		t.Errorf("b.txt ServerSyncID = %d, want the move event's 42 carried through", b.ServerSyncID)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// Nothing was left at the abandoned source.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt exists after the compensated drop: %v", err)
	}
}

// TestDroppedDirRenameCompensatesWithMkdir is the directory arm: a dir move whose subtree is
// absent from disk drops, cascades its relocated descendant baselines
// (unchanged) and compensates with an inbound OpMkdir at the destination - so the directory
// itself stops being permanently missing from the replica.
func TestDroppedDirRenameCompensatesWithMkdir(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	fBytes := []byte("descendant-content")
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f", store.Baseline{
		FileID: "ff", FileType: "file", ServerHash: "hf",
		LocalHash: md4.ContentHash(fBytes), LocalSize: int64(len(fBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed descendant baseline: %v", err)
	}
	// Keep the view non-empty so a later localReconcile takes the diff path.
	keepBytes := []byte("keep")
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keepBytes, 0o644); err != nil {
		t.Fatalf("seed keep file: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash(keepBytes), LocalSize: int64(len(keepBytes)),
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keep baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fd", FromPath: "d", Path: "e", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the dir rename still drops)", n)
	}
	// THE FIX: the directory materialised at the destination.
	if fi, err := os.Lstat(filepath.Join(viewDir, "e")); err != nil || !fi.IsDir() {
		t.Fatalf("e on disk = %v (err %v), want the re-requested directory", fi, err)
	}
	b, err := st.GetBaseline(7, "e")
	if err != nil || b.FileID != "fd" || b.FileType != "dir" || b.State != store.StateSynced {
		t.Fatalf("e baseline = %+v (err %v), want synced dir fd", b, err)
	}
	// The stranded descendant cleanup is unchanged: e/f was never materialised, so no record
	// claims it.
	if _, err := st.GetBaseline(7, "e/f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("e/f baseline = %v, want ErrNotFound (the cascade still runs)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// And no spurious outbound delete of the freshly re-created node.
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted %+v; want no OpRemove after the compensation", op)
		}
	}
}

// TestDroppedRenameCompensationPreservesOccupiedDest covers the "destination occupied by foreign
// content" case the plan deliberately does NOT special-case: the compensating download runs
// through the ordinary never-lose funnel. That funnel only fires when a not-yet-synced record
// sits at the target, which the drop had just cleaned away - so the compensation writes it back
// (trackCompensation). Without that write the repair would overwrite the user's file directly.
func TestDroppedRenameCompensationPreservesOccupiedDest(t *testing.T) {
	ts := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("the user's own file at the move destination")
	serverBytes := []byte("the moved server node")
	userHash := md4.ContentHash(userBytes)
	serverHash := md4.ContentHash(serverBytes)
	serverNode := actions.Node{FileID: "fmov", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555}

	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": serverBytes},
		downloadNode:    map[string]actions.Node{"fmov": serverNode},
		queryNode:       map[string]actions.Node{"b.txt": serverNode},
		uploadNode: map[string]actions.Node{
			filepath.Base(conflictCopyName("b.txt", host, ts)): {FileID: "copyf", Hash: userHash, MTime: 777},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The user's untracked file sits exactly where the moved node belongs.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The user's bytes survive as a keep-both copy...
	copyRel := conflictCopyName("b.txt", host, ts)
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy %s = %q (err %v), want the user file %q", copyRel, got, err, userBytes)
	}
	// ...and the re-requested server node lands on the canonical path.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved server node %q", got, err, serverBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" {
		t.Fatalf("conflict records = %+v, want one for b.txt", recs)
	}
}

// TestDroppedRenameChainCompensatesOnce pins the duplicate guard: a same-node chain a->b->c whose
// hops BOTH drop must re-request the node once, at its final destination. Compensating each hop
// would land the file at the abandoned path b as well as at c, and markSynced would then evict one
// of the two baselines through the byid index - leaving an untracked duplicate the next scan
// re-uploads as a second server node.
func TestDroppedRenameChainCompensatesOnce(t *testing.T) {
	body := []byte("chain node")
	hash := md4.ContentHash(body)
	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": body},
		downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: hash, Size: int64(len(body))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, FileID: "fmov", Path: "a.txt", NewPath: "b.txt"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, FileID: "fmov", Path: "b.txt", NewPath: "c.txt"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %+v: %v", op, err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if n, _ := st.DeadLetterLen(); n != 2 {
		t.Fatalf("dead letters = %d, want 2 (both hops drop)", n)
	}
	// Exactly one materialisation, at the chain's final destination.
	if _, err := os.Lstat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt exists: %v - the abandoned hop must not be re-requested", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "c.txt"))
	if err != nil || string(got) != string(body) {
		t.Fatalf("c.txt = %q (err %v), want the node at the chain's destination", got, err)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline = %v, want ErrNotFound (no record at the abandoned hop)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestDroppedIDLessRenameCompensationSkipped pins the two drops that must stay bare. An id-less
// move has nothing to ask the server for (the download verb addresses a node by id), and the
// kept-synced-destination marker arm is the revertRelocatedRemove shape, where re-requesting the
// node would resurrect the very file the user deleted locally.
//
// The suppression is asserted STRUCTURALLY - no transfer was requested and the destination is
// still absent from disk - never by an empty queue. A compensation that IS enqueued is applied
// inside the same drainQueue pass (that is exactly what the positive tests above rely on), so
// PendingLen == 0 holds either way and would let both regressions ship green. In the id-less case
// the regression enqueues an OpDownload with an empty FileID, which applyDownload has no guard
// against and promotes as an empty file; in the marker case it downloads back precisely the file
// the user deleted, and the resulting markSynced reproduces the very baseline the state assertion
// below checks for.
func TestDroppedIDLessRenameCompensationSkipped(t *testing.T) {
	t.Run("id-less move", func(t *testing.T) {
		c := &pushClient{}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		if _, err := st.Enqueue(store.Op{
			Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
			FileID: "", Path: "d", NewPath: "e",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}
		if n, err := st.DeadLetterLen(); err != nil || n != 1 {
			t.Fatalf("dead letters = %d (err %v), want 1", n, err)
		}
		if n, err := st.PendingLen(); err != nil || n != 0 {
			t.Fatalf("PendingLen = %d (err %v), want no compensation for an id-less move", n, err)
		}
		if c.downloadCalls != 0 {
			t.Errorf("Download calls = %d, want 0: there is no node id to ask the server for", c.downloadCalls)
		}
		if _, err := os.Lstat(filepath.Join(viewDir, "e")); !os.IsNotExist(err) {
			t.Errorf("Lstat(e) = %v, want IsNotExist: a compensation would have materialised an empty node there", err)
		}
	})

	t.Run("synced destination marker", func(t *testing.T) {
		c := &pushClient{
			downloadContent: map[string][]byte{"fmov": []byte("the-file-the-user-deleted")},
			downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: "h", Size: 25}},
		}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		// The marker revertRelocatedRemove leaves: our node recorded StateSynced at the move's
		// destination with no file on disk, so detectRemoves re-emits the user's local delete there.
		if err := st.PutBaseline(7, "e.txt", store.Baseline{
			FileID: "fmov", FileType: "file", LocalHash: "h", State: store.StateSynced,
		}); err != nil {
			t.Fatalf("seed marker baseline: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
			FileID: "fmov", Path: "d.txt", NewPath: "e.txt",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}
		if n, err := st.DeadLetterLen(); err != nil || n != 1 {
			t.Fatalf("dead letters = %d (err %v), want 1", n, err)
		}
		if n, err := st.PendingLen(); err != nil || n != 0 {
			t.Fatalf("PendingLen = %d (err %v), want no compensation over the delete-re-emission marker", n, err)
		}
		if c.downloadCalls != 0 {
			t.Errorf("Download calls = %d, want 0: re-requesting the node resurrects the user's deleted file", c.downloadCalls)
		}
		if _, err := os.Lstat(filepath.Join(viewDir, "e.txt")); !os.IsNotExist(err) {
			t.Errorf("Lstat(e.txt) = %v, want IsNotExist: the user's delete must stay applied on disk", err)
		}
		// The marker itself is untouched, so the pending local delete still re-emits.
		b, err := st.GetBaseline(7, "e.txt")
		if err != nil || b.State != store.StateSynced || b.FileID != "fmov" {
			t.Fatalf("e.txt baseline = %+v (err %v), want the synced marker kept", b, err)
		}
	})
}

// TestDroppedRenameCompensatesForeignSourceDrop covers applyRename's other two drop sites - the
// ones reached BEFORE the gone-source branch - so the repair is not confined to the cold-start-gap
// path: a source path held by a different node, and a source held by an untracked entity of the
// wrong kind, both while our own node is untracked.
func TestDroppedRenameCompensatesForeignSourceDrop(t *testing.T) {
	body := []byte("moved node bytes")
	hash := md4.ContentHash(body)

	t.Run("foreign node at the source", func(t *testing.T) {
		c := &pushClient{
			downloadContent: map[string][]byte{"fmov": body},
			downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: hash, Size: int64(len(body))}},
		}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		// A DIFFERENT node's baseline sits at the rename source while ours is untracked.
		if err := st.PutBaseline(7, "a.txt", store.Baseline{
			FileID: "fother", FileType: "file", State: store.StatePending,
		}); err != nil {
			t.Fatalf("seed foreign source baseline: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
			FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}
		if n, _ := st.DeadLetterLen(); n != 1 {
			t.Fatalf("dead letters = %d, want 1", n)
		}
		if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(body) {
			t.Fatalf("b.txt = %q (err %v), want the re-requested node", got, err)
		}
		// The foreign source record is untouched by the repair.
		if b, err := st.GetBaseline(7, "a.txt"); err != nil || b.FileID != "fother" {
			t.Fatalf("a.txt baseline = %+v (err %v), want the foreign record intact", b, err)
		}
	})

	t.Run("untracked entity of the wrong kind at the source", func(t *testing.T) {
		c := &pushClient{
			downloadContent: map[string][]byte{"fmov": body},
			downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: hash, Size: int64(len(body))}},
		}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		// The server moves a FILE while the local source path holds the user's own directory.
		if err := os.MkdirAll(filepath.Join(viewDir, "a.txt"), 0o755); err != nil {
			t.Fatalf("seed wrong-kind source: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
			FileID: "fmov", Path: "a.txt", NewPath: "b.txt", IsDir: false,
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}
		if n, _ := st.DeadLetterLen(); n != 1 {
			t.Fatalf("dead letters = %d, want 1", n)
		}
		if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(body) {
			t.Fatalf("b.txt = %q (err %v), want the re-requested node", got, err)
		}
		// The user's directory is untouched.
		if fi, err := os.Lstat(filepath.Join(viewDir, "a.txt")); err != nil || !fi.IsDir() {
			t.Fatalf("a.txt = %v (err %v), want the user's directory intact", fi, err)
		}
	})
}

// TestDroppedRenameSkipsCompensationForNodeTrackedElsewhere pins the byid guard. Every
// compensating drop reasons from "our node was never materialised", but the executor's
// doomed-feeder arm runs dropStrandedBaseline first, and that helper deliberately PRESERVES a
// pending record whose resolved path holds a real entity of the node's kind - the node IS on disk
// there. Re-requesting it at the rename's destination would download a second copy, and the
// pending record's own write would evict the preserved one through the byid index (one path per
// file id), orphaning a live file for the next scan to re-upload as a duplicate server node.
func TestDroppedRenameSkipsCompensationForNodeTrackedElsewhere(t *testing.T) {
	body := []byte("the node, already on disk somewhere else")
	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": body},
		downloadNode:    map[string]actions.Node{"fmov": {FileID: "fmov", Hash: md4.ContentHash(body), Size: int64(len(body))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// The node is materialised (and tracked) at "elsewhere.txt": exactly the shape
	// dropStrandedBaseline preserves rather than dropping.
	if err := os.WriteFile(filepath.Join(viewDir, "elsewhere.txt"), body, 0o644); err != nil {
		t.Fatalf("seed live file: %v", err)
	}
	if err := st.PutBaseline(7, "elsewhere.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: md4.ContentHash(body),
		LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed live baseline: %v", err)
	}
	// A foreign node's record at the rename source with ours tracked away from it: the drop arm
	// that fires when the on-disk source is not ours.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fother", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed foreign source baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The live file keeps its baseline - the whole point of the guard.
	b, err := st.GetBaseline(7, "elsewhere.txt")
	if err != nil || b.FileID != "fmov" {
		t.Fatalf("elsewhere.txt baseline = %+v (err %v), want the live record intact", b, err)
	}
	if p, err := st.PathByID(7, "fmov"); err != nil || p != "elsewhere.txt" {
		t.Errorf("PathByID(fmov) = %q (err %v), want elsewhere.txt", p, err)
	}
	// No second copy was downloaded at the destination, and no repair was queued for one.
	if _, err := os.Lstat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt exists: %v; the node is already on disk elsewhere, so no repair was owed", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops queued = %d, want 0 (no compensation for a node tracked elsewhere)", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead-letters = %+v, want the drop still traced", dls)
	}
	if strings.Contains(dls[0].Reason, "re-requested") {
		t.Errorf("dead-letter reason = %q, want no re-request clause", dls[0].Reason)
	}
}

// TestRenameCompensationSkipsTheDeleteReEmissionMarker pins the one destination record a dropped
// rename must never re-request its node onto: the StateSynced marker revertRelocatedRemove leaves
// so the user's pending LOCAL delete is re-derived on the moved node's new path.
//
// applyRename's own cold-start-gap arm already refuses to compensate over it, but the executor's
// doomed-feeder arm does not go through applyRename, and dropStrandedBaseline deliberately
// PRESERVES a synced record - so that arm reached renameCompensation with the marker intact and
// the compensation was accepted: an inbound download that re-materialises exactly the file the
// user deleted, racing the outbound delete the marker exists to produce. The exclusion belongs in
// the shared entry point, where both drop sites obey it.
func TestRenameCompensationSkipsTheDeleteReEmissionMarker(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed the re-emission marker: %v", err)
	}
	qo := store.QueuedOp{Seq: 1, Op: store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}}

	comp, err := e.renameCompensation(dropRenameCompensated(qo.Op, false), qo)
	if err != nil {
		t.Fatalf("renameCompensation: %v", err)
	}
	if comp != nil {
		t.Fatalf("compensation = %+v, want none: re-requesting the node here resurrects the deleted file", comp)
	}

	// A record at the destination that is NOT the marker (still pending) leaves the repair owed:
	// the exclusion must be the synced marker alone, not "any record at the destination".
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", State: store.StatePending,
	}); err != nil {
		t.Fatalf("re-seed as pending: %v", err)
	}
	comp, err = e.renameCompensation(dropRenameCompensated(qo.Op, false), qo)
	if err != nil {
		t.Fatalf("renameCompensation (pending): %v", err)
	}
	if comp == nil || comp.Type != store.OpDownload || comp.Path != "b.txt" {
		t.Fatalf("compensation = %+v, want the download at b.txt still owed", comp)
	}
}

// TestDroppedRenameCompensationRecordIsAtomicWithTheDrop pins that the compensation's pending
// baseline commits with the trace, the ack and the compensating op - not before them. Written
// before, it survives a failed drop, and the byid-indexed claim it leaves at the destination is
// exactly what makes the STILL-QUEUED rename read its never-materialised node as tracked on
// replay: applyRename's foreign-source and wrong-kind guards both key on that miss, so the replay
// would fall through to the on-disk rename and haul the foreign source content onto the
// destination under the moved node's identity.
func TestDroppedRenameCompensationRecordIsAtomicWithTheDrop(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// The user's own directory sits at the rename source while the server moves a FILE: the
	// wrong-kind drop arm.
	if err := os.MkdirAll(filepath.Join(viewDir, "a.txt"), 0o755); err != nil {
		t.Fatalf("seed wrong-kind source: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt", IsDir: false,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	orig := recordDeadLetterAndAck
	recordDeadLetterAndAck = func(*Engine, store.DeadLetter, *store.Op, *store.TrackedBaseline, uint64) (uint64, error) {
		return 0, errors.New("commit failed")
	}
	t.Cleanup(func() { recordDeadLetterAndAck = orig })

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the failed commit surfaced, got nil")
	}

	// Nothing committed: no trace, the op still queued for a replay that must re-derive the
	// same drop...
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead-letters = %d, want 0 after a failed commit", n)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("ops queued = %d, want the rename left queued", n)
	}
	// ...and, critically, no record at the destination claiming the node is tracked there.
	if _, err := st.GetBaseline(7, "b.txt"); !isNotFound(err) {
		t.Errorf("GetBaseline(b.txt) err = %v, want not-found: the compensation's record must not "+
			"outlive a drop that never committed", err)
	}
	if _, err := st.PathByID(7, "fmov"); !isNotFound(err) {
		t.Errorf("PathByID(fmov) err = %v, want not-found: an indexed claim here disarms the "+
			"replayed rename's foreign-source guards", err)
	}

	// The replay, with the commit healthy, re-derives the same drop and leaves the user's
	// directory untouched.
	recordDeadLetterAndAck = orig
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("replay drainQueue: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(viewDir, "a.txt")); err != nil || !fi.IsDir() {
		t.Fatalf("a.txt = %v (err %v), want the user's directory intact after the replay", fi, err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("dead-letters after replay = %d, want 1", n)
	}
}

// TestRenameDropCompensationIgnoresForeignSourceKind pins where a dropped rename's compensation
// may NOT take its kind from. op.MovedBaseIsDir looks like a witness for the moved node, but
// reconcile captures it through snap.node(ev.FileID, ev.FromPath), which falls back to the record
// tracked at the SOURCE PATH the moment the file id misses byid - and a byid miss is the
// precondition of every arm that drops with a compensation. So in exactly these shapes the field
// describes whoever squats the source, not the node being moved.
//
// Here a foreign DIRECTORY is tracked at the source while the moved node is a FILE the client has
// never materialised. Typed from that record the repair became an inbound OpMkdir carrying the
// file's id: applyMkdir creates a real directory and markSyncs it as fd, the server file is never
// fetched, and a later local delete of that directory pushes an outbound OpRemove of the live
// server FILE. The kind must come from a baseline that IDENTIFIES the node (resolveMovedIsDir) or
// from the event; unprovable stays false, which is the download - a repair that can only stall,
// never one that destroys the node it was meant to restore.
func TestRenameDropCompensationIgnoresForeignSourceKind(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A DIFFERENT node - a directory - holds the move's source, on disk and in the store, while
	// the moved node fd is tracked nowhere. That is applyRename's foreign-source drop arm, and it
	// is also the exact shape in which reconcile's capture reads this foreign record's kind.
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed source occupant: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{
		FileID: "other", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign source baseline: %v", err)
	}

	// The fake serves bytes for fd: the moved node really is a FILE.
	dl := &dlClient{
		content: map[string][]byte{"fd": []byte("fd-bytes")},
		nodes:   map[string]actions.Node{"fd": {Hash: md4.ContentHash([]byte("fd-bytes")), Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a", NewPath: "c", FileID: "fd", ServerSyncID: 71,
		// What reconcile's path fallback captures off the FOREIGN directory above.
		MovedBaseSynced: true, MovedBaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, err := os.Lstat(filepath.Join(viewDir, "c"))
	if err != nil {
		t.Fatalf("the dropped rename left nothing at its destination: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("c is a directory: the compensation took its kind from the FOREIGN record at the source")
	}
	if b, err := st.GetBaseline(7, "c"); err != nil || b.FileID != "fd" || b.FileType == "dir" ||
		b.State != store.StateSynced {
		t.Fatalf("c baseline = %+v (err %v), want the synced FILE fd", b, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops queued = %d, want the compensation applied within the pass", n)
	}
	// The foreign source node is the user's own content and was never ours: it stays untouched.
	if fi, err := os.Lstat(filepath.Join(viewDir, "a")); err != nil || !fi.IsDir() {
		t.Errorf("the foreign source directory was disturbed: err=%v", err)
	}
}

// TestDoomedRenameCompensationIgnoresForeignSourceKind is the same rule on the executor's
// doomed-feeder arm, which is the one drop site with NO baseline left to read: the feeder is
// matched to the rename by file id, so the feeder's own dead-letter cleanup drops the relocated
// record through byid before the rename is compensated. That leaves the event as the only witness,
// and op.MovedBaseIsDir must not stand in for it - a doomed move of a FILE whose source path is
// held by a foreign directory would otherwise be repaired as a directory.
func TestDoomedRenameCompensationIgnoresForeignSourceKind(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// The feeder's download is killed through the fake transport's own seam, not by parking a
	// DIRECTORY on its target: a directory occupant is now something the download RESOLVES
	// (preserved aside as a keep-both copy, server file landed), so that shape no longer dooms
	// anything. Why the feeder dies is not this test's subject.

	// The fake serves bytes for fd: the moved node really is a FILE.
	dl := &dlClient{
		content: map[string][]byte{"fd": []byte("fd-bytes")},
		nodes:   map[string]actions.Node{"fd": {Hash: md4.ContentHash([]byte("fd-bytes")), Size: 8}},
		// Terminal for the feeder's attempt only; the compensating re-request must land.
		dlErrOnce: map[string]error{"fd": errRejectedPath},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue feeder: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a", NewPath: "c", FileID: "fd", ServerSyncID: 71,
		// The path-fallback capture again: a directory's kind, read off a record that is not
		// this node's.
		MovedBaseSynced: true, MovedBaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, err := os.Lstat(filepath.Join(viewDir, "c"))
	if err != nil {
		t.Fatalf("the doomed rename left nothing at its destination: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("c is a directory: the compensation took its kind from a record that is not the moved node's")
	}
	b, err := st.GetBaseline(7, "c")
	if err != nil || b.FileID != "fd" || b.FileType == "dir" || b.State != store.StateSynced {
		t.Fatalf("c baseline = %+v (err %v), want the synced FILE fd", b, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops queued = %d, want the compensation applied within the pass", n)
	}
}

// TestDroppedRenameCompensationAdoptsTheTrackedSpelling pins the byid-eviction hazard
// compensationTracking's doc comment claims to avoid, on the one input that used to defeat it: a
// destination the node is tracked at under a DIFFERENT SPELLING (NFD vs the op's NFC).
//
// renameCompensation compares fold keys, so it correctly reads "tracked at this very path" and
// lets the repair through - but compensationTracking then read the OP's spelling, missed the
// record, and returned a bare StatePending TrackedBaseline for it. Writing that record deletes the
// live one through the byid index (one path per file id), replacing a real baseline with an empty
// pending one: the file at the destination is orphaned and the repair's own download then
// preserves it as a spurious _Conflict copy. The compensation must be addressed at the spelling
// the STORE holds, as every other path-addressed verb does (trackedSpelling).
//
// The two halves are driven directly, because the end-to-end route cannot reach them: a record at
// the destination makes applyRename's nodeTracked true, so no drop arm fires at all and the rename
// resolves as an ordinary idempotent replay - a drain-level fixture asserts only that nothing
// happened, which it would do just as happily with the adoption deleted.
func TestDroppedRenameCompensationAdoptsTheTrackedSpelling(t *testing.T) {
	if normpath.Key(nfcName) != normpath.Key(nfdName) {
		t.Fatalf("fixture broken: %q and %q must fold to one key", nfcName, nfdName)
	}
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	// The node IS tracked at the destination, under the OTHER spelling and NOT synced (a synced
	// record is the delete re-emission marker, which is excluded by a different arm).
	if err := st.PutBaseline(7, nfdName, store.Baseline{
		FileID: "fmov", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed the tracked record: %v", err)
	}
	qo := store.QueuedOp{Seq: 1, Op: store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: nfcName,
	}}

	comp, err := e.renameCompensation(dropRenameCompensated(qo.Op, false), qo)
	if err != nil {
		t.Fatalf("renameCompensation: %v", err)
	}
	if comp == nil {
		t.Fatal("compensation = nil, want the repair owed at the tracked spelling")
	}
	if comp.Path != nfdName {
		t.Errorf("compensation path = %q, want the STORE's spelling %q", comp.Path, nfdName)
	}

	// And with the spelling adopted, the tracking write is correctly suppressed: a record already
	// stands there, so writing another would evict it through the byid index.
	track, terr := e.compensationTracking(*comp)
	if terr != nil {
		t.Fatalf("compensationTracking: %v", terr)
	}
	if track != nil {
		t.Errorf("tracking = %+v, want none: a record already holds the destination", track)
	}

	// The regression itself: addressed at the OP's spelling instead, the tracking write is not
	// suppressed - which is the eviction this adoption exists to prevent.
	atOpSpelling := *comp
	atOpSpelling.Path = nfcName
	stray, serr := e.compensationTracking(atOpSpelling)
	if serr != nil {
		t.Fatalf("compensationTracking (op spelling): %v", serr)
	}
	if stray == nil {
		t.Fatal("fixture broken: the op's spelling must miss the record, else this pins nothing")
	}
	if _, err := st.PathByID(7, "fmov"); err != nil {
		t.Fatalf("PathByID(fmov) = %v; the seeded record must own the id", err)
	}
}

// TestDroppedRenameErrorMessageNamesTheRepair pins the drop's operator-facing text in both shapes.
// Every other case reads the structured compensation, so the message was produced nowhere - yet it
// is what a `%v` log or a wrapped chain shows for a rename the client discarded, and whether it
// left a repair behind is the first thing an operator needs from it.
func TestDroppedRenameErrorMessageNamesTheRepair(t *testing.T) {
	bare := (&droppedRename{}).Error()
	if !strings.Contains(bare, "dropped") || strings.Contains(bare, "re-requesting") {
		t.Errorf("bare drop message = %q, want it to say only that the rename was dropped", bare)
	}
	withComp := (&droppedRename{compensation: &store.Op{Type: store.OpDownload, Path: "b.txt"}}).Error()
	if !strings.Contains(withComp, "b.txt") || !strings.Contains(withComp, string(store.OpDownload)) {
		t.Errorf("compensated drop message = %q, want it to name the repair and its path", withComp)
	}
}

// TestRenameCompensationSkipsAnUndecodableDestinationRecord pins the read arm of the
// re-emission-marker guard. The marker check used to discard its store error, so a record that no
// longer decodes read as "no marker here" and the compensation was accepted - downloading back
// precisely the file the user deleted, which is the outcome the marker exists to prevent.
//
// An undecodable record cannot RULE THE MARKER OUT, and the compensation is the destructive
// direction here (it re-materialises a node), so the repair is suppressed. That is the mirror of
// how compensationTracking reads the same sentinel: it too declines to act on a record it cannot
// read. Only a definitive "no record at this path" proves there is no marker.
func TestRenameCompensationSkipsAnUndecodableDestinationRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// The byid index must resolve the node to b.txt so renameCompensation reaches the marker
	// check at all; the record's VALUE is what the test then makes unreadable.
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed the destination record: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, 7, "b.txt")
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	e := New(st2, &concTransport{client: &pushClient{}}, t.TempDir(), "sess", nil)

	qo := store.QueuedOp{Seq: 1, Op: store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}}
	comp, cerr := e.renameCompensation(dropRenameCompensated(qo.Op, false), qo)
	if cerr != nil {
		t.Fatalf("renameCompensation = %v, want the repair suppressed, not a failure", cerr)
	}
	if comp != nil {
		t.Fatalf("compensation = %+v, want none: an unreadable record may BE the marker, and "+
			"re-requesting the node over it resurrects the deleted file", comp)
	}
}
