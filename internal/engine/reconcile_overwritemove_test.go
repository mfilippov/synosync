package engine

// These tests pin the overwriting-move fix: a server-side OVERWRITING MOVE - a move
// claiming an occupied path as its destination for a DIFFERENT node - destroys the occupant
// node server-side without emitting an explicit delete event. The doomed-download supersession
// machinery used to key exclusively on delete events, so the occupant's queued OpDownload
// failed every drain forever (the inbound download path had no terminal server-absence exit at
// all) while the authoritative OpRename was serialized BEHIND it on the shared path key: a
// permanent wedge in both directions. Three layers close it:
//   - (a) reconcile treats an overwriting move as a supersession trigger - the same-batch
//     elision and dropSupersededQueuedDownloads (now triggered by moves too) drop the doomed
//     download so the rename applies (supersededByLaterDelete's destination-match case);
//   - (b) the apply-time backstop: a download failing with ErrPathError probes the node's
//     absence STRICTLY by file id (nodeGoneByID - leaf-name occupancy by a different id is
//     exactly the overwrite-move shape and must not read as presence) and a positively-gone
//     node resolves as a race (drop + phantom-baseline cleanup, ack, no durable trace);
//   - (c) guardRenameDest live-rechecks the destination baseline: a download that applied
//     between reconcile's capture and the rename's drain slot leaves fresh SERVER bytes on
//     disk under a live StateSynced record, which must not be hauled into a spurious
//     _Conflict copy on the stale pre-batch DestUntracked capture.

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

// assertOverwriteMoveConverged checks the shared postcondition of the (a)-layer tests: the
// moved node's bytes sit at x.txt (synced, tracked by fmov), the source is gone, nothing was
// preserved as a conflict copy, and the queue fully drained with no durable drop trace.
func assertOverwriteMoveConverged(t *testing.T, st *store.Store, viewDir string, movedBytes []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(viewDir, "x.txt"))
	if err != nil || string(got) != string(movedBytes) {
		t.Fatalf("x.txt = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived the move: %v", err)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "x.txt" {
		names := make([]string, len(entries))
		for i, en := range entries {
			names[i] = en.Name()
		}
		t.Errorf("view dir = %v, want only x.txt (a _Conflict copy leaked?)", names)
	}
	b, err := st.GetBaseline(7, "x.txt")
	if err != nil {
		t.Fatalf("GetBaseline(x.txt): %v", err)
	}
	if b.FileID != "fmov" || b.State != store.StateSynced {
		t.Errorf("x.txt baseline = %+v, want synced fmov", b)
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
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0 (a race resolution must leave no Dropped trace)", n)
	}
}

// TestReconcileOverwriteMoveSupersedesOccupantDownload is the same-batch shape:
// [create x (focc), move a->x (fmov)] in one reconcile call. The occupant node focc is
// destroyed by the overwriting move before its download could ever run, so the download must
// be elided (no download turn at all) and the rename must apply. Before the fix the download
// was enqueued, failed ErrPathError every drain (the node is gone server-side), and the
// rename never ran behind it - a permanent wedge.
func TestReconcileOverwriteMoveSupersedesOccupantDownload(t *testing.T) {
	movedBytes := []byte("moved-node-content")
	c := &pushClient{downloadErr: actions.ErrPathError}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}

	ops, cursor, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "x.txt", Hash: "hocc", Size: 4},
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "a.txt", Path: "x.txt"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 6 {
		t.Errorf("cursor = %d, want 6", cursor)
	}
	// The unit-level pin: the occupant's download is elided; only the rename is enqueued.
	if len(ops) != 1 || ops[0].Type != store.OpRename || ops[0].FileID != "fmov" {
		t.Fatalf("ops = %+v, want only the overwriting rename", ops)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if c.downloadCalls != 0 {
		t.Errorf("download turns = %d, want 0 (the doomed download must never reach the server)", c.downloadCalls)
	}
	assertOverwriteMoveConverged(t, st, viewDir, movedBytes)
}

// TestReconcileOverwriteMoveDropsQueuedOccupantDownload is the cross-batch shape: the
// occupant's create arrives in batch 1 (its download queued and not yet applied) and the
// overwriting move in batch 2 - two PullEvent pages of one backlog, or two poll cycles.
// dropSupersededQueuedDownloads must trigger on the move (it used to run only for deletes)
// and drop the doomed download from the durable queue so the rename applies.
func TestReconcileOverwriteMoveDropsQueuedOccupantDownload(t *testing.T) {
	movedBytes := []byte("moved-node-content")
	c := &pushClient{downloadErr: actions.ErrPathError}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}

	// Batch 1: the occupant's create alone - its download is queued durably.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "x.txt", Hash: "hocc", Size: 4},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpDownload {
		t.Fatalf("batch1 ops = %+v, want one download", ops1)
	}

	// Batch 2: the overwriting move. The queued download must be dropped from the queue.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "a.txt", Path: "x.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRename {
		t.Fatalf("batch2 ops = %+v, want one rename", ops2)
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpRename {
		t.Fatalf("queue after batch2 = %+v, want only the rename (occupant download dropped)", pending)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if c.downloadCalls != 0 {
		t.Errorf("download turns = %d, want 0 (the doomed download must never reach the server)", c.downloadCalls)
	}
	assertOverwriteMoveConverged(t, st, viewDir, movedBytes)
}

// TestOverwriteMoveSupersessionMatrix pins supersededByLaterDelete's move-destination rules:
// a foreign node landing exactly on the tracked path supersedes, a move
// OF the tracked node (by id, or by source path for an id-less event) follows it instead, and
// a move landing elsewhere - including on an ANCESTOR directory's path, where a server-side
// directory merge could leave the descendants alive - never supersedes (the apply-time
// node-gone probe is the backstop for that conservative miss).
func TestOverwriteMoveSupersessionMatrix(t *testing.T) {
	cases := []struct {
		name   string
		events []actions.Event
		path   string
		fileID string
		want   bool
	}{
		{"foreign move onto tracked path supersedes",
			[]actions.Event{{Action: "move", FileID: "f2", FromPath: "b.txt", Path: "a.txt"}}, "a.txt", "f1", true},
		{"id-less foreign move onto tracked path supersedes",
			[]actions.Event{{Action: "move", FromPath: "b.txt", Path: "a.txt"}}, "a.txt", "f1", true},
		{"move of the node itself by id follows, no supersession",
			[]actions.Event{{Action: "move", FileID: "f1", FromPath: "a.txt", Path: "c.txt"}}, "a.txt", "f1", false},
		{"id-less move of the node itself follows by source path",
			[]actions.Event{{Action: "move", FromPath: "a.txt", Path: "c.txt"}}, "a.txt", "f1", false},
		{"move of the node then delete at its destination supersedes",
			[]actions.Event{
				{Action: "move", FileID: "f1", FromPath: "a.txt", Path: "c.txt"},
				{Action: "delete", FileID: "f1", Path: "c.txt"},
			}, "a.txt", "f1", true},
		{"move of the node then foreign move onto its new path supersedes",
			[]actions.Event{
				{Action: "move", FileID: "f1", FromPath: "a.txt", Path: "c.txt"},
				{Action: "move", FileID: "f2", FromPath: "b.txt", Path: "c.txt"},
			}, "a.txt", "f1", true},
		{"move onto an ancestor directory path does not supersede",
			[]actions.Event{{Action: "move", FileID: "f2", FromPath: "b", Path: "dir"}}, "dir/a.txt", "f1", false},
		{"move landing elsewhere does not supersede",
			[]actions.Event{{Action: "move", FileID: "f2", FromPath: "b.txt", Path: "c.txt"}}, "a.txt", "f1", false},
	}
	for _, tc := range cases {
		if got := supersededByLaterDelete(tc.events, -1, tc.path, tc.fileID); got != tc.want {
			t.Errorf("%s: supersededByLaterDelete = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestInboundDownloadNodeGoneDropsDoomedOp pins the (b) backstop: a queued download failing
// with ErrPathError whose node a strict id-matched parent listing positively confirms gone is
// resolved as a race - dropped from the queue, its phantom pending baseline cleaned, no
// durable Dropped trace. The listing deliberately shows the leaf STILL OCCUPIED by a
// different node (the overwrite-move occupancy): the path-occupancy classifiers would read
// that as "present" and wedge forever, which is exactly what the id-strict probe closes.
func TestInboundDownloadNodeGoneDropsDoomedOp(t *testing.T) {
	c := &pushClient{
		downloadErr: actions.ErrPathError,
		listResult: map[string][]actions.Node{
			"": {{Name: "x.txt", FileID: "fmov"}}, // the overwriting node occupies the leaf
		},
		// The corroborating probe of the path sees the same overwriting node: a DIFFERENT
		// non-empty id, which is not ours, so the listing's verdict stands.
		queryNode: map[string]actions.Node{"x.txt": {Name: "x.txt", FileID: "fmov"}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	ops, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "x.txt", Hash: "hocc", Size: 4},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want one download", ops)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (a positively-gone node must resolve, not fail)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue = %d ops, want 0 (doomed download dropped)", n)
	}
	if _, err := st.GetBaseline(7, "x.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("x.txt baseline: err=%v, want ErrNotFound (phantom pending record cleaned)", err)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0 (a race resolution must leave no Dropped trace)", n)
	}
}

// TestInboundDownloadNodeGoneAncestorWalkConfirms is the deep-path variant: the parent
// directory's own listing fails with ErrPathError (the whole directory was destroyed) and the
// grandparent (view root) lists cleanly without it - the ancestor walk supplies the positive
// proof, and the doomed descendant download resolves instead of reading "inconclusive" forever.
func TestInboundDownloadNodeGoneAncestorWalkConfirms(t *testing.T) {
	c := &pushClient{
		downloadErr: actions.ErrPathError,
		listErrByPath: map[string]error{
			"d": actions.ErrPathError,
		},
		listResult: map[string][]actions.Node{
			"": {{Name: "other", FileID: "fz"}},
		},
		// The destroyed directory answers the path-error sentinel at its own path, corroborating
		// the root listing that omits it (a listing alone can be a page).
		queryNodeErrByPath: map[string]error{"d": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "d/x.txt", Hash: "hocc", Size: 4},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue = %d ops, want 0", n)
	}
	if _, err := st.GetBaseline(7, "d/x.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("d/x.txt baseline: err=%v, want ErrNotFound", err)
	}
}

// TestInboundDownloadNodeGoneInconclusiveStaysQueued is the conservative direction: anything
// short of positive id-matched proof of absence - the node still visible by id (a probe
// timeout shape), an id-less child that could hide it, a failing listing, a SUCCESSFUL turn
// that enumerated nothing (errno:0 with no file_list), or an id-less op
// that can never be identity-matched at all - leaves the download queued for the next drain.
func TestInboundDownloadNodeGoneInconclusiveStaysQueued(t *testing.T) {
	cases := []struct {
		name            string
		opFileID        string
		listResult      map[string][]actions.Node
		listErr         error
		listUnevidenced map[string]bool
		wantProbes      bool
	}{
		{"node still present by id under another name", "focc",
			map[string][]actions.Node{"": {{Name: "renamed.txt", FileID: "focc"}}}, nil, nil, true},
		{"id-less child makes the listing inconclusive", "focc",
			map[string][]actions.Node{"": {{Name: "mystery.txt"}}}, nil, nil, true},
		{"listing failure is inconclusive", "focc", nil, errors.New("transport down"), nil, true},
		// Before listing evidence was required, this reply decoded as an empty node slice
		// indistinguishable from a genuinely empty parent, so nodeGoneByID read it as positive
		// proof the node was gone and the live download was dropped (its phantom baseline
		// cleaned, the op acked) on a reply that enumerated nothing at all.
		{"errno-only reply enumerated nothing", "focc", nil, nil, map[string]bool{"": true}, true},
		{"id-less op never probes", "",
			map[string][]actions.Node{"": {}}, nil, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{
				downloadErr:     actions.ErrPathError,
				listResult:      tc.listResult,
				listErr:         tc.listErr,
				listUnevidenced: tc.listUnevidenced,
			}
			e, st, _ := outboundEngine(t, c, "Photos", "root1")
			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound,
				ViewID: 7, Path: "x.txt", FileID: tc.opFileID,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if err := e.drainQueue(context.Background()); err == nil {
				t.Fatalf("drainQueue succeeded, want the inconclusive download to fail transiently")
			}
			if n, _ := st.PendingLen(); n != 1 {
				t.Errorf("queue = %d ops, want the download still queued", n)
			}
			if n, _ := st.DeadLetterLen(); n != 0 {
				t.Errorf("dead letters = %d, want 0 (an unproven absence must never drop the op)", n)
			}
			if gotProbes := len(c.listCalls) > 0; gotProbes != tc.wantProbes {
				t.Errorf("probe listings = %v, want probed=%v", c.listCalls, tc.wantProbes)
			}
		})
	}
}

// TestGuardRenameDestLiveRecheckSkipsFreshServerBytes pins the (c) layer: the occupant's
// download applied BEFORE the overwriting rename's drain slot (its live baseline is
// StateSynced with the freshly downloaded content's hash), while the rename still carries the
// stale pre-batch DestUntracked capture. The live recheck must adopt the synced hash as the
// divergence base - the on-disk bytes match it, so the rename overwrites directly instead of
// hauling just-downloaded SERVER bytes into a spurious _Conflict copy that the next scan
// would re-upload.
func TestGuardRenameDestLiveRecheckSkipsFreshServerBytes(t *testing.T) {
	serverBytes := []byte("fresh-server-content")
	serverHash := md4.ContentHash(serverBytes)
	movedBytes := []byte("moved-node-content")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC))

	// Destination: the occupant's download already applied - server bytes on disk under a
	// live synced baseline.
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), serverBytes, 0o644); err != nil {
		t.Fatalf("seed destination file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "focc", FileType: "file", ServerHash: serverHash,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed destination baseline: %v", err)
	}
	// Source: the moved node.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	// The stale pre-batch capture: at reconcile time the occupant's baseline was still
	// StatePending (its download had not applied yet), so the capture read "untracked".
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "x.txt", DestUntracked: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertOverwriteMoveConverged(t, st, viewDir, movedBytes)
}

// TestGuardRenameDestLiveRecheckPreservesPostApplyEdit is the direction regression for the
// live recheck: an edit landed AFTER the occupant's download applied (on-disk content no
// longer matches the live synced hash) is genuine user content and must still be preserved as
// a _Conflict copy before the overwriting rename lands.
func TestGuardRenameDestLiveRecheckPreservesPostApplyEdit(t *testing.T) {
	ts := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	const host = "laptop"
	serverHash := md4.ContentHash([]byte("fresh-server-content"))
	editedBytes := []byte("user-edit-after-the-download-applied")
	movedBytes := []byte("moved-node-content")

	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The live baseline records the applied download's hash, but the on-disk file was edited
	// since.
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), editedBytes, 0o644); err != nil {
		t.Fatalf("seed edited destination: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "focc", FileType: "file", ServerHash: serverHash,
		LocalHash: serverHash, LocalSize: 20, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed destination baseline: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "x.txt", DestUntracked: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(viewDir, "x.txt"))
	if err != nil || string(got) != string(movedBytes) {
		t.Fatalf("x.txt = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(editedBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestGuardRenameDestLiveSyncedEmptyHashPreserved pins the empty-hash never-lose
// branch of guardRenameDest that the two live-recheck tests above (both seeding a NON-empty
// destination hash) leave unexercised. A server move lands on a path holding a LIVE FOREIGN
// StateSynced baseline whose LocalHash is EMPTY - the inbound-rename bare/promoted-pending write
// anomaly (markSynced never leaves a real file record hash-less). With no trustworthy divergence
// base, the regular file there must be preserved WHOLE as a _Conflict copy, not clobbered under a
// false server-authority read. The op carries NO reconcile-time DestUntracked/DestBaseLocalHash, so
// the live empty-hash branch is the ONLY thing that makes the guard fire - reverting it (return nil /
// server authority) would clobber the user's file with the moved node's bytes and record no copy.
func TestGuardRenameDestLiveSyncedEmptyHashPreserved(t *testing.T) {
	ts := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	const host = "laptop"
	userBytes := []byte("local-user-content-the-server-must-not-clobber")
	movedBytes := []byte("moved-node-content")

	copyRel := conflictCopyName("x.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// A live FOREIGN synced baseline at x.txt with an EMPTY LocalHash, and a regular file on disk.
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed destination file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "focc", FileType: "file", State: store.StateSynced, // LocalHash intentionally empty
	}); err != nil {
		t.Fatalf("seed destination baseline: %v", err)
	}
	// A tracked source node to move onto x.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	// No DestUntracked / DestBaseLocalHash: only the live empty-hash foreign baseline drives the guard.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "x.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(viewDir, "x.txt"))
	if err != nil || string(got) != string(movedBytes) {
		t.Fatalf("x.txt = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved user content %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "x.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one x.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestOverwriteMoveDeadLetterCleansSupersededDownloadBaseline pins the fix
// for an overwrite move stranding a superseded download baseline. A queued download is
// superseded by an OVERWRITING MOVE, but the mover is untracked at its source and its rename
// dead-letters (neither endpoint on disk - the cold-start-gap path), whose cleanup touches only
// the mover, never the victim. Before the fix dropSupersededQueuedDownloads acked the doomed
// download but left the victim's phantom StatePending baseline indexed forever, and - because a
// successful overwriting rename would have overwritten that baseline but the dead-lettered one
// does not - the victim's path silently fell out of outbound sync (localReconcile drives only
// StateSynced). The fix drops the victim's stranded baseline right after the supersession ack, so
// the path is free to sync again.
func TestOverwriteMoveDeadLetterCleansSupersededDownloadBaseline(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Batch 1: the occupant's create - its download is queued, StatePending baseline at x.txt
	// (indexed by focc in byid). The file is never materialised (superseded next batch).
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "x.txt", Hash: "hocc", Size: 4},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpDownload {
		t.Fatalf("batch1 ops = %+v, want one download", ops1)
	}
	if b, err := st.GetBaseline(7, "x.txt"); err != nil || b.FileID != "focc" || b.State != store.StatePending {
		t.Fatalf("x.txt baseline after batch1 = %+v (err %v), want pending focc", b, err)
	}

	// Batch 2: an overwriting move of an UNTRACKED node fmov onto x.txt. Neither the source
	// a.txt nor the destination x.txt exists on disk and fmov is untracked, so applyRename
	// dead-letters via the cold-start-gap path - dropColdStartGapRename drops only fmov's
	// (absent) baseline, never the victim focc's at x.txt.
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "a.txt", Path: "x.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRename {
		t.Fatalf("batch2 ops = %+v, want one rename", ops2)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("queue after batch2 = %d ops, want just the rename (occupant download dropped)", n)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the untracked-mover rename dead-letters)", n)
	}

	// The FIX: the victim focc's phantom baseline is gone, not stranded StatePending forever.
	// What tracks x.txt now is the MOVER fmov, materialised by the compensating download the
	// dropped rename left behind (finding H2) - the server really does hold fmov at this path.
	b, err := st.GetBaseline(7, "x.txt")
	if err != nil || b.FileID != "fmov" || b.State != store.StateSynced {
		t.Fatalf("x.txt baseline = %+v (err %v), want the re-requested mover fmov, not the victim's phantom", b, err)
	}

	// Outbound sync of the path resumes: a local edit at x.txt now diffs as a modification and
	// queues an OpUpload (before the fix the stranded StatePending baseline silently blocked
	// every diff, since localReconcile only drives StateSynced baselines).
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("local-content"), 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(lops) != 1 || lops[0].Type != store.OpUpload || lops[0].Path != "x.txt" {
		t.Fatalf("localReconcile ops = %+v, want one OpUpload for x.txt (outbound sync resumed)", lops)
	}
}

// TestOverwriteMoveSupersededDropCrashWindowConverges pins the ordering:
// dropSupersededQueuedDownloads resolves the victim's stranded baseline BEFORE acking the
// superseded download - two separate Update transactions - so a crash between the two leaves the
// recoverable state (cleanup committed, op still queued) rather than the acked-with-stranded-
// phantom state, which is the stranded-baseline outcome narrowed to the crash window: nothing
// ever replays the cleanup once the op has left the queue. The crash is injected through the
// ackOp seam at the worst point of the window - the ack's queue-delete transaction commits, then
// the process "dies" before any code after it runs (the enqueueOp-seam pattern).
// Before the fix the ack ran first, so this crash stranded the victim's phantom
// StatePending baseline forever; with the fix the cleanup has already committed, and the
// replayed batch (its cursor never advanced) re-runs the drop idempotently and converges.
func TestOverwriteMoveSupersededDropCrashWindowConverges(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Batch 1: the occupant's create - its download is queued, StatePending baseline at x.txt.
	ops1, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", FileID: "focc", Path: "x.txt", Hash: "hocc", Size: 4},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}
	if len(ops1) != 1 || ops1[0].Type != store.OpDownload {
		t.Fatalf("batch1 ops = %+v, want one download", ops1)
	}
	if b, err := st.GetBaseline(7, "x.txt"); err != nil || b.FileID != "focc" || b.State != store.StatePending {
		t.Fatalf("x.txt baseline after batch1 = %+v (err %v), want pending focc", b, err)
	}

	// Simulated crash: the ack's queue delete commits durably, then nothing after it ever runs
	// (reconcile aborts with the injected error; the batch's per-page cursor never advances).
	crashErr := errors.New("simulated crash after ack commit")
	e.ackOp = func(seq uint64) error {
		if err := st.Ack(seq); err != nil {
			return err
		}
		return crashErr
	}

	// Batch 2: an overwriting move of an UNTRACKED node fmov onto x.txt - the stranded-baseline
	// shape, where the mover's dead-lettered rename never touches the victim's record, so the
	// supersession drop's own cleanup is the only thing that ever cleans it.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "a.txt", Path: "x.txt"},
	}, 5); !errors.Is(err, crashErr) {
		t.Fatalf("reconcile batch2 = %v, want the injected crash", err)
	}

	// The ordering pin (fails before the fix): by the time the ack committed, the victim's
	// phantom baseline had ALREADY been dropped - the crash-consistent durable state is
	// recoverable, never acked-with-phantom.
	if _, err := st.GetBaseline(7, "x.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("x.txt baseline after crash = %v, want ErrNotFound (cleanup committed before the ack)", err)
	}

	// Restart: the crashed batch replays against a healthy store (its cursor never advanced).
	// The drop finds nothing left to do and the batch converges like the non-crashed flow.
	e.ackOp = st.Ack
	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "a.txt", Path: "x.txt"},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile replay: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpRename {
		t.Fatalf("replay ops = %+v, want one rename", ops2)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("dead letters = %d, want 1 (the untracked-mover rename dead-letters)", n)
	}
	// No stranded phantom of the VICTIM survives; the record here is the mover's own, written by
	// the compensating download the dropped rename left behind (finding H2).
	if b, err := st.GetBaseline(7, "x.txt"); err != nil || b.FileID != "fmov" || b.State != store.StateSynced {
		t.Errorf("x.txt baseline after replay = %+v (err %v), want the re-requested mover, no victim phantom", b, err)
	}

	// Outbound sync of the victim's path resumes - the observable: a local edit at
	// x.txt diffs instead of hiding behind a stranded StatePending record (localReconcile only
	// drives StateSynced).
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("local-content"), 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(lops) != 1 || lops[0].Type != store.OpUpload || lops[0].Path != "x.txt" {
		t.Fatalf("localReconcile ops = %+v, want one OpUpload for x.txt (outbound sync resumed)", lops)
	}
}

// TestRejectedDirRemoveDropsPhantomDescendantBaselines pins the descendant tail: a
// directory OpRemove that dead-letters (a symlink occupies its on-disk path) used to clean only
// op.Path, stranding any phantom StatePending descendant baseline reconcile pre-wrote below it -
// localReconcile drives only StateSynced, so that descendant's path silently fell out of outbound
// sync forever. dropRejectedRemoveBaseline now drops the phantom (non-synced, unmaterialised)
// descendants too, mirroring applyRemove's success path (dropDescendantBaselines), while
// preserving a synced descendant (a real apply placed it) so no live file is orphaned.
func TestRejectedDirRemoveDropsPhantomDescendantBaselines(t *testing.T) {
	requireSymlinks(t)
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	// "d" is a symlink on disk (points outside the tree), so a remove of it is refused by the
	// symlink guard and dead-lettered rather than applied - the directory stays in place.
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(viewDir, "d")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	// The removed directory's baseline (synced), a PHANTOM descendant (StatePending, never
	// materialised - e.g. a superseded id-less download), and a SYNCED descendant (proof a real
	// apply placed it) that must be preserved.
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fdir", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/x.txt", store.Baseline{
		FileID: "focc", FileType: "file", ServerHash: "h", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed phantom descendant: %v", err)
	}
	if err := st.PutBaseline(7, "d/keep.txt", store.Baseline{
		FileID: "fkeep", FileType: "file", ServerHash: "hk", LocalHash: "hk", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced descendant: %v", err)
	}

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "d", FileID: "fdir", BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the symlink-rejected dir remove)", n)
	}

	// The FIX: the phantom descendant baseline is dropped, not stranded StatePending forever.
	if _, err := st.GetBaseline(7, "d/x.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("d/x.txt baseline = %v, want ErrNotFound (phantom descendant cleaned)", err)
	}
	// The synced descendant (a real apply placed it) is preserved - never orphaned.
	if b, err := st.GetBaseline(7, "d/keep.txt"); err != nil || b.FileID != "fkeep" || b.State != store.StateSynced {
		t.Errorf("d/keep.txt baseline = %+v (err %v), want the synced descendant preserved", b, err)
	}
}
