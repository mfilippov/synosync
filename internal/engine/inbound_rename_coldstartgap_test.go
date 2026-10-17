package engine

// These tests pin the fix for a directory OpRename that hits the cold-start-gap
// path (source AND destination both absent on disk): it used to drop only the moved directory's
// ROOT baseline, leaving the relocated StateSynced DESCENDANT baselines stranded. reconcile's
// relocatePending eagerly rebases the whole subtree to the destination prefix (MoveBaseline copies
// each record verbatim, so a synced descendant stays synced), so dropping only the root left those
// descendants describing files that were never materialised. The next localScan then diffs each
// stranded descendant as a vanished synced file and pushes a spurious outbound OpRemove that would
// delete the freshly-moved SERVER node. dropColdStartGapRename now cascades dropDescendantBaselines
// under the destination prefix (mirroring applyRemove / resolveRejectedRename / guardRemoveDir),
// so no descendant is stranded and no spurious remove is emitted.

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

// TestColdStartGapDirRenameDropsDescendantBaselines seeds a tracked directory d with a synced,
// on-disk descendant d/f, then reconciles a server move d->e while the on-disk subtree has
// transiently vanished (neither d nor e exists on disk). The rename dead-letters via the
// cold-start-gap path. Before the fix the relocated descendant baseline e/f survived StateSynced,
// so a subsequent localReconcile diffed it as a vanished file and emitted OpRemove(e/f) - a
// spurious delete of the freshly-moved server node. After the fix e/f is dropped with the root, so
// localReconcile emits nothing for it.
func TestColdStartGapDirRenameDropsDescendantBaselines(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// A tracked directory d with a tracked, synced descendant file d/f. The store records them,
	// but they are NOT materialised on disk - the subtree vanished in the throttle/poll window
	// between the last healthy scan and this apply (a flaky removable/network mount under d, or a
	// user deleting the folder). Only an unrelated in-sync file keeps the view non-empty so the
	// empty-scan debounce does not mask the outbound remove we are checking for.
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
	// An unrelated in-sync file that IS on disk, so scanView is non-empty and localReconcile takes
	// the diff path (not the empty-scan debounce).
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

	// Server move d->e (a directory). reconcile relocates the root baseline to e (StatePending)
	// and the descendant baseline d/f -> e/f (StateSynced, copied verbatim by MoveBaseline).
	ops, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fd", FromPath: "d", Path: "e", IsDir: true},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("reconcile ops = %+v, want one rename", ops)
	}
	// The descendant was relocated to the destination prefix and is StateSynced (the stranding
	// precondition).
	if b, err := st.GetBaseline(7, "e/f"); err != nil || b.FileID != "ff" || b.State != store.StateSynced {
		t.Fatalf("e/f baseline after reconcile = %+v (err %v), want synced ff", b, err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the cold-start-gap dir rename dead-letters)", n)
	}

	// The FIX: the root e's relocated record AND the relocated descendant e/f are both dropped,
	// not left stranded. The root path is re-tracked afterwards by the compensating OpMkdir the
	// drop enqueues (finding H2) - an apply that really created the directory - so what must NOT
	// survive there is the stale relocated record; the descendant has no such compensation and
	// stays absent.
	if b, err := st.GetBaseline(7, "e"); err != nil || b.State != store.StateSynced || b.FileID != "fd" {
		t.Fatalf("e baseline = %+v (err %v), want the compensating mkdir's synced record", b, err)
	}
	if _, err := st.GetBaseline(7, "e/f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("e/f baseline = %v, want ErrNotFound (stranded descendant cascaded)", err)
	}

	// The consequence check: with no stranded synced descendant, localReconcile emits no spurious
	// OpRemove. Before the fix e/f survives StateSynced with no on-disk file, so the diff path
	// would emit OpRemove(e/f) that deletes the freshly-moved server node.
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted %+v; want no OpRemove for the cascaded descendant", op)
		}
	}
}

// TestColdStartGapDirRenameSrcMovedCascadesSource covers the directly-seeded / srcIsMoved arm: when
// reconcile did NOT relocate (the source baseline still holds the moved node), the descendants sit
// under the SOURCE prefix, so the cascade must drop them there too. The op is seeded directly (no
// reconcile relocation) so srcIsMoved is true at apply time.
func TestColdStartGapDirRenameSrcMovedCascadesSource(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	fBytes := []byte("descendant-content")
	// Source baseline still holds the moved node (srcIsMoved): a directly-seeded op, no reconcile
	// relocation, so the descendant stays under the SOURCE prefix d/f.
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

	// Directly-seeded rename op; neither d nor e exists on disk. op.IsDir carries dir-ness (the
	// event signal), and the source baseline (fd) still holds the node so srcIsMoved is true.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fd", Path: "d", NewPath: "e", IsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1", n)
	}

	// Source root d and its descendant d/f are both dropped.
	if _, err := st.GetBaseline(7, "d"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("d baseline = %v, want ErrNotFound (source root dropped)", err)
	}
	if _, err := st.GetBaseline(7, "d/f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("d/f baseline = %v, want ErrNotFound (stranded source descendant cascaded)", err)
	}

	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted %+v; want no OpRemove for the cascaded descendant", op)
		}
	}
}

// TestColdStartGapRenameUntrackedNodeOntoOccupiedDirDrops pins the occupied-destination fix. The
// gone-source drop used to be gated on `!destExists(to)` alone, so an UNTRACKED moved node (byid
// miss - our node was never materialised) whose destination path happens to hold the user's own
// directory fell through to the swallowed-no-op path: applyRename recorded a bare
// {FileID: op.FileID} baseline at the destination and forced it StateSynced - a false synced claim
// over a foreign directory. The bare record carries no FileType, so the next localScan reads a
// file->dir type replacement and detectRemoves emits an outbound OpRemove of op.FileID, deleting
// the LIVE server node and its whole subtree, after which the local directory is re-uploaded as an
// unrelated fresh tree. The invariant proven a few lines above (a node we materialised always
// carries a byid-indexed baseline) is what makes the occupant provably foreign, so !nodeTracked now
// drives dropColdStartGapRename regardless of whether the destination exists.
func TestColdStartGapRenameUntrackedNodeOntoOccupiedDirDrops(t *testing.T) {
	ts := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	const host = "laptop"
	userBytes := []byte("user-content")
	serverBytes := []byte("server-node-content")
	serverNode := actions.Node{
		FileID: "fmov", Hash: md4.ContentHash(serverBytes), Size: int64(len(serverBytes)), MTime: 99,
	}
	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": serverBytes},
		downloadNode:    map[string]actions.Node{"fmov": serverNode},
		queryNode:       map[string]actions.Node{"dst": serverNode},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The user's own untracked directory sits at the move's DESTINATION path, with a file inside
	// it. Nothing here is tracked: no baseline, no byid entry.
	if err := os.MkdirAll(filepath.Join(viewDir, "dst"), 0o755); err != nil {
		t.Fatalf("seed occupant dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dst", "user.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed occupant file: %v", err)
	}

	// A server move of a node we never materialised (its create predates our event-log window)
	// onto that occupied path. The source src.txt does not exist on disk either.
	ops, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 6, Action: "move", FileID: "fmov", FromPath: "src.txt", Path: "dst"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("reconcile ops = %+v, want one rename", ops)
	}

	// The drain that preserves the occupant reports a plain error and leaves the op queued, by
	// design: the path is free only after the move, so the download lands on the next pass.
	for i := 0; i < 3; i++ {
		_ = e.drainQueue(context.Background())
		if n, _ := st.PendingLen(); n == 0 {
			break
		}
	}
	// ONE trace: the dropped rename. The compensating download it re-requests (finding H2) no
	// longer dead-letters on the occupant - it reaches the destination through the ordinary
	// fresh-create never-lose funnel, exactly as the FILE-occupant arm below already did. The two
	// arms differ only in the occupant's kind, so they must not differ in outcome.
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the rename drop alone)", n)
	}

	// The FIX this test exists for, unchanged: no false synced claim was ever stamped over the
	// user's content. The record at the destination now describes bytes an apply really placed.
	b, err := st.GetBaseline(7, "dst")
	if err != nil {
		t.Fatalf("dst baseline: %v", err)
	}
	if b.FileType != "file" || b.ServerHash != serverNode.Hash {
		t.Fatalf("dst baseline = %+v, want a truthful file record for the landed server node", b)
	}

	// The user's directory survived WHOLE as a keep-both copy - nothing deleted, nothing clobbered.
	recs, err := st.ListConflicts(7)
	if err != nil || len(recs) != 1 || recs[0].CanonicalPath != "dst" {
		t.Fatalf("ListConflicts = %+v (err %v), want the occupant preserved", recs, err)
	}
	preserved := filepath.Join(viewDir, filepath.Base(recs[0].CopyPath))
	if got, err := os.ReadFile(filepath.Join(preserved, "user.txt")); err != nil || string(got) != string(userBytes) {
		t.Fatalf("preserved user.txt = %q (err %v), want %q", got, err, userBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "dst")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("dst = %q (err %v), want the landed server bytes", got, err)
	}

	// The consequence check this test was written for: the next scan must not push a delete of
	// the live server node. Before the original fix a bare synced record at dst against an
	// on-disk directory read as a type replacement and detectRemoves emitted
	// OpRemove{FileID: "fmov"} - the server node and its whole subtree.
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted %+v; want no OpRemove of the live server node", op)
		}
	}
}

// TestColdStartGapRenameUntrackedNodeOntoOccupiedFileDrops is the file-occupant arm of the
// occupied-destination fix. The op is seeded directly with NO destination capture (DestUntracked
// false, no DestBaseLocalHash) - the shape guardRenameDest documents as "records predating the
// capture fields", which is exactly the arm that leaves the guard with no basis and lets the flow
// reach the gone-source branch with the occupant still on disk. Before the fix the swallowed no-op
// stamped a BARE synced record over the user's file; once the user deleted that file the next scan
// diffed a vanished synced node and pushed an outbound OpRemove of the LIVE server node op.FileID.
//
// The op is still dropped and no bare record is ever written. What the drop now leaves behind is
// the compensating download (finding H2), which reaches the destination through the ordinary
// fresh-create never-lose funnel: the user's file is preserved as a _Conflict copy and the server
// node lands on the canonical path under a record earned by an apply that really placed those
// bytes - precisely what the pre-fix bare stamp was not.
func TestColdStartGapRenameUntrackedNodeOntoOccupiedFileDrops(t *testing.T) {
	ts := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	const host = "laptop"
	userBytes := []byte("user-content")
	serverBytes := []byte("server-node-content")
	serverNode := actions.Node{
		FileID: "fmov", Hash: md4.ContentHash(serverBytes), Size: int64(len(serverBytes)), MTime: 99,
	}
	c := &pushClient{
		downloadContent: map[string][]byte{"fmov": serverBytes},
		downloadNode:    map[string]actions.Node{"fmov": serverNode},
		queryNode:       map[string]actions.Node{"dst.txt": serverNode},
		uploadNode: map[string]actions.Node{
			filepath.Base(conflictCopyName("dst.txt", host, ts)): {
				FileID: "copyf", Hash: md4.ContentHash(userBytes), MTime: 777,
			},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	occupant := filepath.Join(viewDir, "dst.txt")
	if err := os.WriteFile(occupant, userBytes, 0o644); err != nil {
		t.Fatalf("seed occupant file: %v", err)
	}
	// A no-basis rename op for a node we never materialised: source absent on disk, byid holds
	// nothing for fmov.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "src.txt", NewPath: "dst.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the untracked-node rename dead-letters)", n)
	}
	// The user's bytes were never clobbered - they are set aside as a keep-both copy.
	copyRel := conflictCopyName("dst.txt", host, ts)
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy %s = %q (err %v), want the user content %q", copyRel, got, err, userBytes)
	}
	// The record at the destination describes bytes that are really there, hash and all - never
	// the FileType-less, hash-less stamp the pre-fix swallow wrote over foreign content, which is
	// what a later local delete turned into a remove of the live server node.
	b, err := st.GetBaseline(7, "dst.txt")
	if err != nil || b.FileID != "fmov" || b.LocalHash != md4.ContentHash(serverBytes) {
		t.Fatalf("dst.txt baseline = %+v (err %v), want the downloaded node's own record", b, err)
	}
	if got, err := os.ReadFile(occupant); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("dst.txt = %q (err %v), want the server node %q", got, err, serverBytes)
	}
}

// TestColdStartGapRenameTrackedNodeOntoOccupiedDestStillSwallows pins the unchanged half of the
// gate: when the moved node IS tracked (byid resolves op.FileID), a gone source with a present
// destination still means the on-disk move already happened, so the apply swallows the failed
// rename and keeps its bookkeeping - the crash-replay repair path the occupied-destination fix must
// not disturb.
func TestColdStartGapRenameTrackedNodeOntoOccupiedDestStillSwallows(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// The move already applied on disk: the node sits at the destination, the source is gone.
	// Its baseline is still the relocated StatePending record reconcile wrote (the crash window
	// between the on-disk rename and the baseline write), so byid resolves fmov -> b.txt.
	movedBytes := []byte("moved-content")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
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
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Fatalf("dead letters = %d, want 0 (a tracked node's replay is swallowed, not dropped)", n)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.FileID != "fmov" || b.State != store.StateSynced {
		t.Fatalf("b.txt baseline = %+v (err %v), want synced fmov (replay completed the bookkeeping)", b, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want %q", got, err, movedBytes)
	}
}

// TestColdStartGapRenameIDLessLeavesLocalOnlyBaselines pins the id-less identity degeneration the
// occupied-destination gate introduced. reconcile's id-less-move backfill leaves op.FileID EMPTY
// whenever the source is untracked, local-only or corrupt, and putBaselineTx never indexes an
// empty file id - so PathByID misses and nodeTracked is false for EVERY id-less rename, routing it
// into dropColdStartGapRename via the new `|| !nodeTracked` term. There both identity tests
// degenerate: `dst.FileID == op.FileID` matches any local-only (never-uploaded) destination record,
// and srcIsMoved matches any local-only source. So a sparse id-less directory move whose source is
// gone from disk while the destination holds the USER's own tree cascaded dropDescendantBaselines
// over that live subtree - untracking descendants that do carry server ids, which the next
// localScan then re-uploads as fresh creates (duplicate server nodes, originals orphaned). The
// helper's documented safety argument ("BOTH cleanup arms are gated on a baseline carrying
// op.FileID, which the store indexes in byid") only holds for a non-empty id, so both arms now
// require one.
func TestColdStartGapRenameIDLessLeavesLocalOnlyBaselines(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// The user's own tree at the move's DESTINATION path: a directory whose baseline is local-only
	// (empty FileID - a create not yet pushed) holding a descendant that IS already synced with a
	// real server file id. The descendant is the one a cascade would destroy.
	if err := os.MkdirAll(filepath.Join(viewDir, "e"), 0o755); err != nil {
		t.Fatalf("seed dest dir: %v", err)
	}
	fBytes := []byte("user-descendant")
	if err := os.WriteFile(filepath.Join(viewDir, "e", "f"), fBytes, 0o644); err != nil {
		t.Fatalf("seed descendant file: %v", err)
	}
	if err := st.PutBaseline(7, "e", store.Baseline{
		FileType: "dir", State: store.StatePending, // local-only: no server id yet
	}); err != nil {
		t.Fatalf("seed dest baseline: %v", err)
	}
	if err := st.PutBaseline(7, "e/f", store.Baseline{
		FileID: "fserver", FileType: "file", ServerHash: "hf",
		LocalHash: md4.ContentHash(fBytes), LocalSize: int64(len(fBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed descendant baseline: %v", err)
	}

	// A sparse ID-LESS directory move whose source never existed on disk and has no baseline (so
	// srcRecreated is false and the flow reaches the gone-source branch).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "", Path: "d", NewPath: "e", IsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1 (the id-less rename still dead-letters)", n)
	}

	// The FIX: neither arm cleaned anything up, so the user's live records survive. Before the fix
	// both e and e/f were dropped (dst.FileID "" == op.FileID "" matched, and movedIsDir came out
	// true from the same degenerate comparison against the dir-typed record).
	if b, err := st.GetBaseline(7, "e/f"); err != nil || b.FileID != "fserver" || b.State != store.StateSynced {
		t.Fatalf("e/f baseline = %+v (err %v), want the synced fserver record intact", b, err)
	}
	if b, err := st.GetBaseline(7, "e"); err != nil || b.FileType != "dir" {
		t.Fatalf("e baseline = %+v (err %v), want the local-only dir record intact", b, err)
	}
	// The user's files are untouched on disk.
	if got, err := os.ReadFile(filepath.Join(viewDir, "e", "f")); err != nil || string(got) != string(fBytes) {
		t.Fatalf("e/f = %q (err %v), want %q", got, err, fBytes)
	}

	// The consequence check: the synced descendant still matches its baseline, so the next scan
	// pushes nothing for it. Before the fix its baseline was gone, so the diff read the on-disk file
	// as a brand-new local create and emitted an OpUpload - minting a SECOND server node for content
	// the server already held under "fserver".
	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Path == "e/f" {
			t.Fatalf("localReconcile emitted %+v; want nothing for the still-synced descendant", op)
		}
	}
}

// TestColdStartGapDirRenameOpIsDirDrivesCascade isolates the `op.IsDir` term of the
// `isDir := movedIsDir || op.IsDir` gate: the moved node's baselines do NOT report a "dir"
// FileType, so resolveMovedIsDir returns movedIsDir == false, and only the move event's own
// op.IsDir can drive the descendant cascade. The two sibling tests above seed a dir-typed baseline
// (movedIsDir already true), so a regression dropping the `|| op.IsDir` term would still pass them
// while re-stranding the descendant for the exact case the fallback guards; this test fails on that
// regression.
func TestColdStartGapDirRenameOpIsDirDrivesCascade(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	fBytes := []byte("descendant-content")
	// Root baseline holds the moved node (srcIsMoved) but is intentionally NOT typed "dir", so
	// resolveMovedIsDir cannot testify dir-ness - op.IsDir is the only dir signal reaching the gate.
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed root baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/f", store.Baseline{
		FileID: "ff", FileType: "file", ServerHash: "hf",
		LocalHash: md4.ContentHash(fBytes), LocalSize: int64(len(fBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed descendant baseline: %v", err)
	}
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

	// Directly-seeded rename op; neither d nor e exists on disk. op.IsDir carries dir-ness; the
	// source baseline (fd) still holds the node so srcIsMoved is true.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fd", Path: "d", NewPath: "e", IsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("dead letters = %d, want 1", n)
	}

	// Root d and its descendant d/f are both dropped - proof op.IsDir alone drove the cascade.
	if _, err := st.GetBaseline(7, "d"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("d baseline = %v, want ErrNotFound (source root dropped)", err)
	}
	if _, err := st.GetBaseline(7, "d/f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("d/f baseline = %v, want ErrNotFound (op.IsDir drove the descendant cascade)", err)
	}

	lops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range lops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted %+v; want no OpRemove for the cascaded descendant", op)
		}
	}
}
