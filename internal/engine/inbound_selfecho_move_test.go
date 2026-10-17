package engine

// These tests pin a HIGH-severity data-loss fix: an inbound OpDownload whose
// node was relocated by a SELF-ECHO move (our own already-pushed rename) must be applied at the
// node's CURRENT tracked path, not the stale enqueue-time op.Path. A self-echo move enqueues no
// inbound OpRename (applySelfEchoMove skips the queue), so nothing would carry the download from
// the stale path - before the fix the download materialised a ghost at the old name while
// removeRelocatedPriorFile deleted the REAL relocated file with no _Conflict copy and markSynced
// evicted the correct baseline through byid, leaving the replica permanently diverged.
//
// The batch is [modify a (remote, older sync id), self-echo move a->b (ours, newer sync id)]:
// reconcile enqueues OpDownload{Path:a} while the self-echo move relocates the baseline to b and
// bumps its ServerSyncID above the download's. downloadRelocatedBySelfEcho recognises that shape
// (cur is StateSynced with a strictly newer ServerSyncID, and no queued rename will carry it) and
// applyDownload redirects to b.

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

// seedSelfEchoMovePreState wires the pre-poll state common to the self-echo tests: node F is
// synced at b.txt (our local rename a->b already applied, so byid points at b and the file lives
// there) holding origBytes, while the server has since (older sync id) a modify at the OLD path
// a.txt carrying serverBytes for the same node. It returns the engine, store and view directory.
func seedSelfEchoMovePreState(t *testing.T, c *pushClient, origBytes []byte) (*Engine, *store.Store, string) {
	t.Helper()
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed b.txt: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: md4.ContentHash(origBytes),
		LocalHash: md4.ContentHash(origBytes), LocalSize: int64(len(origBytes)),
		ServerSyncID: 50, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	return e, st, viewDir
}

// selfEchoMoveBatch is the [modify a.txt (remote, sync 100), self-echo move a.txt->b.txt (ours,
// sync 101)] batch. The modify is at the stale pre-move path; the self-echo move (committed by
// this client's session) carries the higher sync id.
func selfEchoMoveBatch(serverHash string, serverSize int64) []actions.Event {
	return []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: serverHash, Size: serverSize, MTime: 555},
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "sess"},
	}
}

// TestInboundSelfEchoMoveRedirectsDownloadConverges is the primary case with no unscanned
// local edit: the download must land at b.txt (the node's current path) with the server bytes,
// leaving no ghost at a.txt and never deleting the relocated file. Before the fix the download
// materialised a ghost at a.txt and removeRelocatedPriorFile deleted b.txt, leaving the replica
// diverged (file under the old name, canonical path empty).
func TestInboundSelfEchoMoveRedirectsDownloadConverges(t *testing.T) {
	origBytes := []byte("last-synced-content-h1")
	serverBytes := []byte("remote-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)

	if _, _, err := e.reconcile(7, selfEchoMoveBatch(serverHash, int64(len(serverBytes))), 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The server bytes landed at b.txt (the node's current path), not a.txt.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt ghost materialised: %v", err)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "b.txt" {
		names := make([]string, len(entries))
		for i, en := range entries {
			names[i] = en.Name()
		}
		t.Errorf("view dir = %v, want only b.txt", names)
	}

	// The baseline is synced at b.txt with the server hash; a.txt is untracked.
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "F" || b.State != store.StateSynced || b.LocalHash != serverHash {
		t.Errorf("b.txt baseline = %+v, want synced F/%s", b, serverHash)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !isNotFound(err) {
		t.Errorf("a.txt baseline exists: err=%v", err)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(F) = %q (err %v), want b.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0", n)
	}
	recs, _ := st.ListConflicts(7)
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
}

// TestInboundSelfEchoMoveRedirectIsReplayIdempotent pins the at-least-once crash-replay window
// of the self-echo redirect: markSynced advances the relocated baseline to synced but must NOT
// rewind its ServerSyncID from the self-echo move's (101) down to this older download event's
// (100), or downloadRelocatedBySelfEcho's "cur is strictly newer" proof would be destroyed. The
// durable queue op still carries the stale pre-move path (a.txt) - the redirect only mutates the
// in-memory copy - so a crash between markSynced's PutBaseline and the Ack replays the op at
// a.txt. If the redirect no longer fired (the rewind bug), the download would materialise a ghost
// at a.txt while removeRelocatedPriorFile deleted the real file at b.txt with no _Conflict copy -
// permanent divergence. With the monotonic ServerSyncID guard the replay still redirects to b.txt;
// the download's captured base (origHash) no longer matches b.txt's landed server bytes, so it
// funnels through keep-both, which resolves it as a false conflict (local == server) and adopts -
// the same data-safe replay outcome as any inbound download, never a ghost or a lost file.
func TestInboundSelfEchoMoveRedirectIsReplayIdempotent(t *testing.T) {
	origBytes := []byte("last-synced-content-h1")
	serverBytes := []byte("remote-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			// The keep-both false-conflict path the replay funnels through re-queries the node.
			"b.txt": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)

	if _, _, err := e.reconcile(7, selfEchoMoveBatch(serverHash, int64(len(serverBytes))), 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Capture the durable download op (Path == a.txt) BEFORE the drain acks it - this is exactly
	// the record a crash-replay would re-read.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	var dl store.Op
	for _, qo := range pending {
		if qo.Type == store.OpDownload && qo.Path == "a.txt" {
			dl = qo.Op
		}
	}
	if dl.Type != store.OpDownload {
		t.Fatalf("no durable OpDownload at a.txt captured; pending=%+v", pending)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The fix: markSynced advanced the baseline to synced but preserved the newer move sync id,
	// so the redirect gate still recognises the relocation on replay.
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.ServerSyncID != 101 {
		t.Fatalf("b.txt ServerSyncID = %d, want 101 (not rewound to the download event's 100)", b.ServerSyncID)
	}

	// Simulate the crash-replay: the not-yet-acked op is re-enqueued from its durable form.
	if _, err := st.Enqueue(dl); err != nil {
		t.Fatalf("re-enqueue replay op: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue (replay): %v", err)
	}

	// The replay is data-safe: b.txt still holds the server bytes, no ghost at a.txt, and the real
	// file was never deleted.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server content %q after replay", got, err, serverBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt ghost materialised on replay: %v", err)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "b.txt" {
		names := make([]string, len(entries))
		for i, en := range entries {
			names[i] = en.Name()
		}
		t.Errorf("view dir = %v after replay, want only b.txt", names)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(F) = %q (err %v) after replay, want b.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained after replay: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d after replay, want 0", n)
	}
	// A false-conflict adoption preserves nothing, so no _Conflict copy exists.
	recs, _ := st.ListConflicts(7)
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v after replay, want 0 (false-conflict adoption)", recs)
	}
}

// TestInboundSelfEchoMovePreservesUnscannedEdit is the loss-variant: the user edited the
// relocated file (b.txt -> editBytes) after the local rename but before the poll, so no outbound
// upload was queued (the cheap scan had not run). The redirected download must detect the
// divergence against the captured pre-batch base and funnel to keep-both, preserving the edit as
// a _Conflict copy while the server bytes land on the canonical path. Before the fix the edit was
// destroyed with no copy.
func TestInboundSelfEchoMovePreservesUnscannedEdit(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("last-synced-content-h1")
	editBytes := []byte("unscanned-local-edit-h3")
	serverBytes := []byte("remote-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)
	editHash := md4.ContentHash(editBytes)

	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"b.txt": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: editHash, MTime: 777}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)
	pinClock(e, host, ts)

	if _, _, err := e.reconcile(7, selfEchoMoveBatch(serverHash, int64(len(serverBytes))), 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The unscanned edit lands on disk at b.txt before the drain (the 5s scan throttle window).
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), editBytes, 0o644); err != nil {
		t.Fatalf("seed unscanned edit: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path holds the server bytes...
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	// ...and the unscanned edit survives as the _Conflict copy rather than being destroyed.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(editBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt ghost materialised: %v", err)
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

// TestInboundSelfEchoMoveCrossBatchRedirects is the cross-batch shape: the remote modify arrives
// in batch 1 (its download queued, not yet drained), and the self-echo move arrives in batch 2.
// The redirect must still fire - downloadRelocatedBySelfEcho reads the live baseline, so the
// relocation carries across batches exactly like the same-batch case.
func TestInboundSelfEchoMoveCrossBatchRedirects(t *testing.T) {
	origBytes := []byte("last-synced-content-h1")
	serverBytes := []byte("remote-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)

	// Batch 1: the remote modify at the stale path a.txt. The download is enqueued but the drain
	// is deferred (a transient failure between reconciles), so it is still queued at batch 2.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}, 0); err != nil {
		t.Fatalf("reconcile batch 1: %v", err)
	}
	// Batch 2: our self-echo move a.txt->b.txt. It relocates the baseline and bumps its sync id
	// but enqueues nothing.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "sess"},
	}, 100); err != nil {
		t.Fatalf("reconcile batch 2: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt ghost materialised: %v", err)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(F) = %q (err %v), want b.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundNonSelfEchoMoveModifyNotRedirected is a regression test: a non-self-echo
// [modify a.txt, move a.txt->b.txt] batch (both events remote) enqueues an inbound OpRename that
// carries the download from a.txt to b.txt. The download MUST stay pinned to op.Path (a.txt) and
// ride behind that queued rename - downloadRelocatedBySelfEcho returns false because the
// relocated baseline is StatePending (a queued rename will carry it), not the StateSynced
// higher-sync-id shape a self-echo move leaves.
func TestInboundNonSelfEchoMoveModifyNotRedirected(t *testing.T) {
	origBytes := []byte("last-synced-content-h1")
	serverBytes := []byte("remote-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// Node F is synced at a.txt (the source), file on disk there.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: md4.ContentHash(origBytes),
		LocalHash: md4.ContentHash(origBytes), LocalSize: int64(len(origBytes)),
		ServerSyncID: 50, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Both events are remote (no CommitterSessionID == "sess").
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The download rode behind the queued rename: the modified bytes end up at b.txt, a.txt gone.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived: %v", err)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(F) = %q (err %v), want b.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundSelfEchoMoveRedirectedDownloadOfDirectoryMkdirsAtTheCurrentPath joins the redirect
// above to the errDownloadNodeIsDir conversion. applyDownload works on a COPY of the op and
// rebases its path onto the node's current tracked location; downloadNodeIsDir then probes THAT
// path, while applyOp still holds the queue record's stale spelling. Building the replacement
// OpMkdir from the stale path placed the directory where the node no longer is - markSynced there
// evicts the live baseline through the byid index, and a directory record still standing at the
// stale path makes relocateTrackedDirForMkdir rename the user's just-relocated subtree back to the
// old name, after which disk and baseline agree and localReconcile never re-derives anything. So
// the resolved path rides with the signal, and the compensation is queued at b.txt.
func TestInboundSelfEchoMoveRedirectedDownloadOfDirectoryMkdirsAtTheCurrentPath(t *testing.T) {
	origBytes := []byte("last-synced-content-h1")
	serverHash := md4.ContentHash([]byte("remote-modify-content-h2"))

	c := &pushClient{
		// The transfer fails with a server verdict, and the kind probe of the REDIRECTED path
		// places the node - id-matched, is_dir positively set - as a directory.
		downloadErr: actions.ErrPathError,
		queryNode:   map[string]actions.Node{"b.txt": {FileID: "F", IsDir: true}},
	}
	e, st, _ := seedSelfEchoMovePreState(t, c, origBytes)

	if _, _, err := e.reconcile(7, selfEchoMoveBatch(serverHash, 24), 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// One pass: the download leaves the queue and its replacement is enqueued behind it (the
	// replacement holds the path key for the rest of the pass, so it drains on the next one).
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	var comps []store.QueuedOp
	if err := st.ScanPending(func(qo store.QueuedOp) error {
		comps = append(comps, qo)
		return nil
	}); err != nil {
		t.Fatalf("ScanPending: %v", err)
	}
	if len(comps) != 1 {
		t.Fatalf("queued ops = %+v, want exactly the compensating mkdir", comps)
	}
	if comps[0].Type != store.OpMkdir || comps[0].Direction != store.DirInbound {
		t.Fatalf("compensation = %s/%s, want an inbound OpMkdir", comps[0].Type, comps[0].Direction)
	}
	if comps[0].Path != "b.txt" {
		t.Errorf("compensation path = %q, want the node's CURRENT path %q (a.txt is the stale spelling the redirect left behind)",
			comps[0].Path, "b.txt")
	}
	if comps[0].FileID != "F" {
		t.Errorf("compensation file id = %q, want the moved node's id", comps[0].FileID)
	}
}
