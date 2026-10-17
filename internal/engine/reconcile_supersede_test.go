package engine

// These tests pin the trailing-modify fix: a trailing modify of a node whose queued download
// a SELF-ECHO move relocated used to bypass the self-echo redirect gate - markUpsertPending
// flips the relocated record back to StatePending, downloadRelocatedBySelfEcho (which requires
// StateSynced at cur) goes dark, and the stale and the fresh download of the SAME file id applied
// in parallel on disjoint conflict keys with order-dependent divergent outcomes (the freshly
// synced file deleted at its current path / a synced baseline over an empty path whose next scan
// pushed an outbound OpRemove of the live server node / spurious _Conflict copies).
//
// The fix is two-sided. (a) modify-supersedes-download: enqueuing a download with a non-empty
// file id first drops every OLDER queued download of the same id (dropSupersededSameIDDownloads)
// - a download always fetches the node's CURRENT content by id, so the older op is pure
// liability - EXCEPT when a queued inbound OpRename of the id carries it (the non-self-echo
// cross-batch flow, whose rename bridges both paths' conflict keys). (b) an inbound download's
// serialize keys additionally include the node's byid-resolved current path (opSerializeKeys),
// so a redirected apply never mutates a location outside the op's declared key set.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// trailingModifyBatch is the [modify F@a (stale pre-move path), self-echo move a->b, modify F@b]
// batch of the trailing-modify scenario: the first modify was committed server-side before our own
// rename landed, its echo arrives after, and a second remote modify at the current path trails it.
func trailingModifyBatch(staleHash string, staleSize int64, serverHash string, serverSize int64) []actions.Event {
	return []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: staleHash, Size: staleSize, MTime: 555},
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "sess"},
		{SyncID: 102, Action: "modify", FileID: "F", Path: "b.txt", Hash: serverHash, Size: serverSize, MTime: 777},
	}
}

// assertConvergedAtB asserts the post-drain trailing-modify convergence: the server bytes at b.txt
// only (no ghost at a.txt, no _Conflict copies), a synced baseline at b.txt, an empty queue and no
// dead letters or conflict records.
func assertConvergedAtB(t *testing.T, st *store.Store, viewDir string, serverBytes []byte, serverHash string) {
	t.Helper()
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
		t.Errorf("view dir = %v, want only b.txt (no ghost, no _Conflict copy)", names)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "F" || b.State != store.StateSynced || b.LocalHash != serverHash {
		t.Errorf("b.txt baseline = %+v, want synced F/%s", b, serverHash)
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

// TestInboundTrailingModifySupersedesStaleDownload is the single-batch trailing-modify scenario:
// node F is synced at b.txt (our local rename already pushed), and one batch delivers [modify
// F@a.txt (stale pre-move path), self-echo move a.txt->b.txt, modify F@b.txt]. The trailing
// modify's enqueue must drop the stale download at a.txt as superseded - before the fix both
// downloads stayed queued (the trailing markUpsertPending flipped the record StatePending,
// extinguishing the redirect gate) and applied in parallel on disjoint keys, with divergent
// order-dependent outcomes. After the fix exactly one download (b.txt) remains, the drain
// converges, and the next local scan emits nothing - in particular no outbound OpRemove of the live
// server node.
func TestInboundTrailingModifySupersedesStaleDownload(t *testing.T) {
	origBytes := []byte("last-synced-content-h0")
	staleBytes := []byte("first-modify-content-h1")
	serverBytes := []byte("second-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 777, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)

	batch := trailingModifyBatch(md4.ContentHash(staleBytes), int64(len(staleBytes)), serverHash, int64(len(serverBytes)))
	if _, _, err := e.reconcile(7, batch, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Exactly ONE download is queued - the newest, at the node's current path. The stale one at
	// a.txt was dropped as superseded when the trailing modify enqueued its replacement.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpDownload || pending[0].Path != "b.txt" {
		t.Fatalf("pending = %+v, want exactly one OpDownload at b.txt", pending)
	}
	if pending[0].ServerSyncID != 102 {
		t.Errorf("queued download ServerSyncID = %d, want 102 (the newest event)", pending[0].ServerSyncID)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertConvergedAtB(t, st, viewDir, serverBytes, serverHash)

	// The next local scan sees agreement everywhere: no op is emitted, in particular no
	// outbound OpRemove of the live server node (the second divergent outcome above).
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("next scan emitted %+v, want nothing", ops)
	}
}

// TestInboundTrailingModifyCrossBatchSupersedesStaleDownload is the cross-batch shape: the stale
// modify's download is queued from batch 1 (its drain deferred by a transient failure), and the
// [self-echo move, trailing modify] pair arrives in batch 2. The trailing modify's enqueue must
// still find and drop the stale prior-batch download (the queue scan is durable-state-driven, not
// per-batch), leaving exactly one download that converges cleanly.
func TestInboundTrailingModifyCrossBatchSupersedesStaleDownload(t *testing.T) {
	origBytes := []byte("last-synced-content-h0")
	staleBytes := []byte("first-modify-content-h1")
	serverBytes := []byte("second-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 777, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := seedSelfEchoMovePreState(t, c, origBytes)

	// Batch 1: the stale modify at the pre-move path. The download is enqueued but not drained.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: md4.ContentHash(staleBytes), Size: int64(len(staleBytes)), MTime: 555},
	}, 0); err != nil {
		t.Fatalf("reconcile batch 1: %v", err)
	}
	// Batch 2: our self-echo move (enqueues nothing, re-stamps the record synced) followed by the
	// trailing remote modify at the current path.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt", CommitterSessionID: "sess"},
		{SyncID: 102, Action: "modify", FileID: "F", Path: "b.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 777},
	}, 100); err != nil {
		t.Fatalf("reconcile batch 2: %v", err)
	}

	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpDownload || pending[0].Path != "b.txt" {
		t.Fatalf("pending = %+v, want exactly one OpDownload at b.txt (stale prior-batch download dropped)", pending)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertConvergedAtB(t, st, viewDir, serverBytes, serverHash)
}

// TestInboundCarriedDownloadNotSuperseded is the carried-download regression: in the NON-self-echo
// cross-batch flow - batch 1 [modify F@a, move F a->b (both remote)], batch 2 [modify F@b] - the
// first download is CARRIED by the queued inbound OpRename, which bridges both paths' conflict
// keys, so the supersession must leave it queued (queuedInboundRenameCarries): download#1 lands
// at a.txt, the rename carries it to b.txt, and the second download resolves on top (here through
// the keep-both funnel's false-conflict adoption, since the by-id fetch already delivered the
// newest bytes). The pinned assertions: the carried download survives the trailing modify's
// enqueue, and the whole chain still converges with no divergence artifacts.
func TestInboundCarriedDownloadNotSuperseded(t *testing.T) {
	origBytes := []byte("last-synced-content-h0")
	serverBytes := []byte("second-modify-content-h2")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"F": serverBytes},
		downloadNode:    map[string]actions.Node{"F": {FileID: "F", Hash: serverHash, MTime: 777, Size: int64(len(serverBytes))}},
		queryNode: map[string]actions.Node{
			// The second download funnels through keep-both (its inherited base is the pre-batch
			// hash, the on-disk bytes are already the newest); the resolver re-queries the node
			// and adopts the false conflict (local == server), preserving nothing.
			"b.txt": {FileID: "F", Hash: serverHash, MTime: 777, Size: int64(len(serverBytes))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// Node F is synced at a.txt (the move source), file on disk there.
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

	// Batch 1: both events remote - the rename that will carry download#1 is enqueued.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 100, Action: "modify", FileID: "F", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 101, Action: "move", FileID: "F", FromPath: "a.txt", Path: "b.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile batch 1: %v", err)
	}
	// Batch 2: the trailing remote modify at the destination.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 102, Action: "modify", FileID: "F", Path: "b.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 777},
	}, 101); err != nil {
		t.Fatalf("reconcile batch 2: %v", err)
	}

	// The carried download at a.txt survives: the queue holds download@a, rename a->b, download@b.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending = %+v, want 3 ops (carried download untouched by the supersession)", pending)
	}
	if pending[0].Type != store.OpDownload || pending[0].Path != "a.txt" {
		t.Errorf("pending[0] = %+v, want the carried OpDownload at a.txt", pending[0])
	}
	if pending[1].Type != store.OpRename || pending[1].Path != "a.txt" || pending[1].NewPath != "b.txt" {
		t.Errorf("pending[1] = %+v, want OpRename a.txt->b.txt", pending[1])
	}
	if pending[2].Type != store.OpDownload || pending[2].Path != "b.txt" {
		t.Errorf("pending[2] = %+v, want the trailing OpDownload at b.txt", pending[2])
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	assertConvergedAtB(t, st, viewDir, serverBytes, serverHash)
}

// TestOpSerializeKeysIncludesRelocatedDownloadPath pins the serialization half of the
// trailing-modify fix: an inbound download carrying a file id whose node is tracked at a DIFFERENT
// path serialises on BOTH the enqueue-time op.Path and the byid-resolved current path, so a
// redirected apply (downloadRelocatedBySelfEcho rebases op.Path to cur) never mutates a location
// outside the op's declared conflict-key set. Every other shape keeps the pure serializeKeys set
// unchanged.
func TestOpSerializeKeysIncludesRelocatedDownloadPath(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "F", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	keyA := cleanKey("Photos", "a.txt")
	keyB := cleanKey("Photos", "b.txt")

	tests := []struct {
		name string
		op   store.Op
		want []string
	}{
		{
			name: "relocated inbound download carries both paths",
			op:   store.Op{Direction: store.DirInbound, Type: store.OpDownload, ViewID: 7, Path: "a.txt", FileID: "F"},
			want: []string{keyA, keyB},
		},
		{
			name: "download already at the tracked path keeps one key",
			op:   store.Op{Direction: store.DirInbound, Type: store.OpDownload, ViewID: 7, Path: "b.txt", FileID: "F"},
			want: []string{keyB},
		},
		{
			name: "id-less download keeps one key",
			op:   store.Op{Direction: store.DirInbound, Type: store.OpDownload, ViewID: 7, Path: "a.txt"},
			want: []string{keyA},
		},
		{
			name: "untracked id keeps one key",
			op:   store.Op{Direction: store.DirInbound, Type: store.OpDownload, ViewID: 7, Path: "a.txt", FileID: "Z"},
			want: []string{keyA},
		},
		{
			name: "non-download direction/type is untouched",
			op:   store.Op{Direction: store.DirOutbound, Type: store.OpUpload, ViewID: 7, Path: "a.txt", FileID: "F"},
			want: []string{keyA},
		},
		{
			name: "rename keeps its two endpoint keys",
			op:   store.Op{Direction: store.DirInbound, Type: store.OpRename, ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "F"},
			want: []string{keyA, cleanKey("Photos", "c.txt")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := e.opSerializeKeys("Photos", tc.op)
			if len(got) != len(tc.want) {
				t.Fatalf("opSerializeKeys = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("opSerializeKeys = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
