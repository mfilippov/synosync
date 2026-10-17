package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestReconcileCaseInsensitiveDownloadBase is a regression test: on a
// case-insensitive filesystem a node tracked as "Foo.txt" can receive a remote modify
// addressed as "foo.txt" for the same file id. The inbound-download divergence guard must
// resolve its base baseline by file id (not an exact GetBaseline(ev.Path)) or it captures an
// empty BaseLocalHash and applyDownload would silently clobber a cheap-scan-missed local edit.
// The resulting OpDownload must carry the synced base hash.
func TestReconcileCaseInsensitiveDownloadBase(t *testing.T) {
	restore := normpath.SetCaseInsensitive(true)
	defer restore()

	e, st := newReconcileEngine(t, "self")
	const syncedHash = "synced-local-hash"
	// Baseline tracked under the original case, StateSynced with a recorded local hash.
	if err := st.PutBaseline(1, "Foo.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "orig", LocalHash: syncedHash,
		LocalSize: 3, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// A remote modify for the SAME file id but a differently-cased path.
	events := []actions.Event{
		{SyncID: 9, Action: "modify", FileID: "f1", Path: "foo.txt", Hash: "h2", Size: 5, MTime: 200},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want one download", ops)
	}
	if ops[0].BaseLocalHash != syncedHash {
		t.Errorf("download BaseLocalHash = %q, want %q (base must resolve by file id across case)",
			ops[0].BaseLocalHash, syncedHash)
	}
}

// TestReconcileDownloadBaseFollowsMove is a regression test: a single batch of
// modify a.txt, move a.txt->b.txt, modify b.txt. The first download (a.txt) captures the synced
// base hash and markPending then wipes it; the move relocates the (now pending) baseline to b.txt,
// so the second download's own baseline lookup finds no synced hash. The base-inheritance map must
// follow the node by file id across the move, or the second download (b.txt) carries an empty
// BaseLocalHash and applyDownload silently clobbers a cheap-scan-missed local edit the rename just
// carried from a.txt to b.txt. Both downloads must carry the synced base so each funnels.
func TestReconcileDownloadBaseFollowsMove(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	const syncedHash = "synced-local-hash"
	// a.txt tracked synced with a recorded local hash; same file id across the whole batch.
	if err := st.PutBaseline(1, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "orig", LocalHash: syncedHash,
		LocalSize: 3, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "h1", Size: 5, MTime: 200},
		{SyncID: 7, Action: "move", FileID: "f1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 9, Action: "modify", FileID: "f1", Path: "b.txt", Hash: "h2", Size: 6, MTime: 300},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("ops = %+v, want three (download a.txt, rename, download b.txt)", ops)
	}
	var dlA, dlB *store.Op
	for i := range ops {
		switch {
		case ops[i].Type == store.OpDownload && ops[i].Path == "a.txt":
			dlA = &ops[i]
		case ops[i].Type == store.OpDownload && ops[i].Path == "b.txt":
			dlB = &ops[i]
		}
	}
	if dlA == nil || dlB == nil {
		t.Fatalf("ops = %+v, want a download for a.txt and one for b.txt", ops)
	}
	if dlA.BaseLocalHash != syncedHash {
		t.Errorf("a.txt download BaseLocalHash = %q, want %q", dlA.BaseLocalHash, syncedHash)
	}
	if dlB.BaseLocalHash != syncedHash {
		t.Errorf("b.txt download BaseLocalHash = %q, want %q (base must follow the node across the move)",
			dlB.BaseLocalHash, syncedHash)
	}
}

// TestInboundDownloadFunnelsLocalDivergence is the Phase-9d safety-net regression test:
// the cheap scan can miss a same-size, same-mtime-second local edit, so no
// outbound upload is queued and reconcile finds nothing to funnel through keep-both. If a remote
// modify for the same path then arrives, the inbound download must NOT silently overwrite the
// unrecorded local edit - applyDownload re-hashes the on-disk file against the captured base hash,
// detects the divergence, and routes it to the keep-both resolver so both versions survive.
func TestInboundDownloadFunnelsLocalDivergence(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	// origBytes is what was last synced; the baseline still records its hash (the cheap scan
	// never re-hashed the file). localBytes is the unrecorded local edit on disk. serverBytes is
	// the new remote modify. All three differ.
	origBytes := []byte("original-content-v0")
	localBytes := []byte("local-edit-not-yet-pushed")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: the local edit. Baseline: still the last-synced facts (cheap scan missed it).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// A remote modify for a.txt arrives. No outbound upload is queued (the cheap scan missed
	// the edit), so reconcile cannot defer via the pending-upload funnel.
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path now holds the server bytes (the resolver promoted them)...
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// ...and the local edit was preserved as the _Conflict copy rather than lost.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy content = %q (err %v), want the preserved local edit %q", got, err, localBytes)
	}
	// A durable conflict record was written.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one a.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundDownloadMoveFunnelDropsStaleResolver is a regression test: a single batch
// of modify a.txt, move a.txt->b.txt, modify b.txt against a locally-diverged file (a cheap-scan-
// missed edit). Both downloads now correctly carry BaseLocalHash, so the a.txt download funnels to
// keep-both instead of clobbering - but that enqueues an OpResolveConflict for the OLD path a.txt.
// By the time that resolver drains, the queued inbound rename has moved the local bytes to b.txt and
// the server node lives on as b.txt, so QueryNode(a.txt) returns ErrPathError while the parent
// listing still finds the node (by file id) under b.txt. The resolver must recognise the node moved
// away (with no local content left at a.txt) and DROP the stale op, not retry QueryNode(a.txt)
// forever and wedge the queue. The real divergence is resolved at b.txt's own resolver.
func TestInboundDownloadMoveFunnelDropsStaleResolver(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("original-content-v0")
	localBytes := []byte("local-edit-not-yet-pushed")
	serverBytes := []byte("remote-modify-version-b")
	origHash := md4.ContentHash(origBytes)
	serverHashB := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// a.txt is gone server-side (moved to b.txt); b.txt holds the diverged server node.
		queryNodeErrByPath: map[string]error{"a.txt": actions.ErrPathError},
		queryNode: map[string]actions.Node{
			"b.txt": {FileID: "srv1", Hash: serverHashB, MTime: 555, Size: int64(len(serverBytes))},
		},
		// The parent listing the stale a.txt resolver issues still finds the node - under b.txt.
		listResult: map[string][]actions.Node{
			"": {{Name: "b.txt", FileID: "srv1"}},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHashB, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: the local edit at a.txt. Baseline: still the last-synced facts (cheap scan missed it).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// modify a.txt, move a.txt->b.txt, modify b.txt - one batch, same file id throughout.
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: "stale-a", Size: 3, MTime: 200},
		{SyncID: 7, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 9, Action: "modify", FileID: "srv1", Path: "b.txt", Hash: serverHashB, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The decisive assertion: the drain MUST complete. Before the fix the stale a.txt resolver
	// (QueryNode(a.txt) ErrPathError, node found under b.txt) returns a transient error every pass,
	// so drainQueue never returns nil and the queue never empties.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (the stale a.txt resolver wedged the queue)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain (stale resolver not dropped)", n)
	}

	// b.txt holds the server bytes (its own resolver promoted them) and the local edit survives as
	// the _Conflict copy - the move carried the divergent bytes to b.txt where keep-both ran.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt content = %q (err %v), want server bytes %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy content = %q (err %v), want preserved local edit %q", got, err, localBytes)
	}
	// No phantom a.txt conflict baseline was synthesised for the dropped stale resolver.
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Errorf("a.txt baseline still present; the dropped stale resolver must not synthesise one")
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
}

// TestInboundDownloadMoveNoSecondModifyKeepsBoth is a regression test: a
// batch of modify a.txt, move a.txt->b.txt with NO downstream modify b.txt, against a locally-
// diverged file (a cheap-scan-missed edit). The a.txt download funnels to keep-both, enqueuing a
// resolve for the OLD path a.txt. The inbound rename then carries the local bytes to b.txt and the
// server node lives on as b.txt. Unlike the modify->move->modify case there is NO resolver at b.txt
// to keep both versions, so dropping the stale a.txt resolver as "moot" would silently lose the
// server modify (the next scan would upload the local edit, clobbering it) with no conflict copy.
// The resolver must recognise the node was relocated (via the store byid index) and re-funnel the
// keep-both at b.txt, where both versions are preserved.
func TestInboundDownloadMoveNoSecondModifyKeepsBoth(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
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

	// On disk: the local edit at a.txt. Baseline: still the last-synced facts (cheap scan missed it).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// modify a.txt, move a.txt->b.txt - one batch, same file id, NO modify b.txt.
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 7, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "b.txt"},
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

	// The decisive assertions: keep-both ran at b.txt - the server version landed and the local edit
	// survives as the _Conflict copy. Before the fix the stale a.txt resolver was dropped with no
	// downstream resolver, so neither happened and the server modify was lost.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt content = %q (err %v), want server bytes %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy content = %q (err %v), want preserved local edit %q", got, err, localBytes)
	}
	// No phantom a.txt baseline for the dropped stale resolver.
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Errorf("a.txt baseline still present; the dropped stale resolver must not synthesise one")
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
}

// TestInboundDownloadCrossDirMoveKeepsBoth is a regression test: a batch of
// modify a.txt, move a.txt->sub/b.txt (a CROSS-directory move) against a locally-diverged file. The
// a.txt download funnels to keep-both at the old path a.txt; the inbound rename then carries the
// local bytes to sub/b.txt. The old-parent listing the stale a.txt resolver would issue cannot find
// the node (it now lives in sub/), so resolveServerNodeAbsence mis-reads it as a vanish and
// markOutboundConflict synthesises a phantom a.txt baseline whose file id (srv1) collides with - and
// so DELETES, via the byid index - the real moved baseline at sub/b.txt. The resolver must instead
// consult the store, see srv1 relocated to sub/b.txt, and re-funnel keep-both there - leaving the
// real baseline intact and preserving both versions.
func TestInboundDownloadCrossDirMoveKeepsBoth(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("original-content-v0")
	localBytes := []byte("local-edit-not-yet-pushed")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("sub/b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"sub/b.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// A destination directory (with a tracked baseline so the copy upload can resolve its parent id).
	if err := os.Mkdir(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("seed sub dir: %v", err)
	}
	if err := st.PutBaseline(7, "sub", store.Baseline{
		FileID: "subid", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed sub baseline: %v", err)
	}
	// On disk: the local edit at a.txt. Baseline: still the last-synced facts (cheap scan missed it).
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// modify a.txt, move a.txt->sub/b.txt - one batch, same file id, cross-directory move.
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 7, Action: "move", FileID: "srv1", FromPath: "a.txt", Path: "sub/b.txt"},
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

	// Keep-both ran at the new path: the server version landed and the local edit survives as the copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, "sub", "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("sub/b.txt content = %q (err %v), want server bytes %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(localBytes) {
		t.Fatalf("conflict copy content = %q (err %v), want preserved local edit %q", got, err, localBytes)
	}
	// The decisive assertion: the real moved baseline at sub/b.txt is intact (file id srv1), NOT
	// deleted by a phantom a.txt baseline colliding on the byid index.
	mb, err := st.GetBaseline(7, "sub/b.txt")
	if err != nil {
		t.Fatalf("GetBaseline sub/b.txt: %v (the real moved baseline was clobbered)", err)
	}
	if mb.FileID != "srv1" || mb.State != store.StateSynced {
		t.Errorf("sub/b.txt baseline = %+v, want synced srv1", mb)
	}
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Errorf("a.txt baseline still present; the dropped stale resolver must not synthesise one")
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "sub/b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one sub/b.txt -> %s", recs, copyRel)
	}
}

// TestInboundDownloadDoubleModifyFunnelsDivergence is a regression test: when a single
// path receives MORE THAN ONE inbound download (two remote modifies in the same batch, or a backlog
// split across batches), only the first sees the baseline StateSynced and captures BaseLocalHash -
// markPending then wipes the synced hash. Without inheriting that base, the SECOND download carries
// an empty base and clobbers the unrecorded local edit before the keep-both resolver the first
// download funnelled (enqueued at the queue tail) can run in a later drain. Both the within-batch
// and cross-batch seedings of the base must keep every download funnelling so the local edit
// survives. The two sub-tests differ only in whether the two modifies arrive in one reconcile call
// (same-batch capture map) or two (queued-download base scan).
func TestInboundDownloadDoubleModifyFunnelsDivergence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sameBatch bool
	}{
		{"same batch", true},
		{"cross batch", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
			const host = "laptop"

			origBytes := []byte("original-content-v0")
			localBytes := []byte("local-edit-not-yet-pushed")
			serverBytes := []byte("remote-modify-version")
			origHash := md4.ContentHash(origBytes)
			serverHash := md4.ContentHash(serverBytes)
			localHash := md4.ContentHash(localBytes)

			copyRel := conflictCopyName("a.txt", host, ts)
			copyName := filepath.Base(copyRel)

			c := &pushClient{
				queryNode: map[string]actions.Node{
					"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
				},
				downloadContent: map[string][]byte{"srv1": serverBytes},
				downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
				uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
			}
			e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
			pinClock(e, host, ts)

			if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
				t.Fatalf("seed local file: %v", err)
			}
			if err := st.PutBaseline(7, "a.txt", store.Baseline{
				FileID: "srv1", FileType: "file", ServerHash: origHash,
				LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
			}); err != nil {
				t.Fatalf("seed baseline: %v", err)
			}

			// Two remote modifies for a.txt. In the same-batch case both arrive in one reconcile;
			// in the cross-batch case the second arrives in a separate reconcile (the first download
			// is already queued, baseline already StatePending) before any drain.
			ev1 := actions.Event{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555}
			ev2 := actions.Event{SyncID: 6, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 556}
			if tc.sameBatch {
				if _, _, err := e.reconcile(7, []actions.Event{ev1, ev2}, 0); err != nil {
					t.Fatalf("reconcile: %v", err)
				}
			} else {
				if _, _, err := e.reconcile(7, []actions.Event{ev1}, 0); err != nil {
					t.Fatalf("reconcile batch1: %v", err)
				}
				if _, _, err := e.reconcile(7, []actions.Event{ev2}, 5); err != nil {
					t.Fatalf("reconcile batch2: %v", err)
				}
			}
			if err := e.drainQueue(context.Background()); err != nil {
				t.Fatalf("drainQueue: %v", err)
			}

			if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
				t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
			}
			// The decisive assertion: the local edit was preserved as a _Conflict copy. Without the
			// base-inheritance fix the second download overwrites it before the resolver runs and the
			// copy never exists.
			if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
				t.Fatalf("conflict copy content = %q (err %v), want the preserved local edit %q", got, err, localBytes)
			}
			recs, err := st.ListConflicts(7)
			if err != nil {
				t.Fatalf("ListConflicts: %v", err)
			}
			if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
				t.Fatalf("conflict records = %+v, want one a.txt -> %s", recs, copyRel)
			}
			if n, _ := st.PendingLen(); n != 0 {
				t.Errorf("queue not drained: %d ops remain", n)
			}
		})
	}
}

// TestInboundDownloadFalseConflictStampsLatestSyncID is a regression test: two same-path
// downloads with NO local edit. The first applies the current server bytes and stamps its (older)
// sync id; the second inherits a base predating that apply, so it sees the just-written server bytes
// as a divergence and funnels into keep-both. The resolver takes the false-conflict fast path (local
// already matches server). The funnelling download's sync id must be carried into the resolve op and
// stamped onto the resolved baseline, so the baseline records the LATEST server event (sync 6), not
// the first download's (sync 5).
func TestInboundDownloadFalseConflictStampsLatestSyncID(t *testing.T) {
	origBytes := []byte("original-content-v0")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// On disk and baseline agree: no unrecorded local edit.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), ServerSyncID: 1, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Two remote modifies for a.txt in one batch (sync 5 then 6).
	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
		{SyncID: 6, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 556},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Content converged to the server bytes and the false conflict raised no copy/record.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (false conflict)", recs)
	}
	// The decisive assertion: the resolved baseline records the latest server event, not sync 5.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.ServerSyncID != 6 {
		t.Errorf("baseline = %+v, want synced with ServerSyncID 6", b)
	}
}

// TestInboundDownloadNoDivergenceOverwrites confirms the safety net does not fire on the common
// case: when the on-disk file still matches the last-synced hash (no local edit), the inbound
// download overwrites it normally and no keep-both conflict is raised.
func TestInboundDownloadNoDivergenceOverwrites(t *testing.T) {
	origBytes := []byte("original-content-v0")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// On disk and baseline agree: no unrecorded edit.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "srv1", Path: "a.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 555},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The download overwrote the file with the server bytes; no conflict copy or record.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("content = %q (err %v), want %q", got, err, serverBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (no keep-both)", len(c.uploads))
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %d, want 0", len(recs))
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != serverHash {
		t.Errorf("baseline = %+v, want synced with server hash %s", b, serverHash)
	}
}

// TestResolveConflictReadOnlyViewMovedNode is a regression test: a
// keep-both resolve op enqueued at the OLD path a.txt while the view was writable now drains under a
// read-only view, but the diverged node (srv1) was relocated server-side to b.txt by an inbound
// rename that already ran. The read-only early branch returns BEFORE resolveConflict's moved-node
// guard, so without the fix it calls markOutboundConflict(a.txt): a.txt has no baseline (it moved to
// b.txt), so a phantom conflict baseline is synthesised carrying file id srv1, which through the byid
// index DELETES the real moved baseline at b.txt. The branch must instead re-funnel a fresh resolve
// at b.txt and drop the stale op without touching any baseline.
func TestResolveConflictReadOnlyViewMovedNode(t *testing.T) {
	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", Privilege: "ro"}); err != nil {
		t.Fatalf("downgrade view: %v", err)
	}

	// The inbound rename already carried the local bytes (and the baseline) to b.txt; the stale
	// resolve op still names the old path a.txt but carries the node's file id srv1.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The decisive assertion: the real moved baseline at b.txt is intact (file id srv1), NOT deleted
	// by a phantom a.txt baseline colliding on the byid index.
	mb, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline b.txt: %v (the real moved baseline was clobbered)", err)
	}
	if mb.FileID != "srv1" {
		t.Errorf("b.txt baseline = %+v, want file id srv1 preserved", mb)
	}
	if p, err := st.PathByID(7, "srv1"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(srv1) = %q (err %v), want b.txt (not re-pointed to the stale path)", p, err)
	}
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Errorf("a.txt baseline still present; the read-only branch must not synthesise a phantom one")
	}
	// No server turn: the read-only relocation handling is store-only (re-funnel + the re-funnelled
	// op also dead-letters under read-only without a lease).
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0", len(c.queryNodeCalls))
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0", len(c.uploads))
	}
}

// TestLocalReconcileProtectsMovedResolvePath is a regression test: a
// keep-both resolve op is queued at the OLD path a.txt while its diverged node (srv1) was relocated
// server-side to b.txt by an inbound rename that already ran. A fresh local edit then lands at b.txt.
// The queued-path index must protect the node's CURRENT path (b.txt, resolved by file id), not just
// the stale op.Path a.txt, or localReconcile re-detects the edit at b.txt and enqueues a racing
// OpUpload that bypasses keep-both - overwriting the diverged server version before the resolver
// re-funnels to b.txt.
func TestLocalReconcileProtectsMovedResolvePath(t *testing.T) {
	origBytes := []byte("original-content-v0")
	freshBytes := []byte("fresh-local-edit-at-new-path")
	origHash := md4.ContentHash(origBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// The node lives at b.txt (relocated). On disk it now holds a fresh edit diverging from the
	// tracked LocalHash, so a plain scan would want to upload it.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), freshBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}
	// The stale resolve op still names the old path a.txt but carries the node's file id srv1.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	// The decisive assertion: NO outbound op for b.txt - the queued resolve protects the node's
	// current path. Before the fix only a.txt was protected and an OpUpload(b.txt) was emitted.
	for _, op := range ops {
		t.Errorf("unexpected op %+v; b.txt should be protected by the queued resolve for srv1", op)
	}
}

// TestInboundDownloadFreshCreateIntruderMidTransferPreserved is a
// regression test: a fresh server create (no BaseLocalHash) whose target path is FREE when the
// pre-transfer guard runs, but where a local file lands WHILE the download streams. The old
// replacing promote (os.Rename) silently clobbered that file - and since the promoted content
// then equals the baseline, no later scan could ever notice the loss. The promote must be a
// no-replace rename that fails on the intruder and funnels through keep-both: the user file
// survives as a _Conflict copy and the server version lands on the canonical path.
func TestInboundDownloadFreshCreateIntruderMidTransferPreserved(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	intruderBytes := []byte("user-file-created-mid-transfer")
	serverBytes := []byte("brand-new-server-content")
	serverHash := md4.ContentHash(serverBytes)
	intruderHash := md4.ContentHash(intruderBytes)
	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: intruderHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The intruder lands during the first Download call - after the pre-transfer guard found
	// the path free, before the promote.
	c.downloadHook = func() {
		if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), intruderBytes, 0o644); err != nil {
			t.Errorf("write intruder: %v", err)
		}
	}

	// reconcile pre-writes a pending baseline for a fresh create before the op drains.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
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
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The canonical path holds the server bytes (the resolver promoted them)...
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// ...and the decisive assertion: the mid-transfer user file survives as the _Conflict copy.
	// Before the fix the replacing os.Rename clobbered it and no copy ever existed.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(intruderBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved intruder %q", got, err, intruderBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one a.txt -> %s", recs, copyRel)
	}
}

// TestInboundDownloadModifyMidTransferEditFunnels is the modify-path
// regression: the pre-transfer divergence guard passes (the on-disk file still matches the
// captured base), but a local edit lands WHILE the download streams. The old code promoted
// unconditionally after the transfer, replacing the edit with the server bytes - and since the
// promoted content then equals the synced baseline, the loss was invisible to every later scan.
// The post-transfer recheck must re-hash the target right before the rename, detect the
// divergence and funnel through keep-both so both versions survive.
func TestInboundDownloadModifyMidTransferEditFunnels(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("original-content-v0")
	editedBytes := []byte("local-edit-landed-mid-transfer")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
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

	// On disk: the last-synced content, matching the captured base - the pre-transfer guard passes.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The edit lands during the first Download call - after the pre-transfer guard, before the
	// promote.
	c.downloadHook = func() {
		if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), editedBytes, 0o644); err != nil {
			t.Errorf("write mid-transfer edit: %v", err)
		}
	}

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
		BaseLocalHash: origHash,
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
	// The decisive assertion: the mid-transfer edit survives as the _Conflict copy. Before the
	// fix the post-transfer promote replaced it and no copy ever existed.
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
	if b.State != store.StateSynced || b.LocalHash != serverHash {
		t.Errorf("baseline = %+v, want synced with server hash %s", b, serverHash)
	}
}

// TestRemoveRelocatedNodePreservesLocalEdit is a regression test for
// removeRelocatedNode: a server delete resolves its node by file id to a relocated path (a
// prior rejected rename re-tracked it there) and the file at that path holds a local edit the
// server never saw. dropRemoveBaselineAt used to fsutil.Remove it by identity alone, with no
// hash-vs-base check - destroying the edit with no _Conflict copy. The guard must set the
// diverged content aside first, mirroring the primary guardRemoveDivergence path.
func TestRemoveRelocatedNodePreservesLocalEdit(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("last-synced-content")
	editedBytes := []byte("local-edit-at-relocated-path")
	origHash := md4.ContentHash(origBytes)
	copyRel := conflictCopyName("kept.txt", host, ts)

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// The node f1 is tracked (synced, with a recorded last-synced hash) at kept.txt, but the
	// on-disk file was edited since.
	if err := os.WriteFile(filepath.Join(viewDir, "kept.txt"), editedBytes, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	pinClock(e, host, ts)

	// The server delete names the node at its (empty) server path; only byid reaches kept.txt.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The delete took effect at the relocated path...
	if _, err := os.Lstat(filepath.Join(viewDir, "kept.txt")); !os.IsNotExist(err) {
		t.Errorf("kept.txt still present: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "kept.txt"); !isNotFound(err) {
		t.Errorf("kept.txt baseline survived the delete: err=%v", err)
	}
	if _, err := st.PathByID(7, "f1"); !isNotFound(err) {
		t.Errorf("byid entry survived the delete: err=%v", err)
	}
	// ...and the decisive assertion: the local edit survives as the _Conflict copy. Before the
	// fix dropRemoveBaselineAt deleted it outright and no copy ever existed.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(editedBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "kept.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one kept.txt -> %s", recs, copyRel)
	}
}

// TestRemoveRelocatedNodeSyncedEmptyHashPreserved pins the never-lose branch of
// relocatedRemoveBase that TestRemoveRelocatedNodePreservesLocalEdit above (seeding a NON-empty
// last-synced hash) leaves unexercised. The relocated node's live baseline is a synced FILE record
// with an EMPTY LocalHash - the inbound-rename bare/promoted-pending write anomaly (markSynced never
// leaves a real file record hash-less). With no trustworthy divergence base, the regular file at the
// relocated path must be preserved WHOLE as a _Conflict copy, not deleted under a false
// "synced-without-hash → server authority" read. Before this branch preserved, it returned
// ok=false and dropRemoveBaselineAt deleted the file outright with no copy.
func TestRemoveRelocatedNodeSyncedEmptyHashPreserved(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	onDisk := []byte("anomalous-empty-hash-record-must-not-be-clobbered")
	copyRel := conflictCopyName("kept.txt", host, ts)

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// f1 is tracked as a SYNCED FILE record with NO recorded LocalHash (the anomaly), with a regular
	// file on disk at the relocated path.
	if err := os.WriteFile(filepath.Join(viewDir, "kept.txt"), onDisk, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced, // LocalHash intentionally empty
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	pinClock(e, host, ts)

	// The server delete names the node at its (empty) server path; only byid reaches kept.txt.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The anomalous record's on-disk file survives WHOLE as the _Conflict copy rather than being
	// deleted (the decisive assertion): the empty hash is no divergence base, so preserve, never clobber.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(onDisk) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved content %q", got, err, onDisk)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "kept.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one kept.txt -> %s", recs, copyRel)
	}
}

// TestRemoveRelocatedNodeInSyncDeletesCleanly pins the negative side of the relocated-remove
// guard: when the file at the relocated path still matches its synced baseline's last-synced
// hash, the server-authoritative delete proceeds cleanly with no spurious _Conflict copy.
func TestRemoveRelocatedNodeInSyncDeletesCleanly(t *testing.T) {
	origBytes := []byte("last-synced-content")
	origHash := md4.ContentHash(origBytes)

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "kept.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "kept.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Lstat(filepath.Join(viewDir, "kept.txt")); !os.IsNotExist(err) {
		t.Errorf("kept.txt still present: err=%v", err)
	}
	// No conflict copy and no record: the content matched the last sync, server authority holds.
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("view dir entries = %v, want none (no spurious conflict copy)", entries)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
}

// TestRemoveRelocatedDirectoryPreservesDivergentSubtree covers the directory
// variant of the relocated-node delete. A locally edited descendant must move
// aside with the whole tree, and its stale baseline must be dropped so the
// preserved tree is discovered as local content on the next scan.
func TestRemoveRelocatedDirectoryPreservesDivergentSubtree(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	orig := []byte("last-synced")
	edited := []byte("locally-edited")
	copyRel := conflictCopyName("kept", host, ts)

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "kept"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "kept", "child.txt"), edited, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := st.PutBaseline(7, "kept", store.Baseline{FileID: "dir1", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline root: %v", err)
	}
	if err := st.PutBaseline(7, "kept/child.txt", store.Baseline{
		FileID: "child1", FileType: "file", LocalHash: md4.ContentHash(orig), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline child: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	pinClock(e, host, ts)
	if _, err := st.Enqueue(store.Op{Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "gone", FileID: "dir1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Lstat(filepath.Join(viewDir, "kept")); !os.IsNotExist(err) {
		t.Fatalf("relocated directory survived at canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel), "child.txt")); err != nil || string(got) != string(edited) {
		t.Fatalf("preserved child = %q (err %v), want %q", got, err, edited)
	}
	if _, err := st.GetBaseline(7, "kept/child.txt"); !isNotFound(err) {
		t.Fatalf("descendant baseline survived preservation: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "kept" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want kept -> %s", recs, copyRel)
	}
}

// TestRemoveRelocatedDirectoryInSyncDeletesCleanly pins the negative branch:
// a fully synced relocated subtree follows server authority without creating a
// conflict copy.
func TestRemoveRelocatedDirectoryInSyncDeletesCleanly(t *testing.T) {
	content := []byte("last-synced")
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "kept"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "kept", "child.txt"), content, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := st.PutBaseline(7, "kept", store.Baseline{FileID: "dir1", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline root: %v", err)
	}
	if err := st.PutBaseline(7, "kept/child.txt", store.Baseline{
		FileID: "child1", FileType: "file", LocalHash: md4.ContentHash(content), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline child: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "gone", FileID: "dir1"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "kept")); !os.IsNotExist(err) {
		t.Fatalf("clean relocated directory survived delete: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("conflict records = %+v, want none", recs)
	}
	// The DESCENDANT records go with the subtree. Dropping only the root left them behind as
	// StateSynced records with nothing on disk, and the next scan diffed each one as a vanished
	// file: an outbound OpRemove per descendant, every one against a node the server's own
	// recursive delete had already taken, each paced through the absence-corroboration streak.
	// That is the churn applyRemove's descendant drop exists to prevent, and this path never
	// did it.
	if _, err := st.GetBaseline(7, "kept/child.txt"); !isNotFound(err) {
		t.Errorf("descendant baseline survived the subtree delete (err %v); the next scan will "+
			"emit an outbound remove for a node the server already took", err)
	}
	if _, err := st.GetBaseline(7, "kept"); !isNotFound(err) {
		t.Errorf("root baseline survived: %v", err)
	}
}

// TestRemoveRelocatedUnresolvablePathKeepsDescendants pins the other side of that drop: when
// the path cannot be resolved on disk - it escapes the root, or sits behind a symlink - nothing
// was removed, so the files are probably still there and their records must stay. Dropping them
// would make a live subtree untracked and hand the next scan a re-upload of the whole thing.
func TestRemoveRelocatedUnresolvablePathKeepsDescendants(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// A path that escapes the view root: e.abs refuses it, so nothing is removed.
	const escaping = "../outside"
	if err := st.PutBaseline(7, escaping, store.Baseline{
		FileID: "dir1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline root: %v", err)
	}
	if err := st.PutBaseline(7, escaping+"/child.txt", store.Baseline{
		FileID: "child1", FileType: "file", LocalHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline child: %v", err)
	}

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", discardLogger())
	if err := e.dropRemoveBaselineAt(context.Background(),
		store.Op{Type: store.OpRemove, ViewID: 7, Path: "gone", FileID: "dir1"},
		escaping, true, true); err != nil {
		t.Fatalf("dropRemoveBaselineAt: %v", err)
	}

	if _, err := st.GetBaseline(7, escaping+"/child.txt"); err != nil {
		t.Errorf("a descendant was dropped although nothing was removed from disk: %v", err)
	}
}

// TestRemoveRelocatedPriorDirectoryPreservesUntrackedChild exercises the other
// caller of preserveRelocatedDivergence: stale-prior cleanup must not recursively
// delete a directory that took the old file path and contains untracked content.
func TestRemoveRelocatedPriorDirectoryPreservesUntrackedChild(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	copyRel := conflictCopyName("old", host, ts)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "old"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "old", "local.txt"), []byte("untracked"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := st.PutBaseline(7, "old", store.Baseline{FileID: "f1", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	pinClock(e, host, ts)
	if err := e.removeRelocatedPriorFile(context.Background(), store.Op{ViewID: 7, Path: "new", FileID: "f1"}); err != nil {
		t.Fatalf("removeRelocatedPriorFile: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel), "local.txt")); err != nil || string(got) != "untracked" {
		t.Fatalf("preserved child = %q (err %v), want untracked", got, err)
	}
}

// TestDownloadPreservesEditedRelocatedPriorFile is a regression test for
// removeRelocatedPriorFile: a download lands a node at op.Path while byid still tracks the same
// node at a prior path (a rejected rename re-tracked it there), and the prior file holds a local
// edit. The stale-copy cleanup used to fsutil.Remove it by identity alone - destroying the edit.
// The guard must verify the content against the prior baseline's last-synced hash and set a
// diverged file aside as a _Conflict copy before the remove.
func TestDownloadPreservesEditedRelocatedPriorFile(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	origBytes := []byte("last-synced-content")
	editedBytes := []byte("local-edit-at-prior-path")
	serverBytes := []byte("server-content-at-new-path")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)
	copyRel := conflictCopyName("old.txt", host, ts)

	c := &pushClient{
		downloadContent: map[string][]byte{"f1": serverBytes},
		downloadNode:    map[string]actions.Node{"f1": {FileID: "f1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The node f1 is tracked (synced, recorded hash) at old.txt; the on-disk file was edited.
	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), editedBytes, 0o644); err != nil {
		t.Fatalf("seed edited prior file: %v", err)
	}
	if err := st.PutBaseline(7, "old.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed prior baseline: %v", err)
	}
	// A later event downloads the same node at its (now appliable) server path new.txt.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "new.txt", FileID: "f1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The download landed at the new path and the node is tracked there.
	if got, err := os.ReadFile(filepath.Join(viewDir, "new.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("new.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if p, err := st.PathByID(7, "f1"); err != nil || p != "new.txt" {
		t.Errorf("PathByID(f1) = %q (err %v), want new.txt", p, err)
	}
	// The stale prior path is cleaned up...
	if _, err := os.Lstat(filepath.Join(viewDir, "old.txt")); !os.IsNotExist(err) {
		t.Errorf("old.txt still present: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "old.txt"); !isNotFound(err) {
		t.Errorf("old.txt baseline survived: err=%v", err)
	}
	// ...and the decisive assertion: the edit survives as the _Conflict copy. Before the fix
	// removeRelocatedPriorFile deleted it outright.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(editedBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved edit %q", got, err, editedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "old.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one old.txt -> %s", recs, copyRel)
	}
}

// TestDownloadRemovesInSyncRelocatedPriorFile pins the negative side of the prior-file guard:
// a prior copy whose content still matches its synced baseline's last-synced hash is the
// node's own stale bytes, so it is removed cleanly with no spurious _Conflict copy.
func TestDownloadRemovesInSyncRelocatedPriorFile(t *testing.T) {
	origBytes := []byte("last-synced-content")
	serverBytes := []byte("server-content-at-new-path")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"f1": serverBytes},
		downloadNode:    map[string]actions.Node{"f1": {FileID: "f1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed prior file: %v", err)
	}
	if err := st.PutBaseline(7, "old.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed prior baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "new.txt", FileID: "f1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "new.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("new.txt = %q (err %v), want server content %q", got, err, serverBytes)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "old.txt")); !os.IsNotExist(err) {
		t.Errorf("stale prior file old.txt still present: err=%v", err)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "new.txt" {
		t.Errorf("view dir entries = %v, want only new.txt (no spurious conflict copy)", entries)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0", recs)
	}
}

// TestInboundDownloadFreshCreateSyncedOccupantOverwrites pins the promote-policy split:
// a fresh-create download (no BaseLocalHash) whose target already held a
// file under a StateSynced baseline when the pre-transfer guard ran - the sequential
// same-path-downloads case untrackedFilePresent deliberately overwrites directly - must keep
// the REPLACING promote. A no-replace rename here would fail on the legitimately-present file
// and spuriously funnel a routine second download through keep-both. The prior download's
// markSynced always records a non-empty LocalHash (an empty one under a synced FILE baseline is
// the inbound-rename anomaly untrackedFilePresent now PRESERVES, so the realistic sequential-
// download seed carries the prior output's hash; the on-disk file still matches it, so the
// download overwrites directly).
func TestInboundDownloadFreshCreateSyncedOccupantOverwrites(t *testing.T) {
	prevBytes := []byte("first-download-output")
	serverBytes := []byte("second-download-content")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 556, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	// A prior download's output sits at the path under a synced baseline recording that output's
	// hash (markSynced always derives one); the follow-up op still carries no BaseLocalHash because
	// the pre-batch snapshot held no baseline.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), prevBytes, 0o644); err != nil {
		t.Fatalf("seed prior download output: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: md4.ContentHash(prevBytes),
		LocalSize: int64(len(prevBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed synced baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 556,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// Overwritten directly: no keep-both round-trip, no conflict copy or record.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("a.txt = %q (err %v), want %q", got, err, serverBytes)
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0 (no keep-both resolve)", len(c.queryNodeCalls))
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
}

// TestInboundRemoveDirReplacedByFilePreserves is a regression test: the user
// locally replaced a tracked directory with a regular file (rm -rf d && echo data > d), and a
// server delete of the directory drains before the next scan uploads the file. The pre-batch
// baseline is a synced DIR record, whose LocalHash is empty structurally - before the fix the
// capture read as BaseWasSynced=true with no hash, removeBaseFor's "synced without hash"
// branch claimed server authority, and fsutil.Remove destroyed the never-uploaded file
// irrecoverably. With Op.BaseIsDir captured, the guard preserves it whole as a _Conflict copy.
func TestInboundRemoveDirReplacedByFilePreserves(t *testing.T) {
	ts := time.Date(2026, 7, 15, 10, 20, 3, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-file-that-replaced-the-dir")
	copyRel := conflictCopyName("d", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: a regular file where the tracked directory used to be.
	if err := os.WriteFile(filepath.Join(viewDir, "d"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The store still tracks d as a synced DIRECTORY (dir baselines carry no content hash).
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d", IsDir: true}}
	ops, _, err := e.reconcile(7, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || !ops[0].BaseWasSynced ||
		ops[0].BaseLocalHash != "" || !ops[0].BaseIsDir {
		t.Fatalf("reconcile ops = %+v, want one OpRemove with BaseWasSynced=true, empty BaseLocalHash, BaseIsDir=true", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The canonical path honours the server delete, but the file survives as the copy.
	if _, err := os.Lstat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d still present on the canonical path: err=%v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one d -> %s", recs, copyRel)
	}
	if _, err := st.GetBaseline(7, "d"); !isNotFound(err) {
		t.Errorf("d baseline survived the remove: err=%v", err)
	}
}

// TestRemoveRelocatedDirBaseFileOccupantPreserved mirrors the dir-base gate of
// TestInboundRemoveDirReplacedByFilePreserves onto the
// relocated-node delete path (relocatedRemoveBase reads the LIVE baseline's FileType): the
// byid-resolved path is tracked as a synced DIRECTORY but a regular file occupies it on disk.
// Before the fix the dir record's structurally-empty LocalHash drove the server-authority
// branch and dropRemoveBaselineAt's fsutil.Remove destroyed the file; now it is preserved
// whole as a _Conflict copy.
func TestRemoveRelocatedDirBaseFileOccupantPreserved(t *testing.T) {
	ts := time.Date(2026, 7, 15, 10, 20, 3, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("file-that-replaced-the-relocated-dir")
	copyRel := conflictCopyName("kept", host, ts)

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// On disk a regular file sits where the store tracks a synced directory.
	if err := os.WriteFile(filepath.Join(viewDir, "kept"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "kept", store.Baseline{
		FileID: "f1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	pinClock(e, host, ts)

	// The server delete names the node at its (empty) server path; only byid reaches kept.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound,
		ViewID: 7, Path: "gone", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The delete took effect at the relocated path...
	if _, err := os.Lstat(filepath.Join(viewDir, "kept")); !os.IsNotExist(err) {
		t.Errorf("kept still present: err=%v", err)
	}
	if _, err := st.GetBaseline(7, "kept"); !isNotFound(err) {
		t.Errorf("kept baseline survived the delete: err=%v", err)
	}
	// ...and the decisive assertion: the file survives as the _Conflict copy. Before the fix
	// dropRemoveBaselineAt deleted it outright and no copy ever existed.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "kept" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one kept -> %s", recs, copyRel)
	}
}

// TestSubtreeDivergenceSyncedDirDescendantPreserved is a regression test: the
// TestInboundRemoveDirReplacedByFilePreserves shape (a regular file locally replacing a
// tracked DIRECTORY) one level deeper,
// where guardRemoveDir's subtree walk - not removeBaseFor - is the deciding gate. The view tracks
// d/ and d/sub/ as synced directories; the user ran `rm -rf d/sub; echo unsaved > d/sub`, and a
// server delete of the ancestor d drains before the next scan uploads the file. The walk reaches
// only regular files, so a synced record with FileType=="dir" at d/sub is exactly "a file replaced
// the tracked directory" - but its structurally-empty LocalHash used to read as "synced with no
// recorded local hash → nothing to call divergent", the walk reported clean, and fsutil.Remove
// destroyed the never-uploaded file with no _Conflict copy (dropDescendantBaselines then erased
// the evidence). The dir arc must now count as divergence and preserve the tree whole.
func TestSubtreeDivergenceSyncedDirDescendantPreserved(t *testing.T) {
	ts := time.Date(2026, 7, 24, 9, 8, 7, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("unsaved-work-that-replaced-the-dir")
	copyRel := conflictCopyName("d", host, ts)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// On disk: d/ still a directory, but d/sub is now a regular file the server never saw.
	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "sub"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The store still tracks both as synced DIRECTORIES (dir baselines carry no content hash).
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline d: %v", err)
	}
	if err := st.PutBaseline(7, "d/sub", store.Baseline{
		FileID: "fsub", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline d/sub: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d", IsDir: true}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The canonical path honours the server delete...
	if _, err := os.Lstat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d still present on the canonical path: err=%v", err)
	}
	// ...and the decisive assertion: the replacing file survives inside the preserved copy.
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel), "sub"))
	if err != nil || string(got) != string(userBytes) {
		t.Fatalf("preserved file = %q (err %v), want %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one d -> %s", recs, copyRel)
	}
	if _, err := st.GetBaseline(7, "d/sub"); !isNotFound(err) {
		t.Errorf("descendant baseline survived preservation: err=%v", err)
	}
}

// TestSubtreeDivergenceBareSyncedDescendantPreserved is the mirror for the
// TestRemoveRelocatedNodeSyncedEmptyHashPreserved shape: a descendant tracked by an
// anomalous synced FILE record with an EMPTY
// LocalHash (the inbound-rename bare/promoted-pending write) under a directory the server deletes.
// Five sibling gates already treat such a record as no trustworthy base and preserve whole; the
// subtree walk used to read it as "synced with no recorded local hash → in sync" and let
// fsutil.Remove destroy the file with no _Conflict copy.
func TestSubtreeDivergenceBareSyncedDescendantPreserved(t *testing.T) {
	ts := time.Date(2026, 7, 24, 9, 8, 7, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("content-behind-a-bare-synced-record")
	copyRel := conflictCopyName("d", host, ts)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "sub.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline d: %v", err)
	}
	// The bare-synced anomaly: a synced FILE record with no recorded local hash.
	if err := st.PutBaseline(7, "d/sub.txt", store.Baseline{
		FileID: "fsub", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline d/sub.txt: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fd", Path: "d", IsDir: true}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	if _, err := os.Lstat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d still present on the canonical path: err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel), "sub.txt"))
	if err != nil || string(got) != string(userBytes) {
		t.Fatalf("preserved file = %q (err %v), want %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "d" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one d -> %s", recs, copyRel)
	}
}

// TestInboundDownloadDirectoryOccupantPreservedNotDropped is the file/dir clash seen from the
// losing side, and the second half of TestIntegration_TwoClientsFileDirClash.
//
// Peer A makes a path a FILE; this client made it a DIRECTORY. The inbound download of A's file
// finds the path held by that directory, so the no-replace promote fails with fs.ErrExist.
// funnelFreshCreateIntruder used to keep that error for a non-regular occupant, which applyOp
// classifies as unappliable, so the download was DEAD-LETTERED: this client never received A's
// file, A never received this client's subtree, and the two stayed permanently divergent with
// nothing left in the queue to say so. The rationale - "file-over-dir can never apply" - holds
// only if one refuses to move the directory, which is exactly what the never-lose doctrine does
// instead.
//
// The directory must be preserved aside with its whole subtree, and the download must then land
// on the freed path. Both bodies survive, and the preserved copy is ordinary local content the
// next scan uploads under its new name.
func TestInboundDownloadDirectoryOccupantPreservedNotDropped(t *testing.T) {
	ts := time.Date(2026, 8, 25, 11, 56, 55, 0, time.UTC)
	const host = "laptop"

	innerBytes := []byte("this client put a directory here, holding this")
	serverBytes := []byte("the peer made the same path a file")
	serverHash := md4.ContentHash(serverBytes)
	copyRel := conflictCopyName("thing", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"thing": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The local shape: a DIRECTORY at the path the server holds as a file.
	if err := os.MkdirAll(filepath.Join(viewDir, "thing"), 0o755); err != nil {
		t.Fatalf("mkdir occupant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "thing", "inner.txt"), innerBytes, 0o644); err != nil {
		t.Fatalf("write inner: %v", err)
	}

	if err := st.PutBaseline(7, "thing", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "thing", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The first drain preserves the occupant and leaves the op queued; the next lands the file
	// on the freed path. What must NOT happen is a dead-letter.
	for i := 0; i < 3; i++ {
		_ = e.drainQueue(context.Background())
		if n, _ := st.PendingLen(); n == 0 {
			break
		}
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("the download was dead-lettered (%d); the clash must be resolved, not abandoned", dl)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The server's file now holds the canonical path...
	if got, err := os.ReadFile(filepath.Join(viewDir, "thing")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server bytes %q", got, err, serverBytes)
	}
	// ...and the decisive assertion: the local subtree survived, whole, under the keep-both name.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName, "inner.txt")); err != nil || string(got) != string(innerBytes) {
		t.Fatalf("preserved subtree = %q (err %v), want the local directory's content %q", got, err, innerBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "thing" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one thing -> %s", recs, copyRel)
	}
}

// TestInboundDownloadDirectoryAppearingMidTransferPreserved is #20: the same clash the fresh-create
// path resolves, arriving through the REPLACING promote instead.
//
// A modify download proves the target holds the last-agreed base, streams, and re-proves it just
// before the rename. If the file becomes a DIRECTORY inside that window - or replaces a file whose
// kind the recheck cannot see, since localDivergedFrom hashes and a directory answers
// errUploadSourceGone, i.e. "not diverged" - the replacing rename meets a directory it was never
// entitled to overwrite.
//
// That used to dead-letter on every platform, so the never-lose answer for a directory in the way
// applied or not depending on WHEN the directory appeared rather than on what stands there. It is
// also the one place the two OSes disagree on the errno: Linux reports fs.ErrExist from os.Rename's
// userspace dir-guard, Windows ERROR_ACCESS_DENIED. The classification therefore looks at the
// occupant instead of the error, and both promote policies reach the same resolution.
func TestInboundDownloadDirectoryAppearingMidTransferPreserved(t *testing.T) {
	ts := time.Date(2026, 8, 26, 9, 30, 0, 0, time.UTC)
	const host = "laptop"

	baseBytes := []byte("the agreed base this download was entitled to replace")
	serverBytes := []byte("the server's newer content")
	innerBytes := []byte("what the user put inside the directory that appeared")
	baseHash := md4.ContentHash(baseBytes)
	serverHash := md4.ContentHash(serverBytes)
	copyRel := conflictCopyName("doc.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"doc.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	abs := filepath.Join(viewDir, "doc.txt")
	if err := os.WriteFile(abs, baseBytes, 0o644); err != nil {
		t.Fatalf("seed the agreed base: %v", err)
	}
	// The swap lands while the body streams: the file becomes a directory holding user content.
	c.downloadHook = func() {
		if err := os.Remove(abs); err != nil {
			t.Errorf("remove base: %v", err)
			return
		}
		if err := os.MkdirAll(abs, 0o755); err != nil {
			t.Errorf("mkdir occupant: %v", err)
			return
		}
		if err := os.WriteFile(filepath.Join(abs, "inner.txt"), innerBytes, 0o644); err != nil {
			t.Errorf("write inner: %v", err)
		}
	}

	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash,
		LocalHash: baseHash, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// BaseLocalHash set: this is the MODIFY path, so the promote replaces rather than no-replaces.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "doc.txt", FileID: "srv1", UnixPerm: 644,
		BaseLocalHash: baseHash,
		LocalHash:     serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	for i := 0; i < 3; i++ {
		_ = e.drainQueue(context.Background())
		if n, _ := st.PendingLen(); n == 0 {
			break
		}
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("the download was dead-lettered (%d); a directory in the way is preserved, "+
			"whenever it arrived", dl)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The server's bytes hold the canonical path...
	if got, err := os.ReadFile(abs); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server bytes", got, err)
	}
	// ...and the directory that appeared survived whole, with its content.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName, "inner.txt")); err != nil || string(got) != string(innerBytes) {
		t.Fatalf("preserved subtree = %q (err %v), want the user content %q", got, err, innerBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "doc.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one doc.txt -> %s", recs, copyRel)
	}
}

// TestInboundDownloadTransientFailureIsNotReadAsTheDirClash guards the false positive the #20 fix
// would otherwise introduce.
//
// The directory-occupant classification looks at what stands at the TARGET, so on its own it
// cannot tell "the rename met a directory" from "something else failed while a directory happened
// to be there". A TRANSIENT transfer failure is the dangerous form of the latter: the parent is
// perfectly writable, so preserving would succeed - the user's directory would be renamed to a
// _Conflict copy on the strength of a network blip, and again on the next one.
//
// Nothing about a failed transfer says the directory is in the way. The classification is
// therefore gated on the failure having come from the rename itself, and the op must simply
// retry with the directory untouched.
func TestInboundDownloadTransientFailureIsNotReadAsTheDirClash(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	// A directory at the target, in a WRITABLE parent - so a spurious preserve would succeed.
	blocker := filepath.Join(viewDir, "doc.txt")
	if err := os.MkdirAll(filepath.Join(blocker, "child"), 0o755); err != nil {
		t.Fatalf("seed blocking dir: %v", err)
	}
	userBytes := []byte("user content the blip must not relocate")
	if err := os.WriteFile(filepath.Join(blocker, "child", "user.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user content: %v", err)
	}

	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("server file")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 11}},
		dlErr:   errors.New("connection reset by peer"),
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "f1", FileType: "file", LocalHash: "base", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	// BaseLocalHash set, so this takes the REPLACING promote - the arm #20 changed.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "doc.txt", FileID: "f1", UnixPerm: 644, BaseLocalHash: "base",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d succeeded, want the injected transfer failure", i+1)
		}
	}

	// The decisive assertion: the user's directory is exactly where they left it, whole.
	if fi, err := os.Stat(blocker); err != nil || !fi.IsDir() {
		t.Fatalf("the directory was relocated on a transfer blip: isdir=%v err=%v",
			fi != nil && fi.IsDir(), err)
	}
	if got, err := os.ReadFile(filepath.Join(blocker, "child", "user.txt")); err != nil || string(got) != string(userBytes) {
		t.Fatalf("user content = %q (err %v), want it untouched at the original path", got, err)
	}
	if recs, err := st.ListConflicts(7); err != nil || len(recs) != 0 {
		t.Fatalf("conflicts = %+v (err %v), want none: a failed transfer is not a clash", recs, err)
	}
	// And the op is still queued, retrying - a transient failure is not terminal.
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("pending = %d, want the op still queued to retry", n)
	}
}
