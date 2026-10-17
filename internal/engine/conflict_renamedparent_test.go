package engine

// These tests cover a keep-both OpResolveConflict whose parent baseline was
// RELOCATED by a server-side rename of an ancestor (relocatePending re-homes the subtree's
// baselines at reconcile time) - or whose parent was deleted - died in resolveParentID BEFORE
// any server turn. An earlier fix gave applyUploadOut/applyMkdirOut the errParentBaselineAbsent
// exits, but the keep-both resolver was never wired up: an id-less resolve fell into the
// transient branch forever while the authoritative inbound OpRename serialized BEHIND it on
// the ancestor conflict key skipped every drain - a permanent mutual wedge, visible only as
// Stuck. (An id-carrying resolve is already handled by refunnelIfMoved, which runs ahead of
// resolveParentID; the second test pins that it still converges.)

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

// TestResolveConflictUnderRenamedParentIdlessReemits reproduces the mutual wedge and pins the
// fix: an id-less fresh-create funnel enqueued OpResolveConflict{FileID:""} at a/f; before it
// drained, another client renamed a -> b server-side, so reconcile eagerly relocated the
// baselines (a -> b, a/f -> b/f) and queued the inbound OpRename(a->b) BEHIND the resolve. The
// resolve's resolveParentID("a/f") then found no parent baseline under any spelling
// (errParentBaselineAbsent). Before the fix that fell to the transient branch forever, blocking
// the rename. With the fix the resolve re-bases its path through the queued inbound rename,
// re-funnels a fresh resolve at b/f (dropping the stale op with no durable trace), the rename
// applies in the same drain, and the re-funnelled resolve runs keep-both at the new path.
func TestResolveConflictUnderRenamedParentIdlessReemits(t *testing.T) {
	ts := time.Date(2026, 7, 18, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	// After the rename, the diverged node lives at b/f server-side; the re-funnelled resolve
	// resolves keep-both there.
	copyRel := conflictCopyName("b/f", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"b/f": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srvf": serverBytes},
		downloadNode:    map[string]actions.Node{"srvf": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "f"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	// a is a synced directory; a/f is an id-less fresh-create pending its resolution.
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/f", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed a/f baseline: %v", err)
	}
	// The resolve op is enqueued FIRST, so it drains ahead of the inbound rename.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a/f", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}
	// The server rename a -> b reconciles into a serialized inbound OpRename(a->b), eagerly
	// relocating the stored baselines a -> b and a/f -> b/f (relocatePending).
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "b"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want convergence in one drain; mutual wedge before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (mutual wedge before the fix)", n)
	}
	// The re-funnel is a race resolution, not a drop: no durable dead-letter trace.
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("re-funnel wrote a durable dead-letter trace: %d, want 0", dl)
	}
	// The inbound rename applied and the re-funnelled resolve won the server version onto b/f.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b", "f")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical b/f = %q (err %v), want %q", got, err, serverBytes)
	}
	// The divergent local content survives as the _Conflict copy under the new parent.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyRel)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy %q = %q (err %v), want %q", copyRel, got, err, localBytes)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (the preserved copy pushed back)", len(c.uploads))
	}
	if u := c.uploads[0]; u.parentID != "fa" || u.name != copyName {
		t.Errorf("copy upload = parent %q name %q, want fa/%q", u.parentID, u.name, copyName)
	}
	// Both baselines are synced at the new path and the divergence is recorded.
	if cb, err := st.GetBaseline(7, "b/f"); err != nil || cb.State != store.StateSynced || cb.FileID != "srvf" {
		t.Errorf("canonical b/f baseline = %+v (err %v), want synced with the server id", cb, err)
	}
	if conflicts, _ := st.ListConflicts(7); len(conflicts) == 0 {
		t.Error("no conflict recorded for the resolved divergence")
	}
}

// TestResolveConflictUnderRenamedParentIdCarryingRefunnels is the id-carrying regression pin: a
// modify-flavoured resolve (FileID set) under the same server rename must still converge via the
// EXISTING refunnelIfMoved (which resolves the relocation through the byid index, ahead of
// resolveParentID) - the change to the id-less path must not regress it.
func TestResolveConflictUnderRenamedParentIdCarryingRefunnels(t *testing.T) {
	ts := time.Date(2026, 7, 18, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("b/f", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"b/f": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srvf": serverBytes},
		downloadNode:    map[string]actions.Node{"srvf": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "f"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// a/f is a modify conflict carrying the node's server id, so refunnelIfMoved can resolve
	// the relocation through the byid index.
	if err := st.PutBaseline(7, "a/f", store.Baseline{
		FileID: "srvf", FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed a/f baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a/f", FileID: "srvf", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", IsDir: true, FromPath: "a", Path: "b"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want convergence in one drain)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("re-funnel wrote a durable dead-letter trace: %d, want 0", dl)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "b", "f")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical b/f = %q (err %v), want %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyRel)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy %q = %q (err %v), want %q", copyRel, got, err, localBytes)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
}

// TestResolveConflictUnderDeletedParentIdlessDeadLetters covers the rebase-impossible fallback:
// an id-less resolve whose parent baseline is absent with NO queued inbound rename to re-base
// through means the parent was genuinely deleted. The parentGoneConfirmed ancestor walk proves
// it, so the op is dead-lettered terminally (errParentGone, marked StateConflict) rather than
// wedging the queue forever - so any authoritative inbound remove behind it can apply.
func TestResolveConflictUnderDeletedParentIdlessDeadLetters(t *testing.T) {
	c := &pushClient{
		// The ancestor walk lists the view root; a clean listing that omits "a", corroborated by
		// "a" itself answering the path-error sentinel, is positive proof the
		// parent chain is gone.
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"a": actions.ErrPathError},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "f"), []byte("divergent"), 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	// No baseline tracks the parent "a": resolveParentID returns errParentBaselineAbsent.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a/f", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the resolve dead-lettered, not wedged)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (resolve wedged before the fix)", n)
	}
	// The confirmed-gone parent dead-lettered the resolve with a durable trace.
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the gone-parent resolve", dl)
	}
	// The baseline is marked StateConflict so the blocked path is visible in the Unresolved count.
	if b, err := st.GetBaseline(7, "a/f"); err != nil || b.State != store.StateConflict {
		t.Errorf("a/f baseline = %+v (err %v), want StateConflict", b, err)
	}
}

// TestResolveConflictUnderDeletedParentIdCarryingDeadLetters is the id-carrying half of the
// gone-parent fallback. An id-carrying resolve reaches errParentBaselineAbsent
// only when refunnelIfMoved found no byid relocation - i.e. the node's baseline was DROPPED by
// an ancestor's applied delete (dropDescendantBaselines), not relocated - so PathByID reports
// not-found (moved=false) and the byid index gives no positive proof. Before the fix the
// errParentBaselineAbsent handling was gated on op.FileID == "", so an id-carrying resolve fell
// straight to the transient branch and wedged the queue forever (visible only as Stuck),
// blocking any authoritative inbound remove serialized behind it on the ancestor key. With the
// fix the parentGoneConfirmed ancestor walk proves the parent gone for BOTH forms and the op is
// dead-lettered terminally (errParentGone, StateConflict).
func TestResolveConflictUnderDeletedParentIdCarryingDeadLetters(t *testing.T) {
	c := &pushClient{
		// The ancestor walk lists the view root; a clean listing that omits "a", corroborated by
		// "a" itself answering the path-error sentinel, is positive proof the
		// parent chain is gone.
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"a": actions.ErrPathError},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "f"), []byte("divergent"), 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	// No baseline tracks the node id "ff" (byid not-found → refunnelIfMoved reports moved=false)
	// and no baseline tracks the parent "a" (resolveParentID returns errParentBaselineAbsent) -
	// the state left after an ancestor's applied delete dropped the subtree's baselines while
	// this id-carrying resolve, funnelled from a stale-base modify, was still queued.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a/f", FileID: "ff", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the resolve dead-lettered, not wedged)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (id-carrying resolve wedged before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the gone-parent id-carrying resolve", dl)
	}
	if b, err := st.GetBaseline(7, "a/f"); err != nil || b.State != store.StateConflict {
		t.Errorf("a/f baseline = %+v (err %v), want StateConflict", b, err)
	}
}
