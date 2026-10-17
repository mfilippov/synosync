package engine

// Tests for case-only renames: a case-only server rename of a diverged node used to wedge
// its keep-both resolver forever. refunnelIfMoved deliberately folds both paths so a case-only
// difference on a case-insensitive filesystem is not read as a relocation - but the resolver then
// kept addressing the server with op.Path, the enqueue-time spelling, while the byid index already
// held the authoritative new one. A DSM that resolves its path parameter case-sensitively answers
// ErrPathError for that stale spelling forever, resolveServerNodeAbsence classifies the node
// "moved" (the id answers under another leaf name), and that arc's exit leaves the op queued: a
// permanent transient that also starves every fold-equal op serialised behind it. The resolver now
// ADOPTS the tracked spelling, mirroring moveSourceByFoldedKey.

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

// caseInsensitiveClient makes the engine fold paths for the duration of the test (the state the
// case-fold probe would leave on macOS/Windows), restoring the process-wide flag on exit.
func caseInsensitiveClient(t *testing.T) {
	t.Helper()
	orig := normpath.CaseInsensitive()
	t.Cleanup(func() { normpath.UseCaseInsensitive(orig) })
	normpath.UseCaseInsensitive(true)
}

// seedCaseRenamedResolve wires the shared fixture: the node srvf is tracked (baseline + byid) at
// the server's NEW spelling "Notes.txt" - a case-only inbound rename already applied - while the
// keep-both op still carries the enqueue-time "notes.txt". The divergent local content sits at
// the tracked spelling, where the inbound rename left it.
func seedCaseRenamedResolve(t *testing.T, c *pushClient, localBytes []byte) (*Engine, *store.Store, string) {
	t.Helper()
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "Notes.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "Notes.txt", store.Baseline{
		FileID: "srvf", FileType: "file", LocalHash: md4.ContentHash(localBytes),
		LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7,
		Path: "notes.txt", FileID: "srvf", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}
	return e, st, viewDir
}

// TestResolveConflictAdoptsTrackedSpellingOnCaseOnlyRename is the red test: the DSM resolves its
// path parameter case-sensitively, so the stale "notes.txt" answers ErrPathError on every retry
// while the grandparent listing reports the id alive under "Notes.txt". Before the fix nothing
// about that ever changed - the resolve was transient forever (or, where the stale spelling also
// missed on disk, dropped without any keep-both at all). It must now query, download and preserve
// at the tracked spelling and converge.
func TestResolveConflictAdoptsTrackedSpellingOnCaseOnlyRename(t *testing.T) {
	caseInsensitiveClient(t)

	ts := time.Date(2026, 7, 29, 11, 20, 0, 0, time.UTC)
	const host = "laptop"
	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("Notes.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// A case-sensitive path lookup: only the server's own spelling answers.
		queryNode: map[string]actions.Node{
			"Notes.txt": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		queryNodeErrByPath: map[string]error{"notes.txt": actions.ErrPathError},
		// The node is alive under its new spelling, so the absence walk reads "moved" - the
		// classification whose exit left the op queued forever.
		listResult:      map[string][]actions.Node{"": {{Name: "Notes.txt", FileID: "srvf"}}},
		downloadContent: map[string][]byte{"srvf": serverBytes},
		downloadNode:    map[string]actions.Node{"srvf": {FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := seedCaseRenamedResolve(t, c, localBytes)
	pinClock(e, host, ts)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the resolve to converge at the tracked spelling)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (the resolve wedged forever before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter traces = %d, want 0 (the resolve is resolvable, not terminal)", dl)
	}
	// The server version won the canonical path and the divergent local content survives as the
	// keep-both copy - the whole point of the resolve that used to never run.
	if got, err := os.ReadFile(filepath.Join(viewDir, "Notes.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical Notes.txt = %q (err %v), want %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyRel)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy %q = %q (err %v), want %q", copyRel, got, err, localBytes)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (the preserved copy pushed back)", len(c.uploads))
	}
	if b, err := st.GetBaseline(7, "Notes.txt"); err != nil || b.State != store.StateSynced || b.ServerHash != serverHash {
		t.Errorf("canonical baseline = %+v (err %v), want synced with the server hash", b, err)
	}
	if conflicts, _ := st.ListConflicts(7); len(conflicts) != 1 {
		t.Errorf("conflict records = %d, want 1", len(conflicts))
	}
	// The stale spelling is never used for a server turn once the tracked one is known.
	for _, p := range c.queryNodeCalls {
		if p == "notes.txt" {
			t.Errorf("QueryNode called with the stale spelling %q; calls = %v", p, c.queryNodeCalls)
		}
	}
}

// TestResolveConflictCaseInsensitiveServerLookupUnchanged pins the other DSM semantics: a server
// whose path lookup folds case answers the stale spelling too, so the resolution itself already
// converged before the fix - the server version wins the canonical path, the divergent content is
// preserved and pushed back, exactly as it must keep doing. What the adoption additionally closes
// here is a latent byid drift: the resolved baseline used to land under the STALE key, a second
// record carrying the same file id as the tracked one.
func TestResolveConflictCaseInsensitiveServerLookupUnchanged(t *testing.T) {
	caseInsensitiveClient(t)

	ts := time.Date(2026, 7, 29, 11, 25, 0, 0, time.UTC)
	const host = "laptop"
	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("Notes.txt", host, ts)
	copyName := filepath.Base(copyRel)
	node := actions.Node{FileID: "srvf", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}

	c := &pushClient{
		// Either spelling resolves to the same node.
		queryNode:       map[string]actions.Node{"Notes.txt": node, "notes.txt": node},
		downloadContent: map[string][]byte{"srvf": serverBytes},
		downloadNode:    map[string]actions.Node{"srvf": node},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := seedCaseRenamedResolve(t, c, localBytes)
	pinClock(e, host, ts)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter traces = %d, want 0", dl)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "Notes.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical Notes.txt = %q (err %v), want %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyRel)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy %q = %q (err %v), want %q", copyRel, got, err, localBytes)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
	if b, err := st.GetBaseline(7, "Notes.txt"); err != nil || b.State != store.StateSynced {
		t.Errorf("canonical baseline = %+v (err %v), want synced", b, err)
	}
}

// TestResolveConflictGenuineLeafMoveStillRefunnels pins the boundary: a NEW leaf name (not a
// case-only difference) is still a relocation, so refunnelIfMoved re-funnels a fresh resolve at
// the node's new path and reports moved - the spelling adoption must not swallow it.
func TestResolveConflictGenuineLeafMoveStillRefunnels(t *testing.T) {
	caseInsensitiveClient(t)

	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := st.PutBaseline(7, "renamed.txt", store.Baseline{
		FileID: "srvf", FileType: "file", State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	op := store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7,
		Path: "notes.txt", FileID: "srvf", UnixPerm: 644,
	}

	// The adoption helper reports no spelling change for a fold-DIFFERENT path...
	if got, err := e.trackedSpelling(op); err != nil || got != op.Path {
		t.Fatalf("trackedSpelling = %q (err %v), want %q unchanged", got, err, op.Path)
	}
	// ...and the relocation check still owns it.
	newPath, moved, err := e.refunnelIfMoved(op)
	if err != nil || !moved || newPath != "renamed.txt" {
		t.Fatalf("refunnelIfMoved = (%q, %v, %v), want (renamed.txt, true, nil)", newPath, moved, err)
	}
	ops, err := st.PendingOpsN(10)
	if err != nil {
		t.Fatalf("PendingOpsN: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "renamed.txt" || ops[0].Type != store.OpResolveConflict {
		t.Fatalf("queued ops = %+v, want one resolve re-funnelled at renamed.txt", ops)
	}
}

// TestTrackedSpellingArcs pins the helper's arcs directly: no file id, an id the byid index does
// not track, an already-current spelling, a fold-equal but byte-different one (adopted), and a
// fold-different one (left to refunnelIfMoved). The last two flip with the case-fold flag, so
// each is exercised under the semantics that make it meaningful.
func TestTrackedSpellingArcs(t *testing.T) {
	caseInsensitiveClient(t)

	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := st.PutBaseline(7, "dir/Notes.txt", store.Baseline{
		FileID: "srvf", FileType: "file", State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	tests := []struct {
		name   string
		op     store.Op
		want   string
		insens bool
	}{
		{name: "no file id keeps op.Path", op: store.Op{ViewID: 7, Path: "dir/notes.txt"}, want: "dir/notes.txt", insens: true},
		{name: "untracked id keeps op.Path", op: store.Op{ViewID: 7, Path: "dir/notes.txt", FileID: "ghost"}, want: "dir/notes.txt", insens: true},
		{name: "current spelling unchanged", op: store.Op{ViewID: 7, Path: "dir/Notes.txt", FileID: "srvf"}, want: "dir/Notes.txt", insens: true},
		{name: "case-only rename adopted", op: store.Op{ViewID: 7, Path: "dir/notes.txt", FileID: "srvf"}, want: "dir/Notes.txt", insens: true},
		{name: "different leaf left to the relocation check", op: store.Op{ViewID: 7, Path: "dir/other.txt", FileID: "srvf"}, want: "dir/other.txt", insens: true},
		// On a case-SENSITIVE client the two spellings are different paths, so the case-only row
		// above is a genuine relocation there - no adoption.
		{name: "case-sensitive client adopts nothing", op: store.Op{ViewID: 7, Path: "dir/notes.txt", FileID: "srvf"}, want: "dir/notes.txt", insens: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normpath.UseCaseInsensitive(tt.insens)
			defer normpath.UseCaseInsensitive(true)
			got, err := e.trackedSpelling(tt.op)
			if err != nil || got != tt.want {
				t.Errorf("trackedSpelling(%+v) = %q (err %v), want %q", tt.op, got, err, tt.want)
			}
		})
	}
}

// TestTrackedSpellingStoreFailureSurfaces pins the fail-toward-queued direction: a byid lookup
// that fails for a reason other than not-found is no witness, so the op stays queued rather than
// resolving against a possibly stale spelling.
func TestTrackedSpellingStoreFailureSurfaces(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := e.trackedSpelling(store.Op{ViewID: 7, Path: "notes.txt", FileID: "srvf"})
	if err == nil {
		t.Fatal("trackedSpelling on a closed store = nil error, want the failure surfaced")
	}
	if errors.Is(err, errDeadLettered) {
		t.Errorf("store failure classified terminal: %v", err)
	}
}

// TestEscalatedResolveMarksTheTrackedSpelling pins the interaction between the tracked-spelling
// adoption and the deterministic-rejection escalation: the escalation marks the
// baseline and writes the dead-letter trace from the op it is HANDED, so the adoption has to
// happen in applyOp rather than inside the resolver's own copy. With it confined to the resolver,
// markOutboundConflict ran against the stale "notes.txt", found no baseline there, synthesised one
// from the op - and the byid index, which already held srvf at "Notes.txt", DELETED the real
// record to keep the id unique. The node's hashes, sync id and surfaced state were lost and a
// StateConflict stub was left at a spelling the server no longer uses.
func TestEscalatedResolveMarksTheTrackedSpelling(t *testing.T) {
	caseInsensitiveClient(t)

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)
	c := &pushClient{
		// The server refuses the tracked spelling the same deterministic way every drain.
		queryNodeErrByPath: map[string]error{"Notes.txt": &actions.ServerError{Code: 33333}},
	}
	e, st, _ := seedCaseRenamedResolve(t, c, localBytes)
	advance := stubClock(e)

	for i := 0; i < unmappedServerErrDrains; i++ {
		_ = e.drainQueue(context.Background())
		advance(unmappedServerErrMinAge)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending = %d, want the resolve escalated and dropped", n)
	}
	// The stale spelling must never have been written: a phantom there collides in byid with the
	// real record and takes it out.
	if _, err := st.GetBaseline(7, "notes.txt"); !isNotFound(err) {
		t.Errorf("GetBaseline(notes.txt) = %v, want not-found: the escalation must not write a phantom at the stale spelling", err)
	}
	b, err := st.GetBaseline(7, "Notes.txt")
	if err != nil {
		t.Fatalf("GetBaseline(Notes.txt): %v", err)
	}
	if b.State != store.StateConflict || b.FileID != "srvf" || b.LocalHash != localHash {
		t.Errorf("tracked baseline = %+v, want the real record marked StateConflict with its facts intact (local hash %q)", b, localHash)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || dls[0].Path != "Notes.txt" {
		t.Fatalf("dead letters = %+v, want one trace naming the tracked spelling Notes.txt", dls)
	}
}
