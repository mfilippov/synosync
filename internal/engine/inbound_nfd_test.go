package engine

import (
	"context"
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

// These tests close the inbound half of the Phase-9b NFD/NFC asymmetry: on a
// byte-preserving filesystem (Linux, Windows) a node tracked under the canonical NFC path can
// physically live under its decomposed (NFD) name. The outbound and keep-both paths already
// resolve the real on-disk spelling (resolveNFCPath / onDiskAbs); the inbound OpRemove/OpRename
// apply paths must mirror it, or a server delete/move addresses a phantom NFC path that does not
// exist on disk, the on-disk operation no-ops, the baseline is dropped, and the file lingers
// untracked - re-uploaded on the next scan (a delete/re-create ping-pong). nfcName/nfdName are the
// precomposed/decomposed forms of "café.txt" (defined in normpath_boundary_test.go).

// TestInboundRemoveResolvesNFDPath: a tracked node lives on disk under its NFD name while the op
// carries the NFC path. The inbound remove must delete the real on-disk file, not no-op on the
// absent NFC path.
func TestInboundRemoveResolvesNFDPath(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	content := []byte("server-deletes-me")
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), content, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	// Tracked synced under the canonical NFC key, hash matching the on-disk bytes so the
	// divergence guard honours the server's authoritative delete.
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: nfcName}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The real on-disk (NFD) file must be gone - not lingering because the delete missed it.
	if _, err := os.Stat(filepath.Join(viewDir, nfdName)); !os.IsNotExist(err) {
		t.Errorf("NFD file still present after inbound remove (err %v) - delete missed the real on-disk name", err)
	}
	if _, err := st.GetBaseline(7, nfcName); !isNotFound(err) {
		t.Errorf("baseline still present after remove: err %v", err)
	}
}

// TestInboundRenameResolvesNFDSource: a tracked node lives on disk under its NFD name while a
// server move addresses it by the NFC source path. The inbound rename must move the real on-disk
// file to the (new) destination, not no-op on the absent NFC source.
func TestInboundRenameResolvesNFDSource(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	content := []byte("server-moves-me")
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), content, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: nfcName, Path: "moved.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The destination must hold the moved bytes...
	if got, err := os.ReadFile(filepath.Join(viewDir, "moved.txt")); err != nil || string(got) != string(content) {
		t.Fatalf("moved.txt = %q (err %v), want the moved content %q - rename missed the real NFD source", got, err, content)
	}
	// ...and the old NFD source must be gone.
	if _, err := os.Stat(filepath.Join(viewDir, nfdName)); !os.IsNotExist(err) {
		t.Errorf("NFD source still present after rename (err %v)", err)
	}
	if _, err := st.GetBaseline(7, "moved.txt"); err != nil {
		t.Errorf("destination baseline missing after rename: %v", err)
	}
	if _, err := st.GetBaseline(7, nfcName); !isNotFound(err) {
		t.Errorf("source baseline still present after rename: err %v", err)
	}
}

// The tests below close the remaining inbound NFD/NFC promote gap: the
// download/mkdir apply paths used to materialise onto the LITERAL NFC path, creating a second
// directory entry (or a parallel NFC twin directory) next to the NFD-spelled original on a
// byte-preserving filesystem - the next scan folds both spellings into one normpath.Key,
// protects both as a collision, and the path stops syncing until manual repair. The
// relocated-delete helpers (dropRemoveBaselineAt, removeRelocatedPriorFile) had the same hole
// through a bare e.abs: the delete no-oped on the absent NFC path and the NFD file survived
// untracked, re-uploaded by the next scan (a delete/re-create ping-pong).

const (
	nfcDirName = "álbum"  // álbum - precomposed á (U+00E1)
	nfdDirName = "álbum" // álbum - a + combining acute (U+0301)
)

// countFoldedEntries reports how many directory entries of dir normalize to the canonical
// NFC name - the twin-entry detector: exactly one means the NFD original was reused, two
// means a parallel NFC entry was materialised next to it.
func countFoldedEntries(t *testing.T, dir, canonical string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	n := 0
	for _, ent := range entries {
		if normpath.Normalize(ent.Name()) == canonical {
			n++
		}
	}
	return n
}

// assertNoConflictCopies fails if any entry under dir carries the keep-both _Conflict marker:
// these scenarios are clean server-authoritative applies, so a conflict copy would be spurious.
func assertNoConflictCopies(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	for _, ent := range entries {
		if strings.Contains(ent.Name(), "Conflict") {
			t.Errorf("spurious conflict copy %q", ent.Name())
		}
	}
}

// TestInboundDownloadPromotesOntoNFDPath: a tracked node lives on disk under its NFD name and
// the server modifies it. The download promote must update the real NFD entry in place - not
// rename the staged temp onto the literal NFC path, which on a byte-preserving FS creates a
// SECOND entry with the new content while the NFD original keeps the stale bytes (the fold
// collision then protects both and the path stops syncing).
func TestInboundDownloadPromotesOntoNFDPath(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	oldContent := []byte("old-synced-content")
	newContent := []byte("new-server-content")
	c := &pushClient{
		downloadContent: map[string][]byte{"fa": newContent},
		downloadNode: map[string]actions.Node{"fa": {
			FileID: "fa", Hash: md4.ContentHash(newContent), Size: int64(len(newContent)), MTime: 200,
		}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.WriteFile(filepath.Join(viewDir, nfdName), oldContent, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	// Tracked synced under the canonical NFC key with the on-disk content's hash, so the
	// divergence gate reads "unchanged since last sync" and the overwrite proceeds.
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h-old", LocalHash: md4.ContentHash(oldContent),
		LocalSize: int64(len(oldContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{
		SyncID: 6, Action: "modify", FileID: "fa", Path: nfcName,
		Hash: md4.ContentHash(newContent), Size: int64(len(newContent)), MTime: 200,
	}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The real NFD entry must hold the new server content...
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdName)); err != nil || string(got) != string(newContent) {
		t.Errorf("NFD entry = %q (err %v), want the downloaded content %q - promote missed the real on-disk name", got, err, newContent)
	}
	// ...and exactly one directory entry may fold to the canonical name (no NFC twin).
	if n := countFoldedEntries(t, viewDir, nfcName); n != 1 {
		t.Errorf("%d directory entries fold to %q, want 1 - the promote materialised an NFC twin", n, nfcName)
	}
	b, err := st.GetBaseline(7, nfcName)
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != md4.ContentHash(newContent) {
		t.Errorf("baseline = state %q hash %q, want synced with the new content hash", b.State, b.LocalHash)
	}
	assertNoConflictCopies(t, viewDir)
}

// TestInboundCreatesLandInsideNFDDirectory: a tracked directory lives on disk under its NFD
// name and the server creates a child file and a child subdirectory under it (canonical NFC
// paths). Both must materialise INSIDE the NFD directory - not under a parallel NFC twin
// directory that a literal-path MkdirAll would create.
func TestInboundCreatesLandInsideNFDDirectory(t *testing.T) {
	if nfcDirName == nfdDirName || normpath.Normalize(nfdDirName) != nfcDirName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcDirName, nfdDirName)
	}
	childContent := []byte("child-file-content")
	c := &pushClient{
		downloadContent: map[string][]byte{"f2": childContent},
		downloadNode: map[string]actions.Node{"f2": {
			FileID: "f2", Hash: md4.ContentHash(childContent), Size: int64(len(childContent)), MTime: 300,
		}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.Mkdir(filepath.Join(viewDir, nfdDirName), 0o755); err != nil {
		t.Fatalf("seed NFD dir: %v", err)
	}
	if err := st.PutBaseline(7, nfcDirName, store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}

	events := []actions.Event{
		{SyncID: 6, Action: "create", FileID: "d2", Path: nfcDirName + "/sub", IsDir: true, MTime: 300},
		{SyncID: 7, Action: "create", FileID: "f2", Path: nfcDirName + "/child.txt",
			Hash: md4.ContentHash(childContent), Size: int64(len(childContent)), MTime: 300},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Both children must be inside the real NFD directory...
	if fi, err := os.Stat(filepath.Join(viewDir, nfdDirName, "sub")); err != nil || !fi.IsDir() {
		t.Errorf("NFD-dir/sub missing or not a dir (err %v) - mkdir missed the real on-disk parent", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdDirName, "child.txt")); err != nil || string(got) != string(childContent) {
		t.Errorf("NFD-dir/child.txt = %q (err %v), want %q - download missed the real on-disk parent", got, err, childContent)
	}
	// ...and no parallel NFC twin directory may have been materialised.
	if n := countFoldedEntries(t, viewDir, nfcDirName); n != 1 {
		t.Errorf("%d directory entries fold to %q, want 1 - an NFC twin directory was created", n, nfcDirName)
	}
	for _, p := range []string{nfcDirName + "/sub", nfcDirName + "/child.txt"} {
		b, err := st.GetBaseline(7, p)
		if err != nil {
			t.Errorf("baseline %q missing: %v", p, err)
			continue
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline %q state = %q, want synced", p, b.State)
		}
	}
}

// TestInboundRemoveRelocatedNodeResolvesNFDPath: the server deletes a node whose byid-indexed
// baseline sits at a path other than the delete's server path (a rejected rename re-tracked
// it), and the file physically lives under an NFD spelling. dropRemoveBaselineAt must delete
// the real on-disk file - a bare NFC abs no-ops, drops the baseline anyway, and the orphaned
// NFD file is re-uploaded by the next scan (a delete/re-create ping-pong).
func TestInboundRemoveRelocatedNodeResolvesNFDPath(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	content := []byte("relocated-server-deletes-me")
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), content, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	// The node is tracked (synced, hash matching disk so the delete is honoured) at the NFC
	// key, while the server's delete names a different path - the relocated-node shape.
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "gone.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, nfdName)); !os.IsNotExist(err) {
		t.Errorf("NFD file still present after relocated remove (err %v) - delete missed the real on-disk name", err)
	}
	if _, err := st.GetBaseline(7, nfcName); !isNotFound(err) {
		t.Errorf("relocated baseline still present after remove: err %v", err)
	}
	assertNoConflictCopies(t, viewDir)
}

// TestRemoveRelocatedPriorFileResolvesNFDPath: once a download lands a node at its new server
// path, removeRelocatedPriorFile deletes the stale copy the node left at its prior tracked
// path. When that prior copy lives under an NFD spelling the helper must resolve the real
// on-disk name - a bare NFC abs no-ops and the stale file survives untracked (ping-pong).
func TestRemoveRelocatedPriorFileResolvesNFDPath(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	content := []byte("stale-prior-copy")
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), content, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	op := store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "new-home.txt", FileID: "fa"}
	if err := e.removeRelocatedPriorFile(context.Background(), op); err != nil {
		t.Fatalf("removeRelocatedPriorFile: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, nfdName)); !os.IsNotExist(err) {
		t.Errorf("NFD prior copy still present (err %v) - remove missed the real on-disk name", err)
	}
	assertNoConflictCopies(t, viewDir)
}

// The tests below close the remaining NFD/NFC mirror gaps the promote fix above left open:
// the inbound rename DESTINATION, the keep-both resolver's canonical/copy targets, and the
// relocate/rejected-rename existence probes still resolved (or Lstat'd) the LITERAL NFC path.
// On a byte-preserving filesystem that either materialises a parallel NFC twin directory next
// to the NFD original (the next scan folds both into one normpath.Key and protects the subtree,
// stopping its sync) or misses a live NFD file and drops its baseline as a phantom.

// TestInboundRenameToNewLeafUnderNFDDirectory: a tracked file lives inside an NFD-spelled
// directory and the server renames it to a NEW leaf under the same directory. Because the
// destination leaf does not exist yet, the all-or-nothing onDiskAbs resolution left the WHOLE
// destination path - including the NFD parent - literal NFC, so fsutil.Rename created a parallel
// NFC twin directory and dropped the file inside it. The prefix resolution must reuse the NFD
// parent so the move lands inside the real directory.
func TestInboundRenameToNewLeafUnderNFDDirectory(t *testing.T) {
	if nfcDirName == nfdDirName || normpath.Normalize(nfdDirName) != nfcDirName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcDirName, nfdDirName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	if err := os.Mkdir(filepath.Join(viewDir, nfdDirName), 0o755); err != nil {
		t.Fatalf("seed NFD dir: %v", err)
	}
	content := []byte("server-moves-me-within-nfd-dir")
	if err := os.WriteFile(filepath.Join(viewDir, nfdDirName, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed NFD-dir/a.txt: %v", err)
	}
	if err := st.PutBaseline(7, nfcDirName, store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, nfcDirName+"/a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed file baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: nfcDirName + "/a.txt", Path: nfcDirName + "/b.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The moved bytes must be inside the real NFD directory at the new leaf...
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdDirName, "b.txt")); err != nil || string(got) != string(content) {
		t.Fatalf("NFD-dir/b.txt = %q (err %v), want the moved content %q - rename created an NFC twin directory", got, err, content)
	}
	// ...the old leaf is gone...
	if _, err := os.Stat(filepath.Join(viewDir, nfdDirName, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("NFD-dir/a.txt still present after move (err %v)", err)
	}
	// ...and exactly one directory entry may fold to the canonical dir name (no NFC twin).
	if n := countFoldedEntries(t, viewDir, nfcDirName); n != 1 {
		t.Errorf("%d directory entries fold to %q, want 1 - the rename materialised an NFC twin directory", n, nfcDirName)
	}
	if _, err := st.GetBaseline(7, nfcDirName+"/b.txt"); err != nil {
		t.Errorf("destination baseline missing after rename: %v", err)
	}
	assertNoConflictCopies(t, filepath.Join(viewDir, nfdDirName))
}

// TestResolveConflictKeepBothUnderNFDDirectory: a locally-diverged file lives inside an
// NFD-spelled directory. The keep-both resolver must land BOTH the server download (canonical
// path) and the preserved _Conflict copy INSIDE the real NFD directory - not under a parallel
// NFC twin directory a literal-NFC target/copy path would create.
func TestResolveConflictKeepBothUnderNFDDirectory(t *testing.T) {
	if nfcDirName == nfdDirName || normpath.Normalize(nfdDirName) != nfcDirName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcDirName, nfdDirName)
	}
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	canonical := nfcDirName + "/a.txt"
	copyRel := conflictCopyName(canonical, host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			canonical: {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, nfdDirName), 0o755); err != nil {
		t.Fatalf("seed NFD dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, nfdDirName, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, nfcDirName, store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, canonical, store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: canonical, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path (inside the NFD dir) now holds the server bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdDirName, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q - the download missed the real NFD parent", got, err, serverBytes)
	}
	// The _Conflict copy (inside the NFD dir) holds the old local bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdDirName, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q - the copy missed the real NFD parent", got, err, localBytes)
	}
	// Exactly one directory entry may fold to the canonical dir name (no NFC twin directory).
	if n := countFoldedEntries(t, viewDir, nfcDirName); n != 1 {
		t.Errorf("%d directory entries fold to %q, want 1 - a parallel NFC twin directory was created", n, nfcDirName)
	}
	// The copy was uploaded under the directory's server id.
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
	if u := c.uploads[0]; u.parentID != "d1" || u.name != copyName || string(u.content) != string(localBytes) {
		t.Errorf("upload call = %+v, want parent d1 name %q content %q", u, copyName, localBytes)
	}
}

// TestResolveRejectedRenameResolvesNFDCandidate: a live file physically lives under its NFD
// spelling while tracked (pending) at the canonical NFC key. When a rejected rename runs its
// baseline reconcile, the candidate existence probe must resolve the real NFD name - a bare NFC
// Lstat misses the file, finds no reachable candidate, and drops the live file's baseline as a
// phantom, which the next scan re-uploads as a duplicate.
func TestResolveRejectedRenameResolvesNFDCandidate(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	content := []byte("live-file-under-nfd-name")
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), content, 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	// Tracked PENDING (not synced) at the canonical NFC key: only a non-synced record is a
	// candidate for the rejected-rename re-track/drop decision.
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	op := store.Op{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: nfcName, NewPath: "dest.txt", FileID: "fa"}
	if err := e.resolveRejectedRename(op); err != nil {
		t.Fatalf("resolveRejectedRename: %v", err)
	}

	// The live file's baseline must survive - it was NOT dropped as a phantom.
	b, err := st.GetBaseline(7, nfcName)
	if err != nil {
		t.Fatalf("baseline dropped after rejected-rename reconcile: %v - the candidate probe missed the NFD file", err)
	}
	if b.FileID != "fa" || b.State != store.StatePending {
		t.Errorf("baseline = %+v, want fa still tracked pending", b)
	}
	// And the on-disk file is untouched.
	if got, err := os.ReadFile(filepath.Join(viewDir, nfdName)); err != nil || string(got) != string(content) {
		t.Errorf("NFD file = %q (err %v), want the untouched content %q", got, err, content)
	}
}
