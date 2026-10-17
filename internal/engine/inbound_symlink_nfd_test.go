package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// These tests close the symlink-containment gap the Phase-9b NFC→on-disk resolution
// opened in the inbound destructive apply paths: e.abs runs
// noSymlinkEscape over the LITERAL NFC path, but on a byte-preserving filesystem that path does
// not exist (the name is stored NFD), so the walk returns nil without inspecting anything.
// onDiskAbs then matches directory entries by their normalized name and can resolve through a
// symlinked component the literal-path check never saw, after which os.RemoveAll / os.Rename would
// follow it outside the sync root. onDiskAbsNoSymlink re-validates the resolved path so such an op
// is dead-lettered instead of applied through the symlink.
//
// nfcDir/nfdDir (the precomposed/decomposed forms of "über") are defined in outbound_nfd_test.go.

// skipIfNormalizingFS skips when the filesystem normalizes the NFD directory name to NFC (macOS):
// there the literal-NFC path resolves to the symlink and e.abs's noSymlinkEscape already rejects
// it before onDiskAbs runs, so the resolved-path re-check this test exercises is unreachable.
func skipIfNormalizingFS(t *testing.T, viewDir string) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(viewDir, nfcDir)); err == nil {
		t.Skip("filesystem normalizes names; the literal-NFC symlink check already covers this")
	}
}

// TestInboundRemoveRejectsSymlinkResolvedPath: the directory component is an NFD-named symlink
// pointing outside the sync root. A server delete for <nfcDir>/secret.txt (NFC) must NOT resolve
// through the symlink and os.RemoveAll a file outside the root - it must be dead-lettered.
func TestInboundRemoveRejectsSymlinkResolvedPath(t *testing.T) {
	requireSymlinks(t)
	if normpath.Normalize(nfdDir) != nfcDir {
		t.Fatalf("setup: NFD %q must normalize to NFC %q", nfdDir, nfcDir)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	outside := t.TempDir()
	victim := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(victim, []byte("must survive"), 0o644); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, nfdDir)); err != nil {
		t.Fatalf("create NFD symlink: %v", err)
	}
	skipIfNormalizingFS(t, viewDir)

	rel := nfcDir + "/secret.txt"
	if err := st.PutBaseline(7, rel, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: rel}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(victim); err != nil {
		t.Errorf("file outside the sync root was removed through a symlink: %v", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n == 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want the rejected path dead-lettered", n, err)
	}
}

// TestInboundRenameRejectsSymlinkResolvedSource: the source directory component is an NFD-named
// symlink pointing outside the sync root. A server move of <nfcDir>/secret.txt must NOT resolve
// through the symlink and os.Rename a file out of the tree - it must be dead-lettered.
func TestInboundRenameRejectsSymlinkResolvedSource(t *testing.T) {
	requireSymlinks(t)
	if normpath.Normalize(nfdDir) != nfcDir {
		t.Fatalf("setup: NFD %q must normalize to NFC %q", nfdDir, nfcDir)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	outside := t.TempDir()
	victim := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(victim, []byte("must survive"), 0o644); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, nfdDir)); err != nil {
		t.Fatalf("create NFD symlink: %v", err)
	}
	skipIfNormalizingFS(t, viewDir)

	rel := nfcDir + "/secret.txt"
	if err := st.PutBaseline(7, rel, store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h", LocalHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: rel, Path: "moved.txt"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The out-of-tree file must stay put and not be dragged into the sync root.
	if _, err := os.Stat(victim); err != nil {
		t.Errorf("file outside the sync root was moved through a symlink: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "moved.txt")); !os.IsNotExist(err) {
		t.Errorf("rename through a symlink materialised the destination (err %v); want no move", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n == 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want the rejected path dead-lettered", n, err)
	}
}
