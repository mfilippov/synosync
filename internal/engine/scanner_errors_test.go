package engine

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestScanViewRootEACCESSurfaces confirms an EACCES on the view root (not a
// not-exist) is a genuine view-wide failure that surfaces as an error rather
// than being mistaken for a clean empty scan.
func TestScanViewRootEACCESSurfaces(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix directory permission bits are not meaningful on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	e, viewID, viewDir := newScanEngine(t)
	if err := os.Chmod(viewDir, 0o000); err != nil {
		t.Fatalf("chmod view dir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(viewDir, 0o755) })

	_, _, err := e.scanView(context.Background(), viewID, nil)
	if err == nil {
		t.Fatal("scanView on an unreadable view root returned nil error; a view-wide EACCES must surface")
	}
}

// TestScanViewSkipsUnreadableFile confirms a single file the process cannot read
// (EACCES on open) is skipped and recorded in the unreadable set rather than
// aborting the whole view scan or being read as a deletion.
func TestScanViewSkipsUnreadableFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission checks")
	}
	e, viewID, viewDir := newScanEngine(t)
	if err := os.WriteFile(filepath.Join(viewDir, "open.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write open.txt: %v", err)
	}
	locked := filepath.Join(viewDir, "locked.txt")
	if err := os.WriteFile(locked, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write locked.txt: %v", err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatalf("chmod locked.txt: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o644) })

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView aborted on an unreadable file: %v", err)
	}
	if _, ok := findEntry(entries, "open.txt"); !ok {
		t.Error("readable file outside the locked entry was not scanned")
	}
	if _, ok := findEntry(entries, "locked.txt"); ok {
		t.Error("unreadable file was surfaced as a scan entry")
	}
	if !unreadable["locked.txt"] {
		t.Error("unreadable file not recorded; its baseline could be read as a deletion")
	}
}

// TestScanViewSkipsNonRegular lives in scanner_fifo_test.go: it needs
// syscall.Mkfifo, which does not exist on Windows, so the file carries a
// //go:build !windows tag (a runtime t.Skip cannot save a compile error).

// TestIsStagingName covers the precise staging-name match: a real os.CreateTemp
// temp is filtered, but a dotfile without the ".dl-<digits>" marker (or one whose
// marker is followed by a non-digit / nothing) is real user content.
func TestIsStagingName(t *testing.T) {
	tests := map[string]bool{
		".keep.txt.dl-123456": true,  // a genuine staging temp
		".x.dl-0":             true,  // single-digit suffix
		"keep.txt":            false, // not a dotfile
		".plainfile":          false, // dotfile, no ".dl-" marker
		".notes.dl-draft":     false, // marker but non-digit suffix
		".x.dl-":              false, // marker but empty suffix
		".a.dl-12a":           false, // marker with a trailing non-digit
	}
	for name, want := range tests {
		if got := isStagingName(name); got != want {
			t.Errorf("isStagingName(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestViewRootUnavailableEACCES confirms an EACCES on the view root (the parent
// denies traversal) surfaces as an error rather than being read as a removed
// root, so localReconcile does not erase the view's contents on a permission
// blip.
func TestViewRootUnavailableEACCES(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix directory permission bits are not meaningful on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	e, viewID, viewDir := newScanEngine(t)
	parent := filepath.Dir(viewDir)
	if err := os.Chmod(parent, 0o000); err != nil {
		t.Fatalf("chmod parent: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })

	if _, err := e.viewRootUnavailable(viewID); err == nil {
		t.Fatal("viewRootUnavailable returned nil error on an EACCES root stat")
	}
}

// TestResolveViewDirStoreError confirms a store failure (a closed database)
// while resolving the view directory surfaces as an error rather than panicking.
func TestResolveViewDirStoreError(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	if err := e.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, _, err := e.scanView(context.Background(), viewID, nil); err == nil {
		t.Fatal("scanView on a closed store returned nil error")
	}
}

// TestLoadScanHintsStoreError confirms a store failure while streaming the
// baselines for the cheap-scan hint map surfaces as an error rather than a
// silently-empty hint set (which would mislabel an unchanged file as new).
// loadScanHints is the hint-only helper the cheap-scan unit tests build a hint
// table with; the cheap reconcile path itself now streams the baselines once via
// collectBaselines, which surfaces the same store error (see
// TestCollectBaselinesStoreError).
func TestLoadScanHintsStoreError(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	if err := e.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, err := e.loadScanHints(viewID); err == nil {
		t.Fatal("loadScanHints on a closed store returned nil error")
	}
}
