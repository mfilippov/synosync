//go:build !windows

package engine

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestScanViewSkipsNonRegular confirms a non-regular, non-directory node (a named
// pipe) is silently skipped: it is not syncable content and must not block the
// hash/open path.
func TestScanViewSkipsNonRegular(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	if err := os.WriteFile(filepath.Join(viewDir, "real.txt"), []byte("real"), 0o644); err != nil {
		t.Fatalf("write real.txt: %v", err)
	}
	if err := syscall.Mkfifo(filepath.Join(viewDir, "pipe"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, "pipe"); ok {
		t.Error("named pipe was surfaced as a scan entry")
	}
	if _, ok := findEntry(entries, "real.txt"); !ok {
		t.Error("regular file alongside the pipe was not scanned")
	}
}
