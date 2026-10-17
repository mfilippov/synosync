package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// Regression: dropStrandedBaseline and dropPhantomDescendantBaselines probed the
// disk with a bare Lstat over the LITERAL NFC path. On a byte-preserving filesystem a live node
// materialised under its decomposed (NFD) name is missed by that NFC Lstat, read as a phantom,
// and its baseline dropped - the next scan then re-uploads the orphaned file as a duplicate
// server node. This is exactly the shape resolveRejectedRename GUARANTEES exists:
// it re-tracks a live NFD-spelled source as StatePending under the NFC key. Both drop helpers
// must resolve the real on-disk spelling (onDiskAbsNoSymlink) before probing, mirroring
// dropRemoveBaselineAt.

// TestDropStrandedBaselinePreservesNFDLiveFile pins the fix for dropStrandedBaseline: a live
// file under its NFD name with a pending baseline keyed under the canonical NFC path must be
// PRESERVED (the node is materialised on disk), not dropped as a phantom. Before the fix the
// bare NFC Lstat missed the NFD file and dropped the record.
func TestDropStrandedBaselinePreservesNFDLiveFile(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	// The file exists on disk under its decomposed (NFD) name; the baseline is keyed under the
	// canonical NFC name (what the scanner/reconcile normalize to), as resolveRejectedRename leaves
	// it.
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), []byte("live"), 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "f1", FileType: "file", LocalHash: "h1", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if err := e.dropStrandedBaseline(7, "f1"); err != nil {
		t.Fatalf("dropStrandedBaseline: %v", err)
	}

	b, err := st.GetBaseline(7, nfcName)
	if err != nil {
		t.Fatalf("baseline of a live NFD file was dropped (GetBaseline: %v) - the node is materialised, not a phantom", err)
	}
	if b.FileID != "f1" || b.State != store.StatePending {
		t.Errorf("baseline = %+v, want the pending f1 record preserved", b)
	}
}

// TestDropStrandedBaselineDropsGenuinePhantom is the counterpart regression: with NO file on
// disk under either spelling, the stranded pending record IS a phantom and must still be dropped
// (the NFC-tolerant resolution finds nothing, falls back, and Lstat misses correctly).
func TestDropStrandedBaselineDropsGenuinePhantom(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")

	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileID: "f1", FileType: "file", LocalHash: "h1", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if err := e.dropStrandedBaseline(7, "f1"); err != nil {
		t.Fatalf("dropStrandedBaseline: %v", err)
	}

	if _, err := st.GetBaseline(7, nfcName); err == nil {
		t.Error("phantom baseline (no on-disk file) was preserved, want dropped")
	}
}

// TestDropPhantomDescendantBaselinesPreservesNFDLiveDescendant pins the same fix for the
// dead-letter mirror dropPhantomDescendantBaselines: a descendant materialised under its NFD
// name must be preserved, not dropped, when its directory OpRemove is dead-lettered.
func TestDropPhantomDescendantBaselinesPreservesNFDLiveDescendant(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// A live descendant file under its decomposed (NFD) leaf name.
	if err := os.WriteFile(filepath.Join(viewDir, "sub", nfdName), []byte("live"), 0o644); err != nil {
		t.Fatalf("seed NFD descendant: %v", err)
	}
	nfcPath := "sub/" + nfcName
	if err := st.PutBaseline(7, nfcPath, store.Baseline{
		FileID: "f1", FileType: "file", LocalHash: "h1", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed descendant baseline: %v", err)
	}

	if err := e.dropPhantomDescendantBaselines(7, "sub"); err != nil {
		t.Fatalf("dropPhantomDescendantBaselines: %v", err)
	}

	if _, err := st.GetBaseline(7, nfcPath); err != nil {
		t.Fatalf("baseline of a live NFD descendant was dropped (GetBaseline: %v) - it is materialised, not a phantom", err)
	}
}

// TestDropPhantomDescendantBaselinesDropsAbsentDescendant confirms a genuinely unmaterialised
// (no on-disk file) non-synced descendant is still dropped after the resolution change.
func TestDropPhantomDescendantBaselinesDropsAbsentDescendant(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	nfcPath := "sub/" + nfcName
	if err := st.PutBaseline(7, nfcPath, store.Baseline{
		FileID: "f1", FileType: "file", LocalHash: "h1", LocalSize: 4, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed descendant baseline: %v", err)
	}

	if err := e.dropPhantomDescendantBaselines(7, "sub"); err != nil {
		t.Fatalf("dropPhantomDescendantBaselines: %v", err)
	}

	if _, err := st.GetBaseline(7, nfcPath); err == nil {
		t.Error("phantom descendant (no on-disk file) was preserved, want dropped")
	}
}
