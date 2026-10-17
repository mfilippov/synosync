package engine

// Phase 9b: the NFC ingestion-boundary tests. They assert every path the
// engine ingests - from the local scan, from server events, and from the cold-start
// catalog - is folded to Unicode NFC before it keys a baseline or an op, so the store
// holds a single canonical encoding regardless of how the source (macOS NFD vs NFC)
// encoded the name. The NFD literals are written with explicit escapes so the source
// stays in the intended Unicode form.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

const (
	nfcName = "café.txt"  // café.txt - precomposed é (U+00E9)
	nfdName = "café.txt" // café.txt - e + combining acute (U+0065 U+0301)
)

// TestScanViewNormalizesNFDToNFC writes a file whose on-disk name is in NFD form and
// asserts the scanner surfaces it under the NFC key (and not the raw NFD bytes).
func TestScanViewNormalizesNFDToNFC(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), []byte("content"), 0o644); err != nil {
		t.Fatalf("write NFD file: %v", err)
	}
	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, nfcName); !ok {
		t.Errorf("scan entry not NFC-normalized; entries = %+v", entries)
	}
	if _, ok := findEntry(entries, nfdName); ok {
		t.Errorf("scan surfaced the raw NFD key %q, want NFC %q", nfdName, nfcName)
	}
	if unreadable[nfdName] {
		t.Errorf("protected-path set keyed under raw NFD %q, want NFC", nfdName)
	}
}

// TestReconcileNormalizesServerPathToNFC feeds an NFD-form server create and asserts the
// enqueued op and the stored baseline both key under the NFC form, never the NFD bytes.
func TestReconcileNormalizesServerPathToNFC(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	events := []actions.Event{
		{SyncID: 5, Action: "create", FileID: "f1", Path: nfdName, Hash: "h1", Size: 3, MTime: 100},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != nfcName {
		t.Fatalf("op path = %q, want NFC %q", opPath(ops), nfcName)
	}
	if _, err := st.GetBaseline(1, nfcName); err != nil {
		t.Errorf("baseline not keyed under NFC %q: %v", nfcName, err)
	}
	if _, err := st.GetBaseline(1, nfdName); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline should not be keyed under raw NFD %q, got err %v", nfdName, err)
	}
}

// TestReconcileNFDMatchesExistingNFCBaseline confirms an NFD-form server event matches a
// baseline previously stored under the NFC form as the SAME node (not a spurious new
// path): a delete naming the NFD form flips the existing NFC baseline to pending and
// enqueues a remove, rather than no-op'ing against a missing NFD key.
func TestReconcileNFDMatchesExistingNFCBaseline(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, nfcName, store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: "h1", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed NFC baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 9, Action: "delete", FileID: "f1", Path: nfdName},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != nfcName {
		t.Fatalf("ops = %+v, want one NFC remove", ops)
	}
	b, err := st.GetBaseline(1, nfcName)
	if err != nil {
		t.Fatalf("GetBaseline NFC: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("NFC baseline state = %q, want pending (matched the NFD delete)", b.State)
	}
}

// TestColdStartViewNormalizesCatalogPathToNFC drives a full cold start where the server
// catalog reports a file under its NFD name while disk holds the same content under the
// NFC name. Once the catalog path is NFC-normalized the two sides match, so the cold
// start adopts the file as synced with no transfer rather than treating the NFD and NFC
// forms as two distinct nodes (a spurious download + upload).
func TestColdStartViewNormalizesCatalogPathToNFC(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	content := []byte("shared content")
	if err := os.WriteFile(filepath.Join(viewDir, nfcName), content, 0o644); err != nil {
		t.Fatalf("write NFC file: %v", err)
	}
	hash := md4.ContentHash(content)
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile(nfdName, "s1", hash, int64(len(content))),
	})}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("enqueued %d ops, want 0 (NFD catalog and NFC disk are the same node): %+v", len(ops), ops)
	}
	b, err := st.GetBaseline(7, nfcName)
	if err != nil {
		t.Fatalf("baseline not keyed under NFC %q: %v", nfcName, err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced (adopted)", b.State)
	}
	if _, err := st.GetBaseline(7, nfdName); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline should not be keyed under raw NFD %q, got err %v", nfdName, err)
	}
}

// opPath returns the path of the first op (or "" for none) for a clearer failure message.
func opPath(ops []store.Op) string {
	if len(ops) == 0 {
		return ""
	}
	return ops[0].Path
}
