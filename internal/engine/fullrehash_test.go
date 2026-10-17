package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestShouldFullRehash exercises the per-pass decision directly: with an interval > 0 every
// interval-th pass (counting from 0) forces a full re-hash, and an interval of 0 disables it
// entirely regardless of the pass counter.
func TestRehashSlice(t *testing.T) {
	e := New(newTestStore(t), &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	e.limits.FullRehashInterval = 0
	if e.rehashSlice().covers("a.txt") {
		t.Error("a disabled re-hash covered a file")
	}
	e.limits.FullRehashInterval = 1
	if !e.rehashSlice().covers("a.txt") {
		t.Error("an every-pass re-hash missed a file")
	}
	// Over one cycle of N passes every path falls in exactly one pass's share.
	e.limits.FullRehashInterval = 7
	for _, key := range []string{"a.txt", "docs/b.pdf", "x/y/z.bin", "photo-0001.jpg"} {
		hits := 0
		for pass := uint64(0); pass < 7; pass++ {
			e.scanPass = pass
			if e.rehashSlice().covers(key) {
				hits++
			}
		}
		if hits != 1 {
			t.Errorf("%s re-hashed on %d of 7 passes, want exactly 1", key, hits)
		}
	}
}

// seedSwappedSameSizeMtime records a synced file baseline at relPath and places different
// bytes of the SAME length and SAME mtime on disk - a same-second / same-size modification the
// cheap (size, mtime) short-circuit cannot see but a full re-hash detects. It returns the
// on-disk content's true hash so a test can assert the enqueued op carries it.
func seedSwappedSameSizeMtime(t *testing.T, st *store.Store, viewID uint64, viewDir, relPath string) string {
	t.Helper()
	const mtime = 1_700_000_000
	original := []byte("original")
	swapped := []byte("modIfied") // identical length, different content
	if len(original) != len(swapped) {
		t.Fatalf("test bug: contents must be equal length")
	}
	b := store.Baseline{
		FileType:   "file",
		FileID:     "f-" + relPath,
		LocalHash:  md4.ContentHash(original),
		LocalSize:  int64(len(original)),
		LocalMTime: mtime,
		State:      store.StateSynced,
	}
	if err := st.PutBaseline(viewID, relPath, b); err != nil {
		t.Fatalf("PutBaseline %q: %v", relPath, err)
	}
	writeFileWithMtime(t, filepath.Join(viewDir, relPath), swapped, mtime)
	return md4.ContentHash(swapped)
}

// TestLocalScanPeriodicFullRehash proves the periodic full re-hash safety net: a same-second /
// same-size in-place modification is invisible to a cheap (non-full-rehash) scan pass - no op is
// enqueued - but the periodic full-rehash pass re-hashes the file and detects the divergence,
// enqueueing the OpUpload the cheap path missed.
func TestLocalScanPeriodicFullRehash(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	e.scanInterval = 0 // never throttle: the test drives passes via scanPass directly
	e.limits.FullRehashInterval = 3

	swappedHash := seedSwappedSameSizeMtime(t, st, 7, viewDir, "a.txt")

	// The pass whose share is a.txt's re-hashes it; the others trust the matching (size, mtime).
	var own uint64
	for pass := uint64(0); pass < 3; pass++ {
		if (rehashSlice{of: 3, idx: pass}).covers(normpath.Key("a.txt")) {
			own = pass
		}
	}
	// Another pass short-circuits, so the in-place same-second modify is not detected.
	e.scanPass = own + 1
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("cheap localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("cheap pass enqueued %d ops, want 0 (size+mtime match must short-circuit)", n)
	}

	// Its own pass re-hashes it and detects the divergence.
	e.scanPass = own + 3
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("full-rehash localScan: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("full-rehash pass enqueued %d ops, want 1 (re-hash must catch same-second modify)", len(ops))
	}
	if ops[0].Type != store.OpUpload || ops[0].LocalHash != swappedHash {
		t.Fatalf("op = %+v, want OpUpload with hash %q (the re-hashed on-disk content)", ops[0], swappedHash)
	}
}

// TestLocalScanFullRehashDisabled confirms FullRehashInterval == 0 disables the periodic
// re-hash: even on pass 0 the cheap short-circuit holds, so a same-second / same-size modify is
// not detected.
func TestLocalScanFullRehashDisabled(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	e.scanInterval = 0
	e.limits.FullRehashInterval = 0 // disabled

	seedSwappedSameSizeMtime(t, st, 7, viewDir, "a.txt")

	e.scanPass = 0 // a multiple of every positive interval, but disabled => cheap
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("disabled periodic re-hash enqueued %d ops, want 0", n)
	}
}

// TestLocalScanFullRehashConsistentAcrossViews proves the re-hash share is decided ONCE per
// pass and shared by every view: the pass that re-hashes a path does so in both views, and each
// detects its same-second / same-size modification, so two ops are enqueued in a single scan.
func TestLocalScanFullRehashConsistentAcrossViews(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Alpha", NodeID: "a1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 7: %v", err)
	}
	if err := st.PutView(8, store.View{Name: "Beta", NodeID: "b1", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 8: %v", err)
	}
	root := t.TempDir()
	alpha := filepath.Join(root, "Alpha")
	beta := filepath.Join(root, "Beta")
	for _, d := range []string{alpha, beta} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed view dir %s: %v", d, err)
		}
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	e.scanInterval = 0
	e.limits.FullRehashInterval = 2

	seedSwappedSameSizeMtime(t, st, 7, alpha, "a.txt")
	seedSwappedSameSizeMtime(t, st, 8, beta, "a.txt")

	// The same path in both views: the pass that re-hashes it does so in every view.
	e.scanPass = 0
	if !e.rehashSlice().covers(normpath.Key("a.txt")) {
		e.scanPass = 1
	}
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Fatalf("full-rehash pass across two views enqueued %d ops, want 2 (decision must be pass-wide)", n)
	}
}
