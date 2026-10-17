package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// countScanBaselines wraps e.scanBaselines with a call counter and returns a pointer to the
// count, so a test can assert how many times one reconcile pass decodes the baseline set.
func countScanBaselines(e *Engine) *int {
	calls := 0
	inner := e.scanBaselines
	e.scanBaselines = func(v uint64, fn func(store.PathBaseline) error) ([]string, error) {
		calls++
		return inner(v, fn)
	}
	return &calls
}

// TestLocalReconcileCheapStreamsBaselinesOnce proves the cheap-scan reconcile path streams the
// view's baselines exactly once per pass. The pass formerly streamed them twice - loadScanHints
// to build the cheap-scan hint table, then mergeScanAndBaselines to build the merge-join - even
// though both ran in the same pass; collectBaselines folds them into a single stream. The
// unchanged on-disk file produces no op (the cheap (size, mtime) compare reuses the baseline
// hash), confirming the single stream did not regress the cheap short-circuit.
func TestLocalReconcileCheapStreamsBaselinesOnce(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_000
	content := []byte("hello world")
	hash := md4.ContentHash(content)
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), content, mtime)
	putFileBaseline(t, e.store, viewID, "a.txt", hash, int64(len(content)), mtime)

	calls := countScanBaselines(e)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no ops for an unchanged file, got %+v", ops)
	}
	if *calls != 1 {
		t.Fatalf("cheap reconcile streamed baselines %d times, want 1 (single-pass merge-join)", *calls)
	}
}

// TestLocalReconcileFullRehashStreamsBaselinesOnce confirms the full-rehash path (hints == nil)
// also streams the baselines once: it never built a separate hint table, so it was always a
// single stream, and collectBaselines must not regress that.
func TestLocalReconcileFullRehashStreamsBaselinesOnce(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_000
	content := []byte("hello world")
	hash := md4.ContentHash(content)
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), content, mtime)
	putFileBaseline(t, e.store, viewID, "a.txt", hash, int64(len(content)), mtime)

	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		t.Fatalf("queuedPaths: %v", err)
	}

	calls := countScanBaselines(e)

	ops, err := e.localReconcileWith(context.Background(), viewID, queued, dirCache, rehashSlice{of: 1})
	if err != nil {
		t.Fatalf("localReconcileWith full-rehash: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no ops for an unchanged file, got %+v", ops)
	}
	if *calls != 1 {
		t.Fatalf("full-rehash reconcile streamed baselines %d times, want 1", *calls)
	}
}

// TestLocalReconcileCheapEmitsModifyUpload proves the single-pass refactor is op-equivalent for
// a genuine change: a file whose on-disk content diverges from its synced baseline still emits
// exactly one OpUpload addressing the existing server node by its file id, and the pass still
// streams the baselines once.
func TestLocalReconcileCheapEmitsModifyUpload(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_000
	const newMtime = 1_700_000_999 // different mtime so the cheap compare opens and re-hashes
	original := []byte("original content")
	edited := []byte("edited content, longer")
	origHash := md4.ContentHash(original)

	b := store.Baseline{
		FileType:   "file",
		FileID:     "node-42",
		LocalHash:  origHash,
		LocalSize:  int64(len(original)),
		LocalMTime: mtime,
		State:      store.StateSynced,
	}
	if err := e.store.PutBaseline(viewID, "a.txt", b); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), edited, newMtime)

	calls := countScanBaselines(e)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "a.txt" || ops[0].FileID != "node-42" {
		t.Fatalf("got %+v, want a single OpUpload of a.txt addressing node-42", ops)
	}
	if ops[0].LocalHash != md4.ContentHash(edited) {
		t.Fatalf("OpUpload carried hash %q, want the edited content hash %q", ops[0].LocalHash, md4.ContentHash(edited))
	}
	if *calls != 1 {
		t.Fatalf("cheap reconcile streamed baselines %d times, want 1", *calls)
	}
}

// TestLocalReconcileCheapShortCircuitsOnSizeMtimeMatch proves the integrated single-pass
// reconcile actually takes the cheap short-circuit (reuse the baseline hash without opening the
// file), not merely that an unchanged file yields no op. The on-disk content is replaced with
// DIFFERENT bytes of the SAME length under the SAME mtime as the baseline: if the pass re-hashed
// the content it would see divergence and emit an OpUpload, so the absence of any op proves the
// content was never read - the cheap (size, mtime) compare reused the stale baseline hash. The
// rare swap-at-same-(size, mtime) case is the periodic full re-hash's job, not the cheap pass.
func TestLocalReconcileCheapShortCircuitsOnSizeMtimeMatch(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_000
	original := []byte("hello world") // 11 bytes
	swapped := []byte("HELLO WORLD")  // same 11 bytes, different content
	hash := md4.ContentHash(original)
	if len(original) != len(swapped) {
		t.Fatalf("test setup: original and swapped must be the same length")
	}
	putFileBaseline(t, e.store, viewID, "a.txt", hash, int64(len(original)), mtime)
	// Write the divergent content under the SAME size and mtime the baseline recorded.
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), swapped, mtime)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("cheap short-circuit re-hashed the file (got ops %+v); expected the stale baseline hash to be reused", ops)
	}
}

// TestCollectBaselinesStoreError confirms a store failure while streaming the baselines for the
// merge-join (and the cheap-scan hints folded into the same pass) surfaces as an error rather
// than a silently-empty result, which would mislabel every tracked file as new.
func TestCollectBaselinesStoreError(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	if err := e.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, _, _, _, err := e.collectBaselines(viewID, map[string]scanHint{}); err == nil {
		t.Fatal("collectBaselines on a closed store returned nil error")
	}
}

// TestScanStreamsBaselinesOncePerPassForSymlinks pins the cost of the case-variant fallback that
// protects a tracked path a non-syncable node has replaced.
//
// The fallback only runs when the exact-key lookup misses, which for a stray symlink is the
// NORMAL case - and it used to answer by streaming and folding the view's whole baseline set,
// stopping at a first match that never came. So the cost was O(symlinks x baselines) PER PASS: a
// pnpm-style node_modules is built almost entirely of symlinks, and against a large view that
// turned the cheap steady-state scan into a sustained CPU burn every local_scan_interval.
//
// One lazily-built index per pass answers all of them. The protection itself is unchanged, which
// is what the sibling tests (the case-variant and corrupt-record fold cases) cover; this one
// covers only how many times the baselines are decoded.
func TestScanStreamsBaselinesOncePerPassForSymlinks(t *testing.T) {
	requireSymlinks(t)
	e, viewID, viewDir := newScanEngine(t)

	// A handful of untracked symlinks: every one of them misses the exact lookup and reaches the
	// fold-key fallback.
	for _, name := range []string{"l1", "l2", "l3", "l4", "l5"} {
		if err := os.Symlink("target", filepath.Join(viewDir, name)); err != nil {
			t.Fatalf("symlink %s: %v", name, err)
		}
	}
	putFileBaseline(t, e.store, viewID, "kept.txt", "h", 1, 1)

	calls := countScanBaselines(e)
	if _, _, err := e.scanView(context.Background(), viewID, nil); err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if *calls > 1 {
		t.Errorf("scanView streamed the baselines %d times for 5 untracked symlinks, want at most 1: "+
			"the fold-key fallback must consult one per-pass index, not re-decode the whole view "+
			"per entry", *calls)
	}
}

// countBatches wraps e.batchStore with a call counter and returns a pointer to the count, so a
// test can assert how many durable transactions one pass actually commits.
func countBatches(e *Engine) *int {
	calls := 0
	inner := e.batchStore
	e.batchStore = func(fn func(*store.Batch) error) error {
		calls++
		return inner(fn)
	}
	return &calls
}

// TestLocalReconcilePlansInBatchedTransactions pins the write granularity of the outbound
// planning loop.
//
// Enqueue plus the baseline flip is two committed transactions per planned op, and bolt fsyncs
// and rewrites a b-tree path plus the freelist each time: cold start measured that exact shape at
// 40 ops/s and 47.6 GB of physical I/O for a 241 MB database, which is why it batches. This loop
// produces the same shape and was left unbatched on the reading that the incremental path plans "a
// handful of ops" - but nothing bounds an incremental diff to a handful. Unpacking an archive into
// the sync root plans as many ops as a cold start does.
//
// The ordering rule is unaffected and is what the sibling tests cover: each op's Enqueue still
// precedes its own baseline mutation, now inside the transaction rather than across two.
func TestLocalReconcilePlansInBatchedTransactions(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// Comfortably more than one chunk, so the test would catch a batch size silently becoming 1.
	const files = localReconcileEnqueueBatch + 20
	for i := range files {
		name := fmt.Sprintf("f%04d.txt", i)
		if err := os.WriteFile(filepath.Join(viewDir, name), []byte(name), 0o644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	batches := countBatches(e)
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != files {
		t.Fatalf("planned %d ops, want %d", len(ops), files)
	}
	want := (files + localReconcileEnqueueBatch - 1) / localReconcileEnqueueBatch
	if *batches != want {
		t.Errorf("committed %d transactions for %d ops, want %d (one per chunk of %d): at two "+
			"transactions per op this loop measured 40 ops/s and tens of GB of write amplification",
			*batches, files, want, localReconcileEnqueueBatch)
	}
	// The records are really there - batching must not lose the work it groups.
	for i := range files {
		name := fmt.Sprintf("f%04d.txt", i)
		if b, gerr := e.store.GetBaseline(viewID, name); gerr != nil || b.State != store.StatePending {
			t.Fatalf("baseline %s = %+v (err %v), want a pending record", name, b, gerr)
		}
	}
	if n, _ := e.store.PendingLen(); n != files {
		t.Errorf("queue holds %d ops, want %d", n, files)
	}
}
