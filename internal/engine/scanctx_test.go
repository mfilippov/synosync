package engine

// Tests for context cancellation inside the two long walk+hash units
// (scanView and the pre-delete subtree sweep). scanView took no ctx at all and Run only checked
// between views, so a shutdown or a pause landed behind O(bytes in the view) of hashing - hours on
// a cold-start-sized tree, long enough for systemd's TimeoutStopSec (SIGKILL) or a Windows SCM
// STOP_PENDING hang. subtreeHasDivergence is the same class one verb deeper: it runs on a drain
// worker inside an ordinary inbound remove.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedWideTree writes n regular files (plus a couple of subdirectories) under dir so a walk has
// more entries than scanCtxCheckEvery and must therefore reach a context check.
func seedWideTree(t *testing.T, dir string, n int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < n; i++ {
		name := filepath.Join(dir, fmt.Sprintf("f%04d.bin", i))
		if i%2 == 0 {
			name = filepath.Join(dir, "sub", fmt.Sprintf("f%04d.bin", i))
		}
		if err := os.WriteFile(name, []byte(fmt.Sprintf("content-%d", i)), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

// TestScanViewHonoursContextCancellation is the red test for scan cancellation: a cancelled context
// stops the walk within a bounded number of entries, and the partial scan produces NO diff - the
// caller sees an error and returns before diffing, so a half-finished walk can never read as a
// whole-view deletion.
func TestScanViewHonoursContextCancellation(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	seedWideTree(t, viewDir, 4*scanCtxCheckEvery)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entries, _, err := e.scanView(ctx, 7, nil)
	if err == nil {
		t.Fatalf("scanView on a cancelled context = nil error after %d entries; the walk ran to completion", len(entries))
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("scanView error = %v, want context.Canceled", err)
	}
	if len(entries) > scanCtxCheckEvery {
		t.Errorf("scan surfaced %d entries before aborting, want at most one check batch (%d)", len(entries), scanCtxCheckEvery)
	}

	// The reconcile pass above the scan must not turn the aborted walk into a diff.
	ops, rerr := e.localReconcile(ctx, 7)
	if rerr == nil {
		t.Errorf("localReconcile on a cancelled context = nil error, want the scan failure to propagate")
	}
	if len(ops) != 0 {
		t.Errorf("aborted scan produced %d ops, want 0 (a partial scan is not evidence)", len(ops))
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("aborted scan enqueued %d ops, want 0", n)
	}
}

// cancelAfterHashes installs the pre-hash seam so the context is cancelled once n files have been
// hashed, modelling a shutdown or pause landing in the middle of a check batch - the interleaving
// the batched entry counter alone cannot react to.
func cancelAfterHashes(t *testing.T, n int, cancel context.CancelFunc) *int {
	t.Helper()
	hashed := 0
	beforeHashHook = func() {
		hashed++
		if hashed == n {
			cancel()
		}
	}
	t.Cleanup(func() { beforeHashHook = nil })
	return &hashed
}

// TestScanViewAbortsWithinOneHashOfCancellation pins what actually bounds the abort on a
// full-rehash pass: the walk hashes whole files inside its own callback, so the batched entry
// check can be scanCtxCheckEvery FULL FILE HASHES away from the next one - minutes on a media
// tree, the very window the check exists to close. The unconditional check immediately before
// each read must stop the walk after at most one more hash.
func TestScanViewAbortsWithinOneHashOfCancellation(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	// One batch minus a margin, so the cancellation lands strictly INSIDE a batch and the next
	// batched check is never reached.
	seedWideTree(t, filepath.Join(root, "Photos"), scanCtxCheckEvery-8)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hashed := cancelAfterHashes(t, 3, cancel)

	if _, _, err := e.scanView(ctx, 7, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("scanView = %v, want context.Canceled - the walk ran on past the cancellation", err)
	}
	if *hashed != 3 {
		t.Errorf("hashed %d files after the cancellation landed at 3; want no further reads", *hashed)
	}
}

// TestSubtreeDivergenceAbortsWithinOneHashOfCancellation is the same guarantee for the pre-delete
// sweep, which hashes every descendant on a drain worker.
func TestSubtreeDivergenceAbortsWithinOneHashOfCancellation(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dirRel := "tree"
	dirAbs := filepath.Join(root, "Photos", dirRel)
	seedWideTree(t, dirAbs, scanCtxCheckEvery-8)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	// Every descendant must reach the hash, so give each a synced baseline the sweep can compare
	// against (any earlier arm would short-circuit as divergence before the read).
	seedSyncedSubtree(t, st, 7, dirRel, dirAbs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hashed := cancelAfterHashes(t, 3, cancel)

	diverged, err := e.subtreeHasDivergence(ctx, 7, dirRel, dirAbs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("subtreeHasDivergence = (%v, %v), want context.Canceled", diverged, err)
	}
	if diverged {
		t.Errorf("diverged = true on a cancelled sweep; the verdict must be an error, not a claim")
	}
	if *hashed != 3 {
		t.Errorf("hashed %d files after the cancellation landed at 3; want no further reads", *hashed)
	}
}

// seedSyncedSubtree gives every regular file under dirAbs a StateSynced baseline carrying its real
// content hash, so the divergence sweep reaches the hashing step for each of them.
func seedSyncedSubtree(t *testing.T, st *store.Store, viewID uint64, dirRel, dirAbs string) {
	t.Helper()
	err := filepath.WalkDir(dirAbs, func(p string, d os.DirEntry, werr error) error {
		if werr != nil || d.IsDir() {
			return werr
		}
		rel, rerr := filepath.Rel(dirAbs, p)
		if rerr != nil {
			return rerr
		}
		content, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		key := path.Join(dirRel, filepath.ToSlash(rel))
		// A unique file id per path: the byid index is 1:1, so reusing one id would make each
		// PutBaseline evict the previous record and leave a single baseline behind.
		return st.PutBaseline(viewID, key, store.Baseline{
			FileID: "id-" + key, FileType: "file", State: store.StateSynced, LocalHash: md4.ContentHash(content),
		})
	})
	if err != nil {
		t.Fatalf("seed synced subtree: %v", err)
	}
}

// TestLocalScanAbortsPromptlyOnPause pins the pause half of the same problem: a pause request
// must take effect within a bounded slice of the walk, not after it. The abort is not a failure
// (the scan is read-only and simply did not happen), so localScan returns nil and the scan
// re-runs after the resume rather than being throttled away.
func TestLocalScanAbortsPromptlyOnPause(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true, Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	seedWideTree(t, filepath.Join(root, "Photos"), 4*scanCtxCheckEvery)
	cap := &logCapture{}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", slog.New(cap))

	// The pause lands before the scan starts walking, so this exercises pauseAwareCtx's
	// already-paused fast path. TestLocalScanAbortsOnAPauseLandingMidWalk covers the other arm.
	e.Pause()
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan aborted by a pause = %v, want nil (not a failure, not terminal)", err)
	}
	if n := cap.countRecords("interrupted by a pause request"); n == 0 {
		t.Errorf("no Info naming the pause-interrupted scan; records=%d", len(cap.records))
	}
	if !e.lastScan.IsZero() {
		t.Errorf("an aborted scan stamped the throttle; it must re-run after the resume")
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("a pause-aborted scan enqueued %d ops, want 0", n)
	}

	// After the resume the scan runs to completion and the tree is pushed.
	e.Resume()
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan after resume: %v", err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n == 0 {
		t.Errorf("the resumed scan produced no ops; the abort lost the pass")
	}
	if e.lastScan.IsZero() {
		t.Errorf("a completed scan did not stamp the throttle")
	}
}

// TestLocalScanAbortsOnAPauseLandingMidWalk exercises pauseAwareCtx's watcher arm - the
// `case <-e.pauseReq` receive - which is the whole reason the channel, the per-pass goroutine
// and the stale-token drain exist. The already-paused fast path (the sibling test above)
// short-circuits before any of it, so without this the mid-walk case ships unverified.
func TestLocalScanAbortsOnAPauseLandingMidWalk(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true, Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	seedWideTree(t, filepath.Join(root, "Photos"), 4*scanCtxCheckEvery)
	cap := &logCapture{}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", slog.New(cap))

	// Pause once the walk is demonstrably under way, from inside the walk itself, so the
	// request reaches a scan that has already started rather than the pre-scan flag check.
	paused := false
	beforeHashHook = func() {
		if !paused {
			paused = true
			e.Pause()
		}
	}
	t.Cleanup(func() { beforeHashHook = nil })

	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan aborted by a mid-walk pause = %v, want nil (not a failure, not terminal)", err)
	}
	if !paused {
		t.Fatal("the walk never reached a hash, so no pause was injected mid-walk")
	}
	if n := cap.countRecords("interrupted by a pause request"); n == 0 {
		t.Errorf("no Info naming the pause-interrupted scan; records=%d", len(cap.records))
	}
	if !e.lastScan.IsZero() {
		t.Errorf("an aborted scan stamped the throttle; it must re-run after the resume")
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("a pause-aborted scan enqueued %d ops, want 0", n)
	}
}

// TestSubtreeDivergenceHonoursContextCancellation is the red test for the subtree-sweep mirror: the
// pre-delete subtree sweep is the same unbounded walk+hash, and a cancellation must leave the op
// queued with nothing deleted - never report "no divergence" for a subtree it did not finish
// verifying.
func TestSubtreeDivergenceHonoursContextCancellation(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dirRel := "tree"
	dirAbs := filepath.Join(root, "Photos", dirRel)
	seedWideTree(t, dirAbs, 4*scanCtxCheckEvery)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	diverged, err := e.subtreeHasDivergence(ctx, 7, dirRel, dirAbs)
	if err == nil {
		t.Fatalf("subtreeHasDivergence on a cancelled context = (%v, nil); the sweep ran to completion", diverged)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if diverged {
		t.Errorf("diverged = true on a cancelled sweep; the verdict must be an error, not a claim")
	}

	// Through the real verb: the guard's error leaves the remove queued and the tree intact.
	if err := st.PutBaseline(7, dirRel, store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	op := store.Op{Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: dirRel, IsDir: true, FileID: "fd"}
	// Typed, not merely non-nil: any unrelated failure (a baseline mismatch, a containment
	// rejection) would also leave the tree intact and let a dropped guard pass unnoticed.
	if err := e.applyRemove(ctx, op); !errors.Is(err, context.Canceled) {
		t.Errorf("applyRemove on a cancelled context = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(dirAbs); err != nil {
		t.Errorf("the subtree was deleted despite the cancelled verification: %v", err)
	}
}
