package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// writeFile writes content under the view dir, creating parents.
func writeFile(t *testing.T, viewDir, rel string, content []byte) {
	t.Helper()
	p := filepath.Join(viewDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// putSynced writes a StateSynced baseline for path with the given file id and local facts.
func putSynced(t *testing.T, e *Engine, viewID uint64, path, fileID, ftype, hash string, size int64) {
	t.Helper()
	b := store.Baseline{
		FileID:     fileID,
		FileType:   ftype,
		ServerHash: hash,
		LocalHash:  hash,
		LocalSize:  size,
		State:      store.StateSynced,
	}
	if err := e.store.PutBaseline(viewID, path, b); err != nil {
		t.Fatalf("PutBaseline %s: %v", path, err)
	}
}

// TestLocalReconcileTreatsConflictNamedFileAsOrdinary confirms a file whose name matches the
// keep-both conflict-copy shape ("<name>_<host>_<timestamp>_Conflict.<ext>") is treated as
// ordinary user content, NOT specially filtered: the scanner surfaces it as a normal entry, so
// a synced baseline whose on-disk hash still matches produces no op. (Conflict-copy filtering
// was removed - a copy the Phase-6d resolver pushes is an ordinary server file, so a name match
// could only misfire on a real user file; provenance comes from the DESIGN §13 `conflicts` list.)
// The previous version of this test asserted the scanner *filtered* the copy, which it no longer
// does - it passed vacuously because the baseline already matched the file.
func TestLocalReconcileTreatsConflictNamedFileAsOrdinary(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	keep := []byte("keep")
	conflictName := "report_myhost_Jun-11-143052-2026_Conflict.txt"
	original := []byte("original content")
	writeFile(t, viewDir, "keep.txt", keep)
	writeFile(t, viewDir, conflictName, original)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	putSynced(t, e, viewID, conflictName, "fc", "file", md4.ContentHash(original), int64(len(original)))

	// Synced and unchanged on disk: no op, baseline untouched.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (unchanged conflict-named file): %+v", len(ops), ops)
	}
	if b, err := e.store.GetBaseline(viewID, conflictName); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("conflict-named baseline state = %q, want synced (untouched)", b.State)
	}

	// Editing it on disk must be detected as a modify and pushed - proving the scanner sees it
	// (not filtered away). Without surfacing the entry this change would be invisible to sync.
	edited := []byte("edited content the user changed")
	writeFile(t, viewDir, conflictName, edited)
	ops, err = e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile after edit: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != conflictName {
		t.Fatalf("got %+v, want a single OpUpload for the edited conflict-named file", ops)
	}
}

// TestLocalReconcileUploadsConflictLikeUserFile confirms a user file that merely ends in
// "_Conflict" (without the engine's host/timestamp shape) is treated as ordinary content: a
// brand-new one is uploaded rather than silently excluded from sync.
func TestLocalReconcileUploadsConflictLikeUserFile(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	writeFile(t, viewDir, "report_Conflict.txt", []byte("real user content"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "report_Conflict.txt" {
		t.Fatalf("got %+v, want a single OpUpload for report_Conflict.txt", ops)
	}
}

// TestLocalReconcilePermOnlyChangeNotSynced pins the DESIGN §13 decision that a permission-only
// change (chmod with no content edit) is intentionally NOT synced. Reconcile triggers a modify
// solely on a content-hash divergence (pb.LocalHash != en.md4); UnixPerm is recorded in the
// baseline and carried on the wire but never used as a modify trigger. The test seeds a synced
// baseline whose stored UnixPerm differs from the file's on-disk perm while content/size match,
// then drives BOTH a cheap pass and a forced full-rehash pass and asserts neither enqueues an op.
// (chmod touches ctime, not mtime, so the cheap (size, mtime) short-circuit cannot see it either -
// even the full re-hash, which re-reads the bytes, finds the content identical and emits nothing.)
func TestLocalReconcilePermOnlyChangeNotSynced(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	e, viewID, viewDir := newScanEngine(t)
	content := []byte("content the user never edited")
	rel := "doc.txt"
	p := filepath.Join(viewDir, rel)
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
	// Synced baseline matching the content but recording a DIFFERENT perm (0o644 -> wire 644)
	// than the 0o600 (wire 600) now on disk: a pure permission divergence.
	b := store.Baseline{
		FileID:     "f1",
		FileType:   "file",
		ServerHash: md4.ContentHash(content),
		LocalHash:  md4.ContentHash(content),
		LocalSize:  int64(len(content)),
		UnixPerm:   644,
		State:      store.StateSynced,
	}
	if err := e.store.PutBaseline(viewID, rel, b); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}

	// Cheap pass: content hash matches, perm differs - no op.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("cheap localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("cheap pass: got %d ops, want 0 (perm-only change is not synced): %+v", len(ops), ops)
	}

	// Forced full-rehash pass: the bytes are re-read and still match, so the perm divergence
	// still produces nothing - confirming perm is not a modify trigger on either scan path.
	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		t.Fatalf("queuedPaths: %v", err)
	}
	ops, err = e.localReconcileWith(context.Background(), viewID, queued, dirCache, rehashSlice{of: 1})
	if err != nil {
		t.Fatalf("full-rehash localReconcileWith: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("full-rehash pass: got %d ops, want 0 (perm-only change is not synced): %+v", len(ops), ops)
	}
}

// TestLocalReconcileUnreadableSubtreeProtectsBaselines confirms an EACCES on one subdirectory
// neither aborts the view scan nor causes a spurious deletion of the baselines beneath it,
// while an unrelated change elsewhere in the view is still detected and pushed.
func TestLocalReconcileUnreadableSubtreeProtectsBaselines(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix directory permission bits are not meaningful on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	e, viewID, viewDir := newScanEngine(t)
	// A file outside the locked subtree whose content has diverged from its baseline.
	newTop := []byte("changed top")
	writeFile(t, viewDir, "top.txt", newTop)
	putSynced(t, e, viewID, "top.txt", "ft", "file", "oldhash", 3)
	// A baseline under a subtree that becomes unreadable mid-scan.
	writeFile(t, viewDir, "locked/secret.txt", []byte("secret"))
	putSynced(t, e, viewID, "locked", "fl", "dir", "", 0)
	putSynced(t, e, viewID, "locked/secret.txt", "fs", "file", md4.ContentHash([]byte("secret")), 6)
	locked := filepath.Join(viewDir, "locked")
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatalf("chmod locked: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile aborted on unreadable subtree: %v", err)
	}
	if _, ok := findOp(ops, "locked/secret.txt"); ok {
		t.Error("emitted a spurious remove for a baseline under the unreadable subtree")
	}
	up, ok := findOp(ops, "top.txt")
	if !ok || up.Type != store.OpUpload {
		t.Fatalf("change outside the unreadable subtree was not detected: ops=%+v", ops)
	}
}

func findOp(ops []store.Op, path string) (store.Op, bool) {
	for _, op := range ops {
		if op.Path == path {
			return op, true
		}
	}
	return store.Op{}, false
}

// assertQueueMatches confirms the durable queue holds exactly the returned ops, in order.
func assertQueueMatches(t *testing.T, e *Engine, ops []store.Op) {
	t.Helper()
	queued, err := e.store.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(queued) != len(ops) {
		t.Fatalf("queue has %d ops, want %d: %+v", len(queued), len(ops), queued)
	}
	for i := range ops {
		if queued[i].Op != ops[i] {
			t.Errorf("queued[%d] = %+v, returned %+v", i, queued[i].Op, ops[i])
		}
	}
}

// drainQueue clears every pending op from the durable queue, simulating the executor having
// applied and acked them. Used by tests that need a second localReconcile pass to act on a
// path whose first-pass op has since drained (the queued-path guard otherwise suppresses it).
func drainQueue(t *testing.T, e *Engine) {
	t.Helper()
	for {
		head, err := e.store.PendingOpsN(1)
		if err != nil {
			t.Fatalf("PendingOpsN: %v", err)
		}
		if len(head) == 0 {
			break // queue empty
		}
		if err := e.store.Ack(head[0].Seq); err != nil {
			t.Fatalf("Ack %d: %v", head[0].Seq, err)
		}
	}
}

// TestLocalReconcileEmptyScanSkipsMassRemoval confirms that an INITIAL empty scan never enqueues
// a removal of EVERY synced baseline - neither when the view root is MISSING (a removed or
// not-yet-mounted directory) nor when it is PRESENT BUT EMPTY (an unmounted mount point is an
// ordinary empty directory, indistinguishable from a genuine whole-view delete). A wipe of the
// entire view from a single empty scan is the unavailable-mount signature, so it is suppressed
// data-preservingly in both cases. The suppression is a DEBOUNCE, not permanent: once
// the empty state persists across emptyScanConfirmThreshold scans the deletion converges - see
// TestLocalReconcileEmptyScanConvergesAfterThreshold. Ordinary per-file deletions, where some
// entries remain on disk, still propagate at once - see TestLocalReconcileDeletes.
func TestLocalReconcileEmptyScanSkipsMassRemoval(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	// Two synced files are tracked, but the view root directory is removed entirely.
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	putSynced(t, e, viewID, "b.txt", "fb", "file", "hb", 2)
	if err := os.RemoveAll(viewDir); err != nil {
		t.Fatalf("remove view dir: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("missing root produced %d ops, want 0 (no mass deletion): %+v", len(ops), ops)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty after missing-root reconcile: %d", n)
	}

	// A present-but-empty root (the unmounted-mount-point case) is equally suppressed: it
	// cannot be told apart from a genuine whole-view delete, so the wipe is not propagated.
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("recreate empty view dir: %v", err)
	}
	ops, err = e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile (empty root): %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("empty root produced %d ops, want 0 (no mass deletion): %+v", len(ops), ops)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty after empty-root reconcile: %d", n)
	}
}

// TestLocalReconcileEmptyScanConvergesAfterThreshold confirms the empty-scan suppression is a
// debounce, not a permanent block: a view emptied on disk while synced baselines exist is
// suppressed for the first emptyScanConfirmThreshold-1 scans, then on the threshold scan the
// whole-view deletion is propagated as OpRemoves. This is what lets a legitimate delete-of-all
// (and the vanished-upload recovery's final-file removal) converge rather than wedge.
func TestLocalReconcileEmptyScanConvergesAfterThreshold(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	putSynced(t, e, viewID, "b.txt", "fb", "file", "hb", 2)
	// The view dir exists (newScanEngine created it) and is empty.

	// The first threshold-1 scans are suppressed pending confirmation. Each is spaced past the
	// debounce's wall-clock floor, since a burst of scans no longer counts.
	for i := 1; i < emptyScanConfirmThreshold; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d produced %d ops, want 0 (still debouncing): %+v", i, len(ops), ops)
		}
		advance(emptyScanConfirmMinSpacing)
	}

	// The threshold scan confirms the empty state and propagates the deletions.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile threshold scan: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("threshold scan produced %d ops, want 2 removes: %+v", len(ops), ops)
	}
	for _, want := range []string{"a.txt", "b.txt"} {
		op, ok := findOp(ops, want)
		if !ok || op.Type != store.OpRemove {
			t.Errorf("missing OpRemove for %q: %+v", want, ops)
		}
	}
}

// TestLocalReconcileMissingRootNeverConverges confirms a detectably-unavailable root (a missing
// directory) never advances the empty-scan debounce, so it can never propagate a whole-view
// deletion no matter how many scans run - unlike a present-but-empty root, which converges after
// the threshold. A vanished root is an external anomaly (an unmount, a moved or removed folder),
// not the user deleting every file, so the remote view must be preserved.
func TestLocalReconcileMissingRootNeverConverges(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	putSynced(t, e, viewID, "b.txt", "fb", "file", "hb", 2)
	if err := os.RemoveAll(viewDir); err != nil {
		t.Fatalf("remove view dir: %v", err)
	}

	// Far more scans than the convergence threshold: a missing root must still produce no ops.
	for i := 0; i < emptyScanConfirmThreshold+3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d on a missing root produced %d ops, want 0 (no mass deletion): %+v", i, len(ops), ops)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("missing root advanced the empty-scan debounce to %d, want 0 (never counted)", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty after repeated missing-root reconciles: %d", n)
	}
}

// TestLocalReconcileSymlinkedRootNeverConverges is the symlink counterpart: a view root replaced
// by a symlink is detectably unavailable (it could redirect the whole view outside the sync tree),
// so it must never advance the empty-scan debounce toward a whole-view deletion.
func TestLocalReconcileSymlinkedRootNeverConverges(t *testing.T) {
	requireSymlinks(t)
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)
	if err := os.RemoveAll(viewDir); err != nil {
		t.Fatalf("remove view dir: %v", err)
	}
	// Replace the root with a symlink to an (empty) directory elsewhere.
	if err := os.Symlink(t.TempDir(), viewDir); err != nil {
		t.Fatalf("symlink view root: %v", err)
	}

	for i := 0; i < emptyScanConfirmThreshold+1; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d on a symlinked root produced %d ops, want 0: %+v", i, len(ops), ops)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("symlinked root advanced the debounce to %d, want 0", got)
	}
}

// TestLocalReconcileNonEmptyScanResetsEmptyDebounce confirms a non-empty scan resets the
// consecutive-empty counter, so a transient empty scan followed by a recovery does not leave the
// view one scan away from a mass deletion.
func TestLocalReconcileNonEmptyScanResetsEmptyDebounce(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)

	// Two empty scans (still below threshold 3), spaced past the debounce's wall-clock floor,
	// accumulate.
	for i := 0; i < 2; i++ {
		if _, err := e.localReconcile(context.Background(), viewID); err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		advance(emptyScanConfirmMinSpacing)
	}
	if got := e.emptyViewScans[viewID].count; got != 2 {
		t.Fatalf("emptyViewScans = %d, want 2 before recovery", got)
	}

	// The view reappears with its file: the counter must reset.
	writeFile(t, viewDir, "a.txt", []byte("ha"))
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("recovery scan: %v", err)
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("emptyViewScans = %d after a non-empty scan, want 0 (reset)", got)
	}
}

// TestLocalReconcileUnavailableRootResetsEmptyDebounce confirms an unavailable-root scan
// RESETS the consecutive-empty counter rather than merely not advancing it. The debounce
// confirms a whole-view delete only across uninterrupted consecutive empty scans; an
// unavailable scan is not such an observation, so it must not be allowed to bridge two
// empty scans into a false "consecutive" streak. Without the reset, empty, empty,
// unavailable, empty would reach the threshold and wipe the remote view.
func TestLocalReconcileUnavailableRootResetsEmptyDebounce(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "a.txt", "fa", "file", "ha", 2)

	// Two empty (but present) scans accumulate, one short of the threshold.
	for i := 0; i < emptyScanConfirmThreshold-1; i++ {
		if _, err := e.localReconcile(context.Background(), viewID); err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		advance(emptyScanConfirmMinSpacing)
	}
	if got := e.emptyViewScans[viewID].count; got != emptyScanConfirmThreshold-1 {
		t.Fatalf("emptyViewScans = %d, want %d before the unavailable scan", got, emptyScanConfirmThreshold-1)
	}

	// The root goes unavailable (removed). This scan must reset the counter, not preserve it.
	if err := os.RemoveAll(viewDir); err != nil {
		t.Fatalf("remove view dir: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("unavailable scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("unavailable scan produced %d ops, want 0", len(ops))
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Fatalf("unavailable scan left emptyViewScans = %d, want 0 (reset)", got)
	}

	// Recreate the (empty) root and scan once more: with the counter reset, a single empty
	// scan must NOT reach the threshold, so no mass deletion is propagated.
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("recreate view dir: %v", err)
	}
	ops, err = e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("post-recovery scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("post-recovery empty scan produced %d ops, want 0 (streak was reset): %+v", len(ops), ops)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: an unavailable scan let the debounce wipe the view: %d", n)
	}
}

// TestLocalReconcileDeviceChangeSuppressesEmptyScan confirms that when a view root's
// filesystem device id changes between a healthy (non-empty) scan and a later empty scan - the
// signature of a mount that dropped or swapped underneath the path, which presents as an
// ordinary empty directory Lstat cannot tell from a real empty-out - the empty scan is treated
// as unavailable: suppressed, no ops, debounce reset, even across many scans, so a mount swap
// never wipes the remote view.
func TestLocalReconcileDeviceChangeSuppressesEmptyScan(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	// A synced directory baseline whose on-disk directory is present keeps the healthy scan
	// non-empty AND op-free (a matching dir needs no op), so the only thing it does is record
	// the view root's device id.
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}

	// Drive the device id through a controllable seam.
	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("healthy scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("healthy scan produced %d ops, want 0: %+v", len(ops), ops)
	}
	if got, ok := e.viewRootDev[viewID]; !ok || got != 1 {
		t.Fatalf("recorded device = %d (ok=%v), want 1", got, ok)
	}

	// The directory disappears AND the device changes: a mount dropped, exposing a different
	// underlying filesystem. Every empty scan must now be suppressed.
	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	curDev = 2
	for i := 0; i < emptyScanConfirmThreshold+3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d on a device-swapped root produced %d ops, want 0: %+v", i, len(ops), ops)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("device-swapped root advanced the debounce to %d, want 0 (never counted)", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a device swap let the debounce wipe the view: %d", n)
	}
}

// TestLocalReconcileDevicePersistsAcrossRestart confirms the empty-scan mount-swap guard
// survives a daemon restart: the healthy device id is persisted to the view record, so a FRESH
// engine over the same store (its in-memory viewRootDev map empty, as on a restart) still
// suppresses an empty scan whose device id no longer matches the persisted baseline - closing
// the gap where a restart into an already-dropped/swapped mount would erase the remote view.
func TestLocalReconcileDevicePersistsAcrossRestart(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	e.deviceID = func(string) (uint64, error) { return 42, nil }
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}
	// The healthy device id must have been persisted to the durable view record.
	v, err := e.store.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 42 {
		t.Fatalf("persisted RootDevice = %d, want 42", v.RootDevice)
	}

	// Simulate a restart: a brand-new engine over the same store and root, so its in-memory
	// viewRootDev map starts empty. The mount has dropped (the directory is gone) and the
	// underlying device differs from the persisted baseline.
	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	e2 := New(e.store, &fakeTransport{client: &fakeClient{}}, e.syncRoot, "sess", nil)
	e2.deviceID = func(string) (uint64, error) { return 99, nil } // device swapped underneath
	for i := 0; i < emptyScanConfirmThreshold+2; i++ {
		ops, err := e2.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("post-restart empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("post-restart scan %d produced %d ops, want 0 (persisted device guard): %+v", i, len(ops), ops)
		}
	}
	if got := e2.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("post-restart debounce advanced to %d, want 0 (never counted)", got)
	}
	if n, _ := e2.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a restart into a swapped mount let the debounce wipe the view: %d", n)
	}
}

// TestLocalReconcileLegitDeleteConvergesAcrossRestart confirms the persisted device guard does
// not over-suppress: after a restart, an empty scan whose device id MATCHES the persisted
// baseline (the mount is the same, the user genuinely emptied the view) still converges through
// the debounce to a whole-view delete, rather than being blocked forever.
func TestLocalReconcileLegitDeleteConvergesAcrossRestart(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	e.deviceID = func(string) (uint64, error) { return 7, nil }
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	// Restart with the SAME underlying device, and the user has genuinely deleted the view's
	// contents.
	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	e2 := New(e.store, &fakeTransport{client: &fakeClient{}}, e.syncRoot, "sess", nil)
	advance := stubClock(e2)
	e2.deviceID = func(string) (uint64, error) { return 7, nil } // same device - a real empty-out
	var lastOps []store.Op
	for i := 0; i < emptyScanConfirmThreshold; i++ {
		ops, err := e2.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("post-restart empty scan %d: %v", i, err)
		}
		lastOps = ops
		advance(emptyScanConfirmMinSpacing)
	}
	if len(lastOps) == 0 {
		t.Fatalf("legitimate empty-out never converged after restart; debounce stuck at %d", e2.emptyViewScans[viewID].count)
	}
}

// TestLocalReconcileDeviceUnreadableSuppressesEmptyScan confirms that once a healthy device id
// has been recorded, an empty scan whose device id can no longer be read (a Statfs/handle
// failure - e.g. the mount is mid-drop) is conservatively treated as unavailable rather than
// risking a whole-view delete.
func TestLocalReconcileDeviceUnreadableSuppressesEmptyScan(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}

	healthy := true
	e.deviceID = func(string) (uint64, error) {
		if healthy {
			return 9, nil
		}
		return 0, fmt.Errorf("statfs failed")
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}
	if _, ok := e.viewRootDev[viewID]; !ok {
		t.Fatalf("healthy scan did not record a device id")
	}

	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	healthy = false
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("empty scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("unreadable-device empty scan produced %d ops, want 0: %+v", len(ops), ops)
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("unreadable-device scan advanced the debounce to %d, want 0", got)
	}
}

// TestLocalReconcileSameDeviceStillConverges confirms the device guard does NOT block a genuine
// whole-view delete: when the device id is unchanged between the healthy and the empty scans,
// the empty-scan debounce advances and propagates the deletion after the threshold, exactly as
// before (no false positive from the new guard).
func TestLocalReconcileSameDeviceStillConverges(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	advance := stubClock(e)
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	e.deviceID = func(string) (uint64, error) { return 42, nil } // device never changes

	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	// The directory is genuinely deleted on the SAME device. The debounce must advance and, at
	// the threshold, propagate the removal.
	if err := os.RemoveAll(filepath.Join(viewDir, "sub")); err != nil {
		t.Fatalf("remove sub: %v", err)
	}
	var lastOps []store.Op
	for i := 0; i < emptyScanConfirmThreshold; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		lastOps = ops
		advance(emptyScanConfirmMinSpacing)
	}
	if len(lastOps) != 1 || lastOps[0].Type != store.OpRemove {
		t.Fatalf("same-device delete did not converge to an OpRemove after the threshold: %+v", lastOps)
	}
}

// TestDetectRemovesAndMovesSkipUnreadable confirms a path the scan saw but could not read
// this pass (errUploadSourceGone - a transient unlink/recreate or swap race) is treated as
// still-present, not as a deletion: it produces neither an OpRemove nor a move-source match,
// so a momentary read failure cannot push a spurious remote delete.
func TestDetectRemovesAndMovesSkipUnreadable(t *testing.T) {
	e, viewID, _ := newScanEngine(t)

	baselines := []store.PathBaseline{
		{Path: "gone.txt", Baseline: store.Baseline{FileID: "fg", FileType: "file", LocalHash: "hg", LocalSize: 3, State: store.StateSynced}},
		{Path: "racing.txt", Baseline: store.Baseline{FileID: "fr", FileType: "file", LocalHash: "hr", LocalSize: 4, State: store.StateSynced}},
	}
	// Neither path is in the scan's readable entries, so both are base-only merge rows.
	rows := []joinRow{{base: &baselines[0]}, {base: &baselines[1]}}
	protected := newProtectedIndex(map[string]bool{"racing.txt": true})

	curViewDir := e.viewDir(viewID, map[uint64]string{})
	removes := e.detectRemoves(viewID, curViewDir, rows, map[string]bool{}, queuedIndex{}, protected, nil)
	if len(removes) != 1 || removes[0].op.Path != "gone.txt" {
		t.Fatalf("detectRemoves = %+v, want a single OpRemove for gone.txt (racing.txt is unreadable, deferred)", removes)
	}

	// detectMoves must not consider an unreadable baseline a vanished move source.
	matchedDel, _, _ := e.detectMoves(viewID, curViewDir, rows, queuedIndex{}, protected, map[string]bool{})
	if matchedDel["racing.txt"] {
		t.Errorf("detectMoves matched the unreadable racing.txt as a move source")
	}
}

// TestQueuedIndexMatchesLinearScan asserts the prefix-index queuedIndex.conflict gives the
// same answer as the original brute-force scan (does the candidate key overlap any queued
// endpoint under pathsOverlap?) across exact, ancestor and descendant relationships - the
// equivalence the O((files+queued)×depth) refactor relies on.
func TestQueuedIndexMatchesLinearScan(t *testing.T) {
	endpoints := []string{"a/b/c", "a/x", "d", "e/f"}
	candidates := []string{"a/b/c", "a/b", "a", "a/b/c/d", "a/x", "a/y", "d", "d/e", "e", "e/f", "z", "a/bb"}

	const vd = "view"
	idx := queuedIndex{exact: map[string]bool{}, ancestors: map[string]bool{}}
	var keys []string
	for _, ep := range endpoints {
		k := cleanKey(vd, ep)
		idx.add(k)
		keys = append(keys, k)
	}
	for _, c := range candidates {
		ck := cleanKey(vd, c)
		want := false
		for _, k := range keys {
			if pathsOverlap(ck, k) {
				want = true
				break
			}
		}
		if got := idx.conflict(ck); got != want {
			t.Errorf("conflict(%q) = %v, want %v (linear pathsOverlap scan)", c, got, want)
		}
	}
}

func TestLocalReconcileCreates(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	top := []byte("top content")
	nested := []byte("nested content")
	writeFile(t, viewDir, "a.txt", top)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	writeFile(t, viewDir, "sub/b.txt", nested)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	// Creates ascending by path: a.txt, sub, sub/b.txt.
	if len(ops) != 3 {
		t.Fatalf("got %d ops, want 3: %+v", len(ops), ops)
	}
	want := []struct {
		path string
		typ  store.OpType
	}{
		{"a.txt", store.OpUpload},
		{"sub", store.OpMkdir},
		{"sub/b.txt", store.OpUpload},
	}
	for i, w := range want {
		if ops[i].Path != w.path || ops[i].Type != w.typ {
			t.Errorf("ops[%d] = %s %q, want %s %q", i, ops[i].Type, ops[i].Path, w.typ, w.path)
		}
		if ops[i].Direction != store.DirOutbound {
			t.Errorf("ops[%d] direction = %q, want outbound", i, ops[i].Direction)
		}
		if ops[i].FileID != "" {
			t.Errorf("ops[%d] carried a file id %q on a fresh create", i, ops[i].FileID)
		}
	}
	if a, _ := findOp(ops, "a.txt"); a.LocalHash != md4.ContentHash(top) || a.LocalSize != int64(len(top)) {
		t.Errorf("a.txt op facts = hash %q size %d, want %q %d", a.LocalHash, a.LocalSize, md4.ContentHash(top), len(top))
	}
	assertQueueMatches(t, e, ops)

	// The affected baselines are now StatePending carrying the local facts.
	b, err := e.store.GetBaseline(viewID, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline a.txt: %v", err)
	}
	if b.State != store.StatePending || b.LocalHash != md4.ContentHash(top) {
		t.Errorf("a.txt baseline = %+v, want pending with local hash", b)
	}
}

func TestLocalReconcileModify(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	old := []byte("old content")
	putSynced(t, e, viewID, "a.txt", "F1", "file", md4.ContentHash(old), int64(len(old)))
	// Give the baseline a distinct server fact to confirm it survives the local flip.
	b0, _ := e.store.GetBaseline(viewID, "a.txt")
	b0.ServerSyncID = 99
	if err := e.store.PutBaseline(viewID, "a.txt", b0); err != nil {
		t.Fatalf("seed server sync id: %v", err)
	}

	updated := []byte("brand new content")
	writeFile(t, viewDir, "a.txt", updated)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "a.txt" {
		t.Fatalf("got %+v, want single upload of a.txt", ops)
	}
	if ops[0].FileID != "F1" {
		t.Errorf("modify op file id = %q, want F1", ops[0].FileID)
	}
	if ops[0].LocalHash != md4.ContentHash(updated) {
		t.Errorf("modify op hash = %q, want %q", ops[0].LocalHash, md4.ContentHash(updated))
	}
	b, _ := e.store.GetBaseline(viewID, "a.txt")
	if b.State != store.StatePending || b.LocalHash != md4.ContentHash(updated) {
		t.Errorf("baseline = %+v, want pending with new local hash", b)
	}
	if b.ServerSyncID != 99 || b.FileID != "F1" {
		t.Errorf("baseline lost server facts: %+v", b)
	}
}

func TestLocalReconcileUnchangedNoOp(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	content := []byte("steady")
	putSynced(t, e, viewID, "a.txt", "F1", "file", md4.ContentHash(content), int64(len(content)))
	writeFile(t, viewDir, "a.txt", content)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("unchanged file produced ops: %+v", ops)
	}
}

func TestLocalReconcileDelete(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// keep.txt remains on disk (already synced, so it produces no op); gone.txt is tracked but
	// absent. The remaining entry keeps the scan non-empty so the per-file deletion is not
	// suppressed by the whole-view mass-deletion guard.
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "F1", "file", md4.ContentHash(keep), int64(len(keep)))
	putSynced(t, e, viewID, "gone.txt", "F2", "file", "deadbeef", 4)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "gone.txt" {
		t.Fatalf("got %+v, want single remove of gone.txt", ops)
	}
	if ops[0].FileID != "F2" {
		t.Errorf("remove op file id = %q, want F2", ops[0].FileID)
	}
	b, _ := e.store.GetBaseline(viewID, "gone.txt")
	if b.State != store.StatePending {
		t.Errorf("vanished baseline = %+v, want pending", b)
	}
}

// TestLocalReconcileTypeReplacementFileToDir confirms a synced file baseline whose on-disk
// path is now a directory emits an OpRemove of the stale server file this scan (so the old
// node is deleted before the replacement is created), and that - once that remove drains and
// drops the baseline - a later scan detects the directory as a fresh OpMkdir.
func TestLocalReconcileTypeReplacementFileToDir(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// keep.txt remains so the scan is non-empty (the mass-deletion guard does not fire).
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "FK", "file", md4.ContentHash(keep), int64(len(keep)))

	// "a" was a synced file; on disk it is now a directory.
	putSynced(t, e, viewID, "a", "FA", "file", "deadbeef", 4)
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "a" {
		t.Fatalf("got %+v, want single remove of a", ops)
	}
	if ops[0].FileID != "FA" {
		t.Errorf("remove op file id = %q, want FA", ops[0].FileID)
	}

	// Simulate the remove draining: the baseline is dropped and the queue cleared. The next
	// scan must then detect the directory as a fresh create.
	if err := e.store.DeleteBaseline(viewID, "a"); err != nil {
		t.Fatalf("DeleteBaseline a: %v", err)
	}
	drainQueue(t, e)

	ops2, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile (after remove): %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpMkdir || ops2[0].Path != "a" {
		t.Fatalf("got %+v, want single mkdir of a", ops2)
	}
}

// TestLocalReconcileTypeReplacementFileToDirWithChildren confirms that when a synced file
// baseline's on-disk path becomes a directory that already contains children, the same scan
// emits ONLY the OpRemove of the stale file - the children are deferred. Pushing a child this
// scan would resolve its parent id to the stale file baseline (still carrying the old file's
// id) and mis-parent it under the soon-to-be-removed node. Once the remove drains and the
// baseline is gone, a later scan creates the directory and then its children correctly.
func TestLocalReconcileTypeReplacementFileToDirWithChildren(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "FK", "file", md4.ContentHash(keep), int64(len(keep)))

	// "a" was a synced file; on disk it is now a directory containing a child "a/child.txt".
	putSynced(t, e, viewID, "a", "FA", "file", "deadbeef", 4)
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	child := []byte("child contents")
	writeFile(t, viewDir, filepath.Join("a", "child.txt"), child)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	// Only the remove of the stale file this scan; the child upload must NOT be enqueued yet.
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "a" {
		t.Fatalf("got %+v, want single remove of a", ops)
	}
	for _, op := range ops {
		if op.Path == "a/child.txt" {
			t.Fatalf("child a/child.txt was pushed this scan under the stale parent: %+v", op)
		}
	}

	// Simulate the remove draining: drop the baseline and clear the queue. The next scan now
	// creates the directory and the child, parent-before-child.
	if err := e.store.DeleteBaseline(viewID, "a"); err != nil {
		t.Fatalf("DeleteBaseline a: %v", err)
	}
	drainQueue(t, e)

	ops2, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile (after remove): %v", err)
	}
	if len(ops2) != 2 {
		t.Fatalf("got %d ops, want mkdir a + upload a/child.txt: %+v", len(ops2), ops2)
	}
	if ops2[0].Type != store.OpMkdir || ops2[0].Path != "a" {
		t.Errorf("ops2[0] = %s %q, want mkdir a", ops2[0].Type, ops2[0].Path)
	}
	if ops2[1].Type != store.OpUpload || ops2[1].Path != "a/child.txt" {
		t.Errorf("ops2[1] = %s %q, want upload a/child.txt", ops2[1].Type, ops2[1].Path)
	}
}

// TestLocalReconcileTypeReplacementDirToFile confirms a synced directory baseline (with a
// tracked child) whose on-disk path is now a file emits removes children-before-parents - the
// child file first, then the directory itself - and that a later scan uploads the new file.
func TestLocalReconcileTypeReplacementDirToFile(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "FK", "file", md4.ContentHash(keep), int64(len(keep)))

	// "d" was a synced directory with a child "d/x"; on disk "d" is now a regular file.
	putSynced(t, e, viewID, "d", "FD", "dir", "", 0)
	putSynced(t, e, viewID, "d/x", "FX", "file", "abc", 3)
	now := []byte("d is a file now")
	writeFile(t, viewDir, "d", now)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	// Two removes, children before parents: d/x then d. The replacement upload of "d" waits
	// for a later scan once the directory baseline is gone.
	if len(ops) != 2 {
		t.Fatalf("got %d ops, want 2: %+v", len(ops), ops)
	}
	if ops[0].Type != store.OpRemove || ops[0].Path != "d/x" {
		t.Errorf("ops[0] = %s %q, want remove d/x", ops[0].Type, ops[0].Path)
	}
	if ops[1].Type != store.OpRemove || ops[1].Path != "d" {
		t.Errorf("ops[1] = %s %q, want remove d", ops[1].Type, ops[1].Path)
	}

	// Drain and drop the baselines; the next scan uploads the new file.
	if err := e.store.DeleteBaseline(viewID, "d/x"); err != nil {
		t.Fatalf("DeleteBaseline d/x: %v", err)
	}
	if err := e.store.DeleteBaseline(viewID, "d"); err != nil {
		t.Fatalf("DeleteBaseline d: %v", err)
	}
	drainQueue(t, e)

	ops2, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile (after removes): %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpUpload || ops2[0].Path != "d" {
		t.Fatalf("got %+v, want single upload of d", ops2)
	}
	if ops2[0].LocalHash != md4.ContentHash(now) {
		t.Errorf("upload hash = %q, want %q", ops2[0].LocalHash, md4.ContentHash(now))
	}
}

func TestLocalReconcileRemovesDescending(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// A directory and a child file, both tracked synced, both gone on disk. keep.txt remains
	// on disk (already synced) so the scan is non-empty and the deletions are not suppressed
	// by the whole-view mass-deletion guard.
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "FK", "file", md4.ContentHash(keep), int64(len(keep)))
	putSynced(t, e, viewID, "d", "FD", "dir", "", 0)
	putSynced(t, e, viewID, "d/x", "FX", "file", "abc", 3)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("got %d ops, want 2: %+v", len(ops), ops)
	}
	// Children before parents: d/x then d.
	if ops[0].Path != "d/x" || ops[1].Path != "d" {
		t.Errorf("remove order = [%q,%q], want [d/x, d]", ops[0].Path, ops[1].Path)
	}
}

func TestLocalReconcileSkipsPendingAndConflict(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	put := func(path, fileID string, state store.BaselineState) {
		b := store.Baseline{FileID: fileID, FileType: "file", LocalHash: "old", State: state}
		if err := e.store.PutBaseline(viewID, path, b); err != nil {
			t.Fatalf("PutBaseline %s: %v", path, err)
		}
	}
	put("p.txt", "XP", store.StatePending)
	put("c.txt", "XC", store.StateConflict)
	// Both changed on disk relative to their baseline hash.
	writeFile(t, viewDir, "p.txt", []byte("changed p"))
	writeFile(t, viewDir, "c.txt", []byte("changed c"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("pending/conflict baselines produced ops: %+v", ops)
	}
}

func TestLocalReconcileNoDoubleEnqueue(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// An op for a.txt is already queued (a slow push still draining).
	if _, err := e.store.Enqueue(store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: viewID, Path: "a.txt"}); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	// A rename op already references both endpoints.
	if _, err := e.store.Enqueue(store.Op{Type: store.OpRename, Direction: store.DirOutbound, ViewID: viewID, Path: "from.txt", NewPath: "to.txt"}); err != nil {
		t.Fatalf("seed rename: %v", err)
	}
	writeFile(t, viewDir, "a.txt", []byte("content"))
	writeFile(t, viewDir, "to.txt", []byte("moved content"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("queued paths were re-enqueued: %+v", ops)
	}
}

// TestLocalReconcileProtectsSubtreeUnderQueuedDirOp confirms that when a directory op is
// already queued (e.g. an inbound move "a" -> "b" whose disk apply has not yet run after a
// crashed/failed drain), the scanner does not emit stale ops for the still-on-disk children
// beneath the source. The inbound move relocated the descendant baselines to the destination,
// so the source children have no baseline and would otherwise diff as fresh creates that
// resolve their parent to the soon-removed directory.
func TestLocalReconcileProtectsSubtreeUnderQueuedDirOp(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// An inbound directory move a -> b is queued but not yet applied to disk.
	if _, err := e.store.Enqueue(store.Op{Type: store.OpRename, Direction: store.DirInbound, ViewID: viewID, Path: "a", NewPath: "b"}); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	// Reconcile already relocated the descendant baseline to the destination (StatePending).
	child := []byte("child content")
	if err := e.store.PutBaseline(viewID, "b/x.txt", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(child),
		LocalSize: int64(len(child)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	// Disk still holds the source subtree (the move's disk apply has not run).
	writeFile(t, viewDir, "a/x.txt", child)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %+v, want 0 ops (subtree under the queued dir move must be protected)", ops)
	}
}

// TestLocalReconcileProtectsAncestorOfQueuedOp asserts the symmetric direction of the
// queued-path guard: when a queued inbound op targets a descendant (a child create at
// "a/x.txt"), the scanner does not emit an OpRemove for the synced ancestor directory "a"
// even though "a" has vanished on disk. Without this an outbound remove of "a" would delete
// the directory on the server while the inbound download recreates "a/x.txt" locally - the
// two diverge. Suppressing "a"'s remove lets the server-authoritative inbound op land first
// (asymmetry round).
func TestLocalReconcileProtectsAncestorOfQueuedOp(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// A queued inbound download targets a child of the synced directory "a".
	if _, err := e.store.Enqueue(store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: viewID, Path: "a/x.txt"}); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	// "a" is a synced directory that has vanished on disk; an unrelated file keeps the scan
	// non-empty so the empty-view debounce does not interfere.
	putSynced(t, e, viewID, "a", "FA", "dir", "", 0)
	other := []byte("other content")
	putSynced(t, e, viewID, "other.txt", "FO", "file", md4.ContentHash(other), int64(len(other)))
	writeFile(t, viewDir, "other.txt", other)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %+v, want 0 ops (ancestor of the queued op must be protected)", ops)
	}
}

// TestLocalReconcileProtectsAcrossSharedDirViews asserts the queued-path guard resolves
// endpoints by on-disk location across ALL views, not just the scanned view - mirroring the
// executor's same-directory cross-view serialization (TestDrainQueueSerializesSameDirAcrossViews).
// View 8 shares view 7's on-disk directory (same view name), and view 8 holds a queued inbound
// create at "a/x.txt"; while reconciling view 7 - whose synced directory "a" has vanished on
// disk - the scanner must NOT emit an OpRemove of "a", because the sibling view's queued op
// resolves to the same disk subtree. Without the cross-view key, view 7 would delete "a" on its
// server while view 8's download recreates "a/x.txt" on the shared disk, leaving view 7's server
// permanently short the directory.
func TestLocalReconcileProtectsAcrossSharedDirViews(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// A second view sharing view 7's on-disk directory (same name resolves to the same dir).
	const siblingView = 8
	if err := e.store.PutView(siblingView, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView sibling: %v", err)
	}
	// The sibling view holds a queued inbound download at a child of the synced directory "a".
	if _, err := e.store.Enqueue(store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: siblingView, Path: "a/x.txt"}); err != nil {
		t.Fatalf("seed sibling queue: %v", err)
	}
	// In view 7, "a" is a synced directory that has vanished on disk; an unrelated file keeps
	// the scan non-empty so the empty-view debounce does not interfere.
	putSynced(t, e, viewID, "a", "FA", "dir", "", 0)
	other := []byte("other content")
	putSynced(t, e, viewID, "other.txt", "FO", "file", md4.ContentHash(other), int64(len(other)))
	writeFile(t, viewDir, "other.txt", other)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %+v, want 0 ops (sibling view's queued op on the shared dir must protect \"a\")", ops)
	}
}

func TestLocalReconcileMoveCollapse(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	content := []byte("portable content")
	putSynced(t, e, viewID, "old.txt", "F3", "file", md4.ContentHash(content), int64(len(content)))
	// The file now lives at new.txt with identical bytes; old.txt is gone.
	writeFile(t, viewDir, "new.txt", content)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("got %+v, want single rename", ops)
	}
	if ops[0].Path != "old.txt" || ops[0].NewPath != "new.txt" || ops[0].FileID != "F3" {
		t.Errorf("rename op = %+v, want old.txt->new.txt id F3", ops[0])
	}
	// The baseline travelled to the new path.
	if _, err := e.store.GetBaseline(viewID, "old.txt"); !isNotFound(err) {
		t.Errorf("old baseline survived the move: err=%v", err)
	}
	nb, err := e.store.GetBaseline(viewID, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline new.txt: %v", err)
	}
	if nb.State != store.StatePending || nb.FileID != "F3" {
		t.Errorf("relocated baseline = %+v, want pending id F3", nb)
	}
}

// TestLocalReconcileCaseFoldDivergenceMatchesExisting proves the streaming merge-join
// preserves the GOOS case-fold matching the former map-keyed diff did: on a case-insensitive
// filesystem a baseline stored as "Foo.txt" and an on-disk file "foo.txt" join as the SAME
// node (same normpath.Key), so an unchanged-content scan yields no op (no spurious
// delete+create), and an edit emits a modify addressing the existing baseline path "Foo.txt"
// - never stranding a second baseline at the scan's case.
func TestLocalReconcileCaseFoldDivergenceMatchesExisting(t *testing.T) {
	restore := normpath.SetCaseInsensitive(true)
	defer restore()

	e, viewID, viewDir := newScanEngine(t)
	content := []byte("portable body")
	// Baseline keeps original case "Foo.txt"; the file on disk is "foo.txt".
	putSynced(t, e, viewID, "Foo.txt", "F1", "file", md4.ContentHash(content), int64(len(content)))
	writeFile(t, viewDir, "foo.txt", content)

	// Same content under a case-only divergence: matched as one node, no op.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("case-only divergence produced %d ops, want 0 (matched as same node): %+v", len(ops), ops)
	}

	// Edit on disk: a modify must address the existing baseline path, not the scan's case.
	edited := []byte("portable body, now edited")
	writeFile(t, viewDir, "foo.txt", edited)
	ops, err = e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile after edit: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "Foo.txt" || ops[0].FileID != "F1" {
		t.Fatalf("got %+v, want a single OpUpload of Foo.txt id F1", ops)
	}
	b, err := e.store.GetBaseline(viewID, "Foo.txt")
	if err != nil {
		t.Fatalf("GetBaseline Foo.txt: %v", err)
	}
	if b.State != store.StatePending || b.LocalHash != md4.ContentHash(edited) {
		t.Errorf("Foo.txt baseline = %+v, want pending with edited hash", b)
	}
	if _, err := e.store.GetBaseline(viewID, "foo.txt"); !isNotFound(err) {
		t.Errorf("a stray lower-case baseline was stranded: err=%v", err)
	}
}

// TestLocalReconcileMoveAcrossSortOrder proves move detection still pairs by content, not by
// merge adjacency: the vanished source ("zzz.txt") and the new destination ("aaa.txt") sort
// far apart, yet the streaming merge-join collapses them into a single OpRename. (The former
// map-keyed diff matched on the content key; the merge walk must not regress to matching only
// adjacent rows.)
func TestLocalReconcileMoveAcrossSortOrder(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	content := []byte("relocate me across the sort order")
	putSynced(t, e, viewID, "zzz.txt", "F9", "file", md4.ContentHash(content), int64(len(content)))
	writeFile(t, viewDir, "aaa.txt", content)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("got %+v, want a single OpRename", ops)
	}
	if ops[0].Path != "zzz.txt" || ops[0].NewPath != "aaa.txt" || ops[0].FileID != "F9" {
		t.Errorf("rename op = %+v, want zzz.txt->aaa.txt id F9", ops[0])
	}
	if _, err := e.store.GetBaseline(viewID, "zzz.txt"); !isNotFound(err) {
		t.Errorf("source baseline survived the move: err=%v", err)
	}
	nb, err := e.store.GetBaseline(viewID, "aaa.txt")
	if err != nil {
		t.Fatalf("GetBaseline aaa.txt: %v", err)
	}
	if nb.State != store.StatePending || nb.FileID != "F9" {
		t.Errorf("relocated baseline = %+v, want pending id F9", nb)
	}
}

// TestLocalReconcileLargeTreeMergeJoin exercises the streaming merge-join at scale with a mix
// of unchanged, modified, deleted, created and moved nodes, confirming the bounded-memory path
// produces exactly the same op set the per-path-map diff would. It is the large-input parity
// oracle for the bounded-memory merge (no duplicate full-tree maps): the merge walks one sorted
// scan slice and the streamed baselines, never two duplicate per-path index maps.
func TestLocalReconcileLargeTreeMergeJoin(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const n = 400
	// Unchanged files: synced baseline whose on-disk bytes still match (no op).
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("keep/%04d.txt", i)
		body := []byte(fmt.Sprintf("keep-%d", i))
		writeFile(t, viewDir, p, body)
		putSynced(t, e, viewID, p, fmt.Sprintf("K%04d", i), "file", md4.ContentHash(body), int64(len(body)))
	}
	// Modified files: on-disk content diverged from the baseline (one OpUpload each).
	for i := 0; i < 20; i++ {
		p := fmt.Sprintf("mod/%04d.txt", i)
		old := []byte(fmt.Sprintf("old-%d", i))
		putSynced(t, e, viewID, p, fmt.Sprintf("M%04d", i), "file", md4.ContentHash(old), int64(len(old)))
		writeFile(t, viewDir, p, []byte(fmt.Sprintf("new-%d", i)))
	}
	// Deleted files: synced baseline with no on-disk file (one OpRemove each).
	for i := 0; i < 15; i++ {
		p := fmt.Sprintf("del/%04d.txt", i)
		putSynced(t, e, viewID, p, fmt.Sprintf("D%04d", i), "file", "deadbeef", 4)
	}
	// Brand-new files: on disk with no baseline (one OpUpload each, plus the "new" dir mkdir).
	for i := 0; i < 10; i++ {
		writeFile(t, viewDir, fmt.Sprintf("new/%04d.txt", i), []byte(fmt.Sprintf("fresh-%d", i)))
	}
	// A move: source baseline vanished, identical bytes appear at a new path (one OpRename).
	moved := []byte("uniquely-movable-content-xyz")
	putSynced(t, e, viewID, "movesrc.txt", "MV", "file", md4.ContentHash(moved), int64(len(moved)))
	writeFile(t, viewDir, "movedst.txt", moved)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}

	var uploads, removes, renames, mkdirs int
	for _, op := range ops {
		switch op.Type {
		case store.OpUpload:
			uploads++
		case store.OpRemove:
			removes++
		case store.OpRename:
			renames++
		case store.OpMkdir:
			mkdirs++
		}
	}
	// 20 modifies + 10 new-file uploads = 30 uploads; 15 deletes; 1 move; 2 new dirs (new/, and
	// the moved/deleted files share existing dirs). "del/" has no on-disk dir so it is not created;
	// "mod/" exists on disk and is already tracked implicitly via files (no synced dir baseline),
	// so a fresh mkdir is emitted for any directory present on disk but absent from baselines.
	if renames != 1 {
		t.Errorf("renames = %d, want 1: %+v", renames, ops)
	}
	if uploads != 30 {
		t.Errorf("uploads = %d, want 30 (20 modify + 10 new)", uploads)
	}
	if removes != 15 {
		t.Errorf("removes = %d, want 15", removes)
	}
	// Directories present on disk without a baseline become fresh OpMkdirs: keep/, mod/, new/.
	if mkdirs != 3 {
		t.Errorf("mkdirs = %d, want 3 (keep/, mod/, new/): %+v", mkdirs, mkdirCounts(ops))
	}
	assertQueueMatches(t, e, ops)
}

// mkdirCounts lists the OpMkdir paths for a clearer failure message.
func mkdirCounts(ops []store.Op) []string {
	var out []string
	for _, op := range ops {
		if op.Type == store.OpMkdir {
			out = append(out, op.Path)
		}
	}
	return out
}

func TestLocalReconcileAmbiguousMoveFallsBack(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	content := []byte("dup content")
	h := md4.ContentHash(content)
	putSynced(t, e, viewID, "old1.txt", "FA", "file", h, int64(len(content)))
	putSynced(t, e, viewID, "old2.txt", "FB", "file", h, int64(len(content)))
	// One new file with the same content: 2 deletes vs 1 create is ambiguous.
	writeFile(t, viewDir, "new.txt", content)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	var renames, uploads, removes int
	for _, op := range ops {
		switch op.Type {
		case store.OpRename:
			renames++
		case store.OpUpload:
			uploads++
		case store.OpRemove:
			removes++
		}
	}
	if renames != 0 {
		t.Errorf("ambiguous match collapsed into a rename: %+v", ops)
	}
	if uploads != 1 || removes != 2 {
		t.Errorf("got %d uploads %d removes, want 1 upload and 2 removes: %+v", uploads, removes, ops)
	}
}
