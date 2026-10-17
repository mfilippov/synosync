package engine

// Tests for the engine half of the replay re-flush: the already-applied replay branches must
// re-flush the directory entries they swallow, because the only flush of those entries
// lived in the fsutil call that already returned. A crash (or a genuine EIO) between the
// on-disk rename and its syncRenameDirs otherwise leaves the replay acking a StateSynced
// baseline over non-durable entries - a power loss then resurrects the file at the source
// (pushed as a duplicate create next scan) or loses it at the destination (pushed as a
// spurious server delete).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// recordSyncDirFn swaps e's directory-flush seam for one that records the directories it was
// asked to flush (and optionally fails). The seam lives on the Engine, so each test's
// recording is its own and nothing has to be restored.
func recordSyncDirFn(t *testing.T, e *Engine, fail error) *[]string {
	t.Helper()
	var calls []string
	orig := priorSyncDir(e)
	e.syncDir = func(dir string) error {
		calls = append(calls, dir)
		if fail != nil {
			return fail
		}
		return orig(dir)
	}
	return &calls
}

// TestApplyRenameSwallowedReplayResyncsBothParents drives the gone-source/present-destination
// swallow (the crash-replay repair path) across directories and pins that BOTH affected
// parents are flushed before the op acks. Before the fix the branch acked with neither entry
// re-flushed: the fsutil.Rename that placed them had already returned, so its own flush could
// never be retried.
func TestApplyRenameSwallowedReplayResyncsBothParents(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	subDir := filepath.Join(viewDir, "sub")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("seed source dir: %v", err)
	}
	// The move already applied on disk: the node sits at the destination, the source is gone,
	// and its baseline is still the relocated StatePending record reconcile wrote (so byid
	// resolves fmov and the apply takes the swallow branch rather than the cold-start-gap drop).
	movedBytes := []byte("moved-content")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "sub/a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	calls := recordSyncDirFn(t, e, nil)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Fatalf("dead letters = %d, want 0 (the replay is swallowed, not dropped)", n)
	}
	// Destination parent first (the new entry), then the source parent (the removed entry) -
	// the syncRenameDirs order the swallowed call would have used.
	want := []string{viewDir, subDir}
	if len(*calls) != len(want) || (*calls)[0] != want[0] || (*calls)[1] != want[1] {
		t.Fatalf("directory-flush calls = %v, want %v", *calls, want)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.State != store.StateSynced {
		t.Fatalf("b.txt baseline = %+v (err %v), want synced (the replay still completes)", b, err)
	}
}

// TestApplyRenameSwallowedReplaySameDirResyncsOnce pins the dedup: a same-directory move has
// one affected parent, so the replay flushes it exactly once rather than twice.
func TestApplyRenameSwallowedReplaySameDirResyncsOnce(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	movedBytes := []byte("moved-content")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	calls := recordSyncDirFn(t, e, nil)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(*calls) != 1 || (*calls)[0] != viewDir {
		t.Fatalf("directory-flush calls = %v, want exactly [%q]", *calls, viewDir)
	}
}

// TestApplyRenameSwallowedReplaySyncFailureStillAcks pins the best-effort contract: the disk
// state is already correct and re-running the op cannot improve it, so a flush failure is
// logged rather than returned - surfacing it would leave the op in an eternal transient retry
// (blocking every overlapping successor) that no later write could clear.
func TestApplyRenameSwallowedReplaySyncFailureStillAcks(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	movedBytes := []byte("moved-content")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	calls := recordSyncDirFn(t, e, errors.New("simulated EIO"))
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(*calls) == 0 {
		t.Fatal("the directory-flush seam was never called")
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("pending ops = %d (err %v), want 0 - a failed best-effort flush must not requeue", n, err)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.State != store.StateSynced {
		t.Fatalf("b.txt baseline = %+v (err %v), want synced", b, err)
	}
}

// TestRelocateTrackedDirForMkdirReplayResyncsBothParents is the mkdir-axis mirror: the
// rejected-move-recovery subtree rename already applied on disk (old path gone, destination
// present), so the apply skips it and finishes the baseline relocation - and must re-flush
// both affected parents on the way, for the same reason as applyRename's swallow.
func TestRelocateTrackedDirForMkdirReplayResyncsBothParents(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	oldParent := filepath.Join(viewDir, "old")
	if err := os.MkdirAll(oldParent, 0o755); err != nil {
		t.Fatalf("seed old parent: %v", err)
	}
	// The subtree already sits at the destination; only the old path's baselines still point
	// at the pre-move prefix (the crash window between the disk rename and the baseline writes).
	newDir := filepath.Join(viewDir, "newdir")
	if err := os.MkdirAll(newDir, 0o755); err != nil {
		t.Fatalf("seed destination dir: %v", err)
	}
	if err := st.PutBaseline(7, "old/dir", store.Baseline{
		FileID: "fdir", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed tracked dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7,
		FileID: "fdir", Path: "newdir", IsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	calls := recordSyncDirFn(t, e, nil)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Fatalf("dead letters = %d, want 0", n)
	}
	want := []string{viewDir, oldParent}
	if len(*calls) != len(want) || (*calls)[0] != want[0] || (*calls)[1] != want[1] {
		t.Fatalf("directory-flush calls = %v, want %v", *calls, want)
	}
	b, err := st.GetBaseline(7, "newdir")
	if err != nil || b.FileID != "fdir" || b.State != store.StateSynced {
		t.Fatalf("newdir baseline = %+v (err %v), want synced fdir", b, err)
	}
}

// TestBestEffortSyncDirsSkipsMissingAndDeduplicates pins the two documented elisions of the
// replay re-flush: a directory an idempotent replay already removed (os.ErrNotExist) is silently
// skipped rather than Warned - the entry it would have flushed no longer exists, so there is
// nothing to report - while any other failure IS Warned, and a repeated directory is flushed
// once. Without the not-exist elision every cross-directory replay whose source parent the same
// replay emptied would log a spurious warning on each drain.
func TestBestEffortSyncDirsSkipsMissingAndDeduplicates(t *testing.T) {
	var logBuf bytes.Buffer
	e := New(newTestStore(t), &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess",
		slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	var calls []string
	e.syncDir = func(dir string) error {
		calls = append(calls, dir)
		switch filepath.Base(dir) {
		case "gone":
			return fmt.Errorf("flush %s: %w", dir, os.ErrNotExist)
		case "broken":
			return errors.New("simulated EIO")
		}
		return nil
	}

	root := t.TempDir()
	gone := filepath.Join(root, "gone")
	broken := filepath.Join(root, "broken")
	e.bestEffortSyncDirs(gone, broken, gone)

	if want := []string{gone, broken}; len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
		t.Fatalf("directory-flush calls = %v, want %v (the repeat must be deduplicated)", calls, want)
	}
	logged := logBuf.String()
	if strings.Contains(logged, gone) {
		t.Errorf("a vanished directory was warned about: %s", logged)
	}
	if !strings.Contains(logged, broken) {
		t.Errorf("a genuine flush failure was not warned about: %s", logged)
	}
}
