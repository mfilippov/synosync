package engine

// Observability tests for the Phase-11 dead-letter durable trace and the ENOSPC
// disk-full status classification: a dropped op must leave a durable store record
// (not just an Ack + log Warn), and a disk-full apply failure must latch DiskFull()
// without dead-lettering the op (it stays queued and retries).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDeadLetterLeavesDurableTrace confirms an unappliable op (a path that escapes the
// view root) is not only acked-and-dropped but also leaves a durable DeadLetter record
// so the silent skip is visible in `status` rather than only a one-shot log Warn.
func TestDeadLetterLeavesDurableTrace(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("pwned")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "../../escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop a traversing path, not wedge: %v", err)
	}

	// The op was dropped from the queue...
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want the dropped op gone (0)", n, err)
	}
	// ...but its dead-letter trace is durable and surfaces the data loss.
	n, err := st.DeadLetterLen()
	if err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	}
	if n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1 durable trace for the dropped op", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("ListDeadLetters returned %d records, want 1", len(dls))
	}
	rec := dls[0]
	if rec.ViewID != 7 || rec.Path != "../../escape.txt" || rec.Type != store.OpDownload {
		t.Errorf("dead-letter record = %+v, want view 7 / escape path / download", rec.DeadLetter)
	}
	if rec.Reason == "" {
		t.Errorf("dead-letter record has empty reason")
	}
	if rec.RecordedAt == 0 {
		t.Errorf("dead-letter record has zero RecordedAt")
	}
}

// TestDoomedRenameTraceDeferredUntilCleanupSucceeds is the double-trace regression: a
// dead-letter trace must be written only AFTER the op's baseline cleanup succeeds (and
// atomically with its Ack), so a failing-then-retried drop records exactly one trace
// rather than one per attempt. Here a symlink at a.txt dead-letters f9's download, dooming
// the rename a.txt→c.txt; the doomed rename's cleanup is injected to fail. Before the fix,
// the rename's trace was recorded BEFORE the cleanup ran, so the failed (and retriable)
// drop left a phantom trace - DeadLetterLen would count both the download and the rename
// (2). After the fix, the rename trace is deferred (cleanup failed), so only the download's
// trace survives (1) and a later retry records the rename trace exactly once.
func TestDoomedRenameTraceDeferredUntilCleanupSucceeds(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	// Fail the doomed rename's cleanup so its drop is incomplete and retriable.
	wantErr := errors.New("injected store failure")
	orig := dropDoomedRename
	dropDoomedRename = func(*Engine, store.QueuedOp) error { return wantErr }
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// op1: download f9 to a.txt → dead-lettered (symlink). op2: rename a.txt→c.txt (feeder
	// op1, doomed; its cleanup fails).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	// The injected cleanup error surfaces; the download is still dropped cleanly.
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("drainQueue err = %v, want injected %v", err, wantErr)
	}

	// Only the download's trace must exist - the rename's trace is deferred because its
	// cleanup failed. The pre-fix code recorded it eagerly, giving 2.
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("DeadLetterLen = %d, want exactly 1 (download only; rename trace deferred until its cleanup succeeds)", len(dls))
	}
	if dls[0].Type != store.OpDownload {
		t.Errorf("surviving trace type = %q, want the download (%q); a rename trace means it was recorded before cleanup succeeded", dls[0].Type, store.OpDownload)
	}
	// The doomed rename is still queued for a later drain (cleanup failed).
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the doomed rename still queued (1)", n, err)
	}
}

// TestDiskFullLatchesAndClears confirms an apply failure wrapping syscall.ENOSPC is NOT
// dead-lettered (it is transient - the op stays queued to retry) but does latch the
// engine's DiskFull() status flag, and that a later successful apply clears it.
func TestDiskFullLatchesAndClears(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f1": []byte("hello")},
		nodes:   map[string]actions.Node{"f1": {Hash: "h1", Size: 5}},
		dlErr:   fmt.Errorf("write body: %w", syscall.ENOSPC),
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The disk-full apply fails the drain, leaves the op queued (transient, not
	// dead-lettered), and latches the status flag.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC apply error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC op still queued for retry (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want ENOSPC NOT dead-lettered (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC apply, want it latched")
	}

	// Free the "disk": the next drain applies the op and clears the flag.
	dl.mu.Lock()
	dl.dlErr = nil
	dl.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "a.txt")); err != nil {
		t.Errorf("file not materialised after retry: %v", err)
	}
}

// TestDiskFullLatchesOnOutboundUpload confirms an ENOSPC failure on the OUTBOUND push path
// - which applyOp routes through an early return ahead of the inbound switch's old latch
// site - still latches DiskFull() and leaves the op queued (not dead-lettered), now that the
// latch is centralised in noteApplyResult. A later clean push clears the latch.
func TestDiskFullLatchesOnOutboundUpload(t *testing.T) {
	c := &pushClient{
		uploadNode: map[string]actions.Node{"a.txt": {FileID: "newf", Hash: "h1", MTime: 123}},
		uploadErr:  fmt.Errorf("write body: %w", syscall.ENOSPC),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: "h1", LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC outbound apply error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC outbound op still queued (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want ENOSPC NOT dead-lettered (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC outbound apply, want it latched")
	}

	// Free the "disk": the next drain pushes the upload and clears the flag.
	c.mu.Lock()
	c.uploadErr = nil
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful outbound apply, want it cleared")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained after retry: %d", n)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "newf" {
		t.Errorf("baseline = %+v, want synced newf after the retry", b)
	}
}

// TestRejectedPathCleanupENOSPCLatchesDiskFull reproduces the cleanup-ENOSPC latch for the
// rejected-path arm: a store-level cleanup failure on a full disk (ENOSPC) must latch DiskFull()
// rather than retrying the op silently. An unappliable (view-root-escaping) download routes into
// applyOp's rejected-path arm, whose cleanup dispatch (cleanupRejected) is injected to fail with
// ENOSPC. Before the fix the cleanup error was returned directly, bypassing the disk-full latch;
// now it is routed through noteApplyResult. A later clean apply clears the latch.
func TestRejectedPathCleanupENOSPCLatchesDiskFull(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f2": []byte("clean")},
		nodes:   map[string]actions.Node{"f2": {Hash: "h2", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// Inject an ENOSPC store failure into the rejected-path cleanup dispatch.
	orig := cleanupRejected
	cleanupRejected = func(*Engine, context.Context, store.Op) error { return fmt.Errorf("commit: %w", syscall.ENOSPC) }
	t.Cleanup(func() { cleanupRejected = orig })

	// A download whose path escapes the sync root fails errRejectedPath at apply time and routes
	// into the rejected-path arm, whose cleanup now fails with ENOSPC.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "../../escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	// The cleanup ENOSPC fails the drain, leaves the op queued (not dead-lettered), and latches.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC cleanup error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC op still queued (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want no trace written on a failed cleanup (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC cleanup failure, want it latched")
	}

	// Free the "disk": restore the real cleanup and add a clean download. The next drain
	// dead-letters the escape op (cleanup now succeeds) and applies the clean op, whose
	// successful apply clears the latch.
	cleanupRejected = orig
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "clean.txt", FileID: "f2", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue clean: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
}

// TestDoomedRenameCleanupENOSPCLatchesDiskFull reproduces the cleanup-ENOSPC latch for the
// doomed-rename arm: when a doomed rename's cleanup fails with a store-level ENOSPC, the goroutine
// must latch DiskFull() (through noteApplyResult) rather than only marking the op bad. Before the
// fix the dropDoomedRename error was recorded into firstErr and the op marked bad without touching
// the disk-full latch, so a disk-full failure there retried silently. A later clean apply clears
// the latch.
func TestDoomedRenameCleanupENOSPCLatchesDiskFull(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A pre-existing symlink at a.txt dead-letters f9's download, dooming the rename off a.txt.
	if err := os.Symlink(outside, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	// Inject an ENOSPC store failure into the doomed rename's cleanup.
	orig := dropDoomedRename
	dropDoomedRename = func(*Engine, store.QueuedOp) error { return fmt.Errorf("commit: %w", syscall.ENOSPC) }
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes"), "f2": []byte("clean")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}, "f2": {Hash: "h2", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// op1 download f9→a.txt dead-letters (symlink); op2 rename a.txt→c.txt is doomed and its
	// cleanup fails with ENOSPC.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC doomed-rename cleanup error")
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC doomed-rename cleanup failure, want it latched")
	}
	// The doomed rename stays queued for the next drain (its cleanup failed).
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the doomed rename still queued (1)", n, err)
	}

	// Free the "disk": a clean independent download applies and clears the latch.
	dropDoomedRename = orig
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "clean.txt", FileID: "f2", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue clean: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
}

// TestRejectedPathDeadLetterENOSPCLatchesDiskFull covers the ENOSPC latch for the rejected-path
// arm's deadLetterAndAck step: an unappliable (view-root-escaping) download's cleanup
// (cleanupRejected) succeeds, but the trace+ack store write that runs AFTER it fails with ENOSPC on
// a full disk. The failure must latch DiskFull() (through noteApplyResult) instead of returning
// bare and retrying silently. The ENOSPC is injected at the recordDeadLetterAndAck seam. A later
// clean apply clears it.
func TestRejectedPathDeadLetterENOSPCLatchesDiskFull(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	dl := &dlClient{
		content: map[string][]byte{"f2": []byte("clean")},
		nodes:   map[string]actions.Node{"f2": {Hash: "h2", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// The rejected-path cleanup succeeds by default; inject the ENOSPC into the deadLetterAndAck
	// store write that runs after it, so the rejected-path arm hits the failure at the trace+ack step.
	orig := recordDeadLetterAndAck
	recordDeadLetterAndAck = func(*Engine, store.DeadLetter, *store.Op, *store.TrackedBaseline, uint64) (uint64, error) {
		return 0, fmt.Errorf("commit: %w", syscall.ENOSPC)
	}
	t.Cleanup(func() { recordDeadLetterAndAck = orig })

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "../../escape.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC dead-letter error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC op still queued (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want no trace written on a failed ack (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC dead-letter failure, want it latched")
	}

	// Free the disk: restore the real store write and add a clean download. The next drain
	// dead-letters the escape op (ack now succeeds) and applies the clean op, whose successful
	// apply clears the latch.
	recordDeadLetterAndAck = orig
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "clean.txt", FileID: "f2", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue clean: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
}

// TestDoomedRenameDeadLetterENOSPCLatchesDiskFull covers the ENOSPC latch for the doomed-rename
// branch's deadLetterAndAck step (executor.go's doomed arm): the doomed rename's cleanup
// (dropDoomedRename) succeeds, but the trace+ack store write that follows fails with ENOSPC. The
// failure must latch DiskFull() (through noteApplyResult) rather than only recording firstErr +
// marking the op bad. The ENOSPC is injected at recordDeadLetterAndAck but scoped to the rename op
// so the feeding download still dead-letters (which is what dooms the rename in the first place). A
// later clean apply clears the latch.
func TestDoomedRenameDeadLetterENOSPCLatchesDiskFull(t *testing.T) {
	requireSymlinks(t)
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	outside := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A pre-existing symlink at a.txt dead-letters f9's download, dooming the rename off a.txt.
	if err := os.Symlink(outside, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Skipf("symlink unsupported here: %v", err)
	}

	// Inject ENOSPC into the dead-letter trace+ack, but only for the rename op: the feeding
	// download must dead-letter successfully so the rename is doomed and reaches the deadLetterAndAck
	// arm whose ENOSPC latch is under test.
	orig := recordDeadLetterAndAck
	recordDeadLetterAndAck = func(e *Engine, dl store.DeadLetter, comp *store.Op, track *store.TrackedBaseline, seq uint64) (uint64, error) {
		if dl.Type == store.OpRename {
			return 0, fmt.Errorf("commit: %w", syscall.ENOSPC)
		}
		return orig(e, dl, comp, track, seq)
	}
	t.Cleanup(func() { recordDeadLetterAndAck = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes"), "f2": []byte("clean")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}, "f2": {Hash: "h2", Size: 5}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	// op1 download f9→a.txt dead-letters (symlink); op2 rename a.txt→c.txt is doomed and its
	// trace+ack fails with ENOSPC.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC doomed-rename dead-letter error")
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC doomed-rename dead-letter failure, want it latched")
	}
	// The doomed rename stays queued for the next drain (its trace+ack failed).
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the doomed rename still queued (1)", n, err)
	}

	// Free the disk: a clean independent download applies and clears the latch.
	recordDeadLetterAndAck = orig
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "clean.txt", FileID: "f2", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue clean: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
}

// TestNodeGoneDownloadCleanupENOSPCLatchesDiskFull covers the ENOSPC latch for the
// errDownloadNodeGone arm: an inbound download whose node is positively gone server-side (a strict
// id-matched parent listing shows the leaf occupied by a different id - the overwrite-move race)
// drops its phantom pending baseline, but that store cleanup fails with ENOSPC on a full disk. The
// failure must latch DiskFull() (through noteApplyResult) instead of returning bare and retrying
// silently. The ENOSPC is injected at the dropStrandedDownloadBaseline seam. A later clean apply
// clears the latch.
func TestNodeGoneDownloadCleanupENOSPCLatchesDiskFull(t *testing.T) {
	c := &pushClient{
		downloadErr: actions.ErrPathError,
		listResult: map[string][]actions.Node{
			"": {{Name: "x.txt", FileID: "fmov"}}, // the overwriting node occupies the leaf → focc gone
		},
		// The corroborating probe of the path sees the same overwriting node (a different,
		// non-empty id), so the listing's verdict stands.
		queryNode: map[string]actions.Node{"x.txt": {Name: "x.txt", FileID: "fmov"}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	orig := dropStrandedDownloadBaseline
	dropStrandedDownloadBaseline = func(*Engine, uint64, string) error {
		return fmt.Errorf("commit: %w", syscall.ENOSPC)
	}
	t.Cleanup(func() { dropStrandedDownloadBaseline = orig })

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "x.txt", FileID: "focc",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if e.DiskFull() {
		t.Fatal("DiskFull() true before any apply")
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC node-gone cleanup error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC op still queued (1)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC node-gone cleanup failure, want it latched")
	}

	// Free the disk: restore the cleanup and let the download complete (downloadErr cleared), which
	// applies successfully and clears the latch.
	dropStrandedDownloadBaseline = orig
	c.mu.Lock()
	c.downloadErr = nil
	c.downloadContent = map[string][]byte{"focc": []byte("occ!")}
	c.downloadNode = map[string]actions.Node{"focc": {Hash: "hocc", Size: 4}}
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful apply, want it cleared")
	}
}

// TestOutboundReadOnlyDeadLetterRecordsOneTrace confirms the outbound dead-letter sites route
// through the atomic deadLetterAndAck: an outbound op for a view the
// account now holds read-only is marked StateConflict, dropped from the queue, and leaves exactly
// one durable trace - the trace and the Ack now committed in one transaction, so an Ack failure
// can no longer leave the op queued with a phantom trace to be re-recorded (and double-counted)
// on the next drain. The atomicity of the primitive itself is covered by store's
// TestRecordDeadLetterAndAckIsAtomic and engine's TestDoomedRenameTraceDeferredUntilCleanupSucceeds.
func TestOutboundReadOnlyDeadLetterRecordsOneTrace(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	// Downgrade the view to read-only after the outbound op was enqueued while writable.
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", Privilege: "ro"}); err != nil {
		t.Fatalf("PutView ro: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: "h1", LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop a read-only outbound op, not wedge: %v", err)
	}

	// The op is gone from the queue and left exactly one durable trace.
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want the dropped op gone (0)", n, err)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("DeadLetterLen = %d, want exactly 1 trace for the dropped outbound op", len(dls))
	}
	if dls[0].Type != store.OpUpload || dls[0].ViewID != 7 || dls[0].Path != "a.txt" {
		t.Errorf("trace = %+v, want upload / view 7 / a.txt", dls[0].DeadLetter)
	}
	// The baseline was marked conflict (the divergence the read-only side cannot resolve).
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
}
