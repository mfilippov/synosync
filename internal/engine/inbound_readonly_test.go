package engine

// These tests cover an inbound apply whose destination is marked
// read-only. On Windows MoveFileEx(MOVEFILE_REPLACE_EXISTING) fails such a move with
// ERROR_ACCESS_DENIED, which the executor classifies as transient ACL noise - but the
// attribute never clears itself, so the op was deterministically inapplicable while every
// drain first streamed the whole body into a fresh staging temp: O(size) of NAS traffic per
// drain cycle, forever. The replace-promote (and the inbound rename's fsutil.Rename) now
// clears the attribute and retries once, behind the divergence/dest guards that already
// proved the destination may be replaced.
//
// Both tests run on every platform. On Unix a mode-0444 destination is replaced by any
// rename - the permission comes from the parent directory - so the Unix arm pins that the
// behaviour there is unchanged.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// markReadOnly clears the owner-write bit on path, which on Windows is exactly
// FILE_ATTRIBUTE_READONLY (os.Chmod maps the 0200 bit to the attribute). It skips the test
// when the host does not honour it, so the fixture never asserts against a filesystem that
// silently dropped the mark (the project's probe-not-GOOS convention).
func markReadOnly(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("mark %q read-only: %v", path, err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat %q: %v", path, err)
	}
	if fi.Mode().Perm()&0o200 != 0 {
		t.Skipf("host does not honour a read-only file mode on %q; nothing to pin", path)
	}
}

// TestInboundDownloadReplacesReadOnlyTarget is the red test: a tracked modify whose local
// file is marked read-only and whose content still equals the last-agreed base (the
// divergence gate passes, so this is a plain replacing promote). It must apply in ONE drain,
// with a single transfer - before the fix the promote-rename failed with ERROR_ACCESS_DENIED
// on Windows and the op stayed queued, re-downloading the whole body on every drain.
func TestInboundDownloadReplacesReadOnlyTarget(t *testing.T) {
	origBytes := []byte("last-synced-content")
	serverBytes := []byte("remote-modify-version")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode: map[string]actions.Node{
			"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	abs := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(abs, origBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	markReadOnly(t, abs)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "srv1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
		BaseLocalHash: origHash,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain (the read-only destination wedged the promote)", n)
	}
	if got, err := os.ReadFile(abs); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("local content = %q (err %v), want the server version %q", got, err, serverBytes)
	}
	if c.downloadCalls != 1 {
		t.Errorf("Download calls = %d, want exactly 1 (a retried op re-streams the whole body)", c.downloadCalls)
	}
	// No keep-both funnel: the content equalled the base, so this is an ordinary overwrite.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want none", recs)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != serverHash {
		t.Errorf("baseline = %+v, want synced with the server hash %s", b, serverHash)
	}
	if dl, derr := st.ListDeadLetters(); derr != nil {
		t.Fatalf("ListDeadLetters: %v", derr)
	} else if len(dl) != 0 {
		t.Errorf("dead letters = %+v, want none", dl)
	}
}

// TestInboundRenameOverReadOnlyDest is the incident arc's mirror: a server move onto a
// destination that holds a tracked, non-diverged file marked read-only. fsutil.Rename hits
// the same ERROR_ACCESS_DENIED, without the O(size) transfer cost but with the same
// forever-transient wedge (every same-path successor serialised behind it).
func TestInboundRenameOverReadOnlyDest(t *testing.T) {
	destBytes := []byte("destination-last-synced")
	movedBytes := []byte("moved-node-content")
	destHash := md4.ContentHash(destBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC))

	dst := filepath.Join(viewDir, "x.txt")
	if err := os.WriteFile(dst, destBytes, 0o644); err != nil {
		t.Fatalf("seed destination file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "focc", FileType: "file", ServerHash: destHash,
		LocalHash: destHash, LocalSize: int64(len(destBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed destination baseline: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: "hm",
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}
	markReadOnly(t, dst)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "x.txt",
		DestBaseLocalHash: destHash,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain (the read-only destination wedged the move)", n)
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("destination = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("source still present after the move, err = %v", err)
	}
}
