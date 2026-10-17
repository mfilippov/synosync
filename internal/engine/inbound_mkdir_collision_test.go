package engine

// These tests pin the inbound half of the mkdir occupant-gate fix: a server dir-create landing
// on the path of a TRACKED regular file. OpMkdir had no capture gate, so markUpsertPending
// overwrote the file's baseline with the pending dir record, the mkdir dead-lettered on the
// on-disk occupant (ENOTDIR), the dead-letter cleanup dropped the baseline, and the next scan
// re-uploaded the now-untracked file into the same collision - a silent re-upload cycle, with a
// locally EDITED file's content never preserved. The fix mirrors the OpRemove capture precedent:
// reconcile captures BaseWasSynced/BaseLocalHash/BaseIsDir for OpMkdir from the pre-batch
// snapshot (with the cross-batch inherited fallback), and applyMkdir's occupant gate
// (guardMkdirOccupant) preserves a diverged/untracked file as a _Conflict copy - or clears an
// in-sync one under server authority - before the directory lands.

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

// TestInboundMkdirOverEditedFilePreserves is the plan's headline scenario: the tracked file
// carries a cheap-scan-missed local edit (on-disk content diverges from the last-synced hash,
// no queued upload). The dir-create must preserve the edit as a _Conflict copy, then land the
// directory cleanly - before the fix the mkdir dead-lettered and the edit entered the
// re-upload cycle (verified by stashing the fix).
func TestInboundMkdirOverEditedFilePreserves(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 11, 15, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("edited-locally"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	ops, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", IsDir: true, FileID: "d1", Path: "notes.txt", MTime: 700},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpMkdir {
		t.Fatalf("ops = %+v, want one mkdir", ops)
	}
	if !ops[0].BaseWasSynced || ops[0].BaseLocalHash != cleanHash || ops[0].BaseIsDir {
		t.Errorf("mkdir capture = {BaseWasSynced:%v BaseLocalHash:%q BaseIsDir:%v}, want {true %q false}",
			ops[0].BaseWasSynced, ops[0].BaseLocalHash, ops[0].BaseIsDir, cleanHash)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, serr := os.Lstat(filepath.Join(viewDir, "notes.txt"))
	if serr != nil || !fi.IsDir() {
		t.Fatalf("canonical path = %v (err %v), want the server directory landed", fi, serr)
	}
	copies := findConflictEntries(t, viewDir)
	if len(copies) != 1 {
		t.Fatalf("_Conflict entries = %v, want exactly one preserved copy", copies)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, copies[0]))
	if rerr != nil || string(got) != "edited-locally" {
		t.Errorf("preserved copy = %q (err %v), want the local edit kept whole", got, rerr)
	}
	conflicts, cerr := st.ListConflicts(7)
	if cerr != nil || len(conflicts) != 1 {
		t.Errorf("ListConflicts = %v (err %v), want one durable record", conflicts, cerr)
	}
	b, gerr := st.GetBaseline(7, "notes.txt")
	if gerr != nil {
		t.Fatalf("GetBaseline: %v", gerr)
	}
	if b.State != store.StateSynced || b.FileID != "d1" || b.FileType != "dir" {
		t.Errorf("baseline = %+v, want synced dir d1", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue = %d ops, want empty (no re-upload cycle)", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("DeadLetterLen = %d, want 0 (the collision resolves, not drops)", n)
	}
}

// TestInboundMkdirOverCleanSyncedFileServerAuthority pins the server-authority branch: the
// on-disk file still matches its last-synced hash, so the server's replacement clears it with
// NO _Conflict copy and the directory lands - mirroring the remove gate's match-honours-server
// outcome.
func TestInboundMkdirOverCleanSyncedFileServerAuthority(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 11, 15, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), cleanBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", IsDir: true, FileID: "d1", Path: "notes.txt", MTime: 700},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, serr := os.Lstat(filepath.Join(viewDir, "notes.txt"))
	if serr != nil || !fi.IsDir() {
		t.Fatalf("canonical path = %v (err %v), want the server directory landed", fi, serr)
	}
	if copies := findConflictEntries(t, viewDir); len(copies) != 0 {
		t.Errorf("_Conflict entries = %v, want none for an in-sync file", copies)
	}
	if conflicts, cerr := st.ListConflicts(7); cerr != nil || len(conflicts) != 0 {
		t.Errorf("ListConflicts = %v (err %v), want none", conflicts, cerr)
	}
	if b, gerr := st.GetBaseline(7, "notes.txt"); gerr != nil || b.State != store.StateSynced || b.FileType != "dir" {
		t.Errorf("baseline = %+v (err %v), want synced dir", b, gerr)
	}
}

// TestInboundMkdirOverSyncedEmptyHashFilePreserves pins the empty-hash never-lose
// branch of the mkdir occupant guard, which the clean-synced test above (a NON-empty hash) leaves
// unexercised. A server dir-create lands on a path holding a SYNCED FILE baseline with an EMPTY
// LocalHash - the inbound-rename bare/promoted-pending write anomaly (markSynced never leaves a real
// file record hash-less). removeBaseFor returns an empty base (no trustworthy divergence base), so
// the regular file must be preserved WHOLE as a _Conflict copy rather than removed under a false
// "synced-without-hash → server authority" read; the directory still lands.
func TestInboundMkdirOverSyncedEmptyHashFilePreserves(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 11, 15, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("anomalous-empty-hash-content"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// A SYNCED FILE record with NO recorded LocalHash (the anomaly the empty-hash gate targets).
	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced, // LocalHash intentionally empty
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", IsDir: true, FileID: "d1", Path: "notes.txt", MTime: 700},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, serr := os.Lstat(filepath.Join(viewDir, "notes.txt"))
	if serr != nil || !fi.IsDir() {
		t.Fatalf("canonical path = %v (err %v), want the server directory landed", fi, serr)
	}
	copies := findConflictEntries(t, viewDir)
	if len(copies) != 1 {
		t.Fatalf("_Conflict entries = %v, want the anomalous-record file preserved whole", copies)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, copies[0]))
	if rerr != nil || string(got) != "anomalous-empty-hash-content" {
		t.Errorf("preserved copy = %q (err %v), want the content kept whole", got, rerr)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("DeadLetterLen = %d, want 0", n)
	}
}

// TestInboundMkdirOverUntrackedFilePreserves pins the never-lose default: an UNTRACKED local
// file at the dir-create's path (no baseline at all, so no divergence base exists) is
// preserved whole rather than destroyed or dead-lettered - the same doctrine as the download
// fresh-create guard and removeBaseFor's untracked branch.
func TestInboundMkdirOverUntrackedFilePreserves(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 11, 15, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("user-content"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "create", IsDir: true, FileID: "d1", Path: "notes.txt", MTime: 700},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, serr := os.Lstat(filepath.Join(viewDir, "notes.txt"))
	if serr != nil || !fi.IsDir() {
		t.Fatalf("canonical path = %v (err %v), want the server directory landed", fi, serr)
	}
	copies := findConflictEntries(t, viewDir)
	if len(copies) != 1 {
		t.Fatalf("_Conflict entries = %v, want the untracked file preserved", copies)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, copies[0]))
	if rerr != nil || string(got) != "user-content" {
		t.Errorf("preserved copy = %q (err %v), want the untracked content kept whole", got, rerr)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("DeadLetterLen = %d, want 0", n)
	}
}

// TestInboundMkdirCrossBatchInheritedBase pins the inherited fallback (reconcile-level only):
// a [modify f] in batch 1 flips f's live baseline StatePending, so batch 2's dir-create at f
// cannot see the synced base in its snapshot - it must inherit it from the queued OpDownload
// via the tracked path's fold key, exactly as the remove gate does, or an id-keyed-only seed
// would leave BaseWasSynced false and the apply gate unable to honour server authority.
func TestInboundMkdirCrossBatchInheritedBase(t *testing.T) {
	cleanBytes := []byte("clean-synced-content")
	cleanHash := md4.ContentHash(cleanBytes)

	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: cleanHash,
		LocalHash: cleanHash, LocalSize: int64(len(cleanBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 5, Action: "modify", FileID: "f1", Path: "notes.txt", Hash: "newhash", Size: 3},
	}, 0); err != nil {
		t.Fatalf("reconcile batch1: %v", err)
	}

	ops2, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 7, Action: "create", IsDir: true, FileID: "d1", Path: "notes.txt", MTime: 700},
	}, 5)
	if err != nil {
		t.Fatalf("reconcile batch2: %v", err)
	}
	if len(ops2) != 1 || ops2[0].Type != store.OpMkdir {
		t.Fatalf("batch2 ops = %+v, want one mkdir", ops2)
	}
	if !ops2[0].BaseWasSynced || ops2[0].BaseLocalHash != cleanHash || ops2[0].BaseIsDir {
		t.Errorf("inherited mkdir capture = {BaseWasSynced:%v BaseLocalHash:%q BaseIsDir:%v}, want {true %q false}",
			ops2[0].BaseWasSynced, ops2[0].BaseLocalHash, ops2[0].BaseIsDir, cleanHash)
	}
}
