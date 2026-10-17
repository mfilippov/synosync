package engine

// These tests pin the id-less move backfill: a server move that OMITS the file id
// (move{FromPath:a, Path:b, FileID:""}) for a node we DO track at the source path must be
// applied as an ordinary move, not dead-lettered. Before the fix relocatePending's id gate
// ("F" != "") skipped the relocation, so the source baseline stayed StateSynced, and at apply
// time PathByID of the empty id read the node as untracked while classifyRenameSource saw a
// "recreated" foreign source - the srcRecreated && !nodeTracked arm dropped the op. The file
// then stayed at the old path locally (synced, agreement, so the scan never re-emitted it) while
// the server held it at the new one: a permanent silent path divergence. reconcile now backfills
// the moved id from the tracked source baseline so the whole apply path runs the id-carrying flow.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestInboundIdlessMoveTrackedFileConverges is the primary case: an id-less move of a
// tracked file must relocate the file and its baseline to the new path and converge. Before the
// fix it dead-lettered and left the file stranded at the old path.
func TestInboundIdlessMoveTrackedFileConverges(t *testing.T) {
	origBytes := []byte("tracked-file-synced-content-h1")
	origHash := md4.ContentHash(origBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Node F is synced at a.txt with the file on disk there.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// The server moves the node a.txt->b.txt but omits the file id.
	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, batch, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The file and its baseline relocated to b.txt; nothing lingers at a.txt.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(origBytes) {
		t.Fatalf("b.txt = %q (err %v), want moved content %q", got, err, origBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("a.txt survived the move: %v", err)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "F" || b.State != store.StateSynced || b.LocalHash != origHash {
		t.Errorf("b.txt baseline = %+v, want synced F/%s", b, origHash)
	}
	if _, err := st.GetBaseline(7, "a.txt"); !isNotFound(err) {
		t.Errorf("a.txt baseline survived: err=%v", err)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(F) = %q (err %v), want b.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0 (the move applied, not dropped)", n)
	}
}

// TestInboundIdlessMoveTrackedDirConverges is the directory variant: an id-less (and sparse:
// IsDir false) move of a tracked directory must relocate the directory AND its descendant
// baselines to the new prefix. Backfilling the id lets relocatePending recognise the dir and
// rebase its descendants, and the nodeTracked apply path skips the wrong-kind guard the sparse
// IsDir would otherwise trip.
func TestInboundIdlessMoveTrackedDirConverges(t *testing.T) {
	childBytes := []byte("dir-child-content")
	childHash := md4.ContentHash(childBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Directory d/ tracked (id D) with a tracked child d/c.txt (id C), both on disk.
	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "c.txt"), childBytes, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "D", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/c.txt", store.Baseline{
		FileID: "C", FileType: "file", ServerHash: childHash,
		LocalHash: childHash, LocalSize: int64(len(childBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}

	// Sparse id-less directory move d->e (IsDir omitted/false).
	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "d", Path: "e"}}
	if _, _, err := e.reconcile(7, batch, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The directory and its child moved to e/.
	if got, err := os.ReadFile(filepath.Join(viewDir, "e", "c.txt")); err != nil || string(got) != string(childBytes) {
		t.Fatalf("e/c.txt = %q (err %v), want moved child content %q", got, err, childBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("d/ survived the move: %v", err)
	}
	dirB, err := st.GetBaseline(7, "e")
	if err != nil || dirB.FileID != "D" || dirB.State != store.StateSynced {
		t.Errorf("e baseline = %+v (err %v), want synced D dir", dirB, err)
	}
	childB, err := st.GetBaseline(7, "e/c.txt")
	if err != nil || childB.FileID != "C" || childB.State != store.StateSynced || childB.LocalHash != childHash {
		t.Errorf("e/c.txt baseline = %+v (err %v), want synced C", childB, err)
	}
	if _, err := st.GetBaseline(7, "d/c.txt"); !isNotFound(err) {
		t.Errorf("d/c.txt baseline stranded under old prefix: err=%v", err)
	}
	if p, err := st.PathByID(7, "C"); err != nil || p != "e/c.txt" {
		t.Errorf("PathByID(C) = %q (err %v), want e/c.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0", n)
	}
}

// TestInboundIdlessMoveUntrackedSourceStillDeadLetters is the guard-preservation regression: an
// id-less move whose source is UNTRACKED (no baseline) and holds an on-disk entity of the wrong
// kind must still dead-letter - the backfill fires only for a tracked source, so the existing
// foreign-content / wrong-kind guard is untouched for genuinely-untracked id-less moves.
func TestInboundIdlessMoveUntrackedSourceStillDeadLetters(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// An untracked DIRECTORY sits on disk at a (no baseline), while the id-less move claims a
	// FILE (IsDir false). The wrong-kind guard must drop it rather than drag the dir across.
	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed untracked dir: %v", err)
	}

	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "a", Path: "b", IsDir: false}}
	if _, _, err := e.reconcile(7, batch, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The untracked directory stayed put; the op dead-lettered.
	if _, err := os.Stat(filepath.Join(viewDir, "a")); err != nil {
		t.Errorf("untracked a/ was moved/removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "b")); !os.IsNotExist(err) {
		t.Errorf("b materialised from a wrong-kind move: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("dead letters = %d, want 1 (wrong-kind guard preserved)", n)
	}
}

// TestInboundIdCarryingRecreatedSourceReplayUnaffected is the id-carrying regression: a replayed
// OpRename for a node already moved to the destination, with a DIFFERENT node recreated at the
// source, must stay a superseded-replay no-op - the recreated node is not dragged across. The
// id-less backfill never fires here (the op carries an id), so the classic behaviour is unchanged.
func TestInboundIdCarryingRecreatedSourceReplayUnaffected(t *testing.T) {
	fBytes := []byte("moved-node-F-content")
	fHash := md4.ContentHash(fBytes)
	gBytes := []byte("recreated-node-G-content")
	gHash := md4.ContentHash(gBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// F already moved to b.txt (synced); a different node G recreated at a.txt (synced).
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), fBytes, 0o644); err != nil {
		t.Fatalf("seed b.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), gBytes, 0o644); err != nil {
		t.Fatalf("seed a.txt: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: fHash, LocalHash: fHash,
		LocalSize: int64(len(fBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed b.txt baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "G", FileType: "file", ServerHash: gHash, LocalHash: gHash,
		LocalSize: int64(len(gBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed a.txt baseline: %v", err)
	}

	// A replayed id-carrying rename F: a.txt->b.txt (the move already applied).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "F", Path: "a.txt", NewPath: "b.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The replay is a no-op: G stays at a.txt, F stays at b.txt, both untouched.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(gBytes) {
		t.Fatalf("a.txt = %q (err %v), want recreated G content %q", got, err, gBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(fBytes) {
		t.Fatalf("b.txt = %q (err %v), want F content %q", got, err, fBytes)
	}
	if gb, err := st.GetBaseline(7, "a.txt"); err != nil || gb.FileID != "G" {
		t.Errorf("a.txt baseline = %+v (err %v), want G preserved", gb, err)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0", n)
	}
}
