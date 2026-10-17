package engine

// Never-lose: a server move that drags an UNTRACKED/foreign on-disk file onto a
// path records a baseline with an EMPTY LocalHash - the inbound-rename bare/promoted-pending write.
// (The BARE half of that pair is StateConflict now rather than StateSynced: see applyRename. The
// gates below are what protects the promoted-pending half, which still reaches StateSynced.) markSynced never leaves a file record hash-less, so that record is anomalous and
// carries no trustworthy divergence base. Before this treatment every never-lose gate
// (untrackedFilePresent, removeBaseFor/guardMkdirOccupant, guardRenameDest) read "synced without
// hash → in sync → server authority" and clobbered/deleted the user's bytes with no _Conflict copy.
// These tests reproduce the loss and confirm the file now survives whole as a _Conflict copy. A
// regression pins that the change fires ONLY on the anomalous empty-hash record: a legitimate rich
// (hash-carrying) baseline a clean tracked-node move produces still honours a server-authoritative
// delete directly.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestInboundRenameUntrackedSourceThenModifyPreserves is the plan's [move X a->b, modify X] one-batch
// scenario. X is untracked locally (an event-log gap) and an untracked user file sits at the move's
// source a.txt. The move drags that user file to b.txt and - via the promoted-pending destination
// baseline the modify wrote - stamps a StateSynced record with an empty LocalHash. The same-drain
// fresh-create modify-download must NOT read that as "in sync" and overwrite the user's file; the
// empty-hash gate now treats the present file as untracked user content and funnels it through
// keep-both, so the server's content wins the canonical path and the user's file survives as a
// _Conflict copy.
func TestInboundRenameUntrackedSourceThenModifyPreserves(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-file-dragged-from-a-then-modified-over")
	serverBytes := []byte("the-modified-server-content-h2")
	serverHash := md4.ContentHash(serverBytes)
	userHash := md4.ContentHash(userBytes)
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode:       map[string]actions.Node{"b.txt": {FileID: "fx", Hash: serverHash, MTime: 556, Size: int64(len(serverBytes))}},
		downloadContent: map[string][]byte{"fx": serverBytes},
		downloadNode:    map[string]actions.Node{"fx": {FileID: "fx", Hash: serverHash, MTime: 556, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: userHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// X is untracked (no baseline); the user's unrelated file sits at the move source.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	events := []actions.Event{
		{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a.txt", Path: "b.txt"},
		{SyncID: 6, Action: "modify", FileID: "fx", Path: "b.txt", Hash: serverHash, Size: int64(len(serverBytes)), MTime: 556},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The server's modified content wins the canonical path...
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("b.txt = %q (err %v), want the server content %q", got, err, serverBytes)
	}
	// ...and the user's file (dragged from a.txt) survives whole as the _Conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved user file %q", got, err, userBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("source a.txt survived the move: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundRenameUntrackedSourceThenRemovePreserves is the plan's [move X a->b] + later remove X
// scenario. The move (no accompanying modify) reaches applyRename's movedToDest branch and stamps a
// bare StateSynced record with an empty LocalHash and the user's file now at b.txt. A subsequent
// server delete captures BaseWasSynced=true with an empty BaseLocalHash; removeBaseFor must return an
// empty base (no trustworthy divergence base) so the file is preserved whole as a _Conflict copy
// rather than os.Removed. Previously removeBaseFor returned ok=false and the file was deleted with
// no copy.
func TestInboundRenameUntrackedSourceThenRemovePreserves(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-file-dragged-then-server-deleted")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	// Batch 1: the bare move of an untracked node drags the user's file to b.txt and records
	// it there with NEITHER hash. That record names the moved node but carries no evidence the
	// bytes under it were ever agreed with the server, so applyRename records it as
	// StateConflict rather than asserting an agreement no transfer established.
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, move, 0); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue move: %v", err)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.State != store.StateConflict || b.LocalHash != "" {
		t.Fatalf("b.txt baseline = %+v (err %v), want StateConflict with empty LocalHash (a bare record is not agreement)", b, err)
	}

	// Batch 2: a later server delete of the same node. The capture reads a non-synced record,
	// so it offers no divergence base at all - one step more direct than the empty-hash gate,
	// and the same protective answer.
	del := []actions.Event{{SyncID: 6, Action: "delete", FileID: "fx", Path: "b.txt"}}
	ops, _, err := e.reconcile(7, del, 5)
	if err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].BaseWasSynced || ops[0].BaseLocalHash != "" {
		t.Fatalf("delete ops = %+v, want one OpRemove with no captured base", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue delete: %v", err)
	}

	// The canonical path is gone (server delete applied to the vacated path) but the user's file
	// survives whole as the _Conflict copy.
	if _, err := os.Stat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt survived as the canonical path: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved user file %q", got, err, userBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s (never-lose preserve)", recs, copyRel)
	}
}

// TestInboundRenameOverwriteMoveOntoBareRecordPreserves is the capture-side scenario:
// the reconcile-time rename-destination capture was the LAST consumer of the bare-record anomaly
// (a StateSynced FILE record with an empty LocalHash) still reading it as "in sync → server
// authority". (1) A server move of an UNTRACKED node drags an untracked user file onto b.txt and
// stamps the bare {fx, Synced, LocalHash:""} record (the documented producer). (2) A
// later server overwrite-move of a TRACKED node F c->b captures that pre-batch bare record: the
// old arm yielded neither a base nor DestUntracked, and because relocatePending then EVICTS the
// bare record (the live destination baseline becomes OUR pending record, so guardRenameDest's
// foreign-only live recheck cannot fire), the apply ran fsutil.Rename with no basis and clobbered
// the user's file with no _Conflict copy. The fixed capture treats the hash-less synced record as
// untracked (DestUntracked=true, mirroring removeBaseFor), so the user's file survives whole.
func TestInboundRenameOverwriteMoveOntoBareRecordPreserves(t *testing.T) {
	ts := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	const host = "laptop"

	userBytes := []byte("user-file-dragged-to-b-then-overwrite-moved-over")
	movedBytes := []byte("tracked-node-f-moved-onto-b")
	copyRel := conflictCopyName("b.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The user's untracked file sits at a.txt; node X (fx) is untracked locally.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), userBytes, 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	// Batch 1: the bare move of untracked X drags the user's file to b.txt and records it there
	// with NEITHER hash - a record naming the node with no evidence the bytes under it were
	// ever agreed, so it is StateConflict rather than a claimed agreement.
	move1 := []actions.Event{{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, move1, 0); err != nil {
		t.Fatalf("reconcile move 1: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue move 1: %v", err)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.State != store.StateConflict || b.LocalHash != "" {
		t.Fatalf("b.txt baseline = %+v (err %v), want StateConflict with empty LocalHash (a bare record is not agreement)", b, err)
	}

	// The tracked node F (ff) lives at c.txt with a rich synced baseline.
	if err := os.WriteFile(filepath.Join(viewDir, "c.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "c.txt", store.Baseline{
		FileID: "ff", FileType: "file", ServerHash: md4.ContentHash(movedBytes),
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}

	// Batch 2: the server overwrite-moves tracked F c->b. The capture must read the bare
	// synced record as untracked (no trustworthy base).
	move2 := []actions.Event{{SyncID: 6, Action: "move", FileID: "ff", FromPath: "c.txt", Path: "b.txt"}}
	ops, _, err := e.reconcile(7, move2, 5)
	if err != nil {
		t.Fatalf("reconcile move 2: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename || !ops[0].DestUntracked || ops[0].DestBaseLocalHash != "" {
		t.Fatalf("move ops = %+v, want one OpRename with DestUntracked=true (bare synced record has no base)", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue move 2: %v", err)
	}

	// The moved tracked node wins the canonical path...
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	// ...and the user's file survives whole as the _Conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(userBytes) {
		t.Fatalf("conflict copy = %q (err %v), want the preserved user file %q", got, err, userBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "c.txt")); !os.IsNotExist(err) {
		t.Errorf("source c.txt survived the move: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "b.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one b.txt -> %s (never-lose preserve)", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundRenameOverwriteMoveRichForeignBaseHonoured is the capture-arm regression: a synced
// foreign destination record with a NON-empty LocalHash is still captured as the divergence base,
// and an on-disk file matching that base is overwritten directly under server authority - no
// spurious _Conflict copy. This pins that the untracked treatment fires ONLY on the
// hash-less anomaly.
func TestInboundRenameOverwriteMoveRichForeignBaseHonoured(t *testing.T) {
	occBytes := []byte("foreign-occupant-last-synced-content")
	movedBytes := []byte("tracked-node-f-moved-onto-b")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))

	// The foreign occupant at b.txt: rich synced baseline whose hash matches the on-disk file.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), occBytes, 0o644); err != nil {
		t.Fatalf("seed occupant file: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "focc", FileType: "file", ServerHash: md4.ContentHash(occBytes),
		LocalHash: md4.ContentHash(occBytes), LocalSize: int64(len(occBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed occupant baseline: %v", err)
	}
	// The tracked node F at c.txt.
	if err := os.WriteFile(filepath.Join(viewDir, "c.txt"), movedBytes, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "c.txt", store.Baseline{
		FileID: "ff", FileType: "file", ServerHash: md4.ContentHash(movedBytes),
		LocalHash: md4.ContentHash(movedBytes), LocalSize: int64(len(movedBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed moved baseline: %v", err)
	}

	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "ff", FromPath: "c.txt", Path: "b.txt"}}
	ops, _, err := e.reconcile(7, move, 0)
	if err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename || ops[0].DestUntracked ||
		ops[0].DestBaseLocalHash != md4.ContentHash(occBytes) {
		t.Fatalf("move ops = %+v, want one OpRename carrying the rich foreign base (DestUntracked=false)", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The matching occupant is overwritten directly under server authority - no copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil || string(got) != string(movedBytes) {
		t.Fatalf("b.txt = %q (err %v), want the moved node's content %q", got, err, movedBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (matching rich base honours server authority)", recs)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "b.txt" {
		t.Errorf("view dir entries = %v, want only b.txt (no spurious copy)", entries)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundRenameTrackedNodeMoveThenDeleteHonoured is the regression guard: the never-lose change
// fires ONLY on the anomalous empty-hash record. A clean overwrite-move of a TRACKED node carries its
// rich (hash-carrying) baseline to the destination, so a subsequent server delete whose on-disk
// content still matches that hash honours the delete directly - no spurious _Conflict copy. This pins
// that removeBaseFor's empty-base preserve did not weaken the legitimate server-authoritative delete.
func TestInboundRenameTrackedNodeMoveThenDeleteHonoured(t *testing.T) {
	origBytes := []byte("tracked-node-original-synced-content")

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: md4.ContentHash(origBytes),
		LocalHash: md4.ContentHash(origBytes), LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// Batch 1: a clean move of the tracked node to b.txt - its rich baseline travels.
	move := []actions.Event{{SyncID: 5, Action: "move", FileID: "fa", FromPath: "a.txt", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, move, 0); err != nil {
		t.Fatalf("reconcile move: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue move: %v", err)
	}
	b, err := st.GetBaseline(7, "b.txt")
	if err != nil || b.State != store.StateSynced || b.LocalHash != md4.ContentHash(origBytes) {
		t.Fatalf("b.txt baseline = %+v (err %v), want the rich moved record (non-empty LocalHash)", b, err)
	}

	// Batch 2: a server delete of the still-in-sync node is honoured directly.
	del := []actions.Event{{SyncID: 6, Action: "delete", FileID: "fa", Path: "b.txt"}}
	if _, _, err := e.reconcile(7, del, 5); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue delete: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt survived an in-sync delete: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want 0 (rich in-sync record honours the server delete)", recs)
	}
	if _, err := st.GetBaseline(7, "b.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("b.txt baseline survived the delete: err=%v, want ErrNotFound", err)
	}
}
