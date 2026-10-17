package engine

// The doom-propagation direction gate. A rename is doomed -
// dead-lettered, with the baseline reconcile relocated for it dropped - when the op that
// was to place its node at the source died without placing it. Only an INBOUND op places a
// node: it materialises the server's bytes on disk. An OUTBOUND op of the same node PUSHES
// a change the user already made locally, so the path it names is on disk before it runs
// and stays there when it fails. Treating a dead outbound op as a feeder therefore dropped
// a perfectly applicable inbound rename over an unrelated push failure - and with it the
// relocated baseline, which the next scan then re-detects as a local create.
//
// Both halves of the carry are gated: the in-window feeder scan and the cross-window
// deadOutputs mark. A read-only view supplies a deterministic outbound death (an op
// enqueued while the view was writable can never be pushed once the privilege downgrades,
// so it is dead-lettered), with the inbound side unaffected - inbound applies write local
// disk, which read-only says nothing about.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedDoomDirectionView builds the shared fixture: a read-only view whose on-disk b.txt
// holds tracked node f9, ready for an inbound rename b.txt -> c.txt.
func seedDoomDirectionView(t *testing.T) (*store.Store, string, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", Privilege: "ro"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	body := []byte("f9-bytes")
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), body, 0o644); err != nil {
		t.Fatalf("seed b.txt: %v", err)
	}
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "f9", FileType: "file", ServerHash: md4.ContentHash(body),
		LocalHash: md4.ContentHash(body), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	return st, root, viewDir
}

// checkRenameApplied asserts the inbound rename actually ran: the node moved to c.txt with
// its bytes, the queue drained, and exactly the outbound op was dead-lettered.
func checkRenameApplied(t *testing.T, st *store.Store, viewDir string) {
	t.Helper()
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err != nil {
		t.Fatalf("c.txt missing: the inbound rename was doomed by a dead OUTBOUND op: %v", err)
	} else if string(got) != "f9-bytes" {
		t.Errorf("c.txt = %q, want %q", got, "f9-bytes")
	}
	if _, err := os.Stat(filepath.Join(viewDir, "b.txt")); !os.IsNotExist(err) {
		t.Errorf("b.txt still present after the move: err=%v", err)
	}
	if b, err := st.GetBaseline(7, "c.txt"); err != nil {
		t.Errorf("c.txt baseline missing: %v", err)
	} else if b.FileID != "f9" {
		t.Errorf("c.txt baseline = %+v, want f9", b)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f9) = %q (err %v), want c.txt", p, err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	// Only the outbound push was dropped; the rename applied rather than dead-lettering.
	if n, err := st.DeadLetterLen(); err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	} else if n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 (only the un-pushable outbound op)", n)
	}
}

// TestDeadOutboundOpDoesNotDoomInWindowRename covers the in-window half: the feeder scan
// must skip outbound producers. Both ops share file id f9 and the outbound op's output key
// (its rename destination b.txt) is the inbound rename's source, so before the gate the
// inbound rename found it as its feeder, saw it dead, and dropped itself.
func TestDeadOutboundOpDoesNotDoomInWindowRename(t *testing.T) {
	st, root, viewDir := seedDoomDirectionView(t)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)

	// The local move a.txt -> b.txt the user already performed, queued for the server and
	// no longer pushable (the view went read-only), followed by the server's own move of
	// the same node off b.txt.
	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %s: %v", op.Direction, err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	checkRenameApplied(t, st, viewDir)
}

// TestDeadOutboundOpDoesNotDoomLaterWindowRename covers the cross-window half: only a dead
// INBOUND op may arm the deadOutputs mark that a later drain window consults. With one op
// per window the two land in separate windows, so the in-window feeder scan cannot see the
// outbound op at all and the mark is the only carrier.
func TestDeadOutboundOpDoesNotDoomLaterWindowRename(t *testing.T) {
	st, root, viewDir := seedDoomDirectionView(t)
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	e.limits.DrainBatch = 1

	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %s: %v", op.Direction, err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	checkRenameApplied(t, st, viewDir)
}

// TestDeadInboundOpStillDoomsRename is the control: the gate must narrow the doom to the
// outbound direction only, never disarm it. A dead INBOUND producer of the same node at the
// rename's source still dooms the rename, so the user's untracked file sitting there is not
// hauled across (the doom guarantee).
func TestDeadInboundOpStillDoomsRename(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// A foreign node tracked at hop 1's source, so hop 1 (for the untracked f9) drops.
	if err := os.WriteFile(filepath.Join(viewDir, "x.txt"), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	if err := st.PutBaseline(7, "x.txt", store.Baseline{
		FileID: "fb", FileType: "file", ServerHash: "hb", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed foreign baseline: %v", err)
	}
	// The user's own untracked file at hop 2's source.
	if err := os.WriteFile(filepath.Join(viewDir, "b.txt"), []byte("user"), 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	for _, op := range []store.Op{
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "x.txt", NewPath: "b.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "b.txt", NewPath: "c.txt", FileID: "f9"},
	} {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "b.txt")); err != nil {
		t.Errorf("user b.txt was moved/removed: %v", err)
	} else if string(got) != "user" {
		t.Errorf("b.txt = %q, want %q", got, "user")
	}
	// The doom holds: whatever ends up at c.txt is never the user's file. What DOES end up there
	// is the server's own copy of f9, re-requested by the dropped rename (finding H2) - the
	// dlClient scripts no content for it, so the download writes an empty file. The distinction
	// is the whole point: the user's bytes stay at b.txt, the server's node is fetched fresh.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err == nil && string(got) == "user" {
		t.Errorf("c.txt = %q: the doom was disarmed and the user's file hauled across", got)
	}
	if p, err := st.PathByID(7, "f9"); err != nil || p != "c.txt" {
		t.Errorf("PathByID(f9) = %q (err %v), want the re-requested node tracked at c.txt", p, err)
	}
}
