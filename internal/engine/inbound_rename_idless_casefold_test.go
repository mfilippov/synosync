package engine

// These tests pin the case-variant id backfill: an id-less server move whose FromPath differs from
// the stored baseline path only by case (a case-only server rename before the move) must still
// adopt the tracked source's file id - and its stored spelling - on a case-insensitive matching
// platform, so the move applies as an ordinary id-carrying rename instead of dead-lettering into
// the permanent silent path divergence the exact-key backfill closed for the exact-key spelling. On
// a case-sensitive filesystem the fallback stays off: "foo.txt" and "Foo.txt" are genuinely
// distinct nodes there, so the case-variant move is left id-less (prior behaviour).

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestInboundIdlessCaseVariantMoveFoldAdoptsAndConverges is the primary case: on a
// case-insensitive platform an id-less move whose FromPath is a case-variant of the stored path
// ("foo.txt" for a node tracked as "Foo.txt") adopts the source id + spelling by fold key and
// converges - the file and its rich baseline relocate to the new path. Before the fix the exact
// key missed, the move stayed id-less, and it dead-lettered (the source stranded synced forever).
func TestInboundIdlessCaseVariantMoveFoldAdoptsAndConverges(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()

	origBytes := []byte("case-variant-move-source-content")
	origHash := md4.ContentHash(origBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Node F is synced under the stored spelling Foo.txt, with the file on disk there.
	if err := os.WriteFile(filepath.Join(viewDir, "Foo.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed Foo.txt: %v", err)
	}
	if err := st.PutBaseline(7, "Foo.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	// The server moves foo.txt->bar.txt but omits the file id, and reports the source under the
	// case-variant spelling foo.txt (a case-only rename preceded the move on the server).
	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "foo.txt", Path: "bar.txt"}}
	ops, _, err := e.reconcile(7, batch, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("ops = %+v, want a single OpRename", ops)
	}
	// The op adopted the tracked source's id AND its stored spelling.
	if ops[0].FileID != "F" || ops[0].Path != "Foo.txt" || ops[0].NewPath != "bar.txt" {
		t.Fatalf("rename op = %+v, want F Foo.txt->bar.txt", ops[0])
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The file and its rich baseline relocated to bar.txt; nothing lingers at the source.
	if got, err := os.ReadFile(filepath.Join(viewDir, "bar.txt")); err != nil || string(got) != string(origBytes) {
		t.Fatalf("bar.txt = %q (err %v), want moved content %q", got, err, origBytes)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "Foo.txt")); !os.IsNotExist(err) {
		t.Errorf("Foo.txt survived the move: %v", err)
	}
	b, err := st.GetBaseline(7, "bar.txt")
	if err != nil {
		t.Fatalf("GetBaseline(bar.txt): %v", err)
	}
	if b.FileID != "F" || b.State != store.StateSynced || b.LocalHash != origHash {
		t.Errorf("bar.txt baseline = %+v, want synced F/%s (rich facts carried across)", b, origHash)
	}
	if _, err := st.GetBaseline(7, "Foo.txt"); !isNotFound(err) {
		t.Errorf("Foo.txt baseline survived: err=%v", err)
	}
	if p, err := st.PathByID(7, "F"); err != nil || p != "bar.txt" {
		t.Errorf("PathByID(F) = %q (err %v), want bar.txt", p, err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0 (the move applied, not dropped)", n)
	}
}

// TestInboundIdlessCaseVariantMoveCaseSensitiveStaysIdless is the case-sensitive regression: on a
// case-sensitive filesystem the fold fallback is gated off, so a case-variant id-less move is left
// id-less (the exact key missed and "foo.txt" != "Foo.txt" are distinct nodes there). The op keeps
// FileID == "" - the prior behaviour - and the source baseline is untouched by reconcile.
func TestInboundIdlessCaseVariantMoveCaseSensitiveStaysIdless(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()

	origBytes := []byte("case-sensitive-untouched-content")
	origHash := md4.ContentHash(origBytes)

	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if err := st.PutBaseline(7, "Foo.txt", store.Baseline{
		FileID: "F", FileType: "file", ServerHash: origHash,
		LocalHash: origHash, LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "foo.txt", Path: "bar.txt"}}
	ops, _, err := e.reconcile(7, batch, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("ops = %+v, want a single OpRename", ops)
	}
	if ops[0].FileID != "" || ops[0].Path != "foo.txt" {
		t.Errorf("rename op = %+v, want id-less at foo.txt (fold fallback gated off)", ops[0])
	}
	// reconcile left the source baseline untouched (relocatePending's exact key missed too).
	b, err := st.GetBaseline(7, "Foo.txt")
	if err != nil || b.FileID != "F" || b.State != store.StateSynced {
		t.Errorf("Foo.txt baseline = %+v (err %v), want synced F preserved", b, err)
	}
}

// TestInboundIdlessCaseVariantMoveAmbiguousStaysIdless pins the ambiguity guard: two baselines
// folding to the same key (a case-only collision the case-fold collision guard would protect) must
// NOT let the backfill guess which one the id-less move meant, so it stays id-less.
func TestInboundIdlessCaseVariantMoveAmbiguousStaysIdless(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()

	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if err := st.PutBaseline(7, "Foo.txt", store.Baseline{
		FileID: "F", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed Foo.txt: %v", err)
	}
	if err := st.PutBaseline(7, "FOO.txt", store.Baseline{
		FileID: "G", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed FOO.txt: %v", err)
	}

	batch := []actions.Event{{SyncID: 100, Action: "move", FromPath: "foo.txt", Path: "bar.txt"}}
	ops, _, err := e.reconcile(7, batch, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRename {
		t.Fatalf("ops = %+v, want a single OpRename", ops)
	}
	if ops[0].FileID != "" {
		t.Errorf("rename op adopted an id under ambiguity: %+v", ops[0])
	}
}
