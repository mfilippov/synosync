package engine

// A root-level name beginning with "id:" aliases the wire's
// id-addressing form (id:<file_id>), which shares the single "path" parameter with
// view-relative addressing (rename/remove/query_node/list). Such a path must never be
// originated by the local scanner nor materialised from a server event, or a later
// outbound Remove(viewID, "id:12345")/Rename could round-trip the literal name into an
// id-addressed mutation of a FOREIGN node. These tests pin the scanner-side protect and
// its inbound mirror; a nested "a/id:foo" (prefix mid-parameter) is unaffected.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestScanViewProtectsRootIDAliasName verifies the scanner does not surface a root file or
// directory whose name begins with "id:" (fold-insensitive), records it in the unsurfaced
// set (so reconcile treats it as still-present, never a create/delete/move source), and
// leaves a nested "sub/id:foo" surfaced as ordinary content.
func TestScanViewProtectsRootIDAliasName(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a ':' cannot appear in a Windows filename; the protect is a no-op there")
	}
	e, viewID, viewDir := newScanEngine(t)

	// Root id-alias file and directory (with a child), plus fold variants and a normal file.
	if err := os.WriteFile(filepath.Join(viewDir, "id:12345"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write id:12345: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "ID:99"), []byte("y"), 0o644); err != nil {
		t.Fatalf("write ID:99: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "id:dir"), 0o755); err != nil {
		t.Fatalf("mkdir id:dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "id:dir", "child.txt"), []byte("z"), 0o644); err != nil {
		t.Fatalf("write id:dir/child.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write keep.txt: %v", err)
	}
	// A NESTED id: name is not exposed (the prefix is mid-parameter, not at the start).
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "sub", "id:foo"), []byte("n"), 0o644); err != nil {
		t.Fatalf("write sub/id:foo: %v", err)
	}

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}

	for _, protected := range []string{"id:12345", "ID:99", "id:dir"} {
		if _, ok := findEntry(entries, protected); ok {
			t.Errorf("root id-alias %q was surfaced as a scan entry; must be protected", protected)
		}
		if !unreadable[protected] {
			t.Errorf("root id-alias %q not recorded in the unsurfaced set; reconcile could diff it as a deletion", protected)
		}
	}
	// The protected directory's child must not be walked (SkipDir) nor surfaced.
	if _, ok := findEntry(entries, "id:dir/child.txt"); ok {
		t.Error("child under a protected root id-alias dir was surfaced")
	}
	// Ordinary and nested-id names still sync.
	if _, ok := findEntry(entries, "keep.txt"); !ok {
		t.Error("ordinary root file keep.txt was not surfaced")
	}
	if _, ok := findEntry(entries, "sub/id:foo"); !ok {
		t.Error("nested sub/id:foo must be surfaced normally (prefix is mid-parameter)")
	}
	if unreadable["sub/id:foo"] {
		t.Error("nested sub/id:foo must not be protected")
	}
}

// TestReconcileSkipsRootIDAliasEvent verifies the inbound mirror: a server event naming a
// root "id:*" path (any action, either move endpoint) is skipped without writing a
// baseline or emitting an op, while the cursor still advances; a nested "a/id:foo" and an
// ordinary path are processed as usual.
func TestReconcileSkipsRootIDAliasEvent(t *testing.T) {
	e, st := newReconcileEngine(t, "self")

	// A tracked synced node whose server move would relocate it onto a root id-alias
	// destination: without the fix this emits an OpRename and flips the baseline to the
	// aliased destination path; with the fix the whole event is skipped, leaving the
	// source baseline synced in place.
	if err := st.PutBaseline(1, "clean", store.Baseline{
		FileID: "f2", FileType: "file", ServerHash: "sh", LocalHash: "lh",
		LocalSize: 3, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline clean: %v", err)
	}

	events := []actions.Event{
		{SyncID: 3, Action: "create", FileID: "f1", Path: "id:12345", Hash: "h1", Size: 3, MTime: 100},
		{SyncID: 4, Action: "move", FileID: "f2", FromPath: "clean", Path: "ID:77"},
		{SyncID: 5, Action: "delete", FileID: "f3", Path: "id:evil"},
		{SyncID: 6, Action: "create", FileID: "f4", Path: "a/id:foo", Hash: "h4", Size: 2, MTime: 200},
		{SyncID: 8, Action: "create", FileID: "f5", Path: "normal.txt", Hash: "h5", Size: 4, MTime: 300},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 8 {
		t.Errorf("cursor = %d, want 8 (skipped events still advance it)", cursor)
	}

	// No op may reference a root id-alias on EITHER endpoint (a rename's destination is
	// op.NewPath, its source op.Path).
	for _, op := range ops {
		if isRootIDAlias(op.Path) || isRootIDAlias(op.NewPath) {
			t.Errorf("op %+v references a root id-alias path; the event must have been skipped", op)
		}
	}
	// Only the nested-id and ordinary creates survive as ops.
	gotPaths := map[string]bool{}
	for _, op := range ops {
		gotPaths[op.Path] = true
	}
	// The nested survivor carries a colon, which is a reserved character in a
	// Windows path component: there containable() (filepath.IsLocal) rejects
	// "a/id:foo" before the id-alias check ever runs, so the event is skipped for
	// that unrelated - and equally safe - reason and no such file could exist on
	// disk anyway. The protect under test is the ROOT-level alias, so assert the
	// nested survivor against the host's own path rules.
	if containable("a/id:foo") {
		if !gotPaths["a/id:foo"] {
			t.Error("nested a/id:foo event was skipped; only root id: names must be protected")
		}
	} else if gotPaths["a/id:foo"] {
		t.Error("nested a/id:foo produced an op although the host path rules reject the name")
	}
	if !gotPaths["normal.txt"] {
		t.Error("ordinary event normal.txt was skipped")
	}

	// No baseline was written for any create/delete protected path (not even StatePending),
	// and the skipped move left the tracked source baseline synced in place.
	for _, skipped := range []string{"id:12345", "ID:77", "id:evil"} {
		if _, err := st.GetBaseline(1, skipped); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetBaseline(%q) = %v, want ErrNotFound (skipped event must not mutate the store)", skipped, err)
		}
	}
	src, err := st.GetBaseline(1, "clean")
	if err != nil {
		t.Fatalf("GetBaseline(clean): %v", err)
	}
	if src.State != store.StateSynced || src.FileID != "f2" {
		t.Errorf("source baseline = %+v, want unchanged StateSynced f2 (skipped move must not relocate it)", src)
	}
}
