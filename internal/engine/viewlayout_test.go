package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
)

// layoutEngine builds an engine over a store holding two named views.
func layoutEngine(t *testing.T, v config.Views) (*Engine, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "home", NodeID: "n7"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutView(9, store.View{Name: "Team", NodeID: "n9"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	e := New(st, nil, root, "sess", nil)
	e.SetViews(v)
	return e, root
}

// TestFlatLayoutResolvesToTheSyncRoot is the layout the whole feature turns on:
// with one selected view its contents live directly in the sync root, so the
// view name never appears in a path.
func TestFlatLayoutResolvesToTheSyncRoot(t *testing.T) {
	e, root := layoutEngine(t, config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}, Flat: true})

	got, err := e.abs(7, "docs/report.txt")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	want := filepath.Join(root, "docs", "report.txt")
	if got != want {
		t.Errorf("abs = %q, want %q", got, want)
	}
	if strings.Contains(got, "home") {
		t.Errorf("abs = %q, want no view directory in a flat layout", got)
	}
	if d := e.viewDir(7, map[uint64]string{}); d != "" {
		t.Errorf("viewDir = %q, want empty in a flat layout", d)
	}
}

// TestPerViewLayoutKeepsTheSubdirectory pins the default, which every install
// predating the setting has on disk: the view name IS the directory.
func TestPerViewLayoutKeepsTheSubdirectory(t *testing.T) {
	e, root := layoutEngine(t, config.Views{})

	got, err := e.abs(7, "docs/report.txt")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	want := filepath.Join(root, "home", "docs", "report.txt")
	if got != want {
		t.Errorf("abs = %q, want %q", got, want)
	}
	if d := e.viewDir(7, map[uint64]string{}); d != "home" {
		t.Errorf("viewDir = %q, want home", d)
	}
}

// TestViewSuppressedFollowsTheSelection covers the gate that stops an unselected
// view from being polled or scanned. Local files are left exactly where they are
// - the same treatment a view that vanished from the server enumeration gets.
func TestViewSuppressedFollowsTheSelection(t *testing.T) {
	e, _ := layoutEngine(t, config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}, Flat: true})

	if e.viewSuppressed(7) {
		t.Error("the selected view is suppressed")
	}
	if !e.viewSuppressed(9) {
		t.Error("an unselected view is not suppressed")
	}
}

// TestEmptySelectionSyncsEveryView pins the meaning of the zero value: an
// install that predates the setting keeps syncing everything, including a view
// shared after the fact.
func TestEmptySelectionSyncsEveryView(t *testing.T) {
	e, _ := layoutEngine(t, config.Views{})
	for _, id := range []uint64{7, 9} {
		if e.viewSuppressed(id) {
			t.Errorf("view %d suppressed under an empty selection", id)
		}
	}
	// A view the store knows nothing about is not suppressed either: refreshViews
	// records it before anything syncs it.
	if e.viewSuppressed(1234) {
		t.Error("an unknown view is suppressed under an empty selection")
	}
}

// TestSuppressionIgnoresTheStoredName pins that the gate matches by ID. Matching
// by name made a server-side rename suppress the view for good: the engine
// adopts the new name into its store record, the config still holds the old one,
// and the daemon silently syncs nothing.
func TestSuppressionIgnoresTheStoredName(t *testing.T) {
	e, _ := layoutEngine(t, config.Views{
		Selected: []config.SelectedView{{ID: 7, Name: "whatever this used to be called"}},
		Flat:     true,
	})
	if e.viewSuppressed(7) {
		t.Error("a selected view was suppressed because its stored name is stale")
	}
}

// TestAbsentViewStaysSuppressedRegardlessOfSelection confirms the two reasons to
// leave a view alone share one gate rather than racing each other.
func TestAbsentViewStaysSuppressedRegardlessOfSelection(t *testing.T) {
	e, _ := layoutEngine(t, config.Views{})
	e.absentViews[7] = true
	if !e.viewSuppressed(7) {
		t.Error("a view absent from the server enumeration is not suppressed")
	}
}

// TestFlatScanReadsTheSyncRoot is the regression guard for the whole-view
// delete. abs() and viewDir() were taught that a flat layout collapses the view
// onto the sync root; resolveViewDir - the resolver behind scanView, the
// root-availability probe, the device probes and cold start - was not. Apply
// therefore wrote to the sync root while every local-side path still walked
// syncRoot/<name>, so the scan found no file for any baseline and detectRemoves
// emitted an OpRemove per baseline. The server-subtree check cannot catch that:
// the server really does agree with the store; the files were never looked for.
func TestFlatScanReadsTheSyncRoot(t *testing.T) {
	e, root := layoutEngine(t, config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}, Flat: true})

	name, viewAbs, err := e.resolveViewDir(7)
	if err != nil {
		t.Fatalf("resolveViewDir: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q, want empty under a flat layout", name)
	}
	if viewAbs != root {
		t.Errorf("viewAbs = %q, want the sync root %q", viewAbs, root)
	}

	// A file written where apply would put it must be seen by the scan.
	if err := os.WriteFile(filepath.Join(root, "report.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, _, err := e.scanView(context.Background(), 7, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	var found bool
	for _, en := range entries {
		if en.relPath == "report.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("scanView missed a file at the sync root; entries = %+v", entries)
	}
}

// TestFlatScanDoesNotTreatALikeNamedDirectoryAsTheViewRoot: under a flat layout
// a top-level folder that happens to share the view's name is ordinary user
// content, not the view root. Resolving it as the root is what made the scan look
// in the wrong place.
func TestFlatScanDoesNotTreatALikeNamedDirectoryAsTheViewRoot(t *testing.T) {
	e, root := layoutEngine(t, config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}, Flat: true})
	if err := os.MkdirAll(filepath.Join(root, "home"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "home", "inside.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	entries, _, err := e.scanView(context.Background(), 7, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	var found bool
	for _, en := range entries {
		if en.relPath == "home/inside.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("a like-named directory was treated as the view root; entries = %+v", entries)
	}
}

// TestRenameOfADeselectedViewIsDeferredNotAdopted is the round-3 critical, and
// a fix that created one.
//
// Round 2 stopped the directory MOVE for a deselected view but still wrote the
// new name into the store. The adoption matrix is the only thing that ever
// reconciles the stored name with the on-disk directory, and once the name
// matched, every later refresh took the "unchanged name" branch - so the rename
// was forgotten for good. Deselection is reversible, so re-selecting the view
// then resolved it to a directory nothing had written: the tree stranded at the
// old name, and anything that happened to exist at the new one made every
// baseline read as locally deleted, to the server.
func TestRenameOfADeselectedViewIsDeferredNotAdopted(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(9, store.View{Name: "Team", NodeID: "r9"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Team"), 0o755); err != nil {
		t.Fatal(err)
	}

	fc := &fakeClient{views: []actions.View{{ViewID: 9, Name: "Projects", Privilege: "rw", NodeID: "r9"}}}
	e := New(st, &fakeTransport{client: fc}, root, "sess", nil)
	// View 9 is NOT selected.
	e.SetViews(config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}})

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}

	rec, err := st.GetView(9)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if rec.Name != "Team" {
		t.Errorf("stored name = %q, want the OLD name kept until the move can happen", rec.Name)
	}
	if got := e.pendingViewRenames[9]; got != "Projects" {
		t.Errorf("pendingViewRenames[9] = %q, want the rename deferred", got)
	}
	// The contract that started this: the local directory is untouched.
	if _, err := os.Stat(filepath.Join(root, "Team")); err != nil {
		t.Errorf("a deselected view's directory was moved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Projects")); err == nil {
		t.Error("a deselected view's directory was renamed on disk")
	}
}
