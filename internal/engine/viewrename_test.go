package engine

// Tests for the view-rename guard (viewrename.go): a server-side
// view rename must never be read as a mass local deletion. Before the guard,
// refreshViews overwrote the stored name unconditionally, the scanner looked under
// the new directory while every baseline described the old one, and the diff pushed
// an OpRemove for each tracked file - erasing the whole view server-side.

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
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// newRenameEngine builds an engine over a real temp store and sync root whose fake
// transport enumerates view 7 under serverName, capturing logs into the returned
// buffer. The stored record is seeded separately by each test.
func newRenameEngine(t *testing.T, serverName string) (*Engine, *store.Store, string, *bytes.Buffer) {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	ft := &fakeTransport{client: &fakeClient{views: []actions.View{
		{ViewID: 7, Name: serverName, Privilege: "rw", NodeID: "root7"},
	}}}
	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e := New(st, ft, root, "sess", log)
	return e, st, root, &logBuf
}

// seedSyncedFile writes viewDir/rel on disk and a matching StateSynced baseline
// whose (size, mtime, hash) agree with the on-disk file, so a subsequent scan of a
// consistent tree is clean (no ops).
func seedSyncedFile(t *testing.T, st *store.Store, viewID uint64, viewDir, rel string, content []byte) {
	t.Helper()
	writeFile(t, viewDir, rel, content)
	fi, err := os.Stat(filepath.Join(viewDir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("stat seeded file: %v", err)
	}
	b := store.Baseline{
		FileID:     "f-" + rel,
		FileType:   "file",
		ServerHash: md4.ContentHash(content),
		LocalHash:  md4.ContentHash(content),
		LocalSize:  int64(len(content)),
		LocalMTime: fi.ModTime().Unix(),
		State:      store.StateSynced,
	}
	if err := st.PutBaseline(viewID, rel, b); err != nil {
		t.Fatalf("PutBaseline %s: %v", rel, err)
	}
}

// requireNoQueuedOps fails the test if any op - in particular an OpRemove - reached
// the durable queue.
func requireNoQueuedOps(t *testing.T, st *store.Store) {
	t.Helper()
	n, err := st.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		ops, _ := st.PendingOps()
		t.Fatalf("queue holds %d ops, want 0: %+v", n, ops)
	}
}

// TestRefreshViewsRenameMovesDirectory covers the safe default: the old directory
// exists and the new one does not, so refreshViews moves the directory itself and
// adopts the new name - no OpRemove, and the following scan is clean.
func TestRefreshViewsRenameMovesDirectory(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedFile(t, st, 7, filepath.Join(root, "old"), "f.txt", []byte("hello"))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}

	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.Name != "new" {
		t.Errorf("view name = %q, want new", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want empty", e.pendingViewRenames)
	}
	if _, err := os.Lstat(filepath.Join(root, "old")); !os.IsNotExist(err) {
		t.Errorf("old directory still present (err=%v), want moved away", err)
	}
	if _, err := os.Stat(filepath.Join(root, "new", "f.txt")); err != nil {
		t.Errorf("moved file missing under the new directory: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("scan after adoption produced %d ops, want 0 (clean): %+v", len(ops), ops)
	}
	requireNoQueuedOps(t, st)
}

// TestRefreshViewsPreviouslyUnnamedViewMovesNumericDirectory covers a view
// first observed without a server name. Such views materialise under their
// numeric id; when a name later appears it must pass through the rename matrix
// instead of blindly re-pointing scans away from tracked content.
func TestRefreshViewsPreviouslyUnnamedViewMovesNumericDirectory(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "named")
	if err := st.PutView(7, store.View{Name: "", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedFile(t, st, 7, filepath.Join(root, "7"), "f.txt", []byte("hello"))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "named" {
		t.Fatalf("view name = %q, want named", v.Name)
	}
	if _, err := os.Lstat(filepath.Join(root, "7")); !os.IsNotExist(err) {
		t.Errorf("numeric directory still present (err=%v), want moved", err)
	}
	if _, err := os.Stat(filepath.Join(root, "named", "f.txt")); err != nil {
		t.Fatalf("tracked file missing after numeric-directory adoption: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("scan after adoption produced ops: %+v", ops)
	}
}

// TestRefreshViewsRenameBothDirsSuppressesScan covers the ambiguous case:
// directories exist under BOTH names, so the old name is kept, the view's scan is
// suppressed (no OpRemove ever reaches the queue) and a Warn with the operator hint
// is logged each scan pass. Once the operator removes the new directory, the next
// scan pass adopts the rename by moving the old directory, and the pass after that
// scans cleanly.
func TestRefreshViewsRenameBothDirsSuppressesScan(t *testing.T) {
	e, st, root, logBuf := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedFile(t, st, 7, filepath.Join(root, "old"), "f.txt", []byte("hello"))
	if err := os.MkdirAll(filepath.Join(root, "new"), 0o755); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "old" {
		t.Fatalf("view name = %q, want old kept while deferred", v.Name)
	}
	if got := e.pendingViewRenames[7]; got != "new" {
		t.Fatalf("pendingViewRenames[7] = %q, want new", got)
	}
	if !strings.Contains(logBuf.String(), "BOTH names") {
		t.Errorf("log %q, want the both-directories Warn", logBuf.String())
	}

	// The scan pass skips the deferred view entirely: no ops (in particular no
	// OpRemove for the old-name baseline), and the operator hint is re-logged.
	logBuf.Reset()
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	requireNoQueuedOps(t, st)
	if v, _ := st.GetView(7); v.Name != "old" {
		t.Errorf("view name = %q, want old still kept", v.Name)
	}
	if !strings.Contains(logBuf.String(), "BOTH names") {
		t.Errorf("log %q, want the operator hint re-logged by the scan pass", logBuf.String())
	}

	// Operator resolves the ambiguity: the empty new directory is removed. The next
	// scan pass adopts the rename (moving old → new, persisting the name)…
	if err := os.Remove(filepath.Join(root, "new")); err != nil {
		t.Fatalf("remove new dir: %v", err)
	}
	e.lastScan = time.Time{} // defeat the scan throttle
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan (adoption): %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "new" {
		t.Errorf("view name = %q, want new after adoption", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want cleared", e.pendingViewRenames)
	}
	if _, err := os.Stat(filepath.Join(root, "new", "f.txt")); err != nil {
		t.Errorf("moved file missing under the new directory: %v", err)
	}
	requireNoQueuedOps(t, st)

	// …and the pass after that scans cleanly under the new name.
	e.lastScan = time.Time{}
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan (clean): %v", err)
	}
	requireNoQueuedOps(t, st)
}

// TestRefreshViewsRenameOldMissingTrackedSuppresses covers the unmounted-volume
// shape: neither directory exists on disk but baselines are still tracked, so
// adopting the new name would let the eventual remount be read as a mass delete -
// the rename stays deferred.
func TestRefreshViewsRenameOldMissingTrackedSuppresses(t *testing.T) {
	e, st, _, logBuf := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	b := store.Baseline{FileID: "f1", FileType: "file", LocalHash: "aa", State: store.StateSynced}
	if err := st.PutBaseline(7, "f.txt", b); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "old" {
		t.Errorf("view name = %q, want old kept", v.Name)
	}
	if got := e.pendingViewRenames[7]; got != "new" {
		t.Errorf("pendingViewRenames[7] = %q, want new", got)
	}
	if !strings.Contains(logBuf.String(), "unmounted") {
		t.Errorf("log %q, want the missing-directory Warn", logBuf.String())
	}
	requireNoQueuedOps(t, st)
}

// TestRefreshViewsRenameAdoptsUntrackedView confirms a rename of a view with
// nothing materialised locally (no directory under either name, no baselines) is
// adopted directly - there is nothing a mis-scan could delete.
func TestRefreshViewsRenameAdoptsUntrackedView(t *testing.T) {
	e, st, _, _ := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "new" {
		t.Errorf("view name = %q, want new", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want empty", e.pendingViewRenames)
	}
}

// TestRefreshViewsRenameAdoptsManualMove covers the natural manual fix: the
// operator already renamed the on-disk directory to the new name (old absent, new
// present), so the engine adopts without warning forever - and the following scan
// of the moved tree is clean.
func TestRefreshViewsRenameAdoptsManualMove(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedFile(t, st, 7, filepath.Join(root, "new"), "f.txt", []byte("hello"))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "new" {
		t.Errorf("view name = %q, want new", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want empty", e.pendingViewRenames)
	}

	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("scan after manual-move adoption produced %d ops, want 0: %+v", len(ops), ops)
	}
}

// seedSyncedBaselineOnly records a StateSynced baseline WITHOUT materialising the
// file, the shape of a view whose volume is unmounted: tracked state exists, nothing
// of it is on disk.
func seedSyncedBaselineOnly(t *testing.T, st *store.Store, viewID uint64, rel string, size int64, isDir bool) {
	t.Helper()
	b := store.Baseline{FileID: "f-" + rel, FileType: "file", LocalHash: "h-" + rel, LocalSize: size, State: store.StateSynced}
	if isDir {
		b.FileType = "dir"
		b.LocalHash = ""
		b.LocalSize = 0
	}
	if err := st.PutBaseline(viewID, rel, b); err != nil {
		t.Fatalf("PutBaseline %s: %v", rel, err)
	}
}

// TestRefreshViewsRenameForeignDirUnderNewNameDefers covers a view rename during an unmount: the
// view's volume is unmounted (the old directory is absent) while a FOREIGN directory
// happens to carry the view's new server-side name. The newExists branch used to adopt
// on that directory's mere existence, re-pointing the scan at a stranger's tree - and
// the non-empty scan that follows has neither the empty-scan debounce nor (with no
// device recorded) a mount guard, so the very next pass emitted an OpRemove for every
// tracked file and cross-uploaded the foreign content. The adoption now demands
// evidence that the new-named directory holds this view's tracked content; a foreign
// tree supplies none, so the rename defers with the operator hint.
func TestRefreshViewsRenameForeignDirUnderNewNameDefers(t *testing.T) {
	e, st, root, logBuf := newRenameEngine(t, "Media")
	if err := st.PutView(7, store.View{Name: "Photos", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Tracked content lives on the unmounted volume: baselines but no files.
	seedSyncedBaselineOnly(t, st, 7, "a.txt", 5, false)
	seedSyncedBaselineOnly(t, st, 7, "trip/notes.txt", 9, false)
	// The stranger's directory carries the new name AND a same-named file, so a
	// name-only match would be fooled; its size differs from the tracked record's.
	writeFile(t, filepath.Join(root, "Media"), "a.txt", []byte("stranger content"))
	writeFile(t, filepath.Join(root, "Media"), "unrelated.bin", []byte("xyz"))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "Photos" {
		t.Fatalf("view name = %q, want Photos kept while deferred", v.Name)
	}
	if got := e.pendingViewRenames[7]; got != "Media" {
		t.Fatalf("pendingViewRenames[7] = %q, want Media", got)
	}
	if !strings.Contains(logBuf.String(), "holds none of this view's tracked files") {
		t.Errorf("log %q, want the foreign-directory Warn", logBuf.String())
	}

	// The destructive consequence stays closed: the scan pass skips the deferred view
	// entirely - no OpRemove for the tracked baselines, no upload of foreign content.
	logBuf.Reset()
	if err := e.localScan(context.Background()); err != nil {
		t.Fatalf("localScan: %v", err)
	}
	requireNoQueuedOps(t, st)
	if v, _ := st.GetView(7); v.Name != "Photos" {
		t.Errorf("view name = %q, want Photos still kept", v.Name)
	}
	if !strings.Contains(logBuf.String(), "holds none of this view's tracked files") {
		t.Errorf("log %q, want the operator hint re-logged by the scan pass", logBuf.String())
	}
}

// TestRefreshViewsRenameManualMoveOfDirOnlyViewAdopts pins the weaker evidence grade a
// directories-only view can offer: it tracks no file baseline at all, so a tracked
// directory present under the new name is the strongest confirmation available and the
// operator's manual move is adopted on the first pass.
func TestRefreshViewsRenameManualMoveOfDirOnlyViewAdopts(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedBaselineOnly(t, st, 7, "albums", 0, true)
	if err := os.MkdirAll(filepath.Join(root, "new", "albums"), 0o755); err != nil {
		t.Fatalf("mkdir moved tree: %v", err)
	}

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "new" {
		t.Errorf("view name = %q, want new adopted", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want empty", e.pendingViewRenames)
	}
}

// TestNewNameTreeConfirmed is the table over the evidence predicate itself.
func TestNewNameTreeConfirmed(t *testing.T) {
	type rec struct {
		rel   string
		size  int64
		isDir bool
	}
	cases := []struct {
		name          string
		baselines     []rec
		pending       []string // extra non-synced baselines
		onDisk        map[string][]byte
		dirs          []string
		wantConfirmed bool
		wantSynced    bool
	}{
		{
			name:          "file size match confirms",
			baselines:     []rec{{rel: "a.txt", size: 5}},
			onDisk:        map[string][]byte{"a.txt": []byte("hello")},
			wantConfirmed: true, wantSynced: true,
		},
		{
			name:       "same name different size is not evidence",
			baselines:  []rec{{rel: "a.txt", size: 5}},
			onDisk:     map[string][]byte{"a.txt": []byte("stranger content")},
			wantSynced: true,
		},
		{
			name:       "absent tracked file is not evidence",
			baselines:  []rec{{rel: "a.txt", size: 5}},
			wantSynced: true,
		},
		{
			name:       "dir match ignored while files are tracked",
			baselines:  []rec{{rel: "a.txt", size: 5}, {rel: "sub", isDir: true}},
			dirs:       []string{"sub"},
			wantSynced: true,
		},
		{
			name:          "dir match confirms a directories-only view",
			baselines:     []rec{{rel: "sub", isDir: true}},
			dirs:          []string{"sub"},
			wantConfirmed: true, wantSynced: true,
		},
		{
			name:       "no synced baseline: nothing to protect",
			pending:    []string{"a.txt"},
			onDisk:     map[string][]byte{"a.txt": []byte("hello")},
			wantSynced: false,
		},
		{
			name:       "tracked file replaced by a directory is not evidence",
			baselines:  []rec{{rel: "a.txt", size: 5}},
			dirs:       []string{"a.txt"},
			wantSynced: true,
		},
		{
			// A zero-byte tracked file discriminates nothing: every tree holds empty files, so a
			// 0 == 0 size match would confirm a stranger's directory carrying the new name.
			name:       "empty tracked file is not evidence",
			baselines:  []rec{{rel: ".gitkeep", size: 0}},
			onDisk:     map[string][]byte{".gitkeep": {}},
			wantSynced: true,
		},
		{
			name:          "non-empty file confirms alongside an empty one",
			baselines:     []rec{{rel: ".gitkeep", size: 0}, {rel: "a.txt", size: 5}},
			onDisk:        map[string][]byte{".gitkeep": {}, "a.txt": []byte("hello")},
			wantConfirmed: true, wantSynced: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, st, root, _ := newRenameEngine(t, "new")
			newAbs := filepath.Join(root, "new")
			if err := os.MkdirAll(newAbs, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			for _, b := range tc.baselines {
				seedSyncedBaselineOnly(t, st, 7, b.rel, b.size, b.isDir)
			}
			for _, p := range tc.pending {
				pb := store.Baseline{FileID: "p-" + p, FileType: "file", LocalHash: "h", LocalSize: 5, State: store.StatePending}
				if err := st.PutBaseline(7, p, pb); err != nil {
					t.Fatalf("PutBaseline: %v", err)
				}
			}
			for _, d := range tc.dirs {
				if err := os.MkdirAll(filepath.Join(newAbs, filepath.FromSlash(d)), 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", d, err)
				}
			}
			for rel, content := range tc.onDisk {
				writeFile(t, newAbs, rel, content)
			}
			confirmed, hasSynced, err := e.newNameTreeConfirmed(7, newAbs)
			if err != nil {
				t.Fatalf("newNameTreeConfirmed: %v", err)
			}
			if confirmed != tc.wantConfirmed || hasSynced != tc.wantSynced {
				t.Fatalf("confirmed=%v hasSynced=%v, want %v/%v", confirmed, hasSynced, tc.wantConfirmed, tc.wantSynced)
			}
		})
	}
}

// TestNewNameTreeConfirmedStoreErrorPropagates pins that an unreadable baseline stream
// never decides "no evidence" - the adoption branch must defer on it, not adopt.
func TestNewNameTreeConfirmedStoreErrorPropagates(t *testing.T) {
	e, st, root, logBuf := newRenameEngine(t, "new")
	if err := st.PutView(7, store.View{Name: "old", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedBaselineOnly(t, st, 7, "a.txt", 5, false)
	if err := os.MkdirAll(filepath.Join(root, "new"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	boom := errors.New("baseline stream broke")
	e.scanBaselines = func(uint64, func(store.PathBaseline) error) ([]string, error) { return nil, boom }

	if _, _, err := e.newNameTreeConfirmed(7, filepath.Join(root, "new")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the store error", err)
	}
	if e.adoptViewRename(7, "old", "new") {
		t.Fatal("adopted the rename on an unreadable baseline stream, want deferral")
	}
	if !strings.Contains(logBuf.String(), "inspecting the directory under the new name") {
		t.Errorf("log %q, want the inspection-failure Warn", logBuf.String())
	}
}

// TestRefreshViewsRenameCaseOnlyAdopts covers a case-only rename under
// case-insensitive matching: both names fold to the same key, so they address the
// same node - the rename is only a spelling fix and is adopted immediately rather
// than deferring forever on "both directories exist".
func TestRefreshViewsRenameCaseOnlyAdopts(t *testing.T) {
	restore := normpath.SetCaseInsensitive(true)
	defer restore()

	e, st, root, _ := newRenameEngine(t, "docs")
	if err := st.PutView(7, store.View{Name: "Docs", Privilege: "rw", NodeID: "root7", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedSyncedFile(t, st, 7, filepath.Join(root, "Docs"), "f.txt", []byte("hello"))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews: %v", err)
	}
	if v, _ := st.GetView(7); v.Name != "docs" {
		t.Errorf("view name = %q, want docs", v.Name)
	}
	if len(e.pendingViewRenames) != 0 {
		t.Errorf("pendingViewRenames = %v, want empty", e.pendingViewRenames)
	}
	// On this (byte-preserving) test filesystem the spelling fix is a real rename.
	if _, err := os.Stat(filepath.Join(root, "docs", "f.txt")); err != nil {
		t.Errorf("file missing under the re-spelled directory: %v", err)
	}
}

// TestNewNameTreeConfirmedCapDoesNotInvertGrading pins that maxRenameAdoptProbes bounds the
// Lstat work ONLY, never the hashableSynced determination. Baselines stream in path-byte
// order, so a view whose leading records are all directories would - with the cap ending the
// stream - decide hashableSynced from a truncated prefix and let a bare directory-name match
// adopt for a view that DOES track files. That is exactly the weak "name match alone"
// evidence the grading rejects, in the mass-delete direction.
func TestNewNameTreeConfirmedCapDoesNotInvertGrading(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	newAbs := filepath.Join(root, "new")
	if err := os.MkdirAll(newAbs, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Directory baselines sort before "zz.txt", so the file record lands past the probe cap.
	for i := 0; i <= maxRenameAdoptProbes+2; i++ {
		rel := fmt.Sprintf("d%03d", i)
		seedSyncedBaselineOnly(t, st, 7, rel, 0, true)
		if err := os.MkdirAll(filepath.Join(newAbs, rel), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}
	// The view tracks a synced FILE whose content is NOT under the new name, so the only
	// evidence available is the weak directory match - which must not confirm.
	seedSyncedBaselineOnly(t, st, 7, "zz.txt", 5, false)

	confirmed, hasSynced, err := e.newNameTreeConfirmed(7, newAbs)
	if err != nil {
		t.Fatalf("newNameTreeConfirmed: %v", err)
	}
	if !hasSynced {
		t.Fatal("hasSynced = false, want true")
	}
	if confirmed {
		t.Fatal("a directory-only match confirmed for a view that tracks a synced file: the probe cap inverted the evidence grading")
	}
}

// TestNewNameTreeConfirmedDirProbesDoNotStarveFiles pins that the probe budget is charged per
// evidence KIND. Baselines stream in path-byte order, so a row of empty directories easily fills
// maxRenameAdoptProbes before the first file record; with one shared counter that file - the very
// record that WOULD confirm - arrived with the budget spent, and since seeing it also set
// hashableSynced (retiring the weak directory grade), the view could never confirm on ANY pass:
// the operator who had already moved the tree to the new name saw outbound sync frozen forever.
func TestNewNameTreeConfirmedDirProbesDoNotStarveFiles(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	newAbs := filepath.Join(root, "new")
	if err := os.MkdirAll(newAbs, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Directory baselines (and their real directories) sort before "zz.txt".
	for i := 0; i <= maxRenameAdoptProbes+2; i++ {
		rel := fmt.Sprintf("d%03d", i)
		seedSyncedBaselineOnly(t, st, 7, rel, 0, true)
		if err := os.MkdirAll(filepath.Join(newAbs, rel), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}
	// The tracked file IS present under the new name at its recorded size: the operator moved
	// the tree, so the adoption must confirm.
	seedSyncedBaselineOnly(t, st, 7, "zz.txt", 5, false)
	writeFile(t, newAbs, "zz.txt", []byte("hello"))

	confirmed, hasSynced, err := e.newNameTreeConfirmed(7, newAbs)
	if err != nil {
		t.Fatalf("newNameTreeConfirmed: %v", err)
	}
	if !hasSynced {
		t.Fatal("hasSynced = false, want true")
	}
	if !confirmed {
		t.Fatal("the tracked file present under the new name did not confirm: directory probes consumed the file budget, freezing the view permanently")
	}
}

// TestNewNameTreeConfirmedEmptyFilesDoNotStarveFiles pins that a record which STRUCTURALLY
// cannot confirm does not charge the probe budget. A zero-length tracked file is excluded from
// confirming on purpose (a 0 == 0 size match discriminates nothing - every tree holds a
// .gitkeep), but the budget used to be charged before that exclusion was applied, so 64 empty
// files in path-byte order - ubiquitous and lexicographically early - spent the file budget and
// the record that WOULD confirm hit the spent-budget stop instead. Seeing those records had
// already set hashableSynced, retiring the weak directory grade, and the baseline stream order
// is deterministic: the deferral was therefore PERMANENT, freezing outbound sync for a view whose
// operator had already moved the tree to the new name. This is the sibling of the per-kind budget
// split above, and the reason scanConfirmsTrackedTree skips an emptyContentMD4 candidate BEFORE
// charging maxDeviceConfirmHashes.
func TestNewNameTreeConfirmedEmptyFilesDoNotStarveFiles(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	newAbs := filepath.Join(root, "new")
	if err := os.MkdirAll(newAbs, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Zero-length tracked files, present under the new name, sorting before "zz.txt".
	for i := 0; i <= maxRenameAdoptProbes+2; i++ {
		rel := fmt.Sprintf("e%03d.txt", i)
		seedSyncedBaselineOnly(t, st, 7, rel, 0, false)
		writeFile(t, newAbs, rel, nil)
	}
	// The one discriminating record: a non-empty tracked file present at its recorded size.
	seedSyncedBaselineOnly(t, st, 7, "zz.txt", 5, false)
	writeFile(t, newAbs, "zz.txt", []byte("hello"))

	confirmed, hasSynced, err := e.newNameTreeConfirmed(7, newAbs)
	if err != nil {
		t.Fatalf("newNameTreeConfirmed: %v", err)
	}
	if !hasSynced {
		t.Fatal("hasSynced = false, want true")
	}
	if !confirmed {
		t.Fatal("the non-empty tracked file present under the new name did not confirm: zero-length records consumed the file budget, freezing the view permanently")
	}
}

// TestNewNameTreeConfirmedOnlyEmptyFilesDefers pins the other half: a view whose only synced
// file baselines are zero-length supplies no file evidence and must keep deferring (the
// fail-toward-protected direction), even though those files are present under the new name at
// their recorded size - a 0 == 0 match would confirm a stranger's directory.
func TestNewNameTreeConfirmedOnlyEmptyFilesDefers(t *testing.T) {
	e, st, root, _ := newRenameEngine(t, "new")
	newAbs := filepath.Join(root, "new")
	if err := os.MkdirAll(newAbs, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seedSyncedBaselineOnly(t, st, 7, "a.txt", 0, false)
	writeFile(t, newAbs, "a.txt", nil)
	// A matching directory must not rescue the grade: the view tracks a synced file record.
	seedSyncedBaselineOnly(t, st, 7, "d", 0, true)
	if err := os.MkdirAll(filepath.Join(newAbs, "d"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	confirmed, hasSynced, err := e.newNameTreeConfirmed(7, newAbs)
	if err != nil {
		t.Fatalf("newNameTreeConfirmed: %v", err)
	}
	if !hasSynced {
		t.Fatal("hasSynced = false, want true")
	}
	if confirmed {
		t.Fatal("a zero-length size match confirmed the adoption: 0 == 0 discriminates nothing, so a stranger's tree would be adopted")
	}
}
