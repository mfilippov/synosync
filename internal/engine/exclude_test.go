package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

func excluding(viewID uint64, paths ...string) config.Views {
	var v config.Views
	for _, p := range paths {
		v.Excluded = append(v.Excluded, config.ExcludedPath{ViewID: viewID, Path: p})
	}
	return v
}

// TestColdStartSkipsAnExcludedSubtree: the first sync neither lists nor plans an excluded
// directory. "Photos" is unscripted, so listing it would fail the walk as unevidenced.
func TestColdStartSkipsAnExcludedSubtree(t *testing.T) {
	st, root := coldStreamFixture(t)
	c := &catalogClient{tree: map[string][]actions.Node{"": {
		{Name: "Photos", FileID: "dp", IsDir: true},
		{Name: "a.txt", FileID: "fa", Size: 1, Hash: "h", MTime: 50},
	}}}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	e.SetViews(excluding(7, "Photos"))
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v (did the walk list the excluded directory?)", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "a.txt" {
		t.Fatalf("ops = %+v, want only the download of a.txt", ops)
	}
	if _, err := st.GetBaseline(7, "Photos"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline for the excluded directory: err = %v, want ErrNotFound", err)
	}
}

// TestScanProtectsAnExcludedSubtree: a local file under an exclusion is not uploaded, and a
// baseline there whose file is gone is not read as a local delete.
func TestScanProtectsAnExcludedSubtree(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	e.SetViews(excluding(viewID, "Pics"))
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	writeFile(t, viewDir, "Pics/new.jpg", []byte("never uploaded"))
	putSynced(t, e, viewID, "Pics/gone.jpg", "fg", "file", "hg", 3)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("ops = %+v, want none under an exclusion", ops)
	}
}

// TestReconcileScopesEventsToTheExclusions covers each event shape at the boundary.
func TestReconcileScopesEventsToTheExclusions(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutView(1, store.View{Name: "home", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e.SetViews(excluding(1, "Photos"))
	putSynced(t, e, 1, "Docs/out.txt", "fo", "file", "ho", 2)

	ops, cursor, err := e.reconcile(1, []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "Photos/a.jpg", Hash: "h", Size: 1},
		{SyncID: 2, Action: "move", FileID: "fo", FromPath: "Docs/out.txt", Path: "Photos/out.txt", KindReported: true},
		{SyncID: 3, Action: "move", FileID: "dm", FromPath: "Photos/2024", Path: "Docs/2024", IsDir: true, KindReported: true},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 3 {
		t.Errorf("cursor = %d, want 3 (it advances past skipped events)", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "Docs/out.txt" {
		t.Fatalf("ops = %+v, want one inbound remove of the node that left for the exclusion", ops)
	}
	v, err := st.GetView(1)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if !slices.Equal(v.Rescan, []string{"Docs/2024"}) {
		t.Errorf("Rescan = %v, want the destination of the move that came in", v.Rescan)
	}
}

// TestRescanMergesOneSubtree: a rescan walks only its subtree, adopts what already matches on
// disk, uploads what only the disk holds, and leaves the rest of the view alone. "Docs" is
// unscripted, so listing it would fail the walk.
func TestRescanMergesOneSubtree(t *testing.T) {
	st, root := coldStreamFixture(t)
	same := []byte("same bytes")
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root", InitialSyncDone: true, Rescan: []string{"Photos"}}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := &catalogClient{tree: map[string][]actions.Node{
		"": {
			{Name: "Photos", FileID: "dp", IsDir: true},
			{Name: "Docs", FileID: "dd", IsDir: true},
			{Name: "other.txt", FileID: "fo", Size: 1, Hash: "ho", MTime: 50},
		},
		"Photos": {{Name: "a.jpg", FileID: "fa", Size: int64(len(same)), Hash: md4.ContentHash(same), MTime: 50}},
	}}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	viewDir := filepath.Join(root, "V")
	writeFile(t, viewDir, "Photos/a.jpg", same)
	writeFile(t, viewDir, "Photos/b.jpg", []byte("only here"))

	if err := e.rescanPending(context.Background()); err != nil {
		t.Fatalf("rescanPending: %v", err)
	}
	if v := listViewOf(t, st, 7); len(v.Rescan) != 0 {
		t.Errorf("Rescan = %v after a completed walk, want empty", v.Rescan)
	}
	if b, err := st.GetBaseline(7, "Photos/a.jpg"); err != nil || b.State != store.StateSynced || b.FileID != "fa" {
		t.Errorf("Photos/a.jpg baseline = %+v, %v; want adopted as synced", b, err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	var paths []string
	for _, op := range ops {
		paths = append(paths, string(op.Type)+":"+op.Path)
	}
	if !slices.Equal(paths, []string{"upload:Photos/b.jpg"}) {
		t.Errorf("ops = %v, want only the upload of the local-only file (nothing outside the subtree)", paths)
	}
}

// TestRescanWaitsWhileThePathIsStillExcluded: `exclude remove` records the rescan before it lifts
// the exclusion, so a rescan of a still-excluded path must wait, not be consumed.
func TestRescanWaitsWhileThePathIsStillExcluded(t *testing.T) {
	st, root := coldStreamFixture(t)
	if err := st.PutView(7, store.View{Name: "V", InitialSyncDone: true, Rescan: []string{"Photos"}}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := planOnly(New(st, &catalogTransport{client: &catalogClient{}}, root, "sess", nil))
	e.SetViews(excluding(7, "Photos"))
	if err := e.rescanPending(context.Background()); err != nil {
		t.Fatalf("rescanPending: %v", err)
	}
	if v := listViewOf(t, st, 7); !slices.Equal(v.Rescan, []string{"Photos"}) {
		t.Errorf("Rescan = %v, want the entry kept while the path is excluded", v.Rescan)
	}
}

// TestColdStartClearsPendingRescans: a full first sync covers every subtree.
func TestColdStartClearsPendingRescans(t *testing.T) {
	st, root := coldStreamFixture(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root", Rescan: []string{"Photos"}}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "V"), 0o755); err != nil {
		t.Fatal(err)
	}
	e := planOnly(New(st, &catalogTransport{client: &catalogClient{tree: map[string][]actions.Node{"": {}}}}, root, "sess", nil))
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if v := listViewOf(t, st, 7); len(v.Rescan) != 0 {
		t.Errorf("Rescan = %v after the cold start, want empty", v.Rescan)
	}
}

func withDefaultNames(v config.Views) config.Views {
	v.ExcludedNames = config.DefaultExcludedNames()
	return v
}

// TestScanFiltersNamesAndKeepsFoldersCalledTmp: a name rule hides the file it matches (and
// protects its record), while a files-only rule never hides a folder of that name.
func TestScanFiltersNamesAndKeepsFoldersCalledTmp(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	e.SetViews(withDefaultNames(config.Views{}))
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	writeFile(t, viewDir, "desktop.ini", []byte("[.ShellClassInfo]"))
	writeFile(t, viewDir, "work.tmp", []byte("scratch"))
	writeFile(t, viewDir, "tmp/real.txt", []byte("a real file in a folder called tmp"))
	putSynced(t, e, viewID, "Thumbs.db", "ft", "file", "ht", 3) // gone from disk: must not push a remove

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	var got []string
	for _, op := range ops {
		got = append(got, string(op.Type)+":"+op.Path)
	}
	slices.Sort(got)
	if want := []string{"mkdir:tmp", "upload:tmp/real.txt"}; !slices.Equal(got, want) {
		t.Errorf("ops = %v, want only the folder tmp and its file", got)
	}
}

// TestColdStartSkipsSystemNames: the NAS's own folders are neither listed nor planned, with no
// rule configured at all. "Photos/@eaDir" is unscripted, so listing it would fail the walk.
func TestColdStartSkipsSystemNames(t *testing.T) {
	st, root := coldStreamFixture(t)
	c := &catalogClient{tree: map[string][]actions.Node{
		"":       {{Name: "Photos", FileID: "dp", IsDir: true}},
		"Photos": {{Name: "@eaDir", FileID: "de", IsDir: true}, {Name: "a.jpg", FileID: "fa", Size: 1, Hash: "h", MTime: 50}},
	}}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	ops, _ := st.PendingOps()
	for _, op := range ops {
		if op.Path == "Photos/@eaDir" {
			t.Errorf("a NAS system folder was planned: %+v", op)
		}
	}
}

// TestReconcileAppliesNameRulesToEvents: a filtered create is dropped, and a rename INTO a
// filtered name is the node leaving this replica.
func TestReconcileAppliesNameRulesToEvents(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutView(1, store.View{Name: "home", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e.SetViews(withDefaultNames(config.Views{}))
	putSynced(t, e, 1, "notes.txt", "fn", "file", "hn", 2)
	ops, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "draft.tmp", Hash: "h", Size: 1},
		{SyncID: 2, Action: "create", FileID: "d1", Path: "tmp", IsDir: true},
		{SyncID: 3, Action: "move", FileID: "fn", FromPath: "notes.txt", Path: "notes.tmp", KindReported: true},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got []string
	for _, op := range ops {
		got = append(got, string(op.Type)+":"+op.Path)
	}
	if want := []string{"mkdir:tmp", "remove:notes.txt"}; !slices.Equal(got, want) {
		t.Errorf("ops = %v, want %v", got, want)
	}
}

// TestRescanOfTheWholeView: lifting a name rule rescans "" - the whole tree - and brings back
// what the rule hid wherever it is.
func TestRescanOfTheWholeView(t *testing.T) {
	st, root := coldStreamFixture(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root", InitialSyncDone: true, Rescan: []string{""}}); err != nil {
		t.Fatal(err)
	}
	c := &catalogClient{tree: map[string][]actions.Node{
		"":  {{Name: "d", FileID: "dd", IsDir: true}},
		"d": {{Name: "x.tmp", FileID: "fx", Size: 1, Hash: "hx", MTime: 50}},
	}}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	if err := e.rescanPending(context.Background()); err != nil {
		t.Fatalf("rescanPending: %v", err)
	}
	if v := listViewOf(t, st, 7); len(v.Rescan) != 0 {
		t.Errorf("Rescan = %q after the walk, want empty", v.Rescan)
	}
	ops, _ := st.PendingOps()
	var got []string
	for _, op := range ops {
		got = append(got, string(op.Type)+":"+op.Path)
	}
	if !slices.Contains(got, "download:d/x.tmp") {
		t.Errorf("ops = %v, want the file the lifted rule had hidden", got)
	}
}

// TestColdStartRerunTrustsWhatItAlreadyVerified: a synced record left by an interrupted run of the
// same cold start lets the rerun reuse its hash when (size, mtime) match, instead of re-reading
// the file. The record's hash deliberately differs from the bytes on disk: only a hint-honouring
// scan sees it, and so plans no transfer for a node the server reports with that hash.
func TestColdStartRerunTrustsWhatItAlreadyVerified(t *testing.T) {
	st, root := coldStreamFixture(t)
	const mtime = 1_700_000_000
	writeFileWithMtime(t, filepath.Join(root, "V", "done.bin"), []byte("on disk!"), mtime)
	recorded := md4.ContentHash([]byte("recorded"))
	if err := st.PutBaseline(7, "done.bin", store.Baseline{FileType: "file", FileID: "fd", LocalHash: recorded,
		ServerHash: recorded, LocalSize: 8, LocalMTime: mtime, State: store.StateSynced}); err != nil {
		t.Fatal(err)
	}
	c := &catalogClient{tree: map[string][]actions.Node{"": {{Name: "done.bin", FileID: "fd", Size: 8, Hash: recorded, MTime: mtime}}}}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if ops, _ := st.PendingOps(); len(ops) != 0 {
		t.Errorf("ops = %+v, want none: the verified file's stored hash should have been reused", ops)
	}
}
