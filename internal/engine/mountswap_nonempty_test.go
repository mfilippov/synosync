package engine

// Non-empty mount-swap guard: the empty-scan branch has consulted the view
// root's filesystem device id, but the NON-EMPTY branch used to adopt whatever
// device it found and diff immediately - so the wrong USB disk / an fstab typo / a foreign
// dataset at the view root produced a mass OpRemove plus upload-overwrites in a single pass,
// with no debounce and with the comparison baseline the empty-scan guard relies on destroyed in
// the same breath. These tests pin the guard and its confirmation criterion: a changed device is
// adopted only when the scanned tree still carries positive content evidence of being the
// tracked tree.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// baselineState reports a baseline's stored state, failing the test when the record is missing.
func baselineState(t *testing.T, e *Engine, viewID uint64, path string) store.BaselineState {
	t.Helper()
	b, err := e.store.GetBaseline(viewID, path)
	if err != nil {
		t.Fatalf("GetBaseline %s: %v", path, err)
	}
	return b.State
}

// TestNonEmptyScanForeignVolumeSuppressed drives the scenario: a healthy view is
// scanned on device 1 (recording it), then the mount is swapped for a FOREIGN filesystem that is
// populated - it even holds a same-named file and a same-named directory, so name matching alone
// would be fooled - and reports device 2. Every such scan must be suppressed: no ops, no device
// adoption, baselines untouched. Before the fix the very first swapped scan adopted device 2 and
// emitted an OpRemove for each vanished baseline plus an OpUpload overwriting the server copy of
// the coincidentally-shared path.
func TestNonEmptyScanForeignVolumeSuppressed(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	ours := []byte("our content")
	notes := []byte("our notes")
	writeFile(t, viewDir, "a.txt", ours)
	writeFile(t, viewDir, "docs/notes.txt", notes)
	putSynced(t, e, viewID, "a.txt", "fa", "file", md4.ContentHash(ours), int64(len(ours)))
	putSynced(t, e, viewID, "docs", "fdocs", "dir", "", 0)
	putSynced(t, e, viewID, "docs/notes.txt", "fnotes", "file", md4.ContentHash(notes), int64(len(notes)))

	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("healthy scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("healthy scan produced %d ops, want 0: %+v", len(ops), ops)
	}
	if got, ok := e.viewRootDev[viewID]; !ok || got != 1 {
		t.Fatalf("healthy scan recorded device = %d (ok=%v), want 1", got, ok)
	}

	// The mount is swapped for a foreign, populated filesystem: our tree is gone, and what is
	// there shares one file name (different content) and one directory name with the baselines.
	if err := os.RemoveAll(viewDir); err != nil {
		t.Fatalf("remove view dir: %v", err)
	}
	writeFile(t, viewDir, "a.txt", []byte("somebody else's file"))
	writeFile(t, viewDir, "docs/other.bin", []byte("stranger"))
	curDev = 2

	for i := 0; i < 3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("swapped scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("swapped scan %d produced %d ops, want 0 (foreign volume): %+v", i, len(ops), ops)
		}
	}
	if got := e.viewRootDev[viewID]; got != 1 {
		t.Errorf("foreign volume adopted device %d, want the recorded 1 to be kept", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a non-empty mount swap enqueued %d ops", n)
	}
	for _, p := range []string{"a.txt", "docs", "docs/notes.txt"} {
		if got := baselineState(t, e, viewID, p); got != store.StateSynced {
			t.Errorf("baseline %s state = %v, want StateSynced (untouched)", p, got)
		}
	}
}

// TestNonEmptyScanLegitMigrationAdopts confirms the guard does not freeze a legitimate disk
// migration or remount - a filesystem's device number is not stable across reboots, so this must
// converge on the FIRST scan: the tracked content is still there (one file even matches
// byte-for-byte), so the new device id is adopted and the genuinely-changed file pushes as
// usual.
func TestNonEmptyScanLegitMigrationAdopts(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	keep := []byte("unchanged content")
	before := []byte("before")
	writeFile(t, viewDir, "keep.txt", keep)
	writeFile(t, viewDir, "edit.txt", before)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	putSynced(t, e, viewID, "edit.txt", "fe", "file", md4.ContentHash(before), int64(len(before)))

	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	// The same tree now sits on a new device (disk migrated / remounted), and one file was
	// edited in the meantime.
	writeFile(t, viewDir, "edit.txt", []byte("after the migration"))
	curDev = 2

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("migrated scan: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "edit.txt" {
		t.Fatalf("migrated scan ops = %+v, want a single OpUpload of edit.txt", ops)
	}
	if got := e.viewRootDev[viewID]; got != 2 {
		t.Errorf("in-memory device = %d, want the migrated 2 adopted", got)
	}
	v, err := e.store.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 2 {
		t.Errorf("persisted RootDevice = %d, want 2", v.RootDevice)
	}
}

// TestNonEmptyScanDirOnlyViewAdoptsOnDirMatch pins the graded evidence rule: a view that tracks
// no hashable synced file has no content to compare, so a surviving tracked DIRECTORY is the
// strongest evidence available and adopts the new device. (When hashable files DO exist a bare
// directory-name match is ignored - TestNonEmptyScanForeignVolumeSuppressed covers that side.)
func TestNonEmptyScanDirOnlyViewAdoptsOnDirMatch(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	putSynced(t, e, viewID, "sub", "fsub", "dir", "", 0)
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}

	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	curDev = 2
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("remounted scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("remounted scan produced %d ops, want 0: %+v", len(ops), ops)
	}
	if got := e.viewRootDev[viewID]; got != 2 {
		t.Errorf("dir-only view device = %d, want the remounted 2 adopted", got)
	}
}

// TestNonEmptyScanUnreadableDeviceSuppressesForeignTree confirms an UNREADABLE device id (which
// viewRootDeviceChanged reports like a change) suppresses a non-empty scan whose tree matches
// nothing tracked, while leaving the recorded device intact.
func TestNonEmptyScanUnreadableDeviceSuppressesForeignTree(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	ours := []byte("our content")
	writeFile(t, viewDir, "a.txt", ours)
	putSynced(t, e, viewID, "a.txt", "fa", "file", md4.ContentHash(ours), int64(len(ours)))

	healthy := true
	e.deviceID = func(string) (uint64, error) {
		if healthy {
			return 5, nil
		}
		return 0, os.ErrPermission
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	if err := os.Remove(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Fatalf("remove a.txt: %v", err)
	}
	writeFile(t, viewDir, "stranger.txt", []byte("not ours"))
	healthy = false

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("unreadable-device scan: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("unreadable-device scan produced %d ops, want 0: %+v", len(ops), ops)
	}
	if got := e.viewRootDev[viewID]; got != 5 {
		t.Errorf("recorded device = %d, want the healthy 5 kept", got)
	}
	if got := baselineState(t, e, viewID, "a.txt"); got != store.StateSynced {
		t.Errorf("baseline a.txt state = %v, want StateSynced (untouched)", got)
	}
}

// TestScanConfirmsTrackedTree is the table-level pin on the evidence grading itself. Rows are
// paired with the bytes actually on disk, because the file arm re-hashes the candidate rather
// than trusting scanEntry.md4 - on the default cheap scan that field is a COPY of the baseline's
// own hash whenever (size, mtime) match the hint, so trusting it would degrade the guard's
// "identical content" criterion to "same path, same size, same mtime".
func TestScanConfirmsTrackedTree(t *testing.T) {
	const ourText = "our content"
	ourHash := md4.ContentHash([]byte(ourText))
	foreignHash := md4.ContentHash([]byte("somebody else's file"))

	file := func(path, hash string, state store.BaselineState) *store.PathBaseline {
		return &store.PathBaseline{Path: path, Baseline: store.Baseline{FileType: "file", LocalHash: hash, State: state}}
	}
	dir := func(path string, state store.BaselineState) *store.PathBaseline {
		return &store.PathBaseline{Path: path, Baseline: store.Baseline{FileType: "dir", State: state}}
	}
	scanFile := func(path, hash string) *scanEntry { return &scanEntry{relPath: path, md4: hash} }
	scanDir := func(path string) *scanEntry { return &scanEntry{relPath: path, isDir: true} }

	tests := []struct {
		name string
		disk map[string]string // view-relative path -> content laid down before the check
		rows []joinRow
		want bool
	}{
		{
			name: "content-identical synced file confirms",
			disk: map[string]string{"a.txt": ourText},
			rows: []joinRow{{scan: scanFile("a.txt", ourHash), base: file("a.txt", ourHash, store.StateSynced)}},
			want: true,
		},
		{
			name: "same name different content does not confirm",
			disk: map[string]string{"a.txt": ourText},
			rows: []joinRow{{scan: scanFile("a.txt", foreignHash), base: file("a.txt", ourHash, store.StateSynced)}},
			want: false,
		},
		{
			// The cheap scan copied the baseline hint into scanEntry.md4 because (size, mtime)
			// matched, but the bytes on disk are a stale mtime-preserving clone. Only the
			// re-hash can tell them apart - this is the case the tautological comparison passed.
			name: "cheap-scan hint match over foreign content does not confirm",
			disk: map[string]string{"a.txt": "somebody else's file"},
			rows: []joinRow{{scan: scanFile("a.txt", ourHash), base: file("a.txt", ourHash, store.StateSynced)}},
			want: false,
		},
		{
			name: "vanished candidate supplies no evidence",
			rows: []joinRow{{scan: scanFile("a.txt", ourHash), base: file("a.txt", ourHash, store.StateSynced)}},
			want: false,
		},
		{
			name: "dir match ignored while a hashable synced file exists",
			disk: map[string]string{"docs/keep.txt": ourText},
			rows: []joinRow{
				{scan: scanDir("docs"), base: dir("docs", store.StateSynced)},
				{base: file("a.txt", ourHash, store.StateSynced)},
			},
			want: false,
		},
		{
			name: "dir match confirms a view with no hashable synced file",
			disk: map[string]string{"docs/keep.txt": ourText},
			rows: []joinRow{{scan: scanDir("docs"), base: dir("docs", store.StateSynced)}},
			want: true,
		},
		{
			name: "non-synced baseline supplies no evidence",
			disk: map[string]string{"a.txt": ourText},
			rows: []joinRow{{scan: scanFile("a.txt", ourHash), base: file("a.txt", ourHash, store.StatePending)}},
			want: false,
		},
		{
			name: "untracked scan entries alone supply no evidence",
			disk: map[string]string{"a.txt": ourText},
			rows: []joinRow{{scan: scanFile("a.txt", ourHash)}},
			want: false,
		},
		{
			name: "hash-less synced file is not a comparison base",
			disk: map[string]string{"a.txt": ourText},
			rows: []joinRow{{scan: scanFile("a.txt", ""), base: file("a.txt", "", store.StateSynced)}},
			want: false,
		},
		{
			// An EMPTY tracked file hashes to a constant, so a foreign tree holding its own
			// empty file at the same relative path (.gitkeep, __init__.py, a lock file) would
			// re-hash to exactly the same digest and confirm - a name match dressed up as a
			// content match, in the mass-delete direction.
			name: "empty tracked file supplies no evidence",
			disk: map[string]string{".gitkeep": ""},
			rows: []joinRow{{scan: scanFile(".gitkeep", emptyContentMD4), base: file(".gitkeep", emptyContentMD4, store.StateSynced)}},
			want: false,
		},
		{
			// The empty-content skip must not retire the file GRADE itself: a non-empty tracked
			// file alongside the empty one still confirms.
			name: "non-empty file confirms alongside an empty one",
			disk: map[string]string{".gitkeep": "", "a.txt": ourText},
			rows: []joinRow{
				{scan: scanFile(".gitkeep", emptyContentMD4), base: file(".gitkeep", emptyContentMD4, store.StateSynced)},
				{scan: scanFile("a.txt", ourHash), base: file("a.txt", ourHash, store.StateSynced)},
			},
			want: true,
		},
		{
			// ...and it must not promote the weak directory grade either: the view still tracks a
			// hashable synced file, so a bare directory-name match stays insufficient.
			name: "empty tracked file still retires the dir grade",
			disk: map[string]string{"docs/keep.txt": ourText},
			rows: []joinRow{
				{scan: scanDir("docs"), base: dir("docs", store.StateSynced)},
				{base: file(".gitkeep", emptyContentMD4, store.StateSynced)},
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, viewID, viewDir := newScanEngine(t)
			for rel, content := range tc.disk {
				writeFile(t, viewDir, rel, []byte(content))
			}
			if got := e.scanConfirmsTrackedTree(viewID, tc.rows); got != tc.want {
				t.Fatalf("scanConfirmsTrackedTree = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestScanConfirmsTrackedTreeCapsRehashes pins maxDeviceConfirmHashes: a foreign tree whose
// files all coincide with a tracked (size, mtime) hint passes the cheap pre-filter on every row,
// so without the cap one suppressed scan pass would hash the whole tree. The cap bounds the work
// and the verdict stays "no evidence".
func TestScanConfirmsTrackedTreeCapsRehashes(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	ourHash := md4.ContentHash([]byte("our content"))

	var rows []joinRow
	total := maxDeviceConfirmHashes + 5
	for i := 0; i < total; i++ {
		rel := filepath.ToSlash(filepath.Join("d", "f"+string(rune('a'+i))+".txt"))
		writeFile(t, viewDir, rel, []byte("foreign bytes"))
		rows = append(rows, joinRow{
			scan: &scanEntry{relPath: rel, md4: ourHash},
			base: &store.PathBaseline{Path: rel, Baseline: store.Baseline{
				FileType: "file", LocalHash: ourHash, State: store.StateSynced,
			}},
		})
	}
	if e.scanConfirmsTrackedTree(viewID, rows) {
		t.Fatal("scanConfirmsTrackedTree confirmed a wholly foreign tree")
	}
	// Make the LAST row the only genuine match: it sits past the cap, so the pass must not
	// reach it - the guard trades a rare false suppression for a bounded, predictable cost.
	last := rows[len(rows)-1]
	writeFile(t, viewDir, last.scan.relPath, []byte("our content"))
	if e.scanConfirmsTrackedTree(viewID, rows) {
		t.Fatal("scanConfirmsTrackedTree hashed past maxDeviceConfirmHashes candidates")
	}
}
