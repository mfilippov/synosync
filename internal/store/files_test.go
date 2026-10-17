package store

import (
	"errors"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func sampleBaseline() Baseline {
	return Baseline{
		FileID:       "fid-1",
		FileType:     "file",
		ServerHash:   "aabbcc",
		ServerMTime:  1000,
		ServerSyncID: 7,
		LocalHash:    "aabbcc",
		LocalMTime:   1000,
		LocalSize:    2048,
		UnixPerm:     0o644,
		State:        StateSynced,
	}
}

// baselinesForView materialises a view's baselines through ScanBaselinesForView, ordered as
// the stream yields them. The store used to export this as BaselinesForView for the engine's
// subtree sweeps (they iterated a detached snapshot while mutating the bucket); those now
// stream the bounded ScanBaselinesUnderPrefix and collect the subtree themselves,
// so the whole-view slice survives only as this test oracle.
func baselinesForView(t *testing.T, s *Store, viewID uint64) []PathBaseline {
	t.Helper()
	var out []PathBaseline
	if _, err := s.ScanBaselinesForView(viewID, func(pb PathBaseline) error {
		out = append(out, pb)
		return nil
	}); err != nil {
		t.Fatalf("ScanBaselinesForView(%d): %v", viewID, err)
	}
	return out
}

func TestBaselineRoundTrip(t *testing.T) {
	s, _ := openTemp(t)

	if _, err := s.GetBaseline(1, "a/b.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBaseline before put: got %v, want ErrNotFound", err)
	}

	want := sampleBaseline()
	if err := s.PutBaseline(1, "a/b.txt", want); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	got, err := s.GetBaseline(1, "a/b.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if got != want {
		t.Errorf("GetBaseline = %+v, want %+v", got, want)
	}

	// The byid index must resolve the file_id to the same path.
	path, err := s.PathByID(1, "fid-1")
	if err != nil {
		t.Fatalf("PathByID: %v", err)
	}
	if path != "a/b.txt" {
		t.Errorf("PathByID = %q, want %q", path, "a/b.txt")
	}
}

func TestBaselineOverwriteUpdatesState(t *testing.T) {
	s, _ := openTemp(t)

	b := sampleBaseline()
	if err := s.PutBaseline(2, "f", b); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	b.State = StateConflict
	b.LocalHash = "ddeeff"
	if err := s.PutBaseline(2, "f", b); err != nil {
		t.Fatalf("PutBaseline overwrite: %v", err)
	}
	got, err := s.GetBaseline(2, "f")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if got.State != StateConflict || got.LocalHash != "ddeeff" {
		t.Errorf("after overwrite = %+v, want state=conflict hash=ddeeff", got)
	}
}

func TestDeleteBaselineClearsIndex(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.PutBaseline(1, "p", sampleBaseline()); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := s.DeleteBaseline(1, "p"); err != nil {
		t.Fatalf("DeleteBaseline: %v", err)
	}
	if _, err := s.GetBaseline(1, "p"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline after delete: got %v, want ErrNotFound", err)
	}
	if _, err := s.PathByID(1, "fid-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("PathByID after delete: got %v, want ErrNotFound", err)
	}
	// Deleting an absent baseline is a no-op.
	if err := s.DeleteBaseline(1, "p"); err != nil {
		t.Errorf("DeleteBaseline absent: %v", err)
	}
}

func TestBaselineRenamePathKeepsIndexConsistent(t *testing.T) {
	s, _ := openTemp(t)

	b := sampleBaseline() // file_id fid-1
	if err := s.PutBaseline(1, "old/name", b); err != nil {
		t.Fatalf("PutBaseline old: %v", err)
	}
	// A rename: same file_id moves to a new path. Delete the old record, then put
	// the new one.
	if err := s.DeleteBaseline(1, "old/name"); err != nil {
		t.Fatalf("DeleteBaseline old: %v", err)
	}
	if err := s.PutBaseline(1, "new/name", b); err != nil {
		t.Fatalf("PutBaseline new: %v", err)
	}
	// The index must now point at the new path, and the old path is gone.
	if _, err := s.GetBaseline(1, "old/name"); !errors.Is(err, ErrNotFound) {
		t.Errorf("old path still present: %v", err)
	}
	path, err := s.PathByID(1, "fid-1")
	if err != nil {
		t.Fatalf("PathByID: %v", err)
	}
	if path != "new/name" {
		t.Errorf("PathByID = %q, want %q", path, "new/name")
	}
}

func TestPutBaselineSingleCallMoveDropsOrphan(t *testing.T) {
	s, _ := openTemp(t)

	b := sampleBaseline() // file_id fid-1
	if err := s.PutBaseline(1, "old/name", b); err != nil {
		t.Fatalf("PutBaseline old: %v", err)
	}
	// A move expressed as a single Put at the new path (no DeleteBaseline first).
	if err := s.PutBaseline(1, "new/name", b); err != nil {
		t.Fatalf("PutBaseline new: %v", err)
	}
	// The old path must be gone, the index must resolve to the new path, and the
	// view must hold exactly one baseline for fid-1.
	if _, err := s.GetBaseline(1, "old/name"); !errors.Is(err, ErrNotFound) {
		t.Errorf("orphan old path still present: %v", err)
	}
	path, err := s.PathByID(1, "fid-1")
	if err != nil {
		t.Fatalf("PathByID: %v", err)
	}
	if path != "new/name" {
		t.Errorf("PathByID = %q, want %q", path, "new/name")
	}
	if all := baselinesForView(t, s, 1); len(all) != 1 {
		t.Errorf("view holds %d records, want 1 (no orphan)", len(all))
	}
}

func TestPutBaselineEmptyFileIDNotIndexedNorMovedAway(t *testing.T) {
	s, _ := openTemp(t)

	// Two distinct local-only baselines with no server file_id yet. They must not
	// collide in byid (the empty id would collapse to the bare view prefix) and the
	// second Put must not be mistaken for a move of the first.
	local := sampleBaseline()
	local.FileID = ""
	if err := s.PutBaseline(1, "a.txt", local); err != nil {
		t.Fatalf("PutBaseline a: %v", err)
	}
	if err := s.PutBaseline(1, "b.txt", local); err != nil {
		t.Fatalf("PutBaseline b: %v", err)
	}

	// Both records must survive; neither was deleted as a phantom move.
	if _, err := s.GetBaseline(1, "a.txt"); err != nil {
		t.Errorf("GetBaseline a.txt: %v", err)
	}
	if _, err := s.GetBaseline(1, "b.txt"); err != nil {
		t.Errorf("GetBaseline b.txt: %v", err)
	}
	if all := baselinesForView(t, s, 1); len(all) != 2 {
		t.Errorf("view holds %d records, want 2", len(all))
	}

	// An empty file_id is never indexed, so PathByID must not resolve it.
	if _, err := s.PathByID(1, ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("PathByID empty: got %v, want ErrNotFound", err)
	}

	// Assigning a real server id later must index that record without disturbing
	// the other local-only baseline.
	assigned := local
	assigned.FileID = "fid-9"
	if err := s.PutBaseline(1, "a.txt", assigned); err != nil {
		t.Fatalf("PutBaseline assigned: %v", err)
	}
	path, err := s.PathByID(1, "fid-9")
	if err != nil {
		t.Fatalf("PathByID fid-9: %v", err)
	}
	if path != "a.txt" {
		t.Errorf("PathByID fid-9 = %q, want %q", path, "a.txt")
	}
	if _, err := s.GetBaseline(1, "b.txt"); err != nil {
		t.Errorf("GetBaseline b.txt after assign: %v", err)
	}
}

func TestPutBaselineRepointsStaleIndex(t *testing.T) {
	s, _ := openTemp(t)

	first := sampleBaseline() // fid-1
	if err := s.PutBaseline(1, "p", first); err != nil {
		t.Fatalf("PutBaseline first: %v", err)
	}
	// Overwrite the same path with a different file_id (server replaced the file).
	second := sampleBaseline()
	second.FileID = "fid-2"
	if err := s.PutBaseline(1, "p", second); err != nil {
		t.Fatalf("PutBaseline second: %v", err)
	}
	// The stale fid-1 index entry must be gone; fid-2 must resolve to the path.
	if _, err := s.PathByID(1, "fid-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("stale index fid-1 present: %v", err)
	}
	path, err := s.PathByID(1, "fid-2")
	if err != nil {
		t.Fatalf("PathByID fid-2: %v", err)
	}
	if path != "p" {
		t.Errorf("PathByID fid-2 = %q, want %q", path, "p")
	}
}

func TestMoveBaselineIndexedNode(t *testing.T) {
	s, _ := openTemp(t)

	b := sampleBaseline() // fid-1
	if err := s.PutBaseline(1, "old", b); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := s.MoveBaseline(1, "old", "new", b); err != nil {
		t.Fatalf("MoveBaseline: %v", err)
	}
	if _, err := s.GetBaseline(1, "old"); !errors.Is(err, ErrNotFound) {
		t.Errorf("old path survived the move: %v", err)
	}
	got, err := s.GetBaseline(1, "new")
	if err != nil {
		t.Fatalf("GetBaseline new: %v", err)
	}
	if got != b {
		t.Errorf("moved baseline = %+v, want %+v", got, b)
	}
	if path, err := s.PathByID(1, "fid-1"); err != nil || path != "new" {
		t.Errorf("PathByID = %q, %v, want \"new\"", path, err)
	}
}

// TestMoveBaselineEmptyIDNoDuplicate is the crux of the atomic move: a local-only (empty
// file_id) record must move atomically, leaving no copy behind at the source. A
// separate Put+Delete could crash between the two and strand a duplicate at the old
// path that the engine's relocation guard would then skip forever.
func TestMoveBaselineEmptyIDNoDuplicate(t *testing.T) {
	s, _ := openTemp(t)

	local := sampleBaseline()
	local.FileID = ""
	if err := s.PutBaseline(1, "d/x", local); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := s.MoveBaseline(1, "d/x", "e/x", local); err != nil {
		t.Fatalf("MoveBaseline: %v", err)
	}
	if _, err := s.GetBaseline(1, "d/x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("empty-id record left a duplicate at the source: %v", err)
	}
	if _, err := s.GetBaseline(1, "e/x"); err != nil {
		t.Errorf("empty-id record not present at the destination: %v", err)
	}
	if all := baselinesForView(t, s, 1); len(all) != 1 {
		t.Errorf("view holds %d records, want 1 (no duplicate)", len(all))
	}
}

// TestMoveBaselinePreservesForeignDestIndex confirms moving a node onto a path that
// already holds a different indexed node drops only the displaced node's stale byid
// entry, and the moved node's index ends pointing at the destination.
func TestMoveBaselinePreservesForeignDestIndex(t *testing.T) {
	s, _ := openTemp(t)

	moved := sampleBaseline() // fid-1
	if err := s.PutBaseline(1, "src", moved); err != nil {
		t.Fatalf("PutBaseline src: %v", err)
	}
	foreign := sampleBaseline()
	foreign.FileID = "fid-2"
	if err := s.PutBaseline(1, "dst", foreign); err != nil {
		t.Fatalf("PutBaseline dst: %v", err)
	}
	if err := s.MoveBaseline(1, "src", "dst", moved); err != nil {
		t.Fatalf("MoveBaseline: %v", err)
	}
	if _, err := s.PathByID(1, "fid-2"); !errors.Is(err, ErrNotFound) {
		t.Errorf("displaced node's stale index survived: %v", err)
	}
	if path, err := s.PathByID(1, "fid-1"); err != nil || path != "dst" {
		t.Errorf("PathByID fid-1 = %q, %v, want \"dst\"", path, err)
	}
	if _, err := s.GetBaseline(1, "src"); !errors.Is(err, ErrNotFound) {
		t.Errorf("source survived the move: %v", err)
	}
}

// TestMoveBaselineSamePathIsPut confirms fromPath == toPath collapses to a plain
// overwrite rather than deleting the record it just wrote.
func TestMoveBaselineSamePathIsPut(t *testing.T) {
	s, _ := openTemp(t)

	b := sampleBaseline()
	if err := s.MoveBaseline(1, "p", "p", b); err != nil {
		t.Fatalf("MoveBaseline same path: %v", err)
	}
	if got, err := s.GetBaseline(1, "p"); err != nil {
		t.Errorf("same-path move dropped the record: %v", err)
	} else if got != b {
		t.Errorf("baseline = %+v, want %+v", got, b)
	}
	if path, err := s.PathByID(1, "fid-1"); err != nil || path != "p" {
		t.Errorf("PathByID = %q, %v, want \"p\"", path, err)
	}
}

func TestScanBaselinesForViewOrderedAndIsolated(t *testing.T) {
	s, _ := openTemp(t)

	if got := baselinesForView(t, s, 1); len(got) != 0 {
		t.Fatalf("empty view streamed %v, want nothing", got)
	}

	// Insert paths out of order across two views.
	put := func(view uint64, path, fid string) {
		b := sampleBaseline()
		b.FileID = fid
		if err := s.PutBaseline(view, path, b); err != nil {
			t.Fatalf("PutBaseline %d/%q: %v", view, path, err)
		}
	}
	put(1, "c", "v1c")
	put(1, "a", "v1a")
	put(1, "b", "v1b")
	put(2, "z", "v2z")

	got := baselinesForView(t, s, 1)
	wantPaths := []string{"a", "b", "c"}
	if len(got) != len(wantPaths) {
		t.Fatalf("stream len = %d, want %d", len(got), len(wantPaths))
	}
	for i, pb := range got {
		if pb.Path != wantPaths[i] {
			t.Errorf("stream[%d].Path = %q, want %q", i, pb.Path, wantPaths[i])
		}
	}
	// View 2 must be isolated: the prefix cursor never crosses into another view.
	got2 := baselinesForView(t, s, 2)
	if len(got2) != 1 || got2[0].Path != "z" {
		t.Errorf("view 2 streamed %+v, want single path z", got2)
	}
}

// TestScanBaselinesUnderPrefix pins the subtree-bounded stream the engine's directory
// relocate/drop sweeps run on: it yields exactly the records under
// dir + "/", in ascending path order, and DECODES nothing else - the whole point of seeking
// the cursor instead of filtering a whole-view stream in Go. The table also pins the
// boundary cases a naive HasPrefix(path, dir) would get wrong: the directory's own record,
// and a sibling whose name merely starts with the same letters ("d2" next to "d").
func TestScanBaselinesUnderPrefix(t *testing.T) {
	s, _ := openTemp(t)
	all := []string{"a.txt", "d", "d/x", "d/y", "d/sub/z", "d2/keep", "e.txt"}
	for _, p := range all {
		b := sampleBaseline()
		b.FileID = "fid-" + p
		if err := s.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	if err := s.PutBaseline(2, "d/other-view", sampleBaseline()); err != nil {
		t.Fatalf("PutBaseline other view: %v", err)
	}

	for _, tc := range []struct {
		name string
		dir  string
		want []string
	}{
		{name: "subtree", dir: "d", want: []string{"d/sub/z", "d/x", "d/y"}},
		{name: "nested subtree", dir: "d/sub", want: []string{"d/sub/z"}},
		{name: "sibling prefix is its own subtree", dir: "d2", want: []string{"d2/keep"}},
		{name: "leaf has no subtree", dir: "a.txt", want: nil},
		{name: "unknown dir", dir: "nope", want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			skipped, err := s.ScanBaselinesUnderPrefix(1, tc.dir, func(pb PathBaseline) error {
				got = append(got, pb.Path)
				return nil
			})
			if err != nil {
				t.Fatalf("ScanBaselinesUnderPrefix(%q): %v", tc.dir, err)
			}
			if len(skipped) != 0 {
				t.Errorf("skipped = %v, want none", skipped)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("streamed %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("streamed[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestScanBaselinesUnderPrefixEarlyExit pins that a non-nil fn error stops the prefix scan
// and is returned verbatim, mirroring ScanBaselinesForView's sentinel-stop contract.
func TestScanBaselinesUnderPrefixEarlyExit(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"d/a", "d/b", "d/c"} {
		b := sampleBaseline()
		b.FileID = "fid-" + p
		if err := s.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	stop := errors.New("stop")
	seen := 0
	if _, err := s.ScanBaselinesUnderPrefix(1, "d", func(PathBaseline) error {
		seen++
		return stop
	}); !errors.Is(err, stop) {
		t.Fatalf("err = %v, want the stop sentinel", err)
	}
	if seen != 1 {
		t.Errorf("fn called %d times, want 1 (the scan must stop on the first error)", seen)
	}
}

func TestScanBaselinesForViewEmpty(t *testing.T) {
	s, _ := openTemp(t)
	calls := 0
	if _, err := s.ScanBaselinesForView(1, func(PathBaseline) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("ScanBaselinesForView empty: %v", err)
	}
	if calls != 0 {
		t.Errorf("fn called %d times for empty view, want 0", calls)
	}
}

func TestScanBaselinesForViewEarlyExit(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"a", "b", "c"} {
		b := sampleBaseline()
		b.FileID = "f" + p
		if err := s.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	stop := errors.New("stop")
	seen := 0
	_, err := s.ScanBaselinesForView(1, func(pb PathBaseline) error {
		seen++
		if pb.Path == "b" {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("ScanBaselinesForView err = %v, want stop sentinel", err)
	}
	if seen != 2 {
		t.Errorf("fn called %d times, want 2 (stopped at b)", seen)
	}
}

func TestBaselineSurvivesReopen(t *testing.T) {
	s, path := openTemp(t)

	want := sampleBaseline()
	if err := s.PutBaseline(9, "keep/me", want); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetBaseline(9, "keep/me")
	if err != nil {
		t.Fatalf("GetBaseline after reopen: %v", err)
	}
	if got != want {
		t.Errorf("GetBaseline after reopen = %+v, want %+v", got, want)
	}
	p, err := s2.PathByID(9, "fid-1")
	if err != nil {
		t.Fatalf("PathByID after reopen: %v", err)
	}
	if p != "keep/me" {
		t.Errorf("PathByID after reopen = %q, want %q", p, "keep/me")
	}
}

// TestDeleteBaselineCorruptRecordSelfHeals confirms one undecodable baseline value
// cannot wedge its key forever: DeleteBaseline drops the corrupt
// record anyway and sweeps the view's byid entries still pointing at the path, while
// sibling paths and other views stay untouched. Before the tolerant path the decode
// failure aborted the transaction, so every delete of the key re-failed and only a
// full Reset/re-pair cured it.
func TestDeleteBaselineCorruptRecordSelfHeals(t *testing.T) {
	s, _ := openTemp(t)

	corrupt := sampleBaseline() // fid-1
	if err := s.PutBaseline(1, "bad.txt", corrupt); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	sibling := sampleBaseline()
	sibling.FileID = "fid-2"
	if err := s.PutBaseline(1, "good.txt", sibling); err != nil {
		t.Fatalf("PutBaseline sibling: %v", err)
	}
	// Same path string under another view: the sweep must stay view-scoped.
	other := sampleBaseline()
	other.FileID = "fid-3"
	if err := s.PutBaseline(2, "bad.txt", other); err != nil {
		t.Fatalf("PutBaseline other view: %v", err)
	}

	// Corrupt the record in place, bypassing the typed API.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).Put(baselineKey(1, "bad.txt"), []byte("{corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	if _, err := s.GetBaseline(1, "bad.txt"); err == nil {
		t.Fatal("GetBaseline on corrupt record succeeded, want decode error")
	}

	if err := s.DeleteBaseline(1, "bad.txt"); err != nil {
		t.Fatalf("DeleteBaseline on corrupt record: %v", err)
	}
	if _, err := s.GetBaseline(1, "bad.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline after delete = %v, want ErrNotFound", err)
	}
	// The corrupt record's file_id is unknowable, so the sweep must have removed
	// the index entry that pointed at the path.
	if _, err := s.PathByID(1, "fid-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("PathByID(fid-1) after delete = %v, want ErrNotFound (swept)", err)
	}
	// Sibling and other-view records stay fully intact.
	if p, err := s.PathByID(1, "fid-2"); err != nil || p != "good.txt" {
		t.Errorf("PathByID(fid-2) = %q, %v; want good.txt", p, err)
	}
	if p, err := s.PathByID(2, "fid-3"); err != nil || p != "bad.txt" {
		t.Errorf("PathByID(2, fid-3) = %q, %v; want bad.txt", p, err)
	}
	if _, err := s.GetBaseline(2, "bad.txt"); err != nil {
		t.Errorf("GetBaseline other view after delete: %v", err)
	}
	// Deleting the already-gone key stays a no-op.
	if err := s.DeleteBaseline(1, "bad.txt"); err != nil {
		t.Fatalf("second DeleteBaseline: %v", err)
	}
}

// corruptBaselineValue overwrites the baseline record at view/path with an undecodable
// value in place, bypassing the typed API - simulating latent file corruption (bbolt
// checksums only its meta pages).
func corruptBaselineValue(t *testing.T, s *Store, viewID uint64, path string) {
	t.Helper()
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).Put(baselineKey(viewID, path), []byte("{corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt baseline %d/%q: %v", viewID, path, err)
	}
}

// A corrupt baseline value must not poison the whole view's stream:
// before the skip, ScanBaselinesForView returned an error on the first undecodable
// record, so every local reconcile of the view (and the status unresolved count) failed
// forever - even the healthy records around the corrupt one were unreachable. The
// record is skipped and its path reported so callers can Warn and freeze just that path.
func TestScanBaselinesForViewSkipsCorruptValue(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"a.txt", "bad.txt", "c.txt"} {
		b := sampleBaseline()
		b.FileID = "fid-" + p
		if err := s.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	corruptBaselineValue(t, s, 1, "bad.txt")

	var seen []string
	skipped, err := s.ScanBaselinesForView(1, func(pb PathBaseline) error {
		seen = append(seen, pb.Path)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanBaselinesForView over corrupt record: %v", err)
	}
	if len(seen) != 2 || seen[0] != "a.txt" || seen[1] != "c.txt" {
		t.Errorf("fn saw %v, want the healthy [a.txt c.txt] around the corrupt record", seen)
	}
	if len(skipped) != 1 || skipped[0] != "bad.txt" {
		t.Errorf("skipped = %v, want [bad.txt]", skipped)
	}

	// The subtree-bounded stream is symmetrically tolerant: only the corrupt record under
	// the prefix is reported, the healthy ones around it still decode.
	for _, p := range []string{"d/ok", "d/bad"} {
		b := sampleBaseline()
		b.FileID = "fid-" + p // distinct ids: a repeated id would read as a move
		if err := s.PutBaseline(1, p, b); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	corruptBaselineValue(t, s, 1, "d/bad")
	var under []string
	skippedUnder, err := s.ScanBaselinesUnderPrefix(1, "d", func(pb PathBaseline) error {
		under = append(under, pb.Path)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanBaselinesUnderPrefix over corrupt record: %v", err)
	}
	if len(under) != 1 || under[0] != "d/ok" {
		t.Errorf("prefix stream saw %v, want [d/ok]", under)
	}
	if len(skippedUnder) != 1 || skippedUnder[0] != "d/bad" {
		t.Errorf("prefix skipped = %v, want [d/bad] (and never the corrupt record outside the prefix)", skippedUnder)
	}
}

// An early fn exit (the sentinel-stop pattern viewHasBaselines uses)
// must still surface the corrupt paths encountered before the stop, or a corrupt record
// ahead of the match point would silently read as absent.
func TestScanBaselinesForViewEarlyExitReportsSkips(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"a", "b"} {
		bl := sampleBaseline()
		bl.FileID = "fid-" + p
		if err := s.PutBaseline(1, p, bl); err != nil {
			t.Fatalf("PutBaseline %q: %v", p, err)
		}
	}
	corruptBaselineValue(t, s, 1, "a")

	stop := errors.New("stop")
	skipped, err := s.ScanBaselinesForView(1, func(PathBaseline) error {
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("ScanBaselinesForView err = %v, want stop sentinel", err)
	}
	if len(skipped) != 1 || skipped[0] != "a" {
		t.Errorf("skipped = %v, want [a] collected before the early exit", skipped)
	}
}

// A corrupt prior record must not make its key unwritable: the
// overwrite is the one operation that can heal the record, so putBaselineTx treats an
// undecodable prior as absent and sweeps the view's byid entries still pointing at the
// path - before the fix the strict decode-prior failed every Put of the key forever,
// so the view's inbound cursor wedged behind it until Reset/re-pair.
func TestPutBaselineOverCorruptRecordHeals(t *testing.T) {
	cases := []struct {
		name  string
		newID string
	}{
		{name: "same file id", newID: "fid-old"},
		{name: "new file id", newID: "fid-new"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTemp(t)
			prior := sampleBaseline()
			prior.FileID = "fid-old"
			if err := s.PutBaseline(1, "p.txt", prior); err != nil {
				t.Fatalf("PutBaseline prior: %v", err)
			}
			corruptBaselineValue(t, s, 1, "p.txt")

			fresh := sampleBaseline()
			fresh.FileID = tc.newID
			fresh.LocalHash = "ddeeff"
			if err := s.PutBaseline(1, "p.txt", fresh); err != nil {
				t.Fatalf("PutBaseline over corrupt record: %v", err)
			}
			got, err := s.GetBaseline(1, "p.txt")
			if err != nil {
				t.Fatalf("GetBaseline after heal: %v", err)
			}
			if got != fresh {
				t.Errorf("healed record = %+v, want %+v", got, fresh)
			}
			// The corrupt prior's id was swept; the incoming id is (re-)indexed.
			if tc.newID != "fid-old" {
				if _, err := s.PathByID(1, "fid-old"); !errors.Is(err, ErrNotFound) {
					t.Errorf("PathByID(fid-old) = %v, want ErrNotFound (swept)", err)
				}
			}
			if p, err := s.PathByID(1, tc.newID); err != nil || p != "p.txt" {
				t.Errorf("PathByID(%s) = %q, %v; want p.txt", tc.newID, p, err)
			}
		})
	}
}

// A corrupt record at a move's SOURCE must not wedge the relocation:
// MoveBaseline's moved-from cleanup deletes the record anyway (mirroring
// DeleteBaseline's tolerant path) and sweeps the byid entries still pointing at the
// source. The destination write uses a local-only (empty) file id here so the byid
// move inside putBaselineTx cannot pre-delete the source - the strict moved-from
// decode was the only path, and before the fix it failed the whole move forever.
func TestMoveBaselineOverCorruptSourceHeals(t *testing.T) {
	s, _ := openTemp(t)
	src := sampleBaseline()
	src.FileID = "fid-x"
	if err := s.PutBaseline(1, "from", src); err != nil {
		t.Fatalf("PutBaseline source: %v", err)
	}
	corruptBaselineValue(t, s, 1, "from")

	moved := sampleBaseline()
	moved.FileID = ""
	moved.State = StatePending
	if err := s.MoveBaseline(1, "from", "to", moved); err != nil {
		t.Fatalf("MoveBaseline over corrupt source: %v", err)
	}
	if _, err := s.GetBaseline(1, "from"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline(from) = %v, want ErrNotFound (corrupt source deleted)", err)
	}
	if _, err := s.PathByID(1, "fid-x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("PathByID(fid-x) = %v, want ErrNotFound (swept)", err)
	}
	got, err := s.GetBaseline(1, "to")
	if err != nil {
		t.Fatalf("GetBaseline(to): %v", err)
	}
	if got != moved {
		t.Errorf("destination record = %+v, want %+v", got, moved)
	}
}

// TestDeleteBaselineCorruptUnindexedRecord exercises the tolerant delete when the
// corrupt record never had a byid entry: the sweep finds nothing to remove and the
// delete still succeeds.
func TestDeleteBaselineCorruptUnindexedRecord(t *testing.T) {
	s, _ := openTemp(t)

	local := sampleBaseline()
	local.FileID = "" // local-only baseline, never indexed
	if err := s.PutBaseline(3, "pending.txt", local); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).Put(baselineKey(3, "pending.txt"), []byte("not json"))
	})
	if err != nil {
		t.Fatalf("corrupt record: %v", err)
	}

	if err := s.DeleteBaseline(3, "pending.txt"); err != nil {
		t.Fatalf("DeleteBaseline on corrupt unindexed record: %v", err)
	}
	if _, err := s.GetBaseline(3, "pending.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline after delete = %v, want ErrNotFound", err)
	}
}

// TestHasBaselines covers the question the CLI asks before letting a view
// selection change the on-disk layout: is there a synced tree to relocate?
func TestHasBaselines(t *testing.T) {
	s, _ := openTemp(t)

	got, err := s.HasBaselines()
	if err != nil {
		t.Fatalf("HasBaselines: %v", err)
	}
	if got {
		t.Error("a fresh store reports baselines")
	}

	if err := s.PutBaseline(7, "a.txt", Baseline{FileType: "file", LocalHash: "h", State: StateSynced}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	got, err = s.HasBaselines()
	if err != nil {
		t.Fatalf("HasBaselines: %v", err)
	}
	if !got {
		t.Error("a store holding a baseline reports none")
	}
}
