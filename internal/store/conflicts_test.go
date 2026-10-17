package store

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestConflictRoundTrip(t *testing.T) {
	s, _ := openTemp(t)

	c := Conflict{
		CanonicalPath: "dir/report.txt",
		CopyPath:      "dir/report_host_Jun-11-120000-2026_Conflict.txt",
		CopyFileID:    "fid-99",
		DetectedAt:    1_700_000_000,
	}
	if err := s.PutConflict(7, c); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}

	got, err := s.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListConflicts len = %d, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0], c) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got[0], c)
	}
}

func TestConflictListOrderedAndViewScoped(t *testing.T) {
	s, _ := openTemp(t)

	// Two conflicts in view 1 (out of order) and one in view 2.
	c1b := Conflict{CanonicalPath: "b.txt", CopyPath: "b_copy", CopyFileID: "2"}
	c1a := Conflict{CanonicalPath: "a.txt", CopyPath: "a_copy", CopyFileID: "1"}
	c2 := Conflict{CanonicalPath: "a.txt", CopyPath: "v2_copy", CopyFileID: "3"}
	for _, p := range []struct {
		view uint64
		c    Conflict
	}{{1, c1b}, {1, c1a}, {2, c2}} {
		if err := s.PutConflict(p.view, p.c); err != nil {
			t.Fatalf("PutConflict: %v", err)
		}
	}

	got1, err := s.ListConflicts(1)
	if err != nil {
		t.Fatalf("ListConflicts(1): %v", err)
	}
	if len(got1) != 2 || got1[0].CanonicalPath != "a.txt" || got1[1].CanonicalPath != "b.txt" {
		t.Fatalf("ListConflicts(1) not ascending/scoped: %+v", got1)
	}

	got2, err := s.ListConflicts(2)
	if err != nil {
		t.Fatalf("ListConflicts(2): %v", err)
	}
	if len(got2) != 1 || got2[0].CopyFileID != "3" {
		t.Fatalf("ListConflicts(2): %+v", got2)
	}
}

func TestConflictDelete(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.PutConflict(3, Conflict{CanonicalPath: "x", CopyFileID: "9"}); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}
	if err := s.DeleteConflict(3, "x"); err != nil {
		t.Fatalf("DeleteConflict: %v", err)
	}
	got, err := s.ListConflicts(3)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(got))
	}
	// Deleting an absent record is a no-op.
	if err := s.DeleteConflict(3, "x"); err != nil {
		t.Fatalf("DeleteConflict (absent): %v", err)
	}
}

// TestGetConflict covers the point read the keep-both rollback uses to restore what its
// speculative pre-write displaced: a present record round-trips with found=true, an absent one
// reports found=false without an error, and a bit-rotted VALUE is reported ABSENT (matching
// ScanConflicts' skip-and-warn policy) rather than failing the resolution that reads it.
func TestGetConflict(t *testing.T) {
	s, _ := openTemp(t)

	want := Conflict{CanonicalPath: "a.txt", CopyPath: "a_copy", CopyFileID: "1", DetectedAt: 42}
	if err := s.PutConflict(1, want); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}

	got, found, err := s.GetConflict(1, "a.txt")
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("GetConflict(present) = %+v, %v, %v; want %+v, true, nil", got, found, err, want)
	}
	// Absent in this view, and absent for another view holding no record at all.
	if got, found, err := s.GetConflict(1, "missing.txt"); err != nil || found || got != (Conflict{}) {
		t.Fatalf("GetConflict(absent) = %+v, %v, %v; want zero, false, nil", got, found, err)
	}
	if got, found, err := s.GetConflict(2, "a.txt"); err != nil || found {
		t.Fatalf("GetConflict(other view) = %+v, %v, %v; want zero, false, nil", got, found, err)
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketConflicts).Put(conflictKey(1, "a.txt"), []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt conflict value: %v", err)
	}
	if got, found, err := s.GetConflict(1, "a.txt"); err != nil || found || got != (Conflict{}) {
		t.Fatalf("GetConflict(corrupt) = %+v, %v, %v; want zero, false, nil", got, found, err)
	}
}

func TestConflictPutOverwrites(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.PutConflict(1, Conflict{CanonicalPath: "p", CopyFileID: "old"}); err != nil {
		t.Fatalf("PutConflict: %v", err)
	}
	if err := s.PutConflict(1, Conflict{CanonicalPath: "p", CopyFileID: "new"}); err != nil {
		t.Fatalf("PutConflict overwrite: %v", err)
	}
	got, err := s.ListConflicts(1)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(got) != 1 || got[0].CopyFileID != "new" {
		t.Fatalf("overwrite: %+v", got)
	}
}

// TestListConflictsSkipsCorruptValue pins that a bit-rotted conflict VALUE
// (undecodable JSON under a well-formed key) is skipped rather than aborting the
// whole scan: ListConflicts backs both buildStatus and buildConflicts, run on
// every `synosync status`/`synosync conflicts`, so surfacing the decode error
// here would make exactly the two verbs an operator would diagnose with fail
// forever on one corrupt record (the global poison-pill class already closed
// for the queue/files/views buckets, left open here for conflicts). The healthy
// records around it must still list.
func TestListConflictsSkipsCorruptValue(t *testing.T) {
	s, _ := openTemp(t)

	a := Conflict{CanonicalPath: "a.txt", CopyPath: "a_copy", CopyFileID: "1"}
	c := Conflict{CanonicalPath: "c.txt", CopyPath: "c_copy", CopyFileID: "3"}
	for _, rec := range []Conflict{a, c} {
		if err := s.PutConflict(1, rec); err != nil {
			t.Fatalf("PutConflict %q: %v", rec.CanonicalPath, err)
		}
	}
	// A middle record "b.txt" that is corrupt: healthy siblings sort on both sides.
	if err := s.PutConflict(1, Conflict{CanonicalPath: "b.txt", CopyFileID: "2"}); err != nil {
		t.Fatalf("PutConflict b: %v", err)
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketConflicts).Put(conflictKey(1, "b.txt"), []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt conflict value: %v", err)
	}

	got, err := s.ListConflicts(1)
	if err != nil {
		t.Fatalf("ListConflicts over corrupt value: %v", err)
	}
	if len(got) != 2 || got[0].CanonicalPath != "a.txt" || got[1].CanonicalPath != "c.txt" {
		t.Fatalf("ListConflicts = %+v, want [a.txt c.txt] (corrupt b.txt skipped)", got)
	}
}

// countConflicts is the daemon's `status` counting shape (an unresolved-count walk over
// ScanConflicts) reproduced in the test so the stream's own contract is pinned here.
func countConflicts(s *Store, viewID uint64) (int, error) {
	n := 0
	if err := s.ScanConflicts(viewID, func(Conflict) error {
		n++
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// TestScanConflictsIsViewScopedAndSkipsCorrupt confirms a streaming count over ScanConflicts -
// the shape the daemon's `status` uses - matches
// ListConflicts's length per view without materialising the bucket: it is view-scoped and, like
// ListConflicts, decodes each value so a bit-rotted record is skipped from the count rather than
// counted as a phantom key. The count an operator sees in `status` matches the listing
// `conflicts` shows.
func TestScanConflictsIsViewScopedAndSkipsCorrupt(t *testing.T) {
	s, _ := openTemp(t)

	// Two conflicts in view 1, one in view 2.
	for _, p := range []struct {
		view uint64
		path string
	}{{1, "a.txt"}, {1, "b.txt"}, {2, "c.txt"}} {
		if err := s.PutConflict(p.view, Conflict{CanonicalPath: p.path, CopyFileID: "x"}); err != nil {
			t.Fatalf("PutConflict %s: %v", p.path, err)
		}
	}

	if n, err := countConflicts(s, 1); err != nil || n != 2 {
		t.Fatalf("count(1) = %d, %v; want 2, nil", n, err)
	}
	if n, err := countConflicts(s, 2); err != nil || n != 1 {
		t.Fatalf("count(2) = %d, %v; want 1, nil", n, err)
	}
	if n, err := countConflicts(s, 99); err != nil || n != 0 {
		t.Fatalf("count(99) = %d, %v; want 0, nil", n, err)
	}

	// A corrupt VALUE is skipped from the count, matching ListConflicts.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketConflicts).Put(conflictKey(1, "b.txt"), []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt conflict value: %v", err)
	}
	got, err := s.ListConflicts(1)
	if err != nil {
		t.Fatalf("ListConflicts(1): %v", err)
	}
	n, err := countConflicts(s, 1)
	if err != nil {
		t.Fatalf("count(1) over corrupt: %v", err)
	}
	if n != len(got) || n != 1 {
		t.Fatalf("count(1) over corrupt = %d, want %d (== len(ListConflicts) == 1)", n, len(got))
	}
}

// TestScanConflictsEarlyExit confirms ScanConflicts stops and returns fn's error
// so a bounded-N consumer can abort mid-stream. (The corrupt-record skip is covered by
// TestScanConflictsIsViewScopedAndSkipsCorrupt and TestListConflictsSkipsCorruptValue.)
func TestScanConflictsEarlyExit(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := s.PutConflict(1, Conflict{CanonicalPath: p}); err != nil {
			t.Fatalf("PutConflict %s: %v", p, err)
		}
	}
	sentinel := fmt.Errorf("stop")
	var seen []string
	err := s.ScanConflicts(1, func(c Conflict) error {
		seen = append(seen, c.CanonicalPath)
		if c.CanonicalPath == "b.txt" {
			return sentinel
		}
		return nil
	})
	if err != sentinel {
		t.Fatalf("ScanConflicts err = %v, want sentinel", err)
	}
	if len(seen) != 2 || seen[0] != "a.txt" || seen[1] != "b.txt" {
		t.Fatalf("ScanConflicts visited %v, want [a.txt b.txt] before early exit", seen)
	}
}

// TestMigrateV1ToV2 opens a store, downgrades it to a simulated v1 on-disk state
// (schema_version=1, conflicts bucket removed) while keeping real data, then
// reopens and confirms the schema bumps to v2, the conflicts bucket is present, and
// no existing data is lost. (On reopen bootstrap recreates the conflicts bucket from
// allBuckets and the v1->v2 migration step records the version bump.)
func TestMigrateV1ToV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed some data that must survive the migration.
	if err := s.PutView(5, View{Name: "vw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}

	// Simulate a v1 database: drop the conflicts bucket and stamp schema_version=1.
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketConflicts); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(1))
	})
	if err != nil {
		t.Fatalf("simulate v1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must bring it to v2 with the conflicts bucket present.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}
	err = s2.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketConflicts) == nil {
			t.Error("conflicts bucket missing after migration")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}

	// No data loss.
	gotView, err := s2.GetView(5)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if gotView.Name != "vw" {
		t.Errorf("view name = %q, want %q", gotView.Name, "vw")
	}

	// The conflicts bucket is usable after migration.
	if err := s2.PutConflict(5, Conflict{CanonicalPath: "c", CopyFileID: "1"}); err != nil {
		t.Fatalf("PutConflict post-migrate: %v", err)
	}
	got, err := s2.ListConflicts(5)
	if err != nil || len(got) != 1 {
		t.Fatalf("ListConflicts post-migrate: %v, %+v", err, got)
	}
}

// TestMigrationsMapHasV1Step guards that the runner has a step for every version
// below current; without the v1→v2 entry migrate would reject a real v1 database.
func TestMigrationsMapHasV1Step(t *testing.T) {
	if _, ok := migrations[1]; !ok {
		t.Fatal("missing v1→v2 migration step")
	}
}
