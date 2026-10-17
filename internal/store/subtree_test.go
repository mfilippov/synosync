package store

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// TestDropSubtreeForgetsBaselinesAndQueuedOps: the subtree's baselines (with their byid entries)
// and every queued op with an endpoint in it go; the rest of the view and other views stay.
func TestDropSubtreeForgetsBaselinesAndQueuedOps(t *testing.T) {
	s, _ := openTemp(t)
	for _, p := range []string{"Photos", "Photos/a.jpg", "Docs/b.txt"} {
		if err := s.PutBaseline(1, p, Baseline{FileID: "id-" + p}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutBaseline(2, "Photos/a.jpg", Baseline{FileID: "other-view"}); err != nil {
		t.Fatal(err)
	}
	for _, op := range []Op{
		{Type: OpDownload, ViewID: 1, Path: "Photos/c.jpg"},
		{Type: OpRename, ViewID: 1, Path: "Docs/d.txt", NewPath: "Photos/d.txt"},
		{Type: OpUpload, ViewID: 1, Path: "Docs/e.txt"},
		{Type: OpDownload, ViewID: 2, Path: "Photos/c.jpg"},
	} {
		if _, err := s.Enqueue(op); err != nil {
			t.Fatal(err)
		}
	}
	match := func(p string, _ bool) bool { return p == "Photos" || strings.HasPrefix(p, "Photos/") }
	baselines, ops, err := s.DropSubtree(1, match)
	if err != nil || baselines != 2 || ops != 2 {
		t.Fatalf("DropSubtree = %d, %d, %v; want 2 baselines and 2 ops", baselines, ops, err)
	}
	if _, err := s.PathByID(1, "id-Photos/a.jpg"); !errors.Is(err, ErrNotFound) {
		t.Errorf("byid entry survived: %v", err)
	}
	for _, k := range []struct {
		view uint64
		path string
	}{{1, "Docs/b.txt"}, {2, "Photos/a.jpg"}} {
		if _, err := s.GetBaseline(k.view, k.path); err != nil {
			t.Errorf("baseline %d/%s: %v, want kept", k.view, k.path, err)
		}
	}
	left, err := s.PendingOps()
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 2 || left[0].Path != "Docs/e.txt" || left[1].ViewID != 2 {
		t.Errorf("queue = %+v, want the upload outside and the other view's op", left)
	}
}

// TestMergeRescansKeepsOneEntryPerSubtree.
func TestMergeRescansKeepsOneEntryPerSubtree(t *testing.T) {
	got := MergeRescans([]string{"Photos/2024", "Docs"}, []string{"Photos/2024/x", "Photos", "Docs"})
	if want := []string{"Docs", "Photos"}; !slices.Equal(got, want) {
		t.Errorf("MergeRescans = %v, want %v", got, want)
	}
}
