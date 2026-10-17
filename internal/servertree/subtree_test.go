package servertree

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestWalkStartsAtRoot: a subtree rescan walks only that subtree, addressing it by its own path,
// and never lists the view root.
func TestWalkStartsAtRoot(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"a":   {dir("b"), file("f.txt")},
		"a/b": {},
	}}
	seen, err := walkRels(t, f, Options{Root: "a"})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if want := []string{"a", "a/b"}; !slices.Equal(seen, want) {
		t.Fatalf("visited %v, want %v", seen, want)
	}
}

// TestWalkNeverDelegatesASubtreeRoot: the root of a subtree walk is the walk's root, so its
// failure is fatal rather than skipped as a vanished child.
func TestWalkNeverDelegatesASubtreeRoot(t *testing.T) {
	boom := errors.New("gone")
	f := &fakeLister{fail: map[string]error{"a": boom}}
	err := Walk(context.Background(), f, 7, Options{
		Root:       "a",
		OnDirError: func(string, error) error { return nil },
	}, func(Dir) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("Walk error = %v, want the subtree root's failure", err)
	}
}

// TestWalkDoesNotDescendIntoASkippedDirectory: an excluded directory is still reported as its
// parent's child, but its own listing is never requested.
func TestWalkDoesNotDescendIntoASkippedDirectory(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/":    {dir("Photos"), dir("Docs")},
		"Docs": {},
	}}
	var children []string
	err := Walk(context.Background(), f, 7, Options{Skip: func(rel string) bool { return rel == "Photos" }},
		func(d Dir) error {
			for _, c := range d.Children {
				children = append(children, c.Name)
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Walk: %v (a listing of the skipped directory was requested)", err)
	}
	if !slices.Contains(children, "Photos") {
		t.Errorf("children = %v, want the skipped directory still reported", children)
	}
}
