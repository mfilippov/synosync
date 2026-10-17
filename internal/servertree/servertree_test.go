package servertree

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
)

// fakeLister scripts `list` by the path it is asked for. It is deliberately NOT
// kinder than the measured server: an unscripted path answers the way an
// errno-only reply does - zero children AND listed=false - rather than as an
// empty directory, and a path can be scripted to fail outright.
type fakeLister struct {
	dirs     map[string][]actions.Node
	unlisted map[string]bool
	fail     map[string]error
	asked    []string
}

func (f *fakeLister) List(_ context.Context, _ int64, p string) ([]actions.Node, bool, error) {
	f.asked = append(f.asked, p)
	if err := f.fail[p]; err != nil {
		return nil, false, err
	}
	if f.unlisted[p] {
		return nil, false, nil
	}
	nodes, ok := f.dirs[p]
	if !ok {
		return nil, false, nil
	}
	return nodes, true, nil
}

func dir(name string) actions.Node  { return actions.Node{Name: name, IsDir: true} }
func file(name string) actions.Node { return actions.Node{Name: name, Hash: "h", Size: 1} }

// walkRels runs a walk and returns the directories visited, in order.
func walkRels(t *testing.T, f *fakeLister, opt Options) ([]string, error) {
	t.Helper()
	var seen []string
	err := Walk(context.Background(), f, 7, opt, func(d Dir) error {
		seen = append(seen, d.Rel)
		return nil
	})
	return seen, err
}

// TestWalkVisitsParentsBeforeChildren pins the ordering contract. It is what
// lets a caller enqueue as it goes without sorting the whole tree first: a
// directory's own create is planned before anything inside it.
func TestWalkVisitsParentsBeforeChildren(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/":      {dir("b"), file("f.txt"), dir("a")},
		"a":      {dir("deep")},
		"b":      {file("x.txt")},
		"a/deep": {},
	}}
	seen, err := walkRels(t, f, Options{})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"", "a", "b", "a/deep"}
	if len(seen) != len(want) {
		t.Fatalf("visited %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("visited %v, want %v", seen, want)
		}
	}
}

// TestWalkAddressesTheRootByItsOwnSpelling: the view root is not an empty path
// on the wire.
func TestWalkAddressesTheRootByItsOwnSpelling(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{"/": {}}}
	if _, err := walkRels(t, f, Options{}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(f.asked) != 1 || f.asked[0] != "/" {
		t.Errorf("asked %v, want the root addressed as %q", f.asked, "/")
	}
}

// TestWalkRefusesAnUnevidencedListing is the rule this package exists for.
//
// An errno-only reply iterates as a zero-length list. Reading that as "the
// directory is empty" would hand the caller a subtree that looks server-side
// deleted - and for a first sync that means every local file under it reads as
// local-only and is pushed, and every baseline under it reads as vanished. The
// walk must stop instead, leaving the subtree unknown rather than wrong.
func TestWalkRefusesAnUnevidencedListing(t *testing.T) {
	f := &fakeLister{
		dirs:     map[string][]actions.Node{"/": {dir("quiet")}},
		unlisted: map[string]bool{"quiet": true},
	}
	seen, err := walkRels(t, f, Options{})
	if !errors.Is(err, ErrUnevidenced) {
		t.Fatalf("Walk error = %v, want ErrUnevidenced", err)
	}
	if !strings.Contains(err.Error(), "quiet") {
		t.Errorf("error %q does not name the directory it refused", err)
	}
	// The root was still delivered; only the unprovable subtree stopped the walk.
	if len(seen) != 1 || seen[0] != "" {
		t.Errorf("visited %v, want the root only", seen)
	}
}

// TestWalkRefusesAnUnscriptedDirectory guards the fake's own honesty as much as
// the walk's: a directory nobody scripted is an unevidenced reply, not an empty
// one, so a test that forgets to script a subtree fails loudly.
func TestWalkRefusesAnUnscriptedDirectory(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{"/": {dir("missing")}}}
	if _, err := walkRels(t, f, Options{}); !errors.Is(err, ErrUnevidenced) {
		t.Fatalf("Walk error = %v, want ErrUnevidenced", err)
	}
}

// TestWalkPropagatesAListFailure: a failed turn is not an empty directory either.
func TestWalkPropagatesAListFailure(t *testing.T) {
	boom := errors.New("connection reset")
	f := &fakeLister{
		dirs: map[string][]actions.Node{"/": {dir("broken")}},
		fail: map[string]error{"broken": boom},
	}
	if _, err := walkRels(t, f, Options{}); !errors.Is(err, boom) {
		t.Fatalf("Walk error = %v, want the list failure", err)
	}
}

// TestWalkStopsOnVisitorError lets the caller abort - a planner that cannot
// enqueue must not have the rest of the tree walked at it.
func TestWalkStopsOnVisitorError(t *testing.T) {
	stop := errors.New("planner said no")
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/": {dir("a")},
		"a": {},
	}}
	var visits int
	err := Walk(context.Background(), f, 7, Options{}, func(Dir) error {
		visits++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Walk error = %v, want the visitor's error", err)
	}
	if visits != 1 {
		t.Errorf("visits = %d, want 1 (the walk continued past a refusing visitor)", visits)
	}
}

// TestWalkHonoursCancellation: every unit of work in this client is bounded, and
// a walk driven by server-supplied structure is no exception.
func TestWalkHonoursCancellation(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{"/": {}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Walk(ctx, f, 7, Options{}, func(Dir) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Walk error = %v, want context.Canceled", err)
	}
}

// TestWalkBoundsTheDirectoryCount and TestWalkBoundsTheDepth cover a server whose
// structure does not terminate: without a bound either would hold a worker lease
// indefinitely.
func TestWalkBoundsTheDirectoryCount(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/": {dir("a"), dir("b"), dir("c")},
		"a": {}, "b": {}, "c": {},
	}}
	if _, err := walkRels(t, f, Options{MaxDirs: 2}); !errors.Is(err, ErrTooManyDirs) {
		t.Fatalf("Walk error = %v, want ErrTooManyDirs", err)
	}
}

func TestWalkBoundsTheDepth(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/":     {dir("a")},
		"a":     {dir("b")},
		"a/b":   {dir("c")},
		"a/b/c": {},
	}}
	if _, err := walkRels(t, f, Options{MaxDepth: 2}); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("Walk error = %v, want ErrTooDeep", err)
	}
}

// TestWalkDoesNotDescendIntoAnUnnameableChild: a directory the server reports
// with no name cannot be addressed, so descending would list the parent again
// and never terminate. It still reaches the caller in Children.
func TestWalkDoesNotDescendIntoAnUnnameableChild(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/": {actions.Node{Name: "", IsDir: true}, file("ok.txt")},
	}}
	var got Dir
	err := Walk(context.Background(), f, 7, Options{}, func(d Dir) error { got = d; return nil })
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(f.asked) != 1 {
		t.Errorf("asked %v, want only the root (the unnameable child must not be descended)", f.asked)
	}
	if len(got.Children) != 2 {
		t.Errorf("children = %d, want both reported to the caller", len(got.Children))
	}
}

// TestWalkReportsDepth: the caller uses it to bound its own per-level work.
func TestWalkReportsDepth(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/":   {dir("a")},
		"a":   {dir("b")},
		"a/b": {},
	}}
	depths := map[string]int{}
	if err := Walk(context.Background(), f, 7, Options{}, func(d Dir) error {
		depths[d.Rel] = d.Depth
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	for rel, want := range map[string]int{"": 0, "a": 1, "a/b": 2} {
		if depths[rel] != want {
			t.Errorf("depth of %q = %d, want %d", rel, depths[rel], want)
		}
	}
}

// TestWalkDelegatesADirectoryErrorToTheCaller covers the second way a listing can fail. An
// unevidenced reply says nothing and must always stop the walk; an ERROR may be news the
// caller can corroborate - a directory removed after its parent was listed - and on a view
// large enough to take minutes to walk that is routine, not exceptional. Aborting the whole
// walk for it would leave the view unable to cold-start at all.
func TestWalkDelegatesADirectoryErrorToTheCaller(t *testing.T) {
	gone := errors.New("path error")
	f := &fakeLister{
		dirs: map[string][]actions.Node{
			"/":    {dir("vanished"), dir("kept")},
			"kept": {file("still-here.txt")},
		},
		fail: map[string]error{"vanished": gone},
	}
	var skipped []string
	var seen []string
	err := Walk(context.Background(), f, 7, Options{
		OnDirError: func(rel string, _ error) error { skipped = append(skipped, rel); return nil },
	}, func(d Dir) error { seen = append(seen, d.Rel); return nil })
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(skipped) != 1 || skipped[0] != "vanished" {
		t.Errorf("skipped = %v, want the vanished directory delegated once", skipped)
	}
	if len(seen) != 2 || seen[1] != "kept" {
		t.Errorf("visited = %v, want the walk to continue past the skip", seen)
	}
}

// TestWalkStopsWhenTheCallerRefusesADirectoryError: delegation is the caller's decision, not
// a blanket softening. A handler that returns an error still stops the walk.
func TestWalkStopsWhenTheCallerRefusesADirectoryError(t *testing.T) {
	fatal := errors.New("not a path error")
	f := &fakeLister{
		dirs: map[string][]actions.Node{"/": {dir("broken")}},
		fail: map[string]error{"broken": errors.New("connection reset")},
	}
	err := Walk(context.Background(), f, 7, Options{
		OnDirError: func(string, error) error { return fatal },
	}, func(Dir) error { return nil })
	if !errors.Is(err, fatal) {
		t.Fatalf("Walk error = %v, want the handler's refusal", err)
	}
}

// TestWalkNeverDelegatesTheRoot: a view root that cannot be listed leaves nothing to walk, so
// it is fatal regardless of the handler.
func TestWalkNeverDelegatesTheRoot(t *testing.T) {
	boom := errors.New("root unreadable")
	f := &fakeLister{fail: map[string]error{"/": boom}}
	called := false
	err := Walk(context.Background(), f, 7, Options{
		OnDirError: func(string, error) error { called = true; return nil },
	}, func(Dir) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("Walk error = %v, want the root failure", err)
	}
	if called {
		t.Error("the root was delegated to OnDirError; it must be fatal")
	}
}

// TestWalkRefusesUnsafeChildNames pins the untrusted-reply guard. A `list` reply is server input
// and this walk is the catalog a cold start plans from, so a name that is not one plain component
// must never be joined onto the parent: join uses path.Join, which CLEANS, so "." resolves back to
// the directory being walked and re-queues it - one network turn per iteration under a worker
// lease, with the whole listing re-appended to the catalog each time - while ".." climbs to an
// ancestor and a slash-carrying name fabricates a path the server never enumerated.
//
// The engine's outbound subtree walk has refused these since the recursive-remove precondition was
// written, and INVARIANTS.md states the rule for `list` consumers generally; this walk was the one
// that had not inherited it.
func TestWalkRefusesUnsafeChildNames(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/": {dir("."), dir(".."), dir("a/b"), dir(""), dir("real")},
		// The decisive shape is the NON-root one: path.Join("real", ".") cleans back to "real",
		// so descending into it re-queues the directory being walked, forever.
		"real":    {dir("."), dir("..")},
		"a/b":     {}, // scripted so a walk that DID descend would succeed and be visible
		"sub/sub": {},
	}}
	seen, err := walkRels(t, f, Options{})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"", "real"}
	if len(seen) != len(want) {
		t.Fatalf("visited %v, want exactly %v: every other child name is unjoinable and must be "+
			"left to the caller rather than walked", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("visited %v, want %v", seen, want)
		}
	}
	// The decisive one: "." must not have re-listed the root. Without the guard the walk spins on
	// it until the directory budget trips.
	rootAsks := 0
	for _, p := range f.asked {
		if p == "/" {
			rootAsks++
		}
	}
	if rootAsks != 1 {
		t.Errorf("the root was listed %d times, want exactly 1: a child named \".\" joins back onto "+
			"its own parent, so descending into it re-queues the directory being walked", rootAsks)
	}
}

// TestWalkVisitsEachDirectoryOnce pins the visited set. Plain component names cannot collide
// across different parents, so this only bites on a reply that REPEATS a child - but a server
// repeating one set of names at every level would otherwise be walked combinatorially, spending a
// `list` turn per visit under a worker lease until the budgets trip.
func TestWalkVisitsEachDirectoryOnce(t *testing.T) {
	f := &fakeLister{dirs: map[string][]actions.Node{
		"/":   {dir("a"), dir("a"), dir("a")},
		"a":   {dir("b"), dir("b")},
		"a/b": {},
	}}
	seen, err := walkRels(t, f, Options{})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	want := []string{"", "a", "a/b"}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Errorf("visited %v, want %v: a repeated child must be descended into once", seen, want)
	}
}
