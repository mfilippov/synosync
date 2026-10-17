package servertree

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// slowLister makes a fakeLister safe for concurrent use and slow in a path-dependent way, so
// listings launched together complete out of order - the condition a parallel walk must not
// let leak into its visit order. It records how many listings were in flight at once.
type slowLister struct {
	mu       sync.Mutex
	inner    *fakeLister
	inflight atomic.Int32
	peak     atomic.Int32
}

func (s *slowLister) List(ctx context.Context, viewID int64, p string) ([]actions.Node, bool, error) {
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for {
		old := s.peak.Load()
		if n <= old || s.peak.CompareAndSwap(old, n) {
			break
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(p))
	select {
	case <-time.After(time.Duration(h.Sum32()%5) * time.Millisecond):
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.List(ctx, viewID, p)
}

// tree builds a view `depth` levels deep with `fan` subdirectories and one file per directory.
func tree(fan, depth int) *fakeLister {
	f := &fakeLister{dirs: map[string][]actions.Node{}}
	var build func(rel string, d int)
	build = func(rel string, d int) {
		key := listPath(rel)
		nodes := []actions.Node{file("f")}
		if d < depth {
			for i := fan - 1; i >= 0; i-- { // reverse, so the walk's own name sort matters
				name := fmt.Sprintf("d%d", i)
				nodes = append(nodes, dir(name))
				build(join(rel, name), d+1)
			}
		}
		f.dirs[key] = nodes
	}
	build("", 0)
	return f
}

func walkWith(t *testing.T, l Lister, opt Options) ([]string, error) {
	t.Helper()
	var seen []string
	err := Walk(context.Background(), l, 7, opt, func(d Dir) error {
		seen = append(seen, d.Rel)
		return nil
	})
	return seen, err
}

// TestWalkParallelVisitsInTheSequentialOrder pins what makes concurrency safe to turn on: the
// caller sees exactly the directories, in exactly the order, a one-at-a-time walk produces,
// while the listings really do overlap - and never more of them than asked for.
func TestWalkParallelVisitsInTheSequentialOrder(t *testing.T) {
	want, err := walkWith(t, tree(4, 3), Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := &slowLister{inner: tree(4, 3)}
	got, err := walkWith(t, s, Options{Parallel: 4})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("parallel visit order differs from sequential:\n got  %v\n want %v", got, want)
	}
	if p := s.peak.Load(); p < 2 || p > 4 {
		t.Errorf("peak listings in flight = %d, want overlap bounded by Parallel (2..4)", p)
	}
}

// TestWalkParallelTripsTheBudgetOnTheSameDirectory: the budgets are charged in queue order, so
// a parallel walk stops on the directory a sequential one would, having visited the same ones.
func TestWalkParallelTripsTheBudgetOnTheSameDirectory(t *testing.T) {
	for _, opt := range []Options{{MaxDirs: 7}, {MaxDepth: 1}} {
		want, wantErr := walkWith(t, tree(3, 3), opt)
		par := opt
		par.Parallel = 4
		got, gotErr := walkWith(t, &slowLister{inner: tree(3, 3)}, par)
		// The message carries the count or the path the budget tripped on, so equal text is the
		// same verdict on the same directory.
		if wantErr == nil || gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("%+v: errors sequential=%v parallel=%v", opt, wantErr, gotErr)
		}
		if !errors.Is(gotErr, ErrTooManyDirs) && !errors.Is(gotErr, ErrTooDeep) {
			t.Fatalf("%+v: parallel error %v is not a budget verdict", opt, gotErr)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%+v: visited %v before the budget, want %v", opt, got, want)
		}
	}
}

// TestWalkParallelLeavesNothingInFlight: a walk that stops early - here on a listing failure -
// returns only once every listing it launched has finished, so no lease or goroutine outlives it.
func TestWalkParallelLeavesNothingInFlight(t *testing.T) {
	inner := tree(4, 3)
	boom := errors.New("boom")
	inner.fail = map[string]error{"d1": boom}
	s := &slowLister{inner: inner}
	_, err := walkWith(t, s, Options{Parallel: 4})
	if !errors.Is(err, boom) {
		t.Fatalf("Walk error = %v, want the listing failure", err)
	}
	if n := s.inflight.Load(); n != 0 {
		t.Errorf("%d listings still in flight after Walk returned", n)
	}
}
