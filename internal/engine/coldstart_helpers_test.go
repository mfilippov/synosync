package engine

import (
	"context"
	"sort"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// coldStartReconcile drives the streaming planner over a whole catalog at once, the way the
// walk would hand it over: every server directory (and every parent a node names) as one
// listing, parents first. It returns the ops enqueued, in enqueue order.
//
// The matrix tests were written against the batch merge this replaced; feeding them through
// the planner keeps every one of them asserting the policy the production path now runs.
func (e *Engine) coldStartReconcile(viewID uint64, serverNodes map[string]actions.Event, localEntries []scanEntry, unreadable map[string]bool, readOnly bool) ([]store.Op, error) {
	pl, err := e.newColdPlanner(viewID, localEntries, unreadable, readOnly)
	if err != nil {
		return nil, err
	}
	var ops []store.Op
	pl.ops = &ops
	children := map[string][]actions.Event{"": nil}
	addDir := func(d string) {
		for {
			if _, ok := children[d]; ok {
				return
			}
			children[d] = nil
			d = parentRel(d)
		}
	}
	paths := make([]string, 0, len(serverNodes))
	for p := range serverNodes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		ev := serverNodes[p]
		if p == "" {
			continue
		}
		parent := parentRel(p)
		addDir(parent)
		children[parent] = append(children[parent], ev)
		if ev.IsDir {
			addDir(p)
		}
	}
	dirs := make([]string, 0, len(children))
	for d := range children {
		dirs = append(dirs, d)
	}
	depth := func(d string) int {
		if d == "" {
			return 0
		}
		return strings.Count(d, "/") + 1
	}
	sort.Slice(dirs, func(i, j int) bool {
		if di, dj := depth(dirs[i]), depth(dirs[j]); di != dj {
			return di < dj
		}
		return dirs[i] < dirs[j]
	})
	for _, d := range dirs {
		if err := pl.planDir(coldListing{rel: d, children: children[d]}); err != nil {
			return ops, err
		}
	}
	return ops, pl.finish()
}

// walkServerCatalog walks viewID's tree into one slice in walk order, with the cursor read
// before it.
func (e *Engine) walkServerCatalog(ctx context.Context, viewID uint64) ([]actions.Event, int64, error) {
	head, err := e.coldStartLogHead(ctx, viewID)
	if err != nil {
		return nil, 0, err
	}
	var catalog []actions.Event
	err = e.walkServerTree(ctx, viewID, func(l coldListing) { catalog = append(catalog, l.children...) })
	if err != nil {
		return nil, 0, err
	}
	return catalog, head, nil
}

// planOnly stops e's cold starts at the plan: the queue is left as planned, not drained.
func planOnly(e *Engine) *Engine {
	e.coldDrain = func(context.Context) error { return nil }
	return e
}
