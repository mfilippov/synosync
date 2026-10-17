// Package servertree enumerates a Synology Drive view's CURRENT tree by walking
// `list` top-down, one turn per directory.
//
// It exists as the source a cold start reads instead of replaying the event log.
// pull_event(sync_id=0) is history, so no verdict it carries is final until the
// whole log is folded - which forces a cold start into a batch shape: read
// everything, plan everything, and only then transfer. Measured on a real
// 190k-node view that was four minutes of catalog reading followed by roughly
// seventy minutes of enqueueing with the network idle, holding the entire
// catalog in memory throughout.
//
// A walk over `list` answers the only question a first sync actually asks -
// what is on the server NOW - in a form that streams: one directory at a time,
// parents before children, each answer final on arrival. Two premises were
// measured on a live stand (DSM 7.2.2-72806 / Drive Server 4.0.3-27892,
// 24.08.2026) before this was built, and are pinned by
// TestIntegration_BigListPagination and TestIntegration_ListCarriesContentHashes:
// `list` does not truncate a large directory (10000 children, 10000 returned),
// and it reports a per-file content hash, which is what lets a caller adopt a
// local file without transferring it.
package servertree

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/mfilippov/synosync/internal/actions"
)

// viewRootPath is how the view root is addressed by `list`.
const viewRootPath = "/"

// Lister is the single server call a walk makes. It is an interface so the walk
// is driven in tests without a network, and so the engine can supply a
// worker-leased client without this package depending on the engine.
//
// The bool is not decoration: List reports whether the reply carried positive
// evidence, and an errno-only reply iterates as a zero-length list. See
// actions.List.
type Lister interface {
	List(ctx context.Context, viewID int64, p string) ([]actions.Node, bool, error)
}

// Dir is one directory's listing, handed to the visitor.
type Dir struct {
	// Rel is the view-relative, slash-separated path of the directory, empty for
	// the view root.
	Rel string
	// Children is what the server reported, in the order it reported them.
	Children []actions.Node
	// Depth is 0 for the view root, 1 for its children, and so on.
	Depth int
}

// ErrUnevidenced reports a listing that carried no positive evidence.
//
// This is THE rule the walk exists to enforce. An unevidenced reply iterates as
// a zero-length list, and a walk that took that for "the directory is empty"
// would hand its caller a subtree that looks server-side deleted - which, for a
// first sync, means every local file under it reads as local-only and is pushed,
// and every baseline under it reads as vanished. The client's own doctrine
// already refuses this shape elsewhere: an absence needs corroboration, and
// List's emptiness is only evidence when the reply says it is.
var ErrUnevidenced = errors.New("servertree: listing carried no evidence; refusing to read it as an empty directory")

// ErrTooManyDirs and ErrTooDeep bound the walk. Every unit of work in this
// client carries a bound, and a walk driven by server-supplied structure needs
// one that does not depend on the server being well-behaved: a directory graph
// that never terminates would otherwise hold a worker lease indefinitely.
var (
	ErrTooManyDirs = errors.New("servertree: walk exceeded its directory budget")
	ErrTooDeep     = errors.New("servertree: walk exceeded its depth budget")
)

// Options configures one walk. A zero field takes the default.
type Options struct {
	// MaxDirs caps how many directories one walk may visit.
	MaxDirs int
	// MaxDepth caps how deep it may descend.
	MaxDepth int
	// OnDirError, when set, decides what a FAILED listing of a non-root directory means.
	//
	// It exists because the two ways a listing can fail are not the same claim. An
	// unevidenced reply says nothing and is always fatal (ErrUnevidenced). An ERROR may
	// instead be something the caller can CORROBORATE - a directory removed server-side
	// after its parent was listed is a routine race on a view large enough to take minutes
	// to walk - and aborting the whole walk for it means the view never completes a cold
	// start, and so is gated out of both the incremental poll and the local scan.
	//
	// The handler is where that corroboration happens; this package does none of it and
	// takes no view on what an error means. Returning nil skips the subtree and continues,
	// returning an error stops the walk. A caller that skips on an UNCORROBORATED error
	// trades a loud retried failure for a silent permanent hole, because a subtree the walk
	// never recorded is not something the incremental poll goes back for: it reads FORWARD
	// from the cursor.
	//
	// The view ROOT is never routed here: a root that cannot be listed leaves nothing to
	// walk.
	OnDirError func(rel string, err error) error

	// Parallel is how many `list` turns may be in flight at once; zero or one walks one
	// directory at a time.
	//
	// A walk is latency-bound, not bandwidth-bound: one turn per directory, and a live DSM
	// writes a listing's metadata in one- and two-byte TLS records, so a single connection
	// spends most of its time waiting on the server. Measured on a real 500k-file view, a
	// one-at-a-time walk ran for minutes at ~4% of one CPU with the other workers idle.
	//
	// Only the LISTING is concurrent. visit and OnDirError still run on the caller's
	// goroutine, one directory at a time, in exactly the order a sequential walk would use,
	// so every ordering promise of Walk holds unchanged. The Lister must then be safe for
	// concurrent use.
	Parallel int

	// Root, when set, starts the walk at that view-relative directory instead of the view
	// root: a subtree rescan (an exclusion lifted, a node moved in from an excluded path)
	// needs only that subtree. It is treated as the walk's root throughout: never routed to
	// OnDirError, visited at Depth 0.
	Root string

	// Skip, when set, names directories the walk must not descend into: the caller still
	// sees the directory as a child of its parent, but its own listing is never requested.
	// An excluded subtree is not merely ignored by the caller; on a large one, listing it
	// is the bulk of the walk.
	Skip func(rel string) bool
}

const (
	defaultMaxDirs  = 2_000_000
	defaultMaxDepth = 512
)

func (l Options) parallel() int {
	if l.Parallel > 1 {
		return l.Parallel
	}
	return 1
}

func (l Options) maxDirs() int {
	if l.MaxDirs > 0 {
		return l.MaxDirs
	}
	return defaultMaxDirs
}

func (l Options) maxDepth() int {
	if l.MaxDepth > 0 {
		return l.MaxDepth
	}
	return defaultMaxDepth
}

// Walk enumerates the view's tree top-down, calling visit once per directory
// with that directory's children.
//
// Order is the contract, not an implementation detail: a directory is always
// visited BEFORE any of its own children, so a caller that enqueues as it goes
// gets "create the parent before its contents" for free, with no global sort
// over the whole tree. Siblings are descended in name order so two runs over an
// unchanged tree visit in the same order, which is what makes a failed walk
// resumable by simply running it again.
//
// visit is called on the caller's goroutine and may return an error to stop the
// walk; that error is returned unchanged.
//
// A listing that carries no evidence always stops the walk. It is NOT skipped and NOT
// treated as empty: the subtree beneath it is unknown, and every caller of this package
// acts on what it is told is there. A listing that ERRORS is a different claim, and
// Options.OnDirError is where a caller says what it means - see there.
func Walk(ctx context.Context, l Lister, viewID int64, opt Options, visit func(Dir) error) error {
	type pending struct {
		rel   string
		depth int
	}
	// listing is one directory's outcome. fatal is a budget verdict, which stops the walk
	// and is never offered to OnDirError; err is the listing's own failure, which may be.
	type listing struct {
		children []actions.Node
		listed   bool
		err      error
		fatal    error
	}
	queue := []pending{{rel: opt.Root, depth: 0}}
	visited := map[string]struct{}{opt.Root: {}}
	maxDirs, maxDepth, par := opt.maxDirs(), opt.maxDepth(), opt.parallel()

	// Listings are LAUNCHED ahead, up to par past the directory being processed, but
	// PROCESSED strictly in queue order: results[i] belongs to queue[i]. The budgets are
	// charged at launch, which is also queue order, so they trip on the same directory a
	// sequential walk would - and the verdict is held until that directory reaches the head,
	// so every directory before it is still visited first.
	listCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	// Abandon whatever is in flight on any exit, and do not return while a List still holds
	// a lease or writes into a result nobody will read.
	defer func() {
		cancel()
		wg.Wait()
	}()
	var results []chan listing
	seen := 0
	head := 0
	launch := func() {
		for len(results) < len(queue) && len(results)-head < par {
			cur := queue[len(results)]
			ch := make(chan listing, 1)
			results = append(results, ch)
			if cur.depth > maxDepth {
				ch <- listing{fatal: fmt.Errorf("%w: %d at %q", ErrTooDeep, cur.depth, cur.rel)}
				continue
			}
			seen++
			if seen > maxDirs {
				ch <- listing{fatal: fmt.Errorf("%w: %d directories", ErrTooManyDirs, seen)}
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				children, listed, err := l.List(listCtx, viewID, listPath(cur.rel))
				ch <- listing{children: children, listed: listed, err: err}
			}()
		}
	}

	for head < len(queue) {
		if err := ctx.Err(); err != nil {
			return err
		}
		launch()
		cur := queue[head]
		var r listing
		select {
		case r = <-results[head]:
		case <-ctx.Done():
			return ctx.Err()
		}
		results[head] = nil
		head++

		if r.fatal != nil {
			return r.fatal
		}
		if r.err != nil {
			// The root is never delegated: nothing is left to walk without it.
			if cur.rel == opt.Root || opt.OnDirError == nil {
				return fmt.Errorf("servertree: list %q in view %d: %w", cur.rel, viewID, r.err)
			}
			if derr := opt.OnDirError(cur.rel, r.err); derr != nil {
				return derr
			}
			continue
		}
		if !r.listed {
			return fmt.Errorf("%w: %q in view %d", ErrUnevidenced, cur.rel, viewID)
		}

		if err := visit(Dir{Rel: cur.rel, Children: r.children, Depth: cur.depth}); err != nil {
			return err
		}

		// Descend in name order, and only into directories the server named with a name that is
		// actually a child (safeChildName). A name the walk cannot join is still reported in
		// Children - this package does not edit what the server said - and the caller refuses it
		// in turn; see safeChildName.
		subdirs := make([]pending, 0, len(r.children))
		for _, c := range r.children {
			if !c.IsDir || !safeChildName(c.Name) {
				continue
			}
			rel := join(cur.rel, c.Name)
			if opt.Skip != nil && opt.Skip(rel) {
				continue
			}
			// A directory is descended into ONCE. Names are plain components by the check above,
			// so a well-behaved server cannot produce the same path twice - but a reply that
			// repeats a child, or repeats one set of names at every level, would otherwise be
			// walked combinatorially until the budgets tripped, spending a `list` turn per visit
			// under a worker lease. The sibling subtree walk visits by fold key for the same
			// reason.
			if _, seen := visited[rel]; seen {
				continue
			}
			visited[rel] = struct{}{}
			subdirs = append(subdirs, pending{rel: rel, depth: cur.depth + 1})
		}
		sort.Slice(subdirs, func(i, j int) bool { return subdirs[i].rel < subdirs[j].rel })
		queue = append(queue, subdirs...)
	}
	return nil
}

// listPath renders a view-relative directory for `list`. The view root is
// addressed by its own spelling rather than by an empty string.
func listPath(rel string) string {
	if rel == "" {
		return viewRootPath
	}
	return rel
}

// safeChildName reports whether a name a `list` reply gives for a child is one plain path
// component the walk may join onto its parent.
//
// A `list` reply is UNTRUSTED input, and this walk is the client's own catalog source: whatever it
// reports becomes the cold-start plan. join uses path.Join, which CLEANS, so a child called "."
// re-queues the very directory being walked - one network turn per iteration under a worker lease
// until the directory budget trips, with the whole listing re-appended to the catalog each time
// (with no ColdStartCatalogCap set, that is unbounded memory). ".." climbs back to an ancestor,
// and a name carrying a separator addresses somewhere else entirely, fabricating catalog paths
// that were never enumerated. An empty name identifies nothing.
//
// The separator is "/" and ONLY "/", for the reason the sibling walk records: the wire is
// slash-separated, and a backslash is an ordinary rune in a name a POSIX client can hold, so
// refusing it would lock such a subtree out of the catalog rather than protect anything.
//
// The engine's outbound subtree walk (verifyServerSubtreeAgrees) has enforced this since the
// recursive-remove precondition was written; INVARIANTS.md states it as the rule for `list`
// consumers generally. This walk was the one that had not inherited it.
//
// It guards the DESCENT only. Such a child is still reported in Dir.Children, because Children is
// what the server said and this package takes no view on what that means - but a name the walk
// will not join is not one a CALLER may join either, and the cold-start catalog applies the same
// refusal on its side (walkServerCatalog). Guarding one end alone would have been worse than
// guarding neither: before this check a `.` directory re-queued its own parent until the walk
// aborted, so nothing was ever planned, whereas a walk that completes hands the catalog a `.`
// child that collapses onto its parent's own entry.
func safeChildName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}

// join builds a child's view-relative path.
func join(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}
