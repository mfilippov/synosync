package engine

// The cold start's catalog source: a top-down walk of the view's CURRENT tree.
//
// It replaces reading pull_event from sync_id 0. That log is HISTORY, so nothing it
// says is final until the whole of it is folded, which is what forced the cold start
// into a batch: read everything, plan everything, only then transfer. On a real
// 190k-node view that was four minutes of log reading plus roughly seventy minutes of
// enqueueing with the network idle, and it carried the entire catalog in memory.
//
// It also removed a class of failure that had nothing to do with the tree. The log is
// paged by a fixed-width sync_id window and holed by compaction, so a window inside a
// hole answers empty; a live install read that as the end of the catalog, stopped at
// cursor 16000 against a head of 856387, and replayed ~840k historical rows through the
// ordinary inbound path - resurrecting directories its owner had reorganised away years
// earlier. A walk asks a different question, one whose answer is final on arrival: what
// is on the server now.

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/servertree"
)

// errNoTreeLister marks a leased client that cannot list. It is a programming error
// (the production client always can), surfaced rather than degraded: a cold start that
// silently produced an empty catalog would read as "the server holds nothing" and plan
// the whole local tree as fresh uploads.
var errNoTreeLister = errors.New("engine: leased client does not support list; cannot walk the view tree")

// errNoLogHeadProbe marks a leased client that cannot report get_max_sync_id. The walk
// needs it BEFORE descending - see coldStartLogHead - so there is nothing to degrade to.
var errNoLogHeadProbe = errors.New("engine: leased client does not support get_max_sync_id; cannot fix a cold-start resume point")

// coldStartLogHead reads the cursor the incremental poll resumes from once viewID's cold
// start completes.
//
// It is read BEFORE the first listing, and that order is the whole of its correctness.
// Everything the walk observes is at least as new as that cursor, so replaying the log
// forward from it re-asserts what the walk already recorded (idempotent) and delivers
// anything that changed while the walk was in flight. Reading it AFTER would invert the
// guarantee: a change made mid-walk, in a directory already passed, would sit below the
// cursor and never be replayed - a silently missed file.
//
// get_max_sync_id LAGS the log (measured 31.07.2026), which is the safe direction here: a
// cursor slightly behind replays a few events the walk already saw, and each of those
// re-asserts state that is already recorded.
func (e *Engine) coldStartLogHead(ctx context.Context, viewID uint64) (int64, error) {
	var head int64
	err := e.withWorker(ctx, func(c syncClient) error {
		if _, ok := c.(treeLister); !ok {
			return errNoTreeLister
		}
		probe, ok := c.(logHeadProbe)
		if !ok {
			return errNoLogHeadProbe
		}
		h, err := probe.MaxSyncID(ctx, int64(viewID), e.viewRootNodeID(viewID)) // #nosec G115 -- a view_id is a non-negative identifier
		if err != nil {
			return fmt.Errorf("engine: cold-start read the log head for view %d: %w", viewID, err)
		}
		head = h
		return nil
	})
	return head, err
}

// coldListing is one directory of the walk as the cold start plans it: the directory's
// view-relative path in the server's spelling and its plannable children.
//
// A directory the walk skipped because it was removed mid-walk (see walkOptions) arrives
// with no children: the server provably holds nothing there now, which is exactly what an
// empty listing says.
type coldListing struct {
	rel      string
	children []actions.Event
}

// walkServerTree walks the view's current tree top-down and hands each directory to emit,
// parents before children, as soon as it is listed.
//
// It runs on its own goroutine while the engine loop plans and transfers what it has
// already emitted, so it touches nothing but the pool, the log and the progress atomics;
// emit must be safe to call from it. Walk's ordering promise carries over: emit sees the
// directories in sequential BFS order whatever the parallelism.
//
// The children carry actions.Event because that is what the reconcile matrix is written
// against; every field but one comes straight off the listed node.
//
// SyncID is left ZERO, and that is not an omission. It reaches the server as a download's
// target_sync_id, which asks for a SPECIFIC version of the node - and a walk has no per-node
// version to ask for. Measured on DSM 7.2.2-72806 / Drive Server 4.0.3-27892: target_sync_id
// 0 downloads the current version, while the pre-walk log head is refused outright (32772,
// and 32769 a little above it). Stamping the head there - on the reasoning that it means
// "the sync id at which this state was observed", which is true of the BASELINE - made every
// download of a first sync fail, permanently, which is the failure this walk exists to
// remove.
//
// The cost is that the baseline records no server sync id for a cold-started node, so the
// guard that orders an inbound delete against a later create disarms for it (it disarms at 0
// by design, the state of every op enqueued before that field existed). The window is the
// gap between the cold start and the first event that refreshes the node, and it is the
// correct trade against downloads that never work at all.
func (e *Engine) walkServerTree(ctx context.Context, viewID uint64, emit func(coldListing)) error {
	return e.walkServerSubtree(ctx, viewID, "", emit)
}

// walkServerSubtree is walkServerTree narrowed to one subtree when focus is set (a rescan,
// exclude.go). It walks from focus's PARENT and reports that listing with focus as its only
// child, so the planner decides the subtree root itself by the ordinary rules - including when
// the server no longer holds it - and then descends into focus alone. Excluded paths are
// neither reported nor entered, focused or not.
func (e *Engine) walkServerSubtree(ctx context.Context, viewID uint64, focus string, emit func(coldListing)) error {
	e.beginWalk()
	defer e.endWalk()
	// Each listing takes its own lease (leasedLister), so the walk keeps one listing in flight
	// per worker instead of holding a single lease for its whole length - on a 500k-file view
	// that was minutes with every other worker idle, and with server contact stamped only once,
	// at the very end, so status showed a silent server throughout.
	lister := leasedLister{e: e}
	e.progress.startListing(viewID)
	defer e.progress.endListing()
	var dirs, nodes, unnamed, skipped int
	opts := e.walkOptions(ctx, lister, viewID, &skipped)
	opts.Parallel = e.limits.Workers
	inScope := func(rel string, kind config.NodeKind) bool {
		if e.excluded(viewID, rel, kind) {
			return false
		}
		return focus == "" || pathUnder(rel, focus)
	}
	if focus != "" {
		opts.Root = parentRel(focus)
	}
	opts.Skip = func(rel string) bool { return !inScope(rel, config.KindDir) }
	// A directory the handler proved gone is still a directory the planner must hear about:
	// its local children are planned against it, and against nothing else.
	onDirError := opts.OnDirError
	opts.OnDirError = func(rel string, err error) error {
		if herr := onDirError(rel, err); herr != nil {
			return herr
		}
		emit(coldListing{rel: rel})
		return nil
	}
	werr := servertree.Walk(ctx, lister, int64(viewID), opts, func(d servertree.Dir) error { // #nosec G115 -- a view_id is a non-negative identifier
		dirs++
		e.progress.noteListed(len(d.Children))
		l := coldListing{rel: d.Rel, children: make([]actions.Event, 0, len(d.Children))}
		for _, n := range d.Children {
			if !safeChildName(n.Name) {
				// A node the server did not name, or named with something that is not one
				// plain component, cannot be addressed - so it cannot be planned for either.
				// Counted and reported once rather than silently dropped: it is the shape
				// that would otherwise plan work at the PARENT. joinRel is path.Join, which
				// CLEANS, so a child called "." collapses onto the directory's own catalog
				// entry and - carrying a different file id - would replace the directory's
				// real node id in the adopted baseline, while a name carrying a separator
				// fabricates a catalog path the server never enumerated. servertree.Walk
				// refuses to DESCEND into such a name; the catalog needs the same refusal,
				// because a name that cannot be addressed is no more plannable as a file
				// than as a directory.
				unnamed++
				continue
			}
			// limits.ColdStartCatalogCap bounds how many catalog nodes one pass may walk, and it
			// is checked per NODE. Checking once per directory instead would let a single wide
			// directory overshoot by its whole breadth: the measured stand serves 10000 children
			// in one listing, so a cap of 1000 would be passed tenfold on the very knob an
			// operator sets to bound the pass.
			if cap := e.limits.ColdStartCatalogCap; cap > 0 && nodes >= cap {
				e.log.Warn("engine: cold-start walk exceeded the catalog cap; aborting "+
					"(raise limits.cold_start_catalog_cap or trim the view)",
					"view", viewID, "count", nodes, "cap", cap)
				return errColdStartCatalogCap
			}
			if !inScope(joinRel(d.Rel, n.Name), nodeKindOf(n.IsDir)) {
				continue
			}
			nodes++
			// The path is left in the server's own spelling. Normalising here would defeat
			// the collision guard downstream: catalogKeyCollisions exists to notice two
			// DISTINCT server paths that fold to one key - the NFD and NFC encodings of one
			// name on a byte-preserving server - and protect both. The planner normalises
			// after that check.
			l.children = append(l.children, actions.Event{
				// A live DSM reports an existing node as "modify", not "create"; the
				// reconcile does not read this field, and saying "create" about a node that
				// has been there for years would be a lie in the logs.
				Action: "modify",
				Path:   joinRel(d.Rel, n.Name),
				FileID: n.FileID,
				IsDir:  n.IsDir,
				Size:   n.Size,
				Hash:   n.Hash,
				MTime:  n.MTime,
			})
		}
		emit(l)
		return nil
	})
	if werr != nil {
		return werr
	}
	if unnamed > 0 {
		e.log.Warn("engine: cold-start walk skipped server nodes reported without a name; "+
			"they cannot be addressed and nothing is planned for them",
			"view", viewID, "count", unnamed)
	}
	if focus != "" {
		e.log.Info("engine: rescan walked the server subtree",
			"view", viewID, "path", focus, "directories", dirs, "nodes", nodes, "skipped_dirs", skipped)
		return nil
	}
	e.log.Info("engine: cold-start walked the server tree",
		"view", viewID, "directories", dirs, "nodes", nodes, "skipped_dirs", skipped)
	return nil
}

// coldStartWalkMaxDirs and coldStartWalkMaxDepth are the walk's budgets; see walkOptions.
const (
	coldStartWalkMaxDirs  = 250_000
	coldStartWalkMaxDepth = 128
)

// walkLimits bounds one tree walk.
// walkOptions bounds one tree walk and says what a FAILED listing means.
//
// The budgets are backstops against a structure that does not terminate, not tuning. They are
// deliberately NOT derived from limits.ColdStartCatalogCap: that knob counts catalog NODES and
// spells unlimited as 0, so feeding it here turned the directory budget off entirely.
func (e *Engine) walkOptions(ctx context.Context, lister treeLister, viewID uint64, skipped *int) servertree.Options {
	return servertree.Options{
		MaxDirs:  coldStartWalkMaxDirs,
		MaxDepth: coldStartWalkMaxDepth,
		OnDirError: func(rel string, err error) error {
			if !errors.Is(err, actions.ErrPathError) {
				return fmt.Errorf("servertree: list %q in view %d: %w", rel, viewID, err)
			}
			// ErrPathError is NOT proof the directory is gone. It is the union of
			// 28672/32769/40961, and 28672 conflates a path error with a server-side TIMEOUT -
			// which is why every verdict this client builds on it is corroborated rather than
			// believed. Skipping on the bare errno would read a slow server as an empty
			// subtree: its files would never be downloaded, the view would still be marked
			// done, and status would report it synced. A silent, permanent hole, which is the
			// exact class this walk exists to close.
			//
			// Corroborate the way the outbound absence provers do, with the one call the walk
			// already has: re-list the PARENT. A directory genuinely removed since its parent
			// was first enumerated is no longer among the parent's children; one that merely
			// failed to answer still is.
			gone, cerr := e.directoryGoneFromParent(ctx, lister, viewID, rel)
			if cerr != nil {
				return cerr
			}
			if !gone {
				return fmt.Errorf("servertree: list %q in view %d: %w (its parent still reports it, "+
					"so the errno is not proof of absence)", rel, viewID, err)
			}
			*skipped++
			e.log.Info("engine: cold-start walk skipped a directory its parent no longer reports; "+
				"it was removed while the walk was in flight",
				"view", viewID, "path", rel)
			return nil
		},
	}
}

// directoryGoneFromParent re-lists rel's parent and reports whether rel is absent from it.
//
// An unevidenced or failed parent listing is inconclusive, and inconclusive is an error here,
// never a skip: the whole point is that a subtree is dropped only on positive evidence. The
// view root has no parent to ask, and servertree never routes it here.
func (e *Engine) directoryGoneFromParent(ctx context.Context, lister treeLister, viewID uint64, rel string) (bool, error) {
	parent := path.Dir(rel)
	if parent == "." || parent == "/" {
		parent = ""
	}
	listPath := parent
	if listPath == "" {
		listPath = "/"
	}
	children, listed, err := lister.List(ctx, int64(viewID), listPath) // #nosec G115 -- a view_id is a non-negative identifier
	if err != nil {
		return false, fmt.Errorf("engine: cold-start re-list %q to corroborate the absence of %q in view %d: %w",
			parent, rel, viewID, err)
	}
	if !listed {
		return false, fmt.Errorf("engine: cold-start re-list of %q carried no evidence, so the absence "+
			"of %q in view %d is unproven", parent, rel, viewID)
	}
	name := path.Base(rel)
	for _, c := range children {
		if c.Name == name {
			return false, nil
		}
	}
	return true, nil
}

// joinRel builds a child's view-relative path under its parent.
func joinRel(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}

// leasedLister is the cold-start walk's Lister: every listing runs on its own worker lease
// through withWorker. That is what lets servertree.Walk keep one listing in flight per worker,
// and - since withWorker is where a completed turn is recorded as server contact - what keeps
// status's contact line current through a walk that may last many minutes.
type leasedLister struct{ e *Engine }

// List lists p in viewID on a freshly leased worker. It is safe for concurrent use: each call
// holds its own lease.
func (l leasedLister) List(ctx context.Context, viewID int64, p string) ([]actions.Node, bool, error) {
	var nodes []actions.Node
	var listed bool
	err := l.e.withWalkWorker(ctx, func(c syncClient) error {
		tl, ok := c.(treeLister)
		if !ok {
			return errNoTreeLister
		}
		var lerr error
		nodes, listed, lerr = tl.List(ctx, viewID, p)
		return lerr
	})
	return nodes, listed, err
}
