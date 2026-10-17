package engine

// The streaming half of the cold start: planning and transferring while the walk is still
// listing.
//
// The first version was a batch: walk the whole tree, scan, merge, enqueue, and only then let
// the queue drain. On a stand view of 50k files that was three minutes with nothing moved
// before the first byte, and the whole first sync took the walk PLUS the transfers, where
// the two could have overlapped. On a real 500k-file view the silent part alone was a
// quarter of an hour.
//
// Nothing in the merge needs the whole tree. A row's decision depends only on the server
// node and the local entry at one path, and both sides of a directory's children are known
// the moment the directory is listed: the server side from that listing, the local side from
// the scan, indexed by parent. So the walk runs on its own goroutine and hands over each
// directory as it is listed, and the engine loop plans it, enqueues it and keeps draining
// the queue in between (drainQueue calls back between its windows; see coldIngest).
//
// Three things are still decided over the whole pass, and stay where they were:
//   - the cursor is read before the walk and written, with InitialSyncDone, only once the
//     walk has completed and every listing is planned - a crash in between re-runs the cold
//     start, where what already landed adopts without a transfer and what is still queued is
//     skipped as owned;
//   - the local scan is complete before the first directory is planned, so the scan-side
//     collision guard sees every name, as it always did;
//   - a node met twice (it moved while the walk was in flight, so it is listed under both
//     parents) keeps its FIRST report. The batch kept the last, but no choice is right by
//     itself: the move is in the log above the cursor either way, and the incremental poll
//     replays it onto whichever path was planned. First is the one a stream can keep.
//
// The planner and the drain share the engine loop's goroutine and take turns on it, so none
// of the engine's single-goroutine state needs a lock. Only the walk is concurrent, and it
// touches nothing but the pool and the feed.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// coldFeed carries the walk's listings from its goroutine to the engine loop.
//
// It is unbounded on purpose. A bound would stall the walk behind a slow drain window, which
// is the serialisation this file exists to remove, and what it buffers is never more than the
// catalog the batch version held in memory whole.
type coldFeed struct {
	mu   sync.Mutex
	dirs []coldListing
	done bool
	err  error
	wake chan struct{} // capacity 1: "something changed since you last looked"
}

func newColdFeed() *coldFeed { return &coldFeed{wake: make(chan struct{}, 1)} }

func (f *coldFeed) push(l coldListing) {
	f.mu.Lock()
	f.dirs = append(f.dirs, l)
	f.mu.Unlock()
	f.nudge()
}

// finish records the end of the walk and how it ended.
func (f *coldFeed) finish(err error) {
	f.mu.Lock()
	f.done, f.err = true, err
	f.mu.Unlock()
	f.nudge()
}

func (f *coldFeed) nudge() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// take hands over everything listed so far, and whether the walk has ended. done comes WITH the
// last listings: the caller plans what it was handed before acting on done (ingest does).
//
// Holding done back until a later, empty take hung the cold start. The walk's last push and its
// finish nudge the one-slot wake channel back to back, so the two collapse into one token; the
// loop consumes it, takes the last listings with done withheld, and waits on wake for a signal
// that was already spent. Measured: a rescan test hung for 25 minutes in CI.
func (f *coldFeed) take() (dirs []coldListing, done bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dirs, f.dirs = f.dirs, nil
	return dirs, f.done, f.err
}

// coldPlanner folds the walk's listings into queued work one directory at a time.
//
// Each row is decided exactly as the batch merge decided it (planRow): the policy is
// unchanged, only when it runs is. What makes per-directory planning complete is the
// local-only rule: a local entry is decided when its PARENT is listed, and a local
// directory the server does not hold as a directory will never be listed, so its whole
// subtree is decided at once, as local-only (planLocalSubtree).
type coldPlanner struct {
	e        *Engine
	viewID   uint64
	readOnly bool
	viewDir  string
	queued   queuedIndex
	// protected is the scan's unsurfaced paths plus every catalog collision and every node met
	// twice, all by fold key; a directory protects its subtree.
	protected protectedIndex
	// local holds the scan's entries by the fold key of their parent directory, and loses each
	// directory's entries as they are planned.
	local map[string][]scanEntry
	// seen holds the file id of every node planned so far.
	seen map[string]struct{}

	nodes   int // server nodes planned
	planned []plannedOp
	adopted []adoption
	// ops collects what was enqueued, for a caller that wants it (tests); nil keeps nothing.
	ops      *[]store.Op
	hashLess int
	// hashLessFirst names one hash-less adoption, for the pass's single Warn.
	hashLessFirst string
}

// newColdPlanner indexes a complete local scan for per-directory planning and runs the
// scan-wide guards that need every name at once.
func (e *Engine) newColdPlanner(viewID uint64, localEntries []scanEntry, unreadable map[string]bool, readOnly bool) (*coldPlanner, error) {
	return e.newScopedColdPlanner(viewID, localEntries, unreadable, readOnly, true)
}

// newScopedColdPlanner is newColdPlanner for a scan that covered the whole view (wholeView) or
// only a subtree (a rescan, exclude.go). Only a whole-view scan may retire fold-key collision
// memos: a subtree scan says nothing about the keys outside it.
func (e *Engine) newScopedColdPlanner(viewID uint64, localEntries []scanEntry, unreadable map[string]bool, readOnly, wholeView bool) (*coldPlanner, error) {
	// A path the durable queue already OWNS is skipped below. A cold start that failed partway
	// leaves InitialSyncDone false and re-runs the whole pass; without this guard a still-queued
	// op from the prior attempt would be enqueued again as a duplicate.
	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		return nil, err
	}
	// Detect-and-warn NFD/NFC fold-key collisions, mirroring localReconcile: on a
	// case-sensitive byte-preserving filesystem two distinct on-disk names that fold to one
	// normpath.Key would collapse in the merge and silently leave one file unsynced. Every
	// colliding local path is protected, so both stay untouched until the user renames one,
	// and each gets its durable trace (H6). The cold start runs once per view, off its own
	// walk of the root, so there is no availability guard to defer the trace past.
	coldCollisions := scanKeyCollisions(localEntries)
	e.protectCollisions(viewID, coldCollisions, unreadable)
	e.noteCollisionTraces(viewID, coldCollisions, wholeView && len(localEntries) > 0)

	local := make(map[string][]scanEntry)
	for _, en := range localEntries {
		k := normpath.Key(parentRel(en.relPath))
		local[k] = append(local[k], en)
	}
	return &coldPlanner{
		e:         e,
		viewID:    viewID,
		readOnly:  readOnly,
		viewDir:   e.viewDir(viewID, dirCache),
		queued:    queued,
		protected: newProtectedIndex(unreadable),
		local:     local,
		seen:      make(map[string]struct{}),
	}, nil
}

// parentRel is a view-relative path's parent, "" for a top-level entry.
func parentRel(rel string) string {
	if d := path.Dir(rel); d != "." && d != "/" {
		return d
	}
	return ""
}

func (pl *coldPlanner) protect(p string) {
	if pl.protected == nil {
		pl.protected = make(protectedIndex)
	}
	pl.protected[normpath.Key(p)] = true
}

// planDir plans one listed directory: its server children against its local children.
func (pl *coldPlanner) planDir(l coldListing) error {
	e, viewID := pl.e, pl.viewID
	// A protected directory's subtree is left alone on both sides. Its own row already said
	// so in the log; repeating it for every node beneath would bury that one line.
	if l.rel != "" && pl.protected.has(l.rel) {
		return nil
	}
	kept := make([]actions.Event, 0, len(l.children))
	for _, ev := range l.children {
		if ev.FileID != "" {
			if _, dup := pl.seen[ev.FileID]; dup {
				// The node moved while the walk was in flight and was listed under both parents.
				// The first report was planned; the move itself is above the cursor, so the
				// incremental poll replays it. A directory's second subtree is the same nodes
				// again, so it is protected rather than planned twice.
				e.log.Info("engine: cold-start walk met a node twice (it moved mid-walk); keeping its first report "+
					"for the incremental poll to move", "view", viewID, "path", ev.Path)
				if ev.IsDir {
					pl.protect(ev.Path)
				}
				continue
			}
			pl.seen[ev.FileID] = struct{}{}
		}
		kept = append(kept, ev)
	}
	// Two DISTINCT server names that fold to one key share a parent, so a listing is the whole
	// of where such a collision can be - and protecting a colliding directory protects its
	// subtree, which covers the deeper paths the collision would otherwise reach.
	if collisions := catalogKeyCollisions(kept); len(collisions) > 0 {
		marked := make(map[string]bool)
		e.protectCatalogCollisions(viewID, collisions, marked)
		for p := range marked {
			pl.protect(p)
		}
	}
	// Fold the listing onto NFC paths so it keys and matches the NFC scan entries and the NFC
	// baselines written downstream.
	server := make(map[string]actions.Event, len(kept))
	for _, ev := range kept {
		ev.Path = normpath.Normalize(ev.Path)
		server[ev.Path] = ev
	}
	pl.nodes += len(server)
	key := normpath.Key(l.rel)
	localKids := pl.local[key]
	delete(pl.local, key)
	for _, r := range mergeCatalogAndScan(server, localKids) {
		if err := pl.planRow(r); err != nil {
			return err
		}
		// A local directory the server does not hold as a directory is never listed, so
		// nothing later would decide what is under it: decide it now. A server FILE there is
		// the keep-both arm, which moves the whole local directory aside - the batch planned
		// its children as local-only too, and this keeps that.
		if r.local != nil && r.local.isDir && (r.server == nil || !r.server.IsDir) {
			if err := pl.planLocalSubtree(r.local.relPath); err != nil {
				return err
			}
		}
	}
	e.warnIfLargeTree(viewID, pl.nodes, "cold-start walk")
	return pl.flushIfFull()
}

// planLocalSubtree plans everything the scan found beneath dir as local-only, parents first.
func (pl *coldPlanner) planLocalSubtree(dir string) error {
	key := normpath.Key(dir)
	kids := pl.local[key]
	delete(pl.local, key)
	sort.Slice(kids, func(i, j int) bool { return normpath.Key(kids[i].relPath) < normpath.Key(kids[j].relPath) })
	for i := range kids {
		en := kids[i]
		if err := pl.planRow(coldRow{local: &en}); err != nil {
			return err
		}
		if en.isDir {
			if err := pl.planLocalSubtree(en.relPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// planRow decides one merged row under the safe-merge policy and buffers what it plans.
//
// The policy (DESIGN §6, confirmed with the user) never deletes - a path is acted on only when
// present on at least one side, and a baseline-less local-or-server-only path is created, never
// removed:
//   - present on both, both directories → adopt a StateSynced baseline (no transfer);
//   - present on both, both files with equal content hash → adopt StateSynced (no transfer);
//   - present on both, files with differing hashes (or a file↔dir type mismatch) → divergent:
//     enqueue an OpResolveConflict (direction-agnostic keep-both) and mark the baseline
//     StateConflict so localReconcile leaves it to the resolver;
//   - server-only file → inbound OpDownload (StatePending with the server facts);
//   - server-only directory → inbound OpMkdir (StatePending);
//   - local-only file → outbound OpUpload (StatePending with the local facts);
//   - local-only directory → outbound OpMkdir (StatePending).
//
// On a read-only view (the account holds no write privilege) the outbound branches are
// suppressed: a local-only node and a divergent file are logged and left untouched - pushing
// either would only draw a server rejection - while inbound downloads and adoptions of matching
// content still run.
func (pl *coldPlanner) planRow(r coldRow) error {
	e, viewID := pl.e, pl.viewID
	onServer := r.server != nil
	onLocal := r.local != nil
	var ev actions.Event
	var en scanEntry
	if onServer {
		ev = *r.server
	}
	if onLocal {
		en = *r.local
	}
	// Canonical view-relative path for this row: the server is authoritative for path/case
	// when it knows the node, otherwise the local file's own path, so a case-only divergence
	// records under the server's case.
	p := en.relPath
	if onServer {
		p = ev.Path
	}
	// Skip a path the durable queue already OWNS as one of its own endpoints (an op left by a
	// previous, partially-failed cold-start attempt that has not yet drained).
	//
	// `owns` (exact path), not `conflict` (ancestor/descendant overlap). conflict is the
	// EXECUTOR's serialisation rule - "may this path be acted on right now" - and using it to
	// answer "does this row already have an owner" skipped every DESCENDANT of a queued
	// directory too. A cold start that committed some of its batches and then failed leaves
	// exactly such an op queued; on the retry the whole subtree beneath it was dropped from the
	// plan, and the view was nevertheless marked InitialSyncDone at the end. Those server-only
	// nodes then became unreachable: the forward poll starts past them and cold start never runs
	// again. Duplicate work is not a risk either - the executor serialises an ancestor/descendant
	// overlap on its own, so the child's op simply waits behind the directory's.
	if pl.queued.owns(foldedKey(pl.viewDir, p)) {
		return nil
	}
	// A path the scanner observed on disk but could not surface (an unreadable/raced/EACCES
	// file, an internal-artifact name, or a path under an unreadable subtree) is present
	// locally even though it is absent from the surfaced scan entries. Treat it as still-present
	// and leave it untouched - mirroring localReconcile's pathProtected - so a server node at the
	// same path is never downloaded over a local file the scan merely could not read.
	if pl.protected.has(p) {
		e.log.Info("engine: cold-start leaving observed-but-unreadable local path untouched", "view", viewID, "path", p)
		return nil
	}
	if onServer && p == "" {
		// The view's own root (server path "/", which decodes to the empty view-relative path)
		// is not a node to create: the view directory already exists.
		return nil
	}
	// Reject a non-containable server path up front (matching the incremental reconcile), so a
	// traversal/garbled path is skipped before any baseline write rather than being written
	// StatePending only for the executor to dead-letter it. Local scan entries are already
	// containment-filtered by scanView, so this only screens the server side.
	if onServer && !containable(p) {
		pl.e.skipUncontainableServerPath(viewID, "cold-start", p)
		return nil
	}
	switch {
	case onServer && onLocal:
		matched, degraded := coldStartContentMatch(ev, en)
		if matched {
			// Same node on both sides: adopt it as synced with no transfer.
			if degraded {
				pl.hashLess++
				if pl.hashLessFirst == "" {
					pl.hashLessFirst = p
				}
			}
			// Collected rather than written here. An adoption is the whole of a lost-store cold
			// start - every path matches, nothing transfers - so writing one transaction per node
			// put the commit cost back exactly where the batched enqueue took it out, on the case
			// that has the most of them.
			pl.adopted = append(pl.adopted, adoption{path: p, b: adoptedBaseline(ev, en)})
			return nil
		}
		// The server holds a DIRECTORY where disk holds a file: plan the inbound mkdir and let
		// the subtree plan normally behind it.
		//
		// This used to defer the whole subtree and surface the root as an OpResolveConflict,
		// which cannot settle a server directory - keep-both resolves file CONTENT - so it
		// dead-lettered, and the subtree was rescued only by the incremental replay a cursor
		// clamp arranged. A walk has no clamp to offer, and re-walking does not converge: the
		// dead-lettered op is no longer pending, so the queued-path guard does not suppress a
		// re-plan, and every pass re-enqueues the same unresolvable op.
		//
		// The mkdir was always the thing that actually converged it. guardMkdirOccupant is the
		// never-lose gate on it: the occupying local file is set aside whole as a keep-both
		// _Conflict copy, never deleted, and the path it held is freed - which is exactly what
		// the descendants' downloads need. The executor's ancestor/descendant serialisation
		// lands them behind it.
		//
		// It is planned on a READ-ONLY view too, deliberately, and so must sit ahead of the
		// read-only early-return below: an inbound mkdir is not a push. Skipping it there left
		// the view with a collision nothing would ever resolve and nothing an operator could see.
		if ev.IsDir && !en.isDir {
			e.log.Info("engine: cold-start server directory over a local file; planning the mkdir "+
				"whose occupant guard preserves the local file as a keep-both copy",
				"view", viewID, "path", p)
			pl.planned = append(pl.planned, e.planColdInbound(viewID, ev))
			return nil
		}
		// The mirror image - a server FILE where disk holds a directory - needs no special case:
		// the ordinary keep-both resolver moves a whole directory aside just as it moves a file
		// (preserveAside), so it converges on its own. The keep-both push is suppressed on a
		// read-only view, where the local side can never be pushed; the local copy is left
		// untouched.
		if pl.readOnly {
			e.log.Info("engine: cold-start divergence on read-only view; leaving local copy untouched",
				"view", viewID, "path", p)
			return nil
		}
		pl.planned = append(pl.planned, e.planColdConflict(viewID, p, ev, en))
	case onServer:
		// Server-only node: pull it down.
		pl.planned = append(pl.planned, e.planColdInbound(viewID, ev))
	case onLocal:
		// Local-only node: push it up, unless the view is read-only.
		if pl.readOnly {
			e.log.Info("engine: cold-start local-only node on read-only view; not uploading",
				"view", viewID, "path", p)
			return nil
		}
		if en.isDir {
			pl.planned = append(pl.planned, e.planMkdir(viewID, en))
		} else {
			pl.planned = append(pl.planned, e.planUpload(viewID, en, "", en.relPath, "", false))
		}
	}
	return nil
}

// flushIfFull writes the buffered work once a whole batch of it has accumulated.
func (pl *coldPlanner) flushIfFull() error {
	if len(pl.planned) < coldStartEnqueueBatch && len(pl.adopted) < coldStartEnqueueBatch {
		return nil
	}
	return pl.flush()
}

// flush writes everything buffered: the adoptions in one transaction, the planned ops in
// batches of coldStartEnqueueBatch, each op enqueued BEFORE its baseline mutation.
//
// The order rule holds inside a batch just as it did across transactions, and a batch is
// all-or-nothing, so a crash discards whole chunks - chunks that never happened, which the
// next attempt re-derives. Ops are written in the order they were planned, which is parents
// first: a listing's children are planned after the listing that named their directory, and
// a local subtree after its root.
func (pl *coldPlanner) flush() error {
	if err := pl.e.flushAdoptions(pl.viewID, pl.adopted); err != nil {
		return err
	}
	pl.adopted = pl.adopted[:0]
	for len(pl.planned) > 0 {
		n := min(len(pl.planned), coldStartEnqueueBatch)
		chunk := pl.planned[:n]
		err := pl.e.store.Batch(func(b *store.Batch) error {
			for _, op := range chunk {
				if _, err := b.Enqueue(op.op); err != nil {
					return fmt.Errorf("engine: enqueue %s %d/%q: %w", op.op.Type, op.op.ViewID, op.op.Path, err)
				}
				if err := op.apply(b); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if pl.ops != nil {
			for _, op := range chunk {
				*pl.ops = append(*pl.ops, op.op)
			}
		}
		pl.planned = pl.planned[n:]
	}
	pl.planned = nil
	return nil
}

// finish writes what is still buffered and reports the pass's degraded adoptions once.
func (pl *coldPlanner) finish() error {
	if pl.hashLess > 0 {
		// The catalog carried no content hash for these both-present files, so the match was
		// decided on size+kind alone (coldStartContentMatch). One Warn for the pass rather than
		// one per path: a catalog that omits hashes omits them for the whole tree.
		pl.e.log.Warn("engine: cold-start adopted hash-less catalog nodes on size+kind evidence; content equality unverified",
			"view", pl.viewID, "count", pl.hashLess, "first_path", pl.hashLessFirst)
	}
	return pl.flush()
}

// ingest plans everything the walk has listed so far and writes it, so the queue sees it at
// once. done reports that the walk has ended and every listing is planned; walkErr is how the
// walk ended.
func (pl *coldPlanner) ingest(feed *coldFeed) (done bool, walkErr, err error) {
	dirs, done, walkErr := feed.take()
	for _, l := range dirs {
		if err := pl.planDir(l); err != nil {
			return false, walkErr, err
		}
	}
	if len(dirs) > 0 {
		if err := pl.flush(); err != nil {
			return false, walkErr, err
		}
	}
	return done, walkErr, nil
}

// errColdStartPaused reports a cold start a pause request interrupted. It is not a failure:
// the view stays pending and the cold start runs again after the resume.
var errColdStartPaused = errors.New("engine: cold start interrupted by a pause request")

// streamColdStart walks viewID's server tree and plans each listed directory against the local
// scan while the queue drains, returning once every listing is planned and written.
//
// The queue is drained by the ordinary drainQueue, which calls coldIngest between its windows,
// so the pass keeps taking on what the walk lists while it transfers what was planned before.
// A pass ends when the queue does, or once the walk has ended and every listing is planned; if
// the walk is still going, the loop waits for its next listing and starts another pass.
func (e *Engine) streamColdStart(ctx context.Context, viewID uint64, scan func() (*coldPlanner, error)) (*coldPlanner, error) {
	return e.streamColdStartFrom(ctx, viewID, "", scan)
}

// streamColdStartFrom is streamColdStart over one subtree when focus is set (walkServerSubtree).
func (e *Engine) streamColdStartFrom(ctx context.Context, viewID uint64, focus string, scan func() (*coldPlanner, error)) (*coldPlanner, error) {
	// The walk alone is pause-aware: an interrupted walk is simply re-run. The drain keeps the
	// run context, as it does everywhere else, so a pause never surfaces as failed applies.
	wctx, stopWatch := e.pauseAwareCtx(ctx)
	defer stopWatch()
	wctx, cancelWalk := context.WithCancel(wctx)
	feed := newColdFeed()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		feed.finish(e.walkServerSubtree(wctx, viewID, focus, feed.push))
	}()
	defer func() {
		cancelWalk()
		wg.Wait()
	}()

	// The scan runs while the walk lists. Nothing is planned before it completes: the
	// scan-side guards need every local name.
	pl, err := scan()
	if err != nil {
		return nil, err
	}

	var ingestErr error
	e.coldIngest = func() bool {
		if ingestErr != nil {
			return true
		}
		done, _, err := pl.ingest(feed)
		if err != nil {
			ingestErr = err
			return true
		}
		// Once every listing is planned, the pass stops here and the cold start completes:
		// what is still queued is the ordinary cycle's to drain, with the poll and the scan
		// running again in between. Draining it all first kept the cursor unwritten for the
		// whole backlog - a crash in it re-walked the tree - and the poll gated just as long.
		return done
	}
	defer func() { e.coldIngest = nil }()

	for {
		done, walkErr, err := pl.ingest(feed)
		if err != nil {
			return nil, err
		}
		if done {
			if walkErr != nil {
				if errors.Is(context.Cause(wctx), errScanPaused) && ctx.Err() == nil {
					return nil, errColdStartPaused
				}
				return nil, walkErr
			}
			return pl, nil
		}
		drain := e.drainQueue
		if e.coldDrain != nil {
			drain = e.coldDrain
		}
		if err := drain(ctx); err != nil {
			if isTerminal(err) {
				return nil, err
			}
			e.log.Warn("engine: queue drain during the cold start failed; the op will be retried", "view", viewID, "err", err)
		}
		if ingestErr != nil {
			return nil, ingestErr
		}
		select {
		case <-feed.wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
