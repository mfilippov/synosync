package engine

// This file carries the Phase-6e cold-start full-catalog pull: the first full
// reconciliation a view undergoes when it has never synced before (a fresh pairing or
// a lost store). For such a view there is no incremental cursor to resume from, so the
// engine reads the entire server catalog by WALKING `list` top-down (internal/servertree,
// via walkServerTree), diffs it against a local scan+hash, and folds the two sides
// together under the safe-merge policy
// (server-only → download, equal hash → adopt, local-only → upload, divergent →
// keep-both), one listed directory at a time while the queue drains. Once merged the cursor is written and the view is marked InitialSyncDone
// so the expensive cold start is never repeated; the view then joins the ordinary
// incremental poll + localScan loop.
//
// walkServerTree (coldstartwalk.go) is the catalog reader: it walks `list` top-down and hands
// over each directory as it is listed. coldPlanner (coldstartstream.go) folds each listing
// against the local scan under the safe-merge policy, while the queue drains. coldStartView
// ties the two together per view and coldStartPending drives them from the loop, gating the
// incremental path (pollOnce/localScan) on View.InitialSyncDone so a view only joins the
// ordinary poll + scan once its cold start has run.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/servertree"
	"github.com/mfilippov/synosync/internal/store"
)

// errColdStartCatalogCap marks a cold start aborted because the server catalog grew past
// the configured ColdStartCatalogCap (DESIGN §11). It is deliberately non-terminal: the
// loop logs it and leaves the view InitialSyncDone=false so the cold start is retried on a
// later cycle (after the operator raises the cap or trims the view), rather than crashing
// or OOMing on an oversized tree.
var errColdStartCatalogCap = errors.New("engine: cold-start catalog exceeded configured cap")

// isColdStartCapAbort reports whether err is one of the retryable cold-start budget aborts -
// the catalog-node cap or either of the walk's own directory/depth budgets - so
// coldStartPending arms its cycle-skip backoff on any of them, while an ordinary transient (a
// dropped connection, a decoder-cap breach) retries at the normal cadence.
//
// The walk's budgets belong here as much as the node cap does: a view that trips one of them
// trips it again on the next pass, and re-walking a quarter of a million directories at full
// cycle cadence holds a worker lease for the whole run each time.
func isColdStartCapAbort(err error) bool {
	return errors.Is(err, errColdStartCatalogCap) ||
		errors.Is(err, servertree.ErrTooManyDirs) || errors.Is(err, servertree.ErrTooDeep)
}

// coldStartEnqueueBatch is how many planned ops share one durable transaction.
//
// bolt fsyncs per transaction and rewrites a b-tree path plus the freelist each time, so the
// per-op cost is dominated by the commit, not by the record. Measured on a real cold start of
// a 190k-node view, two transactions per planned op - the queue record, then its baseline -
// ran at 40 ops/s and wrote 47.6 GB of physical I/O into a database that ended up 241 MB. The
// first sync spent over an hour there with the network idle.
//
// The size is a trade between that commit cost and how much work a crash discards: a batch is
// all-or-nothing, so an interrupted chunk is simply re-derived from the catalog on the next
// attempt, which the queued-path guard already makes idempotent. A few hundred keeps the
// discarded work small while amortising the commit far enough that it stops being the
// bottleneck.
const coldStartEnqueueBatch = 256

// coldStartContentMatch decides whether a catalog node and the local scan entry it merged with
// hold the SAME node, and reports whether that verdict had to be degraded for want of a content
// hash.
//
// A directory matches a directory by kind alone (no content to compare), and a type mismatch
// never matches. For two files the proof is the content hash - but the catalog's per-file hash is
// an unconfirmed reply shape (DESIGN §13): a DSM whose event_list omits it would make EVERY
// pre-seeded file compare unequal, turning the single most common onboarding scenario (a tree
// already copied to the sync root) into O(tree) keep-both resolutions, copies and full
// re-transfers of content that is already identical on both sides.
//
// When either side carries no hash the verdict degrades to size+kind, mirroring the download
// verb's hash-less path (skip verification, Warn, keep going) rather than asserting divergence
// from missing evidence. The degrade is reported to the caller so the pass logs it once.
//
// The price is stated honestly: two files of equal size and different content are adopted as
// synced, so the divergence is recorded as agreement instead of being kept-both. The cold start
// itself deletes and overwrites nothing - both sides keep their own bytes, and adoptColdStart
// records the LOCAL hash so the baseline describes what is actually on disk - but the adoption
// is not free afterwards: a later LOCAL edit is diffed against that hash and pushed (the local
// side wins, no loss), while a later SERVER modify is reconciled with the same hash as its
// divergence base, finds the file unchanged since the adopted "agreement" and overwrites it with
// NO _Conflict copy. So the residual is a same-size local divergence lost to the first server
// event on that path. It is still the cheaper error than a tree-wide conflict storm, which
// rewrites every file on disk into a _Conflict copy on the commonest onboarding scenario - and
// it is confined to a hash-less catalog, which the live assert in docs/INTEGRATION.md exists to
// settle.
func coldStartContentMatch(ev actions.Event, en scanEntry) (matched, degraded bool) {
	if ev.IsDir || en.isDir {
		return ev.IsDir && en.isDir, false
	}
	if ev.Hash != "" && en.md4 != "" {
		return ev.Hash == en.md4, false
	}
	return ev.Size == en.size, true
}

// coldRow is one node of the cold-start merge-join: a catalog node and/or a local scan entry
// that share a fold key (normpath.Key - the "same node" identity). A server-only row
// (local == nil) is pulled down, a local-only row (server == nil) is pushed up, and a
// both-present row is adopted or kept-both by content. It replaces the former serverByKey/local
// per-path index maps: the merge already pairs the two sides by key, so the reconcile reads a
// row's two pointers instead of doing a map lookup per path. The pointed-to actions.Event /
// scanEntry are owned by the merge (one copy per row), not aliases into the caller's
// catalog/scan.
type coldRow struct {
	server *actions.Event
	local  *scanEntry
}

// mergeCatalogAndScan folds the catalog (serverNodes, keyed by NFC view-relative path from
// coldStartView) and the in-memory scan entries into one fold-key-ordered slice of coldRows.
// Both sides are keyed by normpath.Key (NFC + case fold on a case-insensitive filesystem, NFC
// identity otherwise) so a catalog node and a local file differing only in case join as the
// same node and never produce a redundant download+upload pair; the merged entities keep their
// original-case paths (ev.Path / en.relPath) so downstream store mutations address the right
// key. Each side is sorted once by fold key and walked in lockstep, replacing the former
// serverByKey/local maps and the seen/keys union slice with a single sorted pass. Duplicate
// fold keys within a side - which a consistent tree never produces (a case-insensitive
// filesystem cannot hold two on-disk names folding alike, and the catalog is collapsed to one
// node per NFC path upstream) - collapse to the last occurrence, mirroring the former maps'
// last-wins. Rows are returned in ascending fold-key order so the cold-start enqueue order
// stays deterministic and parent-before-child.
func mergeCatalogAndScan(serverNodes map[string]actions.Event, localEntries []scanEntry) []coldRow {
	type keyed[T any] struct {
		key string
		val T
	}
	ks := make([]keyed[scanEntry], len(localEntries))
	for i, en := range localEntries {
		ks[i] = keyed[scanEntry]{key: normpath.Key(en.relPath), val: en}
	}
	sort.SliceStable(ks, func(i, j int) bool { return ks[i].key < ks[j].key })

	kv := make([]keyed[actions.Event], 0, len(serverNodes))
	for _, ev := range serverNodes {
		kv = append(kv, keyed[actions.Event]{key: normpath.Key(ev.Path), val: ev})
	}
	sort.SliceStable(kv, func(i, j int) bool { return kv[i].key < kv[j].key })

	// Two-pointer merge over the two fold-key-sorted sides. lastOf collapses a run of equal
	// adjacent keys to its final index (last-wins) and returns the index after the run.
	lastScan := func(i int) (int, scanEntry) {
		k := ks[i].key
		for i+1 < len(ks) && ks[i+1].key == k {
			i++
		}
		return i + 1, ks[i].val
	}
	lastServer := func(j int) (int, actions.Event) {
		k := kv[j].key
		for j+1 < len(kv) && kv[j+1].key == k {
			j++
		}
		return j + 1, kv[j].val
	}
	var rows []coldRow
	i, j := 0, 0
	for i < len(ks) || j < len(kv) {
		switch {
		case j >= len(kv) || (i < len(ks) && ks[i].key < kv[j].key):
			ni, en := lastScan(i)
			i = ni
			rows = append(rows, coldRow{local: &en})
		case i >= len(ks) || kv[j].key < ks[i].key:
			nj, ev := lastServer(j)
			j = nj
			rows = append(rows, coldRow{server: &ev})
		default: // equal fold keys: a tracked node present on both sides
			ni, en := lastScan(i)
			nj, ev := lastServer(j)
			i, j = ni, nj
			rows = append(rows, coldRow{server: &ev, local: &en})
		}
	}
	return rows
}

// catalogKeyCollisions detects two or more DISTINCT catalog paths whose normpath.Key forms
// collide, mirroring scanKeyCollisions on the server side of the cold start. The catalog's
// last-wins collapses (the NFC serverNodes map in coldStartView and mergeCatalogAndScan's
// lastServer) key strictly by normalized path, so without this guard a byte-preserving server
// tree holding both the NFD and NFC encodings of one name - or both cases of one name pulled
// onto a case-insensitive client - silently drops one node from sync. A group whose events all
// carry the same non-empty file id is NOT a collision: that is one node re-reported (a case-only
// rename mid-walk listing it under both spellings). The planner keeps such a node's first report
// before this guard runs (coldPlanner.planDir), so the exemption is defense-in-depth for a caller
// that has not deduped. Returns the shared fold key → sorted distinct colliding paths (length ≥ 2).
func catalogKeyCollisions(creates []actions.Event) map[string][]string {
	type group struct {
		paths map[string]bool
		ids   map[string]bool // distinct FileIDs; "" recorded as its own entry
	}
	byKey := make(map[string]*group, len(creates))
	for _, ev := range creates {
		k := normpath.Key(ev.Path)
		g := byKey[k]
		if g == nil {
			g = &group{paths: make(map[string]bool, 1), ids: make(map[string]bool, 1)}
			byKey[k] = g
		}
		g.paths[ev.Path] = true
		g.ids[ev.FileID] = true
	}
	var collisions map[string][]string
	for k, g := range byKey {
		if len(g.paths) < 2 {
			continue
		}
		// One shared non-empty id across every event: the same node re-reported, not two
		// distinct nodes. An empty id anywhere leaves the identity ambiguous, so the group
		// stays a collision (protect rather than guess).
		if len(g.ids) == 1 && !g.ids[""] {
			continue
		}
		if collisions == nil {
			collisions = make(map[string][]string)
		}
		paths := make([]string, 0, len(g.paths))
		for p := range g.paths {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		collisions[k] = paths
	}
	return collisions
}

// protectCatalogCollisions warns about each catalog-side fold-key collision (view, fold key,
// the colliding server paths and their count) and marks every colliding path protected by
// adding it to unreadable, so coldPlanner's pathProtected guard skips the surviving
// merged row - and, for a directory, its whole subtree by prefix. Neither server node is
// downloaded and any local file at the shared fold key is left untouched, strictly safer than
// the collapse's silent last-wins, which would sync one node and drop the other.
func (e *Engine) protectCatalogCollisions(viewID uint64, collisions map[string][]string, unreadable map[string]bool) {
	for k, paths := range collisions {
		e.log.Warn("engine: cold-start catalog fold-key collision; leaving colliding server nodes unsynced until one is renamed (DESIGN §13)",
			"view", viewID, "fold_key", k, "paths", paths, "count", len(paths))
		for _, p := range paths {
			unreadable[p] = true
		}
	}
}

// adoption is a proved-equal path and the baseline that records it.
type adoption struct {
	path string
	b    store.Baseline
}

// flushAdoptions writes a run of adopted baselines in ONE transaction.
//
// An adoption carries no queue record - there is nothing to transfer - so it has no ordering
// constraint against anything else and batches freely. What it does have is volume: a re-pair
// over an intact tree adopts every file it finds, which is the cold start that does the most
// database writes and the least network work.
func (e *Engine) flushAdoptions(viewID uint64, adopted []adoption) error {
	if len(adopted) == 0 {
		return nil
	}
	return e.store.Batch(func(b *store.Batch) error {
		for _, a := range adopted {
			if err := b.PutBaseline(viewID, a.path, a.b); err != nil {
				return fmt.Errorf("engine: cold-start adopt baseline %d/%q: %w", viewID, a.path, err)
			}
		}
		return nil
	})
}

// adoptedBaseline builds the StateSynced baseline for a path present and matching on both sides
// (same directory, or a file whose local hash equals the server's), recording the server facts
// and overlaying the local size/mtime/perm so the baseline reflects the bytes already on disk
// without any transfer.
func adoptedBaseline(ev actions.Event, en scanEntry) store.Baseline {
	b := baselineFromEvent(ev, store.StateSynced)
	b.UnixPerm = en.unixPerm
	if !ev.IsDir {
		b.LocalSize = en.size
		b.LocalMTime = en.mtime
		// Record the LOCAL hash rather than the catalog's: identical for a hash-proved match
		// (that is what proved it), and the only truthful value for a hash-less catalog adopted
		// on size+kind (coldStartContentMatch's degrade), where baselineFromEvent would otherwise
		// leave LocalHash empty and make every later scan read the file as diverged from its own
		// baseline - re-pushing it forever. ServerHash stays as the catalog reported it (possibly
		// empty), so the missing server-side evidence is not fabricated.
		b.LocalHash = en.md4
	}
	return b
}

// planColdInbound builds the inbound plannedOp for a server-only node: an OpMkdir for a
// directory or an OpDownload for a file, plus the StatePending baseline carrying the server
// facts (baselineFromEvent), mirroring reconcile's create/modify handling for a brand-new node.
func (e *Engine) planColdInbound(viewID uint64, ev actions.Event) plannedOp {
	op := store.Op{
		Direction:    store.DirInbound,
		ViewID:       viewID,
		Path:         ev.Path,
		FileID:       ev.FileID,
		ServerSyncID: ev.SyncID,
	}
	if ev.IsDir {
		op.Type = store.OpMkdir
		op.LocalMTime = ev.MTime
	} else {
		op.Type = store.OpDownload
		// Carry the server facts so applyDownload picks the chunked resume_download path
		// for a large file and can verify the whole-file MD4 of the assembled result
		// (the per-chunk resume replies describe only their chunk); mirrors opForEvent.
		op.LocalHash = ev.Hash
		op.LocalSize = ev.Size
		op.LocalMTime = ev.MTime
	}
	apply := func(w store.Writer) error {
		return w.PutBaseline(viewID, ev.Path, baselineFromEvent(ev, store.StatePending))
	}
	return plannedOp{op: op, apply: apply}
}

// planColdConflict builds the keep-both plannedOp for a path that diverges across the cold
// start: an OpResolveConflict (direction-agnostic; the resolver does its own QueryNode and
// preserves the local copy) plus a StateConflict baseline carrying both the server facts and
// the local facts, so localReconcile leaves the path to the resolver. The op carries the server
// file id (for the resolver's vanished-node disambiguation) and the local perm bits.
func (e *Engine) planColdConflict(viewID uint64, path string, ev actions.Event, en scanEntry) plannedOp {
	op := store.Op{
		Type:      store.OpResolveConflict,
		Direction: store.DirOutbound,
		ViewID:    viewID,
		Path:      path,
		FileID:    ev.FileID,
		UnixPerm:  en.unixPerm,
	}
	apply := func(w store.Writer) error {
		b := baselineFromEvent(ev, store.StateConflict)
		b.LocalHash = en.md4
		b.LocalSize = en.size
		b.LocalMTime = en.mtime
		b.UnixPerm = en.unixPerm
		if err := w.PutBaseline(viewID, path, b); err != nil {
			return fmt.Errorf("engine: cold-start conflict baseline %d/%q: %w", viewID, path, err)
		}
		return nil
	}
	return plannedOp{op: op, apply: apply}
}

// coldStartView runs the full first-sync reconciliation for one view that has never synced:
// it reads the cursor, walks the server tree while scanning the view's on-disk tree, plans
// each listed directory against the scan under the safe-merge policy and drains the queue as
// it goes (streamColdStart), then advances the per-view cursor to the log head read before the
// walk began and marks the view InitialSyncDone so the expensive cold start is never repeated.
// What is still queued when the walk ends drains in the ordinary cycle.
//
// Failure of any step leaves InitialSyncDone false so the next cycle retries the whole cold
// start (it is idempotent: what already landed adopts without a transfer, and a queued path is
// skipped on the retry). A terminal transport error (a vanished session, a closed pool, a
// cancelled context) is propagated so the caller stops Run; any other error is returned for
// coldStartPending to log and retry.
func (e *Engine) coldStartView(ctx context.Context, view store.ListView) error {
	viewID := view.ViewID
	// The catalog is the view's CURRENT tree, walked top-down, not the event log replayed
	// from the beginning of history. See coldstartwalk.go for why that changed, and for why
	// the cursor is read first.
	cursor, err := e.coldStartLogHead(ctx, viewID)
	if err != nil {
		return err
	}
	scan := func() (*coldPlanner, error) {
		// A cold start must establish a trustworthy first baseline, so it fully hashes every
		// file - except one an interrupted run of this same cold start already downloaded or
		// adopted and recorded as synced: its stored hash was verified by this client, and the
		// cheap (size, mtime) check is as good for it as in any incremental pass. A first run has
		// no such records and hashes everything. Measured on a real NAS: a restart 35 GB into a
		// first sync re-read all of it before the first transfer, 45 s, and it grows with the tree.
		hints, err := e.coldStartHints(viewID)
		if err != nil {
			return nil, err
		}
		local, unreadable, err := e.scanView(ctx, viewID, hints)
		if err != nil {
			return nil, fmt.Errorf("engine: cold-start scan view %d: %w", viewID, err)
		}
		pl, err := e.newColdPlanner(viewID, local, unreadable, isReadOnlyView(view.Privilege))
		if err != nil {
			return nil, fmt.Errorf("engine: cold-start reconcile view %d: %w", viewID, err)
		}
		return pl, nil
	}
	pl, err := e.streamColdStart(ctx, viewID, scan)
	if err != nil {
		return err
	}
	if err := pl.finish(); err != nil {
		return fmt.Errorf("engine: cold-start reconcile view %d: %w", viewID, err)
	}
	// Record the view root's filesystem device id now that the cold start has completed against
	// a healthy mount (the catalog pull and on-disk scan both succeeded), mirroring the
	// non-empty branch of localReconcile, so the empty-scan mount-swap guard has a baseline to
	// compare against from the very FIRST empty scan of a freshly cold-started view.
	// Without a recording here recordViewRootDevice is reached only from that
	// non-empty branch, so a view whose synced baselines are all born of server downloads - the
	// canonical fresh cold start - carries RootDevice == 0 until its first healthy non-empty
	// scan; a mount that drops in that window presents as an ordinary empty directory
	// viewRootDeviceChanged is a no-op against (it returns false at RootDevice == 0), and the
	// empty-scan debounce erases the whole remote view.
	//
	// recordViewRootDevice reads the device id by stat-ing the view root, so for a SERVER-ONLY
	// view (no local files to adopt) it would no-op: the root does not exist on disk until the
	// queued downloads drain, and the stat fails. That leaves the exact window above open
	// between the downloads landing and the first non-empty scan - the case
	// the adoption path, whose root already exists, does not exercise. Materialise the root now,
	// while the mount is proven healthy (the catalog pull and scan just succeeded against it and
	// this Mkdir is itself a successful write to it), so the device id is captured for a
	// server-download-born view exactly as it is for the adoption case. Gated on a non-empty
	// catalog - server content the downloads will fill the root with anyway, so the eager create
	// is not a new artefact, only earlier; a truly-empty view has no synced baselines, so its
	// empty-scan guard never fires and a bare empty directory would be pointless. Best-effort: a
	// Mkdir failure (a read-only or absent mount) just leaves the recording to the first
	// non-empty scan as before. A belt-and-braces capture at empty-scan time is deliberately NOT
	// added: by then the mount may already have dropped, so it would seed the wrong (parent-fs)
	// device as the healthy baseline and still converge to a whole-view delete - the device must
	// be captured while the mount is KNOWN healthy, which this path and the non-empty scan both do.
	if pl.nodes > 0 {
		if _, viewAbs, rerr := e.resolveViewDir(viewID); rerr == nil {
			if err := fsutil.Mkdir(viewAbs); err != nil {
				e.log.Warn("engine: could not pre-create cold-start view root for the mount-swap guard; device id left to the first non-empty scan",
					"view", viewID, "err", err)
			}
		}
	}
	e.recordViewRootDevice(viewID)
	// Persist the cursor and the done flag together: advancing the cursor stops the incremental
	// poll from re-pulling what the merge already turned into queued ops. The value is the log
	// head coldStartLogHead read BEFORE the walk, so everything the walk recorded is at least
	// that new and replaying forward from it re-asserts rather than contradicts. The view record
	// was upserted by refreshViews, so GetView must find it.
	v, err := e.store.GetView(viewID)
	if err != nil {
		return fmt.Errorf("engine: cold-start load view %d: %w", viewID, err)
	}
	if cursor > v.LastSeenSyncID {
		v.LastSeenSyncID = cursor
	}
	v.InitialSyncDone = true
	// The whole tree was just merged, so every pending subtree rescan is covered.
	v.Rescan = nil
	if err := e.store.PutView(viewID, v); err != nil {
		return fmt.Errorf("engine: cold-start mark view %d done: %w", viewID, err)
	}
	e.log.Info("engine: cold start complete", "view", viewID, "cursor", v.LastSeenSyncID)
	return nil
}

// coldStartPending runs the cold start for every view that has not yet completed its initial
// full-catalog pull (View.InitialSyncDone false). It is called at the top of each cycle, before
// the incremental scan/poll, so a fresh pairing or a lost store performs the one-time full
// reconciliation before the ordinary path touches the view. A per-view failure is logged and
// the view stays pending for the next cycle; only a terminal error (a vanished session, a closed
// pool, a cancelled context) is returned for Run to stop on.
func (e *Engine) coldStartPending(ctx context.Context) error {
	views, err := e.store.ListViews()
	if err != nil {
		return fmt.Errorf("engine: list views for cold start: %w", err)
	}
	for _, v := range views {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.viewSuppressed(v.ViewID) {
			// A view removed/revoked server-side must not run its cold-start catalog
			// pull: its view id is gone from the server, so the pull would fail every
			// cycle. Skip it until it reappears in the enumeration.
			continue
		}
		if v.InitialSyncDone {
			// Completed: drop any lingering cap backoff so a future re-pairing of the same
			// view id starts fresh rather than inheriting a stale skip count.
			e.clearColdStartCapBackoff(v.ViewID)
			continue
		}
		// Back off a view whose cold start keeps aborting on the catalog/page cap: skip this
		// cycle while its per-view counter counts down, rather than re-paginating the entire
		// pre-cap catalog from the server every cycle. An ordinary
		// transient (a dropped connection) does not arm the backoff, so it still retries at
		// the normal cadence.
		if e.coldStartCapSkip[v.ViewID] > 0 {
			e.coldStartCapSkip[v.ViewID]--
			continue
		}
		if err := e.coldStartView(ctx, v); err != nil {
			if isTerminal(err) {
				return err
			}
			if errors.Is(err, errColdStartPaused) {
				e.log.Info("engine: cold start interrupted by a pause; it runs again after the resume", "view", v.ViewID)
				continue
			}
			if isColdStartCapAbort(err) {
				e.armColdStartCapBackoff(v.ViewID)
			}
			e.log.Warn("engine: cold start failed; will retry next cycle", "view", v.ViewID, "err", err)
			continue
		}
		// The view completed its cold start (InitialSyncDone is now true); clear the backoff.
		e.clearColdStartCapBackoff(v.ViewID)
	}
	return nil
}

// armColdStartCapBackoff schedules the next cold-start attempt for a view further out under a
// doubling cycle-skip backoff (1, 2, 4 … capped at coldStartCapMaxSkip), so a view that keeps
// aborting on the catalog or page cap is re-paginated with diminishing frequency rather than
// every cycle (mirroring pollOnce's rootIDRetry backoff).
func (e *Engine) armColdStartCapBackoff(viewID uint64) {
	next := e.coldStartCapNext[viewID]
	if next < 1 {
		next = 1
	}
	e.coldStartCapSkip[viewID] = next
	if next < coldStartCapMaxSkip {
		next *= 2
		if next > coldStartCapMaxSkip {
			next = coldStartCapMaxSkip
		}
	}
	e.coldStartCapNext[viewID] = next
}

// clearColdStartCapBackoff drops a view's cold-start cap backoff state (on completion or when
// the view no longer needs a cold start), so a later regression retries promptly.
func (e *Engine) clearColdStartCapBackoff(viewID uint64) {
	delete(e.coldStartCapSkip, viewID)
	delete(e.coldStartCapNext, viewID)
}

// coldStartHints is the cheap-scan table for a cold start: the files this view already holds as
// synced (an interrupted earlier run of the same cold start downloaded or adopted them). Pending,
// conflicted and directory records give no hint, so everything else is hashed in full.
func (e *Engine) coldStartHints(viewID uint64) (map[string]scanHint, error) {
	hints := make(map[string]scanHint)
	if _, err := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		if pb.State == store.StateSynced && pb.FileType != "dir" && pb.LocalHash != "" {
			hints[normpath.Key(pb.Path)] = scanHint{size: pb.LocalSize, mtime: pb.LocalMTime, hash: pb.LocalHash}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("engine: cold-start read baselines of view %d: %w", viewID, err)
	}
	return hints, nil
}
