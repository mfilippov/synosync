package engine

// This file carries the Phase-6c local reconcile pass: it diffs a view's on-disk scan
// (scanner.go) against the stored baselines and turns the differences into the outbound
// (DirOutbound) durable-queue ops the outbound executor (outbound.go) drains. It
// is the push-side mirror of reconcile.go's remote-event pass: pure policy over the store
// and the filesystem scan, no network, so its golden scenarios (create/modify/delete/
// mkdir/move) are exercised against an in-memory store and temp dirs.
//
// Authority model (variant 1): the server is authoritative for inbound, so an outbound op
// is generated only from a baseline already in StateSynced - local and server agreed at
// the recorded facts and the local side has since diverged. A baseline in StatePending or
// StateConflict is owned by the inbound apply path or the Phase-6d conflict
// resolution and is left untouched here. A path that already has a queued op (inbound or
// outbound) is skipped too, so a slow push is not re-enqueued on every scan.
//
// Conflict detection is NOT done here. The push relies on the server's stale-base
// rejection (ErrConflictVersion) as the conflict seam: a rejected push marks the baseline
// StateConflict (outbound executor), never silently overwriting a diverged remote.
// Keep-both resolution (6d, conflict.go) and the cold-start full-catalog pull
// (6e, coldstart.go) are both implemented.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// emptyScanConfirmMinSpacing is the WALL-CLOCK floor between two empty scans counted toward the
// whole-view-delete debounce. limits.EmptyScanThreshold counts SCANS, and a
// scan count is not a duration: the default LocalScanInterval is 5 seconds and a watcher wake
// resets that throttle outright (loop.go), so an fsnotify burst - precisely what an unmounting
// filesystem produces - can deliver the whole threshold inside the seconds during which the mount
// is going away. The debounce exists to tell a transient unavailability from a real empty-out, and
// only elapsed time distinguishes them, so a counted empty scan additionally needs this much wall
// clock since the last counted one. It is the sibling of absenceStreakMinAge and of
// unmappedServerErrMinAge, whose doctrine it carries: pass count alone is not a duration.
//
// A scan inside the floor is still suppressed (it produces no ops), so nothing is lost - the run
// stays intact and the next scan past the floor advances it. A genuine whole-view delete therefore
// converges in (threshold-1) × this floor rather than immediately, which is the price paid for not
// erasing a remote view because a mount vanished noisily.
const emptyScanConfirmMinSpacing = 60 * time.Second

// emptyScanStreak is the per-view evidence that a view root's emptiness PERSISTS: how many scans
// have been counted toward limits.EmptyScanThreshold and when the last counted one landed (the
// wall-clock floor emptyScanConfirmMinSpacing is measured from).
type emptyScanStreak struct {
	count   int
	counted time.Time
}

// noteEmptyViewScan folds one observed-empty scan of a view whose baselines are still synced into
// its debounce and reports whether it COUNTED. A scan within emptyScanConfirmMinSpacing of the last
// counted one does not: it is the same suspicious state observed twice in one window, not two
// observations of a persisting one. It deliberately leaves the entry in place (rather than resetting
// it, which the unavailable/probe-error branches do) - the emptiness genuinely repeated, only too
// soon to be independent evidence - so the first scan past the floor advances the run instead of
// restarting it, and a real empty-out still converges.
func (e *Engine) noteEmptyViewScan(viewID uint64) (emptyScanStreak, bool) {
	now := e.now()
	st, tracked := e.emptyViewScans[viewID]
	if tracked && now.Sub(st.counted) < emptyScanConfirmMinSpacing {
		return st, false
	}
	st = emptyScanStreak{count: st.count + 1, counted: now}
	e.emptyViewScans[viewID] = st
	return st, true
}

// foldedKey resolves a view-relative path to its on-disk conflict key (cleanKey) and folds
// it for case-insensitive matching. The durable-queue index built from these keys then
// treats two ops whose on-disk paths differ only in case as the same node on a
// case-insensitive filesystem (and as distinct nodes on a case-sensitive one, where Fold is
// the NFC identity), so a still-draining push at "Foo" suppresses a re-detected "foo".
func foldedKey(viewDir, p string) string {
	return normpath.Fold(cleanKey(viewDir, p))
}

// protectedIndex is the fold-keyed prefix index over the paths the scanner observed on disk
// but did NOT surface as entries (scanView's second return): an unreadable/raced file, an
// internal-artifact name, a path behind a symlinked component, or anything under an unreadable
// subtree. A path in - or beneath - that set must not be treated as a deletion this scan;
// without the guard it would diff as "vanished" and emit a spurious OpRemove, deleting a
// present remote file.
//
// It is the indexed form of the former linear pathProtected scan, which compared every
// candidate against EVERY protected key and re-folded both sides on each comparison -
// O(baselines × protected) normpath.Key calls per pass, with the protected set growing exactly
// in the situations (an unreadable subtree, a symlinked view-name component) that make it
// largest. The index folds each protected key once at construction and answers a candidate in
// O(depth) map lookups, mirroring queuedIndex's shape.
type protectedIndex map[string]bool

// newProtectedIndex folds the scanner's unsurfaced-path set into the index. An empty set
// yields a nil index, for which has reports false without hashing anything.
func newProtectedIndex(unreadable map[string]bool) protectedIndex {
	if len(unreadable) == 0 {
		return nil
	}
	idx := make(protectedIndex, len(unreadable))
	for k := range unreadable {
		idx[normpath.Key(k)] = true
	}
	return idx
}

// has reports whether path is protected: it is itself an unsurfaced path, or it sits beneath
// one (a directory key protects its whole subtree by prefix - the walk of a candidate's own
// slash-separated ancestors answers exactly what keyUnder does, since every protected key
// is stored in the same fold-normalized form - this walks the ancestors instead so the cost
// is the candidate's depth rather than the index's size).
func (p protectedIndex) has(path string) bool {
	if len(p) == 0 {
		return false
	}
	key := normpath.Key(path)
	for {
		if p[key] {
			return true
		}
		i := strings.LastIndexByte(key, '/')
		if i < 0 {
			return false
		}
		key = key[:i]
	}
}

// queuedIndex is a prefix index over the resolved on-disk conflict keys the durable queue
// already references, built once per scan pass (and incrementally extended as each view's
// reconcile enqueues ops, see localScan/localReconcileWith) so queuedIndex.conflict answers in
// time proportional to a candidate path's depth rather than scanning the whole queue per
// candidate. exact holds each queued endpoint's resolved key (cleanKey of an op's Path and a
// rename's NewPath); ancestors holds every strict ancestor directory of those endpoints. A
// candidate key "conflicts" iff it overlaps some queued endpoint under the executor's
// pathsOverlap test - equal, contains, or is contained by - and the index answers all three
// cases without a per-candidate sweep (see conflict). The earlier implementation looped over
// the entire queued set for every filesystem entry and baseline, i.e. O(files × queued_ops)
// per view with a large crash/offline backlog; this is O((files+queued)×depth).
type queuedIndex struct {
	exact     map[string]bool
	ancestors map[string]bool
}

// conflict reports whether key - a candidate op's path resolved to its on-disk location
// (cleanKey, the same anchored form the executor serialises on) - overlaps any endpoint the
// durable queue already references: they name the same disk node, key sits beneath a queued
// endpoint, OR a queued endpoint sits beneath key. The overlap is symmetric, matching the
// executor's keysConflict/pathsOverlap (an op conflicts with another when one's resolved path
// equals, contains, or is contained by the other), so the scan never enqueues an op that would
// conflict with one already in flight. The three pathsOverlap cases map onto the index as:
// exact[key] (equal), ancestors[key] (key is a strict ancestor of a queued endpoint), and a
// queued endpoint among key's own strict ancestors (key is contained by a queued endpoint).
//
// Endpoints are compared by RESOLVED disk location, not view-relative string, and across ALL
// views - exactly like the executor (serializeKeys/viewDir/cleanKey). Two views that share an
// on-disk directory (same view name → same viewDir) resolve their identically-named paths to
// the same disk key, so a queued op in one view protects an overlapping disk location the
// other view is about to act on. Comparing only the current view's own ops would miss this:
// e.g. sibling view B holds a queued inbound create at a/x while shared-disk view A sees a
// vanished on disk - without the cross-view key A would push an OpRemove of a to its server
// while B's download recreates a/x locally, leaving A's server permanently short the directory.
// Distinct view names resolve to distinct dirs, so for the common
// one-dir-per-view case this is identical to comparing view-relative paths. (Because each
// view's reconcile may enqueue ops a later same-pass view must respect, every op a reconcile
// enqueues is folded straight back into the shared index - localReconcileWith - so a single
// pass-wide index stays as accurate as a per-view rebuild without re-scanning the queue.)
//
// Both overlap directions matter, and for the same reason: the local scan runs BEFORE the
// cycle's leading queue drain (loop.go), so after a crashed or failed drain ops left from the
// previous cycle are still queued while disk holds an intermediate state.
//   - Descendant-of-a-queued-endpoint (queued directory endpoint is an ancestor of key): an
//     inbound directory move may have relocated the descendant baselines to its destination
//     while disk still holds the source subtree. Matching only the exact endpoints
//     (exact[key]) would let the scanner re-detect those still-on-disk children as fresh
//     creates - or reverse renames/removes - beneath the queued move, enqueuing stale ops that
//     undo the remote move or wedge forever against a parent being removed (resolveParentID →
//     errParentUnknown that never resolves).
//   - Ancestor-of-a-queued-endpoint (key is an ancestor of a queued endpoint): a queued
//     inbound create at a/x must protect the synced directory a from an outbound OpRemove this
//     scan. Without it a locally-deleted a is removed on the server while the inbound download
//     recreates a/x locally - the two diverge. Suppressing a's remove lets the inbound (server-
//     authoritative) op land first; the download recreates a on disk, so the next scan no longer
//     sees a as vanished (asymmetry round). The suppression is temporary: once
//     the queued endpoint drains, a genuinely-vanished ancestor is removed on a later scan.
//
// A queued file endpoint protecting a path that cannot have real children on disk is harmless.
func (q queuedIndex) conflict(key string) bool {
	if q.exact[key] || q.ancestors[key] {
		return true
	}
	for _, a := range ancestorKeys(key) {
		if q.exact[a] {
			return true
		}
	}
	return false
}

// owns reports whether some queued op names key as one of its OWN endpoints, i.e. the strict
// subset of conflict's overlap that proves the key still has an owner in the queue. conflict is
// the right test for "may this path be acted on now" (it is the executor's serialisation rule),
// but it also answers true for a mere ancestor/descendant overlap, which proves nothing about
// ownership - the distinction the quarantine repair sweep needs to tell a covered baseline from
// one it must come back for (queuerepair.go).
func (q queuedIndex) owns(key string) bool {
	return q.exact[key]
}

// add records an endpoint key in the index: the key itself in exact and each of its strict
// ancestor directories in ancestors.
func (q queuedIndex) add(key string) {
	q.exact[key] = true
	for _, a := range ancestorKeys(key) {
		q.ancestors[a] = true
	}
}

// ancestorKeys returns key's strict ancestor directories in the anchored, cleaned form
// cleanKey produces - exactly the set {a : isAncestor(a, key)} the executor's prefix test
// recognises - by walking filepath.Dir up to and including the synthetic root. It is the
// indexed counterpart to scanning every queued key with isAncestor.
func ancestorKeys(key string) []string {
	var out []string
	for {
		parent := filepath.Dir(key)
		if parent == key { // filepath.Dir of the root is the root: stop.
			break
		}
		out = append(out, parent)
		key = parent
	}
	return out
}

// underTypeReplaced reports whether relPath sits strictly beneath a path whose on-disk type
// changed (file<->dir) this scan. Creates and move-new-endpoints for such a subtree are
// deferred: the stale parent node is being removed this scan, so its baseline still carries
// the old node's id and resolving a child's parent against it would mis-parent the child. The
// replaced path itself is not matched here (its own create is already suppressed by the
// type-mismatch checks in the detectors); only descendants need this guard.
func underTypeReplaced(relPath string, typeReplaced map[string]bool) bool {
	fr := normpath.Key(relPath)
	for k := range typeReplaced {
		if keyUnder(fr, normpath.Key(k)) {
			return true
		}
	}
	return false
}

// scanKeyCollisions detects two or more scan entries that fold to the same normpath.Key. On a
// byte-preserving case-sensitive filesystem (Linux) two distinct on-disk names that are the NFD
// and NFC encodings of the same string fold alike (Fold is the NFC identity there), so the
// reconcile merge-join - which keys strictly by normpath.Key - would collapse them to one node
// (last-wins) and silently leave the other unsynced. It returns a map from the shared fold key to
// the colliding scan paths (length ≥ 2); the slice may repeat a path because scanView already
// NFC-normalizes every surfaced relPath, so the two distinct on-disk byte sequences arrive here as
// the same NFC string - len() still reports how many on-disk entries collided. The caller warns
// and leaves every colliding path untouched rather than silently dropping one. DESIGN §13: full
// byte-accurate matching on a case-sensitive FS is deferred (it needs a raw-byte key while still
// NFC-normalizing for the macOS cross-encoding case); this is the minimal never-silently-skip guard.
func scanKeyCollisions(entries []scanEntry) map[string][]string {
	byKey := make(map[string][]string, len(entries))
	for _, en := range entries {
		k := normpath.Key(en.relPath)
		byKey[k] = append(byKey[k], en.relPath)
	}
	var collisions map[string][]string
	for k, paths := range byKey {
		if len(paths) > 1 {
			if collisions == nil {
				collisions = make(map[string][]string)
			}
			sort.Strings(paths)
			collisions[k] = paths
		}
	}
	return collisions
}

// protectCollisions warns about each detected NFD/NFC fold-key collision (view, fold key, the
// colliding on-disk paths and their count) and marks every colliding path protected by adding it
// to unreadable, so the rest of the reconcile pass treats it as still-present - never a vanished
// delete, a move source, or a path to download/upload over. Both colliding files are left
// untouched until the user renames one; that is strictly safer than the merge's silent last-wins,
// which would sync one file and drop the other.
//
// It owns the PROTECTION half only. The durable, once-per-key trace that makes the exclusion
// visible in `status` (collisiontrace.go, finding H6) is written by noteCollisionTraces, which the
// caller invokes separately and deliberately LATER - past every availability guard, since retiring
// a key's trace-memo takes evidence that the pass actually enumerated the tracked tree, and being
// called for protection proves no such thing.
func (e *Engine) protectCollisions(viewID uint64, collisions map[string][]string, unreadable map[string]bool) {
	for k, paths := range collisions {
		e.log.Warn("engine: Unicode path collision (NFD/NFC, a singleton such as the Kelvin sign, or case); leaving both files unsynced until one is renamed (DESIGN §13)",
			"view", viewID, "fold_key", k, "paths", paths, "count", len(paths))
		for _, p := range paths {
			unreadable[p] = true
		}
	}
}

// dropCollidedRows removes from rows every node whose fold key is in collisions (an NFD/NFC
// collision detected by scanKeyCollisions) AND every node strictly beneath a collided directory
// key, so none of the local-reconcile detectors - including detectCreatesAndModifies, which does
// not consult pathProtected - emits an op for either colliding file or for any descendant of a
// collided directory. The descendant case matters because two directories whose names are the NFD
// and NFC encodings of one string collide at the directory key, but their children fold to distinct
// keys (e.g. café/a.txt and café/b.txt): without dropping descendants, detectCreatesAndModifies
// would enqueue child OpUploads whose suppressed-parent OpMkdir left no parent baseline, so
// resolveParentID wedges the queue on errParentUnknown forever - or, with a pre-existing parent
// baseline, merges children from two physical directories under one canonical remote path.
// A row is matched by its node's fold key: a scan-present row by its scan path, an
// otherwise base-only row by its baseline path. Filtering in place is safe (the rows slice is owned
// by the caller and discarded after) and only runs when a collision exists, which a consistent tree
// never produces.
func dropCollidedRows(rows []joinRow, collisions map[string][]string) []joinRow {
	out := rows[:0]
	for _, r := range rows {
		var key string
		switch {
		case r.scan != nil:
			key = normpath.Key(r.scan.relPath)
		case r.base != nil:
			key = normpath.Key(r.base.Path)
		}
		if keyCollidesOrUnder(key, collisions) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// dropSkippedBaselineRows removes from rows every node whose fold key matches a baseline
// record the store stream skipped as corrupt. The corrupt record itself
// produced no base row, so the match is normally against the scan side: without the drop a
// still-on-disk file at the path would diff as a brand-new local create (its baseline is
// invisible) and upload over the server node the record tracks. Unlike dropCollidedRows this
// matches the exact key only, never the subtree beneath it - a corrupt record's type is
// unknowable, and freezing a whole directory of healthy descendants on the chance it was a
// directory would be strictly worse than letting their own intact records drive them. A
// DIRECT child's create/modify still resolves its parent id against the corrupt record, so
// it cannot actually sync; that is caught terminally at apply time (resolveParentID →
// errParentCorrupt → dead-letter) rather than here, keeping this drop minimal.
func dropSkippedBaselineRows(rows []joinRow, skipped []string) []joinRow {
	keys := make(map[string]bool, len(skipped))
	for _, p := range skipped {
		keys[normpath.Key(p)] = true
	}
	out := rows[:0]
	for _, r := range rows {
		var key string
		switch {
		case r.scan != nil:
			key = normpath.Key(r.scan.relPath)
		case r.base != nil:
			key = normpath.Key(r.base.Path)
		}
		if keys[key] {
			continue
		}
		out = append(out, r)
	}
	return out
}

// keyCollidesOrUnder reports whether a node's fold key is itself a collided key or sits strictly
// beneath a collided directory key (so a child of two NFD/NFC-colliding directories is dropped
// along with the directories themselves). collisions is small (a collision is rare), so the
// per-key ancestor scan is cheap and only runs when a collision exists.
func keyCollidesOrUnder(key string, collisions map[string][]string) bool {
	if collisions[key] != nil {
		return true
	}
	for ck := range collisions {
		if keyUnder(key, ck) {
			return true
		}
	}
	return false
}

// plannedOp pairs an outbound op with the baseline mutation that must accompany it. The
// op is enqueued first and the baseline flip applied second (see localReconcile): a crash
// in between leaves the op durably queued while the baseline is still StateSynced, so the
// next scan re-detects the change but the queued-path guard suppresses a duplicate enqueue
// - strictly safer than the reverse order, which could strand a StatePending baseline with
// no op to resolve it.
type plannedOp struct {
	op store.Op
	// apply performs the op's baseline mutation through the given writer.
	//
	// It takes a writer rather than reaching for e.store so ONE implementation serves both
	// granularities: the incremental path passes e.store, where each call is its own
	// transaction, and a cold start passes a *store.Batch, where hundreds share one. The
	// enqueue-first rule is about the ORDER of the two writes, not about how many
	// transactions they span, so both are correct - and a cold start that fsynced twice per
	// planned op spent over an hour on a real view doing nothing else.
	apply func(store.Writer) error
}

// localReconcile scans the view's on-disk directory, diffs it against the stored
// baselines, and folds the differences into outbound durable-queue ops: a new directory
// becomes OpMkdir, a new or content-changed file becomes OpUpload, a vanished synced
// baseline becomes OpRemove, and a uniquely-matched vanished-baseline/new-file pair with
// identical content collapses into a single OpRename. It marks each affected baseline
// StatePending (carrying the local facts the push will transfer) and returns the ops it
// enqueued, in enqueue order. Only StateSynced baselines drive an op; a path already
// present in the durable queue is skipped.
//
// This single-view wrapper is a test-only convenience: production drives reconciliation
// through localScan, which builds the shared queued index / dir cache once per pass and
// calls localReconcileWith for every view in turn.
func (e *Engine) localReconcile(ctx context.Context, viewID uint64) ([]store.Op, error) {
	dirCache := make(map[uint64]string)
	queued, err := e.queuedPaths(dirCache)
	if err != nil {
		return nil, err
	}
	// Single-view wrapper (tests / ad-hoc callers): default to the cheap (size, mtime)
	// short-circuit. The periodic full re-hash is a loop-level safety net driven by
	// localScan's per-pass shouldFullRehash decision, not something an isolated single-view
	// reconcile needs to force.
	return e.localReconcileWith(ctx, viewID, queued, dirCache, rehashSlice{})
}

// localReconcileWith is the body of localReconcile parameterised on a shared queued index
// and dir cache. localScan builds both ONCE per scan pass and feeds them to every view's
// reconcile in turn, then this method folds each op it durably enqueues straight back into
// the shared index (queued.add, below). That preserves the view A → view B visibility the
// per-view rebuild gave - a later view in the pass still sees the ops an earlier view just
// queued - without re-running ScanPending over the whole durable queue for every view, which
// cost O(views × queued_ops × depth) with a fresh map allocation each iteration on a large
// crash/offline backlog. The single-view localReconcile wrapper above keeps its
// own one-shot index, so test and ad-hoc callers are unaffected.
func (e *Engine) localReconcileWith(ctx context.Context, viewID uint64, queued queuedIndex, dirCache map[uint64]string, rehash rehashSlice) ([]store.Op, error) {
	// Cheap scan (Phase 9d): by default build the per-view (size, mtime, hash) hints once and
	// let scanView reuse a baseline's stored hash for any file whose on-disk (size, mtime) still
	// match, so an idle pass does not re-hash the whole tree. The periodic full re-hash safety
	// net overrides this every limits.FullRehashInterval passes: localScan decides once per pass
	// (shouldFullRehash) and threads fullRehash here, in which case hints stay nil and scanView
	// re-hashes every file - catching a same-second / same-size modification the cheap compare
	// would miss.
	hints := make(map[string]scanHint)
	// Stream the view's stored baselines exactly ONCE per pass (one read transaction): the
	// same callback fills the cheap-scan hint table (when hints != nil) AND captures the
	// fold-keyed baseline rows the merge-join below consumes. The cheap path formerly decoded
	// the whole baseline set twice - loadScanHints here, then mergeScanAndBaselines streamed it
	// again - even though both ran in the same pass; collectBaselines folds them into one
	// stream. The full-rehash path passes hints == nil and gets only the rows (one stream too).
	kb, baseCount, hasSynced, skipped, err := e.collectBaselines(viewID, hints)
	if err != nil {
		return nil, err
	}
	// This pass's share of the periodic re-hash loses its hints, so scanView reads those files.
	for key := range hints {
		if rehash.covers(key) {
			delete(hints, key)
		}
	}
	entries, unreadable, err := e.scanView(ctx, viewID, hints)
	if err != nil {
		return nil, err
	}
	curViewDir := e.viewDir(viewID, dirCache)

	// Build the streaming merge-join input: the in-memory scan entries sorted by fold key,
	// joined against the already-captured baseline rows (kb) re-sorted by fold key, then walked
	// in lockstep into joinRows. This replaces the former pair of full-tree scanByPath/baseByPath
	// index maps with a single sorted pass. The diff itself is O(scan + baselines): peak memory is
	// one scan copy plus the joinRow slice and the captured baseline rows, not two duplicate
	// per-path maps. hasSynced (for the empty-scan debounce) and baseCount (for the large-tree
	// warning) were computed during the single collectBaselines stream above, so no second pass
	// over a baseline slice is needed.
	rows := mergeScanRows(entries, kb)

	// One-shot heads-up when a view is unusually large (by scanned entries or stored
	// baselines), so an operator sees memory pressure coming before any limit bites.
	e.warnIfLargeTree(viewID, max(len(entries), baseCount), "local-reconcile")

	// Detect-and-warn NFD/NFC fold-key collisions: on a case-sensitive byte-preserving
	// filesystem two distinct on-disk names that fold to one normpath.Key would collapse in the
	// merge above and silently leave one file unsynced. Warn, mark both protected (unreadable),
	// and drop the collided rows so neither side produces an op - both files stay untouched until
	// one is renamed, never silently clobbered or dropped.
	collisions := scanKeyCollisions(entries)
	// The PROTECTION runs here, unconditionally and before every detector: both spellings must
	// stay untouched whatever this pass later decides about the view root. The durable TRACE does
	// not - noteCollisionTraces is deferred to past the availability guards below (see its call
	// site), because retiring the memo is a claim about the tracked tree.
	e.protectCollisions(viewID, collisions, unreadable)
	if len(collisions) > 0 {
		rows = dropCollidedRows(rows, collisions)
	}

	// A baseline record whose stored value no longer decodes was skipped by the stream:
	// it produced no kb row, so a still-on-disk file at its path would
	// diff as a brand-new local create and push over the server node the record actually
	// tracks. Warn and drop the path's row so it stays untouched - tracked-but-frozen -
	// until the record heals (any inbound overwrite now rewrites a corrupt record, and a
	// remove deletes it) rather than clobbering either side. Only the corrupt node's own row
	// is dropped: descendants with healthy records keep syncing - except a DIRECT child's
	// create/modify, which resolves its parent id against the corrupt directory record; that
	// lookup surfaces store.ErrCorruptRecord and the outbound/keep-both apply dead-letters
	// the child (errParentCorrupt) instead of retrying it forever, so the queue never wedges
	// on it.
	if len(skipped) > 0 {
		e.log.Warn("engine: skipping baseline records that no longer decode (state.db corruption); their paths stay unsynced until the records are rewritten",
			"view", viewID, "paths", skipped, "count", len(skipped))
		rows = dropSkippedBaselineRows(rows, skipped)
	}

	// Guard against reading a transiently-unavailable view root as a full deletion. An empty
	// scan can mean the user genuinely deleted every file, but it just as commonly means the
	// view root is unavailable: missing or a symlink (a removed directory), OR present-but-empty
	// because a filesystem expected to mount there is not mounted - an unmounted mount point is
	// an ordinary empty directory, so Lstat().IsDir() cannot tell it apart from a real empty-out.
	// In every such case each synced baseline would diff as "vanished" and enqueue an OpRemove,
	// erasing the entire remote view. The cases are indistinguishable from the filesystem alone,
	// so debounce rather than suppress forever: a single suspicious empty scan never
	// wipes the remote view, but once the empty state PERSISTS across emptyScanConfirmThreshold
	// consecutive scans it is propagated, so a genuine whole-view delete (and the vanished-upload
	// recovery's final-file OpRemove) still converges instead of being blocked permanently. Any
	// non-empty scan, or a view with no synced baselines, resets the counter (below / on the
	// non-empty path). Suppressing every empty scan unconditionally would block legitimate
	// deletions forever; debouncing keeps the data-preserving guard while still converging.
	if len(entries) == 0 {
		if hasSynced {
			// A root that is detectably unavailable - missing, a symlink, or not a directory -
			// is not a legitimate empty-out, so it must never advance the debounce toward erasing
			// the remote view. Suppress this scan AND reset the counter: the debounce
			// confirms a whole-view delete only across a run of CONSECUTIVE observed-empty scans,
			// and an unavailable scan is not such an observation. Leaving the counter intact would
			// let an unavailable scan bridge two empty scans into a false "consecutive" streak -
			// empty, empty, unavailable, empty would reach the threshold and wipe the remote view
			// even though emptiness was never confirmed without interruption. Only a present-but-
			// empty root (a genuine delete-all or an indistinguishable unmounted mount point) is
			// allowed to debounce.
			unavailable, uerr := e.rootUnavailable(viewID)
			if uerr != nil {
				// The probe itself failed (an EACCES/EIO stat of the root, a view-dir resolve
				// error): this scan observed NOTHING about the root's availability, so it is not
				// a confirmed-empty observation either. Reset the counter for exactly the reason
				// the unavailable branch below does: leaving it intact lets
				// an indeterminate scan BRIDGE two empty scans into a false "consecutive" streak
				// - empty, empty, stat-error, empty reaches the threshold and wipes the remote
				// view - which is the very bridging the branch below exists to prevent. The error
				// still propagates (localScan logs and continues), so the failure stays visible.
				delete(e.emptyViewScans, viewID)
				return nil, uerr
			}
			if unavailable {
				e.log.Warn("engine: view root unavailable (missing, symlink, or not a directory) while synced baselines exist; suppressing this scan and resetting the empty-scan debounce",
					"view", viewID)
				delete(e.emptyViewScans, viewID)
				return nil, nil
			}
			// A root that now sits on a DIFFERENT filesystem device than when its content was
			// last seen healthy - or whose device id can no longer be read - is a mount that
			// dropped or swapped underneath the path. Lstat above cannot tell that
			// empty directory apart from a real empty-out, but the device id can. Treat it like
			// the other unavailable cases: suppress this scan and reset the debounce so a remount
			// swap never bridges toward erasing the remote view.
			//
			// The suppression needs an exit, though. The non-empty branch
			// re-adopts a changed device on positive CONTENT evidence, because one filesystem's
			// device number is not stable across reboots (dm/LVM/btrfs/network mounts renumber);
			// an empty view can produce no such evidence BY CONSTRUCTION, so a routine
			// renumbering under an emptied view suppressed every later scan forever - a
			// legitimate whole-view delete never converged, the misleading "mount swap" Warn
			// repeated indefinitely, and neither it nor anything else named a way out. The exit
			// is corroboration rather than a bare adopt (adopting without evidence would hand
			// back exactly the mass-wipe risk the guard exists to prevent): a sibling view of the
			// SAME filesystem that walked its own still-tracked tree and adopted the very same
			// old→new transition this process. With it, this view adopts the new id and resumes
			// the ordinary empty-scan debounce; without it the scan stays suppressed and the Warn
			// says so honestly, naming what the operator can do about it.
			if probe := e.viewRootDeviceProbe(viewID); probe.changed {
				if !e.deviceMoveCorroborated(probe) {
					e.log.Warn("engine: view root device id changed or unreadable while synced baselines exist and an empty view can offer no content evidence; suppressing this scan and resetting the empty-scan debounce",
						"view", viewID, "recorded_device", probe.prev, "current_device", probe.dev,
						"cause", "an unmounted or swapped mount at the view root, or a filesystem renumbered across a reboot",
						"remedy", "restore the expected mount; if the view really was emptied on this filesystem, let one tracked file exist on disk again (a non-empty scan re-adopts the device id) or run `synosync unlink` and re-pair")
					delete(e.emptyViewScans, viewID)
					return nil, nil
				}
				e.log.Warn("engine: view root device id changed but another view of the same filesystem confirmed the same renumbering; adopting the new device id and resuming the empty-scan debounce",
					"view", viewID, "recorded_device", probe.prev, "current_device", probe.dev)
				e.recordViewRootDeviceID(viewID, probe.dev)
			}
			// Consecutive scans are not elapsed time: a scan landing within
			// emptyScanConfirmMinSpacing of the last counted one is suppressed WITHOUT advancing
			// the debounce, so a burst of wake-driven scans during an unmount cannot deliver the
			// whole threshold inside the seconds the mount takes to disappear. The run is kept
			// intact - the emptiness did persist - so the next scan past the floor advances it.
			streak, counted := e.noteEmptyViewScan(viewID)
			if !counted {
				e.log.Warn("engine: view scanned empty again within the debounce's wall-clock floor; suppressing this scan without counting it (a burst of scans is not a persisting empty state)",
					"view", viewID, "consecutive_empty_scans", streak.count, "threshold", e.limits.EmptyScanThreshold,
					"min_spacing", emptyScanConfirmMinSpacing)
				return nil, nil
			}
			if streak.count < e.limits.EmptyScanThreshold {
				e.log.Warn("engine: view scanned empty while synced baselines exist; suppressing this scan pending confirmation (unavailable mount or whole-view delete)",
					"view", viewID, "consecutive_empty_scans", streak.count, "threshold", e.limits.EmptyScanThreshold)
				return nil, nil
			}
			e.log.Warn("engine: view confirmed empty across consecutive scans spanning the debounce's wall-clock floor; propagating whole-view deletion",
				"view", viewID, "consecutive_empty_scans", streak.count, "min_spacing", emptyScanConfirmMinSpacing)
			// fall through: detectRemoves emits an OpRemove for each vanished synced baseline.
		} else {
			delete(e.emptyViewScans, viewID)
		}
	} else {
		delete(e.emptyViewScans, viewID)
		// The view scanned non-empty: normally its root is present and healthy on its current
		// filesystem device, so record that device id - a later empty scan can then tell a real
		// empty-out from a mount that dropped/swapped underneath the path (viewRootDeviceChanged).
		// Best-effort; a stat failure leaves any prior recording intact.
		//
		// But a POPULATED root can be a swapped mount too: the wrong USB disk
		// or a foreign dataset at the view root looks like an ordinary non-empty directory, and
		// recording its device id unconditionally would both erase the comparison baseline the
		// empty-scan guard depends on AND let this same pass diff the foreign tree - an OpRemove
		// per missing baseline plus an upload over every coincidentally-shared path, with no
		// debounce anywhere. adoptViewRootDevice therefore gates the recording on positive
		// content evidence that this tree is the tracked one, and suppresses the scan (exactly as
		// the empty branch does) when the device changed and nothing matches.
		if !e.adoptViewRootDevice(viewID, rows, hasSynced) {
			return nil, nil
		}
	}

	// PAST every availability guard, deliberately. len(entries) > 0 is the "this pass actually
	// enumerated the view" evidence the trace memo needs, and the raw entry count alone is not
	// that evidence: a POPULATED foreign mount at the view root (the wrong USB disk, a swapped
	// dataset) enumerates plenty of entries and none of them are the tracked tree, so its empty
	// collision set proves nothing about the tracked keys - retiring the memo on it re-armed the
	// recording for every still-colliding key and one mount flap per pass then evicted the genuine
	// op-drop traces from the durable ring. That is the very churn the observed parameter was
	// added to prevent, reached through a populated swap instead of an empty one. Every arm above
	// that cannot vouch for the tree has already returned, so reaching here IS the vouching.
	e.noteCollisionTraces(viewID, collisions, len(entries) > 0)

	planned := e.diffLocal(viewID, curViewDir, rows, queued, unreadable)

	ops := make([]store.Op, 0, len(planned))
	// One transaction per CHUNK, not per op. Enqueue plus the baseline flip is two committed
	// transactions per planned op, and bolt fsyncs and rewrites a b-tree path plus the freelist
	// each time: cold start measured that shape at 40 ops/s and 47.6 GB of physical I/O for a
	// 241 MB database, which is why it batches. This loop produces the SAME shape and was left
	// unbatched on the reading that the incremental path plans "a handful of ops, latency over
	// throughput" - but nothing bounds an incremental diff to a handful. Unpacking an archive
	// into the sync root, or restoring a backup, plans as many ops as a cold start does, and
	// spent the same tens of minutes committing them with the network idle.
	//
	// The ordering rule is unchanged: each op's Enqueue still precedes its own baseline mutation
	// inside the batch, which is what "enqueue before baseline mutation" states. A batch is
	// all-or-nothing, so a crash discards a whole chunk - a chunk that never happened, which the
	// next scan re-derives from disk.
	for start := 0; start < len(planned); start += localReconcileEnqueueBatch {
		chunk := planned[start:min(start+localReconcileEnqueueBatch, len(planned))]
		if err := e.batchStore(func(b *store.Batch) error {
			for _, p := range chunk {
				if _, err := b.Enqueue(p.op); err != nil {
					return fmt.Errorf("engine: enqueue %s %d/%q: %w", p.op.Type, p.op.ViewID, p.op.Path, err)
				}
				if err := p.apply(b); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return ops, err
		}
		// Reflect the committed ops in the shared index so a later view in the same scan pass
		// treats their disk locations as already queued - the cross-view visibility the per-view
		// ScanPending rebuild used to provide. It happens AFTER the commit, not inside the batch:
		// a rolled-back chunk enqueued nothing, and an index claiming ops that do not exist would
		// suppress re-detection for the rest of the pass.
		for _, p := range chunk {
			queued.add(foldedKey(curViewDir, p.op.Path))
			if p.op.NewPath != "" {
				queued.add(foldedKey(curViewDir, p.op.NewPath))
			}
			ops = append(ops, p.op)
		}
	}
	return ops, nil
}

// localReconcileEnqueueBatch is how many planned ops share one durable transaction, matching
// coldStartEnqueueBatch: the two loops write the same records for the same reason, and a chunk
// this size keeps the rollback unit small enough that a failure re-derives cheaply while
// amortising the fsync over hundreds of records rather than paying two per op.
const localReconcileEnqueueBatch = coldStartEnqueueBatch

// queuedPaths builds a prefix index over the resolved on-disk conflict keys the durable queue
// already references across ALL views - each op's Path (and a rename's NewPath) joined with its
// view's on-disk directory via cleanKey, exactly as the executor's serializeKeys resolves them.
// localReconcile skips a candidate whose resolved key overlaps this set (queuedIndex.conflict),
// so a push still draining - or an inbound op for the same disk location, including one in a
// sibling view that shares the on-disk directory - is not re-detected and re-enqueued on every
// scan. The keys span views (not filtered to the scanned view) so the cross-view, same-directory
// case the executor serialises is protected here too; views with distinct names resolve to
// distinct dirs and so never collide, leaving the common one-dir-per-view behaviour unchanged.
// dirCache memoises the per-view directory lookup shared with the caller's curViewDir resolution.
// A queued keep-both resolve op additionally protects the node's CURRENT tracked path (resolved by
// file id), not just its possibly-stale op.Path, so a node relocated server-side off op.Path by an
// already-applied inbound rename stays protected from a racing outbound upload until the resolver
// re-funnels to the new path.
func (e *Engine) queuedPaths(dirCache map[uint64]string) (queuedIndex, error) {
	idx := queuedIndex{exact: make(map[string]bool), ancestors: make(map[string]bool)}
	// A keep-both resolve op protects op.Path, but its diverged node may already have been
	// relocated server-side off op.Path by an inbound rename that ran ahead of the resolve (the
	// resolver only re-funnels to the node's new path when it later drains - refunnelIfMoved). In
	// that window op.Path is stale and the node's CURRENT path is unprotected, so a fresh local
	// edit there would enqueue a racing OpUpload that bypasses keep-both. Protect the current
	// tracked path too, resolved by file id. The lookups run AFTER ScanPending returns rather than
	// inside its callback, since PathByID opens its own read transaction that must not nest in the
	// scan's.
	type movedResolve struct {
		viewID uint64
		fileID string
	}
	var resolves []movedResolve
	// Deliberately CROSS-view (so not scanQueuedInView): this index is the whole queue,
	// keyed per view directory, because the local scan reconciles every view in one pass.
	err := e.store.ScanPending(func(qo store.QueuedOp) error {
		vd := e.viewDir(qo.ViewID, dirCache)
		idx.add(foldedKey(vd, qo.Path))
		if qo.NewPath != "" {
			idx.add(foldedKey(vd, qo.NewPath))
		}
		if qo.Type == store.OpResolveConflict && qo.FileID != "" {
			resolves = append(resolves, movedResolve{viewID: qo.ViewID, fileID: qo.FileID})
		}
		return nil
	})
	if err != nil {
		return queuedIndex{}, fmt.Errorf("engine: scan queue for local reconcile: %w", err)
	}
	for _, r := range resolves {
		cur, perr := e.store.PathByID(r.viewID, r.fileID)
		if isNotFound(perr) {
			continue
		}
		if perr != nil {
			return queuedIndex{}, fmt.Errorf("engine: resolve queued conflict node %d/%q: %w", r.viewID, r.fileID, perr)
		}
		idx.add(foldedKey(e.viewDir(r.viewID, dirCache), cur))
	}
	return idx, nil
}

// joinRow is one node of the local reconcile's streaming merge-join: a scan entry and/or a
// baseline that share a fold key (normpath.Key - the "same node" identity). A scan-only row
// (base == nil) is a local-only path, a base-only row (scan == nil) is a vanished/server-only
// baseline, and a both-present row is a tracked node. It replaces the former scanByPath and
// baseByPath per-path index maps: the merge already pairs the two sides by key, so the
// detectors read a row's two pointers instead of doing a map lookup per path. The pointers
// alias the merge's own sorted ks/kb backing arrays (not the caller's entries slice, and never
// re-materialised into a third per-row copy), so the rows slice adds only one pointer pair per
// node on top of the two sorted scratch slices it indexes into.
type joinRow struct {
	scan *scanEntry
	base *store.PathBaseline
}

// keyedBaseline pairs a stored baseline with its fold key (normpath.Key) for the merge-join.
// collectBaselines fills the slice from a single ScanBaselinesForView stream and mergeScanRows
// re-sorts it by key, so the captured rows are the merge's kb side.
type keyedBaseline struct {
	key string
	val store.PathBaseline
}

// collectBaselines streams a view's stored baselines exactly once (one read transaction via the
// scanBaselines seam over store.ScanBaselinesForView) and captures them as fold-keyed rows for
// the merge-join, computing the baseline count (for the large-tree warning) and whether any
// baseline is StateSynced (for the empty-scan debounce) in the same pass. When hints != nil it
// ALSO fills the cheap-scan hint table (keyed by the SAME normpath.Key, so scanView's lookup
// matches a baseline through a case-only divergence on a case-insensitive filesystem) from this
// one stream - folding what loadScanHints used to stream separately into the same pass, so a
// cheap reconcile decodes each baseline once rather than twice. Only regular files with a
// recorded content hash get a hint; directories and never-hashed baselines fall through to a
// full hash in scanView. The full-rehash path passes hints == nil and gets only the rows.
// skipped is the stream's corrupt-record report (paths whose stored value no longer decodes);
// localReconcileWith Warns and freezes those paths for the pass.
func (e *Engine) collectBaselines(viewID uint64, hints map[string]scanHint) (kb []keyedBaseline, baseCount int, hasSynced bool, skipped []string, err error) {
	skipped, serr := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		key := normpath.Key(pb.Path)
		kb = append(kb, keyedBaseline{key: key, val: pb})
		if pb.State == store.StateSynced {
			hasSynced = true
		}
		if hints != nil && pb.FileType != "dir" && pb.LocalHash != "" {
			hints[key] = scanHint{size: pb.LocalSize, mtime: pb.LocalMTime, hash: pb.LocalHash}
		}
		return nil
	})
	if serr != nil {
		return nil, 0, false, nil, fmt.Errorf("engine: stream baselines for local reconcile %d: %w", viewID, serr)
	}
	return kb, len(kb), hasSynced, skipped, nil
}

// mergeScanRows folds the in-memory scan entries and the already-captured stored baselines (kb,
// from collectBaselines) into one fold-key-ordered slice of joinRows. Both sides are keyed by
// normpath.Key (NFC + case fold on a case-insensitive filesystem, NFC identity otherwise) so a
// scan entry and a baseline differing only in case join as the same node and never diff as a
// spurious delete+create; the stored entities keep their original-case paths (en.relPath /
// pb.Path) so store mutations still address the right key. kb is re-sorted by fold key here
// because bbolt's path-byte order and the fold order diverge on a case-insensitive filesystem.
// Duplicate fold keys within a side - which a consistent tree never produces (a case-insensitive
// filesystem cannot hold two on-disk names folding alike, and the store keeps one canonical key
// per node) - collapse to the last occurrence, mirroring the former maps' last-wins.
func mergeScanRows(entries []scanEntry, kb []keyedBaseline) (rows []joinRow) {
	type keyedScan struct {
		key string
		val scanEntry
	}
	ks := make([]keyedScan, len(entries))
	for i, en := range entries {
		ks[i] = keyedScan{key: normpath.Key(en.relPath), val: en}
	}
	sort.SliceStable(ks, func(i, j int) bool { return ks[i].key < ks[j].key })
	sort.SliceStable(kb, func(i, j int) bool { return kb[i].key < kb[j].key })

	// Two-pointer merge over the two fold-key-sorted sides. Each lastOf collapses a run of
	// equal adjacent keys (last-wins) and returns both the index after the run and the run's
	// final index; rows then point straight into the ks/kb backing arrays (stable - neither is
	// appended to after its sort) rather than escaping a per-row copy, so the rows slice is a
	// thin pointer index over the two scratch slices, not a third materialisation of the tree.
	lastScan := func(i int) (next, last int) {
		k := ks[i].key
		for i+1 < len(ks) && ks[i+1].key == k {
			i++
		}
		return i + 1, i
	}
	lastBase := func(j int) (next, last int) {
		k := kb[j].key
		for j+1 < len(kb) && kb[j+1].key == k {
			j++
		}
		return j + 1, j
	}
	i, j := 0, 0
	for i < len(ks) || j < len(kb) {
		switch {
		case j >= len(kb) || (i < len(ks) && ks[i].key < kb[j].key):
			ni, si := lastScan(i)
			i = ni
			rows = append(rows, joinRow{scan: &ks[si].val})
		case i >= len(ks) || kb[j].key < ks[i].key:
			nj, bj := lastBase(j)
			j = nj
			rows = append(rows, joinRow{base: &kb[bj].val})
		default: // equal fold keys: a tracked node
			ni, si := lastScan(i)
			nj, bj := lastBase(j)
			i, j = ni, nj
			rows = append(rows, joinRow{scan: &ks[si].val, base: &kb[bj].val})
		}
	}
	return rows
}

// diffLocal computes the ordered list of outbound ops for one view from its merge-joined scan
// entries and stored baselines (mergeScanAndBaselines), skipping any path already queued. Ops
// are emitted in dependency order: creates (mkdir + upload) ascending so a parent directory
// precedes its children, then renames, then removes descending so a child precedes its parent.
// The executor's path-overlap conflict serialisation enforces these dependencies at apply time
// too; the enqueue order keeps the FIFO chain between conflicting ops correct.
func (e *Engine) diffLocal(viewID uint64, curViewDir string, rows []joinRow, queued queuedIndex, unreadable map[string]bool) []plannedOp {
	// Paths whose on-disk type changed (file<->dir) this scan. detectRemoves emits the stale
	// node's OpRemove now, but its baseline lingers (StatePending, carrying the old node's
	// FileID) until that remove drains. Any child that appears under such a path THIS scan must
	// not be pushed yet: resolveParentID would read the lingering baseline and parent the child
	// under the soon-to-be-removed node's id. Defer the whole subtree to a later scan, after the
	// remove drops the baseline and the replacement parent is recreated. Without this a file
	// replaced on disk by a non-empty directory pushes the directory's children under the
	// soon-deleted file's id, mis-parenting (or losing) them server-side.
	typeReplaced := make(map[string]bool)
	for _, r := range rows {
		if r.scan == nil || r.base == nil || r.base.State != store.StateSynced {
			continue
		}
		if r.scan.isDir != (r.base.FileType == "dir") {
			typeReplaced[r.base.Path] = true
		}
	}

	// A record that is out of scope NOW - an exclusion or a name rule added after it was written
	// (exclude.go) - is never a local deletion or a move source, whether its file is still on
	// disk (the scan protected it) or not (nothing for the scan to protect). `exclude add` drops
	// such records, but the engine must not lean on that alone: a deletion it pushed would remove
	// a node the user only meant to stop syncing.
	for _, r := range rows {
		if r.base == nil {
			continue
		}
		kind := config.KindFile
		if r.base.FileType == "dir" {
			kind = config.KindDir
		}
		if e.excluded(viewID, r.base.Path, kind) {
			if unreadable == nil {
				unreadable = make(map[string]bool)
			}
			unreadable[r.base.Path] = true
		}
	}
	// Fold the scanner's unsurfaced-path set once for the whole diff (both detectors consult
	// it per row), rather than re-folding every key against every candidate.
	protected := newProtectedIndex(unreadable)

	matchedDel, matchedNew, renames := e.detectMoves(viewID, curViewDir, rows, queued, protected, typeReplaced)

	creates := e.detectCreatesAndModifies(viewID, curViewDir, rows, matchedNew, queued, typeReplaced)
	// Directories that are present on disk but empty while still recording contents - the shape
	// an unmounted nested mount point takes, and one a deletion never produces. Their subtrees'
	// removes wait for the same corroboration the view root's emptiness needs (emptieddir.go).
	suspect := e.suspectEmptiedDirs(viewID, rows, e.limits.EmptyScanThreshold)
	removes := e.detectRemoves(viewID, curViewDir, rows, matchedDel, queued, protected, suspect)

	sort.SliceStable(creates, func(i, j int) bool { return creates[i].op.Path < creates[j].op.Path })
	sort.SliceStable(renames, func(i, j int) bool { return renames[i].op.NewPath < renames[j].op.NewPath })
	// Removes children-before-parents: descending path order so "a/b" precedes "a".
	sort.SliceStable(removes, func(i, j int) bool { return removes[i].op.Path > removes[j].op.Path })

	planned := make([]plannedOp, 0, len(creates)+len(renames)+len(removes))
	planned = append(planned, creates...)
	planned = append(planned, renames...)
	planned = append(planned, removes...)
	return planned
}

// detectMoves finds same-scan move pairs: a synced file baseline whose path vanished on
// disk and a brand-new on-disk file with identical content (MD4 + size). A pair is
// collapsed into a single OpRename only when the content key matches exactly one vanished
// baseline AND one new file - an ambiguous many-way match falls back to the plain
// remove+upload the other detectors emit. It returns the matched baseline paths and new
// scan paths (so the create/remove detectors skip them) plus the rename plannedOps. Only
// regular files with a non-empty hash and a server-assigned file id are move candidates;
// directories (empty hash) would match indiscriminately and are deliberately excluded.
func (e *Engine) detectMoves(viewID uint64, curViewDir string, rows []joinRow,
	queued queuedIndex, protected protectedIndex, typeReplaced map[string]bool,
) (matchedDel, matchedNew map[string]bool, renames []plannedOp) {
	matchedDel = make(map[string]bool)
	matchedNew = make(map[string]bool)

	// A vanished synced file baseline (base-only row) is a candidate move source; a brand-new
	// on-disk file (scan-only row) is a candidate destination. The two are disjoint row shapes,
	// so one walk classifies both. A both-present row is a tracked node, never a move endpoint.
	delByKey := make(map[string][]store.PathBaseline)
	newByKey := make(map[string][]scanEntry)
	for _, r := range rows {
		switch {
		case r.base != nil && r.scan == nil:
			pb := *r.base
			if queued.conflict(foldedKey(curViewDir, pb.Path)) || pb.State != store.StateSynced {
				continue
			}
			// A path the scan saw but did not surface (an unreadable/raced file, an internal-artifact
			// name, or a path under an unreadable subtree) is not a vanished move source: treating it
			// as one risks pairing it with an unrelated new file and moving it away.
			// Leave it for the next scan.
			if protected.has(pb.Path) {
				continue
			}
			if pb.FileType == "dir" || pb.FileID == "" || pb.LocalHash == "" {
				continue
			}
			k := moveKey(pb.LocalHash, pb.LocalSize)
			delByKey[k] = append(delByKey[k], pb)
		case r.scan != nil && r.base == nil:
			en := *r.scan
			if queued.conflict(foldedKey(curViewDir, en.relPath)) || en.isDir || en.md4 == "" {
				continue
			}
			// A new file under a path whose type changed this scan is part of a replacement subtree:
			// do not pair it into a move, its parent is being removed (see underTypeReplaced).
			if underTypeReplaced(en.relPath, typeReplaced) {
				continue
			}
			k := moveKey(en.md4, en.size)
			newByKey[k] = append(newByKey[k], en)
		}
	}

	for k, dels := range delByKey {
		news := newByKey[k]
		if len(dels) != 1 || len(news) != 1 {
			continue // ambiguous (or no) match: leave to remove + upload
		}
		d, n := dels[0], news[0]
		matchedDel[d.Path] = true
		matchedNew[n.relPath] = true
		renames = append(renames, e.planRename(viewID, d, n))
	}
	return matchedDel, matchedNew, renames
}

// detectCreatesAndModifies emits an OpMkdir for a new directory, an OpUpload for a new
// file, and an OpUpload for a file whose on-disk hash differs from its synced baseline.
// A path already queued, or matched as a move's new endpoint, is skipped; a baseline that
// is not StateSynced is left to the inbound/conflict paths. A node whose on-disk type
// differs from its baseline (a file replaced by a directory or vice versa) emits no create
// here - detectRemoves removes the stale server node first, and a later scan (with the
// baseline gone) then detects the replacement as a fresh create. The whole subtree under such
// a path is deferred the same way (underTypeReplaced): a child cannot be pushed before its
// replacement parent directory exists server-side.
func (e *Engine) detectCreatesAndModifies(viewID uint64, curViewDir string, rows []joinRow,
	matchedNew map[string]bool, queued queuedIndex, typeReplaced map[string]bool,
) []plannedOp {
	var creates []plannedOp
	for _, r := range rows {
		if r.scan == nil {
			continue
		}
		en := *r.scan
		if queued.conflict(foldedKey(curViewDir, en.relPath)) || matchedNew[en.relPath] {
			continue
		}
		// Defer any node beneath a path whose type changed this scan: its parent directory does
		// not exist server-side yet (the stale file is being removed), so a child push would
		// resolve its parent to the soon-deleted node. A later scan recreates the parent first.
		if underTypeReplaced(en.relPath, typeReplaced) {
			continue
		}
		if r.base == nil {
			// Brand-new node with no baseline: a fresh create.
			if en.isDir {
				creates = append(creates, e.planMkdir(viewID, en))
			} else {
				creates = append(creates, e.planUpload(viewID, en, "", en.relPath, "", false))
			}
			continue
		}
		pb := *r.base
		if pb.State != store.StateSynced {
			continue
		}
		if en.isDir || pb.FileType == "dir" {
			// A directory whose type still matches needs no op (its content changes surface as
			// child entries). A type change at the same path (file<->dir) is handled by
			// detectRemoves, which removes the stale server node; the replacement create is
			// detected on a later scan once the baseline is gone.
			continue
		}
		if pb.LocalHash != en.md4 {
			// Local content diverged from the synced baseline: push the new content,
			// addressing the existing server node by its file id. Use the baseline's stored
			// path (pb.Path), not the scan's case, so on a case-insensitive filesystem the
			// store mutation hits the existing key instead of stranding a second baseline.
			//
			// pb is StateSynced here (checked above), so its LocalHash is the content the last
			// agreement recorded - capture it as the H1 guard's fallback divergence base BEFORE
			// the plan's apply stamps the new edit over it, and record the synced-ness itself:
			// this arm is the only producer of a MODIFY, and a record whose facts are all empty
			// (an id-less inbound rename's bare baseline) would otherwise be guarded as a create.
			creates = append(creates, e.planUpload(viewID, en, pb.FileID, pb.Path, pb.LocalHash, true))
		}
	}
	return creates
}

// detectRemoves emits an OpRemove for every synced baseline whose path no longer exists on
// disk and was not consumed by a move match. It also emits an OpRemove when the path still
// exists but its on-disk TYPE differs from the baseline (a file replaced by a directory or
// vice versa): the old server node must be deleted before the new one can be created, so the
// remove is emitted now and - once it drains and drops the baseline - a later scan sees the
// path as a fresh create and pushes the new node. Sequencing the replacement remove-before-
// create across scans avoids the same-path apply-ordering hazard a single-scan remove+create
// pair would carry, and routing it through this (children-before-parents) removes list keeps a
// directory's vanished children ordered ahead of the directory's own replacement remove. A
// queued path, or a baseline that is not StateSynced, is skipped.
func (e *Engine) detectRemoves(viewID uint64, curViewDir string, rows []joinRow,
	matchedDel map[string]bool, queued queuedIndex, protected protectedIndex, suspect map[string]bool,
) []plannedOp {
	var removes []plannedOp
	for _, r := range rows {
		if r.base == nil {
			continue
		}
		pb := *r.base
		if queued.conflict(foldedKey(curViewDir, pb.Path)) || matchedDel[pb.Path] || pb.State != store.StateSynced {
			continue
		}
		if r.scan != nil {
			// The path still exists on disk. If its type matches the baseline there is nothing to
			// remove - a directory needs no op (its content surfaces as child entries) and a file is
			// hash-compared by detectCreatesAndModifies. If the type DIFFERS it is a replacement:
			// fall through to emit the remove of the now-stale server node (the matching create
			// follows on a later scan once this remove has dropped the baseline). A present path is
			// a surfaced scan entry, so it is never unreadable/protected - the pathProtected guard
			// below only applies to the vanished case.
			// An UNSET FileType is no evidence of a replacement - it is no evidence of anything,
			// the same reading verifyServerSubtreeAgrees' child arms and removeTargetFacts apply
			// to the field. Bare records are routine (applyRename writes store.Baseline{FileID: …}
			// with no FileType and forces StateSynced), and comparing "" against "dir" made every
			// such record over an on-disk DIRECTORY read as a file→dir replacement: a spurious
			// OpRemove for a directory that is still present, re-emitted by every scan, whose
			// apply is a RECURSIVE server delete of a live tree. Only a positively recorded kind
			// can disagree with the disk; an unset one leaves the path to
			// detectCreatesAndModifies, and a genuine replacement is re-derived once some pass
			// records a kind for it.
			if pb.FileType == "" || r.scan.isDir == (pb.FileType == fileType(true)) {
				continue
			}
		} else if protected.has(pb.Path) {
			// The scanner observed this path on disk but did not surface it as an entry - a
			// transient unreadable/raced file, an internal-artifact name (a staging temp the scanner
			// filters out), or a path under an unreadable subtree the walk skipped. None is a confirmed
			// deletion; emitting an OpRemove here would push a spurious remote delete for a file that
			// is still present. Skip it and let a later scan reconcile against the settled reality.
			continue
		} else if underSuspectDir(suspect, pb.Path) {
			// The containing directory is still on disk but empty, and its emptiness has not yet
			// been observed often enough, or over enough wall clock, to be a persisting state.
			// That shape is what an unmounted nested mount point looks like, and it is NOT what a
			// deletion looks like: removing a folder removes the folder (emptieddir.go).
			continue
		}
		removes = append(removes, e.planRemove(viewID, pb))
	}
	return removes
}

// planUpload builds the OpUpload (and its pending-baseline flip) for a new or modified
// file. fileID is the existing server node id for a modify, empty for a fresh create.
// path is the canonical view-relative path the op and baseline are recorded under: the
// scan entry's own path for a fresh create, but the existing baseline's stored path for a
// modify, so a case-only divergence on a case-insensitive filesystem updates the existing
// node in place rather than stranding a second baseline at the scan's case.
//
// baseLocalHash is the reconcile-time capture of the content this push is written AGAINST -
// the record's last-synced LocalHash, read before the apply below overwrites it with the new
// edit. It is the H1 upload guard's fallback divergence base (uploadDivergenceBase): the
// record's ServerHash is the primary one, but a successful push writes the upload reply's hash
// there verbatim, so a reply shape that omits it BLANKS the field and leaves the guard with
// nothing to compare on every later modify. Only a caller holding a StateSynced record may
// supply it - for a synced baseline markSynced recorded LocalHash and ServerHash from the same
// agreement, so the local hash describes exactly the bytes the server was last known to hold.
// Every other caller passes "" (a create has no base at all; queuerepair's stranded record is
// StatePending, its LocalHash already the un-pushed edit).
//
// baseSynced travels with it and carries the same rule - only a caller holding a StateSynced
// record sets it - but it is the guard's CLASS discriminator (uploadIsCreate) rather than its
// comparison base, and the two cannot be collapsed: a synced record can carry no id and no local
// hash at all (an id-less inbound rename's bare facts, revertRelocatedOutbound's cleared ones), so
// the capture being empty is no evidence that this push is a create.
func (e *Engine) planUpload(viewID uint64, en scanEntry, fileID, path, baseLocalHash string, baseSynced bool) plannedOp {
	op := store.Op{
		Type:          store.OpUpload,
		Direction:     store.DirOutbound,
		ViewID:        viewID,
		Path:          path,
		FileID:        fileID,
		LocalHash:     en.md4,
		LocalSize:     en.size,
		LocalMTime:    en.mtime,
		UnixPerm:      en.unixPerm,
		BaseLocalHash: baseLocalHash,
		BaseWasSynced: baseSynced,
	}
	apply := func(w store.Writer) error {
		return markLocalPending(e, w, viewID, path, func(b *store.Baseline) {
			b.FileType = fileType(false)
			b.LocalHash = en.md4
			b.LocalSize = en.size
			b.LocalMTime = en.mtime
			b.UnixPerm = en.unixPerm
		})
	}
	return plannedOp{op: op, apply: apply}
}

// planMkdir builds the OpMkdir (and its pending-baseline flip) for a new directory.
func (e *Engine) planMkdir(viewID uint64, en scanEntry) plannedOp {
	op := store.Op{
		Type:       store.OpMkdir,
		Direction:  store.DirOutbound,
		ViewID:     viewID,
		Path:       en.relPath,
		LocalMTime: en.mtime,
		UnixPerm:   en.unixPerm,
	}
	apply := func(w store.Writer) error {
		return markLocalPending(e, w, viewID, en.relPath, func(b *store.Baseline) {
			b.FileType = fileType(true)
			b.LocalMTime = en.mtime
			b.UnixPerm = en.unixPerm
		})
	}
	return plannedOp{op: op, apply: apply}
}

// planRemove builds the OpRemove (and its pending-baseline flip) for a vanished baseline.
// The baseline is flipped to StatePending rather than deleted so the executor can resolve
// it on apply; it carries the server file id so the push can address the node to remove.
//
// It also stamps the vanished record's KIND onto the op (BaseIsDir). The recursive-delete
// precondition (verifyServerSubtreeAgrees, finding H7) arms only for a DIRECTORY target, and
// removeTargetFacts can read the kind from the record only while the record still exists - a
// path that drops it between enqueue and apply (a crash in the window between Enqueue and this
// plan's own baseline flip, an ancestor's inbound teardown cascading dropDescendantBaselines, a
// success-path DeleteBaseline whose Ack then failed) would otherwise read "not a directory" and
// issue the RECURSIVE server delete with nothing proved about the subtree. Captured here, the
// kind survives in the queue record across every one of those windows.
func (e *Engine) planRemove(viewID uint64, pb store.PathBaseline) plannedOp {
	op := store.Op{
		Type:      store.OpRemove,
		Direction: store.DirOutbound,
		ViewID:    viewID,
		Path:      pb.Path,
		FileID:    pb.FileID,
		BaseIsDir: pb.FileType == fileType(true),
	}
	apply := func(w store.Writer) error {
		return markLocalPending(e, w, viewID, pb.Path, func(*store.Baseline) {})
	}
	return plannedOp{op: op, apply: apply}
}

// planRename builds the OpRename (and its baseline relocation) for a same-scan move: the
// vanished baseline d is carried to the new file n's path, flipped to StatePending and
// refreshed with the new entry's local facts (the content is identical, but mtime/perm may
// differ). The op carries the moved node's server file id.
func (e *Engine) planRename(viewID uint64, d store.PathBaseline, n scanEntry) plannedOp {
	op := store.Op{
		Type:       store.OpRename,
		Direction:  store.DirOutbound,
		ViewID:     viewID,
		Path:       d.Path,
		NewPath:    n.relPath,
		FileID:     d.FileID,
		LocalHash:  n.md4,
		LocalSize:  n.size,
		LocalMTime: n.mtime,
		UnixPerm:   n.unixPerm,
	}
	apply := func(w store.Writer) error {
		b := d.Baseline
		b.LocalMTime = n.mtime
		b.LocalSize = n.size
		b.UnixPerm = n.unixPerm
		b.State = store.StatePending
		return w.MoveBaseline(viewID, d.Path, n.relPath, b)
	}
	return plannedOp{op: op, apply: apply}
}

// markLocalPending loads the baseline at view_id\0path (treating an absent record as a
// fresh one), applies mutate, forces State to StatePending and writes it back. It is the
// push-side counterpart to markSynced: the single place local-reconcile records that an
// outbound change is owed at a path, preserving any server facts already on the record
// (ServerHash/ServerSyncID/etc.) so a stale-base rejection (6d) can still compare against
// them.
func markLocalPending(e *Engine, w store.Writer, viewID uint64, path string, mutate func(*store.Baseline)) error {
	b, err := w.GetBaseline(viewID, path)
	switch {
	case isNotFound(err):
		b = store.Baseline{}
	case errors.Is(err, store.ErrCorruptRecord):
		// A corrupt prior is treated as fresh so the PutBaseline below overwrites and heals it,
		// mirroring markSynced. localReconcile already drops a scan's
		// corrupt (skipped) rows before emitting ops, so this is defence-in-depth for any other
		// caller reaching a bit-rotted key.
		e.warnCorruptBaseline("mark-local-pending", viewID, path, err)
		b = store.Baseline{}
	case err != nil:
		return fmt.Errorf("engine: load baseline %d/%q: %w", viewID, path, err)
	}
	mutate(&b)
	b.State = store.StatePending
	if err := w.PutBaseline(viewID, path, b); err != nil {
		return fmt.Errorf("engine: write pending baseline %d/%q: %w", viewID, path, err)
	}
	return nil
}

// moveKey is the content identity key for same-scan move detection: a file's MD4 hash and
// size. Two regular files with the same key are byte-identical, so a vanished baseline and
// a new file sharing one are a candidate move.
func moveKey(hash string, size int64) string {
	return fmt.Sprintf("%s|%d", hash, size)
}
