// Package watcher provides a filesystem watcher over the sync root built on
// github.com/fsnotify/fsnotify. It watches the root and all subdirectories,
// registering newly-created subtrees as they appear: fsnotify's public Add is
// non-recursive on every platform, so the watcher registers each directory
// itself (addSubtree at startup and on a directory Create). Bursts of events are debounced and coalesced
// into a single "something changed" wake delivered on a buffered channel, so a
// local change can trigger a prompt engine localScan instead of waiting for the
// next periodic poll.
//
// The watcher is a latency optimization, not a correctness dependency: the
// authoritative diff is still scanView + MD4 driven by the engine's periodic
// catch-up scan, so a missed or filtered event at worst adds latency until that
// scan runs.
//
// That bound holds per event, and holding it for the watcher as a whole takes one more
// thing: the run loop re-verifies periodically that the sync root is still watched
// (recheckRoot), because a root that is deleted and recreated takes every watch with it
// and can never be noticed through the event stream - see defaultRootRecheck.
package watcher

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/fsnotify/fsnotify"
)

// defaultDebounce is the window over which filesystem events are coalesced into
// a single wake.
const defaultDebounce = 500 * time.Millisecond

// defaultRootRecheck is how often the run loop re-verifies that the sync root is still
// watched at all.
//
// A watch is bound to a directory, not to a path. If the sync root itself is deleted -
// a restored folder, a remounted volume, a tool that replaced the directory - the watch
// dies with it, and the replacement is watched by nobody: the root's parent is outside
// the tree and is never watched, so no Create event for the new root can ever arrive,
// and every subdirectory registration under it referred to entries that no longer
// exist. The watcher then stays silent for the rest of the daemon's life. That is worse
// than this package's stated failure mode (one missed event costs latency until the
// next periodic scan): here EVERY local change waits for the periodic scan, forever,
// with nothing said about it.
//
// Detecting it by directory IDENTITY was tried and does not work. On Windows a root
// deleted and immediately recreated under the same name comes back with the same NTFS
// index AND the same creation and modification timestamps - file tunneling preserves
// them deliberately - so os.SameFile reports the replacement as the original and every
// timestamp comparison agrees with it. The authoritative signal is fsnotify's own
// registry instead: measured on Windows, deleting the watched root emits a Remove for
// the root path and drops it from WatchList, and the inotify backend likewise discards
// the watch on IN_IGNORED. So the question the recheck asks is simply whether the root
// is still in WatchList, which is true regardless of how the backend learned it.
//
// Thirty seconds bounds the blind window well under the engine's periodic scan. The
// check allocates one slice of the currently-watched paths per tick, which on an
// uncapped platform is the whole tree's directory count; that is transient garbage
// every 30s, against a failure that otherwise lasts until the daemon restarts.
const defaultRootRecheck = 30 * time.Second

// darwinMaxWatches caps the file DESCRIPTORS the macOS watcher may cause fsnotify to
// open, so an unbounded tree cannot walk the process toward kern.maxfilesperproc and
// land the collateral EMFILE on the engine's hashing, the connection pool or the IPC
// listener - a data-plane failure the latency-only watcher must never cause. 4096
// leaves ample headroom under the default ~10240 per-process limit; other platforms
// (inotify, ReadDirectoryChangesW) consume no descriptor per watch and stay uncapped.
//
// The unit is descriptors, not directories, and the difference is the whole point.
// fsnotify's kqueue backend opens one fd for the directory AND one for every entry
// inside it (backend_kqueue.go: watchDirectoryFiles → internalWatch → addWatch, on
// each os.ReadDir entry, files included). Counting directories therefore measured the
// wrong resource by whatever the tree's fan-out happens to be: a single directory
// holding 20k files passed the cap with one registration and exhausted the process's
// descriptors anyway. Worse, the cap's own degradation path - errWatchCap, which sends
// the engine to its periodic scan - was then unreachable, because Add failed with
// EMFILE long before any count reached the limit.
const darwinMaxWatches = 4096

// defaultMaxWatches returns the per-GOOS cap on watch registrations
// (0 = unlimited).
func defaultMaxWatches() int {
	if runtime.GOOS == "darwin" {
		return darwinMaxWatches
	}
	return 0
}

// errWatchCap reports that registering one more directory would exceed the descriptor budget.
var errWatchCap = errors.New("watch cap exceeded")

// fsWatcher is the narrow seam over *fsnotify.Watcher the event loop drives. It
// exposes the watcher's two channels as methods and the Add/Close/WatchList calls
// run uses, so run can be unit-tested with a fake that injects synthetic events
// and errors (notably fsnotify.ErrEventOverflow) without a real kernel watch.
type fsWatcher interface {
	Events() <-chan fsnotify.Event
	Errors() <-chan error
	Add(string) error
	// WatchList reports the paths currently registered. A backend drops a path from
	// it when the directory behind the watch goes away, which is how recheckRoot
	// learns the sync root stopped being watched.
	WatchList() []string
	Close() error
}

// Watcher watches a sync root and emits debounced wake signals.
//
// watched/watchCount/capWarned are not locked: they are written during Watch
// (caller goroutine) and then only from the run goroutine, with the
// happens-before edge established by the `go w.run(...)` handoff.
type Watcher struct {
	root       string
	log        *slog.Logger
	debounce   time.Duration
	maxWatches int                 // descriptor budget for watch registrations; 0 = unlimited
	watched    map[string]struct{} // distinct paths registered so far; maintained ONLY when maxWatches > 0 (its sole reader is the cap-gate)
	watchCount int                 // descriptors charged so far (high-water mark, never decremented); tracked ONLY when maxWatches > 0
	capWarned  bool                // the one runtime cap Warn per Watch has fired

	rootRecheck time.Duration // how often run verifies the root is still in the watch registry
	rootWarned  bool          // the one "cannot re-register the root" Warn per outage has fired

	// recursive is set when one watch on the root covers its whole subtree (the Windows
	// backend), so no directory beneath it is registered on its own.
	recursive bool
}

// Option customises a Watcher.
type Option func(*Watcher)

// WithDebounce overrides the debounce window. A non-positive value is ignored.
// Tests use a short window; production keeps the default.
func WithDebounce(d time.Duration) Option {
	return func(w *Watcher) {
		if d > 0 {
			w.debounce = d
		}
	}
}

// WithMaxWatches overrides the per-GOOS cap on watch registrations. Zero
// disables the cap; a negative value is ignored. Tests use a tiny cap;
// production keeps the default (darwin darwinMaxWatches, unlimited elsewhere).
func WithMaxWatches(n int) Option {
	return func(w *Watcher) {
		if n >= 0 {
			w.maxWatches = n
		}
	}
}

// WithRootRecheck overrides how often the run loop verifies that the sync root is
// still watched. A non-positive value is ignored. Tests use a short interval;
// production keeps the default.
func WithRootRecheck(d time.Duration) Option {
	return func(w *Watcher) {
		if d > 0 {
			w.rootRecheck = d
		}
	}
}

// New constructs a Watcher rooted at root. A nil logger is replaced with a
// discarding logger so callers need not guard every log call.
func New(root string, log *slog.Logger, opts ...Option) *Watcher {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	w := &Watcher{root: root, log: log, debounce: defaultDebounce, rootRecheck: defaultRootRecheck,
		maxWatches: defaultMaxWatches(), watched: make(map[string]struct{})}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Watch starts watching the root and returns a buffered wake channel (capacity
// 1). Each burst of filesystem activity collapses to one pending wake; a send is
// non-blocking so a wake is coalesced rather than lost when the consumer is
// slow. The channel is closed when ctx is cancelled and the underlying fsnotify
// watcher has been shut down. Watch returns an error only if the watcher cannot
// be created or the root cannot be added.
func (w *Watcher) Watch(ctx context.Context) (<-chan struct{}, error) {
	fw, recursive, err := newFSWatcher()
	if err != nil {
		return nil, fmt.Errorf("watcher: new fsnotify watcher: %w", err)
	}
	w.recursive = recursive
	if err := w.addWatch(fw, w.root); err != nil {
		_ = fw.Close()
		return nil, fmt.Errorf("watcher: add root %q: %w", w.root, err)
	}
	// Best-effort registration of existing subdirectories; an unreadable
	// subtree is logged and skipped rather than aborting the watch. Exceeding
	// the watch cap at startup, however, IS fatal: a tree too large to watch
	// degrades to the engine's periodic scan (the SetWatcher failure path)
	// rather than silently watching an arbitrary subset of it.
	if err := w.addSubtree(fw, w.root); err != nil {
		_ = fw.Close()
		return nil, fmt.Errorf("watcher: register subtree of %q (%d watches): %w", w.root, w.watchCount, err)
	}

	wake := make(chan struct{}, 1)
	go w.run(ctx, fw, wake)
	return wake, nil
}

// recheckRoot re-registers the sync root and its subtree when the root has fallen out
// of the underlying watcher's registry, and reports whether the caller should force a
// wake. A dropped root watch means the engine has been blind to everything under it, so
// the tree must be rescanned, not merely re-watched.
//
// Membership in WatchList is the whole test, and it is deliberately not a check on the
// directory's identity - see defaultRootRecheck for why identity cannot answer this on
// Windows. A backend discards the watch when the directory it refers to goes away, so
// the root's absence from the registry is the signal, whether the root was deleted and
// recreated, remounted, or is simply gone right now.
//
// Failure to re-add is not itself an error state: while the root does not exist, Add
// fails every tick, so the Warn fires once per outage and the ticks that follow are
// silent until it succeeds. errWatchCap takes that same path - a replacement tree too
// large to watch degrades to the periodic scan exactly as it would at startup.
//
// The cap bookkeeping needs no special case. w.watched is never pruned, so every path
// that survives the replacement is "already" registered: Add is re-issued as the no-op
// refresh that rebinds it, and nothing is re-charged. Genuinely new directories in the
// replacement tree are charged normally.
func (w *Watcher) recheckRoot(fsw fsWatcher) bool {
	if slices.Contains(fsw.WatchList(), w.root) {
		return false
	}
	if err := w.addWatch(fsw, w.root); err != nil {
		if !w.rootWarned {
			w.rootWarned = true
			w.log.Warn("watcher: the sync root is no longer watched and cannot be re-registered; local changes rely on the periodic scan until it returns",
				"root", w.root, "err", err)
		}
		return false
	}
	w.rootWarned = false
	if err := w.addSubtree(fsw, w.root); err != nil {
		w.warnCapExceeded(w.root)
	}
	w.log.Warn("watcher: the sync root's watch was dropped (deleted and recreated, or remounted); re-registered it and its subtree",
		"root", w.root)
	return true
}

// addWatch registers path with the underlying watcher unless the cap is
// saturated. The count is a high-water mark of DISTINCT registrations: each
// unique path is counted exactly once, tracked in w.watched, because the
// re-walk in registerCreatedDir (and the startup addSubtree) revisits
// already-watched directories - fsnotify's Add is idempotent and consumes no
// new fd for a path it already watches, so re-adding one must neither count
// against the cap nor be refused by it. A path already in w.watched still
// re-issues Add (a no-op refresh that re-establishes the kernel watch if the
// directory was deleted and recreated) but does not re-increment.
//
// The distinct-path set exists ONLY to serve the cap: its sole reader is the
// cap-gate below. On an uncapped platform (maxWatches == 0, every non-darwin
// GOOS) the set therefore has no reader and is NOT maintained - otherwise it
// would grow monotonically with the union of every directory ever created under
// the root over the daemon's lifetime (fsnotify drops the kernel watch when a
// directory is deleted, but the map key would survive; the run loop only
// handles Create, so nothing prunes it), a slow memory leak in a long-lived
// daemon syncing a churn-heavy tree. When the cap IS active (darwin) the set is
// still never pruned on a delete - tracking live watches would let a churn-heavy
// tree oscillate around the cap, which exists to bound worst-case fd use, not to
// be precise, and darwinMaxWatches (≤4096) bounds the set's size regardless.
func (w *Watcher) addWatch(fsw fsWatcher, path string) error {
	if w.maxWatches <= 0 {
		// Uncapped: register directly, keep no per-path bookkeeping.
		return addOrLimit(fsw, path)
	}
	_, already := w.watched[path]
	cost := 0
	if !already {
		cost = watchCost(path)
		if w.watchCount+cost > w.maxWatches {
			return errWatchCap
		}
	}
	if err := addOrLimit(fsw, path); err != nil {
		return err
	}
	if !already {
		w.watched[path] = struct{}{}
		w.watchCount += cost
	}
	return nil
}

// addOrLimit registers path, reporting a SYSTEM watch limit (isWatchLimit) as errWatchCap: the
// kernel's allowance ran out exactly as our own budget can, and every caller already knows
// what that means - stop registering, warn once, and leave the rest to the periodic scan.
func addOrLimit(fsw fsWatcher, path string) error {
	err := fsw.Add(path)
	if err != nil && isWatchLimit(err) {
		return fmt.Errorf("%w: the system's watch limit is reached (on Linux, raise fs.inotify.max_user_watches): %w", errWatchCap, err)
	}
	return err
}

// watchCost is what registering one directory costs the descriptor budget: the
// directory itself plus every entry the kqueue backend opens inside it.
//
// The count is taken once, at registration. It is a budget, not a ledger: entries
// created later are charged when THEIR directory is registered (a created subdirectory
// goes through registerCreatedDir) and files added to an already-watched directory are
// not charged at all. That undercounts a directory that grows after registration, which
// is the same kind of imprecision the never-pruned w.watched set already accepts - the
// cap bounds the worst case at startup, where the whole tree is enumerated at once and
// where the exhaustion actually happened.
//
// A directory that cannot be read is charged 1. Refusing to register it instead would
// turn an unreadable subdirectory - which addSubtree deliberately logs and skips - into
// a hard failure of the whole watch.
func watchCost(path string) int {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 1
	}
	return 1 + len(entries)
}

// addSubtree walks dir and registers every directory under it (dir itself is
// assumed already added). Individual failures are logged, not fatal; hitting
// the watch cap aborts the walk and is returned so the caller can degrade.
func (w *Watcher) addSubtree(fsw fsWatcher, dir string) error {
	if w.recursive {
		return nil // the root's watch already covers every directory beneath it
	}
	var capErr error
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			w.log.Warn("watcher: walk error", "path", path, "err", err)
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() || path == dir {
			return nil
		}
		if err := w.addWatch(fsw, path); err != nil {
			if errors.Is(err, errWatchCap) {
				capErr = err
				return filepath.SkipAll
			}
			w.log.Warn("watcher: add subdir", "path", path, "err", err)
		}
		return nil
	})
	return capErr
}

// registerCreatedDir adds a watch for a directory created at runtime and then
// re-walks it to register any descendants created in the gap before the watch
// was added. It returns errWatchCap when the cap prevented (part of) the
// registration; any other add failure is logged and swallowed as before.
func (w *Watcher) registerCreatedDir(fsw fsWatcher, dir string) error {
	if w.recursive {
		return nil // the root's watch already covers it, descendants included
	}
	if err := w.addWatch(fsw, dir); err != nil {
		if errors.Is(err, errWatchCap) {
			return err
		}
		w.log.Warn("watcher: add created dir", "path", dir, "err", err)
		// The parent add can fail for reasons that do not apply to its
		// descendants (a replaced directory, a transient EMFILE), so still walk
		// the subtree - the pre-cap behaviour.
	}
	// A directory may be populated before we register it; pick up any
	// descendants created in the gap.
	return w.addSubtree(fsw, dir)
}

// warnCapExceeded logs the runtime cap saturation once per Watch - a busy tree
// creating many directories would otherwise repeat the same Warn per event.
func (w *Watcher) warnCapExceeded(path string) {
	if w.capWarned {
		return
	}
	w.capWarned = true
	w.log.Warn("watcher: watch cap exceeded; new subtrees are not watched and rely on the periodic scan",
		"path", path, "max_watches", w.maxWatches)
}

// run is the event loop: it reads fsnotify events, registers newly-created
// subdirectories, and debounces activity into coalesced wakes until ctx is
// cancelled or the fsnotify channels close.
func (w *Watcher) run(ctx context.Context, fsw fsWatcher, wake chan struct{}) {
	defer close(wake)
	defer func() { _ = fsw.Close() }()

	timer := time.NewTimer(w.debounce)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time
	pending := false

	// The root recheck runs on its own ticker rather than off the event stream:
	// the event that would announce the root's disappearance is exactly the one a
	// dropped watch cannot deliver, and the backends disagree about whether they
	// report it at all.
	rootTick := time.NewTicker(w.rootRecheck)
	defer rootTick.Stop()

	// arm marks a change pending and (re)starts the debounce timer. Stop and
	// drain before reset so a concurrently-fired timer can't leave a stale
	// value in the channel (the documented Timer.Reset gotcha).
	arm := func() {
		pending = true
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.debounce)
		timerC = timer.C
	}

	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case ev, ok := <-fsw.Events():
			if !ok {
				return
			}
			if ev.Op&fsnotify.Create != 0 {
				// Lstat (not Stat) so a created symlink to a directory is not
				// followed and registered - that would watch an out-of-tree target,
				// inconsistent with addSubtree's WalkDir (which uses Lstat) and the
				// engine's strict symlink refusal along sync paths.
				if info, err := os.Lstat(ev.Name); err == nil && info.IsDir() {
					if err := w.registerCreatedDir(fsw, ev.Name); err != nil {
						// Cap saturated at runtime: (part of) the new subtree is
						// not watched, so changes under it produce no events.
						// Keep the existing watches, warn once, and force a
						// debounced wake (like an overflow) so the engine scans
						// promptly; the periodic scan covers the unwatched
						// subtree from here on.
						w.warnCapExceeded(ev.Name)
						arm()
					}
				}
			}
			// Drop self-induced/transient noise so an all-filtered burst never
			// wakes; a mixed burst still wakes once via the surviving events.
			if shouldIgnore(ev.Name) {
				continue
			}
			arm()
		case err, ok := <-fsw.Errors():
			if !ok {
				return
			}
			w.log.Warn("watcher: fsnotify error", "err", err)
			// On a kernel-buffer overflow fsnotify has dropped events: we know
			// the tree changed but not where. Force a debounced wake so the
			// engine runs a prompt localScan instead of waiting for the periodic
			// catch-up scan to notice.
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				arm()
			}
		case <-rootTick.C:
			if w.recheckRoot(fsw) {
				arm()
			}
		case <-timerC:
			timerC = nil
			if pending {
				pending = false
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}
}
