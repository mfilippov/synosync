// Package daemon assembles the long-running synosync sync process: it takes a
// single-instance lock, brings up the DESIGN §4 connection pool over a keychain-
// backed Connector, builds and runs the two-way sync engine (Phase 6) plus the
// filesystem watcher (Phase 7), and serves the local-only IPC control plane
// (internal/ipc) so the CLI can query live status.
//
// The daemon is deliberately seam-driven: every network and keychain side effect
// is resolved by the caller (the cmd layer) and injected through Deps, and the
// pool/engine bring-up and the single-instance lock are behind func fields, so
// Run is unit-testable with fakes and no live Synology server.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/backoff"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/engine"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
	"github.com/mfilippov/synosync/internal/watcher"
)

// ErrAlreadyRunning reports that another daemon instance already holds the
// single-instance lock for the same state directory.
var ErrAlreadyRunning = errors.New("daemon: another instance is already running")

// defaultNumWorkers is the worker-connection count requested from the pool when
// Deps leaves it unset. The pool clamps it into its own [2,4] range.
const defaultNumWorkers = 3

// poolState is the slice of the connection pool the status provider reads. The
// production *transport.Pool satisfies it; tests substitute a fake.
type poolState interface {
	Paused() bool
	// Reconnecting reports a mid-run re-dial in flight (a dropped connection being
	// re-established under backoff); surfaced as "reconnecting..." in `status`.
	Reconnecting() bool
	// PauseReason reports the sentinel the pool paused with, or nil while it is not
	// paused. The three terminal causes have three different operator actions, so
	// `status` classifies it rather than printing one hint for all of them.
	PauseReason() error
	NumWorkers() int
}

// engineControl is the slice of the sync engine the live-control IPC handlers
// drive. The production *engine.Engine satisfies it; tests substitute a fake.
type engineControl interface {
	Pause()
	Resume()
	Paused() bool
	TriggerSync()
	// DiskFull reports whether the most recent apply failed with ENOSPC; surfaced as
	// a status line. False before the engine is up.
	DiskFull() bool
	// StuckOps reports how many queued ops have repeatedly failed to apply without
	// being classified terminal; surfaced as a status line. Zero
	// before the engine is up.
	StuckOps() int
	// StuckDetails names the ops behind that count, worst first and capped at limit, with
	// the reason each keeps failing where there is one. The count alone reported that
	// something was wedged and nothing about what.
	StuckDetails(limit int) ([]engine.StuckOp, error)
	// Progress reports what this engine run has applied: operations, bytes of content,
	// and the window it has been moving them over. All zero before the engine is up, and
	// the window is zero until the first op applies - so a caller deriving a rate from it
	// must treat zero as "nothing to report" rather than dividing by it.
	Progress() (ops, bytes int64, window time.Duration)
	// LastContact reports when the server last answered this engine, or the zero time if
	// it has not during this run. Distinct from the pool being "connected", which
	// describes a socket rather than an exchange.
	LastContact() time.Time
	// Listing reports the initial-sync walk of a view's server tree while one runs: the view,
	// the directories and entries listed so far, and how long it has run; ok is false when
	// no walk is running.
	Listing() (viewID uint64, dirs, entries int64, elapsed time.Duration, ok bool)
}

// Deps bundles the daemon's collaborators. All network/keychain side effects are
// resolved by the caller and injected here so Run is unit-testable with fakes.
type Deps struct {
	// Store is the opened local state database.
	Store *store.Store
	// Config is the loaded, validated configuration.
	Config config.Config
	// Connector dials a fresh authenticated Session; built from config + keychain
	// (see NewConnector). Required unless an injected run seam replaces the pool.
	Connector transport.Connector
	// Client is the static client identity this build announces. It is injected
	// into every action request (agent / client_version / client_type), which a
	// live DSM validates on each turn, not just at auth.
	Client auth.ClientInfo
	// Session is the stored session token: the engine's selfSessionID and the auth
	// token injected into every action turn.
	Session string
	// SockPath is the IPC control socket path (typically Paths.DaemonSock()). Run
	// binds it only after taking the single-instance lock, so a duplicate start
	// cannot unlink a live daemon's socket.
	SockPath string
	// Logger is the slog logger; nil falls back to slog.Default.
	Logger *slog.Logger
	// LockPath is the single-instance lock file (typically Paths.LockFile()).
	LockPath string
	// NumWorkers is the requested worker-connection count; 0 uses defaultNumWorkers.
	NumWorkers int
	// WatcherFactory installs the engine's filesystem watcher (SetWatcher). Nil
	// uses the production internal/watcher factory; the engine tolerates a nil
	// return by falling back to the periodic scan.
	WatcherFactory func(ctx context.Context, root string) (<-chan struct{}, error)

	// runSync owns the pool+engine bring-up and run; nil uses defaultRunSync.
	// Injected by tests to drive Run with no live server.
	runSync func(ctx context.Context, d *Daemon) error
}

// Daemon is the assembled, runnable sync process. Construct it with New and drive
// its lifetime with Run.
type Daemon struct {
	store          *store.Store
	cfg            config.Config
	client         auth.ClientInfo
	connector      transport.Connector
	session        string
	sockPath       string
	log            *slog.Logger
	lockPath       string
	numWorkers     int
	watcherFactory func(ctx context.Context, root string) (<-chan struct{}, error)

	runSync func(ctx context.Context, d *Daemon) error

	server *ipc.Server

	// now is the time source (time.Now in production); injected by tests to
	// drive the view-counts cache TTL deterministically.
	now func() time.Time

	// viewScanHook, when set, runs at the start of every per-view cache-filling
	// scan. Tests only: it lets them count the scans a sequence of `status` calls
	// actually performs and advance the injected clock to model a scan slower than
	// the cache TTL (which is what makes cross-view convergence observable).
	viewScanHook func(viewID uint64)

	// countsCache holds the per-view O(view) counts served by buildStatus - the
	// StateConflict baselines and the durable keep-both conflict records - each
	// valid for viewCountsCacheTTL past its refill pass's completion (see
	// status.go). Guarded by its own mutex so a slow scan never holds up mu;
	// countsFlights holds the per-view single-flight slots that serialise the scans
	// themselves, so concurrent cache misses wait for one scan
	// instead of duplicating it. They are cap-1 channels rather than mutexes because
	// the wait must be abandonable: a caller whose handler budget expires while
	// another call's scan holds the slot has to fall through to the stale-counts
	// degrade path, not block for the whole scan (see viewCountsFor).
	countsMu      sync.Mutex
	countsCache   map[uint64]viewCounts
	countsFlights map[uint64]chan struct{}
	// countsFail is the NEGATIVE cache: the last failure of each view's count scan plus
	// when it happened. Errors used to be cached nowhere at all, so a
	// scan that deterministically outruns scanBudget was restarted by every fresh call -
	// one doomed two-minute walk (and its two-minute bbolt read transaction against the
	// data plane) per call, near-continuously under CLI polling - while the caller only
	// ever saw the IPC layer's generic "handler did not finish" from two minutes earlier.
	// Serving the recorded error for scanFailTTL stops the restarts; the diagnosis itself
	// is logged at the scan site, which is the only place that knows WHY the scan ended.
	countsFail map[uint64]scanFailure

	// conflictsCache holds the last ConflictReport buildConflicts assembled, valid
	// for conflictsCacheTTL past the scan's completion, and conflictsFlight
	// serialises the misses that rebuild it - the same TTL-cache + single-flight
	// shape as the per-view counts, for the same reason: the scan decodes every
	// conflict record of every view, so concurrent or polled `conflicts` calls
	// otherwise stacked full-bucket scans against the sync data plane. The cache is
	// global rather than per-view because the report is.
	conflictsMu     sync.Mutex
	conflictsCache  conflictsSnapshot
	conflictsFlight chan struct{}
	// conflictsFail is the whole-report half of countsFail's negative cache.
	conflictsFail scanFailure

	mu     sync.Mutex
	pool   poolState
	engine engineControl
	// baseCtx scopes the shared, cache-filling store scans (scanViewCounts,
	// scanConflicts) to the daemon's own lifetime instead of to the IPC handler
	// call that happened to trigger them - see scanBudget for why the two budgets
	// must differ. Run installs the serve context here so a scan in flight unwinds
	// on shutdown; New seeds context.Background() so a Daemon driven directly by a
	// test (or any pre-Run call) still scans.
	baseCtx context.Context
	// wantPaused is the operator's desired pause state. The engine may lag it
	// (defaultRunSync can sit in bring-up backoff for hours while the server is
	// unreachable), so the pause/resume handlers always record it here, setEngine
	// applies it to a freshly built engine, and buildStatus reports it while the
	// engine isn't up yet. Seeded from the store's persisted value in New so a
	// pause survives a daemon restart until an explicit resume.
	wantPaused bool
}

// New validates deps and constructs a Daemon, filling in production defaults for
// any unset seam (lock acquisition, pool+engine run, watcher factory, logger).
func New(deps Deps) (*Daemon, error) {
	if deps.Store == nil {
		return nil, errors.New("daemon: nil store")
	}
	if deps.SockPath == "" {
		return nil, errors.New("daemon: empty socket path")
	}
	if deps.LockPath == "" {
		return nil, errors.New("daemon: empty lock path")
	}
	// The default run seam (defaultRunSync) brings the pool up over deps.Connector;
	// with no run-seam override and no connector, pool.Start would dereference a nil
	// Connector func deep inside the run loop. Fail fast here with a clear
	// constructor error instead, mirroring the store/sock/lock validation.
	if deps.runSync == nil && deps.Connector == nil {
		return nil, errors.New("daemon: nil connector")
	}
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	// Route the store's best-effort corruption diagnostics (corrupt-record skips,
	// queue quarantine) through the daemon's rotating-file logger so they survive
	// where slog.Default() does not - discarded under Windows SCM, unrotated under
	// launchd - instead of a view silently ceasing to sync with no trace. Set
	// during assembly, before the engine/status goroutines read the store, so the
	// atomic-pointer field is never written concurrently with a read.
	deps.Store.SetLogger(log)
	// Resolve the worker count: an explicit Deps.NumWorkers (tests) wins, else the
	// loaded config's tunable (config.Limits.Workers, normally back-filled to
	// defaultWorkers when the config was loaded), else the package default. The pool
	// clamps the final value into its own [2,4] range.
	numWorkers := deps.NumWorkers
	if numWorkers == 0 {
		numWorkers = deps.Config.Limits.Workers
	}
	if numWorkers == 0 {
		numWorkers = defaultNumWorkers
	}
	d := &Daemon{
		store:          deps.Store,
		cfg:            deps.Config,
		client:         deps.Client,
		connector:      deps.Connector,
		session:        deps.Session,
		sockPath:       deps.SockPath,
		log:            log,
		lockPath:       deps.LockPath,
		numWorkers:     numWorkers,
		watcherFactory: deps.WatcherFactory,
		runSync:        deps.runSync,
		server:         ipc.NewServerWithLogger(log),
		now:            time.Now,
		baseCtx:        context.Background(),
		countsCache:    make(map[uint64]viewCounts),
		countsFlights:  make(map[uint64]chan struct{}),
		countsFail:     make(map[uint64]scanFailure),

		conflictsFlight: make(chan struct{}, 1),
	}
	if d.runSync == nil {
		d.runSync = defaultRunSync
	}
	if d.watcherFactory == nil {
		d.watcherFactory = defaultWatcherFactory(log, deps.Config.Limits.WatcherDebounce)
	}
	// Seed the desired pause state from the store so an explicit `synosync pause`
	// survives a daemon restart. A read failure is not fatal - the daemon starts
	// unpaused (the pre-persistence behaviour) and the store trouble surfaces
	// loudly on its own through the run loop.
	paused, perr := deps.Store.GetPaused()
	if perr != nil {
		log.Warn("daemon: read persisted pause state failed; starting unpaused", "err", perr)
	}
	d.wantPaused = paused
	d.registerHandlers()
	return d, nil
}

// registerHandlers installs the IPC method handlers: the read-only status,
// conflicts and config reports plus the Phase-8b live-control verbs
// (pause/resume/sync).
func (d *Daemon) registerHandlers() {
	d.server.Handle("status", d.handleStatus)
	d.server.Handle("conflicts", d.handleConflicts)
	d.server.Handle("drops", d.handleDrops)
	d.server.Handle("config", d.handleConfig)
	d.server.Handle("pause", d.handlePause)
	d.server.Handle("resume", d.handleResume)
	d.server.Handle("sync", d.handleSync)
}

// setPool records the live pool so the status provider can read its state. It is
// called by the run seam once the pool is up.
func (d *Daemon) setPool(p poolState) {
	d.mu.Lock()
	d.pool = p
	d.mu.Unlock()
}

// setEngine records the live engine so the live-control handlers can reach it. It
// is called by the run seam once the engine is built. A pause requested before
// the engine came up (or persisted across a restart) is applied here so the
// engine never starts syncing against the operator's wishes.
func (d *Daemon) setEngine(e engineControl) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.engine = e
	// Pause under d.mu: the engine is visible to getEngine the moment d.engine is
	// set, so applying the recorded state outside the lock races a concurrent
	// handleResume - its Resume() could land between the read of wantPaused and
	// this Pause(), leaving the engine paused against the recorded (and persisted)
	// unpaused state. Pause is a non-blocking atomic flip, safe under the mutex.
	if e != nil && d.wantPaused {
		e.Pause()
	}
}

// setBaseCtx installs the daemon-lifetime context the shared store scans run
// under. Run calls it before the IPC server accepts its first connection, so no
// handler can observe the pre-Run background context.
func (d *Daemon) setBaseCtx(ctx context.Context) {
	d.mu.Lock()
	d.baseCtx = ctx
	d.mu.Unlock()
}

// scanContext derives the context for one cache-filling store scan: bounded by
// scanBudget and cancelled with the daemon's own lifetime, never by the IPC
// handler that triggered it. The caller must call the returned cancel.
func (d *Daemon) scanContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(d.scanGate(), scanBudget)
}

// scanGate returns the daemon-lifetime context a COMMITTED refill pass gates its
// remaining per-view scans on (see refillViewCounts): the caller's handler budget
// decides whether the pass starts, the daemon's own lifetime decides whether it
// finishes.
func (d *Daemon) scanGate() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.baseCtx
}

// getEngine returns the recorded engine (nil before the run seam builds it).
func (d *Daemon) getEngine() engineControl {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.engine
}

// handlePause is the IPC handler for the "pause" method: it pauses syncing at the
// next cycle boundary. The desired state is always recorded (and persisted) even
// when the engine isn't up yet - setEngine applies it once the engine is built,
// so a pause issued during bring-up backoff is never lost.
func (d *Daemon) handlePause(_ context.Context, _ json.RawMessage) (any, error) {
	d.applyPausedState(true)
	return ipc.ControlAck{OK: true}, nil
}

// handleResume is the IPC handler for the "resume" method: it resumes a paused
// sync loop. Like handlePause, the desired state is always recorded and
// persisted, engine up or not.
func (d *Daemon) handleResume(_ context.Context, _ json.RawMessage) (any, error) {
	d.applyPausedState(false)
	return ipc.ControlAck{OK: true}, nil
}

// applyPausedState serializes the desired state, its durable copy, and the live
// engine transition as one operation. IPC handlers run concurrently; splitting
// these steps lets an older pause apply after a newer resume (and lets their
// store writes complete out of order), leaving memory, disk and engine state in
// disagreement. A persist failure only costs restart durability and is logged.
func (d *Daemon) applyPausedState(paused bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wantPaused = paused
	if err := d.store.PutPaused(paused); err != nil {
		d.log.Warn("daemon: persist pause state failed", "paused", paused, "err", err)
	}
	if d.engine == nil {
		return
	}
	if paused {
		d.engine.Pause()
	} else {
		d.engine.Resume()
	}
}

// handleSync is the IPC handler for the "sync" method: it triggers an immediate
// sync cycle (interrupting an in-flight long-poll). A no-op (still an ack) when
// the engine isn't up yet.
func (d *Daemon) handleSync(_ context.Context, _ json.RawMessage) (any, error) {
	if e := d.getEngine(); e != nil {
		e.TriggerSync()
	}
	return ipc.ControlAck{OK: true}, nil
}

// handleConflicts is the IPC handler for the read-only "conflicts" method. The
// report is assembled (or served from its TTL cache) under the handler's context
// which gates ENTERING the conflicts-bucket scan; a scan already under way finishes on
// its own budget so the next call is served from the cache.
// handleDrops serves the durable trace of work the engine gave up on.
func (d *Daemon) handleDrops(context.Context, json.RawMessage) (any, error) {
	return d.buildDrops()
}

func (d *Daemon) handleConflicts(ctx context.Context, _ json.RawMessage) (any, error) {
	return d.buildConflicts(ctx)
}

// Run acquires the single-instance lock, binds and serves the IPC control plane,
// and runs the sync loop until ctx is cancelled (graceful shutdown) or the sync
// loop ends on its own with a terminal error (a vanished session needing
// re-pair, a closed pool). It returns nil on a clean, ctx-driven shutdown and
// the terminal error otherwise. A second Run against a held lock path returns
// ErrAlreadyRunning.
//
// The lock is taken before the IPC socket is bound, so a duplicate start fails
// fast on the lock rather than first unlinking the live daemon's socket (ipc.Listen
// removes a stale socket before binding).
func (d *Daemon) Run(ctx context.Context) (err error) {
	release, lerr := acquireLock(d.lockPath)
	if lerr != nil {
		return lerr
	}
	defer func() {
		if rerr := release(); rerr != nil && err == nil {
			err = fmt.Errorf("daemon: release lock: %w", rerr)
		}
	}()

	listener, lerr := ipc.Listen(d.sockPath)
	if lerr != nil {
		return lerr
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Scope the shared status/conflicts scans to this run before the control plane
	// accepts anything, so one abandoned at shutdown unwinds with the daemon.
	d.setBaseCtx(ctx)

	// IPC server: Serve closes the listener (unlinking the AF_UNIX socket on Unix)
	// on ctx cancellation and returns nil on a clean shutdown. An unexpected Serve failure
	// (a non-shutdown Accept error) cancels the sync loop and is surfaced below so
	// the daemon does not report a clean (exit 0) shutdown for a crashed control plane.
	serveErrCh := make(chan error, 1)
	go func() {
		serr := d.server.Serve(ctx, listener)
		if serr != nil {
			d.log.Error("daemon: ipc serve failed", "err", serr)
			cancel()
		}
		serveErrCh <- serr
	}()

	// Sync loop: pool + engine. Ends when ctx is cancelled or a terminal error
	// surfaces.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- d.runSync(ctx, d)
	}()

	var syncErr error
	select {
	case <-ctx.Done():
		// Graceful shutdown signalled by the caller; the sync loop sees the same
		// cancelled ctx and unwinds.
		syncErr = <-syncDone
	case syncErr = <-syncDone:
		// The sync loop ended on its own (terminal error); tear the rest down.
		cancel()
	}
	serveErr := <-serveErrCh

	// A plain context cancellation is the graceful path, not an error; anything
	// else (re-pair required, closed pool) is surfaced to the caller - and logged
	// with a classification first: under the Windows SCM
	// stderr is discarded, so without this record the rotating log stays silent
	// on WHY the service exits and crash-loops every restart interval.
	if syncErr != nil && !errors.Is(syncErr, context.Canceled) {
		d.log.Error("daemon: sync loop stopped on terminal error",
			"reason", terminalStopReason(syncErr), "err", syncErr)
		return syncErr
	}
	// A failed control plane is also a failure: don't report a clean shutdown when
	// Serve died on an unexpected error (the sync loop then unwound on the cancel).
	if serveErr != nil {
		return serveErr
	}
	return nil
}

// terminalStopReason classifies a terminal sync/bring-up error for the stop-path
// log records: a vanished session (8193 - re-authenticate
// with `synosync login`), a certificate pin mismatch (a same-server key change or
// a MITM - re-pair with `synosync pair`), a rejected client_version (20481 - the
// server refused this binary; upgrade synosync), or anything
// else. The pin check runs first: the pool surfaces a bring-up mismatch as
// ErrPoolPaused WRAPPING ErrPinMismatch, and the pin is the more specific,
// differently-actioned cause. The client-rejected sentinel is distinct from
// ErrPoolPaused (it does not wrap it), so its arm order relative to the vanished
// arm is immaterial. The stderr hints in cmd/synosync's runDaemonLoop mirror this
// classification.
func terminalStopReason(err error) string {
	switch {
	case errors.Is(err, transport.ErrPinMismatch):
		return "pin mismatch"
	case errors.Is(err, transport.ErrPoolClientRejected), errors.Is(err, auth.ErrClientVersion):
		return "client version rejected"
	case errors.Is(err, auth.ErrSessionVanished), errors.Is(err, transport.ErrPoolPaused):
		return "session vanished"
	default:
		return "other"
	}
}

// defaultRunSync brings up the connection pool over the Connector, assembles the
// sync engine (installing the watcher factory), records the pool for status, and
// runs the engine until ctx is cancelled or a terminal error surfaces. The pool
// is closed on return.
//
// A transient connection failure during pool bring-up (server briefly
// unreachable, a TLS/DNS blip at boot) is not terminal: it is logged and retried
// under context-aware backoff, mirroring how the pool re-dials a mid-run drop, so
// the daemon does not die on the first hiccup if it starts before the network is
// up. Only a vanished session (re-login needed), an already-closed pool, or a
// cancelled context stops the loop.
func defaultRunSync(ctx context.Context, d *Daemon) error {
	bo := backoff.New()
	for {
		pool := transport.NewPoolWithLogger(d.connector, d.numWorkers, d.log)
		err := pool.Start(ctx)
		if err == nil {
			d.setPool(pool)
			t := engine.NewPoolTransportWithIdentity(pool, d.session, actions.Identity{
				Agent:         d.client.Agent(d.cfg.DeviceUUID),
				ClientVersion: d.client.ClientVersion(),
				ClientType:    d.client.Type,
				DsID:          d.cfg.Server.DsID,
			}, d.log)
			eng := engine.New(d.store, t, d.cfg.SyncRoot, d.session, d.log)
			// Apply the operator-tuned resource knobs (scan interval, drain batch, upload
			// thresholds, empty-scan threshold, poll timeouts, cold-start cap). A zero/absent
			// section is back-filled with the historical defaults by SetLimits.
			eng.SetLimits(d.cfg.Limits)
			// Decides which views sync and where each one lands on disk, so it must
			// be installed before Run resolves a single path.
			eng.SetViews(d.cfg.Views)
			d.setEngine(eng)
			if d.watcherFactory != nil {
				eng.SetWatcher(d.watcherFactory)
			}
			runErr := eng.Run(ctx)
			_ = pool.Close()
			return runErr
		}
		// Start already closed any partial sessions it opened; close the pool anyway
		// to release its channels before discarding it.
		_ = pool.Close()

		// A vanished session (8193, surfaced as ErrPoolPaused) or a certificate pin
		// mismatch (ErrPinMismatch - a same-server key change or a MITM) means
		// re-pairing is needed; a rejected client_version (20481, surfaced as
		// ErrPoolClientRejected) means the server refused this binary and re-dialing
		// cannot help (upgrade synosync); an already-closed pool
		// cannot be revived - all terminal, so surface them for Run to stop on rather
		// than retrying a doomed dial. A cancelled parent context ends the daemon
		// cleanly. Everything else is a transient dial error: retry under backoff.
		if errors.Is(err, transport.ErrPoolPaused) || errors.Is(err, transport.ErrPoolClosed) ||
			errors.Is(err, transport.ErrPinMismatch) || errors.Is(err, transport.ErrPoolClientRejected) {
			// Log the terminal bring-up failure at Error before surfacing it:
			// only transient failures were Warned here, so a
			// service restarted by its supervisor died on this same Start every few
			// seconds with no log line naming the stage or the cause.
			d.log.Error("daemon: connection pool start failed terminally; stopping",
				"reason", terminalStopReason(err), "err", err)
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.log.Warn("daemon: connection pool start failed; retrying", "err", err)
		if werr := bo.Wait(ctx); werr != nil {
			return werr
		}
	}
}

// defaultWatcherFactory builds the production SetWatcher factory: it constructs an
// internal/watcher on the root Run supplies and returns its debounced wake channel. The
// debounce window (milliseconds, config.Limits.WatcherDebounce) is passed through as a
// watcher option; a non-positive value is ignored by WithDebounce, so the watcher's own
// default applies when the knob is unset.
func defaultWatcherFactory(log *slog.Logger, debounceMS int) func(ctx context.Context, root string) (<-chan struct{}, error) {
	return func(ctx context.Context, root string) (<-chan struct{}, error) {
		return watcher.New(root, log, watcher.WithDebounce(time.Duration(debounceMS)*time.Millisecond)).Watch(ctx)
	}
}
