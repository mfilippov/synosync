package main

// The `daemon` subcommand runs the sync process in the foreground: it opens the
// local state database, loads the paired config from it, builds the
// keychain-backed connection Connector (verify-pin dial + mode-A session reuse),
// and runs the assembled daemon (pool + engine + watcher + IPC) until interrupted
// with SIGINT/SIGTERM. The daemon itself takes the single-instance lock and only
// then binds the local-only IPC control socket. All side effects are routed
// through injection vars (mirroring pair/login) so the preflight and assembly are
// unit-tested without a live Synology server.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"golang.org/x/term"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/daemon"
	"github.com/mfilippov/synosync/internal/logging"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// Injection points overridden by daemon/status tests. In production they resolve
// the real platform paths, open the real state database and IPC listener, and run
// the assembled daemon.
var (
	// resolveDaemonPaths returns the platform config/state directories.
	resolveDaemonPaths = func() (config.Paths, error) { return config.DefaultPaths() }
	// openDaemonStore opens the bbolt state database at path.
	openDaemonStore = func(path string) (*store.Store, error) { return store.Open(path) }
	// runDaemon runs the assembled daemon until ctx is cancelled. Tests swap this
	// for a stub so the assembly is exercised without a live server.
	runDaemon = func(ctx context.Context, d *daemon.Daemon) error { return d.Run(ctx) }
	// openLogWriter opens the rotating daemon log writer at path (creating the log
	// directory as needed). Tests swap this for a stub so logger assembly is
	// exercised without writing into a real log directory.
	openLogWriter = func(path string) (io.WriteCloser, error) {
		return logging.NewRotatingWriter(path, logging.RotateOptions{})
	}
	// stderrIsTerminal reports whether w (the daemon's stderr) is an interactive
	// terminal, so the file log is mirrored there. Tests override it.
	stderrIsTerminal = func(w io.Writer) bool {
		f, ok := w.(*os.File)
		return ok && term.IsTerminal(int(f.Fd()))
	}
	// reportServiceStartFailure surfaces a daemon start failure that happened
	// before the logger could be built on a per-OS side channel: under the
	// Windows Service Control Manager stderr is discarded and no log file exists
	// yet, so serviceStartFallback writes one best-effort event-log record there
	// (a no-op everywhere else - stderr is already observable in the foreground
	// and under systemd/launchd). An injection var so the pre-logger failure
	// path is testable cross-platform.
	reportServiceStartFailure = serviceStartFallback
)

// daemonStartFailure reports a preflight failure that happened before the
// daemon logger could be built: always on stderr, plus the per-OS pre-logger
// side channel (Windows event log under SCM). Returns the exit code 1.
func daemonStartFailure(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "synosync: %s\n", msg)
	reportServiceStartFailure(msg)
	return 1
}

// buildDaemonLogger constructs the daemon logger from the resolved config and
// paths. With logStderr it writes only to stderr (no file) - the right sink when
// a supervisor (systemd/launchd) captures stderr into its own journal.
// Otherwise it writes to the rotating log file, additionally mirroring to stderr
// when stderr is an interactive terminal. The returned closer (nil in the
// stderr-only case) must be closed when the daemon exits.
func buildDaemonLogger(cfg config.Config, paths config.Paths, logStderr bool, stderr io.Writer) (*slog.Logger, io.Closer, error) {
	opts := logging.Options{Level: cfg.LogLevel, Format: logging.Format(cfg.LogFormat)}
	if logStderr {
		log, err := logging.New(stderr, opts)
		if err != nil {
			return nil, nil, err
		}
		return log, nil, nil
	}

	w, err := openLogWriter(paths.LogFile())
	if err != nil {
		return nil, nil, err
	}
	var out io.Writer = w
	if stderrIsTerminal(stderr) {
		// io.MultiWriter stops at the first failing writer, so an unwrapped file
		// error would silence the stderr mirror too; the adapter suppresses file
		// errors (the RotatingWriter self-heals on later writes) so the mirror
		// receives every record, noting the first failure of a streak on stderr.
		out = io.MultiWriter(&bestEffortWriter{w: w, note: stderr}, stderr)
	}
	log, err := logging.New(out, opts)
	if err != nil {
		_ = w.Close()
		return nil, nil, err
	}
	return log, w, nil
}

// bestEffortWriter suppresses errors from the wrapped log-file writer so the
// io.MultiWriter sibling after it (the interactive stderr mirror) still
// receives every record when the file sink fails. Suppression loses at most the
// records of one failure streak (the RotatingWriter retries its open on every
// write), and the first failure of each streak is noted once on note so the
// operator watching the terminal learns the file sink is down. Writes are
// serialised by the slog handler's internal mutex, so failing needs no
// synchronisation of its own.
type bestEffortWriter struct {
	w       io.Writer
	note    io.Writer
	failing bool
}

func (b *bestEffortWriter) Write(p []byte) (int, error) {
	if _, err := b.w.Write(p); err != nil {
		if !b.failing {
			b.failing = true
			fmt.Fprintf(b.note, "synosync: log file write failed (stderr mirror still active): %s\n", userErr(err))
		}
	} else {
		b.failing = false
	}
	return len(p), nil
}

// daemonHelp is the `daemon` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var daemonHelp = helpSpec{
	name:    "daemon",
	summary: "run the sync daemon in the foreground",
	usage:   []string{"synosync daemon [flags]"},
	describe: `Runs the sync process in the foreground: the connection pool, the two-way sync
engine and the filesystem watcher. Stop it with Ctrl-C.

Requires a paired config, so run ` + "`" + `pair` + "`" + ` first - it stores the session the daemon
reuses, so no separate ` + "`" + `login` + "`" + ` is needed. Run ` + "`" + `login` + "`" + ` only when the daemon
reports that the session is gone. Only one daemon may use a state directory at a
time; a second one exits immediately rather than competing for the database.

It serves the local control plane the other subcommands talk to, and writes a
rotating log file unless --log-stderr is given.

To keep it running without a terminal, use ` + "`synosync service install`" + `, which
registers it with the platform's service manager and starts it. The same unit
files also ship in packaging/ inside the release archive, for a manual install.

Pair FIRST, and as the same user the service will run as - the daemon reads
that user's stored credentials and serves a control plane only that user can
reach.`,
	examples: []string{
		"synosync daemon",
		"synosync daemon --log-stderr    # under systemd, whose journal does the rotating",
		"synosync service install        # run it in the background instead",
	},
}

// cmdDaemon implements the `daemon` subcommand.
func cmdDaemon(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	logStderr := fs.Bool("log-stderr", false, "log to stderr only (no log file); for systemd/launchd capturing stderr")
	if code, ok := parseFlags(fs, daemonHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(daemonHelp, fs.Args(), stderr); !ok {
		return code
	}

	paths, err := resolveDaemonPaths()
	if err != nil {
		return daemonStartFailure(stderr, userErr(err))
	}

	// Open the store first: it is the single home for both the settings blob and
	// the durable state, and its exclusive bbolt lock fails fast (store.ErrLocked)
	// when another daemon instance already holds the database. Opening it also
	// creates the state directory, so the IPC socket and lock file (both alongside
	// state.db) have a directory to live in.
	st, err := openDaemonStore(paths.StateDB())
	if err != nil {
		if errors.Is(err, store.ErrLocked) {
			return daemonStartFailure(stderr, "the state database is in use (another daemon instance running?); stop it and retry")
		}
		return daemonStartFailure(stderr, fmt.Sprintf("open state db: %s", userErr(err)))
	}
	defer func() { _ = st.Close() }()
	// Publish the handle for the stop-reason helpers: the daemon holds bbolt's exclusive lock
	// for its whole life, so they cannot open one of their own.
	daemonStore = st
	defer func() { daemonStore = nil }()

	cfg, err := st.GetConfig()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return daemonStartFailure(stderr, "no config found; run `synosync pair` first")
		}
		if errors.Is(err, store.ErrCorruptRecord) {
			return daemonStartFailure(stderr, fmt.Sprintf("load config: %s; the stored settings are corrupt - run `synosync unlink` to clear local state, then `synosync pair` to re-pair", err))
		}
		return daemonStartFailure(stderr, fmt.Sprintf("load config: %s", userErr(err)))
	}

	// Build the logger as early as the loaded config allows (it carries the log
	// level/format), before the remaining preflight, so every later failure lands
	// in the rotating log file too: under the Windows Service Control Manager
	// stderr is discarded, and the log file is where a start failure is
	// observable. Under SCM there is also no console, so effectiveLogStderr
	// forces the rotating file sink regardless of --log-stderr; on every other
	// platform it returns the flag unchanged. logging.New tolerates an empty
	// level/format from a not-yet-validated blob (it defaults them); an invalid
	// one fails here with the same message Validate would give.
	log, logCloser, err := buildDaemonLogger(cfg, paths, effectiveLogStderr(*logStderr), stderr)
	if err != nil {
		return daemonStartFailure(stderr, userErr(err))
	}
	if logCloser != nil {
		defer func() { _ = logCloser.Close() }()
	}

	// preflightFail reports a post-logger preflight failure on both sinks: stderr
	// for the foreground operator, log.Error for the rotating file (the only
	// surviving sink under SCM, where stderr is discarded).
	preflightFail := func(msg string) int {
		fmt.Fprintf(stderr, "synosync: %s\n", msg)
		log.Error("daemon start failed", "error", msg)
		return 1
	}

	if err := cfg.Validate(); err != nil {
		return preflightFail(userErr(err))
	}
	if cfg.DeviceUUID == "" || cfg.Server.DsID == "" {
		return preflightFail("not paired yet; run `synosync pair` first")
	}

	// A missing pin or session is a clear re-pair/re-login signal.
	connector, session, err := daemon.NewConnector(cfg, openKeyring(), clientInfo(), log)
	if err != nil {
		code := preflightFail(userErr(err))
		if hint := keyringHint(err); hint != "" {
			fmt.Fprintf(stderr, "synosync: hint: %s\n", hint)
		}
		return code
	}

	d, err := daemon.New(daemon.Deps{
		Store:     st,
		Config:    cfg,
		Client:    clientInfo(),
		Connector: connector,
		Session:   session,
		SockPath:  paths.DaemonSock(),
		Logger:    log,
		LockPath:  paths.LockFile(),
	})
	if err != nil {
		return preflightFail(userErr(err))
	}

	// serveDaemon is platform-specific: on non-Windows it runs in the foreground
	// until SIGINT/SIGTERM; on Windows it additionally speaks the Service Control
	// Manager dispatcher protocol when launched as a service.
	return serveDaemon(d, log, stdout, stderr, serverAddr(cfg))
}

// runDaemonLoop runs the assembled daemon under ctx, printing the lifecycle
// messages and mapping terminal errors to an exit code. It is the shared core of
// the foreground (signal-driven) and Windows-service entrypoints in
// service_other.go / service_windows.go. interactive is false under the Windows
// Service Control Manager, where there is no console: the start banner then omits
// the "(Ctrl-C to stop)" hint, which is both meaningless (SCM stops the service
// via a control, not Ctrl-C) and discarded with the rest of the daemon's stdout.
//
// Every stop-with-error branch is duplicated into log (the daemon's rotating-file
// logger) alongside the stderr rendering: under SCM stderr is
// discarded, and these actionable hints (re-login / re-pair) are what turns an
// undiagnosable crash-loop into a one-command fix. A nil log falls back to
// slog.Default, mirroring daemon.New.
func runDaemonLoop(ctx context.Context, d *daemon.Daemon, log *slog.Logger, stdout, stderr io.Writer, addr string, interactive bool) int {
	if log == nil {
		log = slog.Default()
	}
	if interactive {
		fmt.Fprintf(stdout, "synosync daemon started for %s (Ctrl-C to stop)\n", addr)
	} else {
		fmt.Fprintf(stdout, "synosync daemon started for %s\n", addr)
	}
	// The start and a clean stop go to the log file too. Only the error exits did, so a
	// stopped and restarted daemon left no seam in the file, and a report could not say
	// which build was running when.
	log.Info("daemon started", "server", addr, "version", versionLine())
	// Starting clears any recorded stop reason, so a fault that has since been fixed is not
	// still being reported to the next CLI run. A daemon that fails the same way again records
	// it afresh on the way out.
	if cerr := clearLastStop(); cerr != nil {
		log.Debug("daemon: could not clear the previous stop reason", "err", cerr)
	}
	if err := runDaemonAnnouncingStop(ctx, d, stdout, interactive); err != nil {
		if errors.Is(err, daemon.ErrAlreadyRunning) {
			fmt.Fprintln(stderr, "synosync: another daemon instance is already running")
			log.Error("daemon stopped: another instance is already running", "err", err)
			return 1
		}
		fmt.Fprintf(stderr, "synosync: daemon stopped: %s\n", userErr(err))
		// A terminal authentication/pinning failure needs operator action: a
		// vanished session re-authenticates with `synosync login`, a changed
		// certificate re-pairs with `synosync pair`. Mirror login.go's hints so the
		// daemon's exit is actionable rather than a bare error line.
		// The reason is also PERSISTED, because under a service manager none of the above
		// reaches anybody: the supervisor restarts the daemon, it fails the same way and dies
		// again, and `synosync status` - which needs a running daemon to say anything - reports
		// only "daemon is not running". The next CLI run reads the record and says what
		// happened. A failure to record it is not worth failing the exit over; the daemon is
		// already stopping and the log line above stands.
		var stop store.LastStop
		if errors.Is(err, transport.ErrPinMismatch) {
			fmt.Fprintln(stderr, "synosync: the server certificate changed; re-pair with `synosync pair`")
			log.Error("daemon stopped: the server certificate changed; re-pair with `synosync pair`", "err", err)
			stop = store.LastStop{Reason: "the server certificate changed", Hint: "synosync pair"}
		} else if errors.Is(err, transport.ErrPoolClientRejected) || errors.Is(err, auth.ErrClientVersion) {
			// The server rejected this binary's client_version (20481): re-dialing the
			// same client is futile - the fix is a client upgrade, not re-pair/re-login.
			fmt.Fprintln(stderr, "synosync: the server rejected this client version; update synosync to a newer release")
			log.Error("daemon stopped: the server rejected this client version; update synosync to a newer release", "err", err)
			stop = store.LastStop{Reason: "the server rejected this client version", Hint: "update synosync to a newer release"}
		} else if errors.Is(err, auth.ErrSessionVanished) || errors.Is(err, transport.ErrPoolPaused) {
			fmt.Fprintln(stderr, "synosync: the session is no longer valid; re-authenticate with `synosync login`")
			log.Error("daemon stopped: the session is no longer valid; re-authenticate with `synosync login`", "err", err)
			stop = store.LastStop{Reason: "the session is no longer valid", Hint: "synosync login"}
		} else {
			log.Error("daemon stopped", "err", err)
		}
		if stop.Reason != "" {
			stop.At = time.Now().Unix()
			if perr := recordLastStop(stop); perr != nil {
				log.Warn("daemon: could not record the stop reason for the next CLI run", "err", perr)
			}
		}
		return 1
	}
	fmt.Fprintln(stdout, "synosync daemon stopped")
	log.Info("daemon stopped")
	return 0
}

// runDaemonAnnouncingStop runs the daemon and, for an interactive operator, acknowledges
// a stop request the moment it arrives instead of at the end of teardown.
//
// Ctrl-C cancels the context, and everything the user then sees - "synosync daemon
// stopped" - waits for the whole shutdown: the in-flight action turn finishing under its
// own budget, the pool closing its connections, the engine returning from its poll. On a
// busy sync that is seconds of a terminal that has acknowledged nothing, which reads as a
// wedged process and invites a second Ctrl-C. The second one is not harmless: the signal
// registration is released on the first (releaseSignalsWhenCancelled), so it kills the
// process by the default disposition, mid-teardown.
//
// Suppressed under a service manager: there is no one at the terminal, and the line would
// just be noise in the journal beside the daemon's own structured stop record.
func runDaemonAnnouncingStop(ctx context.Context, d *daemon.Daemon, out io.Writer, interactive bool) error {
	if !interactive {
		return runDaemon(ctx, d)
	}
	// Closed once the daemon has returned, so the notice cannot land after the final
	// "stopped" line. The wait also fences the goroutine's write to out against the
	// caller's, which is why this is not a fire-and-forget goroutine.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			fmt.Fprintln(out, "synosync: stopping - finishing in-flight work")
		case <-done:
		}
	}()
	err := runDaemon(ctx, d)
	close(done)
	wg.Wait()
	return err
}
