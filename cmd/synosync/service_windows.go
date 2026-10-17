//go:build windows

package main

// serveDaemon runs the daemon on Windows. When the process is launched by the
// Service Control Manager it speaks the SCM control-dispatcher protocol
// (reporting Running/Stopped and cancelling the run context on a Stop/Shutdown
// control); otherwise it behaves like the foreground subcommand, stopping on
// Ctrl-C / SIGTERM.
//
// The dispatcher is mandatory under SCM: a plain foreground process that never
// calls svc.Run / StartServiceCtrlDispatcher never reports SERVICE_RUNNING, so
// SCM kills it after the start timeout with error 1053 ("the service did not
// respond to the start request in a timely fashion"). svc.IsWindowsService
// distinguishes the two launch modes so the same binary works both ways.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"

	"github.com/mfilippov/synosync/internal/daemon"
)

func serveDaemon(d *daemon.Daemon, log *slog.Logger, stdout, stderr io.Writer, addr string) int {
	isService, err := svc.IsWindowsService()
	if err == nil && isService {
		return runDaemonAsService(d, log, stdout, stderr, addr)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Release the signal registration as soon as the first signal cancels ctx so
	// a second Ctrl-C / SIGTERM during a slow teardown terminates the process by
	// the default disposition instead of being swallowed until runDaemonLoop
	// returns.
	releaseSignalsWhenCancelled(ctx, stop)
	return runDaemonLoop(ctx, d, log, stdout, stderr, addr, true)
}

// effectiveLogStderr reports whether the daemon should log to stderr only. Under
// the Service Control Manager there is no console attached to the process, so its
// stderr is discarded and stderr-only logging would silently lose every log line
// (and the rotating file would never be written). In that mode it forces the
// rotating file sink regardless of the --log-stderr flag; a foreground console
// launch honours the flag as before.
func effectiveLogStderr(logStderr bool) bool {
	if isService, err := svc.IsWindowsService(); err == nil && isService {
		return false
	}
	return logStderr
}

// serviceStartFallback makes a pre-logger daemon start failure observable under
// the Service Control Manager, where stderr is discarded and the rotating log
// file does not exist yet: it writes one best-effort record to the Windows
// Application event log. The "synosync" source needs no registration - an
// unregistered source still records the message, only without a friendly
// category description. A foreground console launch already sees the stderr
// line, so this is a no-op there; event-log errors are themselves best-effort
// (there is no remaining sink to report them to).
func serviceStartFallback(msg string) {
	if isService, err := svc.IsWindowsService(); err != nil || !isService {
		return
	}
	el, err := eventlog.Open("synosync")
	if err != nil {
		return
	}
	defer func() { _ = el.Close() }()
	_ = el.Error(1, "synosync daemon start failed: "+msg)
}

// daemonService adapts the daemon run loop to the svc.Handler interface so SCM
// can start and stop it.
type daemonService struct {
	d      *daemon.Daemon
	log    *slog.Logger
	stdout io.Writer
	stderr io.Writer
	addr   string
	exit   int

	// run replaces the daemon run loop in tests; nil uses runDaemonLoop. It is
	// the seam the SCM state-machine tests drive (a run loop that ignores its
	// cancellation for a while stands in for a slow engine teardown).
	run func(ctx context.Context) int
}

// runLoop runs the daemon under ctx, honouring the test seam.
func (s *daemonService) runLoop(ctx context.Context) int {
	if s.run != nil {
		return s.run(ctx)
	}
	// interactive=false: under SCM there is no console, so suppress the
	// "(Ctrl-C to stop)" hint in the start banner.
	return runDaemonLoop(ctx, s.d, s.log, s.stdout, s.stderr, s.addr, false)
}

// Pending-state wait hints, in milliseconds (the svc.Status.WaitHint unit). A
// pending status with WaitHint 0 gives SCM no estimate, so it falls back to its
// default transition timeouts. Running is reported as soon as the run loop
// goroutine is launched - the single-instance lock, store open and IPC bind all
// happen asynchronously inside daemon.Run, and a failure there surfaces as an
// early Execute return with the exit code (and so an early Stopped) - so the
// start hint only needs to cover the goroutine hand-off.
//
// The stop hint is NOT a bound on the drain. The IPC control plane is genuinely
// bounded (ipc.dispatch stops waiting for a handler once the served context is
// cancelled), but engine teardown is not: a local scan is
// checked for cancellation only between views, so a full-rehash of a large view
// over a slow mount keeps running long after the run context is cancelled. SCM
// treats a pending state as unresponsive once WaitHint elapses without progress,
// and outside an OS shutdown it does not kill the process - the service would sit
// in STOP_PENDING (rejecting both Stop and Start) until the scan finished. So the
// drain reports progress instead of promising a deadline: awaitStop re-sends
// StopPending with an incremented CheckPoint every stopCheckpointInterval, which
// restarts SCM's patience clock each time.
const (
	startPendingWaitHintMs = 15_000 // 15s
	stopPendingWaitHintMs  = 30_000 // 30s
)

// stopCheckpointInterval is how often the drain re-reports StopPending with an
// incremented CheckPoint. It must stay well under stopPendingWaitHintMs so SCM
// always sees progress before the hint expires; a var so tests can shorten it.
var stopCheckpointInterval = 5 * time.Second

// Execute is the SCM service main. It starts the daemon run loop on its own
// goroutine, reports Running, then translates a Stop/Shutdown control into a
// context cancellation and waits for the loop to drain.
//
// It deliberately never reports Stopped itself: the last
// status it writes to changes is StopPending, and the authoritative
// SERVICE_STOPPED is the one the svc package reports from Execute's RETURN
// values. A `changes <- svc.Status{State: svc.Stopped}` is handled by
// svc.serviceMain's status arm, which calls SetServiceStatus with the exit code
// it holds at that moment - still the default (errno 0), because the real code
// only reaches it later over its own channel. So a self-sent Stopped
// deterministically publishes SERVICE_STOPPED/NO_ERROR first, and the exit code
// arrives in a second SetServiceStatus after STOPPED, which the SCM contract
// leaves undefined. SCM would record the terminal failure as a clean stop and
// the packaging's failure actions (`sc.exe failure … restart` + `failureflag 1`,
// internal/service/templates/install-service.ps1) would never fire.
func (s *daemonService) Execute(_ []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const accepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending, WaitHint: startPendingWaitHintMs}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int, 1)
	go func() { done <- s.runLoop(ctx) }()

	changes <- svc.Status{State: svc.Running, Accepts: accepted}
	for {
		select {
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				s.exit = awaitStop(r, changes, done, cancel)
				return svcExitCode(s.exit)
			}
		case code := <-done:
			// The daemon exited on its own (a terminal error such as a vanished
			// session, or a clean stop). Returning the code is what reports
			// Stopped - see the doc comment: sending it here would publish
			// SERVICE_STOPPED with NO_ERROR before the code could be attached.
			s.exit = code
			return svcExitCode(code)
		}
	}
}

// awaitStop reports StopPending, cancels the run context and waits for the run
// loop to drain, re-reporting StopPending with an incremented CheckPoint every
// stopCheckpointInterval until it does. SCM restarts its transition clock on
// every checkpoint increment, so an engine teardown that outlives the wait hint
// (an in-flight full-rehash scan is only cancellable between views) still reads
// as a service making progress rather than one that stopped responding. It
// returns the run loop's exit code; StopPending is the last status anybody here
// reports - the caller turns that code into the single authoritative Stopped by
// RETURNING it (see Execute).
//
// Interrogate keeps being answered while the drain runs - with the current
// pending status, not the stale Running one SCM carries in the request - and a
// repeated Stop/Shutdown is a no-op: the drain is already under way and there is
// nothing left to cancel.
func awaitStop(r <-chan svc.ChangeRequest, changes chan<- svc.Status, done <-chan int, cancel context.CancelFunc) int {
	status := svc.Status{State: svc.StopPending, CheckPoint: 1, WaitHint: stopPendingWaitHintMs}
	changes <- status
	cancel()

	ticker := time.NewTicker(stopCheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case code := <-done:
			return code
		case <-ticker.C:
			status.CheckPoint++
			changes <- status
		case c := <-r:
			if c.Cmd == svc.Interrogate {
				changes <- status
			}
		}
	}
}

// svcExitCode maps the daemon's process exit code onto the svc.Handler return
// contract. A non-zero code must be returned as a service-specific exit code
// (ssec=true), which SCM reports verbatim; returned with ssec=false it would be
// read as a Win32 system error number, so the daemon's "1" would surface as
// ERROR_INVALID_FUNCTION ("Incorrect function") in the service snap-in. Zero
// stays (false, 0): a clean stop. These return values are the ONLY way the code
// reaches SCM - Execute never reports Stopped itself, so the svc package's final
// SetServiceStatus(SERVICE_STOPPED) is the one carrying them.
// A code outside the 0-255 process-exit range cannot be reported faithfully (SCM takes a
// uint32 and the conversion of a negative or oversized int would wrap), so it is normalised
// to 1 - "failed", the only honest answer left - rather than surfacing a wrapped number.
func svcExitCode(code int) (bool, uint32) {
	switch {
	case code == 0:
		return false, 0
	case code < 0 || code > 255:
		return true, 1
	default:
		return true, uint32(code)
	}
}

func runDaemonAsService(d *daemon.Daemon, log *slog.Logger, stdout, stderr io.Writer, addr string) int {
	s := &daemonService{d: d, log: log, stdout: stdout, stderr: stderr, addr: addr}
	if err := svc.Run("synosync", s); err != nil {
		fmt.Fprintf(stderr, "synosync: windows service dispatcher: %s\n", userErr(err))
		return 1
	}
	return s.exit
}
