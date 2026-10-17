//go:build !windows

package main

// serveDaemon runs the daemon in the foreground on non-Windows platforms,
// stopping on SIGINT/SIGTERM. A supervisor (systemd/launchd) sends SIGTERM for
// a clean stop; an interactive user sends SIGINT with Ctrl-C.

import (
	"context"
	"io"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/mfilippov/synosync/internal/daemon"
)

func serveDaemon(d *daemon.Daemon, log *slog.Logger, stdout, stderr io.Writer, addr string) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Release the signal registration as soon as the first signal cancels ctx so
	// a second SIGINT/SIGTERM during a slow teardown terminates the process by the
	// default disposition instead of being swallowed until runDaemonLoop returns.
	releaseSignalsWhenCancelled(ctx, stop)
	return runDaemonLoop(ctx, d, log, stdout, stderr, addr, true)
}

// effectiveLogStderr reports whether the daemon should log to stderr only. On
// non-Windows platforms the --log-stderr flag is honoured verbatim: a supervisor
// (systemd/launchd) captures the foreground process's stderr into its own
// journal, so stderr-only logging is the right sink there.
func effectiveLogStderr(logStderr bool) bool { return logStderr }

// serviceStartFallback is a no-op on non-Windows platforms: a pre-logger start
// failure printed to stderr is already observable in the foreground and under
// systemd/launchd (which capture stderr into their own journal).
func serviceStartFallback(string) {}
