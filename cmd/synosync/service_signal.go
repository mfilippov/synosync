package main

import "context"

// releaseSignalsWhenCancelled restores the default signal disposition once ctx
// is cancelled, which happens on the first SIGINT/SIGTERM the enclosing
// signal.NotifyContext intercepts. Without it that registration stays active
// until the deferred stop() runs - i.e. until runDaemonLoop returns - so during
// a slow or hung teardown every further Ctrl-C / SIGTERM is swallowed and only
// SIGKILL can end the process. Handing the next signal back to the default
// disposition (terminate) gives the operator a working second Ctrl-C.
//
// stop is idempotent (signal.NotifyContext's stop func can be called more than
// once), so the deferred stop() at serveDaemon return remains harmless, and a
// non-signal exit (a terminal sync error) cancels ctx only via that deferred
// stop(), at which point this goroutine's stop() is a no-op.
func releaseSignalsWhenCancelled(ctx context.Context, stop func()) {
	go func() {
		<-ctx.Done()
		stop()
	}()
}
