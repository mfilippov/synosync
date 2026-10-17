// Package netctx makes a context cancellation or deadline interrupt an
// otherwise-blocking request/response turn on a net.Conn, and maps the resulting
// socket error back onto the matching context error. It is the single owner of
// the deadline-propagation / cancellation-race / past-deadline logic that the
// auth, actions and transport layers each drive their turns with - previously
// duplicated byte-for-byte across the three packages.
package netctx

import (
	"context"
	"errors"
	"net"
	"os"
	"time"
)

// Watch makes ctx cancellation interrupt otherwise-blocking I/O on conn. It first
// propagates any context deadline onto the socket, then - if ctx can be cancelled
// - watches ctx.Done() and forces an immediate (past) socket deadline when it
// fires, unblocking a stuck read or write. The returned stop function must be
// called once the turn completes: it joins the watcher goroutine and then clears
// the deadline, so a reused connection is not left expiring at this turn's
// deadline and a racing cancellation cannot stamp a past deadline onto the next
// operation.
//
// Clearing the deadline only makes a cleanly-completed turn reusable; it does NOT
// make an interrupted one safe to reuse. When cancellation expires an in-flight
// read/write the socket is left byte-misaligned, so a caller that reuses the
// connection across turns must additionally mark it broken and refuse subsequent
// turns rather than mistake the abandoned reply's leftover bytes for the next
// turn's (see actions.Client.broken).
func Watch(ctx context.Context, conn net.Conn) (stop func()) {
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			// A non-zero time in the past expires any in-flight I/O at once.
			_ = conn.SetDeadline(time.Unix(1, 0))
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-exited
		// The watcher has now returned, so no past deadline can land after this.
		// Clear the deadline so the connection is reusable without inheriting it.
		_ = conn.SetDeadline(time.Time{})
	}
}

// Err prefers ctx's cancellation or deadline error over the socket error produced
// by the forced past deadline, so callers can match context.Canceled or
// context.DeadlineExceeded with errors.Is. The socket deadline is derived from
// ctx's deadline, so the kernel can time out an in-flight read microseconds
// before the runtime's context timer marks ctx done; an I/O timeout at or past
// the context deadline is therefore mapped to context.DeadlineExceeded even when
// ctx.Err() has not yet been set. When ctx is still live it returns err unchanged.
//
// The substitution is confined to the errors Watch's mechanism can itself produce
// - an expired deadline (os.ErrDeadlineExceeded, however wrapped). Any OTHER
// failure happened for its own reasons and is returned untouched even when ctx is
// already done: a cancellation racing a terminal handshake error must not erase it.
// Masking a transport.ErrPinMismatch or auth.ErrSessionVanished
// as context.Canceled would make the pool's terminalConnectErr classify a changed
// certificate or a vanished session as an ordinary transient failure and keep
// re-dialing under backoff instead of pausing and demanding a re-pair.
func Err(ctx context.Context, err error) error {
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if ce := ctx.Err(); ce != nil {
		return ce
	}
	if dl, ok := ctx.Deadline(); ok && errors.Is(err, os.ErrDeadlineExceeded) && !time.Now().Before(dl) {
		return context.DeadlineExceeded
	}
	return err
}
