package transport

// This file implements the DESIGN §4 connection pool: a dedicated event
// connection plus a small fixed set of worker connections, each an independent
// Session brought up through an injected Connector factory. The pool keeps the
// auth/secrets layer out of transport - the caller (the sync engine in phase 6b)
// composes Connect+Reuse into a Connector and hands it over as an opaque factory.
//
// Worker turns are serialised per connection by leasing: Acquire hands out an
// idle worker Session and a release closure that returns it to the pool. The
// event Session is owned exclusively by the event loop and never leased.
//
// A connection that drops mid-use is re-established through the Connector under
// the package backoff: Replace swaps a dead worker for a fresh one, ReplaceEvent
// does the same for the event connection. A terminal Connector error - an 8193
// session-vanished or a certificate pin mismatch (ErrPinMismatch) - moves the pool
// to a paused state and surfaces as ErrPoolPaused rather than retrying silently,
// signalling the caller to re-pair; a rejected client_version (20481,
// auth.ErrClientVersion) pauses the same way but surfaces as ErrPoolClientRejected,
// signalling the operator to upgrade the client. A mid-run outage is observable rather than
// silent: the reconnect loop logs one Warn when a re-dial
// streak begins and one Info when the connection is re-established (never one
// line per backoff attempt), and Reconnecting reports whether any re-dial loop
// is currently in flight so a status surface can render "reconnecting" instead
// of a false "connected" for the whole downtime.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/backoff"
)

// Worker-count bounds. DESIGN §4 calls for a handful of worker connections; the
// pool clamps NewPool's request into this range.
const (
	minWorkers = 2
	maxWorkers = 4
)

// ErrPoolClosed is returned by Acquire (and Start) once the pool has been closed.
var ErrPoolClosed = errors.New("transport: pool closed")

// ErrPoolPaused reports that the pool has entered the paused state after the
// Connector returned a terminal error - a vanished session token
// (auth.ErrSessionVanished, 8193) or a certificate pin mismatch (ErrPinMismatch,
// a same-server key change or a MITM): in either case reconnecting is futile until
// the caller re-pairs. The pause error wraps the underlying sentinel, so callers
// can match it (or ErrPoolPaused itself) with errors.Is.
var ErrPoolPaused = errors.New("transport: pool paused; re-pairing required")

// ErrPoolClientRejected reports that the pool paused after the Connector returned
// auth.ErrClientVersion (20481): the server rejected THIS binary's client_version,
// so re-dialing the same client forever is futile - unlike ErrPoolPaused, the
// operator action is to UPGRADE synosync, not to re-pair/re-login.
// It is a distinct sentinel (it deliberately does NOT wrap ErrPoolPaused)
// so the daemon's stop-path classification and stderr hint point at the right fix;
// the pause error wraps the underlying auth.ErrClientVersion so callers can match
// either with errors.Is.
var ErrPoolClientRejected = errors.New("transport: pool paused; client version rejected by server; update synosync")

// ErrPoolNotLeased reports that Replace was called with a worker Session the pool
// still manages but that is NOT currently leased - it was already returned by its
// release closure and is sitting idle in the free channel. Replacing such a session
// would close a connection another Acquire is about to hand out and, worse, push a
// second entry onto the numWorkers-sized free buffer (the leased slot the arithmetic
// assumes is vacant is not), so a later release under p.mu could block forever and
// deadlock every pool operation. Replace is defined only on a session the caller
// holds a live lease for; anything else is a caller bug, reported rather than acted
// on.
var ErrPoolNotLeased = errors.New("transport: worker session is not leased")

// ErrPoolNotStarted reports that Acquire was called before Start brought the
// connections up. Nothing will ever arrive on the free channel until Start runs, so
// blocking would hang the caller until its ctx expires (forever, for the engine's
// background ctx) and present as a silent stall with no diagnostic - a programming
// error deserves an immediate, named error instead.
var ErrPoolNotStarted = errors.New("transport: pool not started")

// terminalConnectErr reports whether a Connector error means reconnecting is futile:
// a vanished session token (8193) or a certificate pin mismatch (a same-server key
// change or a MITM) - both re-pair/re-login - or a rejected client_version (20481,
// auth.ErrClientVersion), which needs a client upgrade. Each moves the pool to the
// paused state rather than retrying forever. Non-deterministic reuse anomalies
// (4104/4105 on a reuse turn) and unknown codes stay transient by omission - only a
// deterministic, retry-cannot-fix rejection is terminal here.
func terminalConnectErr(err error) bool {
	return errors.Is(err, auth.ErrSessionVanished) || errors.Is(err, ErrPinMismatch) ||
		errors.Is(err, auth.ErrClientVersion)
}

// pauseSentinel picks the top-level pause sentinel to wrap a terminal Connector
// error with: a rejected client_version (20481) surfaces as ErrPoolClientRejected
// ("update synosync"), every other terminal error (vanished session, pin mismatch)
// as ErrPoolPaused ("re-pairing required"). The specific reason keeps the daemon's
// stop-path hint actionable.
func pauseSentinel(err error) error {
	if errors.Is(err, auth.ErrClientVersion) {
		return ErrPoolClientRejected
	}
	return ErrPoolPaused
}

// Connector establishes a fresh, fully-authenticated Session. The pool treats it
// as an opaque factory and never inspects how the connection is built; callers
// compose Connect with the auth handshake (mode A reuse) behind it.
type Connector func(ctx context.Context) (*Session, error)

// Pool owns one dedicated event Session plus a fixed set of worker Sessions. A
// worker is leased for the duration of a single request/response turn via
// Acquire and returned through the release closure; the event Session is exposed
// directly for the long-poll loop. The zero value is not usable - construct with
// NewPool and bring connections up with Start.
type Pool struct {
	connect    Connector
	numWorkers int

	// log receives the reconnect-streak diagnostics: one Warn
	// when a re-dial streak begins and one Info when the connection comes back,
	// never one line per backoff attempt. Never nil - NewPoolWithLogger defaults
	// it to slog.Default(); the daemon injects its rotating-file logger so a
	// mid-run outage is visible in the daemon log.
	log *slog.Logger

	// newBackoff builds the backoff used by a single reconnect loop. It is a field
	// so tests can inject a zero-delay sequence; production uses backoff.New.
	newBackoff func() *backoff.Backoff

	// free carries idle worker Sessions; Acquire receives from it and release
	// sends back. It is buffered to numWorkers so release never blocks. Closed by
	// Close to unblock any Acquire waiters.
	free chan *Session

	// closeCh is closed by Close; reconnect derives its dial context from it so a
	// shutdown interrupts an in-flight connect or backoff sleep, not just the gap
	// between attempts.
	closeCh chan struct{}

	// pausedCh is closed when the pool enters the paused state (the Connector
	// reported a vanished session). Acquire selects on it so blocked waiters wake
	// with ErrPoolPaused instead of hanging until ctx cancellation.
	pausedCh chan struct{}

	mu      sync.Mutex
	started bool
	closed  bool
	paused  bool
	// pauseErr is the sentinel the pool paused with, written once under mu in the
	// same critical section as paused (first-wins): ErrPoolPaused for a vanished
	// session or a pin mismatch (re-pair), ErrPoolClientRejected for a rejected
	// client_version (update synosync). Read via pausedReason so a paused-state
	// error surfaced by a concurrent Acquire/Replace/ReplaceEvent carries the same
	// recovery hint as the reconnect goroutine that actually detected the terminal
	// cause - without it a mid-run 20481 rejection races a concurrent Acquire that
	// returns the generic ErrPoolPaused, so the daemon prints "re-login" instead of
	// "update synosync".
	pauseErr  error
	eventSess *Session
	workers   []*Session

	// leased holds the worker Sessions currently handed out by Acquire (or by a
	// successful Replace) and not yet returned through their release closure. It is
	// the enforcement side of the invariant makeRelease's "the send cannot block"
	// comment relies on: a session in free is never in leased, so Replace - which
	// closes its argument and installs a replacement into the same slot - is
	// rejected with ErrPoolNotLeased unless the caller actually holds the lease.
	// Guarded by mu; entries are added under the same critical section that clears
	// the session for hand-out and removed by release/Replace.
	leased map[*Session]struct{}

	// reconnecting counts the re-dial loops currently in flight (a dropped worker
	// or the event connection being re-established under backoff). Guarded by mu;
	// read via Reconnecting so a status surface can tell "connected" from "parked
	// in a reconnect loop mid-outage".
	reconnecting int
}

// NewPool returns a pool that will lease numWorkers worker connections, clamped
// to [minWorkers, maxWorkers]. No connections are opened until Start. Reconnect
// diagnostics log through slog.Default(); callers with their own logger use
// NewPoolWithLogger.
func NewPool(connect Connector, numWorkers int) *Pool {
	return NewPoolWithLogger(connect, numWorkers, nil)
}

// NewPoolWithLogger is NewPool with the reconnect-streak logger injected
// (mirroring actions.NewClientWithLogger): the daemon passes
// its rotating-file logger so a mid-run outage's re-dial Warn lands in the
// daemon log at the configured level/format instead of slog.Default(). A nil
// log falls back to slog.Default().
func NewPoolWithLogger(connect Connector, numWorkers int, log *slog.Logger) *Pool {
	if numWorkers < minWorkers {
		numWorkers = minWorkers
	}
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}
	if log == nil {
		log = slog.Default()
	}
	return &Pool{
		connect:    connect,
		numWorkers: numWorkers,
		log:        log,
		newBackoff: backoff.New,
		free:       make(chan *Session, numWorkers),
		closeCh:    make(chan struct{}),
		pausedCh:   make(chan struct{}),
		leased:     make(map[*Session]struct{}, numWorkers),
	}
}

// pause moves the pool into the paused state and wakes any Acquire waiters by
// closing pausedCh, recording sentinel (ErrPoolPaused or ErrPoolClientRejected)
// as the reason so every paused-state surfacing reports the same recovery hint.
// The caller must hold p.mu. It is idempotent: a second concurrent reconnect that
// also reports a terminal error is a no-op rather than a double-close of pausedCh,
// and the first reason wins.
func (p *Pool) pause(sentinel error) {
	if !p.paused {
		p.paused = true
		p.pauseErr = sentinel
		close(p.pausedCh)
	}
}

// pausedReason returns the sentinel the pool paused with. The caller must hold
// p.mu and have observed p.paused; the ErrPoolPaused fallback guards a
// theoretical unset (an observed paused always has pauseErr set in the same
// critical section).
func (p *Pool) pausedReason() error {
	if p.pauseErr != nil {
		return p.pauseErr
	}
	return ErrPoolPaused
}

// stopReason reports why a reconnect loop should bail, or nil to keep trying.
// Pool close and pause take precedence over the caller's own ctx cancellation so
// a shutdown surfaces ErrPoolClosed/ErrPoolPaused rather than a bare ctx error.
func (p *Pool) stopReason(ctx context.Context) error {
	p.mu.Lock()
	closed, paused := p.closed, p.paused
	var pausedErr error
	if paused {
		pausedErr = p.pausedReason()
	}
	p.mu.Unlock()
	switch {
	case closed:
		return ErrPoolClosed
	case paused:
		return pausedErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// reconnectCtx derives a dial context from ctx that is also cancelled when the
// pool closes or pauses, so a connect or backoff sleep in flight is interrupted
// promptly - including when a concurrent reconnect reports a vanished session
// (8193) and pauses the pool while this one is still dialing or sleeping. The
// returned cancel must be called to stop the watcher goroutine.
//
// Like every context.CancelFunc the returned cancel is idempotent: a second call
// is a no-op rather than a panic on the closed stop channel. A CancelFunc that
// could only be called once would be a trap for the ordinary defer-plus-explicit
// pattern, and the panic would take the whole daemon down from inside a reconnect
// loop.
func (p *Pool) reconnectCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	rctx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	go func() {
		select {
		case <-p.closeCh:
			cancel()
		case <-p.pausedCh:
			cancel()
		case <-stop:
		}
	}()
	var once sync.Once
	return rctx, func() {
		once.Do(func() { close(stop) })
		cancel()
	}
}

// NumWorkers reports the (clamped) number of worker connections the pool manages.
func (p *Pool) NumWorkers() int { return p.numWorkers }

// Paused reports whether the pool has entered the paused state (the Connector
// reported a vanished session). Once paused, Replace/ReplaceEvent fail fast with
// ErrPoolPaused; the caller must tear the pool down and re-pair.
func (p *Pool) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// PauseReason reports the sentinel the pool paused with - ErrPoolPaused (a
// vanished session, re-pair) or ErrPoolClientRejected (a deterministic client
// rejection, "update synosync") - or nil while the pool is not paused. It is the
// exported form of pausedReason so a surfacing that observes the pool paused (e.g.
// the engine's EventClient seam) carries the same recovery hint 33e2d30 records
// internally, rather than hardcoding ErrPoolPaused and mislabelling a
// client-rejected pause as a re-login signal.
func (p *Pool) PauseReason() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return nil
	}
	return p.pausedReason()
}

// Reconnecting reports whether at least one re-dial loop (Replace/ReplaceEvent
// after a dropped connection) is currently in flight: the pool is up but a
// connection is parked in the reconnect backoff loop, so a status surface should
// render "reconnecting" rather than an unqualified "connected".
// The flag covers the whole loop from entry - including a first dial
// still blocked on the connect-attempt timeout before any attempt has failed -
// and clears when the loop returns (success, terminal error, or cancellation).
func (p *Pool) Reconnecting() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reconnecting > 0
}

// Start brings up the event connection and all worker connections through the
// Connector. It is idempotent only in the error sense: a second successful Start
// is an error. On any connection failure every already-opened Session is closed
// and the error is returned; a TRANSIENT failure also releases the started flag, so
// the pool is back to its pre-Start state (Acquire reports ErrPoolNotStarted rather
// than blocking forever, and Start may be retried), while a TERMINAL failure pauses
// the pool for good (startErr).
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	if p.started {
		p.mu.Unlock()
		return errors.New("transport: pool already started")
	}
	p.started = true
	p.mu.Unlock()

	// Derive a dial context that Close (via closeCh) cancels, so a blocking
	// Connector does not keep Start running after the pool has been closed; the
	// post-dial closed-recheck below still drops any session that lands anyway.
	ctx, cancel := p.reconnectCtx(ctx)
	defer cancel()

	ev, err := p.connect(ctx)
	if err != nil {
		return p.startErr("event connection", err)
	}

	workers := make([]*Session, 0, p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		w, err := p.connect(ctx)
		if err != nil {
			_ = ev.Close()
			for _, opened := range workers {
				_ = opened.Close()
			}
			return p.startErr(fmt.Sprintf("worker connection %d", i), err)
		}
		workers = append(workers, w)
	}

	p.mu.Lock()
	if p.closed {
		// Close ran while we were dialing: drop the freshly opened sessions rather
		// than installing them or sending onto the now-closed free channel.
		p.mu.Unlock()
		_ = ev.Close()
		for _, w := range workers {
			_ = w.Close()
		}
		return ErrPoolClosed
	}
	p.eventSess = ev
	p.workers = workers
	// Send onto free while holding the lock so it is serialised with Close's
	// close(p.free): the buffer is sized to numWorkers, so these sends never block.
	for _, w := range workers {
		p.free <- w
	}
	p.mu.Unlock()
	return nil
}

// startErr wraps a Start-time connection failure. A terminal Connector error
// (a vanished session, 8193, a certificate pin mismatch, or a rejected
// client_version, 20481) during bring-up is the same pause signal as a mid-run
// reconnect: the pool moves to the paused state and reports ErrPoolPaused (re-pair)
// or ErrPoolClientRejected (client upgrade) so the caller does not retry Start
// against an invalid token, a changed key, or a rejected binary version.
func (p *Pool) startErr(what string, err error) error {
	p.mu.Lock()
	// Close ran while we were dialing and cancelled the dial context: honour the
	// closed-over-everything precedence and report ErrPoolClosed rather than the
	// wrapped context.Canceled (or even a racing 8193) the cancelled Connector
	// produced - matching the post-dial closed-recheck in Start.
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	if terminalConnectErr(err) {
		p.pause(pauseSentinel(err))
		p.mu.Unlock()
		return fmt.Errorf("transport: pool %s: %w: %w", what, pauseSentinel(err), err)
	}
	// A TRANSIENT bring-up failure releases the started flag. Start
	// sets it before the first dial, and leaving it set on a failed bring-up left the pool
	// in a state no verb can escape: the pool holds no sessions and never will, yet Acquire
	// sees started == true and blocks forever on a free channel nothing can fill, while a
	// second Start is refused with "pool already started". That is precisely the shape
	// ErrPoolNotStarted was introduced to eliminate. Every session this
	// attempt opened is closed by the caller before it reaches here, so nothing is leaked
	// and the pool is back to its pre-Start state: Acquire reports ErrPoolNotStarted and
	// Start may be retried. A TERMINAL failure deliberately keeps the flag - the pool is
	// paused above, and paused takes precedence in Acquire and never reopens.
	p.started = false
	p.mu.Unlock()
	return fmt.Errorf("transport: pool %s: %w", what, err)
}

// Event returns the dedicated event Session for the long-poll loop. It is owned
// exclusively by the caller's event loop and must not be shared with worker
// turns. Returns nil before Start, after Close, or once the pool is paused (a
// vanished session): in the paused state the event loop must stop too.
func (p *Pool) Event() *Session {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.paused {
		return nil
	}
	return p.eventSess
}

// Acquire leases an idle worker Session, blocking until one is free or ctx is
// done. It returns the Session and a release closure that must be called exactly
// once to return the connection to the pool; calling release more than once is a
// no-op. After Close, Acquire returns ErrPoolClosed; once the pool is paused (a
// vanished session) it returns ErrPoolPaused, including for waiters already
// blocked when the pause happened. Before Start it returns ErrPoolNotStarted
// rather than blocking on a free channel nothing can ever fill.
func (p *Pool) Acquire(ctx context.Context) (*Session, func(), error) {
	// Checked before the select so a not-yet-started pool reports the caller's bug
	// immediately instead of hanging until ctx expires (never, for a background ctx).
	// Closed/paused keep their own sentinels: those are states callers race
	// legitimately, and Close on an unstarted pool must still read as ErrPoolClosed.
	p.mu.Lock()
	notStarted := !p.started && !p.closed && !p.paused
	p.mu.Unlock()
	if notStarted {
		return nil, nil, ErrPoolNotStarted
	}
	select {
	case <-ctx.Done():
		// Pool close/pause take precedence over the caller's ctx: when the pool is
		// already closed (or paused) and ctx is also done, this select fires
		// nondeterministically, so recheck terminal state to honour the documented
		// ErrPoolClosed/ErrPoolPaused contract instead of leaking ctx.Err().
		p.mu.Lock()
		closed, paused := p.closed, p.paused
		var pausedErr error
		if paused {
			pausedErr = p.pausedReason()
		}
		p.mu.Unlock()
		switch {
		case closed:
			return nil, nil, ErrPoolClosed
		case paused:
			return nil, nil, pausedErr
		}
		return nil, nil, ctx.Err()
	case <-p.pausedCh:
		// Close takes precedence over pause: once a paused pool is also closed,
		// both pausedCh and free are ready and this select fires nondeterministically,
		// so recheck closed here to honour the documented post-close ErrPoolClosed
		// contract instead of leaking ErrPoolPaused.
		p.mu.Lock()
		closed := p.closed
		pausedErr := p.pausedReason()
		p.mu.Unlock()
		if closed {
			return nil, nil, ErrPoolClosed
		}
		return nil, nil, pausedErr
	case s, ok := <-p.free:
		if !ok {
			return nil, nil, ErrPoolClosed
		}
		// A closed channel still yields its buffered idle workers with ok==true;
		// reject those so Acquire never hands out a connection after Close. The
		// session stays a member of p.workers, so Close shuts it down - closing it
		// here too would double-close the connection. The same guard rejects a lease
		// once the pool is paused so no new turn starts against a doomed session.
		p.mu.Lock()
		closed, paused := p.closed, p.paused
		var pausedErr error
		if paused {
			pausedErr = p.pausedReason()
		}
		if !closed && !paused {
			// Record the lease in the same critical section that clears s for
			// hand-out, so a concurrent Replace either sees s as leased (this
			// Acquire won) or not yet handed out at all - never a torn in-between.
			p.leased[s] = struct{}{}
		}
		p.mu.Unlock()
		switch {
		case closed:
			return nil, nil, ErrPoolClosed
		case paused:
			return nil, nil, pausedErr
		}
		return s, p.makeRelease(s), nil
	}
}

// makeRelease builds the once-guarded release closure that returns a worker
// Session to the free pool. It returns the connection only while s is still a
// live member of the worker set: a session that Replace has already swapped out
// is no longer a member, so a stale release (called after Replace) is a no-op
// rather than re-injecting a dead connection. During shutdown the session is
// dropped instead of pushed onto the closed channel.
func (p *Pool) makeRelease(s *Session) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			// The lease ends here regardless of which branch below takes it: from
			// this point on Replace(s) is a caller bug (ErrPoolNotLeased) rather
			// than a swap of a live turn's connection.
			delete(p.leased, s)
			member := false
			for _, w := range p.workers {
				if w == s {
					member = true
					break
				}
			}
			switch {
			case !member:
				// s was replaced out of the pool (already closed) - nothing to do.
				return
			case p.closed:
				// The pool is shutting down: leave s for Close to shut down. It is
				// still a member of p.workers, so Close closes it exactly once -
				// closing here as well would double-close the connection.
				return
			case p.paused:
				// The pool is paused (a vanished session): drop the returned worker
				// rather than re-injecting it into free, so no later Acquire can lease
				// it. It stays a member of p.workers for Close to shut down.
				return
			default:
				// Send while holding the lock so the closed-check and the send are
				// atomic with Close's close(p.free); the buffer is sized to
				// numWorkers and s was leased until the delete above (so it was not
				// also buffered - Replace enforces that with p.leased), which is
				// what makes this send unable to block.
				p.free <- s
			}
		})
	}
}

// reconnect dials a fresh Session through the Connector, retrying transient
// failures under a fresh backoff sequence until one of: success, ctx
// cancellation, pool close, or a terminal Connector error. A terminal error - a
// vanished session (auth.ErrSessionVanished) or a certificate pin mismatch
// (ErrPinMismatch), both wrapped in ErrPoolPaused, or a rejected client_version
// (auth.ErrClientVersion), wrapped in ErrPoolClientRejected - pauses the pool and
// is returned without further retries.
func (p *Pool) reconnect(ctx context.Context) (*Session, error) {
	p.mu.Lock()
	p.reconnecting++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.reconnecting--
		p.mu.Unlock()
	}()

	rctx, cancel := p.reconnectCtx(ctx)
	defer cancel()

	bo := p.newBackoff()
	failures := 0
	for {
		if reason := p.stopReason(ctx); reason != nil {
			return nil, reason
		}

		sess, err := p.connect(rctx)
		if err == nil {
			// Recheck before handing the session out: a concurrent reconnect may
			// have paused the pool (or Close may have run) while we were dialing, in
			// which case this fresh session must not be installed.
			if reason := p.stopReason(ctx); reason != nil {
				_ = sess.Close()
				return nil, reason
			}
			if failures > 0 {
				// The closing bookend of the streak the Warn below opened.
				p.log.Info("transport: connection re-established", "failed_attempts", failures)
			}
			return sess, nil
		}
		if terminalConnectErr(err) {
			p.mu.Lock()
			// Close takes precedence over pause: if Close ran while we were dialing,
			// report ErrPoolClosed rather than pausing a pool that is shutting down.
			if p.closed {
				p.mu.Unlock()
				return nil, ErrPoolClosed
			}
			p.pause(pauseSentinel(err))
			p.mu.Unlock()
			return nil, fmt.Errorf("%w: %w", pauseSentinel(err), err)
		}
		// connect failed transiently. A cancelled dial context means the caller's
		// ctx ended or the pool closed - bail with the right reason rather than
		// retrying. Otherwise sleep the backoff interval, again bailing on the
		// derived context so Close interrupts the wait.
		if reason := p.stopReason(ctx); reason != nil {
			return nil, reason
		}
		if failures == 0 {
			// One Warn per streak, not per attempt: an outage can retry for hours,
			// and the re-established Info above closes the streak. Logged after the
			// stopReason bail so a shutdown-cancelled dial stays silent.
			p.log.Warn("transport: connection lost; re-dialing under backoff", "err", err)
		}
		failures++
		if waitErr := bo.Wait(rctx); waitErr != nil {
			if reason := p.stopReason(ctx); reason != nil {
				return nil, reason
			}
			return nil, waitErr
		}
	}
}

// Replace re-establishes a worker connection whose turn failed because the
// underlying socket dropped. The caller passes the dead Session previously handed
// out by Acquire and must NOT also call that lease's release closure - Replace
// closes the dead connection, dials a replacement through the Connector under
// backoff, swaps it into the worker set and returns it leased with a fresh
// release. On ctx cancellation or a vanished session it returns an error (the
// latter wrapping ErrPoolPaused) and the worker slot stays vacant. Passing a
// Session the pool does not currently manage - including nil - is an error, and
// so is passing a managed worker whose lease has already ended (released, or
// replaced by an earlier call): that one reports ErrPoolNotLeased, because
// closing an idle session sitting in the free channel would both hand a dead
// connection to the next Acquire and overfill the free buffer, deadlocking a
// later release that sends under p.mu.
//
// ctx must be the caller's long-lived run context (the engine loop's), not a
// short per-call one: the pool re-dials only through Replace/ReplaceEvent, so a
// ctx that is cancelled mid-re-dial leaves the worker slot vacant forever and
// permanently shrinks pool capacity until the pool is torn down.
func (p *Pool) Replace(ctx context.Context, dead *Session) (*Session, func(), error) {
	if dead == nil {
		// A vacant slot (a worker mid-Replace) is a nil entry in p.workers, so a nil
		// dead session would match it below and double-install a replacement into
		// that slot, leaking whichever session loses the race. Reject it up front -
		// nil is never a session this pool handed out.
		return nil, nil, errors.New("transport: Replace: nil worker session")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, ErrPoolClosed
	}
	if p.paused {
		reason := p.pausedReason()
		p.mu.Unlock()
		return nil, nil, reason
	}
	idx := -1
	for i, w := range p.workers {
		if w == dead {
			idx = i
			break
		}
	}
	if idx < 0 {
		p.mu.Unlock()
		return nil, nil, errors.New("transport: Replace: unknown worker session")
	}
	if _, leased := p.leased[dead]; !leased {
		// A pool member that is NOT leased is sitting in the free channel: closing
		// it and installing a replacement would hand a dead connection to the next
		// Acquire and overfill the free buffer, so a later release under p.mu would
		// block and wedge the whole pool. Reject instead.
		p.mu.Unlock()
		return nil, nil, fmt.Errorf("transport: Replace: %w", ErrPoolNotLeased)
	}
	// Drop the dead session from the worker set up front so any stale release for
	// it becomes a no-op while we dial the replacement, and end its lease: the
	// caller must not release a session it asked to have replaced.
	p.workers[idx] = nil
	delete(p.leased, dead)
	p.mu.Unlock()

	_ = dead.Close()

	fresh, err := p.reconnect(ctx)
	if err != nil {
		return nil, nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = fresh.Close()
		return nil, nil, ErrPoolClosed
	}
	if p.paused {
		// A concurrent reconnect paused the pool after this dial succeeded: drop
		// the fresh session instead of leasing it back out.
		reason := p.pausedReason()
		p.mu.Unlock()
		_ = fresh.Close()
		return nil, nil, reason
	}
	p.workers[idx] = fresh
	// The replacement is handed back leased, so it is Replace-able in turn (a
	// second drop on the same chain) until its release runs.
	p.leased[fresh] = struct{}{}
	p.mu.Unlock()
	return fresh, p.makeRelease(fresh), nil
}

// ReplaceEvent re-establishes the dedicated event connection after it drops. It
// closes the old event Session, dials a replacement through the Connector under
// backoff and installs it; the returned Session is also available via Event. On
// ctx cancellation or a vanished session it returns an error (the latter wrapping
// ErrPoolPaused) and the event connection is left nil.
//
// Like Replace, ctx must be the caller's long-lived run context: a short ctx
// cancelled mid-re-dial leaves the event connection nil forever (Event returns
// nil and the long-poll loop is dead) - the pool never re-dials on its own.
func (p *Pool) ReplaceEvent(ctx context.Context) (*Session, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if p.paused {
		reason := p.pausedReason()
		p.mu.Unlock()
		return nil, reason
	}
	old := p.eventSess
	p.eventSess = nil
	p.mu.Unlock()

	if old != nil {
		_ = old.Close()
	}

	fresh, err := p.reconnect(ctx)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = fresh.Close()
		return nil, ErrPoolClosed
	}
	if p.paused {
		// A concurrent reconnect paused the pool after this dial succeeded: drop
		// the fresh event session instead of installing it.
		reason := p.pausedReason()
		p.mu.Unlock()
		_ = fresh.Close()
		return nil, reason
	}
	p.eventSess = fresh
	p.mu.Unlock()
	return fresh, nil
}

// Close shuts the pool down: it closes the event connection and every worker
// connection (including any currently leased) and unblocks Acquire waiters.
// Subsequent Acquire calls return ErrPoolClosed. Close is idempotent.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	ev := p.eventSess
	workers := p.workers
	// Cancel any in-flight reconnect (a blocking connect or backoff sleep) by
	// closing closeCh, which reconnectCtx watches.
	close(p.closeCh)
	// Close free while holding the lock so it is serialised with the send sides
	// (Start and makeRelease), which both send under p.mu - this is what makes a
	// release racing Close safe instead of a send-on-closed-channel panic.
	close(p.free)
	p.mu.Unlock()

	var errs []error
	if ev != nil {
		if err := ev.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, w := range workers {
		if w == nil {
			// A worker mid-Replace: its dead session is already closed and the
			// replacement is not yet installed.
			continue
		}
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
