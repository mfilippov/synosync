package engine

// This file defines the engine's seam over the connection pool. The engine never
// touches transport.Pool or actions.Client directly: it drives a narrow syncClient
// interface (the handful of action-client operations the remote→local loop needs)
// behind a Transport that yields the dedicated event connection's client and leases
// worker connections. The production adapter, poolTransport, wraps a transport.Pool
// and builds a fresh actions.Client per connection; tests supply a fake Transport.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/transport"
)

// syncClient is the narrow set of action-client operations the engine drives over
// a single authenticated connection. *actions.Client satisfies it; tests provide a
// fake. Keeping it narrow keeps the engine mockable without a real network turn.
type syncClient interface {
	QueryUserInfo(ctx context.Context) ([]actions.View, error)
	QueryNode(ctx context.Context, viewID int64, path string) (actions.Node, error)
	LongPoll(ctx context.Context, subscribe []actions.Subscription, timeout int) ([]int64, error)
	PullEvent(ctx context.Context, viewID, nodeID, sinceSyncID int64) ([]actions.Event, int64, error)
	Download(ctx context.Context, fileID string, rootNodeID, targetSyncID int64, w io.Writer) (actions.Node, error)
}

// Transport is the engine's seam over the connection pool. EventClient yields a
// syncClient bound to the dedicated event connection (the long-poll loop owns it),
// or a terminal error when that connection is gone (the pool was closed or paused);
// WithWorker leases a worker connection for the duration of fn, builds a client
// over it, and returns the connection to the pool afterwards - even when fn fails,
// re-dialing it instead when the turn left the socket broken. ReplaceEvent re-dials
// the dedicated event connection after a long-poll turn breaks it. All three hide
// transport.Pool/actions.Client so the engine is testable with a fake.
type Transport interface {
	EventClient() (syncClient, error)
	WithWorker(ctx context.Context, fn func(syncClient) error) error
	ReplaceEvent(ctx context.Context) error
}

// brokenReporter is implemented by *actions.Client: it reports whether the turn just
// run left the connection byte-misaligned (a mid-I/O failure). poolTransport checks
// it through this interface so a dead socket is re-dialed rather than handed back to
// the pool, without widening the narrow syncClient seam (test fakes that do not
// implement it simply never trigger a replace).
type brokenReporter interface {
	Broken() bool
}

// logHeadProbe is the get_max_sync_id capability the cold-start walk needs.
//
// It is NOT optional and has no degradation: the walk reads the head BEFORE its first
// listing and persists it as the view's resume cursor, so there is nothing to fall back
// to - a guessed cursor is a missed change. Like treeLister it is asserted at run time
// rather than widening syncClient, and a client that cannot answer it cannot cold-start
// a view. Every fake that stands in for a server must therefore implement it; omitting it
// once made a documented timeout knob silently stop all syncing.
type logHeadProbe interface {
	MaxSyncID(ctx context.Context, viewID, rootNodeID int64) (int64, error)
}

// treeLister is the `list` capability the cold-start tree walk needs. Unlike
// logHeadProbe it is NOT an optional degradation: the walk IS
// the cold start, so a leased client that cannot list cannot cold-start a view,
// and saying so is better than quietly producing an empty catalog - which reads
// as "the server has nothing" and pushes the whole local tree.
type treeLister interface {
	List(ctx context.Context, viewID int64, path string) ([]actions.Node, bool, error)
}

// sessionPool is the subset of *transport.Pool that poolTransport uses. It is an
// interface so the adapter's lease/release wiring is unit-testable without bringing
// up a live, TLS-handshaking pool.
type sessionPool interface {
	Event() *transport.Session
	Paused() bool
	PauseReason() error
	Acquire(ctx context.Context) (*transport.Session, func(), error)
	Replace(ctx context.Context, dead *transport.Session) (*transport.Session, func(), error)
	ReplaceEvent(ctx context.Context) (*transport.Session, error)
}

// poolTransport adapts a transport.Pool into the Transport seam. It builds a fresh
// actions.Client per connection, injecting the session auth token into every turn.
type poolTransport struct {
	pool    sessionPool
	session string
	// log is threaded into every built actions.Client so the action layer's
	// degradation warnings (e.g. a download reply with no content hash, where MD4
	// verification is skipped) land in the engine's logger - the daemon's rotating
	// handler at its configured level/format - instead of slog.Default().
	log *slog.Logger
	// identity is the client-identity block injected into every action request.
	identity actions.Identity
	// newClient builds a syncClient over a leased (or event) session. It is a field
	// so tests can substitute a fake; production wraps actions.NewClientWithLogger.
	newClient func(*transport.Session) syncClient
}

// NewPoolTransport returns a Transport backed by pool, presenting session as the
// auth token injected into every action. The built clients log their degradation
// warnings through log (nil falls back to slog.Default()).
func NewPoolTransport(pool *transport.Pool, session string, log *slog.Logger) Transport {
	return newPoolTransport(pool, session, log)
}

// NewPoolTransportWithIdentity is NewPoolTransport with the client identity every
// action turn must carry (see actions.Identity).
func NewPoolTransportWithIdentity(pool *transport.Pool, session string, id actions.Identity, log *slog.Logger) Transport {
	pt := newPoolTransport(pool, session, log)
	pt.identity = id
	pt.newClient = func(s *transport.Session) syncClient {
		return actions.NewClientWithIdentity(s, session, id, pt.log)
	}
	return pt
}

// newPoolTransport is the shared constructor used by NewPoolTransport and by tests
// that wire a fake pool; it installs the production actions.NewClientWithLogger
// factory. A nil log falls back to slog.Default().
func newPoolTransport(pool sessionPool, session string, log *slog.Logger) *poolTransport {
	if log == nil {
		log = slog.Default()
	}
	pt := &poolTransport{
		pool:    pool,
		session: session,
		log:     log,
	}
	pt.newClient = func(s *transport.Session) syncClient {
		return actions.NewClientWithLogger(s, session, pt.log)
	}
	return pt
}

// EventClient returns a syncClient bound to the pool's dedicated event connection.
// The long-poll loop owns this connection exclusively; it is never leased.
//
// Pool.Event returns nil before Start, after Close, or once the pool is paused (a
// vanished session). Building a client over a nil session would dereference it in
// actions.NewClient and panic. A concurrent Close/pause can win the race
// between the loop's terminal-state checks and this call, so guard the nil here and
// surface a terminal error instead: the pool's own pause reason when it paused (the
// caller should re-pair on ErrPoolPaused, or update the client on
// ErrPoolClientRejected - hardcoding ErrPoolPaused here would mislabel a
// client-rejected pause that won the race as a re-login signal), otherwise
// ErrPoolClosed. Either stops Run via isTerminal rather than crashing the loop.
func (p *poolTransport) EventClient() (syncClient, error) {
	sess := p.pool.Event()
	if sess == nil {
		if p.pool.Paused() {
			if reason := p.pool.PauseReason(); reason != nil {
				return nil, reason
			}
			return nil, transport.ErrPoolPaused
		}
		return nil, transport.ErrPoolClosed
	}
	return p.newClient(sess), nil
}

// WithWorker leases an idle worker connection, runs fn against a client built over
// it, and returns the connection to the pool. When the turn left the socket broken
// (a mid-I/O failure), the connection is re-dialed through the pool's Replace
// instead of being released: returning a dead socket to the pool would hand it to
// the next Acquire and stall sync indefinitely, since the pool only re-dials on
// demand (the missing call this fixes). The broken flag is consulted
// UNCONDITIONALLY, not only when fn returned an error: a callback that swallows a
// probe's mid-I/O failure and returns nil would otherwise hand the byte-misaligned
// socket back to the pool as healthy, and the next lease's fresh client (the broken
// flag is per-Client) would decode the abandoned reply's tail as its own values -
// worst case a stray nested map without a children array read as a clean empty List,
// which confirmAbsentByParentList took as positive proof of absence and acked an
// unperformed remove. That last step is now blocked at the
// decode layer too - a reply carrying no children array reports listed=false, which
// every absence prover treats as inconclusive - but the
// misalignment itself still corrupts every subsequent turn, so the unconditional
// re-dial stands on its own. A clean application error (a non-zero
// errno) leaves the connection healthy and it is released normally. Acquire errors
// (the pool closed or paused) are surfaced without invoking fn.
func (p *poolTransport) WithWorker(ctx context.Context, fn func(syncClient) error) error {
	sess, release, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	c := p.newClient(sess)
	ferr := fn(c)
	if br, ok := c.(brokenReporter); ok && br.Broken() {
		// Swap the dead session for a fresh one. Replace closes the dead connection,
		// re-dials under the pool backoff, and returns the replacement leased; we
		// release it at once since this turn is done. Do NOT also call release() -
		// Replace owns the dead lease. A Replace error (the pool paused on a vanished
		// session, closed, or ctx cancelled) is terminal and surfaced in place of the
		// turn error so the engine loop reacts to it.
		_, freshRelease, rerr := p.pool.Replace(ctx, sess)
		if rerr != nil {
			return rerr
		}
		freshRelease()
		return ferr
	}
	release()
	return ferr
}

// ReplaceEvent re-dials the dedicated event connection after a long-poll turn left
// it broken. It delegates to the pool's ReplaceEvent (which closes the old event
// session and re-dials under backoff); the engine's loop calls it so the next poll
// runs over a fresh socket instead of the dead one.
func (p *poolTransport) ReplaceEvent(ctx context.Context) error {
	_, err := p.pool.ReplaceEvent(ctx)
	return err
}

// withWorker is the engine's single entry point for leasing a worker connection: it
// delegates to the Transport seam and, when limits.OpTurnTimeout is set, hands fn a
// client whose bounded request/reply turns each carry their own context deadline.
// Without one, a silent or protocol-violating server - one that
// never replies, or acks without draining the request body - hangs the op, its worker
// and every op chained behind it on the conflict key until shutdown: nothing ever
// classifies the turn as failed, so not even the stuck-op escalation fires (the apply
// never completes a drain). Under the deadline the turn fails transiently - surfaced
// as the typed errTurnTimeout, NOT context.DeadlineExceeded, so isTerminal never
// mistakes it for a fatal run-ctx deadline - a mid-I/O abort marks
// the connection broken so WithWorker re-dials the worker, and the op stays queued for
// the next drain like any other transient failure. A zero timeout (the default) passes
// the leased client through untouched.
//
// That default is why the deadline is no longer the only bound on those turns: the
// hang above was reachable in the SHIPPED configuration, since nobody sets the knob.
// Every action reply read now also runs under an idle/progress budget in the action
// layer itself (actions.actionIdleTimeout, disarmed only for long_poll), so a server
// that goes silent mid-reply fails the turn as the transient actions.ErrTransferStalled
// whether or not this deadline is configured. The per-turn deadline remains the
// operator's stricter, opt-in bound - it fails a turn that is slow, not merely silent.
//
// Engine code must lease workers through this helper, not e.transport.WithWorker
// directly, so every turn is covered.
// It is also where a completed turn is recorded as contact with the server
// (progress.noteServerContact), for the same reason: this is the one place every worker
// turn passes through, so nothing has to remember to stamp it.
func (e *Engine) withWorker(ctx context.Context, fn func(syncClient) error) error {
	release, err := e.enterBulk(ctx)
	if err != nil {
		return err
	}
	defer release()
	return e.withWalkWorker(ctx, fn)
}

// withWalkWorker is withWorker for the tree walk's own leases: it skips the gate that keeps a
// worker free for them (walklane.go).
func (e *Engine) withWalkWorker(ctx context.Context, fn func(syncClient) error) error {
	var err error
	if e.turnTimeout <= 0 {
		err = e.transport.WithWorker(ctx, fn)
	} else {
		err = e.transport.WithWorker(ctx, func(c syncClient) error {
			return fn(&turnLimitedClient{inner: c, timeout: e.turnTimeout})
		})
	}
	if err == nil {
		e.progress.noteServerContact()
	}
	return err
}

// errTurnTimeout classifies a bounded worker turn aborted by its own
// limits.OpTurnTimeout deadline while the caller's context was still live: a
// transient, retryable failure - the aborted turn marked the connection broken, so
// WithWorker re-dials the worker and the op stays queued for the next drain. It
// exists because the raw context.DeadlineExceeded the turn produces is
// indistinguishable from a fatal run-ctx deadline up the call chain: isTerminal
// reads that sentinel as "stop Run", so the first timed-out turn under an enabled
// op_turn_timeout_s would crash the daemon instead of retrying
// (a regression of the turn-deadline mapping, previously handled inline
// by the long-poll path only).
var errTurnTimeout = errors.New("engine: op turn timed out (limits.op_turn_timeout_s)")

// turnTimeoutError carries the aborted turn's underlying error for the log line
// while matching only the errTurnTimeout sentinel. It deliberately has NO Unwrap:
// exposing the chain would let errors.Is(err, context.DeadlineExceeded) reach the
// context sentinel again and re-open the terminal misclassification this type
// exists to close.
type turnTimeoutError struct{ cause error }

func (e *turnTimeoutError) Error() string {
	return errTurnTimeout.Error() + ": " + e.cause.Error()
}

// Is matches the errTurnTimeout sentinel so callers classify the timeout with
// errors.Is without unwrapping to the context error.
func (e *turnTimeoutError) Is(target error) bool { return target == errTurnTimeout }

// mapTurnTimeout translates an expiry of the turn's OWN deadline into the typed
// transient errTurnTimeout. The turn ctx is parent + the per-turn timeout, so a
// context.DeadlineExceeded surfacing while the parent is still live can only be the
// turn deadline (a parent deadline that fired sets parent.Err()); a dead parent
// passes the error through unchanged - cancellation and a run-level deadline stay
// terminal for the loop exactly as before.
func mapTurnTimeout(parent context.Context, err error) error {
	if err == nil || parent.Err() != nil || !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &turnTimeoutError{cause: err}
}

// turnLimitedClient decorates a leased worker client so each bounded request/reply
// turn runs under its own limits.OpTurnTimeout deadline.
//
// Bounded here means the turn's transfer size is client-controlled, so a deadline
// comfortably above the worst-case transfer time is safe: the catalog/mutate do-turns
// (QueryUserInfo, QueryNode, PullEvent, List, Mkdir, Rename, Remove, ResumeGetToken)
// move small replies, Upload at most limits.UploadResumeThreshold inline bytes and
// ResumeUpload the whole file in one streamed content frame - the upload turns being
// exactly the cited hang sites (a server that acks without draining the request body
// blocks the client's write forever).
//
// Deliberately NOT wrapped:
//
//   - LongPoll: intentionally long - pollOnce owns its client-side deadline, and
//     workers never long-poll anyway (the dedicated event connection does).
//
//   - Download / ResumeDownload: the response length is server-controlled, so no
//     fixed deadline is safe - a legitimately slow large transfer would be aborted on
//     every drain and restart from scratch, so the timeout itself would wedge the op
//     it was meant to protect.
//
//     The exclusion used to leave them with no time bound at all, in ANY
//     configuration: a peer that wedged mid-body (socket alive, no bytes) blocked the
//     decode forever, so the op goroutine never returned, drainBatch never joined and
//     the whole cycle froze with StuckOps at zero. They are now
//     bounded by an IDLE budget instead - actions.downloadIdleTimeout, re-armed on
//     every byte read, so only a transfer making NO progress fails (as the transient
//     actions.ErrTransferStalled, which poisons the connection so the worker is
//     re-dialed and the op stays queued). A fixed per-turn deadline stays deliberately
//     off; the idle budget is the progress guarantee that replaces it, alongside the
//     existing whole-file/per-chunk MD4 verification and the zero-byte-chunk stall
//     guard (which only fires BETWEEN completed resume turns, never mid-turn). The
//     turns listed above carry an idle budget of their own too (see withWorker), so
//     the wrap below is a stricter opt-in bound rather than their only one.
//
// The optional interfaces the apply paths type-assert on a leased client
// (outboundClient, resumeDownloadClient, brokenReporter) are satisfied by forwarding
// to the inner client dynamically, yielding the same errNoOutbound /
// errNoResumeDownload sentinels (or Broken() = false) the assert site would have
// produced on the bare client, so behaviour is unchanged apart from the deadline.
type turnLimitedClient struct {
	inner   syncClient
	timeout time.Duration
}

// turnCtx derives one bounded turn's deadline context.
func (t *turnLimitedClient) turnCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, t.timeout)
}

func (t *turnLimitedClient) QueryUserInfo(ctx context.Context) ([]actions.View, error) {
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	vs, err := t.inner.QueryUserInfo(tctx)
	return vs, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) QueryNode(ctx context.Context, viewID int64, path string) (actions.Node, error) {
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	n, err := t.inner.QueryNode(tctx, viewID, path)
	return n, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) PullEvent(ctx context.Context, viewID, nodeID, sinceSyncID int64) ([]actions.Event, int64, error) {
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	evs, next, err := t.inner.PullEvent(tctx, viewID, nodeID, sinceSyncID)
	return evs, next, mapTurnTimeout(ctx, err)
}

// LongPoll passes through unbounded: the poll is intentionally long and pollOnce owns
// its client-side deadline (workers never long-poll; the event connection does).
func (t *turnLimitedClient) LongPoll(ctx context.Context, subscribe []actions.Subscription, timeout int) ([]int64, error) {
	return t.inner.LongPoll(ctx, subscribe, timeout)
}

// Download passes through without a per-turn deadline: the response length is
// server-controlled (see the type comment). The action layer's idle-progress budget
// bounds a stalled transfer instead.
func (t *turnLimitedClient) Download(ctx context.Context, fileID string, rootNodeID, targetSyncID int64, w io.Writer) (actions.Node, error) {
	return t.inner.Download(ctx, fileID, rootNodeID, targetSyncID, w)
}

func (t *turnLimitedClient) Upload(ctx context.Context, parentID, name string, mtime, rootNodeID int64, content []byte) (actions.Node, error) {
	oc, ok := t.inner.(outboundClient)
	if !ok {
		return actions.Node{}, errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	n, err := oc.Upload(tctx, parentID, name, mtime, rootNodeID, content)
	return n, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) Mkdir(ctx context.Context, parentID, name string, mtime, rootNodeID int64) (actions.Node, error) {
	oc, ok := t.inner.(outboundClient)
	if !ok {
		return actions.Node{}, errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	n, err := oc.Mkdir(tctx, parentID, name, mtime, rootNodeID)
	return n, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) Rename(ctx context.Context, viewID, rootNodeID int64, fromPath, toPath string) error {
	oc, ok := t.inner.(outboundClient)
	if !ok {
		return errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	return mapTurnTimeout(ctx, oc.Rename(tctx, viewID, rootNodeID, fromPath, toPath))
}

func (t *turnLimitedClient) Remove(ctx context.Context, viewID int64, path string) error {
	oc, ok := t.inner.(outboundClient)
	if !ok {
		return errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	return mapTurnTimeout(ctx, oc.Remove(tctx, viewID, path))
}

// MaxSyncID forwards the log-head probe under the per-turn deadline.
//
// It has to exist. inner is a NAMED FIELD, not embedded, so nothing is promoted: a verb
// this wrapper does not declare simply is not on its method set. The cold-start walk
// hard-requires logHeadProbe - there is no head to degrade to - and withWorker installs
// this wrapper for every leased client whenever limits.op_turn_timeout_s is set. Without
// this forwarder that knob alone made every cold start fail before its first list turn,
// which is not a slow sync but no sync at all: a view that never completes its cold start
// is excluded from both the long-poll subscription and localReconcile.
func (t *turnLimitedClient) MaxSyncID(ctx context.Context, viewID, rootNodeID int64) (int64, error) {
	probe, ok := t.inner.(logHeadProbe)
	if !ok {
		return 0, errNoLogHeadProbe
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	head, err := probe.MaxSyncID(tctx, viewID, rootNodeID)
	return head, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) List(ctx context.Context, viewID int64, path string) ([]actions.Node, bool, error) {
	// treeLister, not outboundClient: listing is a READ, and demanding the whole push
	// surface for it means a client that can enumerate but not write cannot be walked. The
	// production client implements both, so this never showed - until the cold-start walk
	// started listing through the wrapper with a read-only client.
	oc, ok := t.inner.(treeLister)
	if !ok {
		return nil, false, errNoTreeLister
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	ns, listed, err := oc.List(tctx, viewID, path)
	return ns, listed, mapTurnTimeout(ctx, err)
}

// ResumeGetToken is declared by both outboundClient and resumeDownloadClient, so it
// forwards on its own single-method interface; a client implementing either side
// implements it. The errNoOutbound sentinel covers the (production-impossible)
// missing case for both callers - each treats it as a failed turn, not by identity.
func (t *turnLimitedClient) ResumeGetToken(ctx context.Context, req actions.ResumeRequest) (actions.ResumeToken, error) {
	rt, ok := t.inner.(interface {
		ResumeGetToken(context.Context, actions.ResumeRequest) (actions.ResumeToken, error)
	})
	if !ok {
		return actions.ResumeToken{}, errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	tok, err := rt.ResumeGetToken(tctx, req)
	return tok, mapTurnTimeout(ctx, err)
}

func (t *turnLimitedClient) ResumeUpload(ctx context.Context, token string, spec actions.ResumeUploadSpec, body io.Reader) (actions.Node, error) {
	oc, ok := t.inner.(outboundClient)
	if !ok {
		return actions.Node{}, errNoOutbound
	}
	tctx, cancel := t.turnCtx(ctx)
	defer cancel()
	n, err := oc.ResumeUpload(tctx, token, spec, body)
	return n, mapTurnTimeout(ctx, err)
}

// ResumeDownload passes through without a per-turn deadline: the chunk length is
// server-controlled (see the type comment). The action layer's idle-progress budget
// bounds a stalled chunk; the between-turns zero-byte stall guard and the per-chunk
// MD4 remain its other guards.
func (t *turnLimitedClient) ResumeDownload(ctx context.Context, token, fileID string, rootNodeID, targetSyncID, offset int64, w io.Writer) (actions.Node, error) {
	rd, ok := t.inner.(resumeDownloadClient)
	if !ok {
		return actions.Node{}, errNoResumeDownload
	}
	return rd.ResumeDownload(ctx, token, fileID, rootNodeID, targetSyncID, offset, w)
}

// Broken forwards the inner client's broken-connection report so the in-turn
// re-dial decisions (and poolTransport's replace-on-broken) keep working through the
// decorator; a client without the report (a plain test fake) reads as not broken,
// matching the bare assert-site behaviour.
func (t *turnLimitedClient) Broken() bool {
	if br, ok := t.inner.(brokenReporter); ok {
		return br.Broken()
	}
	return false
}

// profileDigester is the long_poll profile digest an actions.Client learns from each reply. The
// engine carries it across the event Client being rebuilt; a client without one (a test fake)
// simply polls without it.
type profileDigester interface {
	ProfileDigest() string
	SetProfileDigest(string)
}

// ProfileDigest forwards to the inner client's digest, if it keeps one.
func (t *turnLimitedClient) ProfileDigest() string {
	if pd, ok := t.inner.(profileDigester); ok {
		return pd.ProfileDigest()
	}
	return ""
}

// SetProfileDigest forwards to the inner client, if it keeps a digest.
func (t *turnLimitedClient) SetProfileDigest(d string) {
	if pd, ok := t.inner.(profileDigester); ok {
		pd.SetProfileDigest(d)
	}
}
