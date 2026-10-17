// Package actions implements the typed sproto action-client and the Phase-3
// catalog and (small-file) transfer operations that run over an authenticated,
// TLS-pinned transport.Session after the handshake.
//
// Every post-handshake operation is a CmdAction (0x01) request whose work is
// selected by the "action" string inside the request PObject; the reply is a
// bare PObject carrying an "errno". Client.do centralises request framing
// (injecting "action" and "session"), reply decoding, and errno→typed-error
// mapping; each operation is a thin typed method on top.
//
// Placement note: DESIGN §3 lists the action-client under internal/sproto. It
// lives in this dedicated package instead to keep sproto a pure wire codec, to
// match the existing layering (auth/transport/secrets are separate packages), and
// to avoid an import cycle - the action layer needs auth's error semantics and
// md4, both of which peer or import sproto.
package actions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/transport"
)

// maxReplyInlineBinary bounds how many payload bytes of an inline Binary the
// general (non-streaming) reply path buffers in memory. Catalog/metadata replies
// that flow through do carry no file content - only Download and ResumeDownload
// stream content, and they drive a Decoder directly with their own
// downloadInlineLimit. A do reply that nonetheless presents a Binary larger than
// this is hostile or corrupt: rather than let an attacker-controlled 64-bit length
// drive an unbounded allocation, its bytes are drained (so the connection stays
// byte-aligned and reusable) and the reply is rejected as ErrUnexpectedReply.
const maxReplyInlineBinary = 1 << 20 // 1 MiB

// Identity carries the client-identity fields a live DSM validates on every
// action turn (not only at auth). A zero Identity injects nothing, which is what
// the in-memory fakes want; production wires it from the paired config.
type Identity struct {
	// Agent is the agent PObject (auth.ClientInfo.Agent).
	Agent sproto.Value
	// ClientVersion is the dotted version string ("MAJOR.MINOR.MINI-BUILD").
	ClientVersion string
	// ClientType is the client category the server expects ("drive").
	ClientType string
	// DsID is the DiskStation id, sent as server_id.restore_id.
	DsID string
}

// Client issues typed actions over a single authenticated connection. It is not
// safe for concurrent use; the underlying connection serialises request/response
// turns.
type Client struct {
	conn    *sproto.Conn
	session string
	// identity is injected into every request; see buildRequest.
	identity Identity
	// broken is set once a turn is interrupted mid-I/O (a write, read or decode
	// error). After a partial transfer the socket is byte-misaligned, so reusing it
	// could read leftover bytes of the abandoned reply as the next action's. A
	// broken Client refuses further turns with ErrConnBroken.
	broken bool
	// log records degradations that would otherwise pass silently - notably a
	// download reply that advertises no content hash, where the MD4 integrity check
	// is skipped. It defaults to slog.Default() so a Client built by either
	// constructor always has a usable logger.
	log *slog.Logger
	// profileDigest is the digest the last long_poll reply carried, sent back on the next one
	// (see LongPoll). Empty until the first reply.
	profileDigest string
}

// NewClient returns a Client that issues actions over sess, presenting session as
// the auth token injected into every request. Degradation warnings go to
// slog.Default(); callers with their own logger use NewClientWithLogger.
func NewClient(sess *transport.Session, session string) *Client {
	return NewClientWithLogger(sess, session, nil)
}

// NewClientWithLogger is NewClient with the degradation logger injected: the
// warnings this layer emits (notably the download/resume_download replies that
// carry no content hash, where MD4 verification is skipped) route through log
// instead of slog.Default(), so a daemon-built client lands them in the daemon's
// rotating log at its configured level and format. A nil log falls back to
// slog.Default().
func NewClientWithLogger(sess *transport.Session, session string, log *slog.Logger) *Client {
	return NewClientWithIdentity(sess, session, Identity{}, log)
}

// NewClientWithIdentity is NewClientWithLogger with the client identity injected.
// Production always supplies one: a live DSM rejects an action turn that does not
// carry agent / client_version / client_type / server_id with 20481. A zero
// Identity keeps the older behaviour for callers that speak to an in-memory fake.
func NewClientWithIdentity(sess *transport.Session, session string, id Identity, log *slog.Logger) *Client {
	c := newClient(sess.Conn(), session)
	c.identity = id
	if log != nil {
		c.log = log
	}
	return c
}

// newClient builds a Client directly over a sproto.Conn. It is the shared
// constructor used by NewClient and by tests that wire a mock server without a
// full transport handshake.
func newClient(conn *sproto.Conn, session string) *Client {
	return &Client{conn: conn, session: session, log: slog.Default()}
}

// Broken reports whether a turn on this Client was interrupted mid-I/O (a write,
// read or decode failure), leaving the underlying connection byte-misaligned and
// unusable. The connection-pool layer checks this after a failed turn to decide
// whether to re-dial the connection (a broken socket) rather than return it to the
// pool. A clean server-application error (a non-zero errno, or a malformed but
// fully-read reply) does not break the connection, so Broken stays false.
func (c *Client) Broken() bool { return c.broken }

// actionIdleTimeout bounds how long an ordinary action reply may go with NO read
// progress at all before the turn is failed as ErrTransferStalled.
//
// It is the same idle/progress doctrine downloadIdleTimeout applies to the streamed
// download replies (idle.go), extended to the turns whose only other bound is
// limits.op_turn_timeout_s - which ships DISABLED (config.defaultOpTurnTimeout = 0),
// so in the default configuration nothing bounded them at all. A server that accepts
// the request and then goes silent mid-reply (a wedged storage layer, a SIGSTOPped
// peer - the kernel keeps ACKing, so TCP never notices) left the codec blocked inside
// io.ReadFull forever, and with it the op goroutine, drainBatch's join, and the whole
// sync cycle: the exact freeze the idle watchdog closed for Download, reachable
// through list / query_node / pull_event / mkdir / rename / remove / the upload
// replies.
//
// The budget is deliberately far outside any plausible healthy silence: these replies
// are metadata, not transfers, so minutes without a single byte is a dead peer. It
// only ever tightens the socket deadline, so it composes with netctx.Watch exactly as
// the download guard does, and expiry is TRANSIENT - the connection is poisoned, the
// pool re-dials the worker and the op stays queued. long_poll is the one turn it is
// NOT armed for: its reply is withheld server-side for the whole poll timeout by
// design, so silence there is the healthy state and pollOnce's own deadline owns the
// bound. A var solely so tests can shorten it; defaultActionIdleTimeout keeps the
// shipped value pinnable while they do (the write-side floor rate of the two-frame
// upload turns is derived from it - see progressWriteChunk).
const defaultActionIdleTimeout = 5 * time.Minute

var actionIdleTimeout = defaultActionIdleTimeout

// do runs one action turn: it clones params, injects "action" and "session",
// writes a CmdAction request, reads the bare-PObject reply, asserts it is a Map,
// extracts "errno", and maps a non-zero code to a typed error. The turn is bound
// by ctx so cancellation or a deadline interrupts the blocking I/O, and its reply
// read additionally by actionIdleTimeout so a silent server cannot wedge it forever
// in the default configuration.
func (c *Client) do(ctx context.Context, action string, params *sproto.Map) (*sproto.Map, error) {
	return c.doIdle(ctx, action, params, actionIdleTimeout)
}

// doIdle is do with an explicit idle budget for the reply read. A non-positive budget
// disarms the watchdog, which only long_poll asks for (see actionIdleTimeout).
func (c *Client) doIdle(ctx context.Context, action string, params *sproto.Map, idle time.Duration) (*sproto.Map, error) {
	if c.broken {
		return nil, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()
	req := buildRequest(action, c.session, c.identity, params)
	// The turn that POISONS the connection carries the ErrConnBroken sentinel too, not just the
	// wrapped network error. Only the turns AFTER it used to match the sentinel (the c.broken
	// refusal above), so the first mid-I/O failure - the one that actually broke the socket -
	// looked like an ordinary failure to every caller keying on it: engine.isTerminalRefreshError
	// missed it and needed a direct Broken() check to compensate, and engine.unprovedByTransport
	// inherited the blind spot WITHOUT one, folding a network outage into the synthetic
	// removeDirUnverifiedCode streak. That streak is not a transient class, so a flaky link
	// dead-lettered the user's directory delete and left a StateConflict record localReconcile
	// never drives again. Wrapping both sentinel and cause keeps errors.Is working for each.
	if err := c.conn.WriteRequest(sproto.CmdAction, req); err != nil {
		c.broken = true
		return nil, fmt.Errorf("actions: write %s request: %w: %w", action, ErrConnBroken, netctx.Err(ctx, err))
	}
	resp, oversize, stalled, err := c.readCappedReplyIdle(idle)
	if err != nil {
		c.broken = true
		if stalled {
			return nil, stallError(action, idle)
		}
		return nil, fmt.Errorf("actions: read %s reply: %w: %w", action, ErrConnBroken, netctx.Err(ctx, err))
	}
	if oversize {
		// The oversized Binary was drained, so the connection is still byte-aligned
		// and must not be marked broken - reject just this reply as malformed.
		return nil, fmt.Errorf("actions: %s reply has oversized inline binary (>%d bytes): %w",
			action, maxReplyInlineBinary, ErrUnexpectedReply)
	}
	// A non-zero errno or a malformed reply leaves the connection byte-aligned (the
	// whole reply was read), so decodeReply errors do not poison the Client.
	return decodeReply(action, resp)
}

// doMutate runs one action turn like do but additionally requires the reply to
// carry an "errno" field. The mutating actions (rename, remove,
// unlink_connection) report nothing but success, so a present errno == 0 is
// their only positive evidence that the server performed the mutation: a
// half-formed reply map without one must not be recorded locally as a completed
// server mutation, and is rejected as ErrUnexpectedReply instead. The whole
// reply was read, so the connection stays byte-aligned and is not poisoned.
// Read/metadata actions deliberately stay on the tolerant do (see decodeReply):
// their callers decode action-specific fields as the positive evidence, so an
// absent errno at worst fails that decode rather than faking a committed
// mutation.
func (c *Client) doMutate(ctx context.Context, action string, params *sproto.Map) error {
	m, err := c.do(ctx, action, params)
	if err != nil {
		return err
	}
	// The success evidence is the reply's result VERDICT, not a literal "errno"
	// key: a live DSM states success by marking the frame "type":"response" and
	// omitting "error" entirely (see errno). Requiring the key itself rejected
	// every genuinely committed mutation.
	if _, present, verr := errno(m); verr != nil || !present {
		if verr != nil {
			return verr
		}
		return verdictlessError(action, m)
	}
	return nil
}

// bodyJoinGrace bounds how long the two-frame upload turns (Upload,
// ResumeUpload) wait on their concurrent body-frame writer AFTER the reply has
// already produced its success evidence. On a conforming server that wait is
// zero-length: the server cannot claim it committed the write without having
// drained the whole body frame, and a net.Conn write returns once the bytes are
// handed to the kernel - well before the server can parse them - so the writer
// has already finished. The grace exists only so a non-conforming server that
// answers success WITHOUT draining the body cannot wedge the worker forever (see
// joinBodyFrame). It is a var solely so tests can shorten it.
var bodyJoinGrace = 30 * time.Second

// joinBodyFrame joins the concurrent body-frame writer of a two-frame upload turn
// on the SUCCESS path, bounded by bodyJoinGrace. A write error is fatal to the
// connection, as before.
//
// The bound is the point: the four rejection paths of Upload/ResumeUpload
// poison-and-close BEFORE joining precisely because a server that answered on the
// metadata frame alone may have left the writer wedged on socket/TLS send
// backpressure (a body over the send buffer, ~1-4 MiB) - and with
// limits.op_turn_timeout_s disabled (the default) no deadline ever forces the
// socket. The success path used to join unguarded, so the same non-conforming
// server that replies errno:0 (or an offset-bearing status) without draining the
// body hung the worker forever: exactly the wedge already closed for the
// errno-less shape, reachable through the one branch it did not cover.
//
// On expiry the connection is poisoned and closed - the close is what unblocks
// the wedged writer - the writer is then joined and the turn is rejected as
// ErrUnexpectedReply, which the engine treats as transient: the op stays queued
// and the pool re-dials the poisoned worker. The reply is NOT reported as a
// success: our body never reached the server, so its claim of a committed write
// is unbelievable, and the server's own framing is one frame out of step
// regardless.
//
// A cancelled ctx needs no case here: the caller's netctx.Watch forces a past
// write deadline on cancel, which fails the wedged write and delivers on bodyErr.
func (c *Client) joinBodyFrame(ctx context.Context, action string, bodyErr <-chan error) error {
	timer := time.NewTimer(bodyJoinGrace)
	defer timer.Stop()
	select {
	case werr := <-bodyErr:
		if werr != nil {
			c.broken = true
			// Carries the sentinel for the same reason doIdle's two poisoning returns do: this is
			// the turn that broke the socket, and a caller classifying transport failures must be
			// able to see that from the error alone.
			return fmt.Errorf("actions: write %s body: %w: %w", action, ErrConnBroken, netctx.Err(ctx, werr))
		}
		return nil
	case <-timer.C:
	}
	c.broken = true
	_ = c.conn.Close()
	<-bodyErr
	return fmt.Errorf("actions: %s reply reported success but the body frame was still unwritten after %s "+
		"(the server answered without draining it): %w", action, bodyJoinGrace, ErrUnexpectedReply)
}

// readCappedReplyIdle reads one bare-PObject reply with inline Binary values bounded
// to maxReplyInlineBinary: a thin wrapper over the shared
// sproto.Conn.ReadCappedResponse core (also used by auth). Any Binary larger
// than the cap is drained rather than buffered - keeping the connection
// byte-aligned - and reported via the oversize flag so the caller can reject the
// reply without re-dialing. A genuine I/O or decode failure is returned as err
// (and does leave the connection misaligned).
//
// The read runs under an idle-progress watchdog of the given budget (see
// actionIdleTimeout); stalled reports that the watchdog - rather than the caller's
// context or the server itself - is what ended the read, so the caller can name the
// cause. A non-positive budget disarms it.
func (c *Client) readCappedReplyIdle(idle time.Duration) (resp sproto.Value, oversize, stalled bool, err error) {
	return c.readCappedReplyGuarded(watchIdle(c.conn.NetConn(), idle))
}

// readCappedReplyGuarded is readCappedReplyIdle over an ALREADY-ARMED guard. The
// two-frame upload turns need it: they arm the guard before launching the concurrent
// body writer so the write can report its own progress (idleGuard.wrapWriter) -
// otherwise the budget would cover the whole body transfer, during which no reply byte
// can arrive by protocol, and act as a fixed deadline on it.
func (c *Client) readCappedReplyGuarded(guard *idleGuard) (resp sproto.Value, oversize, stalled bool, err error) {
	resp, oversize, err = c.conn.ReadCappedResponseWith(maxReplyInlineBinary, guard.wrap)
	// Skip any unsolicited keep-alive the server interleaves ahead of the reply.
	//
	// An OVERSIZED keep-alive is stepped over like any other. Its inline Binary was drained, so
	// the connection is byte-aligned and reading on is safe - and it is still not this turn's
	// reply, which is the only question here. Stopping on it instead (the `!oversize` this
	// condition used to carry) handed the caller a keep-alive as the reply, and the oversize arm
	// deliberately does not poison, so one such frame put a session one reply behind back into the
	// pool. Alignment was never the issue: the reply the server still owes is.
	for i := 0; err == nil && isKeepAlive(resp) && i < maxSkippedKeepAlives; i++ {
		resp, oversize, err = c.conn.ReadCappedResponseWith(maxReplyInlineBinary, guard.wrap)
	}
	// Join the watchdog before reading its verdict, so a guard that fires in the
	// instant the read returns is never missed. It deliberately leaves a fired
	// deadline in place; the caller's netctx.Watch clears it on the clean path.
	guard.stop()
	// The bound is spent and a keep-alive is STILL in hand, so this turn's real reply was never
	// read and the server still owes it. Every caller poisons the connection on an error, which is
	// the only correct outcome here: the frames all decoded cleanly, so nothing else would mark
	// the session bad, and it would go back to the pool one reply behind FOREVER. The next turn on
	// it then reads the PREVIOUS turn's reply - and a live DSM states success as a bare
	// `type:"response"` frame with no error block, which errno() reads as a full success verdict,
	// so a stale frame becomes the commit evidence for a mutation it does not answer. That is the
	// reply-evidence doctrine subverted silently, which is worse than the flood it came from.
	if err == nil && isKeepAlive(resp) {
		return resp, oversize, guard.stalled(), errKeepAliveFlood
	}
	return resp, oversize, guard.stalled(), err
}

// errKeepAliveFlood reports that a reply read stepped over maxSkippedKeepAlives unsolicited
// frames and still had no reply. It is not a malformed REPLY (nothing was read to malform) and
// not a transport failure: it is a protocol-STATE verdict, and the only cure is a fresh
// connection, so every caller turns it into a poisoned session. A conforming DSM keep-alives on
// its own 150s cadence and cannot produce it; a peer flooding the connection can.
var errKeepAliveFlood = errors.New("actions: no reply after the keep-alive limit; the connection is one reply behind")

// maxSkippedKeepAlives bounds how many unsolicited frames one reply read will step
// over. Keep-alives arrive on the server's own schedule (the `alive` interval, 150s
// by default), so more than a couple ahead of a single reply is not liveness traffic
// but a peer flooding the connection; the bound turns that into an ordinary
// malformed-reply rejection instead of an unbounded loop.
const maxSkippedKeepAlives = 8

// isKeepAlive reports whether a decoded frame is the server's unsolicited
// keep-alive rather than a reply to the turn in flight.
//
// The server interleaves {"@proto": {...}, "type": "keep_alive"} frames onto an
// established connection at its own cadence. They are NOT answers to anything and
// need no response - the server accepts no wire-level keep-alive in the other
// direction - but a reply read that does not step over them takes one as the
// answer: it decodes cleanly, carries no result verdict, and poisons the connection.
// That was the intermittent large-upload failure, and it was never upload-specific:
// any turn whose reply a keep-alive happens to precede would have taken it.
func isKeepAlive(v sproto.Value) bool {
	m, ok := v.(*sproto.Map)
	if !ok {
		return false
	}
	t, ok := m.String("type")
	return ok && t == "keep_alive"
}

// stallError is the typed transient every idle-watchdog expiry reports: the peer
// stopped delivering bytes without closing the socket. It is returned ahead of
// netctx.Err, which would map the forced deadline onto the caller's context and hide
// the cause. The op stays queued and the pool re-dials the poisoned connection.
func stallError(action string, idle time.Duration) error {
	return fmt.Errorf("actions: read %s reply: no bytes for %s: %w", action, idle, ErrTransferStalled)
}

// decodeReply validates a reply Value and maps its result code. A non-Map reply
// is ErrUnexpectedReply; a present non-zero errno is mapped to a typed error;
// otherwise the Map is returned for the caller to read action-specific fields.
// An absent errno is tolerated here: read/metadata callers take their
// action-specific fields as the positive evidence, while the mutating actions -
// whose only evidence IS the errno - layer doMutate on top to require one.
func decodeReply(action string, resp sproto.Value) (*sproto.Map, error) {
	m, ok := resp.(*sproto.Map)
	if !ok {
		return nil, fmt.Errorf("actions: %s reply is %T, want map: %w", action, resp, ErrUnexpectedReply)
	}
	code, present, err := errno(m)
	if err != nil {
		return nil, err
	}
	if present && code != CodeOK {
		return nil, errorForCode(code)
	}
	return m, nil
}

// buildRequest clones params into a fresh Map with "action", "session" and the
// client identity injected first, leaving the caller's map untouched. Any key
// buildRequest injects is dropped from params in favour of the injected value.
//
// The identity block - agent, client_version, client_type, server_id - is not
// optional decoration: a live DSM validates it on EVERY action turn, not just at
// auth, and answers a turn without it with 20481 ("client version too low").
// Requests carrying only action+session therefore failed against a real server
// even though pairing had succeeded. An empty Identity injects nothing, so the
// in-memory fakes that never look at these keys are unaffected.
func buildRequest(action, session string, id Identity, params *sproto.Map) *sproto.Map {
	req := sproto.NewMap().
		Set("action", sproto.String(action)).
		Set("session", sproto.String(session))
	if id.Agent != nil {
		req.Set("agent", id.Agent)
	}
	if id.ClientVersion != "" {
		req.Set("client_version", sproto.String(id.ClientVersion))
	}
	if id.ClientType != "" {
		req.Set("client_type", sproto.String(id.ClientType))
	}
	if id.DsID != "" {
		req.Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(id.DsID)))
	}
	if params != nil {
		for _, k := range params.Keys() {
			switch k {
			case "action", "session":
				continue
			}
			v, _ := params.Get(k)
			req.Set(k, v)
		}
	}
	return req
}

// errno extracts the server result code from a reply. It returns (code, true,
// nil) when the reply carries a result verdict, (0, false, nil) when it carries
// none, and ErrUnexpectedReply when a verdict is present but malformed - so a
// wrong-typed code is never mistaken for success.
//
// A live DSM states failure as a NESTED map, not a flat integer:
//
//	{"type": "response", "error": {"action": 0, "code": 20481, "reason": "..."}}
//
// and states success by simply omitting "error" while still marking the frame
// "type": "response". There is no "errno" key anywhere in the real protocol, so
// keying only on it made every server error read as success-with-no-evidence and
// every success read as a contentless reply. Both real shapes are now recognised:
//
//   - "error" map with an integer "code"  → that code, present
//   - "type": "response" and no error     → 0 (success), present
//
// The flat "errno" form is still accepted first, since it costs nothing and keeps
// the in-memory fakes that speak it working unchanged.
func errno(m *sproto.Map) (int64, bool, error) {
	if v, ok := m.Get("errno"); ok {
		iv, ok := v.(sproto.Int)
		if !ok {
			return 0, false, fmt.Errorf("actions: errno is %T, want int: %w", v, ErrUnexpectedReply)
		}
		return int64(iv), true, nil
	}
	if v, ok := m.Get("error"); ok {
		em, ok := v.(*sproto.Map)
		if !ok {
			return 0, false, fmt.Errorf("actions: error is %T, want map: %w", v, ErrUnexpectedReply)
		}
		code, ok := em.Int("code")
		if !ok {
			return 0, false, fmt.Errorf("actions: error map carries no integer code: %w", ErrUnexpectedReply)
		}
		return code, true, nil
	}
	// No error block: a frame the server marked as a response is a positive
	// success verdict, which is the only success evidence the real protocol gives.
	if t, ok := m.String("type"); ok && t == "response" {
		return CodeOK, true, nil
	}
	return 0, false, nil
}

// verdictlessError reports a reply that decoded cleanly but carries no result
// verdict - no "error" block, no "errno", and not marked type:"response".
//
// It names the reply's top-level keys because that is the only way to learn what
// such a reply actually was: it has been observed rarely on a live DSM under write
// pressure, always transiently (the connection is poisoned, the pool re-dials and
// the op succeeds on retry), and it never reproduces under the instrumentation
// added to catch it. Carrying the keys in the message means the next occurrence -
// in a test, on a stand, or in an operator's log - identifies itself instead of
// being another opaque "malformed reply".
func verdictlessError(action string, reply *sproto.Map) error {
	if reply == nil {
		return fmt.Errorf("actions: %s reply carries no result verdict (nil reply): %w",
			action, ErrUnexpectedReply)
	}
	detail := "keys: " + strings.Join(reply.Keys(), ",")
	if t, ok := reply.String("type"); ok {
		detail += fmt.Sprintf(", type=%q", t)
	}
	if raw, ok := reply.Get("@proto"); ok {
		if pm, ok := raw.(*sproto.Map); ok {
			detail += ", @proto{" + strings.Join(pm.Keys(), ",") + "}"
			if bc, ok := pm.Int("body-continue"); ok {
				detail += fmt.Sprintf(" body-continue=%d", bc)
			}
			if t, ok := pm.String("type"); ok {
				detail += fmt.Sprintf(" type=%q", t)
			}
		}
	}
	return fmt.Errorf("actions: %s reply carries no result verdict (%s): %w",
		action, detail, ErrUnexpectedReply)
}
