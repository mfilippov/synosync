package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/mfilippov/synosync/internal/backoff"
)

// callTimeout bounds a single client request/response round-trip so a wedged
// daemon cannot hang the CLI indefinitely.
const callTimeout = 10 * time.Second

// maxRequestBytes caps a single control-plane request so a buggy or hostile
// local client cannot make the daemon buffer an unbounded JSON value into memory
// within the deadline window. Control requests are tiny (a method name plus small
// params), so 1 MiB is far above any legitimate call.
const maxRequestBytes = 1 << 20

// maxInFlightConns bounds the number of connections served concurrently. The
// control plane is effectively single-client (the CLI dials, makes one call, and
// closes), so a small cap is generous; it stops a misbehaving same-UID client
// from spawning unbounded goroutines in a tight accept loop (self-inflicted DoS).
const maxInFlightConns = 16

// maxResponseBytes caps the reply the client buffers from the daemon, mirroring
// the server's request cap. The endpoint is the local, OS-access-controlled
// daemon, so this is defense-in-depth/symmetry rather than a trust boundary.
const maxResponseBytes = 1 << 20

// handlerTimeout bounds how long dispatch waits for one handler to produce a
// result. It is the OUTER bound: the handler's own context expires a grace earlier
// (handlerBudget), so a handler that degrades on expiry can still be heard. The connection deadline bounds only the I/O, so before this a slow or
// wedged handler held serveConn - and with it Serve's WaitGroup - open for as
// long as it liked: daemon shutdown and the Windows SCM stop then waited on the
// slowest handler instead of the deadline the code claimed. It is deliberately
// shorter than callTimeout so the timeout reply still fits the client's own
// round-trip budget; otherwise the CLI's deadline fires first and reports a
// generic read failure instead of the daemon's diagnosis. A var so tests can
// shrink it.
var handlerTimeout = 8 * time.Second

// handlerUnwindGrace is the slice of handlerTimeout reserved for a handler to UNWIND and produce
// its answer after its own context expires, rather than being abandoned mid-unwind.
//
// It exists because a handler and the dispatch waiting for it used to race the SAME deadline.
// Both selects woke together, and dispatch always won - it had a ready case, while the handler
// still had to return from whatever it was blocked in, assemble its value and marshal it. So a
// handler written to DEGRADE on ctx expiry could never deliver the degraded answer: it finished
// into a channel nobody read, and the caller got the generic "did not finish" instead.
//
// That is not hypothetical. daemon's status handler bounds its single-flight wait on this very
// context precisely so an expired waiter falls through to lastKnownViewCounts + CountsStale,
// and the whole point of that design is that `status` degrades rather than
// fails while a counts scan is in flight. It never reached the operator: on a store whose scan
// outlives the budget, every status call during the scan returned the generic error instead of
// stale counts, cyclically. The unit test could not see it - it drives buildStatus directly and
// so never crosses the layer that dropped the result.
//
// The handler's budget is shortened by this grace rather than the wait being lengthened, so the
// outer promise - a control verb answers within handlerTimeout - is unchanged, which matters
// because the CLI's own round-trip budget (callTimeout) is sized against it.
const handlerUnwindGrace = time.Second

// handlerBudget is the context deadline one handler gets: handlerTimeout less the unwind grace.
//
// The grace is CLAMPED to half the budget because handlerTimeout is a var the tests shrink to
// milliseconds; subtracting a flat second there would hand the handler an already-expired context
// and turn every timeout test into an instant-cancel test. Production is unaffected - a second is
// far below half of eight.
func handlerBudget() time.Duration {
	grace := handlerUnwindGrace
	if half := handlerTimeout / 2; grace > half {
		grace = half
	}
	return handlerTimeout - grace
}

// request is the wire form of one JSON-RPC call: the caller's protocol version, a
// method name, and its opaque JSON parameters (omitted when the method takes none).
type request struct {
	Protocol int             `json:"protocol,omitempty"`
	Method   string          `json:"method"`
	Params   json.RawMessage `json:"params,omitempty"`
}

// response is the wire form of one reply: the answering daemon's protocol version and
// exactly one of Result or Error.
type response struct {
	Protocol int             `json:"protocol,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// Handler processes one method call. params is the raw JSON parameters (nil when
// the caller sent none); the returned value is marshalled into the reply's
// result, and a non-nil error is propagated to the caller as the reply's error.
//
// ctx is cancelled when the handler's own budget (handlerBudget - handlerTimeout less the
// unwind grace, so a handler that ANSWERS on expiry is still listened to) runs out, or the
// server's Serve context is cancelled (daemon shutdown, an SCM stop), whichever
// comes first. A handler doing bounded-but-slow work - the daemon's O(tree) store
// scans - must honour it: dispatch stops WAITING for a handler that ignores ctx,
// but only a cooperative handler stops consuming the store and CPU the sync data
// plane needs.
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// Server dispatches newline-delimited JSON-RPC requests to registered handlers,
// one request per accepted connection.
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	wg       sync.WaitGroup
	// newBackoff produces the delay sequence Serve sleeps between retries of a
	// transient Accept failure; each Serve call gets its own instance (Backoff is
	// not concurrency-safe). Tests inject a faster sequence.
	newBackoff func() *backoff.Backoff
	// log carries the server's diagnostics (the accept-retry Warn, the
	// handler-panic Error). The daemon injects its rotating-file logger via
	// NewServerWithLogger so these survive environments that discard stderr
	// (Windows SCM) or never rotate it (launchd); NewServer keeps slog.Default().
	log *slog.Logger
}

// NewServer returns a Server with no registered handlers, logging diagnostics
// through slog.Default().
func NewServer() *Server {
	return NewServerWithLogger(nil)
}

// NewServerWithLogger is NewServer with an injected diagnostics logger
// (mirroring actions.NewClientWithLogger); a nil log falls back to
// slog.Default().
func NewServerWithLogger(log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		handlers:   make(map[string]Handler),
		newBackoff: backoff.New,
		log:        log,
	}
}

// Handle registers (or replaces) the handler for method.
func (s *Server) Handle(method string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// Serve accepts connections on l until ctx is cancelled, handling one request
// per connection. It closes l when ctx is cancelled to unblock Accept, waits for
// in-flight connections to finish - each bounded by dispatch's handler budget,
// which the same cancellation collapses to zero, so the drain is bounded rather
// than hostage to the slowest handler - and returns nil on a clean shutdown. Only a
// closed listener is a terminal Accept failure (the net.ErrClosed class - l was
// closed underneath a live Serve; go-winio's ErrPipeListenerClosed aliases
// net.ErrClosed, so the Windows named-pipe listener is covered too). Any other
// Accept error - EMFILE/ENFILE under fd pressure, ECONNABORTED - is treated as
// transient: logged and retried under a capped jittered backoff, because Go does
// not retry these for us and a CLI call at an unlucky moment must degrade the
// control plane, never kill the daemon process that also runs the sync data
// plane.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	// Cancel the watcher goroutine when Serve returns for any reason (including an
	// unexpected Accept error) so it cannot outlive Serve, and always close the
	// listener exactly once.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	// Bound the number of connections served concurrently: a buffered semaphore the
	// accept loop fills before spawning a serve goroutine and the goroutine drains
	// on exit. A flood of connections then blocks the accept loop (which holds the
	// socket) rather than spawning goroutines without limit.
	sem := make(chan struct{}, maxInFlightConns)
	bo := s.newBackoff()
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.wg.Wait()
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return fmt.Errorf("ipc: accept: %w", err)
			}
			s.log.Warn("ipc: accept failed; retrying", "error", err)
			if werr := bo.Wait(ctx); werr != nil {
				s.wg.Wait()
				return nil
			}
			continue
		}
		bo.Reset()
		// Waiting for a free slot must not outlast the shutdown signal: with the cap
		// saturated the accept loop used to block here on a plain send, so a cancel
		// arriving mid-flood was only noticed once a slot freed. Drop the connection
		// instead (the CLI sees a closed connection and reports the daemon as
		// unreachable, which it is about to be) and unwind.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			_ = conn.Close()
			s.wg.Wait()
			return nil
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-sem }()
			s.serveConn(ctx, conn)
		}()
	}
}

// serveConn reads one request, dispatches it under ctx, writes one response, and
// closes.
func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	// Bound the request read so a wedged client cannot hold the connection open
	// indefinitely; the handler that follows it is bounded by dispatch, not by this
	// deadline (an I/O deadline says nothing about compute).
	_ = conn.SetDeadline(time.Now().Add(callTimeout))

	var req request
	if err := json.NewDecoder(io.LimitReader(conn, maxRequestBytes)).Decode(&req); err != nil {
		// Answer rather than hang up. Closing without a word left the client reporting
		// "read response: EOF" - which reads as a daemon that died mid-call, not as a request
		// it refused - and an unknown METHOD already gets a proper error, so a malformed
		// request was the one input whose rejection was invisible. An oversized body is the
		// same story: the LimitReader truncates it and the decode fails here.
		//
		// A write failure is ignored for the same reason it is on the success path: the peer
		// that sent an undecodable request may already be gone.
		_ = conn.SetWriteDeadline(time.Now().Add(callTimeout))
		_ = json.NewEncoder(conn).Encode(response{
			Protocol: Protocol,
			Error:    fmt.Sprintf("ipc: malformed request: %v", err),
		})
		return
	}
	resp := s.dispatch(ctx, req)
	// Stamped here rather than at every construction site inside dispatch, so no reply
	// path can be added later that forgets to say which protocol answered it.
	resp.Protocol = Protocol
	// Give the reply write its own fresh budget so a slow handler can't leave too
	// little time to flush an otherwise-valid response.
	_ = conn.SetWriteDeadline(time.Now().Add(callTimeout))
	_ = json.NewEncoder(conn).Encode(resp)
}

// dispatch routes a request to its handler and builds the reply, bounding how
// long the handler may hold the connection: the handler receives a context
// cancelled at handlerTimeout or with the server's Serve context (daemon
// shutdown, an SCM stop), and dispatch stops waiting the moment that context
// fires even if the handler does not honour it. Shutdown therefore drains in
// bounded time instead of waiting on the slowest control-plane handler.
//
// A handler abandoned that way is deliberately detached rather than waited for:
// it already carries a cancelled context (the daemon's handlers thread it into
// their store scans and unwind on their own), and holding Serve's WaitGroup for
// it would restore the very unbounded shutdown this bound removes. Its result
// lands in a buffered channel nobody reads, so the goroutine never blocks on the
// send.
//
// A handler panic is recovered and turned into a reply error so a buggy
// control-plane handler cannot crash the daemon process that also runs the sync
// data plane - the control plane must stay isolated from the data plane.
func (s *Server) dispatch(ctx context.Context, req request) response {
	// A caller from a newer build may mean something else by a method name this daemon
	// also has, so the version is checked before the method is looked up. A caller that
	// predates the field sends 0 and is answered normally: nothing has changed meaning,
	// and refusing it would break a working setup for nothing (see protocol.go).
	if req.Protocol > Protocol {
		return response{Error: (&ProtocolMismatchError{Peer: req.Protocol, Local: Protocol, Role: "command"}).Error()}
	}
	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()
	if !ok {
		return response{Error: fmt.Sprintf("ipc: unknown method %q", req.Method)}
	}

	// The handler's budget is deliberately SHORTER than the wait below, by handlerUnwindGrace:
	// a handler that degrades when its context expires needs room to produce the degraded answer
	// after the expiry, and racing it against the identical deadline guarantees it loses.
	hctx, cancel := context.WithTimeout(ctx, handlerBudget())
	defer cancel()

	done := make(chan response, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("ipc: handler panicked", "method", req.Method, "panic", r)
				done <- response{Error: fmt.Sprintf("ipc: handler for %q panicked: %v", req.Method, r)}
			}
		}()
		done <- s.call(hctx, h, req)
	}()

	hard := time.NewTimer(handlerTimeout)
	defer hard.Stop()
	select {
	case resp := <-done:
		return resp
	case <-ctx.Done():
		// The SERVER is shutting down (the Serve context), which is not the handler's fault and
		// must not read like a bug in the log.
		s.log.Info("ipc: abandoning in-flight handler; the control plane is shutting down",
			"method", req.Method)
		return response{Error: fmt.Sprintf("ipc: handler for %q did not finish: %v", req.Method, ctx.Err())}
	case <-hard.C:
		// The handler overran its own budget AND the unwind grace on top of it - a stuck
		// control-plane verb, which must be diagnosable from the rotating log.
		s.log.Warn("ipc: handler did not finish within its budget; abandoning it",
			"method", req.Method, "budget", handlerTimeout, "handler_budget", handlerBudget())
		return response{Error: fmt.Sprintf("ipc: handler for %q did not finish: %v", req.Method, context.DeadlineExceeded)}
	}
}

// call invokes h under ctx and turns its outcome into a reply.
func (s *Server) call(ctx context.Context, h Handler, req request) response {
	result, err := h(ctx, req.Params)
	if err != nil {
		return response{Error: err.Error()}
	}
	enc, err := json.Marshal(result)
	if err != nil {
		return response{Error: fmt.Sprintf("ipc: encode result for %q: %v", req.Method, err)}
	}
	return response{Result: enc}
}

// Client is a one-shot connection to the daemon's IPC socket: dial, Call, Close.
type Client struct {
	conn net.Conn
}

// Call sends a single request for method with params (nil to send none), reads
// the single reply, and unmarshals its result into result (nil to discard). A
// non-empty error field in the reply is returned as an error.
func (c *Client) Call(method string, params, result any) error {
	_ = c.conn.SetDeadline(time.Now().Add(callTimeout))

	var raw json.RawMessage
	if params != nil {
		enc, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("ipc: encode params: %w", err)
		}
		raw = enc
	}
	if err := json.NewEncoder(c.conn).Encode(request{Protocol: Protocol, Method: method, Params: raw}); err != nil {
		return fmt.Errorf("ipc: write request: %w", err)
	}

	var resp response
	if err := json.NewDecoder(io.LimitReader(c.conn, maxResponseBytes)).Decode(&resp); err != nil {
		return fmt.Errorf("ipc: read response: %w", err)
	}
	// Before the reply is interpreted: a daemon that speaks a protocol this build does
	// not know may mean anything by the fields it sent, including ones this build
	// recognises by name. The error the daemon may also have set is not more informative
	// than the mismatch itself, so the mismatch wins.
	if err := clientProtocolCheck(resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	if result != nil && len(resp.Result) > 0 {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("ipc: decode result: %w", err)
		}
	}
	return nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
