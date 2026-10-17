package actions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/transport"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// shortBodyJoinGrace shrinks the success-path body-writer join bound for the
// duration of one test, so the wedged-writer cases resolve in milliseconds rather
// than the production 30 s. It restores the previous value at cleanup. The tests
// that use it are single-turn and never run in parallel, so the package-level var
// is not raced.
func shortBodyJoinGrace(t *testing.T, d time.Duration) {
	t.Helper()
	prev := bodyJoinGrace
	bodyJoinGrace = d
	t.Cleanup(func() { bodyJoinGrace = prev })
}

func TestDoInjectsActionAndSession(t *testing.T) {
	const session = "abcdef0123456789abcdef0123456789"
	var got *sproto.Map
	c := mockClient(t, session, func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).Set("ok", sproto.Int(1))
	})

	params := sproto.NewMap().Set("path", sproto.String("/a/b"))
	reply, err := c.do(testContext(t), "query_node", params)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if _, ok := reply.Get("ok"); !ok {
		t.Error("reply map not returned to caller")
	}

	if v, _ := got.Get("action"); v != sproto.String("query_node") {
		t.Errorf("action = %v, want query_node", v)
	}
	if v, _ := got.Get("session"); v != sproto.String(session) {
		t.Errorf("session = %v, want %q", v, session)
	}
	if v, _ := got.Get("path"); v != sproto.String("/a/b") {
		t.Errorf("path = %v, want /a/b", v)
	}
	// action/session must be injected first, ahead of caller params.
	if keys := got.Keys(); len(keys) < 2 || keys[0] != "action" || keys[1] != "session" {
		t.Errorf("request key order = %v, want action, session first", keys)
	}
}

func TestDoDoesNotMutateParams(t *testing.T) {
	params := sproto.NewMap().Set("path", sproto.String("/x"))
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	if _, err := c.do(testContext(t), "list", params); err != nil {
		t.Fatalf("do: %v", err)
	}
	if _, ok := params.Get("action"); ok {
		t.Error("caller params mutated: action injected into the original map")
	}
	if _, ok := params.Get("session"); ok {
		t.Error("caller params mutated: session injected into the original map")
	}
}

func TestDoInjectedFieldsWinOverParams(t *testing.T) {
	var got *sproto.Map
	c := mockClient(t, "real-session", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	params := sproto.NewMap().
		Set("action", sproto.String("evil")).
		Set("session", sproto.String("forged"))
	if _, err := c.do(testContext(t), "list", params); err != nil {
		t.Fatalf("do: %v", err)
	}
	if v, _ := got.Get("action"); v != sproto.String("list") {
		t.Errorf("action = %v, want list (injected, not params)", v)
	}
	if v, _ := got.Get("session"); v != sproto.String("real-session") {
		t.Errorf("session = %v, want real-session (injected, not params)", v)
	}
}

func TestDoErrorCodes(t *testing.T) {
	cases := []struct {
		name    string
		code    int64
		wantErr error
	}{
		{"conflict", CodeConflictVersion, ErrConflictVersion},
		{"path", CodePathError, ErrPathError},
		{"hash", CodeHashMismatch, ErrHashMismatch},
		{"vanished", CodeSessionVanished, auth.ErrSessionVanished},
		// A client_version rejection on an ACTION turn is the same session-level verdict the
		// connect axis stops on, so the turn must surface the shared auth sentinel rather than
		// an anonymous *ServerError the engine would consume per op.
		{"client version", CodeClientVersion, auth.ErrClientVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
				return sproto.NewMap().Set("errno", sproto.Int(tc.code))
			})
			_, err := c.do(testContext(t), "query_node", nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErr)
			}
			if _, unmapped := ServerErrorCode(err); unmapped {
				t.Errorf("code %d surfaced as an unmapped *ServerError; a mapped code must not "+
					"feed the per-op deterministic-rejection escalation", tc.code)
			}
		})
	}
}

func TestDoUnknownErrorCode(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(99999))
	})
	_, err := c.do(testContext(t), "query_node", nil)
	if err == nil {
		t.Fatal("want error for unknown code")
	}
	// An unknown code is not any of the typed sentinels.
	for _, s := range []error{ErrConflictVersion, ErrPathError, ErrHashMismatch, auth.ErrSessionVanished} {
		if errors.Is(err, s) {
			t.Errorf("unknown code matched sentinel %v", s)
		}
	}
}

func TestDoMalformedReplyNotMap(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.String("not a map")
	})
	_, err := c.do(testContext(t), "query_node", nil)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestDoMalformedReplyBadErrno(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.String("oops"))
	})
	_, err := c.do(testContext(t), "query_node", nil)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

// TestNewClientWithLogger pins the logger injection: a supplied logger is
// installed on the Client (so the MD4-skip degradation warnings route through
// the daemon's rotating handler instead of slog.Default() - the routing itself
// is covered by the download/resume no-content-hash tests, which write through
// c.log), and a nil logger falls back to slog.Default().
func TestNewClientWithLogger(t *testing.T) {
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewClientWithLogger(&transport.Session{}, "sess", custom)
	if c.log != custom {
		t.Error("NewClientWithLogger did not install the supplied logger")
	}
	if got := NewClientWithLogger(&transport.Session{}, "sess", nil); got.log != slog.Default() {
		t.Error("NewClientWithLogger(nil) did not fall back to slog.Default()")
	}
	if got := NewClient(&transport.Session{}, "sess"); got.log != slog.Default() {
		t.Error("NewClient did not default the logger to slog.Default()")
	}
}

// TestDoMissingErrnoIsSuccess pins the documented read-action tolerance: do
// itself accepts a reply without an errno, because read/metadata callers decode
// action-specific fields as their positive evidence. The mutating actions
// require an errno on top of do - see TestMutateMissingErrnoRejected.
func TestDoMissingErrnoIsSuccess(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("data", sproto.String("ok"))
	})
	reply, err := c.do(testContext(t), "query_node", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if v, _ := reply.Get("data"); v != sproto.String("ok") {
		t.Errorf("data = %v, want ok", v)
	}
}

func TestDoContextCanceled(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.do(ctx, "query_node", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestDoPoisonsConnAfterInterruptedTurn(t *testing.T) {
	// A turn interrupted mid-I/O (here a cancelled read) leaves the connection
	// byte-misaligned: the abandoned reply may still arrive. The Client must refuse
	// every subsequent turn with ErrConnBroken rather than risk reading those
	// leftover bytes as the next action's reply.
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := c.do(ctx, "query_node", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("first turn err = %v, want context.Canceled", err)
	}
	if _, err := c.do(testContext(t), "query_node", nil); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("second turn err = %v, want ErrConnBroken", err)
	}
}

func TestDoContextDeadline(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.do(ctx, "query_node", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// TestDoRejectsOversizedInlineBinary verifies the general (do) reply path bounds
// inline Binary values: a reply carrying a Binary larger than maxReplyInlineBinary
// is rejected as ErrUnexpectedReply, its bytes are drained rather than buffered,
// and the connection is left aligned and reusable (not marked broken).
func TestDoRejectsOversizedInlineBinary(t *testing.T) {
	const big = maxReplyInlineBinary + 1
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		// Turn 1: a well-formed reply that nonetheless carries an oversized Binary.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		reply := sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("data", sproto.Binary{Data: make([]byte, big)})
		if err := sproto.NewEncoder(server).Encode(reply); err != nil {
			return
		}
		// Turn 2: a normal reply, proving the connection survived turn 1.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("errno", sproto.Int(CodeOK)))
	})
	c := newClient(conn, "sess")

	_, err := c.do(testContext(t), "list", nil)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("do err = %v, want ErrUnexpectedReply", err)
	}
	if c.Broken() {
		t.Fatal("connection marked broken; an oversized-but-drained reply must leave it reusable")
	}
	if _, err := c.do(testContext(t), "list", nil); err != nil {
		t.Fatalf("second turn err = %v, want nil (connection should be reusable)", err)
	}
}

// TestReadCappedReplyDrainsOversizedBinary checks the low-level helper: an
// oversized Binary surfaces as oversize=true with its Data left nil (never
// buffered), while the rest of the reply still decodes.
func TestReadCappedReplyDrainsOversizedBinary(t *testing.T) {
	const big = maxReplyInlineBinary + 1
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		reply := sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("data", sproto.Binary{Data: make([]byte, big)})
		_ = sproto.NewEncoder(server).Encode(reply)
	})
	c := newClient(conn, "sess")

	resp, oversize, stalled, err := c.readCappedReplyIdle(actionIdleTimeout)
	if err != nil {
		t.Fatalf("readCappedReplyIdle err = %v, want nil", err)
	}
	if stalled {
		t.Fatal("stalled = true on a reply that arrived promptly")
	}
	if !oversize {
		t.Fatal("oversize = false, want true for a Binary past the cap")
	}
	m, ok := resp.(*sproto.Map)
	if !ok {
		t.Fatalf("resp is %T, want *sproto.Map", resp)
	}
	v, _ := m.Get("data")
	b, ok := v.(sproto.Binary)
	if !ok {
		t.Fatalf("data is %T, want sproto.Binary", v)
	}
	if b.Data != nil {
		t.Errorf("oversized Binary.Data buffered %d bytes, want nil (drained)", len(b.Data))
	}
	if b.Length != big {
		t.Errorf("Binary.Length = %d, want %d", b.Length, big)
	}
}

// TestDoAllowsSmallInlineBinary is the regression guard: a reply with a Binary at
// or below the cap is decoded normally (not rejected, not drained).
func TestDoAllowsSmallInlineBinary(t *testing.T) {
	payload := []byte("small inline payload")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("data", sproto.Binary{Data: payload})
	})
	reply, err := c.do(testContext(t), "list", nil)
	if err != nil {
		t.Fatalf("do err = %v, want nil", err)
	}
	v, _ := reply.Get("data")
	b, ok := v.(sproto.Binary)
	if !ok {
		t.Fatalf("data is %T, want sproto.Binary", v)
	}
	if string(b.Data) != string(payload) {
		t.Errorf("data = %q, want %q", b.Data, payload)
	}
}

// TestKeepAliveAheadOfReplyIsSkipped pins the fix for the intermittent large-upload
// failure. The server interleaves unsolicited {"@proto":…, "type":"keep_alive"}
// frames onto an established connection at its own cadence; a reply read that does
// not step over them takes one as the answer - it decodes cleanly, carries no result
// verdict, and poisons the connection. It was never upload-specific: any turn whose
// reply a keep-alive happens to precede would have taken it.
func TestKeepAliveAheadOfReplyIsSkipped(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d keep-alives", n), func(t *testing.T) {
			conn := serveMock(t, func(t *testing.T, server net.Conn) {
				if _, err := sproto.ReadHeader(server); err != nil {
					return
				}
				if _, err := sproto.NewDecoder(server).Decode(); err != nil {
					return
				}
				enc := sproto.NewEncoder(server)
				for i := 0; i < n; i++ {
					_ = enc.Encode(sproto.NewMap().
						Set("@proto", sproto.NewMap().Set("type", sproto.String("header"))).
						Set("type", sproto.String("keep_alive")))
				}
				_ = enc.Encode(sproto.NewMap().
					Set("errno", sproto.Int(CodeOK)).
					Set("file_id", sproto.String("real")))
			})
			c := newClient(conn, "sess")
			reply, err := c.do(testContext(t), "query_node", nil)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			if v, _ := reply.Get("file_id"); v != sproto.String("real") {
				t.Errorf("reply = %v, want the real reply behind the keep-alives", reply.Keys())
			}
		})
	}
}

// TestKeepAliveFloodIsBounded confirms the skip cannot spin: a peer that only ever
// sends keep-alives is rejected as a malformed reply rather than looping forever.
//
// Termination is only half of it, and the other half is what makes the bound safe. When the
// budget runs out the turn's real reply has NOT been read and the server still owes it, so the
// session is one reply behind permanently. Every frame decoded cleanly, so nothing else marks it
// bad: the turn has to poison the connection itself, or the pool hands the desynced session to
// the next turn, which reads the PREVIOUS turn's reply - and a live DSM reports success as a bare
// `type:"response"` frame, which errno() accepts as a full verdict. A stale frame would then
// stand as commit evidence for a mutation it never answered. Asserting only that the read
// terminated is how that survived review.
func TestKeepAliveFloodIsBounded(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		enc := sproto.NewEncoder(server)
		for i := 0; i < maxSkippedKeepAlives+5; i++ {
			if err := enc.Encode(sproto.NewMap().Set("type", sproto.String("keep_alive"))); err != nil {
				return
			}
		}
	})
	c := newClient(conn, "sess")
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = c.doMutate(context.Background(), "remove", nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a keep-alive flood spun the reply read instead of being bounded")
	}
	if err == nil {
		t.Fatal("a keep-alive flood was accepted as a mutation reply; a frame carrying no verdict " +
			"must never be read as one")
	}
	if !c.Broken() {
		t.Errorf("after a keep-alive flood Broken() = false, want true: the turn's real reply was "+
			"never read, so the session is one reply behind for good. Released to the pool it "+
			"desyncs every later turn, and the next one reads this turn's reply - a bare "+
			"type:\"response\" frame, which errno() accepts as success. err = %v", err)
	}
}

// TestSingleFrameReplyNotMisread guards the other direction: an ordinary reply must
// never trigger a second read, which would block until the turn's budget expired.
func TestSingleFrameReplyNotMisread(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("type", sproto.String("response")).
			Set("@proto", sproto.NewMap().Set("body-continue", sproto.Int(0))).
			Set("file_id", sproto.String("single"))
	})
	reply, err := c.do(testContext(t), "query_node", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if v, _ := reply.Get("file_id"); v != sproto.String("single") {
		t.Errorf("reply = %v, want the single frame", reply.Keys())
	}
}

// TestPoisoningTurnCarriesTheBrokenSentinel pins the half of the contract above that used to be
// missing: the turn that BREAKS the connection reports it too, not just the turns after it.
//
// Only the subsequent turns matched ErrConnBroken (the c.broken refusal), so the first mid-I/O
// failure - an EOF, a reset, the interrupted read here - was indistinguishable from an ordinary
// failure to callers that classify by error. engine.isTerminalRefreshError missed it and needed a
// direct Broken() check to compensate; engine.unprovedByTransport inherited the same blind spot
// WITHOUT one and folded a network outage into the synthetic removeDirUnverifiedCode streak,
// which is not a transient class - so a flaky link dead-lettered the user's directory delete and
// left a StateConflict record localReconcile never drives again.
//
// The cause stays wrapped alongside the sentinel: both must remain matchable, since the ctx error
// is what the loop reacts to for a cancellation.
func TestPoisoningTurnCarriesTheBrokenSentinel(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.do(ctx, "query_node", nil)
	if !errors.Is(err, ErrConnBroken) {
		t.Errorf("poisoning turn err = %v, want it to match ErrConnBroken", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("poisoning turn err = %v, want it to still match the cause (context.Canceled)", err)
	}
}

// TestOversizedKeepAlivePoisonsTheConnection closes the one arm the keep-alive poison guard would
// otherwise leave open, and it needs no flood at all.
//
// A keep-alive frame can also carry an oversized inline Binary. Its bytes are drained, so the
// connection stays byte-aligned and the ordinary oversize arm deliberately does NOT mark the
// client broken - alignment is intact. But alignment was never the question: the frame is still
// not this turn's reply, and the server still owes it, so releasing the session to the pool leaves
// every later turn one reply behind. The next turn then reads THIS turn's reply, and a live DSM
// states success as a bare type:"response" frame, which errno() accepts as a full verdict - a
// stale frame standing as commit evidence for a mutation it never answered.
//
// Stepping over it (rather than stopping on it) is what the fix does, so a single such frame ahead
// of a real reply costs nothing; a peer that sends only these hits the skip bound and poisons.
func TestOversizedKeepAlivePoisonsTheConnection(t *testing.T) {
	const big = maxReplyInlineBinary + 1
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		enc := sproto.NewEncoder(server)
		// Nothing but oversized keep-alives: the turn's real reply never arrives.
		for i := 0; i < maxSkippedKeepAlives+3; i++ {
			frame := sproto.NewMap().
				Set("type", sproto.String("keep_alive")).
				Set("pad", sproto.Binary{Data: make([]byte, big)})
			if err := enc.Encode(frame); err != nil {
				return
			}
		}
	})
	c := newClient(conn, "sess")

	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = c.doMutate(context.Background(), "remove", nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("an oversized keep-alive spun the reply read instead of being bounded")
	}
	if err == nil {
		t.Fatal("an oversized keep-alive was accepted as a mutation reply")
	}
	if !c.Broken() {
		t.Errorf("after oversized keep-alives Broken() = false, want true: the turn's real reply was "+
			"never read, so the session is one reply behind and must not go back to the pool. "+
			"err = %v", err)
	}
}
