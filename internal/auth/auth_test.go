package auth

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

// testClient is a fixed ClientInfo used across the auth tests.
var testClient = ClientInfo{Platform: "linux", Type: "desktop", Major: 3, Mini: 4, Minor: 1, Build: 25556}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// serverFunc handles one request from the mock server: it receives the decoded
// request Map and returns the reply Value to send back as a bare PObject.
type serverFunc func(t *testing.T, cmd uint8, req *sproto.Map) sproto.Value

// dialMock wires a *sproto.Conn to an in-memory mock server that answers a single
// request via handle. The request header command and decoded body are passed to
// handle; whatever it returns is encoded back as the bare reply.
func dialMock(t *testing.T, handle serverFunc) *sproto.Conn {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		h, err := sproto.ReadHeader(server)
		if err != nil {
			return
		}
		v, err := sproto.NewDecoder(server).Decode()
		if err != nil {
			return
		}
		m, _ := v.(*sproto.Map)
		reply := handle(t, h.Cmd, m)
		_ = sproto.NewEncoder(server).Encode(reply)
	}()
	return sproto.NewConn(client)
}

// TestClientVersion pins the MAJOR.MINOR.MINI-BUILD order the server parses. The
// real client announces agent.version {major:4, minor:0, mini:2} as "4.0.2-…",
// so the string is major.minor.mini - not major.mini.minor.
func TestClientVersion(t *testing.T) {
	if got := testClient.ClientVersion(); got != "3.1.4-25556" {
		t.Errorf("ClientVersion() = %q, want %q", got, "3.1.4-25556")
	}
}

func TestAgent(t *testing.T) {
	agent := testClient.Agent("0123456789abcdef0123456789abcdef")
	want := map[string]string{
		"platform":    "linux",
		"type":        "desktop",
		"device_uuid": "0123456789abcdef0123456789abcdef",
	}
	for k, w := range want {
		v, ok := agent.Get(k)
		if !ok {
			t.Errorf("agent missing %q", k)
			continue
		}
		if s, _ := v.(sproto.String); string(s) != w {
			t.Errorf("agent[%q] = %v, want %q", k, v, w)
		}
	}
	// version is a nested numeric MAP, not the dotted string: a live DSM drops the
	// connection during encrypt_channel when it is sent as a String.
	raw, ok := agent.Get("version")
	if !ok {
		t.Fatal("agent missing version")
	}
	vm, ok := raw.(*sproto.Map)
	if !ok {
		t.Fatalf("agent[version] is %T, want map", raw)
	}
	for k, w := range map[string]int64{"build": 25556, "major": 3, "mini": 4, "minor": 1} {
		if got, ok := vm.Int(k); !ok || got != w {
			t.Errorf("agent.version[%q] = %v (present %v), want %d", k, got, ok, w)
		}
	}
}

func TestNewDeviceUUID(t *testing.T) {
	hexRe := regexp.MustCompile(`^[0-9a-f]{32}$`)
	a, err := NewDeviceUUID()
	if err != nil {
		t.Fatalf("NewDeviceUUID: %v", err)
	}
	if !hexRe.MatchString(a) {
		t.Errorf("device_uuid %q is not 32 lowercase-hex chars", a)
	}
	b, err := NewDeviceUUID()
	if err != nil {
		t.Fatalf("NewDeviceUUID: %v", err)
	}
	if a == b {
		t.Error("two NewDeviceUUID calls returned the same value")
	}
}

func TestPairSuccess(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	conn := dialMock(t, func(t *testing.T, cmd uint8, req *sproto.Map) sproto.Value {
		if cmd != sproto.CmdAuth {
			t.Errorf("cmd = 0x%02x, want CmdAuth", cmd)
		}
		// The request must carry the credentials and client identity.
		for _, k := range []string{"username", "password", "device_uuid", "agent", "client", "client_version"} {
			if _, ok := req.Get(k); !ok {
				t.Errorf("auth request missing %q", k)
			}
		}
		if v, _ := req.Get("client_version"); v != sproto.String("3.1.4-25556") {
			t.Errorf("client_version = %v, want 3.1.4-25556", v)
		}
		// No OTP supplied, so the field must be absent.
		if _, ok := req.Get("otp"); ok {
			t.Error("otp present though no OTP was supplied")
		}
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).Set("session", sproto.String(wantSession))
	})
	creds := Credentials{User: "alice", Password: "s3cret"}
	got, err := Pair(testContext(t), conn, testClient, "0123456789abcdef0123456789abcdef", "ds1", creds)
	if err != nil {
		t.Fatalf("Pair: %v", err)
	}
	if got != wantSession {
		t.Errorf("session = %q, want %q", got, wantSession)
	}
}

func TestPairSendsOTP(t *testing.T) {
	conn := dialMock(t, func(t *testing.T, _ uint8, req *sproto.Map) sproto.Value {
		if v, _ := req.Get("otp"); v != sproto.String("654321") {
			t.Errorf("otp = %v, want 654321", v)
		}
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).Set("session", sproto.String("11112222333344445555666677778888"))
	})
	creds := Credentials{User: "alice", Password: "s3cret", OTP: "654321"}
	if _, err := Pair(testContext(t), conn, testClient, "dev", "ds1", creds); err != nil {
		t.Fatalf("Pair: %v", err)
	}
}

func TestPairErrorCodes(t *testing.T) {
	tests := []struct {
		name string
		code int64
		want error
	}{
		{"user not found", CodeUserNotFound, ErrUserNotFound},
		{"invalid password", CodeInvalidPassword, ErrInvalidPassword},
		{"otp required", CodeOTPRequired, ErrOTPRequired},
		{"otp invalid", CodeOTPInvalid, ErrOTPInvalid},
		{"client version", CodeClientVersion, ErrClientVersion},
		{"session vanished", CodeSessionVanished, ErrSessionVanished},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
				return sproto.NewMap().Set("errno", sproto.Int(tt.code))
			})
			_, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"})
			if !errors.Is(err, tt.want) {
				t.Errorf("Pair error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestPairUnknownErrorCode(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(9999))
	})
	_, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"})
	if err == nil {
		t.Fatal("Pair with unknown error code: want error, got nil")
	}
	// An unknown code matches none of the typed sentinels.
	for _, s := range []error{ErrUserNotFound, ErrInvalidPassword, ErrOTPRequired, ErrOTPInvalid, ErrClientVersion, ErrSessionVanished} {
		if errors.Is(err, s) {
			t.Errorf("unknown code matched sentinel %v", s)
		}
	}
}

// TestPairErrorCarriesServerReason pins that the nested error block's "reason" -
// the server's own words - survives into the returned error.
//
// It is the only evidence available about a verdict this client has never seen,
// and dropping it cost a whole debugging detour: pairing against a live stand
// began failing with a bare "error code 4100", the code matched no sentinel, and
// a deliberately wrong password drew the SAME bare number - which read as
// "credential-independent, therefore a server-state problem" and sent the
// investigation after auto-block and device ceilings. The reason string said
// "Invalid password" and ended it immediately. A mapped code carries it too: a
// live DSM can answer a known code for an unexpected reason.
func TestPairErrorCarriesServerReason(t *testing.T) {
	const reason = "Invalid password"
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("error", sproto.NewMap().
			Set("code", sproto.Int(CodeInvalidPassword)).
			Set("reason", sproto.String(reason)))
	})
	_, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"})
	if !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("Pair error = %v, want ErrInvalidPassword", err)
	}
	if !strings.Contains(err.Error(), reason) {
		t.Errorf("Pair error %q does not carry the server reason %q", err, reason)
	}
}

// TestPairErrorWithoutReason pins the other half: a reply whose error block omits
// "reason" (or types it wrongly) is still a decodable verdict, not a protocol
// failure - the text is optional diagnostics, and refusing the reply over it would
// turn a usable verdict into an unusable one.
func TestPairErrorWithoutReason(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block *sproto.Map
	}{
		{"absent", sproto.NewMap().Set("code", sproto.Int(CodeInvalidPassword))},
		{"wrong type", sproto.NewMap().
			Set("code", sproto.Int(CodeInvalidPassword)).
			Set("reason", sproto.Int(7))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
				return sproto.NewMap().Set("error", tc.block)
			})
			_, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"})
			if !errors.Is(err, ErrInvalidPassword) {
				t.Fatalf("Pair error = %v, want ErrInvalidPassword", err)
			}
			if strings.Contains(err.Error(), "server reason") {
				t.Errorf("Pair error %q claims a server reason it never received", err)
			}
		})
	}
}

func TestPairNoSession(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	_, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"})
	if !errors.Is(err, ErrNoSession) {
		t.Errorf("Pair error = %v, want ErrNoSession", err)
	}
}

// TestPairRejectsOversizedInlineBinary verifies the auth reply path bounds inline
// Binary values: a pairing reply smuggling a Binary past maxAuthReplyInlineBinary
// is rejected without buffering the whole value.
func TestPairRejectsOversizedInlineBinary(t *testing.T) {
	const big = maxAuthReplyInlineBinary + 1
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("session", sproto.String("abcdef0123456789abcdef0123456789")).
			Set("blob", sproto.Binary{Data: make([]byte, big)})
	})
	creds := Credentials{User: "alice", Password: "s3cret"}
	_, err := Pair(testContext(t), conn, testClient, "0123456789abcdef0123456789abcdef", "ds1", creds)
	if err == nil || !strings.Contains(err.Error(), "oversized inline binary") {
		t.Fatalf("Pair err = %v, want oversized inline binary rejection", err)
	}
}

// TestReuseRejectsOversizedInlineBinary verifies the reuse reply path bounds
// inline Binary values the same way.
func TestReuseRejectsOversizedInlineBinary(t *testing.T) {
	const big = maxAuthReplyInlineBinary + 1
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("blob", sproto.Binary{Data: make([]byte, big)})
	})
	err := Reuse(testContext(t), conn, "abcdef0123456789abcdef0123456789", nil, "ds1")
	if err == nil || !strings.Contains(err.Error(), "oversized inline binary") {
		t.Fatalf("Reuse err = %v, want oversized inline binary rejection", err)
	}
}

func TestReuseSuccess(t *testing.T) {
	const session = "abcdef0123456789abcdef0123456789"
	conn := dialMock(t, func(t *testing.T, cmd uint8, req *sproto.Map) sproto.Value {
		if cmd != sproto.CmdAction {
			t.Errorf("cmd = 0x%02x, want CmdAction", cmd)
		}
		if v, _ := req.Get("action"); v != sproto.String("query_user_info") {
			t.Errorf("action = %v, want query_user_info", v)
		}
		if v, _ := req.Get("session"); v != sproto.String(session) {
			t.Errorf("session = %v, want %q", v, session)
		}
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).Set("user", sproto.String("alice"))
	})
	if err := Reuse(testContext(t), conn, session, nil, "ds1"); err != nil {
		t.Fatalf("Reuse: %v", err)
	}
}

func TestReuseSessionVanished(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeSessionVanished))
	})
	err := Reuse(testContext(t), conn, "stale", nil, "ds1")
	if !errors.Is(err, ErrSessionVanished) {
		t.Errorf("Reuse error = %v, want ErrSessionVanished", err)
	}
}

// TestReuseContextCancel ensures cancelling a deadline-less context unblocks a
// reuse turn whose reply never arrives.
func TestReuseContextCancel(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		// Read the request but never reply, leaving the client's read blocked.
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		<-t.Context().Done()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	done := make(chan error, 1)
	go func() { done <- Reuse(ctx, sproto.NewConn(client), "session", nil, "ds1") }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Reuse after cancel: error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reuse did not return after context cancellation")
	}
}

// TestReuseDeadlineExpiry ensures that when a context deadline elapses while the
// reply is awaited, the error maps to context.DeadlineExceeded rather than the
// raw socket I/O timeout produced by the derived socket deadline.
func TestReuseDeadlineExpiry(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		// Read the request but never reply, leaving the client's read blocked
		// until its context deadline expires.
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		<-t.Context().Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Reuse(ctx, sproto.NewConn(client), "session", nil, "ds1") }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Reuse after deadline: error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reuse did not return after the deadline expired")
	}
}

// TestReuseMissingErrnoRejected ensures a reply map carrying neither an errno nor
// a view_list is a protocol error rather than a silent success: unlike Pair,
// whose session token is positive proof, a reuse reply with no success evidence
// (errno or view_list) must not confirm the token as valid.
func TestReuseMissingErrnoRejected(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("user", sproto.String("alice"))
	})
	err := Reuse(testContext(t), conn, "session", nil, "ds1")
	if err == nil || !strings.Contains(err.Error(), "neither errno nor view_list") {
		t.Fatalf("Reuse err = %v, want neither-errno-nor-view_list rejection", err)
	}
}

// TestReuseViewListNoErrno ensures a reuse reply carrying a present view_list
// array but NO errno is accepted as success: reuse presents
// exactly the query_user_info action, whose successful reply legitimately rides
// the views under "view_list" without an errno, so a DSM answering that way must
// not wedge the daemon in an endless reconnect ("connecting..." forever). Both an
// empty and a populated list count as evidence.
func TestReuseViewListNoErrno(t *testing.T) {
	tests := []struct {
		name string
		list sproto.Array
	}{
		{"empty", sproto.Array{}},
		{"populated", sproto.Array{sproto.NewMap().Set("file_id", sproto.String("42"))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
				return sproto.NewMap().Set("view_list", tt.list)
			})
			if err := Reuse(testContext(t), conn, "session", nil, "ds1"); err != nil {
				t.Fatalf("Reuse with view_list and no errno: %v", err)
			}
		})
	}
}

// TestReuseWrongTypedViewList ensures a present-but-wrong-typed view_list (not an
// array) is rejected rather than mistaken for success - it is a half-formed reply,
// so the token is not confirmed (mirroring actions.QueryUserInfo).
func TestReuseWrongTypedViewList(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("view_list", sproto.String("nope"))
	})
	err := Reuse(testContext(t), conn, "session", nil, "ds1")
	if err == nil || !strings.Contains(err.Error(), "view_list is") {
		t.Fatalf("Reuse err = %v, want wrong-typed view_list rejection", err)
	}
}

// TestReuseErrnoWinsOverViewList ensures a present errno is authoritative even
// when a view_list also rides along: a non-OK errno maps to its typed error
// (here 8193 → ErrSessionVanished) rather than the view_list reading as success.
func TestReuseErrnoWinsOverViewList(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeSessionVanished)).
			Set("view_list", sproto.Array{})
	})
	err := Reuse(testContext(t), conn, "stale", nil, "ds1")
	if !errors.Is(err, ErrSessionVanished) {
		t.Errorf("Reuse error = %v, want ErrSessionVanished", err)
	}
}

// TestReuseWrongTypedErrno ensures a present-but-wrong-typed errno is a protocol
// error rather than being mistaken for success.
func TestReuseWrongTypedErrno(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		// errno as a string (e.g. "8193") must not read as success.
		return sproto.NewMap().Set("errno", sproto.String("8193"))
	})
	if err := Reuse(testContext(t), conn, "session", nil, "ds1"); err == nil {
		t.Fatal("Reuse with string errno: want error, got nil")
	}
}

// TestPairWrongTypedErrno ensures Pair rejects a non-integer errno instead of
// proceeding to read the session.
func TestPairWrongTypedErrno(t *testing.T) {
	conn := dialMock(t, func(_ *testing.T, _ uint8, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.String("0")).Set("session", sproto.String("x"))
	})
	if _, err := Pair(testContext(t), conn, testClient, "dev", "ds1", Credentials{User: "a", Password: "b"}); err == nil {
		t.Fatal("Pair with string errno: want error, got nil")
	}
}
