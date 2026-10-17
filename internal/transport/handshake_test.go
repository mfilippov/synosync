package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestTestConn(t *testing.T) {
	s := newMockServer(t)
	reply, err := TestConn(testContext(t), s.addr())
	if err != nil {
		t.Fatalf("TestConn: %v", err)
	}
	if reply.DsID != testDsID {
		t.Errorf("DsID = %q, want %q", reply.DsID, testDsID)
	}
	if reply.ProtoVersion != 70 {
		t.Errorf("ProtoVersion = %d, want 70", reply.ProtoVersion)
	}
}

func TestTestConnDialError(t *testing.T) {
	// Port 0 on the loopback address is not connectable.
	if _, err := TestConn(testContext(t), "127.0.0.1:0"); err == nil {
		t.Fatal("TestConn to an unreachable address: want error, got nil")
	}
}

// TestTestConnReplyTooLarge ensures an unauthenticated endpoint that streams more
// than the cap is rejected rather than buffered without bound.
func TestTestConnReplyTooLarge(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Drain the request header, then flood the connection past the cap.
		buf := make([]byte, 8)
		_, _ = io.ReadFull(conn, buf)
		junk := make([]byte, maxTestConnReply+1024)
		_, _ = conn.Write(junk)
	}()

	_, err = TestConn(testContext(t), ln.Addr().String())
	if err == nil {
		t.Fatal("TestConn with an over-long reply: want error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %v, want it to mention the size cap", err)
	}
}

// TestTestConnContextCancel ensures cancelling a deadline-less context unblocks a
// read that the server never answers.
func TestTestConnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read the request but never reply, leaving the client's read blocked.
		buf := make([]byte, 8)
		_, _ = io.ReadFull(conn, buf)
		<-t.Context().Done()
		_ = conn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	done := make(chan error, 1)
	go func() {
		_, err := TestConn(ctx, ln.Addr().String())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TestConn after cancel: error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TestConn did not return after context cancellation")
	}
}

// TestTestConnDeadlineExpiry ensures that when a context deadline elapses while a
// read is blocked, the error maps to context.DeadlineExceeded rather than the raw
// socket I/O timeout - even though the socket deadline is what physically fires.
func TestTestConnDeadlineExpiry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read the request but never reply, leaving the client's read blocked
		// until its context deadline expires.
		buf := make([]byte, 8)
		_, _ = io.ReadFull(conn, buf)
		<-t.Context().Done()
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := TestConn(ctx, ln.Addr().String())
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("TestConn after deadline: error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TestConn did not return after the deadline expired")
	}
}

// TestEncryptChannelReplyTooLarge ensures the encrypt_channel reply - read from
// an unauthenticated peer before the TLS upgrade - is rejected once it streams
// past the cap, rather than buffered without bound.
func TestEncryptChannelReplyTooLarge(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Send a Binary (0x30) whose declared length is larger than the cap and
		// stream its payload. The decoder keeps reading the payload until the cap
		// fires, exercising the bound.
		var reply [9]byte
		reply[0] = 0x30 // tagBinary
		binary.BigEndian.PutUint64(reply[1:], uint64(maxEncryptChannelReply+1024))
		_, _ = conn.Write(reply[:])
		_, _ = conn.Write(make([]byte, maxEncryptChannelReply+1024))
		// Keep draining the client's request until it gives up and closes the
		// socket. Closing with the request body still unread would RST the client's
		// in-progress write, surfacing as "connection reset by peer" instead of the
		// size-cap rejection this test asserts.
		_, _ = io.Copy(io.Discard, conn)
	}()

	_, err = Connect(testContext(t), ConnectConfig{Addr: ln.Addr().String(), DsID: testDsID})
	if err == nil {
		t.Fatal("Connect with an over-long encrypt_channel reply: want error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %v, want it to mention the size cap", err)
	}
}

// TestEncryptChannelMissingAlive ensures the handshake fails loudly when the
// encrypt_channel reply omits the integer "alive" - a non-conforming (or hostile)
// endpoint - rather than silently defaulting Session.Alive to 0.
func TestEncryptChannelMissingAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Reply with a well-formed bare PObject map that lacks the "alive" key, then
		// drain the client's request so its write never resets (mirroring the
		// over-long-reply test's ordering).
		resp := sproto.NewMap().Set("status", sproto.Int(0))
		if err := sproto.NewEncoder(conn).Encode(resp); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()

	_, err = Connect(testContext(t), ConnectConfig{Addr: ln.Addr().String(), DsID: testDsID})
	if err == nil {
		t.Fatal("Connect with an encrypt_channel reply missing alive: want error, got nil")
	}
	if !strings.Contains(err.Error(), "alive") {
		t.Errorf("error = %v, want it to mention the missing alive value", err)
	}
}

func TestEncryptChannelRequest(t *testing.T) {
	agent := sproto.NewMap().Set("type", sproto.String("desktop"))
	req := EncryptChannelRequest(agent, testDsID)

	if _, ok := req.Get("agent"); !ok {
		t.Error("request missing agent")
	}
	sid, ok := req.Get("server_id")
	if !ok {
		t.Fatal("request missing server_id")
	}
	sm, ok := sid.(*sproto.Map)
	if !ok {
		t.Fatalf("server_id is %T, want *Map", sid)
	}
	if v, _ := sm.Get("restore_id"); v != sproto.String(testDsID) {
		t.Errorf("restore_id = %v, want %q", v, testDsID)
	}

	// A nil agent is simply omitted rather than encoded as a nil value.
	if _, ok := EncryptChannelRequest(nil, testDsID).Get("agent"); ok {
		t.Error("nil agent should be omitted from the request")
	}
}

// TestConnectHandshake walks the full TCP → encrypt_channel → TLS sequence in
// capture (pairing) mode, then issues an action over the encrypted channel.
func TestConnectHandshake(t *testing.T) {
	s := newMockServer(t)
	agent := sproto.NewMap().Set("type", sproto.String("desktop"))
	sess, err := Connect(testContext(t), ConnectConfig{
		Addr:  s.addr(),
		Agent: agent,
		DsID:  testDsID,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer sess.Close()

	// The restore id reached the server (the handshake's own "alive" validation is
	// covered by TestConnectEncryptChannelMissingAlive). Asserted on the wire rather
	// than on the Session, which no longer keeps write-only handshake state.
	if got := s.lastRestoreID(); got != testDsID {
		t.Errorf("server saw restore_id = %q, want %q", got, testDsID)
	}
	if want := s.leafSPKIPin(t); sess.Pin() != want {
		t.Errorf("captured Pin = %q, want SPKI %q", sess.Pin(), want)
	}

	// A request now round-trips over the pinned TLS channel.
	if err := sess.Conn().WriteRequest(sproto.CmdAction, sproto.NewMap().Set("action", sproto.String("hello"))); err != nil {
		t.Fatalf("WriteRequest over TLS: %v", err)
	}
	resp, err := sess.Conn().ReadResponse()
	if err != nil {
		t.Fatalf("ReadResponse over TLS: %v", err)
	}
	m, ok := resp.(*sproto.Map)
	if !ok {
		t.Fatalf("response is %T, want *Map", resp)
	}
	if v, _ := m.Get("echo"); v != sproto.String("hello") {
		t.Errorf("echo = %v, want hello", v)
	}
}

// TestConnectStalledTLSHandshake confirms that when the TLS handshake stalls past
// the context deadline, Connect surfaces context.DeadlineExceeded rather than a
// raw socket I/O timeout - the socket deadline that watchContext stamps from ctx
// can fire microseconds before ctx.Done(), so the error must be mapped through
// contextErr.
func TestConnectStalledTLSHandshake(t *testing.T) {
	addr := newStalledTLSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := Connect(ctx, ConnectConfig{Addr: addr, DsID: testDsID, Pin: "sha256:0000"})
	if err == nil {
		t.Fatal("Connect with stalled TLS handshake: want error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
}

// TestConnectPinMatch pairs to capture the pin, then reconnects in verify mode
// with that pin and expects success.
func TestConnectPinMatch(t *testing.T) {
	s := newMockServer(t)
	sess, err := Connect(testContext(t), ConnectConfig{Addr: s.addr(), DsID: testDsID, Pin: s.leafPin()})
	if err != nil {
		t.Fatalf("Connect with matching pin: %v", err)
	}
	defer sess.Close()
	if sess.Pin() != s.leafPin() {
		t.Errorf("Pin = %q, want %q", sess.Pin(), s.leafPin())
	}
}

// TestConnectCaptureSPKIPin confirms that in capture (pairing) mode both Pin and
// SPKIPin report the SPKI fingerprint of the presented leaf.
func TestConnectCaptureSPKIPin(t *testing.T) {
	s := newMockServer(t)
	sess, err := Connect(testContext(t), ConnectConfig{Addr: s.addr(), DsID: testDsID})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer sess.Close()
	want := s.leafSPKIPin(t)
	if sess.Pin() != want {
		t.Errorf("Pin = %q, want SPKI %q", sess.Pin(), want)
	}
	if sess.SPKIPin() != want {
		t.Errorf("SPKIPin = %q, want %q", sess.SPKIPin(), want)
	}
}

// TestConnectVerifyLegacyPinExposesSPKI confirms that when connecting in verify
// mode with a legacy full-DER pin, Pin reports the legacy pin in force while
// SPKIPin reports the SPKI fingerprint of the same presented leaf - the seam the
// caller uses to migrate a stored legacy pin to the SPKI form.
func TestConnectVerifyLegacyPinExposesSPKI(t *testing.T) {
	s := newMockServer(t)
	sess, err := Connect(testContext(t), ConnectConfig{Addr: s.addr(), DsID: testDsID, Pin: s.leafPin()})
	if err != nil {
		t.Fatalf("Connect with legacy pin: %v", err)
	}
	defer sess.Close()
	if sess.Pin() != s.leafPin() {
		t.Errorf("Pin = %q, want legacy %q", sess.Pin(), s.leafPin())
	}
	if want := s.leafSPKIPin(t); sess.SPKIPin() != want {
		t.Errorf("SPKIPin = %q, want %q", sess.SPKIPin(), want)
	}
}

// TestConnectPinMismatch confirms a wrong pin (a stand-in for a MITM cert) aborts
// the upgrade with ErrPinMismatch.
func TestConnectPinMismatch(t *testing.T) {
	s := newMockServer(t)
	_, err := Connect(testContext(t), ConnectConfig{
		Addr: s.addr(),
		DsID: testDsID,
		Pin:  "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err == nil {
		t.Fatal("Connect with mismatched pin: want error, got nil")
	}
	if !errors.Is(err, ErrPinMismatch) {
		t.Errorf("error = %v, want ErrPinMismatch", err)
	}
}
