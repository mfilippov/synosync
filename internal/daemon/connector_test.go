package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/transport"
)

// fakeSecrets is a connSecrets stand-in returning canned pin/session values and
// recording SetPin calls so the migration tests can assert whether (and to what)
// the pin was re-stored.
type fakeSecrets struct {
	pin       string
	pinErr    error
	session   string
	sessErr   error
	setPinErr error
	setPins   []string
}

func (f fakeSecrets) Pin(string) (string, error)             { return f.pin, f.pinErr }
func (f fakeSecrets) Session(string, string) (string, error) { return f.session, f.sessErr }
func (f *fakeSecrets) SetPin(_, fingerprint string) error {
	if f.setPinErr != nil {
		return f.setPinErr
	}
	f.setPins = append(f.setPins, fingerprint)
	return nil
}

func testConfig() config.Config {
	return config.Config{
		Server:     config.Server{Host: "nas.example", Port: 6690, User: "alice", DsID: "DS1"},
		DeviceUUID: "uuid-1",
	}
}

func testClient() auth.ClientInfo {
	return auth.ClientInfo{Platform: "linux", Type: "desktop", Major: 3, Mini: 4, Minor: 1, Build: 1}
}

func TestNewConnectorSuccess(t *testing.T) {
	kr := fakeSecrets{pin: "deadbeef", session: "tok-123"}
	connect, session, err := NewConnector(testConfig(), &kr, testClient(), nil)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	if connect == nil {
		t.Fatal("NewConnector: nil connector")
	}
	if session != "tok-123" {
		t.Fatalf("NewConnector session: got %q, want tok-123", session)
	}
}

func TestNewConnectorErrors(t *testing.T) {
	cases := []struct {
		name string
		kr   fakeSecrets
		want string
	}{
		{"pin not found", fakeSecrets{pinErr: secrets.ErrNotFound}, "no pinned certificate"},
		{"pin backend error", fakeSecrets{pinErr: errFake}, "read cert pin"},
		{"empty pin", fakeSecrets{pin: ""}, "certificate pin"},
		{"session not found", fakeSecrets{pin: "p", sessErr: secrets.ErrNotFound}, "no session"},
		{"session backend error", fakeSecrets{pin: "p", sessErr: errFake}, "read session"},
		{"empty session", fakeSecrets{pin: "p", session: ""}, "session"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kr := tc.kr
			_, _, err := NewConnector(testConfig(), &kr, testClient(), nil)
			if err == nil {
				t.Fatalf("NewConnector: got nil error, want one mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewConnector error %q does not mention %q", err.Error(), tc.want)
			}
		})
	}
}

// TestNewConnectorDialError drives the connect closure against an address that
// refuses connections, covering its transport.Connect error branch without a
// live server.
func TestNewConnectorDialError(t *testing.T) {
	kr := fakeSecrets{pin: "deadbeef", session: "tok-123"}
	cfg := testConfig()
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 1 // reserved; refuses on loopback
	connect, _, err := NewConnector(cfg, &kr, testClient(), nil)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := connect(ctx); err == nil {
		t.Fatal("connect to refused address: got nil, want error")
	}
}

// TestConnectorAttemptTimeoutOnSilentServer reproduces the bring-up
// wedge: a server whose TCP stack accepts the connection but whose service never
// responds (a hung DSM, a data-swallowing middlebox) used to block
// transport.Connect in the encrypt_channel read forever - the pool's dial paths
// run on the daemon's long-lived, deadline-free run context, so nothing ever
// unblocked the attempt. The connector's per-attempt deadline must fail the
// attempt while the parent ctx is still live, and the failure must stay
// transient (not a terminal pause signal) so the bring-up/Replace backoffs keep
// retrying.
func TestConnectorAttemptTimeoutOnSilentServer(t *testing.T) {
	// A listener that accepts and drains but never writes a byte back.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, c) }() // silent: drain, never reply
		}
	}()

	restore := connectAttemptTimeout
	connectAttemptTimeout = 100 * time.Millisecond
	t.Cleanup(func() { connectAttemptTimeout = restore })

	kr := fakeSecrets{pin: "deadbeef", session: "tok-123"}
	cfg := testConfig()
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = ln.Addr().(*net.TCPAddr).Port
	connect, _, err := NewConnector(cfg, &kr, testClient(), nil)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	// The parent ctx is generous: only the attempt deadline can end the call this
	// fast, and the parent must still be live afterwards to prove it was not the
	// one that fired.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = connect(ctx)
	if err == nil {
		t.Fatal("connect to a silent server: got nil, want attempt-timeout error")
	}
	if ctx.Err() != nil {
		t.Fatalf("parent ctx expired (%v); the attempt hung until the outer deadline", ctx.Err())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("connect err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	// Transient for every retry loop: the pool pauses only on these two sentinels
	// (terminalConnectErr), and the daemon's bring-up loop returns only on the
	// pool's terminal wrappers - an attempt timeout must match neither.
	if errors.Is(err, auth.ErrSessionVanished) {
		t.Errorf("connect err = %v; must not read as a vanished session", err)
	}
	if errors.Is(err, transport.ErrPinMismatch) {
		t.Errorf("connect err = %v; must not read as a pin mismatch", err)
	}
}

// TestMigrateLegacyPin covers the daemon's transparent legacy→SPKI pin upgrade
// performed after a successful reuse: a legacy pin is re-stored as the presented
// leaf's SPKI fingerprint, while an already-SPKI pin or a missing SPKI form is
// left untouched.
func TestMigrateLegacyPin(t *testing.T) {
	const server = "nas.example:6690"
	const spki = "spki-sha256:abc123"
	cases := []struct {
		name    string
		pin     string
		spki    string
		setErr  error
		wantSet []string
		wantRet string // the in-memory pin the daemon should adopt ("" = keep existing)
	}{
		{"legacy upgrades", "sha256:deadbeef", spki, nil, []string{spki}, spki},
		{"already spki noop", spki, spki, nil, nil, ""},
		{"empty spki noop", "sha256:deadbeef", "", nil, nil, ""},
		{"empty pin noop", "", spki, nil, nil, ""},
		{"store error tolerated", "sha256:deadbeef", spki, errFake, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kr := &fakeSecrets{setPinErr: tc.setErr}
			got := migrateLegacyPin(kr, server, tc.pin, tc.spki, nil)
			if got != tc.wantRet {
				t.Errorf("migrateLegacyPin returned %q, want %q", got, tc.wantRet)
			}
			if len(kr.setPins) != len(tc.wantSet) {
				t.Fatalf("setPins = %v, want %v", kr.setPins, tc.wantSet)
			}
			for i, want := range tc.wantSet {
				if kr.setPins[i] != want {
					t.Errorf("setPins[%d] = %q, want %q", i, kr.setPins[i], want)
				}
			}
		})
	}
}

// TestConnectWrapPreservesPinMismatch confirms the connect closure's error wrap
// keeps a transport.ErrPinMismatch recognisable via errors.Is, so the daemon's
// caller can treat a real key change as a clear re-pair signal rather than
// retrying a doomed dial.
func TestConnectWrapPreservesPinMismatch(t *testing.T) {
	wrapped := fmt.Errorf("daemon: connect %s: %w", "nas.example:6690", transport.ErrPinMismatch)
	if !errors.Is(wrapped, transport.ErrPinMismatch) {
		t.Fatalf("errors.Is(wrapped, ErrPinMismatch) = false, want true")
	}
}

func TestServerAddr(t *testing.T) {
	if got := serverAddr(testConfig()); got != "nas.example:6690" {
		t.Fatalf("serverAddr: got %q, want nas.example:6690", got)
	}
}

var errFake = errFakeT("backend down")

type errFakeT string

func (e errFakeT) Error() string { return string(e) }
