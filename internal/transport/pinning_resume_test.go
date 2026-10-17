package transport

// This file holds the regression test for the resumed-session pin-bypass footgun:
// crypto/tls SKIPS VerifyPeerCertificate on a resumed TLS handshake but always
// runs VerifyConnection, so the pin check must live on the latter. The test drives
// a real loopback TLS server with session tickets enabled, performs a full
// handshake followed by a resumed one over a shared client session cache, and
// asserts the VerifyConnection-wired pin fires on BOTH while VerifyPeerCertificate
// fires only on the full handshake.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"testing"
	"time"
)

// loopbackTLSServer starts a TLS echo-ish server on a loopback port. Each accepted
// connection completes the handshake, writes one byte (so the client's first Read
// pumps the TLS 1.3 post-handshake NewSessionTicket message), then drains until the
// client closes. It returns the listen address and a stop func.
func loopbackTLSServer(t *testing.T, cert tls.Certificate) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srvCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				tc := tls.Server(c, srvCfg)
				if err := tc.Handshake(); err != nil {
					return
				}
				_, _ = tc.Write([]byte("x"))
				_, _ = io.Copy(io.Discard, tc)
			}(c)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

// dialOnce performs one client TLS handshake against addr using clientCfg, reads
// the server's priming byte (so any session ticket is processed), and returns the
// negotiated ConnectionState.
func dialOnce(t *testing.T, addr string, clientCfg *tls.Config) tls.ConnectionState {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer raw.Close()
	tc := tls.Client(raw, clientCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := tc.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("client read priming byte: %v", err)
	}
	cs := tc.ConnectionState()
	_ = tc.Close()
	return cs
}

// TestPinEnforcedOnResumedSession is the footgun regression: with the pin wired
// through VerifyConnection (as Connect now does), the check runs on a resumed
// handshake too, whereas VerifyPeerCertificate is skipped on resumption. Were the
// pin only on VerifyPeerCertificate, a resumed connection would bypass it entirely.
func TestPinEnforcedOnResumedSession(t *testing.T) {
	cert := mockCert(t)
	pin, err := FingerprintSPKI(cert.Certificate[0])
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	addr, stop := loopbackTLSServer(t, cert)
	defer stop()

	var connChecks, peerCertChecks int
	clientCfg := &tls.Config{
		ServerName:         "synosync-mock",
		InsecureSkipVerify: true, // #nosec G402 -- trust comes from the pin check below
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
		VerifyConnection: func(cs tls.ConnectionState) error {
			connChecks++
			return pinConnVerifier(pin)(cs)
		},
		VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error {
			peerCertChecks++
			return nil
		},
	}

	// Full handshake: both callbacks fire; the connection is not resumed.
	cs1 := dialOnce(t, addr, clientCfg)
	if cs1.DidResume {
		t.Fatal("first handshake unexpectedly resumed")
	}
	if connChecks != 1 || peerCertChecks != 1 {
		t.Fatalf("after full handshake: VerifyConnection=%d VerifyPeerCertificate=%d, want 1/1",
			connChecks, peerCertChecks)
	}

	// Resumed handshake: VerifyConnection MUST fire again; VerifyPeerCertificate is
	// skipped by crypto/tls on resumption - exactly why the pin lives on the former.
	cs2 := dialOnce(t, addr, clientCfg)
	if !cs2.DidResume {
		t.Skip("server did not issue a resumable session ticket; resumption path not exercised")
	}
	if connChecks != 2 {
		t.Errorf("VerifyConnection fired %d times across full+resumed, want 2 (pin would be bypassed on resume)", connChecks)
	}
	if peerCertChecks != 1 {
		t.Errorf("VerifyPeerCertificate fired %d times, want 1 (it is skipped on resumption - the footgun)", peerCertChecks)
	}
}
