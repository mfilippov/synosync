package main

// A loopback mock sproto server for the pair/login CLI tests. It answers the full
// authenticated flow: TEST_CONN (legacy TLV reply with a ds_id), encrypt_channel
// (an `alive` reply followed by a TLS server handshake), then a request loop over
// the encrypted channel where auth (mode C) and action (mode A) requests are
// dispatched to a test-supplied handler. A loopback TCP socket is used so a
// rejected pin surfaces as a handshake error instead of deadlocking.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/transport"
)

// testDsID is the serial the mock TEST_CONN reply advertises.
const testDsID = "0123456789abcdef0123456789abcdef"

// replyFunc produces the reply for one post-TLS request, given the frame command
// and decoded request body.
type replyFunc func(cmd uint8, req *sproto.Map) sproto.Value

// mockServer is a loopback sproto server for the CLI tests.
type mockServer struct {
	t     *testing.T
	ln    net.Listener
	cert  tls.Certificate
	reply replyFunc
	// testConnFrame is the legacy TLV body returned to a TEST_CONN probe. Tests
	// may overwrite it (e.g. to drop the serial) to exercise the probe guard.
	testConnFrame []byte
}

// newMockServer starts a mock server whose post-TLS requests are answered by
// reply. It registers cleanup and returns the running server.
func newMockServer(t *testing.T, reply replyFunc) *mockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	s := &mockServer{t: t, ln: ln, cert: mockCert(t), reply: reply, testConnFrame: legacyTestConnFrame()}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// host and port return the loopback coordinates the server listens on.
func (s *mockServer) host() string {
	h, _, _ := net.SplitHostPort(s.ln.Addr().String())
	return h
}

func (s *mockServer) port() int {
	_, p, _ := net.SplitHostPort(s.ln.Addr().String())
	n, _ := net.LookupPort("tcp", p)
	return n
}

// addr is the "host:port" the server listens on (also the keychain server key).
func (s *mockServer) addr() string { return s.ln.Addr().String() }

// leafPin returns the legacy full-DER SHA-256 pin of the certificate the server
// presents.
func (s *mockServer) leafPin() string { return transport.Fingerprint(s.cert.Certificate[0]) }

// leafSPKIPin returns the SPKI (public-key) pin of the certificate the server
// presents - the form pair now stores and login migrates a legacy pin to.
func (s *mockServer) leafSPKIPin() string {
	fp, err := transport.FingerprintSPKI(s.cert.Certificate[0])
	if err != nil {
		s.t.Fatalf("FingerprintSPKI: %v", err)
	}
	return fp
}

func (s *mockServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *mockServer) handle(conn net.Conn) {
	h, err := sproto.ReadHeader(conn)
	if err != nil {
		_ = conn.Close()
		return
	}
	switch h.Cmd {
	case sproto.CmdTestConn:
		_, _ = conn.Write(s.testConnFrame)
		_ = conn.Close()
	case sproto.CmdRequestSSL:
		s.handleEncryptChannel(conn)
	default:
		_ = conn.Close()
	}
}

// handleEncryptChannel reads the encrypt_channel body, replies with alive, then
// upgrades to TLS and dispatches framed requests until the client disconnects.
func (s *mockServer) handleEncryptChannel(conn net.Conn) {
	defer conn.Close()
	if _, err := sproto.NewDecoder(conn).Decode(); err != nil {
		return
	}
	if err := sproto.NewEncoder(conn).Encode(sproto.NewMap().Set("alive", sproto.Int(1))); err != nil {
		return
	}
	tc := tls.Server(conn, &tls.Config{
		Certificates: []tls.Certificate{s.cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err := tc.Handshake(); err != nil {
		return
	}
	for {
		h, err := sproto.ReadHeader(tc)
		if err != nil {
			return
		}
		v, err := sproto.NewDecoder(tc).Decode()
		if err != nil {
			return
		}
		m, _ := v.(*sproto.Map)
		if err := sproto.NewEncoder(tc).Encode(s.reply(h.Cmd, m)); err != nil {
			return
		}
	}
}

// legacyTestConnHeader is the 8-byte frame header a real server prefixes to its
// TEST_CONN reply. Unlike post-TLS replies (bare PObjects), this pre-TLS one
// carries the header, and the client strips it before decoding the TLV body.
func legacyTestConnHeader() []byte {
	b, _ := sproto.Header{Cmd: sproto.CmdTestConn}.MarshalBinary()
	return b
}

// legacyTestConnFrame builds a TEST_CONN reply carrying testDsID, in the exact
// shape a live DSM sends: 8-byte header, then TLV sections with single-byte RES
// (success = 1) and PROTO_VERSION (0x1c), and uint16-BE-length vardata for
// SERIAL_NUMBER (0x1b).
func legacyTestConnFrame() []byte {
	b := legacyTestConnHeader()
	b = append(b, 0x01, 0x01) // RES = 1 (success)
	b = append(b, 0x1c, 0x46) // PROTO_VERSION = 0x46
	b = append(b, 0x1b)       // SERIAL_NUMBER = ds_id
	b = binary.BigEndian.AppendUint16(b, uint16(len(testDsID)))
	b = append(b, testDsID...)
	return b
}

// legacyTestConnFrameNoSerial builds a TEST_CONN reply carrying a success result
// but no SERIAL_NUMBER section, so the decoded ds_id is empty.
func legacyTestConnFrameNoSerial() []byte {
	b := legacyTestConnHeader()
	b = append(b, 0x01, 0x01) // RES = 1 (success)
	b = append(b, 0x1c, 0x46) // PROTO_VERSION = 0x46
	return b
}

// mockCert returns a fresh self-signed ECDSA certificate for the mock server.
func mockCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "synosync-mock"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31-1, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"synosync-mock"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// newPairMockServer is newMockServer for the `pair` flow: it answers the
// query_user_info turn pair makes to enumerate views and delegates everything
// else to the test's own reply.
//
// It is deliberately NOT the default. `login` validates a reused session with
// the same call (internal/auth), and its tests script that turn to say the
// session vanished - answering it centrally would steal that reply and make
// every session-vanished test see a healthy session instead.
//
// The empty view_list is a PRESENT but empty list, which QueryUserInfo reports
// as an authoritative "no views", so selectViews records the default layout and
// the pair tests keep the behaviour they were written for. A test that cares
// about selection supplies its own list (see pairviews_test.go).
func newPairMockServer(t *testing.T, reply replyFunc) *mockServer {
	t.Helper()
	return newMockServer(t, answerEmptyViewList(reply))
}

// answerEmptyViewList reports "no views" for the enumeration turn and delegates
// everything else.
func answerEmptyViewList(reply replyFunc) replyFunc {
	return func(cmd uint8, req *sproto.Map) sproto.Value {
		if action, ok := req.Get("action"); ok {
			if s, isStr := action.(sproto.String); isStr && string(s) == "query_user_info" {
				return sproto.NewMap().
					Set("errno", sproto.Int(0)).
					Set("view_list", sproto.Array{})
			}
		}
		return reply(cmd, req)
	}
}
