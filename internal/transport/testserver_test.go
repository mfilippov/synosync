package transport

// A loopback mock sproto server for transport tests. It dispatches on the frame
// header command word: TEST_CONN gets a legacy TLV reply (then the socket is
// closed), encrypt_channel gets a bare {"alive":1} reply followed by a TLS server
// handshake and an echo loop over the encrypted channel. A loopback TCP socket is
// used rather than net.Pipe so an aborted TLS handshake (e.g. a rejected pin)
// surfaces an error instead of deadlocking.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

// testDsID is the serial the mock TEST_CONN reply advertises.
const testDsID = "0123456789abcdef0123456789abcdef"

// mockServer is a loopback sproto server. cert is the leaf it presents on TLS
// upgrade, so tests can compute its expected pin via Fingerprint.
type mockServer struct {
	t    *testing.T
	ln   net.Listener
	cert tls.Certificate

	// mu guards restoreID, the server_id/restore_id the last encrypt_channel
	// request carried. Connect no longer keeps the value on the Session (it was
	// write-only state), so the handshake test asserts on what actually reached
	// the wire instead.
	mu        sync.Mutex
	restoreID string
}

// lastRestoreID reports the restore_id of the most recent encrypt_channel request.
func (s *mockServer) lastRestoreID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreID
}

// newMockServer starts a mock server on a loopback port and registers cleanup.
func newMockServer(t *testing.T) *mockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	s := &mockServer{t: t, ln: ln, cert: mockCert(t)}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// addr returns the loopback "host:port" the server listens on.
func (s *mockServer) addr() string { return s.ln.Addr().String() }

// leafPin returns the legacy full-DER SHA-256 pin of the certificate the server
// presents.
func (s *mockServer) leafPin() string { return Fingerprint(s.cert.Certificate[0]) }

// leafSPKIPin returns the SPKI SHA-256 pin of the certificate the server
// presents - the format captured during pairing.
func (s *mockServer) leafSPKIPin(t *testing.T) string {
	t.Helper()
	pin, err := FingerprintSPKI(s.cert.Certificate[0])
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	return pin
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
		_, _ = conn.Write(legacyTestConnFrame())
		_ = conn.Close()
	case sproto.CmdRequestSSL:
		s.handleEncryptChannel(conn)
	default:
		_ = conn.Close()
	}
}

// handleEncryptChannel reads the encrypt_channel body, replies with alive, then
// upgrades to TLS and echoes framed requests until the client disconnects.
func (s *mockServer) handleEncryptChannel(conn net.Conn) {
	defer conn.Close()
	req, err := sproto.NewDecoder(conn).Decode()
	if err != nil {
		return
	}
	if m, ok := req.(*sproto.Map); ok {
		if sid, ok := m.Get("server_id"); ok {
			if sm, ok := sid.(*sproto.Map); ok {
				if rid, ok := sm.Get("restore_id"); ok {
					if str, ok := rid.(sproto.String); ok {
						s.mu.Lock()
						s.restoreID = string(str)
						s.mu.Unlock()
					}
				}
			}
		}
	}
	resp := sproto.NewMap().Set("alive", sproto.Int(1))
	if err := sproto.NewEncoder(conn).Encode(resp); err != nil {
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
		if _, err := sproto.ReadHeader(tc); err != nil {
			return
		}
		v, err := sproto.NewDecoder(tc).Decode()
		if err != nil {
			return
		}
		var action sproto.Value = sproto.Null{}
		if m, ok := v.(*sproto.Map); ok {
			if a, ok := m.Get("action"); ok {
				action = a
			}
		}
		if err := sproto.NewEncoder(tc).Encode(sproto.NewMap().Set("echo", action)); err != nil {
			return
		}
	}
}

// newStalledTLSServer starts a loopback server that completes the plaintext
// encrypt_channel exchange (replying with alive) but then never performs the TLS
// server handshake - it just holds the connection open. It lets tests drive
// Connect into a TLS handshake that blocks until the context deadline fires.
func newStalledTLSServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := sproto.ReadHeader(conn); err != nil {
					return
				}
				if _, err := sproto.NewDecoder(conn).Decode(); err != nil {
					return
				}
				resp := sproto.NewMap().Set("alive", sproto.Int(1))
				if err := sproto.NewEncoder(conn).Encode(resp); err != nil {
					return
				}
				// Never run the TLS handshake; drain (and discard) the client's
				// ClientHello without ever replying, so the client-side handshake
				// read stalls until the context deadline fires.
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// legacyTestConnFrame builds a TEST_CONN reply byte-for-byte in the shape a live
// DSM sends: the 8-byte frame header (which this reply, unlike a post-TLS one,
// DOES carry) followed by legacy TLV sections.
//
// The tags and value widths are the ones observed on a real Drive Server 4.0.3:
// RES=0x01 and PROTO_VERSION=0x1c are single bytes; SERIAL_NUMBER=0x1b and
// SERVER_ID=0x1d are uint16-BE-length vardata. RES reports success as 1.
// The previous fixture guessed 0x02/0x03 with 4-byte values and no header, so it
// mirrored the client's wrong assumptions instead of the server's behaviour and
// could never fail on the real mismatch.
func legacyTestConnFrame() []byte {
	hdr, _ := sproto.Header{Cmd: sproto.CmdTestConn}.MarshalBinary()
	b := append([]byte(nil), hdr...)
	b = append(b, 0x01, 0x01) // RES = 1 (success)
	b = append(b, 0x1c, 0x46) // PROTO_VERSION = 0x46
	b = append(b, 0x1b)       // SERIAL_NUMBER = ds_id
	b = binary.BigEndian.AppendUint16(b, uint16(len(testDsID)))
	b = append(b, testDsID...)
	b = append(b, 0x1d) // SERVER_ID
	b = binary.BigEndian.AppendUint16(b, 2)
	b = append(b, "--"...)
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
