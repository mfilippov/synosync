package sproto

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// echoRequest reads one framed request from r and returns its decoded header and
// PObject body.
func readFramedRequest(t *testing.T, r io.Reader) (Header, Value) {
	t.Helper()
	h, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	v, err := NewDecoder(r).Decode()
	if err != nil {
		t.Fatalf("Decode request body: %v", err)
	}
	return h, v
}

func TestConnWriteRequestReadResponse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	req := NewMap().Set("action", String("ping"))
	const cmd = 0x01

	type srvResult struct {
		h Header
		v Value
	}
	done := make(chan srvResult, 1)
	go func() {
		h, v := readFramedRequest(t, server)
		// Reply with a bare PObject (no header), as the server does post-handshake.
		resp := NewMap().Set("errno", Int(0)).Set("pong", String("ok"))
		if err := NewEncoder(server).Encode(resp); err != nil {
			t.Errorf("server Encode: %v", err)
		}
		done <- srvResult{h, v}
	}()

	c := NewConn(client)
	if err := c.WriteRequest(cmd, req); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	got, err := c.ReadResponse()
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	res := <-done
	if res.h.Cmd != cmd {
		t.Errorf("server saw cmd 0x%02x, want 0x%02x", res.h.Cmd, cmd)
	}
	reqMap, ok := res.v.(*Map)
	if !ok {
		t.Fatalf("server got %T request body, want *Map", res.v)
	}
	if av, _ := reqMap.Get("action"); av != String("ping") {
		t.Errorf("server saw action %v, want ping", av)
	}

	respMap, ok := got.(*Map)
	if !ok {
		t.Fatalf("client got %T response, want *Map", got)
	}
	if pv, _ := respMap.Get("pong"); pv != String("ok") {
		t.Errorf("client saw pong %v, want ok", pv)
	}
}

// TestConnDecoderStreamsBinary verifies Conn.Decoder exposes the streaming
// decoder so large Binary responses can be read without buffering inline.
func TestConnDecoderStreamsBinary(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := bytes.Repeat([]byte{0xCD}, 4096)
	go func() {
		// The server sends a bare Binary response (no frame header).
		if err := NewEncoder(server).Encode(Binary{Data: payload}); err != nil {
			t.Errorf("server Encode: %v", err)
		}
	}()

	var streamed bytes.Buffer
	c := NewConn(client)
	dec := c.Decoder()
	dec.MaxInlineBinary = 64 // smaller than the payload, forces streaming
	dec.BinaryStream = func(_ Binary, r io.Reader) error {
		_, err := io.Copy(&streamed, r)
		return err
	}
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(streamed.Bytes(), payload) {
		t.Errorf("streamed %d bytes, want %d matching payload", streamed.Len(), len(payload))
	}
}

// TestConnDecoderWith exercises the read-observing decoder the download path uses
// to arm its idle-progress watchdog: every byte the codec pulls
// off the connection passes through the caller's wrapper, and a nil wrapper is the
// plain Decoder.
func TestConnDecoderWith(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := bytes.Repeat([]byte{0xAB}, 2048)
	go func() {
		if err := NewEncoder(server).Encode(NewMap().Set("data", Binary{Data: payload})); err != nil {
			t.Errorf("server Encode: %v", err)
		}
	}()

	var observed int
	c := NewConn(client)
	dec := c.DecoderWith(func(r io.Reader) io.Reader {
		return readerFunc(func(p []byte) (int, error) {
			n, err := r.Read(p)
			observed += n
			return n, err
		})
	})
	v, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := v.(*Map)
	if !ok {
		t.Fatalf("reply is %T, want *Map", v)
	}
	b, _ := m.Get("data")
	if got, ok := b.(Binary); !ok || !bytes.Equal(got.Data, payload) {
		t.Fatalf("decoded data = %#v, want the %d-byte payload", b, len(payload))
	}
	if observed < len(payload) {
		t.Errorf("wrapper observed %d bytes, want at least the %d-byte payload", observed, len(payload))
	}

	// A nil wrapper degrades to the plain Decoder over the same connection.
	if c.DecoderWith(nil) == nil {
		t.Error("DecoderWith(nil) returned nil, want the plain Decoder")
	}
}

// readerFunc adapts a function to io.Reader.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestConnReadCappedResponseDrainsOversizedBinary exercises the shared capped
// reply read (the core deduplicated out of auth and actions): a Binary past the
// cap is drained rather than buffered - Data nil, Length still reporting the
// true size, oversize flagged - and the connection stays byte-aligned, proven
// by cleanly decoding a second reply sent immediately after.
func TestConnReadCappedResponseDrainsOversizedBinary(t *testing.T) {
	const inlineCap = 64
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	oversized := bytes.Repeat([]byte{0xAB}, inlineCap+1)
	go func() {
		reply := NewMap().
			Set("errno", Int(0)).
			Set("data", Binary{Data: oversized})
		if err := NewEncoder(server).Encode(reply); err != nil {
			t.Errorf("server Encode reply 1: %v", err)
			return
		}
		// A second reply straight after: if the drain left even one byte of the
		// oversized Binary unconsumed, this decode misaligns and fails.
		if err := NewEncoder(server).Encode(NewMap().Set("errno", Int(0))); err != nil {
			t.Errorf("server Encode reply 2: %v", err)
		}
	}()

	c := NewConn(client)
	resp, oversize, err := c.ReadCappedResponse(inlineCap)
	if err != nil {
		t.Fatalf("ReadCappedResponse: %v", err)
	}
	if !oversize {
		t.Fatal("oversize = false, want true for a Binary past the cap")
	}
	m, ok := resp.(*Map)
	if !ok {
		t.Fatalf("resp is %T, want *Map", resp)
	}
	v, _ := m.Get("data")
	b, ok := v.(Binary)
	if !ok {
		t.Fatalf("data is %T, want Binary", v)
	}
	if b.Data != nil {
		t.Errorf("oversized Binary.Data buffered %d bytes, want nil (drained)", len(b.Data))
	}
	if b.Length != int64(len(oversized)) {
		t.Errorf("Binary.Length = %d, want %d", b.Length, len(oversized))
	}
	if _, oversize2, err := c.ReadCappedResponse(inlineCap); err != nil || oversize2 {
		t.Fatalf("second read after drain: err = %v, oversize = %v; want nil, false (connection aligned)", err, oversize2)
	}
}

// TestConnReadCappedResponseInlinesSmallBinary is the regression guard for the
// shared capped read: a Binary at or below the cap is buffered inline as usual,
// with no oversize flag.
func TestConnReadCappedResponseInlinesSmallBinary(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte("small inline payload")
	go func() {
		reply := NewMap().Set("data", Binary{Data: payload})
		if err := NewEncoder(server).Encode(reply); err != nil {
			t.Errorf("server Encode: %v", err)
		}
	}()

	c := NewConn(client)
	resp, oversize, err := c.ReadCappedResponse(int64(len(payload)))
	if err != nil {
		t.Fatalf("ReadCappedResponse: %v", err)
	}
	if oversize {
		t.Fatal("oversize = true for a Binary at the cap, want false")
	}
	m, ok := resp.(*Map)
	if !ok {
		t.Fatalf("resp is %T, want *Map", resp)
	}
	v, _ := m.Get("data")
	b, ok := v.(Binary)
	if !ok {
		t.Fatalf("data is %T, want Binary", v)
	}
	if !bytes.Equal(b.Data, payload) {
		t.Errorf("data = %q, want %q", b.Data, payload)
	}
}

// TestConnReadResponseCodecError verifies a malformed response surfaces the
// codec error rather than hanging or panicking.
func TestConnReadResponseCodecError(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		// 0x7f is not a known value tag.
		if _, err := server.Write([]byte{0x7f}); err != nil {
			t.Errorf("server write: %v", err)
		}
		server.Close()
	}()

	c := NewConn(client)
	if _, err := c.ReadResponse(); err == nil {
		t.Fatal("ReadResponse: want error for unknown tag, got nil")
	}
}

// dialTLSEchoServer starts a loopback TLS echo server and returns a raw client
// connection dialed to it plus a channel carrying the server's eventual error.
// A loopback TCP socket is used rather than net.Pipe: net.Pipe is fully
// synchronous, so a handshake that aborts mid-flight (e.g. a rejecting pin hook)
// deadlocks instead of surfacing the error.
func dialTLSEchoServer(t *testing.T) (net.Conn, <-chan error) {
	t.Helper()
	cert := selfSignedCert(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srvErr := make(chan error, 1)
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		srvErr <- runTLSEchoServer(conn, cert)
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return raw, srvErr
}

// TestConnUpgradeTLS upgrades a loopback socket to TLS with a self-signed server
// cert and confirms the pinning hook fires and framed I/O works over the
// encrypted channel.
func TestConnUpgradeTLS(t *testing.T) {
	clientRaw, srvErr := dialTLSEchoServer(t)

	c := NewConn(clientRaw)
	var hookCerts int
	cfg := &TLSConfig{
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			hookCerts = len(rawCerts)
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.UpgradeTLS(ctx, cfg); err != nil {
		t.Fatalf("UpgradeTLS: %v", err)
	}
	if hookCerts == 0 {
		t.Error("VerifyPeerCertificate hook was not called with peer certs")
	}
	if _, ok := c.NetConn().(*tls.Conn); !ok {
		t.Errorf("after upgrade NetConn is %T, want *tls.Conn", c.NetConn())
	}

	// A second upgrade must be rejected.
	if err := c.UpgradeTLS(ctx, cfg); err == nil {
		t.Error("second UpgradeTLS: want error, got nil")
	}

	// Framed request/response now runs over TLS.
	if err := c.WriteRequest(0x01, NewMap().Set("action", String("hello"))); err != nil {
		t.Fatalf("WriteRequest over TLS: %v", err)
	}
	resp, err := c.ReadResponse()
	if err != nil {
		t.Fatalf("ReadResponse over TLS: %v", err)
	}
	m, ok := resp.(*Map)
	if !ok {
		t.Fatalf("got %T response, want *Map", resp)
	}
	if v, _ := m.Get("echo"); v != String("hello") {
		t.Errorf("echo = %v, want hello", v)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := <-srvErr; err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("server: %v", err)
	}
}

// TestConnUpgradeTLSPinReject confirms a rejecting pin hook fails the upgrade.
func TestConnUpgradeTLSPinReject(t *testing.T) {
	clientRaw, srvErr := dialTLSEchoServer(t)
	defer clientRaw.Close()
	defer func() { <-srvErr }()

	c := NewConn(clientRaw)
	wantErr := errors.New("pin mismatch")
	cfg := &TLSConfig{
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error {
			return wantErr
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.UpgradeTLS(ctx, cfg)
	if err == nil {
		t.Fatal("UpgradeTLS: want error from rejecting pin hook, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("UpgradeTLS error = %v, want chain containing %v", err, wantErr)
	}
}

// runTLSEchoServer accepts a TLS handshake on conn, then reads one framed
// request and echoes its "action" string back as a bare {"echo": ...} PObject.
func runTLSEchoServer(conn net.Conn, cert tls.Certificate) error {
	defer conn.Close()
	tc := tls.Server(conn, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err := tc.Handshake(); err != nil {
		return err
	}
	h, err := ReadHeader(tc)
	if err != nil {
		return err
	}
	_ = h
	v, err := NewDecoder(tc).Decode()
	if err != nil {
		return err
	}
	m, ok := v.(*Map)
	if !ok {
		return errors.New("request not a map")
	}
	action, _ := m.Get("action")
	resp := NewMap().Set("echo", action)
	return NewEncoder(tc).Encode(resp)
}

// selfSignedCert returns a fresh self-signed ECDSA certificate for tests.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "synosync-test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31-1, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"synosync-test"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	return cert
}
