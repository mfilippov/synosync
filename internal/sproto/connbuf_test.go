package sproto

// Tests for the connection read buffer installed by UpgradeTLS (DESIGN §13).
// Three properties are load-bearing and each fails silently if broken:
//
//  1. reads are UNBUFFERED before the upgrade - a buffer filled pre-TLS would
//     hold plaintext past the point the socket switches to TLS;
//  2. reads are BUFFERED after it - worth a read-count assertion rather than an
//     "it still decodes" smoke test, since an unbuffered connection decodes
//     perfectly and merely costs a call per few bytes;
//  3. a read observer is installed BELOW the buffer, so it reports the bytes
//     that ARRIVED rather than the decoder's reads out of the buffer. An
//     observer above the buffer still sees "progress" on every decode and so
//     silently disarms the actions idle watchdog.

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

// wideReply is a value whose encoding forces many small decoder reads: every map
// key and every short string costs its own tag and length prefix, which is
// exactly the shape a catalog page has.
func wideReply(n int) Value {
	arr := make(Array, 0, n)
	for i := 0; i < n; i++ {
		arr = append(arr, NewMap().
			Set("file_id", String("965012315441246571")).
			Set("name", String("document.txt")).
			Set("file_size", Int(int64(4096+i))).
			Set("file_hash", String("0123456789abcdef0123456789abcdef")))
	}
	return NewMap().Set("type", String("response")).Set("node_list", arr)
}

// serveOnce accepts one connection, hands it to fn, and reports fn's error.
func serveOnce(t *testing.T, fn func(net.Conn) error) (net.Conn, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	errc := make(chan error, 1)
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer conn.Close()
		errc <- fn(conn)
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return raw, errc
}

// TestConnUnbufferedBeforeUpgrade pins the rule the buffer must not break: no
// byte may be read ahead before the TLS upgrade.
//
// A buffer filled pre-TLS would swallow bytes belonging to the TLS handshake
// (or leave plaintext buffered past the switch), and the failure is invisible
// from inside sproto - the handshake simply fails later, in transport. So the
// assertion is direct: after decoding one pre-TLS frame, the bytes that followed
// it on the wire must still be readable from the socket itself.
func TestConnUnbufferedBeforeUpgrade(t *testing.T) {
	first := NewMap().Set("alive", Int(1))
	const trailer = "TLS-HANDSHAKE-BYTES-MUST-SURVIVE"

	raw, srvErr := serveOnce(t, func(conn net.Conn) error {
		if err := NewEncoder(conn).Encode(first); err != nil {
			return err
		}
		_, err := io.WriteString(conn, trailer)
		return err
	})

	c := NewConn(raw)
	got, err := c.ReadResponse()
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if m, ok := got.(*Map); !ok {
		t.Fatalf("reply is %T, want *Map", got)
	} else if v, ok := m.Int("alive"); !ok || v != 1 {
		t.Fatalf("alive = %v (ok=%v), want 1", v, ok)
	}

	// The trailer stands in for the TLS ClientHello's peer response: it must not
	// have been swallowed into a buffer the upgrade cannot see.
	buf := make([]byte, len(trailer))
	if err := raw.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := io.ReadFull(raw, buf); err != nil {
		t.Fatalf("reading the bytes after the pre-TLS frame: %v - a pre-upgrade "+
			"read buffer consumed them, which would corrupt the in-place TLS upgrade", err)
	}
	if string(buf) != trailer {
		t.Errorf("trailing bytes = %q, want %q", buf, trailer)
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}

// TestConnObserverSeesSocketNotBuffer carries both halves of the buffering
// contract, because one observation point settles both: the reads reaching the
// connection underneath the buffer.
//
// It asserts (a) the buffer is THERE - few reads, not one per tag and length
// prefix - and (b) the DecoderWith observer sits BELOW it, reporting the bytes
// that arrived rather than the decoder's reads out of memory.
//
// (b) is the hazard that made this change worth its own task. The actions idle
// watchdog re-arms its budget from this observer. Wrapped ABOVE the buffer it
// would fire on every value the decoder pulls from memory, so a peer that went
// silent after one refill would keep it re-arming forever: the stalled-peer
// protection gone, with no test failing and no symptom until a live server
// wedged.
//
// Measuring at the SOCKET instead would prove nothing - crypto/tls buffers whole
// records, so socket reads stay in the dozens whether or not this buffer exists
// (verified: removing the buffer leaves the socket-level count unchanged while
// the count here goes from 60 to 5219). That is also why the pre-fix cost was
// tls.Conn.Read CALLS rather than syscalls: 520465 of them for a 20k-node reply
// against 579 buffered, worth ~1.3x on decode wall-clock and far more under the
// race detector, which instruments every one (DESIGN §13).
func TestConnObserverSeesSocketNotBuffer(t *testing.T) {
	const nodes = 200
	reply := wideReply(nodes)

	cert := selfSignedCert(t)
	raw, srvErr := serveOnce(t, func(conn net.Conn) error {
		tc := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tc.Handshake(); err != nil {
			return err
		}
		return NewEncoder(tc).Encode(reply)
	})

	c := NewConn(raw)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.UpgradeTLS(ctx, &TLSConfig{InsecureSkipVerify: true}); err != nil {
		t.Fatalf("UpgradeTLS: %v", err)
	}

	var observed, calls int
	wrap := func(r io.Reader) io.Reader {
		return readerFunc(func(p []byte) (int, error) {
			n, err := r.Read(p)
			if n > 0 {
				calls++
				observed += n
			}
			return n, err
		})
	}
	dec := c.DecoderWith(wrap)
	dec.MaxFrameBytes = 1 << 30
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server: %v", err)
	}

	if calls == 0 {
		t.Fatal("observer was never called: DecoderWith is not wired to the read path")
	}
	// This reply costs well over five thousand decoder-level reads. Anything near
	// that count means either the buffer is missing or the observer sits above it
	// - the assertion cannot tell those apart, and neither is acceptable.
	const maxCalls = 64
	if calls > maxCalls {
		t.Errorf("observer saw %d reads for %d nodes, want at most %d: either the "+
			"post-TLS read buffer is missing, or the observer is installed ABOVE it "+
			"and reports buffer hits as socket progress, disarming the idle "+
			"watchdog (DESIGN §13)", calls, nodes, maxCalls)
	}
	if avg := observed / calls; avg < 512 {
		t.Errorf("observer saw %d bytes over %d reads (%d B/read), want large "+
			"refills: small reads mean it is observing the decoder, not the socket",
			observed, calls, avg)
	}
}

// TestConnBufferSpansTurns pins that bytes read ahead into the buffer are not
// lost: a server that sends two frames back to back (which a live DSM does -
// it interleaves unsolicited keep_alive frames ahead of a reply) will have both
// sitting in one refill, and the SECOND turn must decode from the buffer rather
// than block on a socket that has nothing more to give.
func TestConnBufferSpansTurns(t *testing.T) {
	cert := selfSignedCert(t)
	raw, srvErr := serveOnce(t, func(conn net.Conn) error {
		tc := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tc.Handshake(); err != nil {
			return err
		}
		// Both frames in one write, so they land in a single buffer refill.
		var buf bytes.Buffer
		if err := NewEncoder(&buf).Encode(NewMap().Set("type", String("keep_alive"))); err != nil {
			return err
		}
		if err := NewEncoder(&buf).Encode(NewMap().Set("type", String("response"))); err != nil {
			return err
		}
		_, err := tc.Write(buf.Bytes())
		return err
	})

	c := NewConn(raw)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.UpgradeTLS(ctx, &TLSConfig{InsecureSkipVerify: true}); err != nil {
		t.Fatalf("UpgradeTLS: %v", err)
	}
	if err := c.NetConn().SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	for i, want := range []string{"keep_alive", "response"} {
		v, err := c.ReadResponse()
		if err != nil {
			t.Fatalf("frame %d: ReadResponse: %v", i, err)
		}
		m, ok := v.(*Map)
		if !ok {
			t.Fatalf("frame %d is %T, want *Map", i, v)
		}
		if got, _ := m.String("type"); got != want {
			t.Errorf("frame %d type = %q, want %q", i, got, want)
		}
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}
