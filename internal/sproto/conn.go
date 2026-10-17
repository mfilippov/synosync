package sproto

// This file implements Conn, a thin connection primitive over a net.Conn. It
// frames client requests (8-byte header + PStream, see header.go/pstream.go) and
// reads bare server-response PObjects, and it can upgrade the underlying socket
// to TLS in place - the transport switches from plaintext to encrypted on the
// same connection, exactly as the protocol's `encrypt_channel` → TLS step
// requires.
//
// Conn deliberately contains no handshake logic: the TEST_CONN / encrypt_channel
// / connect exchange and the cert-pinning policy live in internal/transport. What
// lives here are the framing primitives plus the TLS-upgrade hook where transport
// wires in its leaf/SPKI pin check. In production that hook is VerifyConnection
// (which crypto/tls runs on BOTH full and resumed handshakes); the older
// VerifyPeerCertificate field is retained only for callers that do not need
// resumption coverage - crypto/tls skips it on a resumed session, so relying on it
// alone is a pin-bypass footgun (see the field docs on TLSConfig below).

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
)

// connReadBuffer is the size of the read buffer installed once the channel is
// encrypted. The decoder reads tags and length prefixes one and two bytes at a
// time, so an unbuffered connection costs roughly six socket reads per map
// key/value pair - measured at ~4.6 bytes per read, half a million reads for a
// 2.3 MB catalog page. 64 KiB collapses that page to a few dozen reads; the cost
// is one buffer per pooled connection (an event session plus 2-4 workers).
const connReadBuffer = 64 << 10

// Conn wraps a net.Conn with sproto framing and an in-place TLS upgrade. It is
// not safe for concurrent use; higher layers serialise request/response turns.
//
// Reads are UNBUFFERED until the TLS upgrade and buffered after it. The
// no-buffering rule exists for exactly one reason - a buffer filled before
// UpgradeTLS would hold plaintext bytes past the point the socket switches to
// TLS, and the codec must therefore read exactly as many bytes as each value
// needs - and that reason expires with the upgrade: there is no second upgrade,
// and frames after it are self-delimiting, so bytes read ahead into the buffer
// are simply the next frame's, consumed by the next turn from the same buffer.
//
// Applying the rule to the whole connection lifetime instead of to the one
// handshake step is what made every post-TLS decode pay a syscall per few bytes
// (DESIGN §13).
//
// The consequence for callers: after UpgradeTLS, NetConn() must not be READ
// from. The buffer may already hold bytes the socket has delivered, so a direct
// read desynchronises the frame stream. Writing to it and setting deadlines on
// it stay correct, which is all any caller does (the sole direct reader,
// transport's encrypt_channel exchange, runs before the upgrade).
type Conn struct {
	conn net.Conn
	// src is the swappable read source br pulls from. It exists so a per-turn
	// read observer can be installed BELOW the buffer: an observer above it would
	// see buffer hits as socket progress, which would silently disable the
	// actions idle watchdog - it would keep re-arming on bytes that arrived long
	// before, and stop bounding a genuinely silent peer, the exact failure it is
	// there to catch.
	src *srcReader
	// br buffers reads once the channel is encrypted; nil before UpgradeTLS.
	br *bufio.Reader
}

// srcReader is the stable reader br pulls from, indirecting through a per-turn
// observer that DecoderWith/ReadCappedResponseWith install and replace.
type srcReader struct {
	// raw is the connection itself, the reader used when no observer is set.
	raw io.Reader
	// r is raw, or the current turn's observer wrapped around raw.
	r io.Reader
}

func (s *srcReader) Read(p []byte) (int, error) { return s.r.Read(p) }

// observe installs wrap as the current turn's read observer, or clears it when
// wrap is nil. Turns are serialised by the caller, so the swap never races a read.
func (s *srcReader) observe(wrap func(io.Reader) io.Reader) {
	if wrap == nil {
		s.r = s.raw
		return
	}
	s.r = wrap(s.raw)
}

// reader returns the reader a decode should run over, installing wrap as this
// turn's observer. Before the upgrade there is no buffer and wrap is applied
// directly; after it, wrap goes underneath the buffer (see srcReader).
func (c *Conn) reader(wrap func(io.Reader) io.Reader) io.Reader {
	if c.br == nil {
		if wrap == nil {
			return c.conn
		}
		return wrap(c.conn)
	}
	c.src.observe(wrap)
	return c.br
}

// NewConn returns a Conn that frames over the given net.Conn.
func NewConn(conn net.Conn) *Conn {
	return &Conn{conn: conn}
}

// NetConn returns the underlying transport. After UpgradeTLS it is the
// *tls.Conn; before, it is the raw connection passed to NewConn.
func (c *Conn) NetConn() net.Conn {
	return c.conn
}

// WriteRequest writes a client request: the 8-byte frame header for cmd followed
// by the PStream encoding of v.
func (c *Conn) WriteRequest(cmd uint8, v Value) error {
	return WriteRequest(c.conn, cmd, v)
}

// ReadResponse reads a single bare server-response PObject (no frame header), as
// the server sends after the handshake.
func (c *Conn) ReadResponse() (Value, error) {
	return ReadResponse(c.reader(nil))
}

// Decoder returns a Decoder reading from the connection. Use it instead of
// ReadResponse when streaming control over large Binary values is needed
// (MaxInlineBinary / BinaryStream).
func (c *Conn) Decoder() *Decoder {
	return NewDecoder(c.reader(nil))
}

// DecoderWith returns a Decoder like Decoder, but reading the connection's bytes
// through wrap. It exists for callers that must observe the raw read stream while
// the codec decodes it - notably the download path, which arms an idle-progress
// watchdog over the socket reads so a server that goes silent mid-body cannot
// block the decode forever. A nil wrap is the plain Decoder.
//
// wrap observes SOCKET reads, not decoder reads: after the TLS upgrade it is
// installed underneath the connection's read buffer, so it sees each refill -
// the bytes that actually arrived - and not the many small reads the decoder
// satisfies from the buffer. That placement is what keeps an idle watchdog
// meaningful; wrapped above the buffer it would re-arm on bytes that arrived
// long before and stop bounding a silent peer.
//
// wrap itself must not buffer, and it must pass reads through one-for-one: it
// observes the stream, it does not reshape it.
//
// The observer belongs to the turn, and a turn is the caller's unit of
// serialisation: each read turn passes its own wrap (or nil), replacing the
// previous one.
func (c *Conn) DecoderWith(wrap func(io.Reader) io.Reader) *Decoder {
	return NewDecoder(c.reader(wrap))
}

// ReadCappedResponse reads a single bare server-response PObject like
// ReadResponse, but bounds every inline Binary value at inlineCap bytes. A
// Binary past the cap is drained rather than buffered - its Data is left nil
// while Length still reports the true size, and the connection stays
// byte-aligned for the next turn - and the drain is reported via the oversize
// flag so the caller can reject just that reply without re-dialing. A genuine
// I/O or decode failure is returned as err (and does leave the connection
// misaligned). This is the shared core of the auth and actions reply reads;
// each layer keeps its own cap constant and error wrapping on top.
//
// inlineCap follows Decoder.MaxInlineBinary's convention: a positive value is
// the byte cap, zero selects the safe defaultMaxInlineBinary (1 MiB) so a caller
// that forgets to set one is still hostile-input-safe, and a negative value opts
// out of the cap entirely (unlimited inline buffering). Under the unlimited
// (negative) setting no Binary is ever streamed to the drain hook, so oversize is
// always false there; the drain path is reachable only under a positive or the
// default cap.
//
// The drain is itself bounded (Decoder.MaxDrainBytes, 64 MiB by default): a
// Binary whose CLAIMED length is absurd - a hostile or corrupt frame declaring,
// say, 2^62 bytes and drip-feeding them - is refused with ErrDrainTooLarge
// instead of being read to its claimed end, which would pin this goroutine
// forever. That refusal aborts mid-frame, so unlike the ordinary oversize drain
// it does leave the connection misaligned and is returned as err.
func (c *Conn) ReadCappedResponse(inlineCap int64) (resp Value, oversize bool, err error) {
	return c.ReadCappedResponseWith(inlineCap, nil)
}

// ReadCappedResponseWith is ReadCappedResponse reading the connection's bytes through
// wrap, exactly as DecoderWith does for the streaming download path: it exists so a
// caller can observe the raw read stream - notably to arm an idle-progress watchdog
// over a reply whose arrival is entirely at the server's discretion. A nil wrap is
// plain ReadCappedResponse; wrap observes socket reads and must not buffer or
// reshape them (see DecoderWith).
func (c *Conn) ReadCappedResponseWith(inlineCap int64, wrap func(io.Reader) io.Reader) (resp Value, oversize bool, err error) {
	dec := c.DecoderWith(wrap)
	dec.MaxInlineBinary = inlineCap
	dec.BinaryStream = func(_ Binary, _ io.Reader) error {
		// Deliberately read nothing: leaving the payload to the decoder's own
		// drain is what subjects it to MaxDrainBytes. Discarding it here instead
		// would be an unbounded io.Copy the budget never sees.
		oversize = true
		return nil
	}
	resp, err = dec.Decode()
	return resp, oversize, err
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// TLSConfig configures the in-place TLS upgrade. A nil *TLSConfig requests
// standard verification (TLS 1.2+, chain and hostname checked against the system
// roots using ServerName).
type TLSConfig struct {
	// ServerName is the SNI value and the name checked during default
	// verification. It may be empty when verification is delegated entirely to
	// VerifyPeerCertificate.
	ServerName string

	// RootCAs is the trust anchor pool for default verification. A nil pool uses
	// the host's system roots. Ignored when InsecureSkipVerify is set.
	RootCAs *x509.CertPool

	// InsecureSkipVerify disables Go's built-in chain and hostname verification.
	// The transport cert-pinning layer sets this together with
	// VerifyPeerCertificate, replacing chain trust with a manual leaf-pin check.
	InsecureSkipVerify bool

	// VerifyPeerCertificate is the legacy cert-pinning hook. When set it is called
	// after the handshake with the peer's raw certificates; returning a non-nil
	// error aborts the connection. crypto/tls SKIPS this callback on a resumed
	// session (session ticket / session id), so relying on it alone leaves a
	// resumed connection unpinned.
	//
	// Deprecated: no production caller sets this field - the transport pinning
	// layer wires its leaf/SPKI pin through VerifyConnection so the check holds on
	// resumed handshakes too. Prefer VerifyConnection for any new pin check.
	VerifyPeerCertificate func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

	// VerifyConnection is the cert-pinning hook that fires on BOTH a full and a
	// resumed handshake. Unlike VerifyPeerCertificate (which crypto/tls skips on
	// session resumption), tls.Config.VerifyConnection runs on every handshake, so
	// the transport pinning layer wires its leaf/SPKI pin check here to close the
	// resumption-bypass footgun. The negotiated state carries the peer's
	// certificates (populated on resumed connections too); returning a non-nil
	// error aborts the connection.
	VerifyConnection func(cs tls.ConnectionState) error

	// MinVersion is the minimum acceptable TLS version. Zero defaults to TLS 1.2.
	MinVersion uint16
}

// toStdConfig builds the crypto/tls configuration for an upgrade.
func (cfg *TLSConfig) toStdConfig() *tls.Config {
	std := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg == nil {
		return std
	}
	std.ServerName = cfg.ServerName
	std.RootCAs = cfg.RootCAs
	// #nosec G402 -- the transport pinning layer intentionally sets InsecureSkipVerify
	// and supplies VerifyPeerCertificate to pin the leaf instead of trusting a chain.
	std.InsecureSkipVerify = cfg.InsecureSkipVerify
	std.VerifyPeerCertificate = cfg.VerifyPeerCertificate
	// VerifyConnection runs on both full and resumed handshakes, so the transport
	// pinning layer uses it to keep the leaf/SPKI pin enforced across session
	// resumption (where VerifyPeerCertificate is skipped).
	std.VerifyConnection = cfg.VerifyConnection
	if cfg.MinVersion != 0 {
		std.MinVersion = cfg.MinVersion
	}
	return std
}

// UpgradeTLS upgrades the connection to TLS in place, running the client
// handshake over the existing socket. On success the underlying transport is
// replaced by the *tls.Conn and all subsequent request/response I/O runs
// encrypted; on failure the Conn is left using the original transport and the
// caller should close it. It must be called at most once, before any TLS I/O.
func (c *Conn) UpgradeTLS(ctx context.Context, cfg *TLSConfig) error {
	if _, already := c.conn.(*tls.Conn); already {
		return fmt.Errorf("sproto: connection already upgraded to TLS")
	}
	tc := tls.Client(c.conn, cfg.toStdConfig())
	if err := tc.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("sproto: tls handshake: %w", err)
	}
	c.conn = tc
	// Install the read buffer only now. Nothing was buffered before - br was nil,
	// so every pre-upgrade read went straight to the socket - which is what lets
	// the handshake hand a byte-exact socket to tls.Client above.
	c.src = &srcReader{raw: tc, r: tc}
	c.br = bufio.NewReaderSize(c.src, connReadBuffer)
	return nil
}
