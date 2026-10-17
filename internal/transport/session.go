package transport

// This file ties the handshake steps into a single authenticated connection.
// Connect dials TCP, runs encrypt_channel, then upgrades the same socket to TLS
// with cert pinning - capturing the leaf on pairing or verifying it on reuse -
// and hands back a Session whose sproto.Conn is ready for post-handshake actions.
// The multi-connection worker pool lives in pool.go; a Session is a single
// connection.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// ConnectConfig parameterises Connect.
type ConnectConfig struct {
	// Addr is the server "host:port" to dial.
	Addr string
	// ServerName is the TLS SNI value. Empty defaults to the host part of Addr.
	// Verification is delegated to the pin check, so this affects SNI only.
	ServerName string
	// Agent is the client agent PObject announced in encrypt_channel (built by the
	// auth package). It may be nil during low-level tests.
	Agent sproto.Value
	// DsID is the restore id from a prior TestConn, echoed in encrypt_channel.
	DsID string
	// Pin is the expected leaf fingerprint. Empty enables capture mode (pairing):
	// any leaf is accepted and its fingerprint is exposed via Session.Pin. When
	// set, the leaf must match or Connect fails with ErrPinMismatch (reuse).
	Pin string
	// Dialer overrides the TCP dialer; nil uses the default net.Dialer.
	Dialer Dialer
}

// Session is a single authenticated, TLS-pinned connection to a Synology server.
// It is not safe for concurrent use; callers serialise request/response turns on
// the underlying Conn.
//
// It deliberately keeps no copy of the handshake inputs (the restore id) or of the
// server's "alive" reply: both were write-only fields no caller - and no accessor -
// ever read, so they were removed rather than left as state that looks meaningful.
// The restore id belongs to the caller's ConnectConfig, and
// "alive" is validated inside exchangeEncryptChannel, which fails the handshake when
// it is missing.
type Session struct {
	conn    *sproto.Conn
	pin     string
	spkiPin string
	derPin  string
	// peerCerts is the chain the server presented, leaf first, for VerifyServerCertificate.
	peerCerts []*x509.Certificate
}

// Connect establishes a fully-upgraded Session: TCP → encrypt_channel → TLS with
// cert pinning. With an empty cfg.Pin the leaf fingerprint is captured (pairing)
// and exposed via Session.Pin; otherwise the leaf must equal cfg.Pin (reuse) or
// the upgrade fails with ErrPinMismatch. On any error the socket is closed.
func Connect(ctx context.Context, cfg ConnectConfig) (*Session, error) {
	d := cfg.Dialer
	if d == nil {
		d = defaultDialer
	}
	conn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", cfg.Addr, err)
	}
	// Bound the plaintext encrypt_channel round-trip by ctx: unlike the TLS
	// upgrade (HandshakeContext) it has no other cancellation, so without this a
	// server that never replies would hang ReadResponse forever. netctx.Watch
	// honours both a context deadline and an outright cancellation; stop() ends
	// the watcher once the handshake (encrypt_channel + TLS) has completed.
	stop := netctx.Watch(ctx, conn)
	defer stop()
	c := sproto.NewConn(conn)
	// The reply's "alive" value is validated by exchangeEncryptChannel (a reply
	// without it fails the handshake) and carries nothing the client acts on later,
	// so it is not retained.
	if _, err := exchangeEncryptChannel(c, cfg.Agent, cfg.DsID); err != nil {
		_ = conn.Close()
		return nil, netctx.Err(ctx, err)
	}

	tlsCfg := &sproto.TLSConfig{
		ServerName:         serverNameFor(cfg),
		InsecureSkipVerify: true, // trust is established by the pin check below
		// MinVersion is left at the sproto default (TLS 1.2). The SPKI/leaf pin -
		// not the negotiated TLS version - is this client's trust anchor, and it is
		// enforced identically on a 1.2 or 1.3 handshake, so a version floor adds no
		// security here. We keep 1.2 rather than forcing 1.3 because the supported
		// DSM matrix includes releases that do not negotiate TLS 1.3; a 1.3 floor
		// would refuse those servers for no trust gain. Revisit (bump to
		// tls.VersionTLS13) once every supported DSM speaks 1.3.
	}
	// Always record the SPKI fingerprint of the actually-presented leaf, in both
	// modes, so the caller can auto-migrate a legacy DER pin to the SPKI form
	// (Session.SPKIPin) once the same certificate verifies. In verify mode the
	// pinVerifier remains the authority; the capture is best-effort and never
	// overrides a verify failure.
	//
	// The pin check is wired through VerifyConnection rather than
	// VerifyPeerCertificate so it fires on a resumed handshake too - crypto/tls
	// skips VerifyPeerCertificate on session resumption, which would otherwise let
	// a resumed connection bypass the pin entirely.
	capturer := &pinCapturer{}
	if cfg.Pin == "" {
		tlsCfg.VerifyConnection = capturer.VerifyConn
	} else {
		verify := pinConnVerifier(cfg.Pin)
		tlsCfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if err := verify(cs); err != nil {
				return err
			}
			_ = capturer.VerifyConn(cs) // record presented-leaf SPKI; non-fatal
			return nil
		}
	}
	if err := c.UpgradeTLS(ctx, tlsCfg); err != nil {
		_ = conn.Close()
		return nil, netctx.Err(ctx, err)
	}

	pin := cfg.Pin
	if cfg.Pin == "" {
		pin = capturer.Pin()
	}
	return &Session{
		conn:      c,
		pin:       pin,
		spkiPin:   capturer.Pin(),
		derPin:    capturer.DERPin(),
		peerCerts: capturer.certs,
	}, nil
}

// serverNameFor resolves the SNI value: cfg.ServerName when set, else the host
// part of cfg.Addr.
func serverNameFor(cfg ConnectConfig) string {
	if cfg.ServerName != "" {
		return cfg.ServerName
	}
	if host, _, err := net.SplitHostPort(cfg.Addr); err == nil {
		return host
	}
	return cfg.Addr
}

// Conn returns the underlying sproto.Conn for issuing post-handshake actions.
func (s *Session) Conn() *sproto.Conn { return s.conn }

// Pin returns the leaf fingerprint in force for this session: the captured pin in
// pairing mode, or the verified expected pin in reuse mode.
func (s *Session) Pin() string { return s.pin }

// SPKIPin returns the SPKI fingerprint of the certificate actually presented by
// the server on this connection, regardless of the pin format in force. It lets
// the caller migrate a legacy DER pin to the SPKI form once the same certificate
// has verified. It is empty only if the presented leaf could not be parsed.
func (s *Session) SPKIPin() string { return s.spkiPin }

// DERPin returns the legacy full-DER fingerprint of the certificate actually
// presented by the server on this connection. It lets `pair --expect-pin` match a
// pre-SPKI pin an operator recorded out-of-band. It is empty only if no leaf was
// presented.
func (s *Session) DERPin() string { return s.derPin }

// Close closes the underlying connection.
func (s *Session) Close() error { return s.conn.Close() }

// PeerCertificates is the chain the server presented, leaf first.
func (s *Session) PeerCertificates() []*x509.Certificate { return s.peerCerts }
