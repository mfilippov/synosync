// Package transport establishes the authenticated connection to a Synology Drive
// server: the TEST_CONN probe, the pre-TLS encrypt_channel handshake, the in-place
// TLS upgrade with leaf cert-pinning, the Session primitive that ties them
// together, and a reconnect backoff helper.
//
// This file implements certificate pinning. synosync does not trust the server's
// certificate chain; instead it records a SHA-256 fingerprint of the leaf
// certificate observed during pairing and requires every later connection to
// present a matching certificate. The pin is the SHA-256 of the leaf's
// SubjectPublicKeyInfo (SPKI), so a routine re-issue with the same keypair keeps
// the same pin; legacy pins over the full leaf DER are still honoured for
// backward compatibility. The TLS upgrade therefore runs with InsecureSkipVerify
// set and delegates the trust decision to one of the callbacks below (see
// sproto.TLSConfig.VerifyPeerCertificate).
package transport

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// PinPrefix labels the hash algorithm in a legacy pin string so the stored
// format can evolve without ambiguity. A legacy pin is "sha256:" followed by the
// lowercase hex SHA-256 of the full leaf DER.
const PinPrefix = "sha256:"

// SPKIPrefix labels a pin over the leaf's SubjectPublicKeyInfo (SPKI): the
// lowercase hex SHA-256 of RawSubjectPublicKeyInfo. An SPKI pin survives a
// certificate re-issue with the same keypair, unlike a full-DER pin. The "spki-"
// prefix lets pinVerifier dispatch on format without a storage migration.
const SPKIPrefix = "spki-sha256:"

var (
	// ErrPinMismatch reports that the peer's leaf certificate does not match the
	// pinned fingerprint - the signature of a MITM or a server key rotation.
	ErrPinMismatch = errors.New("transport: certificate pin mismatch")
	// errNoPeerCert reports that the TLS peer presented no certificate, so no pin
	// could be computed.
	errNoPeerCert = errors.New("transport: no peer certificate presented")
)

// Fingerprint returns the legacy pin string for a leaf certificate's raw DER
// bytes: the SHA-256 digest in lowercase hex, prefixed with PinPrefix. New pins
// use FingerprintSPKI; Fingerprint remains for verifying already-stored pins and
// for tests.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return PinPrefix + hex.EncodeToString(sum[:])
}

// FingerprintSPKI returns the SPKI pin for a leaf certificate's raw DER bytes:
// the lowercase hex SHA-256 of the certificate's RawSubjectPublicKeyInfo,
// prefixed with SPKIPrefix. An invalid DER blob is reported as an error.
func FingerprintSPKI(der []byte) (string, error) {
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return "", fmt.Errorf("transport: parse leaf certificate: %w", err)
	}
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return SPKIPrefix + hex.EncodeToString(sum[:]), nil
}

// IsLegacyPin reports whether a stored pin is in the legacy full-DER format
// ("sha256:" without the "spki-" prefix), so callers can transparently migrate
// it to the SPKI form on the next successful connection.
func IsLegacyPin(pin string) bool {
	return strings.HasPrefix(pin, PinPrefix) && !strings.HasPrefix(pin, SPKIPrefix)
}

// leafSPKIFingerprint computes the SPKI pin of the first (leaf) raw certificate,
// or errNoPeerCert when none was presented.
func leafSPKIFingerprint(rawCerts [][]byte) (string, error) {
	if len(rawCerts) == 0 {
		return "", errNoPeerCert
	}
	return FingerprintSPKI(rawCerts[0])
}

// leafDERFingerprint computes the legacy full-DER pin of the first (leaf) raw
// certificate, or errNoPeerCert when none was presented.
func leafDERFingerprint(rawCerts [][]byte) (string, error) {
	if len(rawCerts) == 0 {
		return "", errNoPeerCert
	}
	return Fingerprint(rawCerts[0]), nil
}

// pinCapturer records the leaf's SPKI fingerprint seen during a TLS handshake. It
// is the pairing-mode hook: it accepts any certificate and exposes its pin via
// Pin so the caller can persist it. Use pinVerifier for the verify-on-reuse path.
// It also records the leaf's legacy full-DER fingerprint (DERPin) so an operator
// who recorded a pre-SPKI pin out-of-band can still match it via --expect-pin.
type pinCapturer struct {
	pin    string
	derPin string
	certs  []*x509.Certificate
}

// Verify satisfies sproto.TLSConfig.VerifyPeerCertificate: it records the leaf's
// SPKI and legacy full-DER fingerprints and accepts the handshake.
func (p *pinCapturer) Verify(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	fp, err := leafSPKIFingerprint(rawCerts)
	if err != nil {
		return err
	}
	der, err := leafDERFingerprint(rawCerts)
	if err != nil {
		return err
	}
	p.pin = fp
	p.derPin = der
	return nil
}

// VerifyConn adapts Verify to a sproto.TLSConfig.VerifyConnection callback so the
// capture runs on both full and resumed handshakes. The peer certificates in the
// connection state are presented leaf-first, matching the rawCerts shape Verify
// expects (cs.PeerCertificates is populated on resumed connections too).
func (p *pinCapturer) VerifyConn(cs tls.ConnectionState) error {
	p.certs = cs.PeerCertificates
	return p.Verify(rawCertsFromState(cs), cs.VerifiedChains)
}

// Pin returns the captured SPKI fingerprint, or "" if Verify has not yet run.
func (p *pinCapturer) Pin() string { return p.pin }

// DERPin returns the captured legacy full-DER fingerprint, or "" if Verify has
// not yet run.
func (p *pinCapturer) DERPin() string { return p.derPin }

// rawCertsFromState extracts the DER bytes of each peer certificate, leaf-first,
// from a TLS connection state - the rawCerts shape the pin helpers expect. The
// state's PeerCertificates are populated on a resumed session as well, so a pin
// check wired through VerifyConnection holds across session resumption.
func rawCertsFromState(cs tls.ConnectionState) [][]byte {
	raw := make([][]byte, len(cs.PeerCertificates))
	for i, c := range cs.PeerCertificates {
		raw[i] = c.Raw
	}
	return raw
}

// pinVerifier returns a sproto.TLSConfig.VerifyPeerCertificate callback that
// accepts the handshake only when the peer's leaf matches expected, failing with
// ErrPinMismatch otherwise. It dispatches on the pin format: an SPKI pin
// ("spki-sha256:") is compared against the presented leaf's SPKI hash, a legacy
// pin ("sha256:") against the presented leaf's full-DER hash, so already-stored
// legacy pins keep verifying until migrated.
func pinVerifier(expected string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		var (
			fp  string
			err error
		)
		if IsLegacyPin(expected) {
			fp, err = leafDERFingerprint(rawCerts)
		} else {
			fp, err = leafSPKIFingerprint(rawCerts)
		}
		if err != nil {
			return err
		}
		if fp != expected {
			return fmt.Errorf("%w: got %s, want %s", ErrPinMismatch, fp, expected)
		}
		return nil
	}
}

// pinConnVerifier returns a sproto.TLSConfig.VerifyConnection callback that
// enforces the pin on every handshake, full or resumed. It wraps pinVerifier:
// crypto/tls skips VerifyPeerCertificate on a resumed session but always runs
// VerifyConnection, so wiring the pin here closes the resumption-bypass footgun.
func pinConnVerifier(expected string) func(tls.ConnectionState) error {
	verify := pinVerifier(expected)
	return func(cs tls.ConnectionState) error {
		return verify(rawCertsFromState(cs), cs.VerifiedChains)
	}
}
