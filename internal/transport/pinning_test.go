package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

// connStateFor builds a tls.ConnectionState carrying the given leaf DER as the
// peer's leaf certificate, the shape the VerifyConnection adapters consume.
func connStateFor(t *testing.T, der []byte) tls.ConnectionState {
	t.Helper()
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
}

// TestPinConnVerifierMatch confirms the VerifyConnection adapter accepts a cert
// matching the SPKI pin (the path that runs on both full and resumed handshakes).
func TestPinConnVerifierMatch(t *testing.T) {
	der1, key := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, key, 2)
	pin, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	verify := pinConnVerifier(pin)
	if err := verify(connStateFor(t, der2)); err != nil {
		t.Errorf("pinConnVerifier on re-issued cert with same key: %v", err)
	}
}

func TestPinConnVerifierMismatch(t *testing.T) {
	der1, _ := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, nil, 1)
	pin, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	verify := pinConnVerifier(pin)
	if err := verify(connStateFor(t, der2)); !errors.Is(err, ErrPinMismatch) {
		t.Errorf("pinConnVerifier different key: got %v, want ErrPinMismatch", err)
	}
}

func TestPinConnVerifierNoCert(t *testing.T) {
	verify := pinConnVerifier("spki-sha256:whatever")
	if err := verify(tls.ConnectionState{}); !errors.Is(err, errNoPeerCert) {
		t.Errorf("pinConnVerifier with no certs: got %v, want errNoPeerCert", err)
	}
}

func TestPinCapturerVerifyConn(t *testing.T) {
	der, _ := certWithKey(t, nil, 1)
	want, err := FingerprintSPKI(der)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	var p pinCapturer
	if err := p.VerifyConn(connStateFor(t, der)); err != nil {
		t.Fatalf("VerifyConn: %v", err)
	}
	if p.Pin() != want {
		t.Errorf("captured pin = %q, want SPKI %q", p.Pin(), want)
	}
	if wantDER := Fingerprint(der); p.DERPin() != wantDER {
		t.Errorf("captured DERPin = %q, want legacy DER %q", p.DERPin(), wantDER)
	}
}

// certWithKey builds a self-signed ECDSA certificate (DER) for the given key,
// using serial as the only varying field so two certs sharing a key differ in
// DER yet carry an identical SubjectPublicKeyInfo. A nil key generates a fresh
// keypair; the key actually used is returned for reuse.
func certWithKey(t *testing.T, key *ecdsa.PrivateKey, serial int64) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	if key == nil {
		var err error
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "synosync-test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31-1, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return der, key
}

func TestFingerprintDeterministic(t *testing.T) {
	der := []byte("a fake DER blob")
	got := Fingerprint(der)
	if !strings.HasPrefix(got, PinPrefix) {
		t.Errorf("Fingerprint = %q, want %q prefix", got, PinPrefix)
	}
	// SHA-256 hex is 64 chars; with the "sha256:" prefix the total is 71.
	if want := len(PinPrefix) + 64; len(got) != want {
		t.Errorf("Fingerprint length = %d, want %d", len(got), want)
	}
	if again := Fingerprint(der); again != got {
		t.Errorf("Fingerprint not deterministic: %q vs %q", got, again)
	}
	if other := Fingerprint([]byte("different bytes")); other == got {
		t.Error("Fingerprint collided for different inputs")
	}
}

// TestFingerprintSPKISameKey is the core SPKI invariant: two distinct
// certificates issued from one keypair produce the same SPKI pin (so a re-issue
// survives) but different full-DER pins.
func TestFingerprintSPKISameKey(t *testing.T) {
	der1, key := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, key, 2)

	spki1, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI(der1): %v", err)
	}
	spki2, err := FingerprintSPKI(der2)
	if err != nil {
		t.Fatalf("FingerprintSPKI(der2): %v", err)
	}
	if !strings.HasPrefix(spki1, SPKIPrefix) {
		t.Errorf("SPKI pin = %q, want %q prefix", spki1, SPKIPrefix)
	}
	if want := len(SPKIPrefix) + 64; len(spki1) != want {
		t.Errorf("SPKI pin length = %d, want %d", len(spki1), want)
	}
	if spki1 != spki2 {
		t.Errorf("SPKI pins differ for same key: %q vs %q", spki1, spki2)
	}
	if Fingerprint(der1) == Fingerprint(der2) {
		t.Error("DER pins identical for two different certs; serial should differ them")
	}
}

// TestFingerprintSPKIDifferentKeys confirms different keypairs yield different
// SPKI pins.
func TestFingerprintSPKIDifferentKeys(t *testing.T) {
	der1, _ := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, nil, 1)
	spki1, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI(der1): %v", err)
	}
	spki2, err := FingerprintSPKI(der2)
	if err != nil {
		t.Fatalf("FingerprintSPKI(der2): %v", err)
	}
	if spki1 == spki2 {
		t.Error("SPKI pins collided for different keys")
	}
}

func TestFingerprintSPKIBadDER(t *testing.T) {
	if _, err := FingerprintSPKI([]byte("not a certificate")); err == nil {
		t.Error("FingerprintSPKI accepted invalid DER")
	}
}

func TestIsLegacyPin(t *testing.T) {
	cases := []struct {
		pin  string
		want bool
	}{
		{"sha256:abcdef", true},
		{"spki-sha256:abcdef", false},
		{"", false},
		{"other:abcdef", false},
	}
	for _, c := range cases {
		if got := IsLegacyPin(c.pin); got != c.want {
			t.Errorf("IsLegacyPin(%q) = %v, want %v", c.pin, got, c.want)
		}
	}
}

func TestPinCapturerCapturesSPKI(t *testing.T) {
	der, _ := certWithKey(t, nil, 1)
	want, err := FingerprintSPKI(der)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	var p pinCapturer
	if err := p.Verify([][]byte{der, []byte("intermediate")}, nil); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if p.Pin() != want {
		t.Errorf("captured pin = %q, want SPKI %q", p.Pin(), want)
	}
}

func TestPinCapturerNoCert(t *testing.T) {
	var p pinCapturer
	if err := p.Verify(nil, nil); !errors.Is(err, errNoPeerCert) {
		t.Errorf("Verify with no certs: got %v, want errNoPeerCert", err)
	}
}

func TestPinCapturerBadDER(t *testing.T) {
	var p pinCapturer
	if err := p.Verify([][]byte{[]byte("garbage")}, nil); err == nil {
		t.Error("Verify accepted an unparseable leaf")
	}
}

// TestPinVerifierSPKIMatch verifies an SPKI pin against a re-issued cert sharing
// the original key - the headline win of Phase 9e.
func TestPinVerifierSPKIMatch(t *testing.T) {
	der1, key := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, key, 2)
	pin, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	verify := pinVerifier(pin)
	if err := verify([][]byte{der2}, nil); err != nil {
		t.Errorf("Verify re-issued cert with same key: %v", err)
	}
}

func TestPinVerifierSPKIMismatch(t *testing.T) {
	der1, _ := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, nil, 1)
	pin, err := FingerprintSPKI(der1)
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	verify := pinVerifier(pin)
	err = verify([][]byte{der2}, nil)
	if !errors.Is(err, ErrPinMismatch) {
		t.Errorf("Verify different key: got %v, want ErrPinMismatch", err)
	}
	// The mismatch carries both fingerprints (got/want).
	got2, _ := FingerprintSPKI(der2)
	if !strings.Contains(err.Error(), got2) || !strings.Contains(err.Error(), pin) {
		t.Errorf("mismatch error %q missing got/want fingerprints", err)
	}
}

// TestPinVerifierLegacyMatch confirms a stored legacy full-DER pin still verifies
// against the exact same leaf.
func TestPinVerifierLegacyMatch(t *testing.T) {
	der, _ := certWithKey(t, nil, 1)
	verify := pinVerifier(Fingerprint(der))
	if err := verify([][]byte{der}, nil); err != nil {
		t.Errorf("Verify matching leaf against legacy pin: %v", err)
	}
}

// TestPinVerifierLegacyReissueFails shows the legacy pin's limitation: a re-issue
// with the same key no longer matches the full-DER pin (the regression Phase 9e
// fixes for SPKI pins).
func TestPinVerifierLegacyReissueFails(t *testing.T) {
	der1, key := certWithKey(t, nil, 1)
	der2, _ := certWithKey(t, key, 2)
	verify := pinVerifier(Fingerprint(der1))
	if err := verify([][]byte{der2}, nil); !errors.Is(err, ErrPinMismatch) {
		t.Errorf("legacy pin against re-issued cert: got %v, want ErrPinMismatch", err)
	}
}

func TestPinVerifierMismatch(t *testing.T) {
	verify := pinVerifier(Fingerprint([]byte("expected leaf")))
	err := verify([][]byte{[]byte("attacker leaf")}, nil)
	if !errors.Is(err, ErrPinMismatch) {
		t.Errorf("Verify mismatched leaf: got %v, want ErrPinMismatch", err)
	}
}

func TestPinVerifierNoCert(t *testing.T) {
	verify := pinVerifier("sha256:whatever")
	if err := verify(nil, nil); !errors.Is(err, errNoPeerCert) {
		t.Errorf("Verify with no certs: got %v, want errNoPeerCert", err)
	}
}

func TestPinVerifierSPKINoCert(t *testing.T) {
	verify := pinVerifier("spki-sha256:whatever")
	if err := verify(nil, nil); !errors.Is(err, errNoPeerCert) {
		t.Errorf("Verify with no certs: got %v, want errNoPeerCert", err)
	}
}
