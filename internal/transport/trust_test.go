package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"
)

func mkCert(t *testing.T, cn string, dns []string, isCA bool, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		DNSNames:              dns,
		NotBefore:             time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		IsCA:                  isCA,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c, key
}

// TestVerifyServerCertificate covers each verdict pairing acts on: a chain to a trusted root for
// the host passes; another name, a bare address, an expired leaf and an unknown root do not, and
// each failure gets a hint a person can act on.
func TestVerifyServerCertificate(t *testing.T) {
	ca, caKey := mkCert(t, "Test Root", nil, true, nil, nil)
	leaf, _ := mkCert(t, "nas.example.com", []string{"*.example.com"}, false, ca, caKey)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	chain := []*x509.Certificate{leaf}

	if err := verifyServerCertificate(chain, "charon.example.com", roots, now); err != nil {
		t.Errorf("wildcard leaf for its host: %v, want trusted", err)
	}
	for _, c := range []struct {
		host  string
		roots *x509.CertPool
		now   time.Time
		hint  string
	}{
		{"nas.other.org", roots, now, "another name"},
		{"192.168.1.5", roots, now, "does not name the address 192.168.1.5"},
		{"nas.example.com", roots, time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), "expired"},
		{"nas.example.com", x509.NewCertPool(), now, "no root this computer trusts"},
	} {
		err := verifyServerCertificate(chain, c.host, c.roots, c.now)
		if err == nil {
			t.Errorf("%s: trusted, want refused", c.host)
			continue
		}
		if h := TrustHint(err, c.host); !strings.Contains(h, c.hint) {
			t.Errorf("%s: hint %q, want it to mention %q", c.host, h, c.hint)
		}
	}
	if err := VerifyServerCertificate(nil, "x"); err == nil {
		t.Error("an empty chain was trusted")
	}
	if d := DescribeCertificate(leaf); !strings.Contains(d, `"nas.example.com"`) || !strings.Contains(d, `"Test Root"`) {
		t.Errorf("DescribeCertificate = %q", d)
	}
}
