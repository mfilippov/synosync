package transport

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// VerifyServerCertificate checks the presented chain the way a browser would: up to a root
// this system trusts, for host, and in date. Trust in this client rests on the pin captured
// at pairing, not on this check (Connect skips chain verification); pairing runs it once so a
// first contact with a certificate nothing vouches for is not silent — the official client
// warns in the same place. On Windows and macOS crypto/x509 asks the platform verifier, so
// roots an administrator installed count.
func VerifyServerCertificate(certs []*x509.Certificate, host string) error {
	return verifyServerCertificate(certs, host, nil, time.Now())
}

// verifyServerCertificate is VerifyServerCertificate with the roots (nil: the system's) and
// the clock injectable, for tests.
func verifyServerCertificate(certs []*x509.Certificate, host string, roots *x509.CertPool, now time.Time) error {
	if len(certs) == 0 {
		return errors.New("the server presented no certificate")
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	_, err := certs[0].Verify(x509.VerifyOptions{
		DNSName:       strings.TrimSuffix(host, "."),
		Intermediates: inter,
		Roots:         roots,
		CurrentTime:   now,
	})
	return err
}

// DescribeCertificate renders a leaf for a person deciding whether to trust it.
func DescribeCertificate(c *x509.Certificate) string {
	names := append([]string(nil), c.DNSNames...)
	for _, ip := range c.IPAddresses {
		names = append(names, ip.String())
	}
	subject := c.Subject.CommonName
	if subject == "" && len(names) > 0 {
		subject = names[0]
	}
	s := fmt.Sprintf("issued to %q by %q, valid until %s", subject, c.Issuer.CommonName, c.NotAfter.Format("2006-01-02"))
	if len(names) > 0 {
		s += ", for " + strings.Join(names, ", ")
	}
	return s
}

// isIP reports whether host is a literal address, the shape a certificate rarely names.
func isIP(host string) bool { return net.ParseIP(strings.Trim(host, "[]")) != nil }

// TrustHint explains a failed VerifyServerCertificate in the terms a person can act on.
func TrustHint(err error, host string) string {
	var hn x509.HostnameError
	var ua x509.UnknownAuthorityError
	var inv x509.CertificateInvalidError
	switch {
	case errors.As(err, &hn) && isIP(host):
		return "the certificate does not name the address " + host + "; pair by the host name it was issued for"
	case errors.As(err, &hn):
		return "the certificate was issued for another name"
	case errors.As(err, &ua):
		return "no root this computer trusts vouches for it (a self-signed certificate, or a missing intermediate)"
	case errors.As(err, &inv) && inv.Reason == x509.Expired:
		return "the certificate is expired or not yet valid"
	default:
		return err.Error()
	}
}
