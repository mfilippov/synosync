package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
)

// countingPrompter records whether the password was asked for, and refuses the certificate.
type countingPrompter struct {
	fakePrompter
	passwordAsked bool
}

func (c *countingPrompter) ReadPassword(p string) (string, error) {
	c.passwordAsked = true
	return c.fakePrompter.ReadPassword(p)
}

func trustMock(t *testing.T) *mockServer {
	return newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String("abcdef0123456789abcdef0123456789"))
	})
}

// TestPairStopsOnAnUntrustedCertificateBeforeThePassword: the mock serves a self-signed
// certificate; refusing it ends the pairing before any credential is asked for or sent.
func TestPairStopsOnAnUntrustedCertificateBeforeThePassword(t *testing.T) {
	srv := trustMock(t)
	p := &countingPrompter{fakePrompter: fakePrompter{password: "s3cret", refuseCert: true}}
	path := setupCLI(t, p)
	writeServerConfig(t, path, srv, "alice", nil)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code == 0 {
		t.Fatal("pair succeeded after the certificate was refused")
	}
	if p.passwordAsked {
		t.Error("the password was asked for before the certificate was trusted")
	}
	if !strings.Contains(stderr.String(), "not trusted by this computer") || !strings.Contains(stderr.String(), "--accept-untrusted") {
		t.Errorf("stderr = %q, want the warning and the way forward", stderr.String())
	}
	if _, err := secrets.New().Pin(srv.addr()); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("a pin was stored for a refused certificate: %v", err)
	}
}

// TestPairTrustNeedsNoQuestionWhenConfirmedOtherwise: --accept-untrusted, or a matching
// --expect-pin, is the answer, so nothing is asked.
func TestPairTrustNeedsNoQuestionWhenConfirmedOtherwise(t *testing.T) {
	for _, extra := range []func(*mockServer) []string{
		func(*mockServer) []string { return []string{"--accept-untrusted"} },
		func(s *mockServer) []string { return []string{"--expect-pin", s.leafSPKIPin()} },
	} {
		srv := trustMock(t)
		p := &fakePrompter{password: "s3cret", refuseCert: true}
		path := setupCLI(t, p)
		writeServerConfig(t, path, srv, "alice", nil)
		var stdout, stderr bytes.Buffer
		args := append([]string{"pair"}, extra(srv)...)
		if code := run(args, &stdout, &stderr); code != 0 {
			t.Fatalf("%v: exit %d (stderr %q)", args, code, stderr.String())
		}
		if p.confirmed != 0 {
			t.Errorf("%v: asked to confirm %d time(s), want none", args, p.confirmed)
		}
	}
}
