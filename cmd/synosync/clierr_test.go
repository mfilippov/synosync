package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
)

// TestUserErrStripsInternalPrefixes pins what the last line a person reads may contain.
// The internal packages name themselves in every error, which is right for a log line and
// for the next caller up, and wrong here: a mistyped hostname used to reach the terminal
// as "connect nas:6690: transport: dial: ... no such host", where the one word the user
// cannot act on came first.
func TestUserErrStripsInternalPrefixes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"no prefix", errors.New("no such host"), "no such host"},
		{"one prefix", errors.New("transport: dial tcp: no such host"), "dial tcp: no such host"},
		// Both tags describe plumbing: a config error surfacing through the store carries
		// the store's tag over the config one, and neither says anything about the mistake.
		{"stacked prefixes", errors.New("store: config: limits.workers 0 out of range"), "limits.workers 0 out of range"},
		// A tag DEEPER in the chain stays: it says where the failure came from, and is
		// often all that distinguishes two otherwise identical messages.
		{"inner prefix kept", errors.New("connect nas:6690: transport: dial: refused"), "connect nas:6690: transport: dial: refused"},
		// A leading word with a colon that is not a package tag is content, not decoration.
		{"lookalike kept", errors.New("Photos: permission denied"), "Photos: permission denied"},
		{"wrapped", fmt.Errorf("store: %w", errors.New("open state db: locked")), "open state db: locked"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := userErr(tc.err); got != tc.want {
				t.Fatalf("userErr(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestInternalPrefixesCoverEveryPackage guards the list against a new internal package
// whose tag nobody remembered to add. The check is by construction - each entry is the
// package's own directory name plus a colon - so the failure names exactly what to add.
func TestInternalPrefixesCoverEveryPackage(t *testing.T) {
	// The packages whose errors can reach a CLI message. md4 and logging are here for
	// completeness rather than because a user-facing path returns their errors today.
	pkgs := []string{
		"actions", "auth", "backoff", "config", "daemon", "engine", "fsutil", "ipc",
		"logging", "md4", "netctx", "normpath", "secrets", "servertree", "service",
		"sproto", "store", "transport", "watcher",
	}
	have := make(map[string]bool, len(internalPrefixes))
	for _, p := range internalPrefixes {
		have[p] = true
	}
	for _, p := range pkgs {
		if !have[p+":"] {
			t.Errorf("internal/%s is not in internalPrefixes; its errors would reach the user tagged", p)
		}
	}
	if len(internalPrefixes) != len(pkgs) {
		t.Errorf("internalPrefixes has %d entries for %d packages; a stale entry strips text it should not",
			len(internalPrefixes), len(pkgs))
	}
}

// TestConnectHint pins which first-contact failures earn a hint and which one. Each error
// is wrapped the way transport.TestConn wraps it, so the test exercises the chain a real
// failure arrives in rather than the bare cause.
func TestConnectHint(t *testing.T) {
	dial := func(cause error) error {
		return fmt.Errorf("transport: dial nas:6690: %w", &net.OpError{Op: "dial", Net: "tcp", Err: cause})
	}
	cases := []struct {
		name string
		err  error
		want string // a substring of the hint; "" means no hint
	}{
		{"nil", nil, ""},
		// The DSM web UI answers "HTTP/1.1 ...": its first four bytes are the magic it fails on.
		{"http on the port", fmt.Errorf("transport: TEST_CONN reply header: %w: 0x48545450", sproto.ErrBadMagic), "not the Synology Drive client endpoint"},
		{"bad version", fmt.Errorf("transport: TEST_CONN reply header: %w", sproto.ErrBadVersion), "not the Synology Drive client endpoint"},
		{"unknown host", dial(&net.DNSError{Err: "no such host", Name: "nas", IsNotFound: true}), "could not be resolved"},
		{"refused", dial(errors.New("connection refused")), "nothing accepted the connection"},
		{"dns timeout", dial(&net.DNSError{Err: "i/o timeout", IsTimeout: true}), "could not be resolved"},
		{"dial timeout", dial(os.ErrDeadlineExceeded), "did not answer in time"},
		{"deadline", fmt.Errorf("transport: read TEST_CONN reply: %w", context.DeadlineExceeded), "did not answer in time"},
		// Anything the hint cannot read stays unannotated rather than guessed at.
		{"other", errors.New("transport: TEST_CONN reply exceeds 4096 bytes"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := connectHint(tc.err)
			if tc.want == "" {
				if got != "" {
					t.Fatalf("connectHint = %q, want no hint", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Fatalf("connectHint = %q, want it to mention %q", got, tc.want)
			}
		})
	}
}

// TestPrintConnectErrAddsHintLine pins the two-line shape: the raw error first, unchanged,
// then the hint, so nothing the error said is lost to the reading of it.
func TestPrintConnectErrAddsHintLine(t *testing.T) {
	var b strings.Builder
	printConnectErr(&b, "probe", "nas:443", fmt.Errorf("transport: TEST_CONN reply header: %w: 0x48545450", sproto.ErrBadMagic))
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), b.String())
	}
	if want := "synosync: probe nas:443: TEST_CONN reply header: sproto: bad header magic: 0x48545450"; lines[0] != want {
		t.Errorf("line 1 = %q, want %q", lines[0], want)
	}
	if !strings.HasPrefix(lines[1], "synosync: hint: ") {
		t.Errorf("line 2 = %q, want a hint line", lines[1])
	}
	b.Reset()
	printConnectErr(&b, "connect", "nas:6690", errors.New("handshake failed"))
	if strings.Contains(b.String(), "hint") {
		t.Errorf("unrecognised error gained a hint: %q", b.String())
	}
}

// TestKeyringHint pins that only a backend failure earns the keychain requirement: an
// absent item is a pairing question its callers already answer, and naming D-Bus there
// would send the user the wrong way.
func TestKeyringHint(t *testing.T) {
	unavailable := fmt.Errorf("daemon: read session: %w",
		fmt.Errorf("secrets: read %q: %w: %w", "session/nas/alice", secrets.ErrUnavailable, errors.New("dbus: no session bus")))
	if got := keyringHint(unavailable); got != keychainRequirement {
		t.Errorf("keyringHint(unavailable) = %q, want the platform requirement", got)
	}
	notFound := fmt.Errorf("daemon: read session: %w", fmt.Errorf("%w: session/nas/alice", secrets.ErrNotFound))
	if got := keyringHint(notFound); got != "" {
		t.Errorf("keyringHint(not found) = %q, want none", got)
	}

	var b strings.Builder
	printKeyringErr(&b, "store session", unavailable)
	out := b.String()
	if !strings.HasPrefix(out, "synosync: store session: read session: ") || !strings.Contains(out, "\nsynosync: hint: "+keychainRequirement+"\n") {
		t.Errorf("printKeyringErr output:\n%s", out)
	}
	if keychainRequirement == "" {
		t.Error("this platform has no keychain requirement text")
	}
}
