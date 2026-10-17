package main

// User-facing error text.
//
// The internal packages follow the project's convention of stamping their own name on
// every error they return, which is right for a log line and for the next caller up, and
// wrong for the last line a person reads. Those prefixes accumulate down a wrapped chain,
// so a mistyped hostname reached the terminal as
//
//	synosync: connect nas:6690: transport: dial: dial tcp: lookup nas: no such host
//
// where "transport:" names a Go package the user cannot act on, cannot find, and did not
// ask about. The prefix is stripped at the boundary rather than in the packages, which are
// right to identify themselves to every other caller.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
)

// internalPrefixes are the package tags the internal packages stamp on their errors. They
// are listed rather than matched by a general `\w+: ` rule because a legitimate message
// can begin with a word and a colon - a path, a field name, a server verb - and eating
// that would lose real information to save a cosmetic prefix.
var internalPrefixes = []string{
	"actions:", "auth:", "backoff:", "config:", "daemon:", "engine:", "fsutil:",
	"ipc:", "logging:", "md4:", "netctx:", "normpath:", "secrets:", "servertree:",
	"service:", "sproto:", "store:", "transport:", "watcher:",
}

// userErr renders err for a person. Leading internal package tags are removed, including
// several in a row: a config error surfacing through the store arrives as
// "store: config: ..." and both tags describe the plumbing rather than the mistake.
//
// Only LEADING tags go. A tag deeper in the chain still marks where a failure came from
// and is often the only thing distinguishing two otherwise identical messages, so the
// stripping stops at the first segment that is not one.
func userErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	for {
		trimmed := false
		for _, p := range internalPrefixes {
			if strings.HasPrefix(msg, p) {
				msg = strings.TrimSpace(strings.TrimPrefix(msg, p))
				trimmed = true
				break
			}
		}
		if !trimmed {
			return msg
		}
	}
}

// connectHint names the likely cause of a failed first contact with the server, or
// returns "" when the error says nothing a person could act on. The raw error stays on
// the line above it: the hint is a reading of that error, not a replacement for it.
//
// The cases are the ones a fresh setup actually hits. A port that answers with something
// other than sproto - the DSM web UI on 5000/5001, a reverse proxy on 443 - fails with a
// header magic that is really the first bytes of "HTTP", which reads as a protocol bug
// rather than as the wrong port.
func connectHint(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, sproto.ErrBadMagic) || errors.Is(err, sproto.ErrBadVersion) {
		return "the service on that port is not the Synology Drive client endpoint; " +
			"check the host and port (the Drive client port is 6690 by default, not the DSM web port)"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "the host name could not be resolved; check the host and this machine's DNS"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "the server did not answer in time; check that it is reachable from this machine"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		if opErr.Timeout() {
			return "the server did not answer in time; check that it is reachable from this machine"
		}
		return "nothing accepted the connection; check that Synology Drive Server is running " +
			"and that its client port (6690 by default) is reachable from this machine"
	}
	return ""
}

// printConnectErr reports a failed connection step ("probe", "connect") to addr, followed
// by a hint line when connectHint recognises the cause.
func printConnectErr(w io.Writer, step, addr string, err error) {
	fmt.Fprintf(w, "synosync: %s %s: %s\n", step, addr, userErr(err))
	if hint := connectHint(err); hint != "" {
		fmt.Fprintf(w, "synosync: hint: %s\n", hint)
	}
}

// keyringHint returns this platform's keychain requirement when err is a failure of the
// keychain backend itself, or "" otherwise. An absent item is not one: that is a pairing
// question, and its callers already say so.
func keyringHint(err error) string {
	if !errors.Is(err, secrets.ErrUnavailable) {
		return ""
	}
	return keychainRequirement
}

// printKeyringErr reports a failed keychain step (what) followed by a hint line when the
// backend itself is unavailable.
func printKeyringErr(w io.Writer, what string, err error) {
	fmt.Fprintf(w, "synosync: %s: %s\n", what, userErr(err))
	if hint := keyringHint(err); hint != "" {
		fmt.Fprintf(w, "synosync: hint: %s\n", hint)
	}
}
