package main

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

// helpExitCommands are the subcommands whose `--help` must exit 0 (after the
// flag package prints usage) rather than the usage-error code 2. They are all
// resolved before any daemon/store side effect, so no fixture is required.
//
// `version` parses no FlagSet - it takes neither flags nor arguments - so it does
// not get flag.ErrHelp's exit-0 mapping for free and answered a help request with
// the usage-error code 2. It is listed here because the
// contract is the CLI's, not any one command's.
var helpExitCommands = []string{
	"status", "sync", "pause", "resume", "conflicts",
	"pair", "login", "daemon", "unlink", "version",
}

// TestSubcommandHelpExitsZero confirms `synosync <cmd> --help`/`-h` exits 0.
func TestSubcommandHelpExitsZero(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, cmd := range helpExitCommands {
		for _, flag := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			if code := run([]string{cmd, flag}, &stdout, &stderr); code != 0 {
				t.Errorf("%s %s exit = %d, want 0 (stderr: %q)", cmd, flag, code, stderr.String())
			}
		}
	}
}

// TestControlVerbsRejectExtraArgs confirms the no-argument control verbs reject
// an unexpected positional argument with the usage-error code 2, before dialing
// the daemon (so the rejection does not depend on a running daemon).
func TestControlVerbsRejectExtraArgs(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, cmd := range []string{"status", "pause", "resume", "conflicts"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{cmd, "bogus"}, &stdout, &stderr); code != 2 {
			t.Errorf("%s bogus exit = %d, want 2 (stderr: %q)", cmd, code, stderr.String())
		}
		assertUsageError(t, cmd, "bogus", stderr.String())
	}
}

// TestSetupVerbsRejectExtraArgs confirms the flag-only setup/lifecycle verbs
// (plus version) reject an unexpected positional argument with the usage-error
// code 2, before touching the store, keychain, prompt or network:
// `synosync pair myserver.example` (a positional host instead of
// --host) previously paired silently against the stored host, ignoring the
// argument. The prompter fails the test if any prompt fires, so the rejection
// is proven to precede the interactive flow.
func TestSetupVerbsRejectExtraArgs(t *testing.T) {
	setupCLI(t, &noPromptPrompter{t: t})

	for _, cmd := range []string{"version", "pair", "login", "daemon", "unlink"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{cmd, "bogus"}, &stdout, &stderr); code != 2 {
			t.Errorf("%s bogus exit = %d, want 2 (stderr: %q)", cmd, code, stderr.String())
		}
		assertUsageError(t, cmd, "bogus", stderr.String())
	}
}

// assertUsageError pins what a rejected positional argument must tell the user.
// The message it replaced was the bare "synosync: usage: synosync pair", which
// named neither the rejected argument nor the way out, so the test asserted only
// that the word "usage:" appeared somewhere - which that message would still
// satisfy. Assert the three things a stuck user actually needs.
func assertUsageError(t *testing.T, cmd, arg, stderr string) {
	t.Helper()
	if !strings.Contains(stderr, strconv.Quote(arg)) {
		t.Errorf("%s %s stderr = %q, want the rejected argument named", cmd, arg, stderr)
	}
	if !strings.Contains(stderr, "Usage:\n  synosync "+cmd) {
		t.Errorf("%s %s stderr = %q, want the invocation form", cmd, arg, stderr)
	}
	if !strings.Contains(stderr, "synosync "+cmd+" --help") {
		t.Errorf("%s %s stderr = %q, want a pointer at --help", cmd, arg, stderr)
	}
}

// TestPairAndLoginSuggestTheHostFlag pins the "did you mean" line: a bare
// hostname is the overwhelmingly likely thing to find in the rejected slot for
// these two commands, so the error hands back the corrected command line rather
// than making the user assemble it from the flag list.
func TestPairAndLoginSuggestTheHostFlag(t *testing.T) {
	setupCLI(t, &noPromptPrompter{t: t})

	for _, cmd := range []string{"pair", "login"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{cmd, "nas.example.com"}, &stdout, &stderr); code != 2 {
			t.Fatalf("%s exit = %d, want 2", cmd, code)
		}
		want := "synosync " + cmd + " --host nas.example.com"
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("%s stderr = %q, want the suggestion %q", cmd, stderr.String(), want)
		}
	}
}

// TestHelpGoesToStdout pins the stream split flag.FlagSet does not make: it
// writes a requested --help to the same Output as a parse error, which put every
// help page on stderr, so `synosync pair --help > cheatsheet` wrote an empty
// file. Help is output; an error is not.
func TestHelpGoesToStdout(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, cmd := range helpExitCommands {
		var stdout, stderr bytes.Buffer
		if code := run([]string{cmd, "--help"}, &stdout, &stderr); code != 0 {
			t.Fatalf("%s --help exit = %d, want 0", cmd, code)
		}
		if stdout.Len() == 0 {
			t.Errorf("%s --help wrote nothing to stdout", cmd)
		}
		if stderr.Len() != 0 {
			t.Errorf("%s --help wrote %q to stderr, want stdout only", cmd, stderr.String())
		}
	}
}

// TestParseErrorPointsAtHelp confirms an unknown flag keeps flag's own
// diagnostic AND gains the pointer at --help, and stays exit 2 - the outcome
// --help must NOT share.
func TestParseErrorPointsAtHelp(t *testing.T) {
	setupCLI(t, &noPromptPrompter{t: t})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "not defined") {
		t.Errorf("stderr = %q, want flag's own diagnostic", stderr.String())
	}
	if !strings.Contains(stderr.String(), "synosync pair --help") {
		t.Errorf("stderr = %q, want a pointer at --help", stderr.String())
	}
}
