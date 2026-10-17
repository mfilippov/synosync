package main

// Unit tests for the small auth/prompt plumbing in cliauth.go and the daemon
// lifecycle wrapper in daemon.go: the pure error→message mapping, the byte-wise
// line reader, the terminal prompter's non-terminal fallback, the credential
// prompt's error paths, and runDaemonLoop's terminal-error mapping. These hit
// branches the higher-level command tests do not, without a live server.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/daemon"
)

// discardLogger is a throwaway slog logger for runDaemonLoop tests that do not
// assert log content.
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPairErrorMessage(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"user-not-found", auth.ErrUserNotFound, "the server does not know this account name"},
		{"invalid-password", auth.ErrInvalidPassword, "the server rejected this password"},
		{"otp-invalid", auth.ErrOTPInvalid, "incorrect or already-used one-time password"},
		{"otp-required", auth.ErrOTPRequired, "this account requires a one-time password"},
		{"client-version", auth.ErrClientVersion, "the server rejected this client version"},
		{"generic", errors.New("boom"), "pairing failed: boom"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pairErrorMessage(c.err); got != c.want {
				t.Errorf("pairErrorMessage(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestReadLine(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"newline-terminated", "hello\nrest", "hello"},
		{"strips-cr", "hello\r\n", "hello"},
		{"eof-without-newline", "tail", "tail"},
		{"empty-line", "\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readLine(strings.NewReader(c.in))
			if err != nil {
				t.Fatalf("readLine: %v", err)
			}
			if got != c.want {
				t.Errorf("readLine(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// endlessReader yields a fixed byte forever and never a newline or EOF, modelling a
// redirected stdin that is a large file, /dev/zero or a runaway pipe. It counts the
// bytes handed out so the test can prove readLine stopped instead of consuming
// unboundedly.
type endlessReader struct{ n int }

func (e *endlessReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	e.n++
	return 1, nil
}

// TestReadLineCapsInput pins the input bound: without it a
// prompted read from a stream that never emits a newline grows the accumulator until
// the process is OOM-killed. It must return an error after ~maxReadLineBytes rather
// than loop forever, and the error must be reported, not swallowed as a value.
func TestReadLineCapsInput(t *testing.T) {
	r := &endlessReader{}
	got, err := readLine(r)
	if !errors.Is(err, errLineTooLong) {
		t.Fatalf("readLine err = %v, want errLineTooLong", err)
	}
	if got != "" {
		t.Errorf("readLine returned %d bytes of content on overflow, want none", len(got))
	}
	// One read past the cap detects the overflow; anything more means the bound is
	// not what stopped the loop.
	if r.n > maxReadLineBytes+1 {
		t.Errorf("readLine consumed %d bytes, want at most %d", r.n, maxReadLineBytes+1)
	}
	// A line exactly at the cap is still accepted: the bound rejects overflow, not
	// every long-but-legal secret.
	atCap := strings.Repeat("y", maxReadLineBytes) + "\n"
	if got, err := readLine(strings.NewReader(atCap)); err != nil || len(got) != maxReadLineBytes {
		t.Errorf("readLine(at-cap line) = %d bytes (err %v), want %d bytes and no error",
			len(got), err, maxReadLineBytes)
	}
}

// errReader returns a non-EOF error on first read so readLine surfaces it.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestReadLineError(t *testing.T) {
	want := errors.New("read fault")
	if _, err := readLine(errReader{want}); !errors.Is(err, want) {
		t.Errorf("readLine err = %v, want %v", err, want)
	}
}

func TestTerminalPrompterNonTerminal(t *testing.T) {
	// A non-*os.File reader is never a terminal, so ReadPassword falls back to a
	// plain line read (no echo suppression) and ReadLine reads one echoed line.
	var out bytes.Buffer
	p := &terminalPrompter{in: strings.NewReader("secret\ncode\n"), out: &out}

	pw, err := p.ReadPassword("Password: ")
	if err != nil {
		t.Fatalf("ReadPassword: %v", err)
	}
	if pw != "secret" {
		t.Errorf("password = %q, want secret", pw)
	}
	line, err := p.ReadLine("OTP: ")
	if err != nil {
		t.Fatalf("ReadLine: %v", err)
	}
	if line != "code" {
		t.Errorf("line = %q, want code", line)
	}
	if !strings.Contains(out.String(), "Password: ") || !strings.Contains(out.String(), "OTP: ") {
		t.Errorf("prompts = %q, want both prompts echoed", out.String())
	}
}

// failPrompter fails on whichever of ReadPassword/ReadLine is selected, so the
// promptCredentials error paths are exercised independently.
type failPrompter struct {
	pwErr   error
	lineErr error
}

func (f failPrompter) ReadPassword(string) (string, error) {
	if f.pwErr != nil {
		return "", f.pwErr
	}
	return "pw", nil
}

func (f failPrompter) Confirm(string) (bool, error) { return true, nil }

func (f failPrompter) ReadLine(string) (string, error) {
	if f.lineErr != nil {
		return "", f.lineErr
	}
	return "otp", nil
}

func TestPromptCredentials(t *testing.T) {
	creds, err := promptCredentials(failPrompter{}, "alice")
	if err != nil {
		t.Fatalf("promptCredentials: %v", err)
	}
	if creds.User != "alice" || creds.Password != "pw" || creds.OTP != "otp" {
		t.Errorf("creds = %+v, want alice/pw/otp", creds)
	}
}

func TestPromptCredentialsPasswordError(t *testing.T) {
	_, err := promptCredentials(failPrompter{pwErr: errors.New("no tty")}, "alice")
	if err == nil || !strings.Contains(err.Error(), "read password") {
		t.Errorf("err = %v, want read-password wrap", err)
	}
}

func TestPromptCredentialsOTPError(t *testing.T) {
	_, err := promptCredentials(failPrompter{lineErr: errors.New("broken pipe")}, "alice")
	if err == nil || !strings.Contains(err.Error(), "read otp") {
		t.Errorf("err = %v, want read-otp wrap", err)
	}
}

func TestRunDaemonLoopSuccess(t *testing.T) {
	orig := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return nil }
	t.Cleanup(func() { runDaemon = orig })

	var stdout, stderr bytes.Buffer
	if code := runDaemonLoop(context.Background(), nil, discardLogger(), &stdout, &stderr, "ds:6690", true); code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "daemon started for ds:6690") {
		t.Errorf("stdout = %q, want startup banner", stdout.String())
	}
	if !strings.Contains(stdout.String(), "daemon stopped") {
		t.Errorf("stdout = %q, want shutdown line", stdout.String())
	}
}

// TestRunDaemonLoopNonInteractiveBanner confirms the start banner omits the
// "(Ctrl-C to stop)" hint when interactive is false (the Windows SCM path), where
// the hint is both meaningless and discarded along with the daemon's stdout.
func TestRunDaemonLoopNonInteractiveBanner(t *testing.T) {
	orig := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return nil }
	t.Cleanup(func() { runDaemon = orig })

	var stdout, stderr bytes.Buffer
	if code := runDaemonLoop(context.Background(), nil, discardLogger(), &stdout, &stderr, "ds:6690", false); code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "daemon started for ds:6690") {
		t.Errorf("stdout = %q, want startup banner", stdout.String())
	}
	if strings.Contains(stdout.String(), "Ctrl-C") {
		t.Errorf("stdout = %q, want no Ctrl-C hint under SCM", stdout.String())
	}
}

func TestRunDaemonLoopAlreadyRunning(t *testing.T) {
	orig := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return daemon.ErrAlreadyRunning }
	t.Cleanup(func() { runDaemon = orig })

	var stdout, stderr bytes.Buffer
	if code := runDaemonLoop(context.Background(), nil, discardLogger(), &stdout, &stderr, "ds:6690", true); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "already running") {
		t.Errorf("stderr = %q, want already-running message", stderr.String())
	}
}

func TestRunDaemonLoopGenericError(t *testing.T) {
	orig := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return errors.New("pool vanished") }
	t.Cleanup(func() { runDaemon = orig })

	var stdout, stderr bytes.Buffer
	if code := runDaemonLoop(context.Background(), nil, discardLogger(), &stdout, &stderr, "ds:6690", true); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "daemon stopped: pool vanished") {
		t.Errorf("stderr = %q, want daemon-stopped error", stderr.String())
	}
}

// TestClientIdentityIsComplete pins the whole point of the block: a live DSM
// validates agent / client_version / client_type / server_id on EVERY action
// turn and answers a turn missing any of them with 20481 ("client version too
// low"). A partially-filled Identity is therefore not a degraded identity, it is
// a rejected one - and the failure surfaces far from its cause, as a complaint
// about a version that was never the missing part.
//
// pair and unlink used to build a bare actions.NewClient, so their turns carried
// none of it. Measured against DSM 7.2.2-72806 with Drive Server 4.0.3-27892,
// that killed `pair` on QueryUserInfo - the one action it must complete - while
// a more lenient Drive Server had tolerated it.
func TestClientIdentityIsComplete(t *testing.T) {
	id := clientIdentity(clientInfo(), "device-uuid-1", "ds-id-1")

	if id.Agent == nil {
		t.Error("Agent is nil; a turn without the agent PObject draws 20481")
	}
	if id.ClientVersion == "" {
		t.Error("ClientVersion is empty")
	}
	if id.ClientType == "" {
		t.Error("ClientType is empty")
	}
	if id.DsID == "" {
		t.Error("DsID is empty; it is sent as server_id.restore_id")
	}
}

// TestClientIdentityCarriesTheCallersCoordinates guards the wiring rather than
// the shape: the DiskStation id and the client type are per-install values the
// caller holds, and announcing another install's is not a smaller error than
// announcing none.
func TestClientIdentityCarriesTheCallersCoordinates(t *testing.T) {
	ci := clientInfo()
	id := clientIdentity(ci, "uuid-A", "ds-B")

	if id.DsID != "ds-B" {
		t.Errorf("DsID = %q, want the caller's ds id", id.DsID)
	}
	if id.ClientType != ci.Type {
		t.Errorf("ClientType = %q, want %q", id.ClientType, ci.Type)
	}
	if id.ClientVersion != ci.ClientVersion() {
		t.Errorf("ClientVersion = %q, want %q", id.ClientVersion, ci.ClientVersion())
	}
}

// TestDropLeadingNewlineSkipsOrphan pins the fix for the silently-skipped OTP prompt.
// golang.org/x/term's readPasswordLine (terminal.go:1052-1061) returns on '\r' on
// Windows and ignores '\n', so a console's CRLF leaves its '\n' unread. readLine then
// consumed that orphan as a complete empty line and the OTP prompt returned "" INSTANTLY,
// without ever waiting: the code the operator typed afterwards went unread and was left
// in the console buffer for the parent shell to pick up.
func TestDropLeadingNewlineSkipsOrphan(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"orphan-then-otp", "\n045984\r\n", "045984"},
		{"orphan-then-bare-enter", "\n\r\n", ""},
		{"orphan-only-then-eof", "\n", ""},
		// Nothing was left behind: the first byte is real input and must survive.
		{"no-orphan-passes-through", "045984\r\n", "045984"},
		{"no-orphan-bare-enter", "\r\n", ""},
		// Exactly ONE newline is an orphan. A second is a genuine empty line - the
		// operator answering "no OTP" - and must still read as one.
		{"drops-only-one", "\n\n045984\r\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := readLine(&dropLeadingNewline{r: strings.NewReader(c.in)})
			if err != nil {
				t.Fatalf("readLine: %v", err)
			}
			if got != c.want {
				t.Errorf("readLine(dropLeadingNewline(%q)) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestDropLeadingNewlineDoesNotReadAhead pins why the wrapper is not a bufio.Reader.
// Buffering ahead would steal bytes belonging to a later read from the same terminal -
// the exact hazard readLine is written byte-at-a-time to avoid - so a second prompted
// read must still see its own line intact.
func TestDropLeadingNewlineDoesNotReadAhead(t *testing.T) {
	r := &dropLeadingNewline{r: strings.NewReader("\nfirst\r\nsecond\r\n")}
	first, err := readLine(r)
	if err != nil || first != "first" {
		t.Fatalf("first readLine = %q (err %v), want %q", first, err, "first")
	}
	second, err := readLine(r)
	if err != nil || second != "second" {
		t.Errorf("second readLine = %q (err %v), want %q", second, err, "second")
	}
}

// TestDropLeadingNewlineError surfaces a read fault rather than swallowing it as an
// empty line, which would read as "the operator supplied no OTP".
func TestDropLeadingNewlineError(t *testing.T) {
	want := errors.New("read fault")
	if _, err := readLine(&dropLeadingNewline{r: errReader{want}}); !errors.Is(err, want) {
		t.Errorf("readLine err = %v, want %v", err, want)
	}
}

// TestRunDaemonLoopLogsStartAndStop pins the seam a log file needs: a clean run writes when it
// started, on which build, and that it stopped. Only error exits reached the file before, so
// a Ctrl-C'd and restarted daemon left two runs indistinguishable in it.
func TestRunDaemonLoopLogsStartAndStop(t *testing.T) {
	orig := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return nil }
	t.Cleanup(func() { runDaemon = orig })

	var logbuf, stdout, stderr bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, nil))
	if code := runDaemonLoop(context.Background(), nil, log, &stdout, &stderr, "ds:6690", true); code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	out := logbuf.String()
	for _, want := range []string{`msg="daemon started"`, "server=ds:6690", "version=", `msg="daemon stopped"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}
