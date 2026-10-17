package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
)

// followWriter is a concurrency-safe io.Writer that lets a test wait for a
// substring to be emitted by the follow loop running on another goroutine.
type followWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	updated chan struct{}
}

func newFollowWriter() *followWriter { return &followWriter{updated: make(chan struct{}, 1)} }

func (w *followWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, _ := w.buf.Write(p)
	w.mu.Unlock()
	select {
	case w.updated <- struct{}{}:
	default:
	}
	return n, nil
}

func (w *followWriter) string() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *followWriter) waitFor(s string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		ok := strings.Contains(w.buf.String(), s)
		w.mu.Unlock()
		if ok {
			return true
		}
		select {
		case <-w.updated:
		case <-deadline:
			return false
		}
	}
}

// appendLine appends a line to the log file under test.
func appendLine(path, s string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(s)
	return err
}

// writeLogFile points resolveDaemonPaths at a temp log dir and writes content (a
// possibly-empty string) to synosync.log within it, registering cleanup. It
// returns the resolved log file path so a test can append to it.
func writeLogFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	paths := config.Paths{LogDir: dir}

	path := paths.LogFile()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })
	return path
}

func TestLogPrintsFile(t *testing.T) {
	writeLogFile(t, "line 1\nline 2\nline 3\n")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := stdout.String(); got != "line 1\nline 2\nline 3\n" {
		t.Errorf("stdout = %q, want full file", got)
	}
}

func TestLogTailN(t *testing.T) {
	writeLogFile(t, "a\nb\nc\nd\ne\n")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "-n", "2"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := stdout.String(); got != "d\ne\n" {
		t.Errorf("stdout = %q, want last 2 lines", got)
	}
}

func TestLogTailNExceedsFile(t *testing.T) {
	writeLogFile(t, "only\ntwo\n")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "-n", "10"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := stdout.String(); got != "only\ntwo\n" {
		t.Errorf("stdout = %q, want whole file", got)
	}
}

func TestLogMissingFile(t *testing.T) {
	// Point at a temp dir with no log file: open fails with ENOENT.
	dir := t.TempDir()
	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return config.Paths{LogDir: dir}, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no log file at") {
		t.Errorf("stderr = %q, want no-log hint", stderr.String())
	}
}

// TestLogMissingFileHintCoversStderrMode pins the wording of the missing-file
// hint: an absent file is not proof that the daemon has never run (a daemon
// started with --log-stderr - what the shipped systemd unit passes - writes no
// file at all), so the hint must name the stderr/journal explanation and the
// path it looked at, not just "the daemon has not run".
func TestLogMissingFileHintCoversStderrMode(t *testing.T) {
	dir := t.TempDir()
	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return config.Paths{LogDir: dir}, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })

	for _, args := range [][]string{{"log"}, {"log", "-f"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 1 {
			t.Fatalf("%v: exit = %d, want 1", args, code)
		}
		got := stderr.String()
		for _, want := range []string{
			filepath.Join(dir, "synosync.log"),
			"--log-stderr",
			"systemd",
			"journalctl",
			"synosync daemon",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("%v: stderr = %q, want it to mention %q", args, got, want)
			}
		}
	}
}

func TestLogFollowReadsExisting(t *testing.T) {
	writeLogFile(t, "first\nsecond\n")

	// An already-cancelled follow context: follow prints the existing content and
	// returns without looping.
	orig := logFollowContext
	logFollowContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { logFollowContext = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "-f"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := stdout.String(); got != "first\nsecond\n" {
		t.Errorf("stdout = %q, want existing content", got)
	}
}

func TestLogFollowMissingFile(t *testing.T) {
	dir := t.TempDir()
	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return config.Paths{LogDir: dir}, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "--follow"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no log file at") {
		t.Errorf("stderr = %q, want no-log hint", stderr.String())
	}
}

func TestLogFollowTailThenAppend(t *testing.T) {
	path := writeLogFile(t, "old\n")

	// Append after the initial read but before the (already-cancelled) follow loop
	// runs its first tail pass, so the appended bytes are emitted by tailFrom, not
	// the initial ReadFile. The cancelled context still ends the loop after one
	// pass.
	orig := logFollowContext
	logFollowContext = func() (context.Context, context.CancelFunc) {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := f.WriteString("new\n"); err != nil {
			t.Fatalf("append write: %v", err)
		}
		_ = f.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { logFollowContext = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "-f"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := stdout.String(); got != "old\nnew\n" {
		t.Errorf("stdout = %q, want initial + appended", got)
	}
}

// TestLogFollowPicksUpAppendsAcrossPolls drives the follow loop with a LIVE
// context (not pre-cancelled) and a real ticker. It appends "two" only after
// "one" has already surfaced, so "two" cannot have been present during the same
// tailFrom pass that emitted "one" - proving the loop iterated through at least
// one ticker tick and advanced its offset across appends before a clean cancel.
func TestLogFollowPicksUpAppendsAcrossPolls(t *testing.T) {
	path := writeLogFile(t, "old\n")

	ctx, cancel := context.WithCancel(context.Background())
	origCtx, origIv := logFollowContext, logFollowPollInterval
	logFollowContext = func() (context.Context, context.CancelFunc) { return ctx, cancel }
	logFollowPollInterval = time.Millisecond
	t.Cleanup(func() {
		logFollowContext = origCtx
		logFollowPollInterval = origIv
		cancel()
	})

	w := newFollowWriter()
	const timeout = 5 * time.Second
	appendDone := make(chan struct{})
	go func() {
		defer close(appendDone)
		defer cancel() // ensure followLog returns even if a wait fails
		if !w.waitFor("old", timeout) {
			t.Errorf("never saw initial %q", "old")
			return
		}
		if err := appendLine(path, "one\n"); err != nil {
			t.Errorf("append one: %v", err)
			return
		}
		if !w.waitFor("one", timeout) {
			t.Errorf("follow never emitted appended %q", "one")
			return
		}
		if err := appendLine(path, "two\n"); err != nil {
			t.Errorf("append two: %v", err)
			return
		}
		if !w.waitFor("two", timeout) {
			t.Errorf("follow never emitted later append %q", "two")
			return
		}
	}()

	var stderr bytes.Buffer
	if code := followLog(path, 0, w, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	<-appendDone
	if got := w.string(); got != "old\none\ntwo\n" {
		t.Errorf("output = %q, want \"old\\none\\ntwo\\n\"", got)
	}
}

func TestLogBadArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "--nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

// TestLogRejectsExtraArgs verifies that a stray positional argument is a usage
// error (exit 2), mirroring the other no-positional subcommands rather than
// being silently ignored.
func TestLogRejectsExtraArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "extra"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	assertUsageError(t, "log", "extra", stderr.String())
}

// TestLogHelpExitsZero verifies that a requested -h/--help is a success (exit 0,
// flag prints usage), matching the other subcommands rather than the usage-error
// exit code.
func TestLogHelpExitsZero(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"log", arg}, &stdout, &stderr); code != 0 {
			t.Errorf("run log %s exit = %d, want 0 (stderr: %q)", arg, code, stderr.String())
		}
	}
}

func TestTailFromRotation(t *testing.T) {
	// A file that shrank below the offset is re-read from the start.
	dir := t.TempDir()
	path := filepath.Join(dir, "f.log")
	if err := os.WriteFile(path, []byte("short\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	var buf bytes.Buffer
	got, err := tailFrom(path, 1000, &buf)
	if err != nil {
		t.Fatalf("tailFrom: %v", err)
	}
	if buf.String() != "short\n" {
		t.Errorf("buf = %q, want whole rotated file", buf.String())
	}
	if got != int64(len("short\n")) {
		t.Errorf("offset = %d, want %d", got, len("short\n"))
	}
}

func TestTailFromMissing(t *testing.T) {
	// A momentarily-missing file (rotated away) is a no-op: offset preserved, no error.
	got, err := tailFrom(filepath.Join(t.TempDir(), "absent.log"), 42, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("tailFrom: %v", err)
	}
	if got != 42 {
		t.Errorf("offset = %d, want 42 preserved", got)
	}
}
