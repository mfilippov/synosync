package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/daemon"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// TestRunDaemonLoopTerminalHints verifies that a terminal authentication/pinning
// error from the run loop is reported with an actionable operator hint
// (re-pair on a cert change, re-login on a vanished session), while a generic
// error gets no such hint - and that the same hint is
// duplicated into the daemon logger as an Error record: under the Windows SCM
// stderr is discarded, so the rotating log is where the crash-loop cause and
// its one-command fix must land.
func TestRunDaemonLoopTerminalHints(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantHas string
		wantNot string
	}{
		{"pin-mismatch", transport.ErrPinMismatch, "synosync pair", "synosync login"},
		{"session-vanished", auth.ErrSessionVanished, "synosync login", "synosync pair"},
		{"pool-paused", transport.ErrPoolPaused, "synosync login", "synosync pair"},
		{"client-version", transport.ErrPoolClientRejected, "update synosync", "synosync login"},
		{"already-running", daemon.ErrAlreadyRunning, "already running", "synosync login"},
		{"generic", errors.New("boom"), "daemon stopped", "synosync login"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := runDaemon
			runDaemon = func(context.Context, *daemon.Daemon) error { return tc.err }
			t.Cleanup(func() { runDaemon = orig })

			var stdout, stderr, logBuf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&logBuf, nil))
			if code := runDaemonLoop(context.Background(), nil, log, &stdout, &stderr, "ds:6690", true); code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), tc.wantHas) {
				t.Errorf("stderr = %q, want %q", stderr.String(), tc.wantHas)
			}
			if tc.wantNot != "" && strings.Contains(stderr.String(), tc.wantNot) {
				t.Errorf("stderr = %q, must not contain %q", stderr.String(), tc.wantNot)
			}
			// The stop reason (with the hint, where one applies) also lands in the
			// log as an Error record - the only surviving sink under SCM.
			if !strings.Contains(logBuf.String(), "level=ERROR") {
				t.Errorf("log = %q, want an ERROR record", logBuf.String())
			}
			if !strings.Contains(logBuf.String(), tc.wantHas) {
				t.Errorf("log = %q, want %q", logBuf.String(), tc.wantHas)
			}
		})
	}
}

// setupDaemonPaths points resolveDaemonPaths at a fresh temp state/config dir and
// returns it. Originals are restored on cleanup.
func setupDaemonPaths(t *testing.T) config.Paths {
	t.Helper()
	dir := sockDir(t)
	paths := config.Paths{StateDir: dir, LogDir: filepath.Join(dir, "logs")}
	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })
	return paths
}

// writeDaemonConfig writes cfg into the state database at paths.StateDB().
func writeDaemonConfig(t *testing.T, paths config.Paths, cfg config.Config) {
	t.Helper()
	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

// writePairedDaemonConfig writes a fully-paired, valid config into the state
// database at paths.StateDB().
func writePairedDaemonConfig(t *testing.T, paths config.Paths) {
	t.Helper()
	cfg := config.Default()
	cfg.SyncRoot = t.TempDir()
	cfg.Server.Host = "drive.example.com"
	cfg.Server.Port = 6690
	cfg.Server.User = "alice"
	cfg.Server.DsID = testDsID
	cfg.DeviceUUID = "0123456789abcdef0123456789abcdef"
	writeDaemonConfig(t, paths, cfg)
}

// stubLogWriter is an in-memory io.WriteCloser standing in for the rotating log
// file so logger assembly is exercised without writing to disk.
type stubLogWriter struct {
	buf    bytes.Buffer
	closed bool
}

func (s *stubLogWriter) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *stubLogWriter) Close() error                { s.closed = true; return nil }

// stubLogWriterFn installs openLogWriter returning the supplied stub and records
// the path it was opened with. Originals are restored on cleanup.
func stubLogWriterFn(t *testing.T, w *stubLogWriter) *string {
	t.Helper()
	var gotPath string
	orig := openLogWriter
	openLogWriter = func(path string) (io.WriteCloser, error) {
		gotPath = path
		return w, nil
	}
	t.Cleanup(func() { openLogWriter = orig })
	return &gotPath
}

// forceStderrTerminal overrides stderrIsTerminal to report v. Restored on cleanup.
func forceStderrTerminal(t *testing.T, v bool) {
	t.Helper()
	orig := stderrIsTerminal
	stderrIsTerminal = func(io.Writer) bool { return v }
	t.Cleanup(func() { stderrIsTerminal = orig })
}

func TestBuildDaemonLoggerFileOnly(t *testing.T) {
	w := &stubLogWriter{}
	gotPath := stubLogWriterFn(t, w)
	forceStderrTerminal(t, false)

	paths := config.Paths{StateDir: sockDir(t), LogDir: filepath.Join(t.TempDir(), "logs")}
	cfg := config.Default()
	cfg.LogFormat = "json"

	var stderr bytes.Buffer
	log, closer, err := buildDaemonLogger(cfg, paths, false, &stderr)
	if err != nil {
		t.Fatalf("buildDaemonLogger: %v", err)
	}
	if closer == nil {
		t.Fatal("closer = nil, want the log writer")
	}
	if *gotPath != paths.LogFile() {
		t.Errorf("opened %q, want %q", *gotPath, paths.LogFile())
	}
	log.Info("hello-file")
	if !strings.Contains(w.buf.String(), "hello-file") {
		t.Errorf("log file = %q, want hello-file", w.buf.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty (no terminal mirror)", stderr.String())
	}
}

func TestBuildDaemonLoggerFileAndStderr(t *testing.T) {
	w := &stubLogWriter{}
	stubLogWriterFn(t, w)
	forceStderrTerminal(t, true)

	paths := config.Paths{StateDir: sockDir(t), LogDir: filepath.Join(t.TempDir(), "logs")}
	var stderr bytes.Buffer
	log, closer, err := buildDaemonLogger(config.Default(), paths, false, &stderr)
	if err != nil {
		t.Fatalf("buildDaemonLogger: %v", err)
	}
	if closer == nil {
		t.Fatal("closer = nil, want the log writer")
	}
	log.Info("hello-both")
	if !strings.Contains(w.buf.String(), "hello-both") {
		t.Errorf("log file = %q, want hello-both", w.buf.String())
	}
	if !strings.Contains(stderr.String(), "hello-both") {
		t.Errorf("stderr = %q, want mirrored hello-both", stderr.String())
	}
}

// failingLogWriter is an io.WriteCloser whose writes always fail, standing in
// for a rotating log file on a full/broken disk.
type failingLogWriter struct{ writes int }

func (f *failingLogWriter) Write([]byte) (int, error) {
	f.writes++
	return 0, errors.New("disk full")
}
func (f *failingLogWriter) Close() error { return nil }

// TestBuildDaemonLoggerStderrMirrorSurvivesFileError verifies the interactive
// stderr mirror keeps receiving records when the file sink fails:
// io.MultiWriter stops at the first failing writer, so without the
// bestEffortWriter wrapper a file-writer error would silence the mirror too.
func TestBuildDaemonLoggerStderrMirrorSurvivesFileError(t *testing.T) {
	fw := &failingLogWriter{}
	orig := openLogWriter
	openLogWriter = func(string) (io.WriteCloser, error) { return fw, nil }
	t.Cleanup(func() { openLogWriter = orig })
	forceStderrTerminal(t, true)

	paths := config.Paths{StateDir: sockDir(t), LogDir: filepath.Join(t.TempDir(), "logs")}
	var stderr bytes.Buffer
	log, closer, err := buildDaemonLogger(config.Default(), paths, false, &stderr)
	if err != nil {
		t.Fatalf("buildDaemonLogger: %v", err)
	}
	if closer == nil {
		t.Fatal("closer = nil, want the log writer")
	}

	log.Info("mirror-alive")
	if fw.writes == 0 {
		t.Fatal("file writer never reached; test exercises nothing")
	}
	if !strings.Contains(stderr.String(), "mirror-alive") {
		t.Errorf("stderr = %q, want mirrored record despite file sink failure", stderr.String())
	}
	if !strings.Contains(stderr.String(), "log file write failed") {
		t.Errorf("stderr = %q, want a one-time file-failure note", stderr.String())
	}

	log.Info("second-record")
	if !strings.Contains(stderr.String(), "second-record") {
		t.Errorf("stderr = %q, want second mirrored record", stderr.String())
	}
	if got := strings.Count(stderr.String(), "log file write failed"); got != 1 {
		t.Errorf("failure note printed %d times, want 1 (noted once per streak)", got)
	}
}

// flappyWriter fails or succeeds per a scripted sequence, to exercise the
// bestEffortWriter streak re-arm.
type flappyWriter struct {
	fail []bool
	i    int
}

func (f *flappyWriter) Write(p []byte) (int, error) {
	fail := f.fail[f.i]
	f.i++
	if fail {
		return 0, errors.New("boom")
	}
	return len(p), nil
}

func TestBestEffortWriterNotesOncePerStreak(t *testing.T) {
	var note bytes.Buffer
	b := &bestEffortWriter{w: &flappyWriter{fail: []bool{true, true, false, true}}, note: &note}
	for i := range 4 {
		if n, err := b.Write([]byte("x")); err != nil || n != 1 {
			t.Fatalf("Write %d = (%d, %v), want (1, nil)", i, n, err)
		}
	}
	// Two failure streaks (writes 1-2 and write 4) → exactly two notes: the
	// intervening success re-arms the note.
	if got := strings.Count(note.String(), "log file write failed"); got != 2 {
		t.Errorf("notes = %d, want 2 (one per failure streak); note output = %q", got, note.String())
	}
}

func TestBuildDaemonLoggerStderrOnly(t *testing.T) {
	orig := openLogWriter
	openLogWriter = func(string) (io.WriteCloser, error) {
		t.Error("openLogWriter called with --log-stderr")
		return nil, nil
	}
	t.Cleanup(func() { openLogWriter = orig })

	paths := config.Paths{StateDir: sockDir(t), LogDir: filepath.Join(t.TempDir(), "logs")}
	var stderr bytes.Buffer
	log, closer, err := buildDaemonLogger(config.Default(), paths, true, &stderr)
	if err != nil {
		t.Fatalf("buildDaemonLogger: %v", err)
	}
	if closer != nil {
		t.Errorf("closer = %v, want nil (no file in stderr-only mode)", closer)
	}
	log.Info("hello-stderr")
	if !strings.Contains(stderr.String(), "hello-stderr") {
		t.Errorf("stderr = %q, want hello-stderr", stderr.String())
	}
}

func TestBuildDaemonLoggerWriterError(t *testing.T) {
	orig := openLogWriter
	openLogWriter = func(string) (io.WriteCloser, error) { return nil, errors.New("mkdir boom") }
	t.Cleanup(func() { openLogWriter = orig })

	paths := config.Paths{StateDir: sockDir(t), LogDir: filepath.Join(t.TempDir(), "logs")}
	if _, _, err := buildDaemonLogger(config.Default(), paths, false, io.Discard); err == nil {
		t.Fatal("buildDaemonLogger err = nil, want writer error")
	}
}

func TestBuildDaemonLoggerInvalidFormat(t *testing.T) {
	// An invalid format reaches logging.New on the stderr-only path (which skips
	// the writer) and is reported as an error.
	cfg := config.Default()
	cfg.LogFormat = "xml"
	if _, _, err := buildDaemonLogger(cfg, config.Paths{}, true, io.Discard); err == nil {
		t.Fatal("buildDaemonLogger err = nil, want invalid-format error")
	}
}

func TestDaemonBadArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon", "--nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

func TestDaemonConfigMissing(t *testing.T) {
	setupDaemonPaths(t) // empty state dir, no stored config

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no config found") {
		t.Errorf("stderr = %q, want no-config message", stderr.String())
	}
}

// TestDaemonConfigCorrupt confirms the daemon preflight points the operator at
// `synosync unlink` when the stored settings blob no longer decodes:
// a corrupt config would otherwise surface as a bare, dead-end decode
// error under the Service Control Manager, with no recovery hint. It mirrors the
// CLI-verb coverage in TestCorruptConfigStrictCommandsPointToUnlink, which does
// not exercise the daemon's own path/store injection.
func TestDaemonConfigCorrupt(t *testing.T) {
	paths := setupDaemonPaths(t)
	// Seed a valid config first so the store (and its meta bucket) exists, then
	// overwrite the config key with an undecodable blob (an unknown field trips the
	// strict DisallowUnknownFields decode → store.ErrCorruptRecord).
	writePairedDaemonConfig(t, paths)
	corruptConfigBlob(t, paths.StateDB(), []byte(`{"server":{"host":"nas.example"},"bogus_future_field":true}`))

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (%s)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "synosync unlink") {
		t.Errorf("stderr = %q, want a `synosync unlink` recovery hint", stderr.String())
	}
}

func TestDaemonStoreLocked(t *testing.T) {
	setupDaemonPaths(t)
	orig := openDaemonStore
	openDaemonStore = func(string) (*store.Store, error) { return nil, store.ErrLocked }
	t.Cleanup(func() { openDaemonStore = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "another daemon instance running") {
		t.Errorf("stderr = %q, want locked-database hint", stderr.String())
	}
}

func TestDaemonNotPaired(t *testing.T) {
	paths := setupDaemonPaths(t)
	// Valid config (sync_root + server) but no device_uuid/ds_id → not paired.
	cfg := config.Default()
	cfg.SyncRoot = t.TempDir()
	cfg.Server.Host = "drive.example.com"
	cfg.Server.User = "alice"
	writeDaemonConfig(t, paths, cfg)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not paired") {
		t.Errorf("stderr = %q, want not-paired message", stderr.String())
	}
}

func TestDaemonInvalidConfig(t *testing.T) {
	paths := setupDaemonPaths(t)
	// Missing sync_root → Validate fails before the paired check.
	cfg := config.Default()
	cfg.Server.Host = "drive.example.com"
	cfg.Server.User = "alice"
	writeDaemonConfig(t, paths, cfg)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "sync_root") {
		t.Errorf("stderr = %q, want sync_root validation message", stderr.String())
	}
}

func TestDaemonNoCredentials(t *testing.T) {
	keyring.MockInit() // empty keychain: no pin/session stored
	paths := setupDaemonPaths(t)
	writePairedDaemonConfig(t, paths)

	orig := openKeyring
	openKeyring = func() secretStore { return secrets.New() }
	t.Cleanup(func() { openKeyring = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no pinned certificate") {
		t.Errorf("stderr = %q, want re-pair hint", stderr.String())
	}
}

// TestDaemonAssemblesAndRuns drives the full preflight + assembly with a stubbed
// run seam (no live server): paired config, seeded keychain, real store + IPC
// listener, and an injected runDaemon that returns immediately.
func TestDaemonAssemblesAndRuns(t *testing.T) {
	keyring.MockInit()
	paths := setupDaemonPaths(t)
	writePairedDaemonConfig(t, paths)

	server := "drive.example.com:6690"
	kr := secrets.New()
	if err := kr.SetPin(server, "sha256:"+strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("SetPin: %v", err)
	}
	if err := kr.SetSession(server, "alice", "sessiontoken"); err != nil {
		t.Fatalf("SetSession: %v", err)
	}

	origKr := openKeyring
	openKeyring = func() secretStore { return secrets.New() }
	t.Cleanup(func() { openKeyring = origKr })

	var ran bool
	origRun := runDaemon
	runDaemon = func(_ context.Context, d *daemon.Daemon) error {
		ran = true
		if d == nil {
			t.Error("runDaemon got nil daemon")
		}
		return nil
	}
	t.Cleanup(func() { runDaemon = origRun })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !ran {
		t.Error("runDaemon was not called")
	}
	if !strings.Contains(stdout.String(), "daemon started") {
		t.Errorf("stdout = %q, want startup message", stdout.String())
	}
	if !strings.Contains(stdout.String(), "daemon stopped") {
		t.Errorf("stdout = %q, want shutdown message", stdout.String())
	}
	// The store and IPC socket were created under the state dir; the config blob
	// is still readable from the state database.
	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if _, err := st.GetConfig(); err != nil {
		t.Errorf("config no longer loadable: %v", err)
	}
	_ = st.Close()
	if _, err := os.Stat(filepath.Join(paths.StateDir, "state.db")); err != nil {
		t.Errorf("state.db was not created: %v", err)
	}
}

// seedDaemonCreds seeds a mock keychain with a pin + session for the paired
// config and points openKeyring at a fresh secrets store. Originals restored.
func seedDaemonCreds(t *testing.T) {
	t.Helper()
	keyring.MockInit()
	server := "drive.example.com:6690"
	kr := secrets.New()
	if err := kr.SetPin(server, "sha256:"+strings.Repeat("ab", 32)); err != nil {
		t.Fatalf("SetPin: %v", err)
	}
	if err := kr.SetSession(server, "alice", "sessiontoken"); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	orig := openKeyring
	openKeyring = func() secretStore { return secrets.New() }
	t.Cleanup(func() { openKeyring = orig })
}

// captureStartFallback overrides reportServiceStartFailure to record every
// pre-logger start-failure message it receives. Originals restored on cleanup.
func captureStartFallback(t *testing.T) *[]string {
	t.Helper()
	var msgs []string
	orig := reportServiceStartFailure
	reportServiceStartFailure = func(msg string) { msgs = append(msgs, msg) }
	t.Cleanup(func() { reportServiceStartFailure = orig })
	return &msgs
}

// TestDaemonPreflightErrorAfterLoggerLandsInLog verifies the preflight
// reordering: the logger is built right after the config
// loads, so every later preflight failure (validation, paired check, connector)
// is written to the log writer - the only surviving sink under the Windows
// Service Control Manager, where stderr is discarded - as well as stderr, and
// does not use the pre-logger side channel.
func TestDaemonPreflightErrorAfterLoggerLandsInLog(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, paths config.Paths)
		want  string
	}{
		{
			name: "invalid-config",
			setup: func(t *testing.T, paths config.Paths) {
				// Missing sync_root → Validate fails after the logger is up.
				cfg := config.Default()
				cfg.Server.Host = "drive.example.com"
				cfg.Server.User = "alice"
				writeDaemonConfig(t, paths, cfg)
			},
			want: "sync_root",
		},
		{
			name: "not-paired",
			setup: func(t *testing.T, paths config.Paths) {
				// Valid config but no device_uuid/ds_id → paired check fails.
				cfg := config.Default()
				cfg.SyncRoot = t.TempDir()
				cfg.Server.Host = "drive.example.com"
				cfg.Server.User = "alice"
				writeDaemonConfig(t, paths, cfg)
			},
			want: "not paired",
		},
		{
			name: "connector-error",
			setup: func(t *testing.T, paths config.Paths) {
				writePairedDaemonConfig(t, paths)
				keyring.MockInit() // empty keychain: no pin/session stored
				orig := openKeyring
				openKeyring = func() secretStore { return secrets.New() }
				t.Cleanup(func() { openKeyring = orig })
			},
			want: "no pinned certificate",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := setupDaemonPaths(t)
			tc.setup(t, paths)
			w := &stubLogWriter{}
			stubLogWriterFn(t, w)
			forceStderrTerminal(t, false)
			fallback := captureStartFallback(t)

			var stdout, stderr bytes.Buffer
			if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr = %q, want %q", stderr.String(), tc.want)
			}
			if !strings.Contains(w.buf.String(), tc.want) {
				t.Errorf("log writer = %q, want %q (post-logger preflight errors must reach the log file)", w.buf.String(), tc.want)
			}
			if !strings.Contains(w.buf.String(), "daemon start failed") {
				t.Errorf("log writer = %q, want the daemon-start-failed record", w.buf.String())
			}
			if len(*fallback) != 0 {
				t.Errorf("pre-logger fallback called with %q, want none (the log file is the SCM sink here)", *fallback)
			}
		})
	}
}

// TestDaemonPreLoggerFailureUsesServiceFallback verifies that a preflight
// failure before the logger exists (a locked store, a log-writer open error) is
// routed through the per-OS pre-logger side channel - the Windows event log
// under SCM, where stderr is discarded and no log file exists yet - in addition
// to stderr.
func TestDaemonPreLoggerFailureUsesServiceFallback(t *testing.T) {
	t.Run("store-locked", func(t *testing.T) {
		setupDaemonPaths(t)
		orig := openDaemonStore
		openDaemonStore = func(string) (*store.Store, error) { return nil, store.ErrLocked }
		t.Cleanup(func() { openDaemonStore = orig })
		fallback := captureStartFallback(t)

		var stdout, stderr bytes.Buffer
		if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		if len(*fallback) != 1 || !strings.Contains((*fallback)[0], "state database is in use") {
			t.Errorf("fallback = %q, want one locked-database message", *fallback)
		}
	})

	t.Run("log-writer-error", func(t *testing.T) {
		paths := setupDaemonPaths(t)
		writePairedDaemonConfig(t, paths)
		seedDaemonCreds(t)
		orig := openLogWriter
		openLogWriter = func(string) (io.WriteCloser, error) { return nil, errors.New("create log dir: denied") }
		t.Cleanup(func() { openLogWriter = orig })
		fallback := captureStartFallback(t)

		var stdout, stderr bytes.Buffer
		if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		if len(*fallback) != 1 || !strings.Contains((*fallback)[0], "create log dir") {
			t.Errorf("fallback = %q, want one log-writer error message", *fallback)
		}
	})
}

// TestDaemonLogWriterError verifies that a failure to open the rotating log file
// (e.g. the log directory cannot be created) is reported and exits 1.
func TestDaemonLogWriterError(t *testing.T) {
	paths := setupDaemonPaths(t)
	writePairedDaemonConfig(t, paths)
	seedDaemonCreds(t)

	orig := openLogWriter
	openLogWriter = func(string) (io.WriteCloser, error) { return nil, errors.New("create log dir: denied") }
	t.Cleanup(func() { openLogWriter = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "create log dir") {
		t.Errorf("stderr = %q, want log-writer error", stderr.String())
	}
}

// TestDaemonLogStderrFlag verifies that --log-stderr skips the log file entirely
// (openLogWriter must not be called) and still assembles + runs.
func TestDaemonLogStderrFlag(t *testing.T) {
	paths := setupDaemonPaths(t)
	writePairedDaemonConfig(t, paths)
	seedDaemonCreds(t)

	orig := openLogWriter
	openLogWriter = func(string) (io.WriteCloser, error) {
		t.Error("openLogWriter called despite --log-stderr")
		return nil, nil
	}
	t.Cleanup(func() { openLogWriter = orig })

	origRun := runDaemon
	runDaemon = func(context.Context, *daemon.Daemon) error { return nil }
	t.Cleanup(func() { runDaemon = origRun })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"daemon", "--log-stderr"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
}

// TestRunDaemonAnnouncingStopAcknowledgesCtrlC: Ctrl-C cancels the context, and every
// other line the operator sees waits for teardown to finish - the in-flight turn, the
// pool close, the engine's poll returning. On a busy sync that is a terminal that has
// acknowledged nothing, which reads as a wedged process and invites a second Ctrl-C that
// kills it mid-teardown, since the first one already released the signal registration.
func TestRunDaemonAnnouncingStopAcknowledgesCtrlC(t *testing.T) {
	orig := runDaemon
	t.Cleanup(func() { runDaemon = orig })

	// Model a slow teardown: the daemon returns only after the notice has been written,
	// so the assertion is that the notice does not wait for it.
	written := make(chan struct{})
	runDaemon = func(ctx context.Context, _ *daemon.Daemon) error {
		<-ctx.Done()
		// Bounded, so a regression that moves the notice after teardown fails the
		// assertion below instead of hanging until the test binary times out.
		select {
		case <-written:
		case <-time.After(5 * time.Second):
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := &notifyingWriter{written: written}
	go func() {
		cancel()
	}()
	if err := runDaemonAnnouncingStop(ctx, nil, out, true); err != nil {
		t.Fatalf("runDaemonAnnouncingStop: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "stopping") {
		t.Fatalf("stdout = %q, want a stop acknowledgement written before teardown finished", got)
	}
}

// TestRunDaemonAnnouncingStopSilentUnderServiceManager: there is no one at the terminal,
// and the line would be noise in the journal beside the daemon's own stop record.
func TestRunDaemonAnnouncingStopSilentUnderServiceManager(t *testing.T) {
	orig := runDaemon
	t.Cleanup(func() { runDaemon = orig })
	runDaemon = func(ctx context.Context, _ *daemon.Daemon) error { <-ctx.Done(); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := runDaemonAnnouncingStop(ctx, nil, &out, false); err != nil {
		t.Fatalf("runDaemonAnnouncingStop: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing under a service manager", out.String())
	}
}

// TestRunDaemonAnnouncingStopSilentOnItsOwnFailure: a daemon that stops because something
// went wrong was never asked to stop, so there is nothing to acknowledge - the error path
// says what happened.
func TestRunDaemonAnnouncingStopSilentOnItsOwnFailure(t *testing.T) {
	orig := runDaemon
	t.Cleanup(func() { runDaemon = orig })
	boom := errors.New("pool bring-up failed")
	runDaemon = func(context.Context, *daemon.Daemon) error { return boom }

	var out bytes.Buffer
	if err := runDaemonAnnouncingStop(context.Background(), nil, &out, true); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the daemon's own failure", err)
	}
	if out.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing when no stop was requested", out.String())
	}
}

// notifyingWriter records what was written and closes written on the first write, so a
// test can make the fake daemon's teardown wait for the notice rather than sleeping.
type notifyingWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	written chan struct{}
	once    sync.Once
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	w.mu.Unlock()
	w.once.Do(func() { close(w.written) })
	return n, err
}

func (w *notifyingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
