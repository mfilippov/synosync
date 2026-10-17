package main

// Shared test scaffolding for the pair/login CLI tests: a canned prompter, and a
// helper that points the injection vars (state database, keyring, prompter) at
// test doubles so the commands can be driven end-to-end through run().

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zalando/go-keyring"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/store"
)

// sockDir returns a temp directory short enough to hold the daemon's AF_UNIX
// socket. sun_path caps a bound socket path at 104 bytes on macOS/BSD (108 on
// Linux), and t.TempDir() spends most of that budget before daemon.sock is even
// appended: macOS's per-user TMPDIR is already ~48 bytes of
// /var/folders/<2>/<30>/T/, onto which t.TempDir() adds the full test name, a
// random suffix and a per-call counter. The socket then overruns the cap and
// every bind - and every dial - fails with EINVAL ("invalid argument"), which
// both reddened the macOS CI job and made the daemon-not-running tests assert
// against the wrong error shape (EINVAL is neither ENOENT nor ECONNREFUSED, so
// ipc.IsDaemonAbsent rejected it). MkdirTemp with a short prefix keeps every
// StateDir-derived path well inside the cap on every platform.
func sockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sy")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// fakePrompter feeds canned interactive input. otps is consumed in order so a
// flow that prompts for an OTP twice (empty first, then a code) can be scripted.
type fakePrompter struct {
	password string
	otps     []string
	otpIdx   int
	// refuseCert answers no to the untrusted-certificate question (the default trusts it,
	// as the mock server's self-signed certificate would otherwise stop every pairing).
	refuseCert bool
	confirmed  int
}

func (f *fakePrompter) Confirm(string) (bool, error) { f.confirmed++; return !f.refuseCert, nil }

func (f *fakePrompter) ReadPassword(string) (string, error) { return f.password, nil }

func (f *fakePrompter) ReadLine(string) (string, error) {
	if f.otpIdx < len(f.otps) {
		v := f.otps[f.otpIdx]
		f.otpIdx++
		return v, nil
	}
	return "", nil
}

// noPromptPrompter fails the test on any interactive prompt, for flows that
// must reject their input before ever prompting the user.
type noPromptPrompter struct{ t *testing.T }

func (f *noPromptPrompter) ReadPassword(string) (string, error) {
	f.t.Error("password prompted; the flow should have failed before any prompt")
	return "", nil
}

func (f *noPromptPrompter) ReadLine(string) (string, error) {
	f.t.Error("prompted; the flow should have failed before any prompt")
	return "", nil
}

func (f *noPromptPrompter) Confirm(string) (bool, error) {
	f.t.Error("asked to confirm; the flow should have failed before any prompt")
	return false, nil
}

// setupCLI installs in-memory test doubles for the pair/login injection points
// and returns the temp state.db path. It resets the keyring backend, routes
// openConfigStore at a temp state database (opened fresh per call so the command
// can take the bbolt lock), backs the keychain with the mocked backend and makes
// newPrompter yield p. Originals are restored on cleanup.
func setupCLI(t *testing.T, p prompter) string {
	t.Helper()
	keyring.MockInit()
	// MockInit installs a process-global mock keyring that is not cleared between tests.
	// Reset it after this test so the negative "nothing persisted" assertions never pass
	// (or fail) for the wrong reason on a stray entry left by a prior test that happened to
	// reuse a server address.
	t.Cleanup(keyring.MockInit)

	stateDir := sockDir(t)
	statePath := filepath.Join(stateDir, "state.db")
	logDir := filepath.Join(t.TempDir(), "logs")

	origStore := openConfigStore
	origKeyring := openKeyring
	origPrompter := newPrompter
	origPaths := resolveDaemonPaths
	t.Cleanup(func() {
		openConfigStore = origStore
		openKeyring = origKeyring
		newPrompter = origPrompter
		resolveDaemonPaths = origPaths
	})

	openConfigStore = func() (*store.Store, error) { return store.Open(statePath) }
	openKeyring = func() secretStore { return secrets.New() }
	newPrompter = func(io.Reader, io.Writer) prompter { return p }
	// Point daemon-path resolution at the temp state/log dirs so commands that
	// touch on-disk daemon artifacts (e.g. unlink's pipe-secret cleanup) stay
	// hermetic. Tests needing a specific socket/log path override it after setupCLI.
	resolveDaemonPaths = func() (config.Paths, error) {
		return config.Paths{StateDir: stateDir, LogDir: logDir}, nil
	}

	return statePath
}

// loadStoredConfig opens the state database at statePath and returns the
// persisted config, for tests that assert what pair wrote.
func loadStoredConfig(t *testing.T, statePath string) config.Config {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	cfg, err := st.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	return cfg
}
