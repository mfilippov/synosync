package main

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/zalando/go-keyring"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// writeServerConfig writes a minimal config blob into the state database at
// statePath pointing at srv with user, plus any extra fields applied via mutate
// (e.g. DeviceUUID/DsID for login tests).
func writeServerConfig(t *testing.T, statePath string, srv *mockServer, user string, mutate func(*config.Config)) {
	t.Helper()
	cfg := config.Default()
	cfg.SyncRoot = t.TempDir()
	cfg.Server.Host = srv.host()
	cfg.Server.Port = srv.port()
	cfg.Server.User = user
	if mutate != nil {
		mutate(&cfg)
	}
	st, err := store.Open(statePath)
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

func TestPairSuccess(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(cmd uint8, _ *sproto.Map) sproto.Value {
		if cmd != sproto.CmdAuth {
			t.Errorf("cmd = 0x%02x, want CmdAuth", cmd)
		}
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "paired with") {
		t.Errorf("stdout = %q, want pairing confirmation", stdout.String())
	}

	kr := secrets.New()
	if got, err := kr.Session(srv.addr(), "alice"); err != nil || got != wantSession {
		t.Errorf("stored session = %q, err = %v; want %q", got, err, wantSession)
	}
	// pair now stores the SPKI (public-key) pin, not the legacy full-DER pin.
	if got, err := kr.Pin(srv.addr()); err != nil || got != srv.leafSPKIPin() {
		t.Errorf("stored pin = %q, err = %v; want %q", got, err, srv.leafSPKIPin())
	}

	cfg := loadStoredConfig(t, path)
	if cfg.Server.DsID != testDsID {
		t.Errorf("config ds_id = %q, want %q", cfg.Server.DsID, testDsID)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(cfg.DeviceUUID) {
		t.Errorf("config device_uuid = %q, want 32 hex chars", cfg.DeviceUUID)
	}
}

func TestPairPrintsFingerprint(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if want := "certificate pin: " + srv.leafSPKIPin(); !strings.Contains(stdout.String(), want) {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestPairExpectPinMatch(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--expect-pin", srv.leafSPKIPin()}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != wantSession {
		t.Errorf("stored session = %q, want %q", got, wantSession)
	}
}

// TestPinMatches covers the format dispatch: a legacy "sha256:" expected pin is
// matched against the leaf's DER fingerprint, an SPKI / bare-hex one against the
// SPKI fingerprint, ignoring case/whitespace and prefix.
func TestPinMatches(t *testing.T) {
	const spki = "spki-sha256:aabbccdd"
	const der = "sha256:11223344"
	cases := []struct {
		name     string
		expected string
		want     bool
	}{
		{"spki full form", "spki-sha256:aabbccdd", true},
		{"spki bare hex", "AABBCCDD", true},
		{"spki whitespace", "  spki-sha256:AABBCCDD  ", true},
		{"legacy der full form", "sha256:11223344", true},
		{"legacy der case", "SHA256:11223344", true},
		{"legacy der vs spki value mismatch", "sha256:aabbccdd", false},
		{"spki value mismatch", "spki-sha256:deadbeef", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pinMatches(tc.expected, spki, der); got != tc.want {
				t.Errorf("pinMatches(%q, %q, %q) = %v, want %v", tc.expected, spki, der, got, tc.want)
			}
		})
	}
}

// TestPairExpectPinLegacyDERMatch confirms an operator who recorded a pre-SPKI
// full-DER pin out-of-band can still verify it via --expect-pin: the legacy form
// is matched against the leaf's DER fingerprint, not its (always-different) SPKI
// form.
func TestPairExpectPinLegacyDERMatch(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--expect-pin", srv.leafPin()}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != wantSession {
		t.Errorf("stored session = %q, want %q", got, wantSession)
	}
}

func TestPairExpectPinMismatch(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("auth reached despite an expect-pin mismatch")
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--expect-pin", "spki-sha256:deadbeef"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "certificate pin mismatch") {
		t.Errorf("stderr = %q, want pin-mismatch message", stderr.String())
	}
	// Nothing must be persisted to the keychain on a refused pairing.
	if _, err := secrets.New().Session(srv.addr(), "alice"); err == nil {
		t.Error("session stored despite a refused pairing")
	}
	if _, err := secrets.New().Pin(srv.addr()); err == nil {
		t.Error("pin stored despite a refused pairing")
	}
}

// TestPairRejectsInvalidConfigBeforePersist pins runPair's up-front Validate -
// the guard that now runs BEFORE the connect/pairing turn (it moved
// there from its former post-pairing placement, so an invalid config never
// registers a device or writes the keychain). The CLI path also validates up
// front (see TestPairInvalidPortRejected), so this drives
// runPair directly to keep the guard covered for an invalid config that reaches
// the pairing flow itself.
func TestPairRejectsInvalidConfigBeforePersist(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Server coordinates are valid (pairing succeeds), but log_format is invalid:
	// runPair must validate the whole config before PutConfig and refuse, rather
	// than silently persisting an invalid blob.
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) { c.LogFormat = "xml" })

	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	cfg, err := loadConfigFromStore(st)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	deps := authDeps{
		store:  st,
		kr:     openKeyring(),
		prompt: newPrompter(nil, io.Discard),
		client: clientInfo(),
	}

	var stdout, stderr bytes.Buffer
	code := runPair(deps, cfg, priorIdentity{}, "", false, "", &stdout, &stderr)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "log_format") {
		t.Errorf("stderr = %q, want a log_format validation message", stderr.String())
	}
	// The invalid config must not have been mutated/persisted: device_uuid stays unset.
	if cfg := loadStoredConfig(t, path); cfg.DeviceUUID != "" {
		t.Errorf("device_uuid = %q, want empty (PutConfig must be skipped on invalid config)", cfg.DeviceUUID)
	}
	// Validation runs before the keychain writes, so an invalid config that is
	// never persisted must not leave an orphaned pin+session in the keychain.
	if _, err := secrets.New().Session(srv.addr(), "alice"); err == nil {
		t.Error("session stored despite an invalid (never-persisted) config")
	}
	if _, err := secrets.New().Pin(srv.addr()); err == nil {
		t.Error("pin stored despite an invalid (never-persisted) config")
	}
}

// TestPairDeviceUUIDReusedAfterFinalWriteFailure reproduces the pre-registration window:
// the server registers the device, then the FINAL config write fails, so the
// process exits non-zero. Because the freshly-generated device_uuid is now
// persisted up front - BEFORE the pairing turn - a retry loads it and reuses the
// same id rather than generating a fresh one and registering a second, orphaned
// device server-side. The mock server records the device_uuid of every auth
// turn; before the fix the failed final write left no device_uuid behind and the
// two attempts carried two different ids.
func TestPairDeviceUUIDReusedAfterFinalWriteFailure(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	var mu sync.Mutex
	var seen []string
	srv := newPairMockServer(t, func(cmd uint8, req *sproto.Map) sproto.Value {
		if cmd == sproto.CmdAuth {
			mu.Lock()
			if u, ok := req.String("device_uuid"); ok {
				seen = append(seen, u)
			}
			mu.Unlock()
		}
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	// First attempt: the final post-registration config write fails.
	failFinal := true
	orig := putPairedConfig
	putPairedConfig = func(st *store.Store, cfg config.Config) error {
		if failFinal {
			return errors.New("simulated final config write failure")
		}
		return st.PutConfig(cfg)
	}
	t.Cleanup(func() { putPairedConfig = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 1 {
		t.Fatalf("first pair exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	// The up-front write persisted the device_uuid despite the failed final write.
	first := loadStoredConfig(t, path)
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(first.DeviceUUID) {
		t.Fatalf("device_uuid after failed final write = %q, want it persisted up front", first.DeviceUUID)
	}

	// Retry: the final write succeeds; the stored device_uuid must be reused, not
	// regenerated (which would register a second device server-side).
	failFinal = false
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("retry pair exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("server saw %d auth turns, want 2 (%v)", len(seen), seen)
	}
	if seen[0] != seen[1] {
		t.Errorf("device_uuid across attempts = %q vs %q, want the same id reused", seen[0], seen[1])
	}
	if seen[1] != first.DeviceUUID {
		t.Errorf("retry device_uuid = %q, want stored %q", seen[1], first.DeviceUUID)
	}
}

// TestPairFailedTurnLeavesUnpairedBlob covers a pairing turn that fails
// AFTER the up-front device_uuid persist (a wrong OTP here) must leave a blob
// carrying the reusable device_uuid but NOT the ds_id, so every "is
// paired" predicate still reads "not paired". Before the fix the up-front persist
// wrote the ds_id too, leaving a "paired-looking" blob against an empty keychain:
// `daemon` then looped on "no pinned certificate" instead of reporting "not
// paired; run `synosync pair` first", and `unlink`/`config set` mis-messaged an
// orphaned pairing.
func TestPairFailedTurnLeavesUnpairedBlob(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOTPInvalid))
	})

	statePath := setupCLI(t, &fakePrompter{password: "s3cret", otps: []string{"999999"}})
	writeServerConfig(t, statePath, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 1 {
		t.Fatalf("pair exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}

	// The up-front persist saved the reusable device_uuid but must NOT have saved
	// the ds_id - the failed turn never completed pairing.
	cfg := loadStoredConfig(t, statePath)
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(cfg.DeviceUUID) {
		t.Errorf("device_uuid = %q, want it persisted up front for reuse", cfg.DeviceUUID)
	}
	if cfg.Server.DsID != "" {
		t.Errorf("ds_id = %q, want empty until pairing completes (blob must not look paired)", cfg.Server.DsID)
	}

	// Nothing landed in the keychain on the failed turn.
	if _, err := secrets.New().Session(srv.addr(), "alice"); err == nil {
		t.Error("session stored despite a failed pairing turn")
	}

	// `daemon` reading this blob must report "not paired", not spin on a missing
	// pin: point it at the same state database and drive it end-to-end.
	dir := filepath.Dir(statePath)
	paths := config.Paths{StateDir: dir, LogDir: filepath.Join(dir, "logs")}
	origPaths := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = origPaths })

	var dOut, dErr bytes.Buffer
	if code := run([]string{"daemon"}, &dOut, &dErr); code != 1 {
		t.Fatalf("daemon exit = %d, want 1 (stderr: %q)", code, dErr.String())
	}
	if !strings.Contains(dErr.String(), "not paired") {
		t.Errorf("daemon stderr = %q, want a not-paired message", dErr.String())
	}
}

// TestPairInvalidPortRejected mirrors TestLoginInvalidPortRejected:
// an out-of-range --port must fail whole-config validation up front,
// before the TEST_CONN probe and the interactive password/OTP exchange.
// Previously only runPair's post-pairing Validate caught it - after the server
// had already registered the device - discarding a completed pairing and
// forcing the user to repeat the OTP.
func TestPairInvalidPortRejected(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("server contacted despite invalid --port")
		return sproto.Null{}
	})
	path := setupCLI(t, &noPromptPrompter{t: t})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--port", "70000"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "out of range") {
		t.Errorf("stderr = %q, want port-out-of-range validation message", stderr.String())
	}
}

func TestPairOTPRetry(t *testing.T) {
	const wantSession = "11112222333344445555666677778888"
	calls := 0
	srv := newPairMockServer(t, func(cmd uint8, req *sproto.Map) sproto.Value {
		if cmd != sproto.CmdAuth {
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
		}
		calls++
		if calls == 1 {
			if _, ok := req.Get("otp"); ok {
				t.Error("first auth carried an otp")
			}
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOTPRequired))
		}
		if v, _ := req.Get("otp"); v != sproto.String("123456") {
			t.Errorf("retry otp = %v, want 123456", v)
		}
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	// First OTP prompt empty, retry prompt supplies the code.
	path := setupCLI(t, &fakePrompter{password: "s3cret", otps: []string{"", "123456"}})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != wantSession {
		t.Errorf("stored session = %q, want %q", got, wantSession)
	}
}

func TestPairInvalidOTP(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOTPInvalid))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret", otps: []string{"999999"}})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "one-time password") {
		t.Errorf("stderr = %q, want OTP error message", stderr.String())
	}
	// Nothing should have been persisted on failure.
	if _, err := secrets.New().Session(srv.addr(), "alice"); err == nil {
		t.Error("session stored despite failed pairing")
	}
}

func TestPairFlagsOverrideConfig(t *testing.T) {
	const wantSession = "cafebabecafebabecafebabecafebabe"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// No stored config; everything comes from flags.
	root := t.TempDir()

	var stdout, stderr bytes.Buffer
	args := []string{"pair", "--host", srv.host(), "--port", strconv.Itoa(srv.port()), "--user", "bob", "--sync-root", root}
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "bob"); got != wantSession {
		t.Errorf("stored session = %q, want %q", got, wantSession)
	}
	if _, err := keyring.Get(secrets.ServiceName, "pin/"+srv.addr()); err != nil {
		t.Errorf("pin not stored: %v", err)
	}
	cfg := loadStoredConfig(t, path)
	if cfg.Server.User != "bob" || cfg.Server.Host != srv.host() {
		t.Errorf("config server = %+v, want host %q user bob", cfg.Server, srv.host())
	}
	if cfg.SyncRoot != root {
		t.Errorf("config sync_root = %q, want %q", cfg.SyncRoot, root)
	}
}

func TestPairMissingSyncRoot(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("auth reached despite a missing sync root")
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{password: "x"})
	// Stored config supplies the server but no sync_root.
	cfg := config.Default()
	cfg.Server.Host = srv.host()
	cfg.Server.Port = srv.port()
	cfg.Server.User = "alice"
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "requires a local sync directory") {
		t.Errorf("stderr = %q, want missing-sync-root message", stderr.String())
	}
}

func TestPairRelativeSyncRoot(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("auth reached despite a relative sync root")
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{password: "x"})
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) { c.SyncRoot = "" })

	var stdout, stderr bytes.Buffer
	args := []string{"pair", "--sync-root", "relative/dir"}
	if code := run(args, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "must be an absolute path") {
		t.Errorf("stderr = %q, want absolute-path message", stderr.String())
	}
}

func TestPairProbeWithoutDsID(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("auth reached despite a probe without ds_id")
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})
	srv.testConnFrame = legacyTestConnFrameNoSerial()

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no ds_id") {
		t.Errorf("stderr = %q, want no-ds_id message", stderr.String())
	}
	if _, err := secrets.New().Session(srv.addr(), "alice"); err == nil {
		t.Error("session stored despite a probe without ds_id")
	}
}

func TestPairStoreLocked(t *testing.T) {
	setupCLI(t, &fakePrompter{password: "x"})
	// Simulate the state database already held by the running daemon.
	orig := openConfigStore
	openConfigStore = func() (*store.Store, error) { return nil, store.ErrLocked }
	t.Cleanup(func() { openConfigStore = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "in use by another synosync process") {
		t.Errorf("stderr = %q, want locked-database hint", stderr.String())
	}
}

// TestPairRepairToNewServerCleansOrphanedSecrets covers this case: a
// re-pair that changes the server host on an already-paired install erases the OLD
// identity's now-orphaned keychain session, password and certificate pin - the
// pair-path mirror of `config set server.*`'s cleanupOrphanedServerSecrets.
// Before the fix pair wrote only the NEW identity's secrets
// and left the old server's server-valid session token + pin in the keychain
// forever, while `unlink` (keyed on the CURRENT server.*) reported full cleanup.
func TestPairRepairToNewServerCleansOrphanedSecrets(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Already paired to a DIFFERENT server (nasA); the flags below re-point at the
	// mock server (nasB).
	const oldServer = "nasA.example.com:6690"
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) {
		c.Server.Host = "nasA.example.com"
		c.Server.Port = 6690
		c.DeviceUUID = "0123456789abcdef0123456789abcdef"
		c.Server.DsID = "old-ds-id"
	})
	seedServerSecrets(t, oldServer, "alice")

	var stdout, stderr bytes.Buffer
	args := []string{"pair", "--host", srv.host(), "--port", strconv.Itoa(srv.port())}
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	// The old identity's keychain entries are gone.
	if s, p, w := serverSecretsPresent(t, oldServer, "alice"); s || p || w {
		t.Errorf("old identity secrets survived re-pair: session=%v pin=%v password=%v", s, p, w)
	}
	// The new identity's session + pin are present.
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != wantSession {
		t.Errorf("new session = %q, want %q", got, wantSession)
	}
	if _, err := secrets.New().Pin(srv.addr()); err != nil {
		t.Errorf("new pin not stored: %v", err)
	}
}

// TestPairRepairNewUserCleansSessionKeepsPin confirms a user-only re-pair (same
// host, new account) erases the old user's (server, user)-keyed session and
// password but PRESERVES the certificate pin, which is keyed by the server address
// alone and still serves the same server the new user connects to - the pair-path
// mirror of TestConfigSetServerUserCleansSessionKeepsPin.
func TestPairRepairNewUserCleansSessionKeepsPin(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Already paired to THIS server as alice; the flag below re-pairs as bob.
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) {
		c.DeviceUUID = "0123456789abcdef0123456789abcdef"
		c.Server.DsID = "old-ds-id"
	})
	seedServerSecrets(t, srv.addr(), "alice")
	// Record the pin this server actually presents. seedServerSecrets stores a placeholder,
	// and a re-pair now refuses when the recorded key differs from the presented one - which
	// is a different scenario from the one under test here (same server, same key, new user).
	if err := secrets.New().SetPin(srv.addr(), srv.leafSPKIPin()); err != nil {
		t.Fatalf("SetPin: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--user", "bob"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	// The old user's session + password are gone.
	if s, _, w := serverSecretsPresent(t, srv.addr(), "alice"); s || w {
		t.Errorf("old user's session/password survived a user re-pair: session=%v password=%v", s, w)
	}
	// The pin (keyed by server address) is still present, and the new user has a session.
	if _, err := secrets.New().Pin(srv.addr()); err != nil {
		t.Errorf("certificate pin erased on a user-only re-pair: %v", err)
	}
	if got, _ := secrets.New().Session(srv.addr(), "bob"); got != wantSession {
		t.Errorf("new user session = %q, want %q", got, wantSession)
	}
}

// TestPairFreshInstallNoOrphanCleanup confirms a fresh pairing (the install was
// never paired: no DeviceUUID/DsID) performs no orphan cleanup - an unrelated
// keychain entry left from some other identity survives, since a fresh install
// stored nothing under an old identity.
func TestPairFreshInstallNoOrphanCleanup(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Unpaired: DeviceUUID and DsID are left empty by writeServerConfig.
	writeServerConfig(t, path, srv, "alice", nil)
	// An unrelated identity's secrets sit in the keychain; a fresh pair must not
	// touch them (cleanup is gated on the pre-overlay paired state).
	const other = "other.example.com:6690"
	seedServerSecrets(t, other, "carol")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if s, p, w := serverSecretsPresent(t, other, "carol"); !s || !p || !w {
		t.Errorf("fresh pair erased an unrelated identity's secrets: session=%v pin=%v password=%v", s, p, w)
	}
	// The new identity's session is present.
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != wantSession {
		t.Errorf("new session = %q, want %q", got, wantSession)
	}
}

// TestPairFailedFinalWritePreservesOldIdentitySecrets pins the load-bearing ordering of the
// re-pair cleanup: cleanupPairOrphanedSecrets runs ONLY AFTER a successful
// putPairedConfig, so a failed final config write never erases the still-current OLD identity's
// keychain secrets. The install is already paired to nasA; the re-pair to nasB writes the new
// identity's pin/session, then putPairedConfig fails and runPair returns 1 before reaching the
// orphan cleanup - nasA's session, pin and password must all survive (a reorder that erased them
// on a failed write would be the exact orphan/data-loss class this task closed, inverted).
func TestPairFailedFinalWritePreservesOldIdentitySecrets(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Already paired to a DIFFERENT server (nasA); the flags below re-point at the mock server
	// (nasB). DeviceUUID is set, so the up-front persist is skipped and putPairedConfig is the only
	// config write - failing it exercises the failed-final-write window directly.
	const oldServer = "nasA.example.com:6690"
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) {
		c.Server.Host = "nasA.example.com"
		c.Server.Port = 6690
		c.DeviceUUID = "0123456789abcdef0123456789abcdef"
		c.Server.DsID = "old-ds-id"
	})
	seedServerSecrets(t, oldServer, "alice")

	orig := putPairedConfig
	putPairedConfig = func(*store.Store, config.Config) error {
		return errors.New("simulated final config write failure")
	}
	t.Cleanup(func() { putPairedConfig = orig })

	var stdout, stderr bytes.Buffer
	args := []string{"pair", "--host", srv.host(), "--port", strconv.Itoa(srv.port())}
	if code := run(args, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	// The OLD identity's secrets survive the failed write - cleanup runs only after PutConfig succeeds.
	if s, p, w := serverSecretsPresent(t, oldServer, "alice"); !s || !p || !w {
		t.Errorf("old identity secrets erased on a FAILED final write: session=%v pin=%v password=%v", s, p, w)
	}
}

func TestPairMissingServer(t *testing.T) {
	setupCLI(t, &fakePrompter{password: "x"})
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "requires a server host and user") {
		t.Errorf("stderr = %q, want missing-server message", stderr.String())
	}
}

// TestPairRefusesAChangedKeyWithoutExpectPin pins that a re-pair does not silently reset the
// pin.
//
// Re-pairing is the documented remedy the daemon itself points at after a pin mismatch, so it
// is also the path an attacker who can force one sends the operator down. Overwriting the
// recorded pin without a word made that remedy a trust-on-first-use reset, and the password and
// OTP would then be sent over the very session whose identity had just changed.
func TestPairRefusesAChangedKeyWithoutExpectPin(t *testing.T) {
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String("abcdef0123456789abcdef0123456789"))
	})
	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)
	// A pin recorded for this server that is NOT the key it presents.
	if err := secrets.New().SetPin(srv.addr(), "spki-sha256:"+strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("SetPin: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"pair"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("pair accepted a changed server key without out-of-band confirmation")
	}
	out := stderr.String()
	for _, want := range []string{
		"certificate key CHANGED",
		"recorded:",
		"presented:",
		"--expect-pin",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stderr missing %q:\n%s", want, out)
		}
	}
	// The recorded pin is untouched: a refusal must not be a partial reset.
	if got, _ := secrets.New().Pin(srv.addr()); got != "spki-sha256:"+strings.Repeat("aa", 32) {
		t.Errorf("pin = %q, want the recorded one left alone", got)
	}
}

// TestPairAcceptsAChangedKeyWithMatchingExpectPin is the other half: --expect-pin is the
// out-of-band confirmation, so a key change the operator has verified proceeds.
func TestPairAcceptsAChangedKeyWithMatchingExpectPin(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newPairMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})
	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)
	if err := secrets.New().SetPin(srv.addr(), "spki-sha256:"+strings.Repeat("aa", 32)); err != nil {
		t.Fatalf("SetPin: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair", "--expect-pin", srv.leafSPKIPin()}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Pin(srv.addr()); got != srv.leafSPKIPin() {
		t.Errorf("pin = %q, want the newly confirmed %q", got, srv.leafSPKIPin())
	}
}
