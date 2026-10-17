package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// seedConfig writes a minimal valid paired-looking config into the state
// database at statePath, applying any extra mutation.
func seedConfig(t *testing.T, statePath string, mutate func(*config.Config)) {
	t.Helper()
	cfg := config.Default()
	cfg.SyncRoot = t.TempDir()
	cfg.Server.Host = "ds.example.com"
	cfg.Server.User = "alice"
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

func TestConfigGetAll(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.SyncRoot = "/data/sync"
		c.Server.Host = "ds.example.com"
		c.Server.Port = 6690
		c.Server.User = "alice"
		c.LogLevel = "info"
		c.LogFormat = "text"
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"log_level=info",
		"log_format=text",
		"sync_root=/data/sync",
		"server.host=ds.example.com",
		"server.port=6690",
		"server.user=alice",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, missing %q", out, want)
		}
	}
}

func TestConfigGetOne(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) { c.LogLevel = "debug" })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get", "log_level"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "debug" {
		t.Errorf("stdout = %q, want %q", got, "debug")
	}
}

// TestConfigGetUnknownKey pins the usage-error exit code for a key outside the
// whitelist: 2, like every other CLI usage error (a missing or extra argument), not
// the 1 a runtime failure uses - a script that distinguishes "you typed it wrong"
// from "the operation failed" got the wrong answer before this fix. The
// message must also name the valid keys.
func TestConfigGetUnknownKey(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get", "nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (usage error)", code)
	}
	if !strings.Contains(stderr.String(), "unknown config key") {
		t.Errorf("stderr = %q, want unknown-key message", stderr.String())
	}
	if !strings.Contains(stderr.String(), "log_level") {
		t.Errorf("stderr = %q, want the valid keys listed", stderr.String())
	}
}

// TestConfigGetUnknownKeyWithoutStore verifies the key check runs before the store
// is opened: naming a typo must not depend on the state database existing.
func TestConfigGetUnknownKeyWithoutStore(t *testing.T) {
	setupCLI(t, &fakePrompter{}) // no config seeded: the store has no config blob

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get", "nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (usage error)", code)
	}
	if !strings.Contains(stderr.String(), "unknown config key") {
		t.Errorf("stderr = %q, want unknown-key message", stderr.String())
	}
}

func TestConfigSetAndReread(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) { c.LogLevel = "info" })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "log_level", "warn"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "log_level=warn") {
		t.Errorf("stdout = %q, want confirmation", stdout.String())
	}
	if got := loadStoredConfig(t, path); got.LogLevel != "warn" {
		t.Errorf("persisted log_level = %q, want warn", got.LogLevel)
	}
}

func TestConfigSetPort(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.port", "7000"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := loadStoredConfig(t, path); got.Server.Port != 7000 {
		t.Errorf("persisted port = %d, want 7000", got.Server.Port)
	}
}

// TestConfigSetServerIdentityWarnsWhenPaired verifies that changing a
// connection-identity field (host/port/user) on an already-paired install still
// succeeds and warns that the previous identity's keychain secrets are cleared
// automatically while the new identity still requires `synosync pair`.
func TestConfigSetServerIdentityWarnsWhenPaired(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"server.host", "other.example.com"},
		{"server.port", "7001"},
		{"server.user", "bob"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			path := setupCLI(t, &fakePrompter{})
			seedConfig(t, path, func(c *config.Config) {
				c.DeviceUUID = "dev-uuid"
				c.Server.DsID = "ds-serial"
			})

			var stdout, stderr bytes.Buffer
			if code := run([]string{"config", "set", tc.key, tc.value}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "clears the previous identity") {
				t.Errorf("stderr = %q, want old-secrets-cleared warning", stderr.String())
			}
			if !strings.Contains(stderr.String(), "synosync pair") {
				t.Errorf("stderr = %q, want re-pair hint", stderr.String())
			}
		})
	}
}

// TestConfigSetServerIdentityNoWarnUnpaired verifies the identity-change warning
// is suppressed on an install that has not paired yet (no DeviceUUID/DsID), since
// there is no keychain pin/session to orphan.
func TestConfigSetServerIdentityNoWarnUnpaired(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil) // unpaired: DeviceUUID and DsID are empty

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.host", "other.example.com"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "clears the previous identity") {
		t.Errorf("stderr = %q, want no identity-change warning for unpaired install", stderr.String())
	}
}

// TestConfigSetSyncRootWarnsWhenPaired verifies that changing sync_root on an
// already-paired install still succeeds but warns that the state database's
// baselines describe the tree under the previous root, so the files must be
// physically moved before the next daemon start.
func TestConfigSetSyncRootWarnsWhenPaired(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
	})

	// The new root must be absolute under HOST rules (config.Validate applies
	// filepath.IsAbs): a POSIX literal like "/data/new-root" is drive-relative on
	// Windows and the set would fail validation instead of exercising the warning.
	newRoot := filepath.Join(t.TempDir(), "new-root")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "sync_root", newRoot}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "previous sync root") {
		t.Errorf("stderr = %q, want previous-sync-root warning", stderr.String())
	}
	if !strings.Contains(stderr.String(), "locally deleted") {
		t.Errorf("stderr = %q, want mass-delete hint", stderr.String())
	}
	if got := loadStoredConfig(t, path); got.SyncRoot != newRoot {
		t.Errorf("persisted sync_root = %q, want %q", got.SyncRoot, newRoot)
	}
}

// TestConfigSetSyncRootNoWarnUnpaired verifies the sync_root warning is suppressed
// on an install that never paired - there is no synced state to diverge from.
func TestConfigSetSyncRootNoWarnUnpaired(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil) // unpaired: DeviceUUID and DsID are empty
	newRoot := filepath.Join(t.TempDir(), "new-root")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "sync_root", newRoot}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "previous sync root") {
		t.Errorf("stderr = %q, want no sync-root warning for unpaired install", stderr.String())
	}
}

// seedServerSecrets stores a session, certificate pin and password under
// (server, user) in the mocked keychain so the identity-change cleanup tests can
// assert whether they survive a `config set server.*`.
func seedServerSecrets(t *testing.T, server, user string) {
	t.Helper()
	kr := secrets.New()
	if err := kr.SetSession(server, user, "tok-"+server); err != nil {
		t.Fatalf("SetSession(%s): %v", server, err)
	}
	if err := kr.SetPin(server, "spki-sha256:"+server); err != nil {
		t.Fatalf("SetPin(%s): %v", server, err)
	}
	if err := kr.SetLegacyPassword(server, user, "pw-"+server); err != nil {
		t.Fatalf("SetLegacyPassword(%s): %v", server, err)
	}
}

// serverSecretsPresent reports whether the session, pin and password entries for
// (server, user) are still stored in the mocked keychain.
func serverSecretsPresent(t *testing.T, server, user string) (session, pin, password bool) {
	t.Helper()
	kr := secrets.New()
	_, sErr := kr.Session(server, user)
	_, pErr := kr.Pin(server)
	_, wErr := kr.Password(server, user)
	return sErr == nil, pErr == nil, wErr == nil
}

// TestConfigSetServerAddressCleansOrphanedKeychain confirms that changing the
// server host or port on a paired install best-effort erases the previous
// identity's keychain session, certificate pin and password immediately at set
// time - otherwise a later `unlink` would report full
// cleanup while those secrets lingered unreachably (unlink cleans only the
// CURRENT address).
func TestConfigSetServerAddressCleansOrphanedKeychain(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"server.host", "other.example.com"},
		{"server.port", "7001"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			path := setupCLI(t, &fakePrompter{})
			seedConfig(t, path, func(c *config.Config) {
				c.DeviceUUID = "dev-uuid"
				c.Server.DsID = "ds-serial"
			})
			oldServer := "ds.example.com:6690"
			seedServerSecrets(t, oldServer, "alice")

			var stdout, stderr bytes.Buffer
			if code := run([]string{"config", "set", tc.key, tc.value}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if s, p, w := serverSecretsPresent(t, oldServer, "alice"); s || p || w {
				t.Errorf("old keychain entries survived: session=%v pin=%v password=%v", s, p, w)
			}
		})
	}
}

// TestConfigSetServerUserCleansSessionKeepsPin confirms that changing only the
// account user erases the (server, user)-keyed session and password but PRESERVES
// the certificate pin, which is keyed by the server address alone and still
// serves the same server the new user connects to.
func TestConfigSetServerUserCleansSessionKeepsPin(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
	})
	server := "ds.example.com:6690"
	seedServerSecrets(t, server, "alice")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.user", "bob"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	s, p, w := serverSecretsPresent(t, server, "alice")
	if s {
		t.Errorf("old user's session survived a user change")
	}
	if w {
		t.Errorf("old user's password survived a user change")
	}
	if !p {
		t.Errorf("certificate pin (keyed by server address only) was erased on a user-only change")
	}
}

// TestConfigSetServerIdentityCleanupBackendFailureWarns confirms a keyring
// backend failure while erasing the orphaned secrets warns but does not fail the
// set - the new value is already persisted, so the cleanup is best effort.
func TestConfigSetServerIdentityCleanupBackendFailureWarns(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
	})

	orig := openKeyring
	openKeyring = func() secretStore {
		return failingDeleteKeyring{secretStore: secrets.New(), err: errors.New("keychain locked")}
	}
	t.Cleanup(func() { openKeyring = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.host", "other.example.com"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "could not erase orphaned session") {
		t.Errorf("stderr = %q, want orphaned-session cleanup warning", stderr.String())
	}
	if got := loadStoredConfig(t, path); got.Server.Host != "other.example.com" {
		t.Errorf("persisted host = %q, want other.example.com (set must still succeed)", got.Server.Host)
	}
}

// TestConfigSetServerIdentityUnpairedNoCleanup confirms that on an install that
// never paired (no DeviceUUID/DsID) a server.* change touches no keychain entry -
// there is nothing paired to orphan.
func TestConfigSetServerIdentityUnpairedNoCleanup(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil) // unpaired
	server := "ds.example.com:6690"
	seedServerSecrets(t, server, "alice")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.host", "other.example.com"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if s, p, w := serverSecretsPresent(t, server, "alice"); !s || !p || !w {
		t.Errorf("unpaired install erased keychain entries: session=%v pin=%v password=%v", s, p, w)
	}
}

// TestConfigSetNonIdentityNoKeychainCleanup confirms a non-identity field change
// (log_level) leaves every keychain entry intact even on a paired install.
func TestConfigSetNonIdentityNoKeychainCleanup(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
	})
	server := "ds.example.com:6690"
	seedServerSecrets(t, server, "alice")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "log_level", "warn"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if s, p, w := serverSecretsPresent(t, server, "alice"); !s || !p || !w {
		t.Errorf("log_level change erased keychain entries: session=%v pin=%v password=%v", s, p, w)
	}
}

// TestConfigSetServerHostSameValueNoCleanup confirms setting a server.* field to
// its current value (no actual change) leaves the keychain intact - the entries
// are not orphaned.
func TestConfigSetServerHostSameValueNoCleanup(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
	})
	server := "ds.example.com:6690"
	seedServerSecrets(t, server, "alice")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.host", "ds.example.com"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if s, p, w := serverSecretsPresent(t, server, "alice"); !s || !p || !w {
		t.Errorf("unchanged host erased keychain entries: session=%v pin=%v password=%v", s, p, w)
	}
	// The identity-change warning must be suppressed too: nothing was orphaned
	// and nothing was cleared, so claiming otherwise would mislead the operator.
	if strings.Contains(stderr.String(), "clears the previous identity") {
		t.Errorf("stderr = %q, want no identity-change warning for unchanged host", stderr.String())
	}
}

// TestConfigSetNonIdentityNoWarn verifies a non-identity field change never
// emits the identity-change warning even on a paired install.
func TestConfigSetNonIdentityNoWarn(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) {
		c.DeviceUUID = "dev-uuid"
		c.Server.DsID = "ds-serial"
		c.LogLevel = "info"
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "log_level", "warn"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "clears the previous identity") {
		t.Errorf("stderr = %q, want no identity-change warning for log_level change", stderr.String())
	}
}

func TestConfigSetUnknownKey(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "nope", "x"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (usage error)", code)
	}
	if !strings.Contains(stderr.String(), "unknown config key") {
		t.Errorf("stderr = %q, want unknown-key message", stderr.String())
	}
	if !strings.Contains(stderr.String(), "log_level") {
		t.Errorf("stderr = %q, want the valid keys listed", stderr.String())
	}
}

func TestConfigSetInvalidValueNotPersisted(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, func(c *config.Config) { c.LogLevel = "info" })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "log_level", "loud"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid log_level") {
		t.Errorf("stderr = %q, want validation message", stderr.String())
	}
	// The bad value must not have been persisted.
	if got := loadStoredConfig(t, path); got.LogLevel != "info" {
		t.Errorf("persisted log_level = %q, want unchanged info", got.LogLevel)
	}
}

func TestConfigSetNonNumericPort(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "server.port", "abc"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not a number") {
		t.Errorf("stderr = %q, want not-a-number message", stderr.String())
	}
}

// TestConfigSetInvalidValueReportsField confirms that setting an invalid value
// reports the field actually at fault even when the stored config is otherwise
// incomplete - the field-level validator runs before the whole-config Validate,
// so the user sees "invalid log_level" rather than a misleading
// "sync_root must be set" from an unrelated unset field.
func TestConfigSetInvalidValueReportsField(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	// Seed an incomplete config: sync_root/host/user are all unset, so the
	// whole-config Validate would fail on sync_root first.
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	cfg := config.Default()
	cfg.LogLevel = "info"
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "log_level", "loud"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "invalid log_level") {
		t.Errorf("stderr = %q, want log_level-specific message", stderr.String())
	}
	if strings.Contains(stderr.String(), "sync_root") {
		t.Errorf("stderr = %q, should not mention unrelated sync_root", stderr.String())
	}
}

// TestConfigHelp confirms `config --help` (and -h/help) prints usage and exits 0
// rather than reporting an unknown action.
func TestConfigHelp(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, arg := range []string{"--help", "-h", "help"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"config", arg}, &stdout, &stderr); code != 0 {
			t.Fatalf("config %s exit = %d, want 0 (stderr: %q)", arg, code, stderr.String())
		}
		out := stdout.String()
		for _, want := range []string{
			"synosync config - get or set configuration values",
			"Usage:\n  synosync config get [key]",
			"synosync config set <key> <value>",
			// The key list is rendered from configKeyNames, the same table
			// `config set` validates against, so the page cannot advertise a key
			// the command would then reject.
			"Keys:",
			"log_level",
			"limits.op_turn_timeout_s",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("config %s stdout = %q, want it to contain %q", arg, out, want)
			}
		}
		if strings.Contains(stderr.String(), "unknown config action") {
			t.Errorf("config %s stderr = %q, should not report unknown action", arg, stderr.String())
		}
	}
}

// lockConfigStore makes openConfigStore report the state database as held by
// another process (the ErrLocked shape a running daemon produces - a real
// double-open would burn the 5 s bbolt lock timeout per test).
func lockConfigStore(t *testing.T) {
	t.Helper()
	orig := openConfigStore
	openConfigStore = func() (*store.Store, error) { return nil, store.ErrLocked }
	t.Cleanup(func() { openConfigStore = orig })
}

// stubConfigDaemon starts a real in-process IPC server on a temp socket whose
// `config` method returns cfg, and points resolveDaemonPaths at its directory -
// the shape this fallback serves: the store lock is held by a serving daemon.
func stubConfigDaemon(t *testing.T, cfg config.Config) {
	t.Helper()
	dir := sockDir(t)
	paths := config.Paths{StateDir: dir}

	ln, err := ipc.Listen(paths.DaemonSock())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := ipc.NewServer()
	srv.Handle("config", func(context.Context, json.RawMessage) (any, error) {
		return ipc.ConfigReport{Config: cfg}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })
}

// TestConfigGetViaDaemonWhenLocked reproduces the locked-store case:
// with the state database locked by a running daemon, `config get` must fall
// back to the daemon's read-only `config` IPC method instead of demanding the
// daemon be stopped. (A read-only bbolt open cannot serve this - its shared
// file lock conflicts with the daemon's exclusive one, verified empirically -
// which is why the fallback is an IPC method.)
func TestConfigGetViaDaemonWhenLocked(t *testing.T) {
	setupCLI(t, &fakePrompter{})
	lockConfigStore(t)

	cfg := config.Default()
	cfg.SyncRoot = "/srv/sync"
	cfg.Server = config.Server{Host: "live.example.com", Port: 7000, User: "bob"}
	stubConfigDaemon(t, cfg)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"server.host=live.example.com",
		"server.port=7000",
		"server.user=bob",
		"sync_root=/srv/sync",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout missing %q\nfull output:\n%s", want, out)
		}
	}
	if strings.Contains(stderr.String(), "in use by another synosync process") {
		t.Errorf("stderr = %q, should not print the stop-the-daemon hint", stderr.String())
	}

	// Single-key get takes the same fallback.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"config", "get", "server.host"}, &stdout, &stderr); code != 0 {
		t.Fatalf("single-key exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "live.example.com" {
		t.Errorf("single-key stdout = %q, want live.example.com", got)
	}
}

// TestConfigStoreLocked pins the no-daemon-behind-the-lock behaviour: when the
// lock holder is not a serving daemon (a pair/login in flight), `config get`
// still prints the friendly stop-the-daemon hint - and `config set` always
// does, since mutating settings requires the exclusive open.
func TestConfigStoreLocked(t *testing.T) {
	setupCLI(t, &fakePrompter{})
	lockConfigStore(t)
	// An empty state dir: the fallback dial finds no socket → daemon absent.
	dir := sockDir(t)
	origPaths := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return config.Paths{StateDir: dir}, nil }
	t.Cleanup(func() { resolveDaemonPaths = origPaths })

	for _, args := range [][]string{
		{"config", "get"},
		{"config", "set", "log_level", "debug"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 1 {
			t.Fatalf("%v: exit = %d, want 1", args, code)
		}
		if !strings.Contains(stderr.String(), "in use by another synosync process") {
			t.Errorf("%v: stderr = %q, want locked-database hint", args, stderr.String())
		}
	}
}

func TestConfigUsage(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "config needs an action") {
		t.Errorf("stderr = %q, want it to name the missing action", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage:\n  synosync config get [key]") {
		t.Errorf("stderr = %q, want the invocation forms", stderr.String())
	}
}

func TestConfigUnknownAction(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown config action") {
		t.Errorf("stderr = %q, want unknown-action message", stderr.String())
	}
}

// TestConfigGetLimitKeys confirms the new resource-limit knobs are exposed by
// `config get`, printing their (defaulted) values.
func TestConfigGetLimitKeys(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"limits.workers=3",
		"limits.local_scan_interval_s=5",
		"limits.drain_batch=256",
		"limits.upload_resume_threshold=4194304",
		"limits.empty_scan_threshold=3",
		"limits.watcher_debounce_ms=500",
		"limits.cold_start_catalog_cap=0",
		"limits.event_poll_timeout_s=30",
		"limits.poll_deadline_slack_s=300",
		"limits.full_rehash_interval=60",
		"limits.op_turn_timeout_s=0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, missing %q", out, want)
		}
	}
}

// TestConfigSetLimitKey round-trips an int limit knob through set → persist → reread.
func TestConfigSetLimitKey(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.drain_batch", "128"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "limits.drain_batch=128") {
		t.Errorf("stdout = %q, want confirmation", stdout.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.DrainBatch != 128 {
		t.Errorf("persisted DrainBatch = %d, want 128", got.Limits.DrainBatch)
	}
}

// TestConfigSetLimitInt64Key round-trips a byte-count (int64) limit knob.
func TestConfigSetLimitInt64Key(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.upload_resume_threshold", "8388608"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.UploadResumeThreshold != 8388608 {
		t.Errorf("persisted UploadResumeThreshold = %d, want 8388608", got.Limits.UploadResumeThreshold)
	}
}

// TestConfigSetLimitOutOfRangeNotPersisted confirms a limit value that fails
// Config.Validate (workers outside [2,4]) is rejected and not persisted.
func TestConfigSetLimitOutOfRangeNotPersisted(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.workers", "9"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "out of range") {
		t.Errorf("stderr = %q, want range-violation message", stderr.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.Workers != 3 {
		t.Errorf("persisted Workers = %d, want unchanged 3", got.Limits.Workers)
	}
}

// TestConfigSetLimitNonNumericRejected confirms a non-numeric limit value is rejected.
func TestConfigSetLimitNonNumericRejected(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.drain_batch", "lots"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not a number") {
		t.Errorf("stderr = %q, want not-a-number message", stderr.String())
	}
}

// TestConfigSetFullRehashInterval round-trips the Phase-9d full-rehash knob
// through set → persist → reread.
func TestConfigSetFullRehashInterval(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.full_rehash_interval", "120"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "limits.full_rehash_interval=120") {
		t.Errorf("stdout = %q, want confirmation", stdout.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.FullRehashInterval != 120 {
		t.Errorf("persisted FullRehashInterval = %d, want 120", got.Limits.FullRehashInterval)
	}
}

// TestConfigSetFullRehashIntervalZero confirms 0 (disabled) passes validation,
// is accepted by `config set` (exit 0), and - because the field is not
// omitempty - durably persists as 0 rather than re-defaulting to 60 on the next
// read. This is the documented "disable the periodic re-hash" path.
func TestConfigSetFullRehashIntervalZero(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.full_rehash_interval", "0"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "limits.full_rehash_interval=0") {
		t.Errorf("stdout = %q, want confirmation", stdout.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.FullRehashInterval != 0 {
		t.Errorf("persisted FullRehashInterval = %d, want 0 (disabled)", got.Limits.FullRehashInterval)
	}
}

// TestConfigSetFullRehashIntervalNegativeRejected confirms a negative value
// fails Config.Validate and is not persisted.
func TestConfigSetFullRehashIntervalNegativeRejected(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "limits.full_rehash_interval", "-1"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "non-negative") {
		t.Errorf("stderr = %q, want non-negative message", stderr.String())
	}
	if got := loadStoredConfig(t, path); got.Limits.FullRehashInterval != defaultFullRehashIntervalForTest() {
		t.Errorf("persisted FullRehashInterval = %d, want unchanged default", got.Limits.FullRehashInterval)
	}
}

// defaultFullRehashIntervalForTest mirrors config.Default()'s value so the
// rejection test asserts the field stayed at its seeded default.
func defaultFullRehashIntervalForTest() int {
	return config.Default().Limits.FullRehashInterval
}

func TestConfigGetUnconfigured(t *testing.T) {
	// No config written: loadConfigFromStore falls back to config.Default(), so
	// get-all still prints the defaults rather than erroring.
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get", "log_level"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "info" {
		t.Errorf("stdout = %q, want default info", got)
	}
}

// TestConfigSetOnFreshStore reproduces the fresh-install deadlock: on a
// fresh install (no config blob) the former whole-config Validate rejected
// EVERY single-key set - each failed on another still-empty required field, in
// every call order, blocking even unrelated keys like log_level - while pair's
// own hint pointed at "set them in config". With the ValidatePartial gate the
// keys go in one at a time, `config get` sees them, and a subsequent flagless
// `pair` completes against the pre-set host/user/sync_root.
func TestConfigSetOnFreshStore(t *testing.T) {
	const wantSession = "abcdef0123456789abcdef0123456789"
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(wantSession))
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	syncRoot := t.TempDir()

	// Each required field lands on its own, in an order the old gate rejected
	// at every step (host first would fail on sync_root, and vice versa).
	for _, kv := range [][2]string{
		{"server.host", srv.host()},
		{"server.port", strconv.Itoa(srv.port())},
		{"server.user", "alice"},
		{"sync_root", syncRoot},
		{"log_level", "debug"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"config", "set", kv[0], kv[1]}, &stdout, &stderr); code != 0 {
			t.Fatalf("config set %s: exit = %d (stderr: %q)", kv[0], code, stderr.String())
		}
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "get"}, &stdout, &stderr); code != 0 {
		t.Fatalf("config get: exit = %d (stderr: %q)", code, stderr.String())
	}
	for _, want := range []string{
		"server.host=" + srv.host(),
		"server.user=alice",
		"sync_root=" + syncRoot,
		"log_level=debug",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("config get stdout = %q, missing %q", stdout.String(), want)
		}
	}

	// A flagless pair now reads the pre-set coordinates and completes.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("pair: exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "paired with") {
		t.Errorf("pair stdout = %q, want pairing confirmation", stdout.String())
	}
	if got, err := secrets.New().Session(srv.addr(), "alice"); err != nil || got != wantSession {
		t.Errorf("stored session = %q, err = %v; want %q", got, err, wantSession)
	}
	if got := loadStoredConfig(t, path); got.Server.DsID != testDsID {
		t.Errorf("config ds_id = %q, want %q", got.Server.DsID, testDsID)
	}
}

// TestConfigSetFreshStoreInvalidValueRejected confirms the ValidatePartial gate
// still rejects an invalid value on a fresh store: field-shape violations
// (relative sync_root, bad log_level) and the keychain-key safety check on
// server.host (a '/' would alias distinct (host, user) keychain entries - the
// per-key validator only checks non-emptiness, so this exercises the partial
// whole-config pass specifically). Nothing is persisted.
func TestConfigSetFreshStoreInvalidValueRejected(t *testing.T) {
	for _, tc := range []struct{ key, value, wantMsg string }{
		{"log_level", "verbose", "invalid log_level"},
		{"sync_root", "relative/path", "must be an absolute path"},
		{"server.host", "a/b", "must not contain"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			path := setupCLI(t, &fakePrompter{})

			var stdout, stderr bytes.Buffer
			if code := run([]string{"config", "set", tc.key, tc.value}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantMsg) {
				t.Errorf("stderr = %q, want %q", stderr.String(), tc.wantMsg)
			}
			// The rejected set must not have persisted anything: the fresh
			// store still holds no config blob at all.
			st, err := store.Open(path)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer func() { _ = st.Close() }()
			if _, err := st.GetConfig(); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetConfig after rejected set: err = %v, want ErrNotFound (nothing persisted)", err)
			}
		})
	}
}

// TestConfigSetOutOfRangeRejected confirms a value that fails validation is
// REFUSED AND NOT PERSISTED - on a paired-looking blob and on a fresh store alike.
// It used to exercise the upload_chunk_size ≤ upload_resume_threshold pair; that
// knob was removed in schema v9 (nothing on the wire chunks an upload), so the
// property is pinned on a single-field bound instead. What matters is unchanged:
// `config set` validates before it writes.
func TestConfigSetOutOfRangeRejected(t *testing.T) {
	tooBig := strconv.FormatInt(int64(1)<<40, 10)

	t.Run("paired", func(t *testing.T) {
		path := setupCLI(t, &fakePrompter{})
		seedConfig(t, path, func(c *config.Config) {
			c.DeviceUUID = "dev-uuid"
			c.Server.DsID = "ds-serial"
		})

		var stdout, stderr bytes.Buffer
		if code := run([]string{"config", "set", "limits.upload_resume_threshold", tooBig}, &stdout, &stderr); code != 1 {
			t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "exceeds maximum") {
			t.Errorf("stderr = %q, want the range message", stderr.String())
		}
		if got := loadStoredConfig(t, path); got.Limits.UploadResumeThreshold != config.Default().Limits.UploadResumeThreshold {
			t.Errorf("persisted UploadResumeThreshold = %d, want unchanged default", got.Limits.UploadResumeThreshold)
		}
	})

	t.Run("fresh", func(t *testing.T) {
		setupCLI(t, &fakePrompter{})

		var stdout, stderr bytes.Buffer
		if code := run([]string{"config", "set", "limits.upload_resume_threshold", tooBig}, &stdout, &stderr); code != 1 {
			t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "exceeds maximum") {
			t.Errorf("stderr = %q, want the range message", stderr.String())
		}
	})
}

// seedBaseline writes one baseline into the state database, so the store reports
// a synced tree the way it would after a first sync.
func seedBaseline(t *testing.T, statePath string) {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "h", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
}

// seedViews records the views the daemon would have enumerated, which is what
// `config set views.selected` resolves a typed name against.
func seedViews(t *testing.T, statePath string, names ...string) {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	for i, n := range names {
		if err := st.PutView(uint64(i+1), store.View{Name: n, NodeID: "n"}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
	}
}

// TestConfigSetViewsSelected covers the settable form of the selection, and the
// layout that follows on a fresh install.
func TestConfigSetViewsSelected(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if !cfg.Views.Flat || len(cfg.Views.Selected) != 1 || cfg.Views.Selected[0].ID != 1 {
		t.Fatalf("Views = %+v, want flat on view 1", cfg.Views)
	}
	if !strings.Contains(stdout.String(), "layout:") {
		t.Errorf("stdout = %q, want the resulting layout named", stdout.String())
	}
}

// TestConfigSetViewsNeedsKnownViews: the selection is matched by view id, so a
// name can only be resolved once the daemon has enumerated the views.
func TestConfigSetViewsNeedsKnownViews(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no views are known yet") {
		t.Errorf("stderr = %q, want the reason named", stderr.String())
	}
}

// TestConfigSetViewsAllResetsToTheDefault pins the spelling that means "every
// view", which is also what an install predating the setting has.
func TestConfigSetViewsAllResetsToTheDefault(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home,Team"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if code := run([]string{"config", "set", "views.selected", "all"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if len(cfg.Views.Selected) != 0 || cfg.Views.Flat {
		t.Errorf("Views = %+v, want the default", cfg.Views)
	}
}

// TestConfigSetViewsRefusesALayoutChangeOnASyncedTree is the guard. The layout
// decides where every file lives, so changing it relocates a tree the baselines
// describe: the engine would look in the new place, find nothing, read it as a
// local deletion of everything, and push those deletions to the server.
func TestConfigSetViewsRefusesALayoutChangeOnASyncedTree(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home,Team"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	seedBaseline(t, statePath)

	stderr.Reset()
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "would move every synced file") {
		t.Errorf("stderr = %q, want the relocation explained", stderr.String())
	}
	// The only route the gate actually permits. An earlier message advised moving
	// the files and retrying, which could never pass - and following it left the
	// tree in exactly the state the gate exists to prevent.
	// The recovery must be achievable end to end. "unlink and pair again" alone
	// dead-ended in one direction: unlink resets the store but leaves the files,
	// so re-pairing INTO a flat layout then met the empty-sync-root check over the
	// very tree just synced and refused a second time.
	for _, want := range []string{"stop the daemon", "move", "synosync unlink"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr = %q, want the recovery step %q", stderr.String(), want)
		}
	}
	if cfg := loadStoredConfig(t, statePath); cfg.Views.Flat {
		t.Error("the refused layout change was written anyway")
	}
}

// TestConfigSetViewsRefusesAFlatToFlatSwap is the second data-loss path. The flag
// stays true, so a gate keyed on it alone waved this through - while the sync
// root's occupant changed completely: the old view's files stay put, the new view
// finds them untracked, and uploads a personal tree into a shared folder.
func TestConfigSetViewsRefusesAFlatToFlatSwap(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	seedBaseline(t, statePath)

	stderr.Reset()
	if code := run([]string{"config", "set", "views.selected", "Team"}, &stdout, &stderr); code != 1 {
		t.Fatalf("flat-to-flat swap exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "would move every synced file") {
		t.Errorf("stderr = %q, want the swap refused as a relocation", stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if len(cfg.Views.Selected) != 1 || cfg.Views.Selected[0].Name != "home" {
		t.Errorf("Views = %+v, want the original selection kept", cfg.Views)
	}
}

// TestConfigSetViewsAllowsADeselectThatKeepsTheLayout confirms the guard is about
// the LAYOUT, not the selection: dropping one view of several stops syncing it
// and leaves its local files alone, the same shape a view that vanished
// server-side already has.
func TestConfigSetViewsAllowsADeselectThatKeepsTheLayout(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team", "Photos")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home,Team,Photos"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	seedBaseline(t, statePath)

	if code := run([]string{"config", "set", "views.selected", "home,Team"}, &stdout, &stderr); code != 0 {
		t.Fatalf("deselect exit = %d (stderr: %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if len(cfg.Views.Selected) != 2 || cfg.Views.Flat {
		t.Errorf("Views = %+v, want two views and no layout change", cfg.Views)
	}
}

// TestConfigSetViewsRequiresAnEmptyRootWhenTurningFlatOn: the hazard
// requireEmptySyncRoot exists for is identical here, and this key had no
// counterpart to pair's check - so `config set sync_root ~/Documents` followed by
// selecting one view would upload the whole Documents tree.
func TestConfigSetViewsRequiresAnEmptyRootWhenTurningFlatOn(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "taxes.pdf"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "sync_root", root}, &stdout, &stderr); code != 0 {
		t.Fatalf("sync_root exit = %d (stderr: %q)", code, stderr.String())
	}
	stderr.Reset()
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "taxes.pdf") {
		t.Errorf("stderr = %q, want it to name what is in the way", stderr.String())
	}
	if cfg := loadStoredConfig(t, statePath); cfg.Views.Flat {
		t.Error("the flat layout was written over a populated sync root")
	}
}

// TestConfigSetViewsRefusesWhileOpsAreQueued covers the enqueue-first window: an
// op is durably queued BEFORE its pending baseline, so a crash there leaves work
// tied to the current layout with no baseline to show for it. Relayouting then
// recomputes those ops' serialisation keys under the new layout.
func TestConfigSetViewsRefusesWhileOpsAreQueued(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home", "Team")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "views.selected", "home,Team"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}

	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.Enqueue(store.Op{Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	stderr.Reset()
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "would move every synced file") {
		t.Errorf("stderr = %q, want the relayout refused", stderr.String())
	}
}

// TestConfigSetErrorsShareOneShape pins that every way of getting a value wrong reports it the
// same way. There were four shapes: a bare field message, one carrying the internal "config: "
// package prefix, one wrapped in "invalid value:", and a lowercase bare "usage:" line unlike
// every other subcommand's usage error. A user cannot tell which layer rejected their input,
// and should not have to.
func TestConfigSetErrorsShareOneShape(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"per-key validator", []string{"config", "set", "log_level", "bogus"}, "synosync: invalid log_level"},
		{"internal limits validator", []string{"config", "set", "limits.workers", "9"}, "synosync: limits.workers"},
		{"parse failure", []string{"config", "set", "limits.workers", "abc"}, "synosync: limits.workers"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
			}
			got := stderr.String()
			if !strings.HasPrefix(got, tc.want) {
				t.Errorf("stderr = %q, want it to start with %q", got, tc.want)
			}
			if strings.Contains(got, "config: ") {
				t.Errorf("stderr leaks the internal package prefix: %q", got)
			}
			if strings.Contains(got, "invalid value:") {
				t.Errorf("stderr keeps the redundant wrapper: %q", got)
			}
		})
	}
}

// TestConfigUsageErrorsUseTheStandardTemplate pins that a wrong argument COUNT reports like
// every other subcommand's usage error - the "synosync: " prefix and the shared hint - rather
// than a bare lowercase "usage:" line unique to this command.
func TestConfigUsageErrorsUseTheStandardTemplate(t *testing.T) {
	for _, args := range [][]string{
		{"config", "set", "log_level"},
		{"config", "get", "a", "b"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Fatalf("%v: exit = %d, want 2", args, code)
		}
		got := stderr.String()
		if !strings.HasPrefix(got, "synosync: ") {
			t.Errorf("%v: stderr = %q, want the synosync: prefix", args, got)
		}
		if !strings.Contains(got, "--help") {
			t.Errorf("%v: stderr = %q, want the shared usage hint", args, got)
		}
	}
}

// TestConfigSetRefusesASyncRootHoldingTheState pins the refusal at the place the sync root is
// SET. Nothing else stops a user from pointing it at their home directory: with a single
// selected view the sync root IS the view, so the scanner walks state.db while bbolt holds it
// open and uploads this device's pairing to the server.
func TestConfigSetRefusesASyncRootHoldingTheState(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)
	stateDir := filepath.Dir(path)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "sync_root", stateDir}, &stdout, &stderr); code == 0 {
		t.Fatal("config set accepted a sync root containing the state directory")
	}
	out := stderr.String()
	if !strings.Contains(out, "inside the sync root") {
		t.Errorf("stderr = %q, want it to explain the containment", out)
	}
	if strings.Contains(out, "config: ") {
		t.Errorf("stderr leaks the internal package prefix: %q", out)
	}
}

// TestConfigSetAcceptsAnOrdinarySyncRoot is the negative side: the normal layout is unaffected.
func TestConfigSetAcceptsAnOrdinarySyncRoot(t *testing.T) {
	path := setupCLI(t, &fakePrompter{})
	seedConfig(t, path, nil)
	root := t.TempDir()

	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "set", "sync_root", root}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
}
