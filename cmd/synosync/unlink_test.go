package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/store"
)

// corruptConfigBlob overwrites the meta/config key in the closed state database
// at statePath with raw blob bytes the strict GetConfig cannot decode, simulating
// on-disk config corruption (bbolt checksums only its meta pages). The store must
// be closed before calling so bbolt's exclusive lock is free.
func corruptConfigBlob(t *testing.T, statePath string, blob []byte) {
	t.Helper()
	db, err := bolt.Open(statePath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("config"), blob)
	}); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
}

// unlinkPairedConfig returns a valid, paired config for the unlink tests.
func unlinkPairedConfig() config.Config {
	cfg := config.Default()
	cfg.SyncRoot = "/tmp/sync"
	cfg.Server.Host = "nas.example"
	cfg.Server.Port = 6690
	cfg.Server.User = "alice"
	cfg.Server.DsID = "ds123"
	cfg.DeviceUUID = "dev-uuid"
	return cfg
}

// seedPaired writes a paired config and the keychain credentials into the test
// store/keyring, returning the server addr the secrets are keyed under.
func seedPaired(t *testing.T, statePath string, cfg config.Config) string {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.PutView(1, store.View{Name: "home", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	server := serverAddr(cfg)
	kr := secrets.New()
	if err := kr.SetSession(server, cfg.Server.User, "tok"); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	if err := kr.SetPin(server, "pin"); err != nil {
		t.Fatalf("SetPin: %v", err)
	}
	if err := kr.SetLegacyPassword(server, cfg.Server.User, "pw"); err != nil {
		t.Fatalf("SetLegacyPassword: %v", err)
	}
	return server
}

// stubServerUnlink swaps the serverUnlink seam for the test, restoring it on
// cleanup, and returns a pointer to a flag set when it is called.
func stubServerUnlink(t *testing.T, err error) *bool {
	t.Helper()
	called := false
	orig := serverUnlink
	t.Cleanup(func() { serverUnlink = orig })
	serverUnlink = func(context.Context, config.Config, secretStore, auth.ClientInfo) error {
		called = true
		return err
	}
	return &called
}

func TestUnlinkHappyPath(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	server := seedPaired(t, statePath, cfg)
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
	}
	if !*called {
		t.Errorf("serverUnlink was not called")
	}

	// Config wiped: GetConfig now reports ErrNotFound.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfig(); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetConfig after unlink: err = %v, want ErrNotFound", err)
	}
	views, err := st.ListViews()
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("views after unlink: %d, want 0", len(views))
	}

	// Keychain secrets erased.
	kr := secrets.New()
	if _, err := kr.Session(server, cfg.Server.User); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("Session after unlink: err = %v, want ErrNotFound", err)
	}
	if _, err := kr.Pin(server); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("Pin after unlink: err = %v, want ErrNotFound", err)
	}
	if _, err := kr.Password(server, cfg.Server.User); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("Password after unlink: err = %v, want ErrNotFound", err)
	}
	if !strings.Contains(stdout.String(), "unlinked from") {
		t.Errorf("stdout = %q, want unlinked confirmation", stdout.String())
	}
}

// TestUnlinkRemovesPipeSecretAndKeepsLogs confirms unlink best-effort removes the
// per-user pipe-secret file and reports where the intentionally-kept log files
// remain. setupCLI points resolveDaemonPaths at the temp state/log dirs.
func TestUnlinkRemovesPipeSecretAndKeepsLogs(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	stubServerUnlink(t, nil)

	// Seed a pipe-secret file next to state.db (what the Windows IPC listener would
	// have created); unlink must delete it.
	secretPath := filepath.Join(filepath.Dir(statePath), "daemon.pipe-secret")
	if err := os.WriteFile(secretPath, []byte("secret"), 0o600); err != nil {
		t.Fatalf("seed pipe secret: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
	}
	if _, err := os.Stat(secretPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("pipe secret after unlink: err = %v, want removed (ErrNotExist)", err)
	}
	if !strings.Contains(stdout.String(), "log files kept at") {
		t.Errorf("stdout = %q, want a log-files-kept note", stdout.String())
	}
}

// TestUnlinkMissingPipeSecretNoError confirms a missing pipe-secret file (the
// normal Unix case) is a harmless no-op that never fails the unlink.
func TestUnlinkMissingPipeSecretNoError(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "could not remove pipe secret") {
		t.Errorf("stderr = %q, want no pipe-secret warning when absent", stderr.String())
	}
}

func TestUnlinkServerUnreachable(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	stubServerUnlink(t, errors.New("dial: connection refused"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0 (local cleanup must still run)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "could not notify the server") {
		t.Errorf("stderr = %q, want server warning", stderr.String())
	}
	// Local cleanup still happened.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfig(); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetConfig after unlink: err = %v, want ErrNotFound", err)
	}
}

func TestUnlinkCancelWithoutYes(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{otps: []string{"n"}})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
	}
	if *called {
		t.Errorf("serverUnlink called despite cancellation")
	}
	if !strings.Contains(stdout.String(), "cancelled") {
		t.Errorf("stdout = %q, want cancellation", stdout.String())
	}
	// State preserved.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfig(); err != nil {
		t.Errorf("GetConfig after cancel: err = %v, want config preserved", err)
	}
}

func TestUnlinkConfirmAccepted(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{otps: []string{"yes"}})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
	}
	if !*called {
		t.Errorf("serverUnlink not called after affirmative confirmation")
	}
}

func TestUnlinkLockedStore(t *testing.T) {
	setupCLI(t, &fakePrompter{})
	orig := openConfigStore
	t.Cleanup(func() { openConfigStore = orig })
	openConfigStore = func() (*store.Store, error) { return nil, store.ErrLocked }

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "in use by another synosync process") {
		t.Errorf("stderr = %q, want locked-store hint", stderr.String())
	}
}

// errPrompter is a prompter whose ReadLine fails, simulating a closed/broken
// stdin during the unlink confirmation.
type errPrompter struct{}

func (errPrompter) ReadPassword(string) (string, error) { return "", nil }
func (errPrompter) ReadLine(string) (string, error)     { return "", errors.New("stdin closed") }
func (errPrompter) Confirm(string) (bool, error)        { return false, errors.New("stdin closed") }

func TestUnlinkConfirmReadError(t *testing.T) {
	statePath := setupCLI(t, errPrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink"}, &stdout, &stderr) // no --yes → confirmation prompt
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if *called {
		t.Errorf("serverUnlink called despite a confirmation read error")
	}
	if !strings.Contains(stderr.String(), "read confirmation") {
		t.Errorf("stderr = %q, want read-confirmation error", stderr.String())
	}
	// State must be preserved: nothing was confirmed.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfig(); err != nil {
		t.Errorf("config wiped despite read error: %v", err)
	}
}

// failingDeleteKeyring is a secretStore whose Delete* methods all fail with a
// real (non-ErrNotFound) backend error, exercising clearSecret's
// preserve-state-for-retry path.
type failingDeleteKeyring struct {
	secretStore
	err error
}

func (f failingDeleteKeyring) DeleteSession(string, string) error  { return f.err }
func (f failingDeleteKeyring) DeletePin(string) error              { return f.err }
func (f failingDeleteKeyring) DeletePassword(string, string) error { return f.err }

func TestUnlinkKeychainDeleteError(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	stubServerUnlink(t, nil)

	origKr := openKeyring
	t.Cleanup(func() { openKeyring = origKr })
	openKeyring = func() secretStore {
		return failingDeleteKeyring{secretStore: secrets.New(), err: errors.New("keychain busy")}
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d (%s), want 1 (a keychain backend failure must abort before wiping state)", code, stderr.String())
	}
	for _, label := range []string{"session", "password", "certificate pin"} {
		if !strings.Contains(stderr.String(), "could not erase "+label) {
			t.Errorf("stderr = %q, want warning for %s", stderr.String(), label)
		}
	}
	if !strings.Contains(stderr.String(), "left intact") {
		t.Errorf("stderr = %q, want retry hint", stderr.String())
	}
	// Local state must be PRESERVED so the user can retry once the keychain is
	// reachable; wiping the config would orphan the surviving credentials.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfig(); err != nil {
		t.Errorf("GetConfig after failed unlink: err = %v, want config preserved", err)
	}
}

// TestUnlinkCorruptConfigRecoversCoordinates pins that a corrupt settings blob
// no longer dead-ends `unlink` - the very recovery path (unlink → re-pair). The
// strict GetConfig rejects the blob, which blocks pair/login/config/daemon all
// the same, so unlink must instead best-effort recover the keychain-cleanup
// coordinates (server host/port/user) from the raw blob, skip the server detach,
// clear the keychain and reset the store. Before the fix unlink aborted on the
// load error (exit 1) and the store was never reset - the deadlock. The blob
// here carries an unknown field (rejected only by the strict DisallowUnknownFields
// decode) yet valid server coordinates, so a lenient parse recovers them exactly.
func TestUnlinkCorruptConfigRecoversCoordinates(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	server := seedPaired(t, statePath, cfg)
	corruptConfigBlob(t, statePath, []byte(`{"server":{"host":"nas.example","port":6690,"user":"alice"},"bogus_future_field":true}`))
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0 (corrupt config must not dead-end unlink)", code, stderr.String())
	}
	if *called {
		t.Errorf("serverUnlink called on a corrupt config; the detach must be skipped")
	}
	if !strings.Contains(stderr.String(), "corrupt") {
		t.Errorf("stderr = %q, want a corrupt-settings warning", stderr.String())
	}

	// Local state reset, so a fresh `pair` can follow.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfigBlob(); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("config blob after unlink: err = %v, want ErrNotFound", err)
	}
	views, err := st.ListViews()
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("views after unlink: %d, want 0", len(views))
	}

	// Keychain secrets keyed by the recovered coordinates are erased.
	kr := secrets.New()
	if _, err := kr.Session(server, "alice"); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("Session after unlink: err = %v, want ErrNotFound", err)
	}
	if _, err := kr.Pin(server); !errors.Is(err, secrets.ErrNotFound) {
		t.Errorf("Pin after unlink: err = %v, want ErrNotFound", err)
	}
}

// TestUnlinkCorruptConfigUnparseableStillResets pins that even a wholly garbled
// blob (no recoverable coordinates) still resets local state rather than
// dead-ending: the keychain cleanup runs on the default (empty) coordinates and
// the store is reset so the user gets a clean slate.
func TestUnlinkCorruptConfigUnparseableStillResets(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	corruptConfigBlob(t, statePath, []byte(`{not even json`))
	called := stubServerUnlink(t, nil)

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d (%s), want 0 (garbled config must still reset)", code, stderr.String())
	}
	if *called {
		t.Errorf("serverUnlink called on a corrupt config; the detach must be skipped")
	}
	if !strings.Contains(stderr.String(), "could not recover server coordinates") {
		t.Errorf("stderr = %q, want a coordinate-recovery warning", stderr.String())
	}

	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if _, err := st.GetConfigBlob(); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("config blob after unlink: err = %v, want ErrNotFound", err)
	}
}

// TestCorruptConfigStrictCommandsPointToUnlink pins that the strict subcommands
// which cannot recover from a corrupt settings blob (they need the whole config)
// point the user at `synosync unlink` as the recovery path instead of surfacing a
// bare, dead-end decode error.
func TestCorruptConfigStrictCommandsPointToUnlink(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)
	corruptConfigBlob(t, statePath, []byte(`{"server":{"host":"nas.example"},"bogus_future_field":true}`))

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"pair", []string{"pair"}},
		{"login", []string{"login"}},
		{"config-get", []string{"config", "get"}},
		{"config-set", []string{"config", "set", "log_level", "debug"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != 1 {
				t.Fatalf("exit = %d (%s), want 1", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "synosync unlink") {
				t.Errorf("stderr = %q, want a `synosync unlink` recovery hint", stderr.String())
			}
		})
	}
}

// TestUnlinkNotPaired pins that unlinking an install that is already unpaired SUCCEEDS.
//
// The end state the caller asked for is already in place, and `unlink` is what a teardown
// script runs to reach it. Reporting failure for having arrived there meant every such script
// had to special-case the code, or stop at the first re-run.
func TestUnlinkNotPaired(t *testing.T) {
	setupCLI(t, &fakePrompter{})
	// Fresh store, no config written → not paired.
	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Not paired") {
		t.Errorf("stdout = %q, want the nothing-to-do message", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want nothing: this is not a failure", stderr.String())
	}
}

// damageSchemaMarker rewrites (or, for a nil marker, deletes) the
// meta/schema_version key in the closed state database at statePath, simulating
// the three marker-corruption forms store.Open rejects with ErrCorruptRecord.
// The store must be closed so bbolt's exclusive lock is free.
func damageSchemaMarker(t *testing.T, statePath string, marker []byte) {
	t.Helper()
	db, err := bolt.Open(statePath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	defer func() { _ = db.Close() }()
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		if marker == nil {
			return b.Delete([]byte("schema_version"))
		}
		return b.Put([]byte("schema_version"), marker)
	})
	if err != nil {
		t.Fatalf("damage schema marker: %v", err)
	}
}

// TestUnlinkCorruptStoreRecovery drives the no-Open destructive recovery path
// over each of the three marker-corruption forms: unlink
// must detect the ErrCorruptRecord from store.Open, delete state.db (plus the
// best-effort pipe secret), warn that the keychain cleanup is skipped, and leave
// a clean slate so a re-pair starts fresh. `--yes` is honoured, so no prompt
// fires (noPromptPrompter fails the test on one). Before the fix unlink printed
// the Open error - whose own hint says "run `synosync unlink`" - and exited 1
// with state.db intact, a circular dead-end.
func TestUnlinkCorruptStoreRecovery(t *testing.T) {
	forms := []struct {
		name   string
		marker []byte // nil = delete the key
	}{
		{"missing", nil},
		{"truncated", []byte{0x01, 0x02, 0x03}},
		{"zeroed", make([]byte, 8)},
	}
	for _, f := range forms {
		t.Run(f.name, func(t *testing.T) {
			statePath := setupCLI(t, &noPromptPrompter{t})
			cfg := unlinkPairedConfig()
			_ = seedPaired(t, statePath, cfg)
			damageSchemaMarker(t, statePath, f.marker)
			// Seed a pipe secret so the best-effort cleanup is observable.
			secretPath := filepath.Join(filepath.Dir(statePath), "daemon.pipe-secret")
			if err := os.WriteFile(secretPath, []byte("secret"), 0o600); err != nil {
				t.Fatalf("seed pipe secret: %v", err)
			}

			var stdout, stderr bytes.Buffer
			code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
			}
			if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("state.db after recovery: err = %v, want removed (ErrNotExist)", err)
			}
			if _, err := os.Stat(secretPath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("pipe secret after recovery: err = %v, want removed (ErrNotExist)", err)
			}
			if !strings.Contains(stderr.String(), "keychain entries cannot be cleared") {
				t.Errorf("stderr = %q, want a skipped-keychain-cleanup warning", stderr.String())
			}
			if !strings.Contains(stdout.String(), "synosync pair") {
				t.Errorf("stdout = %q, want a re-pair pointer", stdout.String())
			}

			// A re-pair starts from a clean database: a fresh Open succeeds and
			// holds no settings blob.
			st, err := store.Open(statePath)
			if err != nil {
				t.Fatalf("re-open after recovery: %v", err)
			}
			defer func() { _ = st.Close() }()
			if _, err := st.GetConfig(); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetConfig after recovery: err = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestUnlinkCorruptStoreRecoveryConfirm covers the interactive confirmation of
// the destructive recovery: an affirmative answer proceeds to delete state.db,
// a decline cancels cleanly (exit 0, database kept).
func TestUnlinkCorruptStoreRecoveryConfirm(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		statePath := setupCLI(t, &fakePrompter{otps: []string{"y"}})
		cfg := unlinkPairedConfig()
		_ = seedPaired(t, statePath, cfg)
		damageSchemaMarker(t, statePath, nil)

		var stdout, stderr bytes.Buffer
		code := run([]string{"unlink"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("exit = %d (%s), want 0", code, stderr.String())
		}
		if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("state.db after accepted recovery: err = %v, want removed", err)
		}
	})
	t.Run("declined", func(t *testing.T) {
		statePath := setupCLI(t, &fakePrompter{otps: []string{"n"}})
		cfg := unlinkPairedConfig()
		_ = seedPaired(t, statePath, cfg)
		damageSchemaMarker(t, statePath, nil)

		var stdout, stderr bytes.Buffer
		code := run([]string{"unlink"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("exit = %d (%s), want 0 (clean cancel)", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "unlink cancelled") {
			t.Errorf("stdout = %q, want cancel message", stdout.String())
		}
		if _, err := os.Stat(statePath); err != nil {
			t.Errorf("state.db after declined recovery: err = %v, want kept", err)
		}
	})
}

// TestUnlinkTransientOpenErrorNoRecovery pins that the destructive recovery is
// gated on ErrCorruptRecord: any other store.Open failure (a transient I/O
// error) keeps the previous non-destructive behaviour - report and exit 1,
// state.db untouched.
func TestUnlinkTransientOpenErrorNoRecovery(t *testing.T) {
	statePath := setupCLI(t, &noPromptPrompter{t})
	cfg := unlinkPairedConfig()
	_ = seedPaired(t, statePath, cfg)

	orig := openConfigStore
	t.Cleanup(func() { openConfigStore = orig })
	openConfigStore = func() (*store.Store, error) { return nil, errors.New("transient I/O failure") }

	var stdout, stderr bytes.Buffer
	code := run([]string{"unlink", "--yes"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d (%s), want 1", code, stderr.String())
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Errorf("state.db after transient open failure: err = %v, want kept", err)
	}
	if !strings.Contains(stderr.String(), "open state db") {
		t.Errorf("stderr = %q, want the plain open-failure report", stderr.String())
	}
}
