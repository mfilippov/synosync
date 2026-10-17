package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// pairedConfig writes a config that looks already-paired (device_uuid + ds_id)
// and seeds the keychain with the given pin and session.
func pairedConfig(t *testing.T, path string, srv *mockServer, user, pin, session string) {
	t.Helper()
	writeServerConfig(t, path, srv, user, func(c *config.Config) {
		c.DeviceUUID = "0123456789abcdef0123456789abcdef"
		c.Server.DsID = testDsID
	})
	kr := secrets.New()
	if pin != "" {
		if err := kr.SetPin(srv.addr(), pin); err != nil {
			t.Fatalf("SetPin: %v", err)
		}
	}
	if session != "" {
		if err := kr.SetSession(srv.addr(), user, session); err != nil {
			t.Fatalf("SetSession: %v", err)
		}
	}
}

func TestLoginReuseValid(t *testing.T) {
	srv := newMockServer(t, func(cmd uint8, req *sproto.Map) sproto.Value {
		if cmd != sproto.CmdAction {
			t.Errorf("cmd = 0x%02x, want CmdAction", cmd)
		}
		if v, _ := req.Get("action"); v != sproto.String("query_user_info") {
			t.Errorf("action = %v, want query_user_info", v)
		}
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{})
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "goodsession")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "still valid") {
		t.Errorf("stdout = %q, want still-valid message", stdout.String())
	}
}

func TestLoginSessionVanished(t *testing.T) {
	const newSession = "99998888777766665555444433332222"
	srv := newMockServer(t, func(cmd uint8, _ *sproto.Map) sproto.Value {
		switch cmd {
		case sproto.CmdAction:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeSessionVanished))
		case sproto.CmdAuth:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(newSession))
		default:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
		}
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "stalesession")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "re-authenticated") {
		t.Errorf("stdout = %q, want re-auth message", stdout.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != newSession {
		t.Errorf("stored session = %q, want refreshed %q", got, newSession)
	}
}

func TestLoginVanishedThenOTPRetry(t *testing.T) {
	const newSession = "aaaabbbbccccddddeeeeffff00001111"
	authCalls := 0
	srv := newMockServer(t, func(cmd uint8, _ *sproto.Map) sproto.Value {
		switch cmd {
		case sproto.CmdAction:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeSessionVanished))
		case sproto.CmdAuth:
			authCalls++
			if authCalls == 1 {
				return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOTPRequired))
			}
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(newSession))
		default:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
		}
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret", otps: []string{"", "246810"}})
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "stalesession")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got, _ := secrets.New().Session(srv.addr(), "alice"); got != newSession {
		t.Errorf("stored session = %q, want %q", got, newSession)
	}
}

func TestLoginPinMismatch(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{})
	// Store a pin that does not match the server's leaf.
	pairedConfig(t, path, srv, "alice", "sha256:"+strings.Repeat("00", 32), "tok")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	out := stderr.String()
	if !strings.Contains(out, "certificate changed") {
		t.Errorf("stderr = %q, want cert-changed message", out)
	}
	// The friendly message names the got/want fingerprints and the recovery step.
	// The stored pin is the legacy full-DER format, so the verifier reports the
	// server's presented leaf DER pin (got) against the stored pin (want).
	storedPin := "sha256:" + strings.Repeat("00", 32)
	if !strings.Contains(out, srv.leafPin()) {
		t.Errorf("stderr = %q, want presented fingerprint %q", out, srv.leafPin())
	}
	if !strings.Contains(out, storedPin) {
		t.Errorf("stderr = %q, want stored fingerprint %q", out, storedPin)
	}
	if !strings.Contains(out, "synosync pair") {
		t.Errorf("stderr = %q, want pair instruction", out)
	}
}

// spyKeyring wraps a real secretStore and records the fingerprints passed to
// SetPin so the migration tests can assert whether (and to what) the pin was
// re-saved.
type spyKeyring struct {
	secretStore
	setPins []string
}

func (s *spyKeyring) SetPin(server, fingerprint string) error {
	s.setPins = append(s.setPins, fingerprint)
	return s.secretStore.SetPin(server, fingerprint)
}

// installSpyKeyring routes openKeyring at a spy wrapping the mocked backend and
// returns it so the test can inspect SetPin calls after run().
func installSpyKeyring(t *testing.T) *spyKeyring {
	t.Helper()
	spy := &spyKeyring{secretStore: secrets.New()}
	orig := openKeyring
	openKeyring = func() secretStore { return spy }
	t.Cleanup(func() { openKeyring = orig })
	return spy
}

func TestLoginLegacyPinUpgraded(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{})
	// Seed a legacy full-DER pin; a valid session keeps login on the reuse path.
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "goodsession")
	spy := installSpyKeyring(t)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "pin upgraded to SPKI") {
		t.Errorf("stdout = %q, want pin-upgrade message", stdout.String())
	}
	if len(spy.setPins) != 1 || spy.setPins[0] != srv.leafSPKIPin() {
		t.Errorf("SetPin calls = %v, want one SPKI pin %q", spy.setPins, srv.leafSPKIPin())
	}
	if got, _ := secrets.New().Pin(srv.addr()); got != srv.leafSPKIPin() {
		t.Errorf("stored pin = %q, want migrated SPKI %q", got, srv.leafSPKIPin())
	}
}

// TestLoginModeCLegacyPinUpgraded confirms the mode-C re-pair path (taken after
// the server reports the session vanished) also migrates a stored legacy full-DER
// pin to the SPKI form over the same pinned connection, not just the reuse path.
func TestLoginModeCLegacyPinUpgraded(t *testing.T) {
	const newSession = "1234123412341234123412341234abcd"
	srv := newMockServer(t, func(cmd uint8, _ *sproto.Map) sproto.Value {
		switch cmd {
		case sproto.CmdAction:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeSessionVanished))
		case sproto.CmdAuth:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String(newSession))
		default:
			return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
		}
	})

	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	// Legacy full-DER pin + a stale session so login falls through to mode C.
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "stalesession")
	spy := installSpyKeyring(t)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "re-authenticated") {
		t.Errorf("stdout = %q, want re-auth message", stdout.String())
	}
	if !strings.Contains(stdout.String(), "pin upgraded to SPKI") {
		t.Errorf("stdout = %q, want pin-upgrade message on the mode-C path", stdout.String())
	}
	if len(spy.setPins) != 1 || spy.setPins[0] != srv.leafSPKIPin() {
		t.Errorf("SetPin calls = %v, want one SPKI pin %q", spy.setPins, srv.leafSPKIPin())
	}
	if got, _ := secrets.New().Pin(srv.addr()); got != srv.leafSPKIPin() {
		t.Errorf("stored pin = %q, want migrated SPKI %q", got, srv.leafSPKIPin())
	}
}

func TestLoginSPKIPinNoUpgrade(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK))
	})

	path := setupCLI(t, &fakePrompter{})
	// Already-SPKI pin → nothing to migrate.
	pairedConfig(t, path, srv, "alice", srv.leafSPKIPin(), "goodsession")
	spy := installSpyKeyring(t)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "upgraded") {
		t.Errorf("stdout = %q, want no upgrade message for an SPKI pin", stdout.String())
	}
	if len(spy.setPins) != 0 {
		t.Errorf("SetPin calls = %v, want none for an SPKI pin", spy.setPins)
	}
}

func TestLoginEmptyPinRejected(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("server contacted despite empty pin")
		return sproto.Null{}
	})

	path := setupCLI(t, &fakePrompter{})
	// Paired config, but the stored pin is present-but-empty: login must refuse
	// rather than fall through to transport capture mode (trust any leaf).
	pairedConfig(t, path, srv, "alice", "", "tok")
	if err := secrets.New().SetPin(srv.addr(), ""); err != nil {
		t.Fatalf("SetPin: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "pin is empty") {
		t.Errorf("stderr = %q, want empty-pin message", stderr.String())
	}
}

func TestLoginStoreLocked(t *testing.T) {
	setupCLI(t, &fakePrompter{})
	orig := openConfigStore
	openConfigStore = func() (*store.Store, error) { return nil, store.ErrLocked }
	t.Cleanup(func() { openConfigStore = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "in use by another synosync process") {
		t.Errorf("stderr = %q, want locked-database hint", stderr.String())
	}
}

func TestLoginInvalidPortRejected(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value {
		t.Error("server contacted despite invalid --port")
		return sproto.Null{}
	})
	path := setupCLI(t, &fakePrompter{})
	pairedConfig(t, path, srv, "alice", srv.leafPin(), "goodsession")

	var stdout, stderr bytes.Buffer
	// An out-of-range port must be rejected up front with a usage exit (2) and a
	// clear validation message, not a murky connect failure later.
	if code := run([]string{"login", "--port", "70000"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "out of range") {
		t.Errorf("stderr = %q, want port-out-of-range validation message", stderr.String())
	}
}

func TestLoginNotPaired(t *testing.T) {
	srv := newMockServer(t, func(uint8, *sproto.Map) sproto.Value { return sproto.Null{} })
	path := setupCLI(t, &fakePrompter{})
	// Config without device_uuid/ds_id → not paired.
	writeServerConfig(t, path, srv, "alice", nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not paired") {
		t.Errorf("stderr = %q, want not-paired message", stderr.String())
	}
}

// TestLoginFreshStoreNotPaired covers the never-configured install: with no
// config blob at all, loadConfigFromStore falls back to config.Default() (empty
// sync_root). The whole-config validation must NOT fire there - the user should
// see the friendly "not paired" hint (exit 1), not a "sync_root must be set"
// usage error (exit 2).
func TestLoginFreshStoreNotPaired(t *testing.T) {
	// setupCLI initialises an empty store but writes no config.
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"login"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not paired") {
		t.Errorf("stderr = %q, want not-paired message", stderr.String())
	}
	if strings.Contains(stderr.String(), "sync_root") {
		t.Errorf("stderr = %q, should not surface a sync_root validation error", stderr.String())
	}
}
