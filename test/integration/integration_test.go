//go:build integration

package integration

import (
	"os"
	"testing"
)

// TestMain runs the package's scenarios and then releases the two device
// registrations the run created. The harness pairs mode-C (a fresh device_uuid per
// run) because that is what exercises the real pairing path, but nothing else ever
// drops those devices: before this, every `make test-integration` left a PAIR of dead
// clients registered on the stand forever, and a long-lived test DSM accumulated
// hundreds. The unlink is best-effort and runs after m.Run, so it can neither fail a
// passing run nor rescue a failing one - see unlinkPairedDevices.
func TestMain(m *testing.M) {
	code := m.Run()
	unlinkPairedDevices()
	os.Exit(code)
}

// TestIntegration_Skips exercises the env gating: with no SYNOSYNC_IT_* set the
// harness must skip cleanly (never fail), and when the env is present requireEnv
// must return a fully-populated config. It confirms the build-tag + skip
// scaffolding compiles and behaves before any scenario depends on it.
func TestIntegration_Skips(t *testing.T) {
	cfg := requireEnv(t) // t.Skip when env is absent - reaching past here means configured.

	if cfg.Host == "" {
		t.Fatal("requireEnv returned an empty Host without skipping")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		t.Fatalf("requireEnv returned an invalid Port %d without skipping", cfg.Port)
	}
	if cfg.User == "" {
		t.Fatal("requireEnv returned an empty User without skipping")
	}
	if cfg.Pass == "" {
		t.Fatal("requireEnv returned an empty Pass without skipping")
	}
	if cfg.RemoteBase == "" {
		t.Fatal("requireEnv left RemoteBase empty (should default)")
	}

	// Temp fixtures must be usable and isolated.
	root := tempSyncRoot(t)
	if root == "" {
		t.Fatal("tempSyncRoot returned empty path")
	}
	paths := tempPaths(t)
	if paths.StateDir == "" || paths.StateDB() == "" {
		t.Fatal("tempPaths returned empty state dir")
	}
	if base := scratchRemoteBase(t, cfg); base == "" {
		t.Fatal("scratchRemoteBase returned empty path")
	}
}
