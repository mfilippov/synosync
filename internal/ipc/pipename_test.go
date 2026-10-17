package ipc

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipeNameFormatAndDeterminism(t *testing.T) {
	const path = `C:\Users\me\AppData\Roaming\synosync\daemon.sock`
	secret := bytes.Repeat([]byte{0xAB}, pipeSecretLen)

	got := pipeName(path, secret)
	if !strings.HasPrefix(got, `\\.\pipe\synosync_`) {
		t.Fatalf("pipeName = %q, want prefix %q", got, `\\.\pipe\synosync_`)
	}
	base := strings.TrimPrefix(got, `\\.\pipe\synosync_`)
	if len(base) != 32 {
		t.Fatalf("pipeName hash basename = %q (len %d), want 32 hex chars", base, len(base))
	}

	// Same (path, secret) must yield the same name so daemon and CLI agree.
	if again := pipeName(path, secret); again != got {
		t.Fatalf("pipeName not deterministic: %q vs %q", got, again)
	}
}

// TestPipeNameSecretUnpredictable is the core squatting defense: the name must
// depend on the secret, so a different secret (which another local user cannot
// read from the protected state dir) yields a different, unguessable name.
func TestPipeNameSecretUnpredictable(t *testing.T) {
	const path = `C:\Users\me\AppData\Roaming\synosync\daemon.sock`
	a := pipeName(path, bytes.Repeat([]byte{0x01}, pipeSecretLen))
	b := pipeName(path, bytes.Repeat([]byte{0x02}, pipeSecretLen))
	if a == b {
		t.Fatalf("pipeName ignored the secret: same name %q for distinct secrets", a)
	}
}

// TestPipeNameFoldsPathCase pins the case-insensitivity of the derivation: the
// pipe name is a Windows concept and Windows paths are case-insensitive, so two
// spellings of the SAME state dir (the daemon's %AppData% versus a CLI resolving
// the homeDir()+AppData\Roaming fallback, or either typed in another case) must
// dial one pipe. Byte-exact hashing made the CLI report "daemon is not running"
// against a live daemon holding the bbolt lock.
func TestPipeNameFoldsPathCase(t *testing.T) {
	secret := bytes.Repeat([]byte{0xAB}, pipeSecretLen)
	const canonical = `C:\Users\me\AppData\Roaming\synosync\daemon.sock`
	want := pipeName(canonical, secret)

	same := []struct {
		name string
		path string
	}{
		{"lowercased appdata", `C:\Users\me\appdata\roaming\synosync\daemon.sock`},
		{"uppercased whole path", `C:\USERS\ME\APPDATA\ROAMING\SYNOSYNC\DAEMON.SOCK`},
		{"mixed drive letter", `c:\Users\me\AppData\Roaming\synosync\daemon.sock`},
	}
	for _, tt := range same {
		t.Run(tt.name, func(t *testing.T) {
			if got := pipeName(tt.path, secret); got != want {
				t.Fatalf("pipeName(%q) = %q, want %q (same file, one pipe)", tt.path, got, want)
			}
		})
	}

	// filepath.Clean's normalizations must survive the fold too - but Clean is
	// HOST-dependent where the case fold is not: only a Windows filepath treats `\`
	// as a separator, so on a unix host this spelling is one long file name that
	// cannot fold onto the canonical path, and asserting it there fails on a
	// derivation that is correct. Probe the host's path semantics rather than branch
	// on GOOS, per the fixture convention: a run that cannot exercise this says so.
	t.Run("redundant separators", func(t *testing.T) {
		if filepath.Clean(`a\\b`) == `a\\b` {
			t.Skip("host filepath does not treat \\ as a separator; Clean's normalisation of a Windows path is Windows-only")
		}
		const redundant = `C:\Users\me\AppData\\Roaming\.\synosync\daemon.sock`
		if got := pipeName(redundant, secret); got != want {
			t.Fatalf("pipeName(%q) = %q, want %q (same file, one pipe)", redundant, got, want)
		}
	})

	// Folding must not collapse genuinely different paths.
	other := pipeName(`C:\Users\me\AppData\Roaming\synosync2\daemon.sock`, secret)
	if other == want {
		t.Fatalf("distinct state dirs share pipe name %q", other)
	}
}

func TestLoadOrCreatePipeSecret(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "daemon.sock")

	first, err := loadOrCreatePipeSecret(sock)
	if err != nil {
		t.Fatalf("loadOrCreatePipeSecret (create): %v", err)
	}
	if len(first) != pipeSecretLen {
		t.Fatalf("secret len = %d, want %d", len(first), pipeSecretLen)
	}

	// A second call reuses the persisted secret so the name is stable across
	// daemon restarts and matches what the CLI reads.
	second, err := loadOrCreatePipeSecret(sock)
	if err != nil {
		t.Fatalf("loadOrCreatePipeSecret (reuse): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("secret not stable across calls: %x vs %x", first, second)
	}

	// loadPipeSecret (the CLI path) reads the same bytes without creating.
	viaLoad, err := loadPipeSecret(sock)
	if err != nil {
		t.Fatalf("loadPipeSecret: %v", err)
	}
	if !bytes.Equal(first, viaLoad) {
		t.Fatalf("loadPipeSecret mismatch: %x vs %x", first, viaLoad)
	}
}

// TestLoadPipeSecretMissing confirms the CLI path reports a missing secret as
// fs.ErrNotExist (no daemon has ever listened) rather than silently inventing a
// name that would never match the daemon's.
func TestLoadPipeSecretMissing(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "daemon.sock")
	_, err := loadPipeSecret(sock)
	if !os.IsNotExist(err) {
		t.Fatalf("loadPipeSecret missing = %v, want fs.ErrNotExist", err)
	}
}

// TestLoadOrCreatePipeSecretRegeneratesShort confirms a truncated/corrupt secret
// is replaced rather than used (a too-short seed weakens unpredictability).
func TestLoadOrCreatePipeSecretRegeneratesShort(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "daemon.sock")
	if err := os.WriteFile(pipeSecretFile(sock), []byte("short"), 0o600); err != nil {
		t.Fatalf("seed short secret: %v", err)
	}
	secret, err := loadOrCreatePipeSecret(sock)
	if err != nil {
		t.Fatalf("loadOrCreatePipeSecret: %v", err)
	}
	if len(secret) != pipeSecretLen {
		t.Fatalf("short secret not regenerated: len %d", len(secret))
	}
}
