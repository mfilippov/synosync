package ipc

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// pipeSecretLen is the size of the random per-user secret seeding the Windows
// pipe name. 32 bytes makes the derived name infeasible to guess.
const pipeSecretLen = 32

// pipeName derives the Windows named-pipe name from the daemon socket path and a
// per-user secret. The secret (stored only in the user-protected state dir) makes
// the name unpredictable to other local users: the global \\.\pipe\ namespace is
// shared by every user, so a name derived from the public path alone could be
// pre-created ("squatted") by an attacker before the daemon starts, after which
// the CLI would dial the attacker's pipe. Mixing in the secret closes that gap -
// an attacker who cannot read the state dir cannot compute the name to pre-create
// it, and once the daemon owns the name winio's FILE_CREATE first instance
// prevents any second instance.
//
// It lives in a non-OS-specific file (rather than listen_windows.go) so the
// security-relevant derivation is unit-testable on every host; only Listen/Dial
// (which need winio) are Windows-tagged.
func pipeName(sockPath string, secret []byte) string {
	h := sha256.New()
	h.Write([]byte(foldSockPath(sockPath)))
	h.Write(secret)
	sum := h.Sum(nil)
	return `\\.\pipe\synosync_` + hex.EncodeToString(sum[:16])
}

// foldSockPath canonicalizes the socket path feeding the pipe-name hash. Windows
// paths are case-insensitive, so two spellings of the SAME file must hash alike:
// the daemon and the CLI can legitimately resolve different cases of the same
// directory (a %LocalAppData% value the user or an installer typed in another case,
// or the homeDir()+`AppDataocal` fallback in internal/config/paths.go), and a
// byte-exact hash then derived two different names - the CLI reported "daemon is
// not running" while the daemon was alive and holding the bbolt lock.
//
// Upper-casing matches how Windows itself compares path names, and the fold is
// unconditional (not GOOS-gated) because the pipe name is a Windows-only concept
// - Listen/Dial are the only callers, so folding on every host keeps the
// derivation testable everywhere. Only the CASE fold is host-independent, though:
// filepath.Clean is not, and a unix host does not treat `\` as a separator, so the
// separator-normalising half of this canonicalisation can only be exercised on
// Windows (the test probes for it rather than asserting it everywhere). Distinct paths that differ only in case denote
// the same file on Windows, so collapsing them is not a collision.
func foldSockPath(sockPath string) string {
	return strings.ToUpper(filepath.Clean(sockPath))
}

// pipeSecretFile is the path to the per-user secret seeding the pipe name: a
// sibling of the daemon socket under the same user-protected state dir, so it
// inherits that directory's owner-only access control.
func pipeSecretFile(sockPath string) string {
	return filepath.Join(filepath.Dir(sockPath), "daemon.pipe-secret")
}

// loadOrCreatePipeSecret returns the per-user pipe secret, creating it with
// crypto/rand on first use (the daemon path). Access control is the platform's:
// mode 0600 on Unix, an explicit owner-only DACL on Windows (writeSecretFile /
// hardenSecretFile) - Windows ignores the POSIX bits, and the state dir sits in
// the roaming profile, which folder redirection can replicate to a file server.
// A present-but-too-short file (a truncated/corrupt secret) is regenerated.
//
// An existing secret is re-hardened best-effort: it may predate this hardening
// (or live on a filesystem that cannot carry an ACL), and refusing to start an
// otherwise healthy daemon over that would be worse than the inherited-ACL
// protection the profile directory already gives. A freshly created secret, by
// contrast, fails closed - writeSecretFile removes the file rather than leave an
// unprotected one behind.
func loadOrCreatePipeSecret(sockPath string) ([]byte, error) {
	p := pipeSecretFile(sockPath)
	if b, err := os.ReadFile(p); err == nil {
		if len(b) >= pipeSecretLen {
			_ = hardenSecretFile(p)
			return b, nil
		}
		// fall through to regenerate a truncated/corrupt secret
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("ipc: read pipe secret: %w", err)
	}
	secret := make([]byte, pipeSecretLen)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("ipc: generate pipe secret: %w", err)
	}
	if err := writeSecretFile(p, secret); err != nil {
		return nil, fmt.Errorf("ipc: write pipe secret: %w", err)
	}
	return secret, nil
}

// loadPipeSecret reads the per-user pipe secret without creating it (the CLI
// path). A missing file (no daemon has ever listened) returns an fs.ErrNotExist
// the dialer reports as "daemon not running" via IsDaemonAbsent.
func loadPipeSecret(sockPath string) ([]byte, error) {
	b, err := os.ReadFile(pipeSecretFile(sockPath))
	if err != nil {
		return nil, err
	}
	return b, nil
}
