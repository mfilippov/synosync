//go:build !windows

package ipc

import "os"

// writeSecretFile persists the pipe secret with owner-only permissions. On Unix
// the mode bits are enforced by the kernel and the secret's parent (the per-user
// state dir) is already 0700, so nothing beyond the mode is needed.
//
// The pipe secret is a Windows-only concept (only listen_windows.go derives a
// name from it), so this file exists purely to keep pipename.go OS-neutral and
// unit-testable on every host.
func writeSecretFile(path string, secret []byte) error {
	return os.WriteFile(path, secret, 0o600)
}

// hardenSecretFile re-applies owner-only access control to an existing secret.
// On Unix the creating mode is authoritative and never widens on its own, so
// there is nothing to re-apply.
func hardenSecretFile(string) error { return nil }
