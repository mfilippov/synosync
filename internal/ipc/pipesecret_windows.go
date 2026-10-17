//go:build windows

package ipc

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// writeSecretFile persists the pipe secret under an explicit owner-only DACL.
// Windows ignores the POSIX mode bits Unix relies on, and the state dir lives in
// the roaming profile - folder redirection can replicate it to a file server,
// where the profile's inherited ACL is not the local user's. Since the secret is
// what makes the pipe name unguessable (see pipeName), it is locked down by the
// kernel exactly like the pipe itself.
//
// The DACL is applied to the still-empty file BEFORE the secret bytes are
// written, so no window exists in which the plaintext secret carries only the
// inherited ACL. Any failure removes the file rather than leaving an
// unprotected secret behind: the caller regenerates on the next attempt.
func writeSecretFile(path string, secret []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := hardenSecretFile(path); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if _, err := f.Write(secret); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// hardenSecretFile replaces the secret file's DACL with the same protected,
// single-ACE descriptor Listen puts on the pipe (D:P(A;;GA;;;<SID>)): access for
// the owning user and nobody else, with inheritance from the parent directory
// disabled. It is idempotent, so it also migrates a secret written by an older
// build that relied on the inherited ACL alone.
func hardenSecretFile(path string) error {
	sddl, err := currentUserSDDL()
	if err != nil {
		return err
	}
	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return fmt.Errorf("ipc: parse secret-file security descriptor: %w", err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Errorf("ipc: secret-file DACL: %w", err)
	}
	if dacl == nil {
		return errors.New("ipc: secret-file security descriptor carries no DACL")
	}
	// PROTECTED_DACL_SECURITY_INFORMATION is what makes D:P stick: without it the
	// parent's inheritable ACEs would be merged back into the effective ACL.
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil); err != nil {
		return fmt.Errorf("ipc: restrict secret file %q: %w", path, err)
	}
	return nil
}
