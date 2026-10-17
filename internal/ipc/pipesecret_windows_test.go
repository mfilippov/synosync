//go:build windows

package ipc

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// requireFileDACL probes whether the test's temp filesystem can carry an
// explicit DACL at all (a ReFS/FAT/redirected volume in CI may not), following
// the hostcaps convention: probe the behaviour, never branch on GOOS alone.
func requireFileDACL(t *testing.T) {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatalf("write probe file: %v", err)
	}
	if err := hardenSecretFile(probe); err != nil {
		t.Skipf("host filesystem cannot carry a file DACL: %v", err)
	}
}

// currentSID returns the SID the secret file's sole ACE must name.
func currentSID(t *testing.T) string {
	t.Helper()
	u, err := user.Current()
	if err != nil {
		t.Fatalf("current user: %v", err)
	}
	return u.Uid
}

// sddlSpelling returns the way Windows PRINTS sid inside a security-descriptor
// string. ConvertSecurityDescriptorToStringSecurityDescriptor - the call behind
// SECURITY_DESCRIPTOR.String() - abbreviates a well-known SID to its two-letter
// SDDL alias, so the built-in Administrator (RID 500) comes back as "LA" and
// never as the literal S-1-5-21-…-500 that user.Current reports. The
// windows-latest runner logs in as exactly that account, so the former literal
// substring match failed against a DACL that was in fact perfectly correct
// (D:PAI(A;;FA;;;LA)). Round-trip the SID through the same conversion the
// production code already uses (sddlForSID) and let Windows tell us how it spells
// this principal - probing the host's own rendering rather than hard-coding an
// alias table. Returns "" when the round-trip yields nothing usable; the caller
// still has the literal spelling to match against.
func sddlSpelling(t *testing.T, sid string) string {
	t.Helper()
	sd, err := windows.SecurityDescriptorFromString(sddlForSID(sid))
	if err != nil {
		t.Fatalf("round-trip SID %s through SDDL: %v", sid, err)
	}
	round := sd.String()
	open := strings.LastIndex(round, ";;;")
	closing := strings.LastIndex(round, ")")
	if open < 0 || closing <= open+3 {
		return ""
	}
	return round[open+3 : closing]
}

// namesSID reports whether an SDDL DACL's single ACE names the wanted principal
// under either spelling - the literal SID or the alias Windows prints for it.
// Both denote the same principal, so accepting either cannot pass a DACL that
// names somebody else. The match is anchored on the ";;;<sid>)" ACE tail rather
// than a bare substring, so one SID can never satisfy the test by being a prefix
// of another.
func namesSID(sddl, literal, spelling string) bool {
	if strings.Contains(sddl, ";;;"+literal+")") {
		return true
	}
	return spelling != "" && strings.Contains(sddl, ";;;"+spelling+")")
}

// assertOwnerOnlyDACL fails unless path carries a protected DACL whose single
// allow ACE names sid - i.e. inheritance from the parent is off and no other
// principal (SYSTEM, Administrators, a redirected share's ACL) has access.
func assertOwnerOnlyDACL(t *testing.T, path, sid string) {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo(%q): %v", path, err)
	}
	control, _, err := sd.Control()
	if err != nil {
		t.Fatalf("security descriptor control: %v", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("secret file DACL is not protected: %s", sd.String())
	}
	sddl := sd.String()
	if n := strings.Count(sddl, "("); n != 1 {
		t.Fatalf("secret file DACL has %d ACEs, want exactly 1: %s", n, sddl)
	}
	if !namesSID(sddl, sid, sddlSpelling(t, sid)) {
		t.Fatalf("secret file DACL %s does not name the current user %s (also tried the SDDL spelling %s)",
			sddl, sid, sddlSpelling(t, sid))
	}
	if !strings.Contains(sddl, "(A;") {
		t.Fatalf("secret file DACL %s has no allow ACE", sddl)
	}
}

// TestPipeSecretFileDACL is the confidentiality guarantee the pipe name rests
// on: an attacker who cannot read daemon.pipe-secret cannot compute the pipe
// name to squat it. Windows ignores the 0600 the Unix path relies on, so the
// created file must carry the explicit owner-only DACL.
func TestPipeSecretFileDACL(t *testing.T) {
	requireFileDACL(t)
	sock := filepath.Join(t.TempDir(), "daemon.sock")

	secret, err := loadOrCreatePipeSecret(sock)
	if err != nil {
		t.Fatalf("loadOrCreatePipeSecret: %v", err)
	}
	if len(secret) != pipeSecretLen {
		t.Fatalf("secret len = %d, want %d", len(secret), pipeSecretLen)
	}
	assertOwnerOnlyDACL(t, pipeSecretFile(sock), currentSID(t))
}

// TestPipeSecretFileDACLMigratesExisting covers the upgrade path: a secret
// written by an older build (inherited ACL only) is re-hardened in place the
// first time the daemon opens it, and its bytes - hence the pipe name - are
// unchanged, so a CLI built from the same source still finds the daemon.
func TestPipeSecretFileDACLMigratesExisting(t *testing.T) {
	requireFileDACL(t)
	sock := filepath.Join(t.TempDir(), "daemon.sock")
	p := pipeSecretFile(sock)

	legacy := make([]byte, pipeSecretLen)
	for i := range legacy {
		legacy[i] = byte(i)
	}
	if err := os.WriteFile(p, legacy, 0o600); err != nil {
		t.Fatalf("seed legacy secret: %v", err)
	}

	got, err := loadOrCreatePipeSecret(sock)
	if err != nil {
		t.Fatalf("loadOrCreatePipeSecret: %v", err)
	}
	if string(got) != string(legacy) {
		t.Fatalf("secret rewritten: %x, want %x (pipe name would change)", got, legacy)
	}
	assertOwnerOnlyDACL(t, p, currentSID(t))
}

// TestWriteSecretFileLeavesNothingOnFailure pins the fail-closed contract: if
// the DACL cannot be applied, no plaintext secret is left behind under whatever
// ACL it inherited. The failure is forced with a path that cannot be secured
// because it no longer exists by the time the DACL is set.
func TestWriteSecretFileLeavesNothingOnFailure(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no-such-dir", "daemon.pipe-secret")
	if err := writeSecretFile(missing, make([]byte, pipeSecretLen)); err == nil {
		t.Fatalf("writeSecretFile into a missing directory succeeded")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("stat after failed write = %v, want fs.ErrNotExist", err)
	}
}
