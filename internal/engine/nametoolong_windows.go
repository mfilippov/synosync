//go:build windows

package engine

import "syscall"

// Windows never reports the POSIX ENAMETOOLONG for an over-long path component:
// syscall.ENAMETOOLONG there is a synthetic value no Win32 call returns. An over-long
// component surfaces as ERROR_INVALID_NAME (123) - measured, not assumed: creating a
// 256-byte name under a short parent yields "The filename, directory name, or volume
// label syntax is incorrect" - while the wider path/buffer limits surface as
// ERROR_FILENAME_EXCED_RANGE (206) and ERROR_BUFFER_OVERFLOW (111). All three are
// permanent properties of the name, not of the moment: ERROR_INVALID_NAME also covers a
// server-supplied component carrying a character illegal on NTFS ('<', '|' and friends -
// measured: os.WriteFile of "a<b" yields exactly this errno), which likewise can never be
// applied here and belongs in the dead-letter trace rather than in an endless retry.
//
// A RESERVED DEVICE NAME is NOT in that set either, contrary to what this comment claimed
// before: measured on this host, os.WriteFile of "CON"/"PRN"/"AUX"/"COM1"/"LPT1" (bare or with
// any extension) SUCCEEDS and creates an ordinary listed file with the right bytes. No errno
// arrives, so no classifier could catch them. filepath.IsLocal closes only HALF the class -
// measured, Go 1.26: it reports false for a BARE reserved name ("CON", "NUL", "COM1"), so
// containable() rejects those at the reconcile layer and abs() dead-letters an already-queued
// op, exactly as for the trailing-dot class below; but it reports TRUE for the extended forms
// ("con.txt", "LPT1.log", "aux.dat", "conin$.txt"), which therefore pass every gate. Those are
// benign on this host - measured, they create ordinary files, "nul.txt" included - so the class
// is a measured residual (DESIGN §13) rather than an unguarded mis-apply: were a Windows build
// or a network redirector to alias one onto its device, the write would "succeed" into the void
// and markSynced would record a phantom, which is the addressed-≠-landed shape namemangle.go
// exists for. Extending componentHostRepresentable is the fix if one is ever observed.
//
// A trailing space or dot is NOT in that set, contrary to what this comment once claimed:
// Win32 does not reject such a component, it silently STRIPS it, so
// creating "foo." succeeds and yields "foo" with no error at all (empirically pinned on this
// host: os.Mkdir/os.WriteFile/os.Rename/os.Lstat all mangle-and-succeed). A dead-letter arm
// keyed on an errno that never arrives is dead code - that class is closed lexically instead,
// by the host-representability gate in namemangle.go, which refuses the path before any
// baseline is mutated.
var extraNameTooLongErrors = []error{
	syscall.Errno(123), // ERROR_INVALID_NAME
	syscall.Errno(206), // ERROR_FILENAME_EXCED_RANGE
	syscall.Errno(111), // ERROR_BUFFER_OVERFLOW
}
