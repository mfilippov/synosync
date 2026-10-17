//go:build windows

package engine

import (
	"os"

	"golang.org/x/sys/windows"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// hostAliasesShortNames records that Windows can address one directory entry under a second,
// auto-generated 8.3 name (see shortname.go). Short-name generation is a per-volume setting -
// on by default for the system volume, frequently off for data volumes - so this const only says
// the host CAN alias; whether a given path does is what osLongPathName answers.
const hostAliasesShortNames = true

// osLongPathName returns the spelling Windows actually stores for name, resolving every 8.3
// short-name component to its long form (GetLongPathNameW). The path must exist: a component
// that does not resolves to ERROR_FILE_NOT_FOUND / ERROR_PATH_NOT_FOUND, which the caller reads
// as "nothing to compare here" and walks up from.
//
// The call does NOT canonicalise case - it echoes the case it was handed for a component whose
// long name it did not have to look up - which is why aliasedComponent compares case-insensitively
// rather than treating the result as a canonical key.
//
// The path goes through fsutil.ExtendedPath: this is a direct Win32 call and gets none of the
// stdlib's MAX_PATH fixup, so on a tree deeper than 260 characters the
// resolve would fail with a non-IsNotExist error, rejectShortNameAlias would read that as
// inconclusive, and the whole 8.3 gate would be silently dead on exactly the deep trees the os
// package materialises. Only the resolved spelling's TRAILING components are ever compared
// (aliasedComponent), so the prefix the extended form carries never reaches a comparison.
func osLongPathName(name string) (string, error) {
	p, err := windows.UTF16PtrFromString(fsutil.ExtendedPath(name))
	if err != nil {
		return "", &os.PathError{Op: "getlongpathname", Path: name, Err: err}
	}
	buf := make([]uint16, windows.MAX_PATH)
	for {
		// buf is either windows.MAX_PATH long or exactly the uint32 length the previous call
		// reported, so len(buf) always fits a uint32 by construction.
		bufLen := uint32(len(buf)) //nolint:gosec // len(buf) is MAX_PATH or a prior uint32 n
		n, cerr := windows.GetLongPathName(p, &buf[0], bufLen)
		if cerr != nil {
			return "", &os.PathError{Op: "getlongpathname", Path: name, Err: cerr}
		}
		// A return larger than the buffer is the required length (including the null
		// terminator): grow and retry. Otherwise n is the resolved length, sans terminator.
		if int(n) > len(buf) {
			buf = make([]uint16, n)
			continue
		}
		return windows.UTF16ToString(buf[:n]), nil
	}
}
