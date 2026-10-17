//go:build windows

package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// hostShortName asks Windows for the 8.3 short name it generated for path and reports whether
// that alias is a DIFFERENT spelling from the long name - i.e. whether short-name generation is
// switched on for the volume holding path. It is the probe the empirical fixtures gate on, in
// place of a runtime.GOOS branch: the feature is per-volume, so a data disk with 8dot3name
// disabled must skip, not fail.
func hostShortName(t *testing.T, path string) (string, bool) {
	t.Helper()
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatalf("UTF16PtrFromString(%q): %v", path, err)
	}
	buf := make([]uint16, windows.MAX_PATH)
	n, err := windows.GetShortPathName(p, &buf[0], uint32(len(buf)))
	if err != nil {
		return "", false
	}
	if int(n) > len(buf) {
		buf = make([]uint16, n)
		if _, err = windows.GetShortPathName(p, &buf[0], uint32(len(buf))); err != nil {
			return "", false
		}
	}
	short := windows.UTF16ToString(buf[:])
	if short == "" || filepath.Base(short) == filepath.Base(path) {
		return "", false
	}
	return filepath.Base(short), true
}

// TestLongPathNameBeyondMaxPath pins the long-path arm of the 8.3 gate:
// GetLongPathName is a direct Win32 call and gets none of the stdlib's MAX_PATH fixup,
// so on a tree deeper than 260 characters - one os.MkdirAll materialises without
// complaint - the resolve used to fail with a non-IsNotExist error, rejectShortNameAlias
// read that as inconclusive, and the alias gate was silently dead on exactly those trees.
//
// The extended-length form is observable in the answer (GetLongPathName echoes the prefix
// it was handed), and the de-aliasing still has to work through it: the resolved spelling
// names the long file, and aliasedComponent - which compares only trailing components -
// reports the alias regardless of the prefix.
func TestLongPathNameBeyondMaxPath(t *testing.T) {
	root := t.TempDir()
	deep := root
	for len(deep) < 300 {
		deep = filepath.Join(deep, strings.Repeat("d", 30))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Skipf("host cannot materialise a >MAX_PATH tree: %v", err)
	}
	long := filepath.Join(deep, "Document Name Long.txt")
	if err := os.WriteFile(long, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed long-named file: %v", err)
	}

	resolved, err := osLongPathName(deep)
	if err != nil {
		t.Fatalf("osLongPathName on a >MAX_PATH directory: %v", err)
	}
	if !strings.HasPrefix(resolved, `\\?\`) {
		t.Errorf("resolved = %q, want the extended-length form the direct call needs", resolved)
	}

	short, ok := hostShortName(t, long)
	if !ok {
		t.Skip("volume does not generate 8.3 short names; the alias arm is untestable here")
	}
	aliasPath := filepath.Join(deep, short)
	real, err := osLongPathName(aliasPath)
	if err != nil {
		t.Fatalf("osLongPathName(%q): %v", aliasPath, err)
	}
	spelled, actual, aliased := aliasedComponent(aliasPath, real, 1)
	if !aliased {
		t.Fatalf("aliasedComponent(%q, %q) reported no alias; the gate is dead beyond MAX_PATH", aliasPath, real)
	}
	if spelled != short || actual != filepath.Base(long) {
		t.Errorf("alias = %q -> %q, want %q -> %q", spelled, actual, short, filepath.Base(long))
	}
}
