//go:build windows

package engine

import (
	"os"
	"os/exec"
	"testing"
)

// makeJunction creates an NTFS junction (directory mount point) at link pointing at target,
// skipping the test when the host will not make one.
//
// `mklink /J` is deliberately the mechanism rather than a hand-rolled FSCTL_SET_REPARSE_POINT:
// it is exactly what an ORDINARY, unprivileged user runs, and that is the entire point of the
// escape class this fixture covers. A symlink needs SeCreateSymbolicLinkPrivilege (which is why
// every symlink fixture in this package skips on a stock Windows session); a junction needs
// nothing at all, so a local process can plant one inside the sync root at will.
func makeJunction(t *testing.T, link, target string) {
	t.Helper()
	out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput()
	if err != nil {
		t.Skipf("junctions unavailable on this host: %v (%s)", err, out)
	}
	fi, lerr := os.Lstat(link)
	if lerr != nil {
		t.Skipf("junction not observable on this host: %v", lerr)
	}
	// Pin the premise on the live filesystem: a junction is NOT spelled ModeSymlink
	// (Go maps every non-symlink reparse point to ModeIrregular), which is precisely why the
	// mode-bit-only containment gate was blind to it. Should a future Go start reporting it as a
	// symlink, these fixtures would silently stop covering what they claim to - fail loudly
	// instead of passing for the wrong reason.
	if fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("junction %q reports as a symlink (mode %v): the fixture's premise no longer holds", link, fi.Mode())
	}
	if fi.Mode()&os.ModeIrregular == 0 {
		t.Fatalf("junction %q reports neither symlink nor irregular (mode %v): reclassify the gate", link, fi.Mode())
	}
}
