//go:build windows

package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDeviceIDBeyondMaxPath pins the long-path case: osDeviceID is a DIRECT
// CreateFile, so it gets none of the stdlib's own MAX_PATH fixup even though os.MkdirAll
// - which materialises the tree in the first place - does. A view root nested deeper than
// 260 characters would have failed to open on a default-registry Windows and the
// empty-scan debounce would have lost its mount-swap guard on that view permanently.
//
// The volume serial number is the same one the shallow root reports, since both live on
// the same volume: that is what makes the answer usable as the mount-swap identity.
func TestDeviceIDBeyondMaxPath(t *testing.T) {
	root := t.TempDir()
	deep := root
	for len(deep) < 300 {
		deep = filepath.Join(deep, strings.Repeat("d", 30))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Skipf("host cannot materialise a >MAX_PATH tree: %v", err)
	}

	want, err := osDeviceID(root)
	if err != nil {
		t.Fatalf("osDeviceID(root): %v", err)
	}
	got, err := osDeviceID(deep)
	if err != nil {
		t.Fatalf("osDeviceID on a >MAX_PATH directory: %v", err)
	}
	if got != want {
		t.Errorf("device id = %d, want %d (the same volume as its own root)", got, want)
	}
}

// TestDeviceIDMissingPathStillFails pins that the prefixing did not turn an absent path
// into something openable: the empty-scan guard reads a failure as "the root is not
// there", and adoptViewRootDevice's content-evidence gate depends on it.
func TestDeviceIDMissingPathStillFails(t *testing.T) {
	if _, err := osDeviceID(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("osDeviceID on an absent path succeeded, want an error")
	}
}
