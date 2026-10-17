//go:build !windows

package engine

import "testing"

// hostShortName reports that the host generates no 8.3 alias: a Unix-like filesystem stores one
// name per directory entry. The empirical fixtures skip on the false return; the seam-driven
// ones (e.longPath) run everywhere and carry the cross-platform coverage.
func hostShortName(t *testing.T, _ string) (string, bool) {
	t.Helper()
	return "", false
}
