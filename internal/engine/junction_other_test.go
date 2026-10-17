//go:build !windows

package engine

import "testing"

// makeJunction skips: a junction is a Windows-only construct. The Unix arm of the redirection
// class is the symlink, which the requireSymlinks fixtures already cover.
func makeJunction(t *testing.T, _, _ string) {
	t.Helper()
	t.Skip("junctions are a Windows-only construct")
}
