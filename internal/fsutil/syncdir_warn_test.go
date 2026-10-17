package fsutil

import "testing"

// captureFsyncWarns captures the degraded-durability warning without touching the
// flush seam (the real directory open/flush still runs), restoring the warn seam
// and the once-guard on cleanup. It returns a counter of warnings fired. Both
// per-OS arms route their degradation through the same seam, so the helper is
// shared rather than duplicated per build tag.
func captureFsyncWarns(t *testing.T) *int {
	t.Helper()
	origWarn := warnFsyncDegraded
	t.Cleanup(func() {
		warnFsyncDegraded = origWarn
		fsyncDegradedWarned.Store(false)
	})
	fsyncDegradedWarned.Store(false)
	warns := new(int)
	warnFsyncDegraded = func(string, error) { *warns++ }
	return warns
}
