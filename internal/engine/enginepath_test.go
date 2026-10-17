package engine

import "github.com/mfilippov/synosync/internal/fsutil"

// priorSyncDir returns the flush e would perform BEFORE a test replaces its seam, so a
// wrapper can still delegate to it.
//
// It must not be e.fsSyncDir: that is a method value bound to e, and it re-reads e.syncDir
// at CALL time - which by then is the wrapper itself. Capturing it that way is an infinite
// recursion, and the test binary dies with a stack overflow rather than a failure naming
// the test.
func priorSyncDir(e *Engine) func(string) error {
	if e.syncDir != nil {
		return e.syncDir
	}
	return fsutil.SyncDir
}
