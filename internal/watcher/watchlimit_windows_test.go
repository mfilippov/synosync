//go:build windows

package watcher

import (
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestIsWatchLimitWindows(t *testing.T) {
	for _, err := range []error{
		&os.PathError{Op: "open", Path: `C:\x`, Err: windows.ERROR_ACCESS_DENIED},
		&os.PathError{Op: "open", Path: `C:\x`, Err: windows.ERROR_TOO_MANY_OPEN_FILES},
		os.ErrNotExist,
	} {
		if isWatchLimit(err) {
			t.Errorf("isWatchLimit(%v) = true; the subtree backend has no per-directory allowance", err)
		}
	}
}
