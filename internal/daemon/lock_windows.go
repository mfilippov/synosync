//go:build windows

package daemon

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// acquireLock takes an exclusive, immediately-failing lock (LockFileEx with
// LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY) on a held-open lock file at
// path, creating it if absent. It returns a release closure that unlocks and
// closes the file. A second acquisition while the lock is held fails fast with
// ErrAlreadyRunning rather than blocking.
func acquireLock(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open lock file %q: %w", path, err)
	}
	h := windows.Handle(f.Fd())
	ol := new(windows.Overlapped)
	if err := windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, ol); err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("daemon: lock %q: %w", path, err)
	}
	return func() error {
		unlockErr := windows.UnlockFileEx(h, 0, 1, 0, ol)
		closeErr := f.Close()
		if unlockErr != nil {
			return fmt.Errorf("daemon: unlock %q: %w", path, unlockErr)
		}
		if closeErr != nil {
			return fmt.Errorf("daemon: close lock file %q: %w", path, closeErr)
		}
		return nil
	}, nil
}
