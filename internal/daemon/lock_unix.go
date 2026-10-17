//go:build !windows

package daemon

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// acquireLock takes an exclusive, non-blocking advisory lock (flock LOCK_EX) on a
// held-open lock file at path, creating it if absent. It returns a release closure
// that drops the lock and closes the file. A second acquisition while the lock is
// held fails fast with ErrAlreadyRunning rather than blocking.
func acquireLock(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon: open lock file %q: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("daemon: lock %q: %w", path, err)
	}
	return func() error {
		// Unlock explicitly before closing so the lock is dropped promptly; closing
		// the descriptor would release it too, but ordering it first keeps the
		// release deterministic. The lock file itself is left in place (cheap, and
		// re-locked next start) rather than racing an unlink against a new instance.
		unlockErr := unix.Flock(int(f.Fd()), unix.LOCK_UN)
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
