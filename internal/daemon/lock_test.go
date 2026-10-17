package daemon

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAcquireLockExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.lock")

	release, err := acquireLock(path)
	if err != nil {
		t.Fatalf("acquireLock: %v", err)
	}

	// A second acquisition on the held lock must fail fast, not block.
	if _, err := acquireLock(path); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second acquireLock: got %v, want ErrAlreadyRunning", err)
	}

	// After release the lock is free to take again.
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	release2, err := acquireLock(path)
	if err != nil {
		t.Fatalf("re-acquireLock after release: %v", err)
	}
	if err := release2(); err != nil {
		t.Fatalf("release2: %v", err)
	}
}

func TestAcquireLockOpenError(t *testing.T) {
	// A path whose parent does not exist cannot be created → open error, not a lock.
	path := filepath.Join(t.TempDir(), "missing", "daemon.lock")
	if _, err := acquireLock(path); err == nil {
		t.Fatal("acquireLock on uncreatable path: got nil, want error")
	} else if errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("acquireLock open error mis-mapped to ErrAlreadyRunning: %v", err)
	}
}
