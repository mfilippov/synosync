//go:build !windows

package ipc

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"
)

// Listen opens an AF_UNIX listener at path with 0600 permissions. A stale socket
// file left by a previous (crashed) daemon is removed first so a re-listen
// succeeds, and the socket is chmod'd to 0600 after binding so it is reachable
// only by the owning user regardless of umask.
func Listen(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("ipc: remove stale socket %q: %w", path, err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("ipc: listen %q: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("ipc: chmod socket %q: %w", path, err)
	}
	return l, nil
}

// Dial connects to the daemon's AF_UNIX socket at path.
func Dial(path string) (*Client, error) {
	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// IsDaemonAbsent reports whether a dial error means there is no live daemon
// listening: the socket file is missing (no daemon ever bound it) or the
// connection was refused (a stale socket whose daemon has exited). It mirrors the
// Windows variant so the CLI reports a missing daemon the same way on every
// platform without importing OS-specific error shapes.
func IsDaemonAbsent(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED)
}

// IsDaemonBusy reports whether a dial error means a daemon is alive but too
// busy to accept a new connection. On Unix there is no such dial-error shape:
// an AF_UNIX connect succeeds, is refused (a stale socket - absent), or fails
// with ENOENT (no socket - absent); a saturated accept backlog blocks the
// connect rather than erroring. Always false - the func exists so the CLI's
// busy-vs-absent dial classification stays per-OS (the Windows named-pipe
// connect timeout does signal busy) without importing OS-specific error shapes.
func IsDaemonBusy(error) bool {
	return false
}
