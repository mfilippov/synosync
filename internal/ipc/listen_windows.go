//go:build windows

package ipc

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os/user"
	"time"

	"github.com/Microsoft/go-winio"
)

// dialTimeout bounds a single connect attempt to the named pipe so the CLI does
// not block indefinitely when no daemon is listening.
const dialTimeout = 2 * time.Second

// sddlForSID builds a protected DACL granting Generic All to exactly one SID and
// nothing else: D:P disables inheritance from a parent, the single ACE grants the
// owning user full access. Anyone else (including administrators not matching the
// SID) is denied by the absence of an applicable ACE.
func sddlForSID(sid string) string {
	return fmt.Sprintf("D:P(A;;GA;;;%s)", sid)
}

// currentUserSDDL returns the security-descriptor string restricting the pipe to
// the current user, derived from that user's SID.
func currentUserSDDL() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("ipc: current user: %w", err)
	}
	if u.Uid == "" {
		return "", errors.New("ipc: current user has no SID")
	}
	return sddlForSID(u.Uid), nil
}

// Listen opens a named-pipe listener for the daemon control plane. The pipe name
// is derived from path (so callers keep passing Paths.DaemonSock()), and access
// is restricted to the current user via an explicit DACL - unlike the AF_UNIX
// file used on Unix, Windows enforces the pipe's security descriptor. No stale
// file needs removing: a named pipe is not a filesystem object.
func Listen(path string) (net.Listener, error) {
	sddl, err := currentUserSDDL()
	if err != nil {
		return nil, err
	}
	secret, err := loadOrCreatePipeSecret(path)
	if err != nil {
		return nil, err
	}
	name := pipeName(path, secret)
	l, err := winio.ListenPipe(name, &winio.PipeConfig{SecurityDescriptor: sddl})
	if err != nil {
		return nil, fmt.Errorf("ipc: listen pipe %q: %w", name, err)
	}
	return l, nil
}

// Dial connects to the daemon's named pipe derived from path, bounding the
// connect attempt with a short timeout.
func Dial(path string) (*Client, error) {
	secret, err := loadPipeSecret(path)
	if err != nil {
		// A missing secret file (fs.ErrNotExist) means no daemon has ever
		// listened, which IsDaemonAbsent reports as "daemon not running".
		return nil, err
	}
	name := pipeName(path, secret)
	timeout := dialTimeout
	conn, err := winio.DialPipe(name, &timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// IsDaemonAbsent reports whether a dial error means there is no live daemon
// listening: the named pipe (or the pipe-secret file) does not exist, so no
// daemon has created it - a named pipe vanishes with its owning process. A
// connect timeout (winio.ErrTimeout) is deliberately NOT absence: DialPipe only
// keeps waiting on ERROR_PIPE_BUSY, so its timeout is positive evidence that a
// pipe instance exists and a live-but-busy daemon holds it (see IsDaemonBusy).
// It encapsulates the Windows-specific error shapes so the CLI need not import
// winio.
func IsDaemonAbsent(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}

// IsDaemonBusy reports whether a dial error means a daemon is alive but too
// busy to accept a new connection right now: winio.DialPipe retries
// ERROR_PIPE_BUSY until its timeout and only then surfaces winio.ErrTimeout,
// while an absent pipe fails immediately with fs.ErrNotExist - so the
// connect-stage timeout identifies a live daemon whose pipe instances are all
// in use. The classification applies to the connect stage only; a timeout
// during an in-flight call is the caller's to report raw.
func IsDaemonBusy(err error) bool {
	return errors.Is(err, winio.ErrTimeout)
}
