//go:build windows

package main

import (
	"fmt"
	"syscall"
)

// daemonNotRunningPlatformCases covers the Windows divergence: an absent
// named-pipe daemon presents as a missing pipe, never as a refused connection,
// so ECONNREFUSED must NOT classify as daemon-absent (unlike the Unix AF_UNIX
// socket). A connect timeout is not absence either - it means a live-but-busy
// daemon holds the pipe (ipc.IsDaemonBusy, tested in internal/ipc so this
// package need not import winio).
var daemonNotRunningPlatformCases = []daemonNotRunningCase{
	{"connection refused", syscall.ECONNREFUSED, false},
	{"wrapped refused", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), false},
}
