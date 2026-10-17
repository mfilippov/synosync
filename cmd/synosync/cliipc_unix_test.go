//go:build !windows

package main

import (
	"fmt"
	"syscall"
)

// daemonNotRunningPlatformCases covers the Unix-only dial errors that classify as
// "daemon absent": a refused connection to a stale AF_UNIX socket whose daemon
// has exited.
var daemonNotRunningPlatformCases = []daemonNotRunningCase{
	{"connection refused", syscall.ECONNREFUSED, true},
	{"wrapped refused", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true},
}
