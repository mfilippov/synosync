//go:build !windows

package ipc

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"
)

// TestIsDaemonAbsentUnix exercises the Unix classification of dial errors: a
// missing socket (ENOENT) or a refused connection (a stale socket whose daemon
// exited) means "not running"; any other failure (e.g. a permission error) is a
// real problem the CLI must surface rather than mask.
func TestIsDaemonAbsentUnix(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"missing socket", fs.ErrNotExist, true},
		{"enoent syscall", syscall.ENOENT, true},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"wrapped refused", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true},
		{"permission denied", syscall.EACCES, false},
		{"nil error", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsDaemonAbsent(tc.err); got != tc.want {
				t.Errorf("IsDaemonAbsent(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsDaemonBusyUnix confirms Unix never classifies a dial error as "daemon
// busy": there is no AF_UNIX dial-error shape that positively identifies a live
// but saturated daemon (a full accept backlog blocks rather than erroring), so
// the busy hint is Windows-only.
func TestIsDaemonBusyUnix(t *testing.T) {
	errs := []error{
		nil,
		fs.ErrNotExist,
		syscall.ECONNREFUSED,
		syscall.EACCES,
		errors.New("boom"),
	}
	for _, err := range errs {
		if IsDaemonBusy(err) {
			t.Errorf("IsDaemonBusy(%v) = true, want false on Unix", err)
		}
	}
}
