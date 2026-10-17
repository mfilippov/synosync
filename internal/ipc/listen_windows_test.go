//go:build windows

package ipc

import (
	"fmt"
	"io/fs"
	"testing"

	"github.com/Microsoft/go-winio"
)

func TestSddlForSID(t *testing.T) {
	const sid = "S-1-5-21-1004336348-1177238915-682003330-512"
	got := sddlForSID(sid)
	want := "D:P(A;;GA;;;" + sid + ")"
	if got != want {
		t.Fatalf("sddlForSID = %q, want %q", got, want)
	}
}

func TestIsDaemonAbsentWindows(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"missing pipe", fs.ErrNotExist, true},
		// A connect timeout is winio giving up on ERROR_PIPE_BUSY - the pipe
		// exists, so a live-but-busy daemon must NOT read as absent (see
		// IsDaemonBusy).
		{"connect timeout", winio.ErrTimeout, false},
		{"wrapped timeout", fmt.Errorf("dial: %w", winio.ErrTimeout), false},
		{"wrapped not-exist", fmt.Errorf("dial: %w", fs.ErrNotExist), true},
		{"other error", fmt.Errorf("some other failure"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDaemonAbsent(tt.err); got != tt.want {
				t.Fatalf("IsDaemonAbsent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestIsDaemonBusyWindows confirms the busy-vs-absent split: the connect
// timeout (winio retrying ERROR_PIPE_BUSY until the deadline) identifies a
// live-but-busy daemon, while a missing pipe stays "absent" and is never busy.
func TestIsDaemonBusyWindows(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connect timeout", winio.ErrTimeout, true},
		{"wrapped timeout", fmt.Errorf("dial: %w", winio.ErrTimeout), true},
		{"missing pipe", fs.ErrNotExist, false},
		{"other error", fmt.Errorf("some other failure"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDaemonBusy(tt.err); got != tt.want {
				t.Fatalf("IsDaemonBusy(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
