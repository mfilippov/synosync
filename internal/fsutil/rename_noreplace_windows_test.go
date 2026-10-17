//go:build windows

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// captureMoveOperands installs a MoveFileEx seam that records the operand strings the
// syscall is handed and then reports ok to the caller without touching the disk. It is
// how the extended-length prefixing is pinned on ANY host: this machine has
// LongPathsEnabled=1, so a raw >MAX_PATH operand would be accepted by Win32 itself and
// the >MAX_PATH regression could not be reproduced end to end here.
func captureMoveOperands(t *testing.T) *[][2]string {
	t.Helper()
	var seen [][2]string
	orig := moveFileEx
	moveFileEx = func(from, to *uint16, _ uint32) error {
		seen = append(seen, [2]string{windows.UTF16PtrToString(from), windows.UTF16PtrToString(to)})
		return nil
	}
	t.Cleanup(func() { moveFileEx = orig })
	return &seen
}

// TestRenameNoReplaceOperandsBeyondMaxPath pins that the no-replace primitive hands
// MoveFileEx extended-length operands once the paths are long enough for Win32's
// MAX_PATH limit to reject them. Without the prefix, the keep-both core
// (preserveConflictCopy → RenameNoReplace) failed on precisely the trees the rest of
// the client materialises through the os package's own fixLongPath - either as a
// forever-transient ERROR_PATH_NOT_FOUND or as a nameTooLong dead-letter of a
// perfectly valid name.
func TestRenameNoReplaceOperandsBeyondMaxPath(t *testing.T) {
	seen := captureMoveOperands(t)
	deep := `C:\sync\` + strings.Repeat("segment\\", 40)
	from, to := deep+"src.txt", deep+"dst.txt"

	if err := renameNoReplace(from, to); err != nil {
		t.Fatalf("renameNoReplace: %v", err)
	}
	if len(*seen) != 1 {
		t.Fatalf("MoveFileEx calls = %d, want exactly 1", len(*seen))
	}
	got := (*seen)[0]
	for i, want := range [2]string{`\\?\` + from, `\\?\` + to} {
		if got[i] != want {
			t.Errorf("operand %d = %q, want %q", i, got[i], want)
		}
	}
}

// TestRenameNoReplaceOperandsUnchangedForShortPaths pins the other half: the extended
// form also disables Win32's own normalisation, so a path short enough not to need it
// must reach the syscall verbatim - including a UNC path, whose extended spelling is a
// different shape (`\\?\UNC\`) and must not be applied where it is not needed.
func TestRenameNoReplaceOperandsUnchangedForShortPaths(t *testing.T) {
	long := strings.Repeat("segment\\", 40)
	tests := []struct {
		name     string
		from, to string
		wantFrom string
		wantTo   string
	}{
		{
			name: "short drive paths", from: `C:\sync\a.txt`, to: `C:\sync\b.txt`,
			wantFrom: `C:\sync\a.txt`, wantTo: `C:\sync\b.txt`,
		},
		{
			name: "short UNC paths", from: `\\nas\share\a.txt`, to: `\\nas\share\b.txt`,
			wantFrom: `\\nas\share\a.txt`, wantTo: `\\nas\share\b.txt`,
		},
		{
			name: "long UNC paths", from: `\\nas\share\` + long + "a.txt", to: `\\nas\share\` + long + "b.txt",
			wantFrom: `\\?\UNC\nas\share\` + long + "a.txt", wantTo: `\\?\UNC\nas\share\` + long + "b.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := captureMoveOperands(t)
			if err := renameNoReplace(tt.from, tt.to); err != nil {
				t.Fatalf("renameNoReplace: %v", err)
			}
			if len(*seen) != 1 {
				t.Fatalf("MoveFileEx calls = %d, want exactly 1", len(*seen))
			}
			if got := (*seen)[0]; got[0] != tt.wantFrom || got[1] != tt.wantTo {
				t.Errorf("operands = %q, want %q", got, [2]string{tt.wantFrom, tt.wantTo})
			}
		})
	}
}

// TestRenameNoReplaceBeyondMaxPath drives the whole exported primitive - durable parent
// creation, the move, the parent flushes - against a real tree deeper than MAX_PATH, and
// pins that the no-replace contract itself still holds there: the second move onto the
// same destination reports fs.ErrExist rather than clobbering the copy the keep-both
// resolver just parked.
func TestRenameNoReplaceBeyondMaxPath(t *testing.T) {
	root := t.TempDir()
	deep := root
	for len(deep) < 300 {
		deep = filepath.Join(deep, strings.Repeat("d", 30))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Skipf("host cannot materialise a >MAX_PATH tree: %v", err)
	}
	from := filepath.Join(deep, "src.txt")
	to := filepath.Join(deep, "aside.txt")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if err := RenameNoReplace(from, to); err != nil {
		t.Fatalf("RenameNoReplace on a >MAX_PATH path: %v", err)
	}
	if got, err := os.ReadFile(to); err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q (err %v), want the moved payload", got, err)
	}

	if err := os.WriteFile(from, []byte("second"), 0o644); err != nil {
		t.Fatalf("re-seed source: %v", err)
	}
	if err := RenameNoReplace(from, to); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("RenameNoReplace onto an existing >MAX_PATH destination = %v, want fs.ErrExist", err)
	}
	if got, err := os.ReadFile(to); err != nil || string(got) != "payload" {
		t.Errorf("destination = %q (err %v), want the first payload preserved", got, err)
	}
}
