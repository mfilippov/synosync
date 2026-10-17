//go:build windows

package fsutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExtendedPath pins the transformation the direct Win32 calls in this package
// depend on: short paths are untouched (the extended form disables Win32's own
// normalisation, so applying it everywhere would change their semantics), long
// drive-letter paths take the `\\?\` prefix, long UNC paths take `\\?\UNC\`, and an
// already-extended path is left alone.
func TestExtendedPath(t *testing.T) {
	long := strings.Repeat("seg\\", 70) + "leaf" // well past MAX_PATH
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"short drive path", `C:\sync\notes.txt`, `C:\sync\notes.txt`},
		{"short UNC path", `\\nas\share\notes.txt`, `\\nas\share\notes.txt`},
		{"long drive path", `C:\` + long, `\\?\C:\` + long},
		{"long UNC path", `\\nas\share\` + long, `\\?\UNC\nas\share\` + long},
		{"already extended", `\\?\C:\` + long, `\\?\C:\` + long},
		{"already NT-prefixed", `\??\C:\` + long, `\??\C:\` + long},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtendedPath(tt.in); got != tt.want {
				t.Fatalf("ExtendedPath(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestExtendedPathThresholdBoundary pins the exact switch-over point. The threshold is the
// whole reason the transformation is conditional - the extended form disables Win32's own
// normalisation, so an off-by-one here silently gives two logically identical paths different
// name semantics depending only on their length.
//
// It also pins that the trigger is measured in BYTES while the Win32 limit is in UTF-16 code
// units: a non-ASCII path therefore always over-converts (prefixes earlier than strictly
// needed) and never under-converts, which is the safe direction - the extended form works for
// short paths too, whereas a missed prefix is the >MAX_PATH failure this helper exists to remove.
func TestExtendedPathThresholdBoundary(t *testing.T) {
	// `C:\` plus a leaf sized so the whole path lands exactly on either side of the threshold.
	at := `C:\` + strings.Repeat("a", extendedPathThreshold-3)
	below := at[:len(at)-1]
	if len(at) != extendedPathThreshold || len(below) != extendedPathThreshold-1 {
		t.Fatalf("fixture lengths are %d/%d, want %d/%d", len(at), len(below), extendedPathThreshold, extendedPathThreshold-1)
	}
	if got := ExtendedPath(below); got != below {
		t.Errorf("ExtendedPath(len %d) = %q, want it untouched one byte below the threshold", len(below), got)
	}
	if got := ExtendedPath(at); got != `\\?\`+at {
		t.Errorf("ExtendedPath(len %d) = %q, want the extended form AT the threshold", len(at), got)
	}

	// Non-ASCII: 124 two-byte runes are 248 bytes but only 124 UTF-16 units, so this is
	// prefixed well before Win32 would reject it. Over-converting is correct-but-early; the
	// assertion exists so a future change cannot flip it to under-converting.
	wide := `C:\` + strings.Repeat("é", (extendedPathThreshold-3)/2+1)
	if len(wide) < extendedPathThreshold {
		t.Fatalf("fixture is %d bytes, want at least %d", len(wide), extendedPathThreshold)
	}
	if got := ExtendedPath(wide); got != `\\?\`+wide {
		t.Errorf("ExtendedPath(non-ASCII, %d bytes) = %q, want the extended form", len(wide), got)
	}

	// The empty path and other shapes that cannot be expressed are returned unchanged rather
	// than turned into a broken `\\?\` operand.
	for _, in := range []string{"", "a.txt", strings.Repeat("x", extendedPathThreshold)} {
		if got := ExtendedPath(in); strings.HasPrefix(got, `\\?\`) && !filepath.IsAbs(got[4:]) {
			t.Errorf("ExtendedPath(%q) = %q, want a well-formed extended path or the input unchanged", in, got)
		}
	}
}

// TestExtendedPathAbsolutisesRelative covers the relative arm: `\\?\` requires a
// fully qualified path, so a long relative one is made absolute first rather than
// prefixed into nonsense.
func TestExtendedPathAbsolutisesRelative(t *testing.T) {
	rel := strings.Repeat("seg\\", 70) + "leaf"
	got := ExtendedPath(rel)
	if !strings.HasPrefix(got, `\\?\`) {
		t.Fatalf("ExtendedPath(relative) = %q, want an extended-length path", got)
	}
	if !strings.HasSuffix(got, `\leaf`) {
		t.Fatalf("ExtendedPath(relative) = %q, want the original leaf preserved", got)
	}
}

// TestSyncDirAndRenameReplaceBeyondMaxPath drives both direct Win32 call sites
// against a tree deeper than MAX_PATH. The os package materialises such a tree on
// any Windows through its own fixLongPath, so a direct call that skipped the
// prefix would fail on exactly the trees the rest of the client created
// (the >MAX_PATH class); this pins that this package's own calls do not.
func TestSyncDirAndRenameReplaceBeyondMaxPath(t *testing.T) {
	root := t.TempDir()
	deep := root
	for len(deep) < 300 {
		deep = filepath.Join(deep, strings.Repeat("d", 30))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Skipf("host cannot materialise a >MAX_PATH tree: %v", err)
	}

	if err := SyncDir(deep); err != nil {
		t.Fatalf("SyncDir on a >MAX_PATH directory: %v", err)
	}

	from := filepath.Join(deep, "src.txt")
	to := filepath.Join(deep, "dst.txt")
	if err := os.WriteFile(from, []byte("payload"), 0o644); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if err := RenameReplace(from, to); err != nil {
		t.Fatalf("RenameReplace on a >MAX_PATH path: %v", err)
	}
	if got, err := os.ReadFile(to); err != nil || string(got) != "payload" {
		t.Fatalf("destination = %q (err %v), want the moved payload", got, err)
	}
}
