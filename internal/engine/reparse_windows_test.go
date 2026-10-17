//go:build windows

package engine

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// TestTagIsNameSurrogate pins the line the gate draws across the reparse tags Windows defines.
// It is the whole safety argument for not simply refusing every FILE_ATTRIBUTE_REPARSE_POINT:
// the redirecting tags must be refused, and the tags that merely virtualise a file's STORAGE
// must sync like any other file - a sync root on a OneDrive-backed or deduplicated volume is an
// ordinary deployment, and rejecting it would dead-letter the user's whole tree.
//
// The values are the documented Win32 tags; a host that has no cloud or dedup volume cannot
// produce one on disk, which is exactly why the classification is table-tested here rather than
// only through makeJunction.
func TestTagIsNameSurrogate(t *testing.T) {
	for _, tc := range []struct {
		name string
		tag  uint32
		want bool
	}{
		{"not a reparse point at all", 0, false},
		{"symlink", uint32(windows.IO_REPARSE_TAG_SYMLINK), true},
		{"mount point (junction)", uint32(windows.IO_REPARSE_TAG_MOUNT_POINT), true},
		{"cloud files placeholder (OneDrive)", 0x9000_001A, false},
		{"data deduplication chunk", 0x8000_0013, false},
		{"windows container isolation", 0x9000_001C, false},
		{"AF_UNIX socket", 0x8000_0023, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tagIsNameSurrogate(tc.tag); got != tc.want {
				t.Errorf("tagIsNameSurrogate(%#08x) = %v, want %v", tc.tag, got, tc.want)
			}
		})
	}
}

// TestReparseTagOnRealEntries runs the query against the live filesystem: a regular file and a
// plain directory must report no reparse point at all (so pathRedirects never pays the query for
// them and never rejects them), and a junction must report a tag carrying the surrogate bit.
func TestReparseTagOnRealEntries(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "f.txt")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.Mkdir(plain, 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	for _, p := range []string{file, plain} {
		tag, err := reparseTag(p)
		if err != nil {
			t.Fatalf("reparseTag(%q): %v", p, err)
		}
		if tag != 0 {
			t.Errorf("reparseTag(%q) = %#08x, want 0 (not a reparse point)", p, tag)
		}
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("Lstat(%q): %v", p, err)
		}
		if pathRedirects(p, fi.Mode()) {
			t.Errorf("pathRedirects(%q) = true for an ordinary entry", p)
		}
	}

	link := filepath.Join(dir, "junc")
	makeJunction(t, link, plain)
	tag, err := reparseTag(link)
	if err != nil {
		t.Fatalf("reparseTag(junction): %v", err)
	}
	if !tagIsNameSurrogate(tag) {
		t.Errorf("junction tag %#08x is not a name surrogate", tag)
	}
	fi, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("Lstat(junction): %v", err)
	}
	if !pathRedirects(link, fi.Mode()) {
		t.Errorf("pathRedirects(junction) = false: the containment guards are blind to it")
	}
}

// TestReparseTagMissingPathFailsClosed pins the fail-closed arm. reparseTag is only ever asked
// about an entry Lstat already classified irregular, so a query it cannot answer describes an
// unrecognised reparse point - refusing it costs one dead-lettered op with a trace, admitting it
// would apply through a redirection nobody could characterise.
func TestReparseTagMissingPathFailsClosed(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	if _, err := reparseTag(missing); err == nil {
		t.Fatalf("reparseTag(%q) succeeded on a missing path", missing)
	}
	if !isNameSurrogate(missing) {
		t.Errorf("isNameSurrogate on an unanswerable path = false, want true (fail closed)")
	}
}
