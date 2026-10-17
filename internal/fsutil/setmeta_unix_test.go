//go:build !windows

package fsutil

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestSetMetaRefusesSymlinkTarget models the symlink-swap window: the
// freshly promoted target is swapped for a symlink between the promote-rename
// and SetMeta. The former name-based os.Chmod/os.Chtimes followed the link, so
// a hostile server perm 4755 landed ModeSetuid on the link's target anywhere
// outside the sync tree; the O_NOFOLLOW open must refuse the link (the op then
// retries transiently through the engine's normal pre-apply symlink checks)
// and the link's target must be untouched.
func TestSetMetaRefusesSymlinkTarget(t *testing.T) {
	dir := t.TempDir()

	// The "outside the tree" victims a swapped-in link would redirect to.
	victimFile := filepath.Join(dir, "victim.txt")
	if err := os.WriteFile(victimFile, []byte("v"), 0o644); err != nil {
		t.Fatalf("seed victim file: %v", err)
	}
	victimDir := filepath.Join(dir, "victimdir")
	if err := os.Mkdir(victimDir, 0o755); err != nil {
		t.Fatalf("seed victim dir: %v", err)
	}
	before := time.Unix(1_500_000_000, 0)
	for _, p := range []string{victimFile, victimDir} {
		if err := os.Chtimes(p, before, before); err != nil {
			t.Fatalf("Chtimes(%s): %v", p, err)
		}
	}

	cases := []struct {
		name     string
		victim   string
		wantPerm os.FileMode
	}{
		// SetMeta targets both files (download promote) and directories
		// (inbound mkdir), so both link shapes must be refused.
		{name: "file", victim: victimFile, wantPerm: 0o644},
		{name: "dir", victim: victimDir, wantPerm: 0o755},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			link := filepath.Join(dir, "target-"+tc.name)
			if err := os.Symlink(tc.victim, link); err != nil {
				t.Fatalf("Symlink: %v", err)
			}

			// 4755: the setuid shape.
			if err := SetMeta(link, 1_600_000_000, 4755); err == nil {
				t.Fatal("SetMeta on a symlink target succeeded; want a refusal")
			}

			info, err := os.Stat(tc.victim)
			if err != nil {
				t.Fatalf("Stat victim: %v", err)
			}
			if info.Mode()&os.ModeSetuid != 0 {
				t.Fatal("setuid landed on the symlink's target")
			}
			if perm := info.Mode().Perm(); perm != tc.wantPerm {
				t.Fatalf("victim perm = %o, want unchanged %o", perm, tc.wantPerm)
			}
			if got := info.ModTime().Unix(); got != before.Unix() {
				t.Fatalf("victim mtime = %d, want unchanged %d", got, before.Unix())
			}
		})
	}
}

// TestSetMetaFIFOTargetDoesNotBlock pins the O_NONBLOCK flag on the no-follow
// open: a FIFO swapped onto the target would otherwise block the opening
// worker forever (the same shape openUploadFileNoFollow guards against). A
// FIFO is not a symlink, so the metadata still applies - to the local FIFO
// inode, contained in the tree, matching the prior name-based behaviour.
func TestSetMetaFIFOTargetDoesNotBlock(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}

	const mtime = 1_600_000_000
	done := make(chan error, 1)
	go func() { done <- SetMeta(fifo, mtime, 600) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetMeta on FIFO: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetMeta blocked opening a FIFO target (O_NONBLOCK missing)")
	}

	info, err := os.Lstat(fifo)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	if got := info.ModTime().Unix(); got != mtime {
		t.Fatalf("fifo mtime = %d, want %d", got, mtime)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("fifo perm = %o, want 0600", perm)
	}
}
