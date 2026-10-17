package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDirIdentityTellsAnUnmountFromAnEmptyOut pins, on the host's real filesystem, the property
// the nested-unmount guard rests on: emptying a directory keeps its identity, and replacing the
// object at the path - what an unmount does - changes it. Both the free lstat-time identity and
// the by-path read must agree wherever the platform offers both.
func TestDirIdentityTellsAnUnmountFromAnEmptyOut(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "d")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := osDirIdentity(dir)
	if err != nil {
		t.Fatalf("osDirIdentity: %v", err)
	}
	if fi, err := os.Lstat(dir); err != nil {
		t.Fatal(err)
	} else if id, ok := dirIdentity(fi); ok && id != before {
		t.Errorf("lstat identity %v disagrees with the by-path read %v", id, before)
	}

	// Emptied in place: the same object.
	if err := os.Remove(filepath.Join(dir, "f")); err != nil {
		t.Fatal(err)
	}
	if after, err := osDirIdentity(dir); err != nil || after != before {
		t.Errorf("emptying the directory changed its identity: %v -> %v (err %v)", before, after, err)
	}

	// Replaced: the old object is kept alive elsewhere, so its index cannot be reused.
	if err := os.Rename(dir, filepath.Join(root, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if replaced, err := osDirIdentity(dir); err != nil || replaced == before {
		t.Errorf("a different directory at the path kept the identity %v (err %v)", replaced, err)
	}

	if _, err := osDirIdentity(filepath.Join(root, "absent")); err == nil {
		t.Error("an absent path yielded an identity")
	}
}
