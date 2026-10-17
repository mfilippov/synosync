package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMkdir(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b", "c")

	if err := Mkdir(nested); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected a directory")
	}
	// Idempotent.
	if err := Mkdir(nested); err != nil {
		t.Fatalf("Mkdir (second): %v", err)
	}
}

func TestMkdirParentIsFile(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// A file in the parent chain makes MkdirAll fail.
	if err := Mkdir(filepath.Join(blocker, "child")); err == nil {
		t.Fatal("expected error when parent component is a file")
	}
}

func TestRenameParentIsFile(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "from")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	// Destination parent cannot be created because blocker is a file.
	if err := Rename(from, filepath.Join(blocker, "sub", "to")); err == nil {
		t.Fatal("expected error when destination parent cannot be created")
	}
}

func TestRename(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "from.txt")
	to := filepath.Join(dir, "sub", "to.txt")

	if err := os.WriteFile(from, []byte("data"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := Rename(from, to); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := os.Stat(from); !os.IsNotExist(err) {
		t.Fatalf("source should be gone, err = %v", err)
	}
	got, err := os.ReadFile(to)
	if err != nil {
		t.Fatalf("ReadFile dest: %v", err)
	}
	if string(got) != "data" {
		t.Fatalf("content = %q, want %q", got, "data")
	}
}

func TestRenameMissingSource(t *testing.T) {
	dir := t.TempDir()
	err := Rename(filepath.Join(dir, "nope"), filepath.Join(dir, "dest"))
	if err == nil {
		t.Fatal("expected error renaming missing source")
	}
}

func TestRemoveIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("should be gone, err = %v", err)
	}
	// Removing a non-existent path is not an error.
	if err := Remove(path); err != nil {
		t.Fatalf("Remove (idempotent): %v", err)
	}
}

func TestRemoveRecursive(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "tree", "deep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("y"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := Remove(filepath.Join(dir, "tree")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "tree")); !os.IsNotExist(err) {
		t.Fatalf("tree should be gone, err = %v", err)
	}
}

func TestSetMetaMtime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const mtime = 1_600_000_000
	if err := SetMeta(path, mtime, 644); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.ModTime().Unix(); got != mtime {
		t.Fatalf("mtime = %d, want %d", got, mtime)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Fatalf("perm = %o, want 0644 (decimal 644 -> octal)", perm)
		}
	}
}

func TestSetMetaZeroMtimeLeavesUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := time.Unix(1_500_000_000, 0)
	if err := os.Chtimes(path, before, before); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	if err := SetMeta(path, 0, 600); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.ModTime().Unix(); got != before.Unix() {
		t.Fatalf("mtime = %d, want unchanged %d", got, before.Unix())
	}
}

// TestSetMetaZeroPermLeavesUnchanged confirms a zero unixPermDecimal (the wire's
// unset/unknown form) leaves the file's permissions intact rather than chmodding it
// to an unusable 0000.
func TestSetMetaZeroPermLeavesUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := SetMeta(path, 1_600_000_000, 0); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Fatalf("perm = %o, want 0644 (zero perm must not chmod)", perm)
		}
	}
}

func TestSetMetaMissingFile(t *testing.T) {
	err := SetMeta(filepath.Join(t.TempDir(), "nope"), 1_600_000_000, 644)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestDecimalToMode(t *testing.T) {
	tests := []struct {
		dec  uint32
		want os.FileMode
	}{
		{644, 0o644},
		{755, 0o755},
		{600, 0o600},
		{777, 0o777},
		{0, 0},
		// Special bits translate to their os.FileMode positions:
		// the raw octal bits would be silently ignored by os.Chmod.
		{2775, 0o775 | os.ModeSetgid},
		{4755, 0o755 | os.ModeSetuid},
		{1777, 0o777 | os.ModeSticky},
		{7666, 0o666 | os.ModeSetuid | os.ModeSetgid | os.ModeSticky},
		// A full st_mode's file-type bits are dropped; only permission (and
		// mapped special bits) survive.
		{100644, 0o644},
		{104755, 0o755 | os.ModeSetuid},
	}
	for _, tc := range tests {
		got, err := decimalToMode(tc.dec)
		if err != nil {
			t.Fatalf("decimalToMode(%d): %v", tc.dec, err)
		}
		if got != tc.want {
			t.Fatalf("decimalToMode(%d) = %o, want %o", tc.dec, got, tc.want)
		}
	}
}

func TestDecimalToModeInvalid(t *testing.T) {
	// A decimal digit of 8 or 9 is not a valid octal digit.
	if _, err := decimalToMode(648); err == nil {
		t.Fatal("expected error for non-octal digit")
	}
}

// TestSetMetaSpecialBitsApplied confirms a wire perm carrying a
// setuid/setgid/sticky special bit lands on disk: before the fix the
// raw octal bit (e.g. 0o2000) was passed to os.Chmod, which only honours the
// os.FileMode forms, so 2775 was silently applied as 0775.
func TestSetMetaSpecialBitsApplied(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod is skipped on Windows")
	}
	dir := t.TempDir()

	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := SetMeta(path, 0, 2775); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode()&os.ModeSetgid == 0 {
		t.Fatalf("mode = %v, want setgid bit set", info.Mode())
	}
	if perm := info.Mode().Perm(); perm != 0o775 {
		t.Fatalf("perm = %o, want 0775", perm)
	}

	// Sticky on a directory - the classic shared-dir form.
	sub := filepath.Join(dir, "shared")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := SetMeta(sub, 0, 1777); err != nil {
		t.Fatalf("SetMeta(dir): %v", err)
	}
	dinfo, err := os.Stat(sub)
	if err != nil {
		t.Fatalf("Stat(dir): %v", err)
	}
	if dinfo.Mode()&os.ModeSticky == 0 {
		t.Fatalf("dir mode = %v, want sticky bit set", dinfo.Mode())
	}
	if perm := dinfo.Mode().Perm(); perm != 0o777 {
		t.Fatalf("dir perm = %o, want 0777", perm)
	}
}

// TestSetMetaMalformedPermSkipsChmod confirms a malformed wire perm (a decimal
// digit 8/9 - not valid octal) warns and skips the chmod instead of failing
// SetMeta: SetMeta runs after the download promote, so an error here left the
// op transient-retried (and the file re-downloaded) on every drain forever.
// The mtime is still applied and the file left in place.
func TestSetMetaMalformedPermSkipsChmod(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var warns int
	orig := warnMalformedPerm
	warnMalformedPerm = func(string, uint32, error) { warns++ }
	defer func() { warnMalformedPerm = orig }()

	const mtime = 1_600_000_000
	if err := SetMeta(path, mtime, 989); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.ModTime().Unix(); got != mtime {
		t.Fatalf("mtime = %d, want %d (still applied)", got, mtime)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Fatalf("perm = %o, want unchanged 0644", perm)
		}
		if warns != 1 {
			t.Fatalf("warns = %d, want 1", warns)
		}
	}
}

// TestSetMetaOutOfRangeMtimeSkipsStamp confirms an out-of-range mtime (a
// millisecond-scale value a non-conformant DSM might report, or a negative one)
// warns and skips the mtime stamp instead of overflowing the nanosecond
// conversion - which would kernel-clamp to a wrong value on Linux or fail
// os.Chtimes/futimes AFTER the download promote on Windows/macOS, leaving the op
// transient-retried and re-downloaded forever (the
// forever-transient-after-promote class, mirrored onto the mtime axis).
// The perm is still applied and the file left in place, and
// the pre-existing mtime is untouched.
func TestSetMetaOutOfRangeMtimeSkipsStamp(t *testing.T) {
	cases := []struct {
		name  string
		mtime int64
	}{
		{"millisecond-scale", 1_600_000_000_000},
		{"just-over-cap", maxMtimeSeconds + 1},
		{"negative", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "file.txt")
			if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
				t.Fatalf("seed: %v", err)
			}
			before := time.Unix(1_500_000_000, 0)
			if err := os.Chtimes(path, before, before); err != nil {
				t.Fatalf("Chtimes: %v", err)
			}

			var warns int
			orig := warnMalformedMtime
			warnMalformedMtime = func(string, int64) { warns++ }
			defer func() { warnMalformedMtime = orig }()

			if err := SetMeta(path, tc.mtime, 600); err != nil {
				t.Fatalf("SetMeta: %v", err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if got := info.ModTime().Unix(); got != before.Unix() {
				t.Fatalf("mtime = %d, want unchanged %d (out-of-range stamp must be skipped)", got, before.Unix())
			}
			if warns != 1 {
				t.Fatalf("warns = %d, want 1", warns)
			}
			if runtime.GOOS != "windows" {
				if perm := info.Mode().Perm(); perm != 0o600 {
					t.Fatalf("perm = %o, want 0600 (still applied)", perm)
				}
			}
		})
	}
}

// TestSetMetaBoundaryMtimeApplied confirms an mtime exactly at the accepted cap
// is still stamped - the guard rejects only values strictly past it.
func TestSetMetaBoundaryMtimeApplied(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var warns int
	orig := warnMalformedMtime
	warnMalformedMtime = func(string, int64) { warns++ }
	defer func() { warnMalformedMtime = orig }()

	if err := SetMeta(path, maxMtimeSeconds, 0); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.ModTime().Unix(); got != maxMtimeSeconds {
		t.Fatalf("mtime = %d, want %d (cap value is accepted)", got, maxMtimeSeconds)
	}
	if warns != 0 {
		t.Fatalf("warns = %d, want 0 (cap value must not warn)", warns)
	}
}

// withRecordedSyncDir swaps the syncDir seam for one that records the
// directories it was asked to flush, restoring the original on cleanup.
func withRecordedSyncDir(t *testing.T) *[]string {
	t.Helper()
	var calls []string
	orig := syncDir
	syncDir = func(dir string) error {
		calls = append(calls, dir)
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })
	return &calls
}

// TestMkdirSyncsParent confirms Mkdir fsyncs the created leaf's parent so the new
// directory entry is durable.
func TestMkdirSyncsParent(t *testing.T) {
	calls := withRecordedSyncDir(t)
	dir := t.TempDir()
	leaf := filepath.Join(dir, "a", "b")
	if err := Mkdir(leaf); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	want := filepath.Dir(leaf)
	found := false
	for _, c := range *calls {
		if c == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("SyncDir not called for parent %q; calls = %v", want, *calls)
	}
}

// TestMkdirSyncsEveryCreatedAncestor confirms Mkdir fsyncs the parent of EVERY
// directory it creates, not just the leaf's, so a multi-level MkdirAll leaves no
// intermediate directory entry non-durable across a crash.
func TestMkdirSyncsEveryCreatedAncestor(t *testing.T) {
	calls := withRecordedSyncDir(t)
	dir := t.TempDir()
	// dir exists; "a", "a/b" and "a/b/c" are all created by this one Mkdir.
	leaf := filepath.Join(dir, "a", "b", "c")
	if err := Mkdir(leaf); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	// Each created directory's parent must have been fsynced: dir (parent of a),
	// dir/a (parent of b) and dir/a/b (parent of c).
	for _, want := range []string{dir, filepath.Join(dir, "a"), filepath.Join(dir, "a", "b")} {
		found := false
		for _, c := range *calls {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("SyncDir not called for created-ancestor parent %q; calls = %v", want, *calls)
		}
	}
}

// TestMkdirAllDurable confirms the exported durable-create honours the caller's
// permission bits (the store's 0700 state dir) and fsyncs
// the parent of every directory it creates, while an already-existing path
// re-flushes the leaf's parent.
func TestMkdirAllDurable(t *testing.T) {
	calls := withRecordedSyncDir(t)
	base := t.TempDir()
	leaf := filepath.Join(base, "state", "sub")
	if err := MkdirAllDurable(leaf, 0o700); err != nil {
		t.Fatalf("MkdirAllDurable: %v", err)
	}
	for _, p := range []string{filepath.Join(base, "state"), leaf} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		// Windows does not honour Unix permission bits.
		if runtime.GOOS != "windows" {
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Fatalf("perm(%s) = %o, want 0700", p, perm)
			}
		}
	}
	for _, want := range []string{base, filepath.Join(base, "state")} {
		found := false
		for _, c := range *calls {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("SyncDir not called for %q; calls = %v", want, *calls)
		}
	}
	*calls = nil
	if err := MkdirAllDurable(leaf, 0o700); err != nil {
		t.Fatalf("MkdirAllDurable (existing): %v", err)
	}
	if want := []string{filepath.Join(base, "state")}; len(*calls) != 1 || (*calls)[0] != want[0] {
		t.Fatalf("existing path: SyncDir calls = %v, want %v", *calls, want)
	}
}

// TestMkdirExistingResyncsParent confirms Mkdir of an already-existing directory
// creates nothing but still flushes the leaf's parent: the
// created-set is computed before the MkdirAll, so a call whose flush failed leaves
// the directory on disk and the retry sees an empty created-set - a one-shot flush
// would be skipped forever there, leaving the entry non-durable under a StateSynced
// baseline.
func TestMkdirExistingResyncsParent(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "already")
	if err := os.MkdirAll(existing, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	if err := Mkdir(existing); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if len(*calls) != 1 || (*calls)[0] != dir {
		t.Fatalf("SyncDir calls = %v, want exactly [%q]", *calls, dir)
	}
}

// TestMkdirRetriesSyncAfterTransientFailure is the failing interleaving itself: the
// first Mkdir creates the directory but its parent flush fails (a genuine EIO -
// SyncDir degrades only capability gaps), so the op is retried; the retry finds the
// directory already present and MUST flush the parent again. Before the fix
// the retry computed an empty created-set and skipped the flush forever, so
// applyMkdir went on to write a StateSynced baseline and ack over a non-durable
// entry: a power loss before writeback lost the directory and the next local scan
// diffed the synced-baseline-without-directory into an outbound OpRemove of the
// server directory and its whole subtree.
func TestMkdirRetriesSyncAfterTransientFailure(t *testing.T) {
	dir := t.TempDir()
	leaf := filepath.Join(dir, "created")

	var calls []string
	failNext := true
	orig := syncDir
	syncDir = func(d string) error {
		calls = append(calls, d)
		if failNext {
			failNext = false
			return errors.New("simulated EIO")
		}
		return orig(d)
	}
	t.Cleanup(func() { syncDir = orig })

	if err := Mkdir(leaf); err == nil {
		t.Fatal("Mkdir: want the injected flush failure, got nil")
	}
	if len(calls) != 1 || calls[0] != dir {
		t.Fatalf("first attempt: SyncDir calls = %v, want [%q]", calls, dir)
	}
	if _, err := os.Stat(leaf); err != nil {
		t.Fatalf("the directory must be on disk after the failed flush: %v", err)
	}

	calls = nil
	if err := Mkdir(leaf); err != nil {
		t.Fatalf("Mkdir (retry): %v", err)
	}
	if len(calls) != 1 || calls[0] != dir {
		t.Fatalf("retry: SyncDir calls = %v, want the parent flush repeated: [%q]", calls, dir)
	}
}

// TestRenameCrossDirSyncsBothParents confirms a cross-directory Rename fsyncs
// BOTH affected directories - the destination's parent (new entry) first, then
// the source's parent (removed entry) - so a crash immediately after the move
// cannot resurrect the entry at the source path, which the next scan would push
// to the server as a duplicate create.
func TestRenameCrossDirSyncsBothParents(t *testing.T) {
	dir := t.TempDir()
	fromDir := filepath.Join(dir, "a")
	toDir := filepath.Join(dir, "b")
	for _, d := range []string{fromDir, toDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed dir: %v", err)
		}
	}
	from := filepath.Join(fromDir, "f.txt")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	if err := Rename(from, filepath.Join(toDir, "f.txt")); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// The destination parent pre-exists, so mkdirAllDurable creates nothing but still
	// re-flushes ITS parent (the retry guarantee for a parent a
	// previous Rename created and could not flush); then the post-rename pair follows,
	// destination first, then source.
	if len(*calls) != 3 || (*calls)[0] != dir || (*calls)[1] != toDir || (*calls)[2] != fromDir {
		t.Fatalf("SyncDir calls = %v, want [%q %q %q]", *calls, dir, toDir, fromDir)
	}
}

// TestRenameSameDirSyncsOnce confirms a same-directory Rename flushes the shared
// parent exactly once POST-RENAME - the source and destination entries live in the
// same directory, so a second fsync of it would be redundant. (The destination
// parent's own parent is flushed first by mkdirAllDurable's unconditional re-flush;
// it is a different directory and not part of that dedup.)
func TestRenameSameDirSyncsOnce(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "old.txt")
	if err := os.WriteFile(from, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	calls := withRecordedSyncDir(t)
	if err := Rename(from, filepath.Join(dir, "new.txt")); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if len(*calls) != 2 || (*calls)[0] != filepath.Dir(dir) || (*calls)[1] != dir {
		t.Fatalf("SyncDir calls = %v, want exactly [%q %q]", *calls, filepath.Dir(dir), dir)
	}
}

// TestRemoveSyncsParent confirms Remove fsyncs the parent so the entry's removal
// is durable.
func TestRemoveSyncsParent(t *testing.T) {
	calls := withRecordedSyncDir(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	found := false
	for _, c := range *calls {
		if c == dir {
			found = true
		}
	}
	if !found {
		t.Fatalf("SyncDir not called for parent %q; calls = %v", dir, *calls)
	}
}

// TestRemoveMissingParentTolerated confirms that an idempotent remove whose
// parent directory is itself absent does not surface the missing-parent fsync as
// an error.
func TestRemoveMissingParentTolerated(t *testing.T) {
	dir := t.TempDir()
	// Neither "a" nor "a/b" exists, so RemoveAll is a no-op and the parent "a"
	// of the removed path is missing.
	if err := Remove(filepath.Join(dir, "a", "b")); err != nil {
		t.Fatalf("Remove with missing parent: %v", err)
	}
}

func TestSyncDirExisting(t *testing.T) {
	// SyncDir on a real directory succeeds: a POSIX directory fsync on Unix, a
	// FlushFileBuffers over a backup-semantics handle on Windows.
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}
}

// TestSyncDirMissing pins that a missing directory is an error on EVERY platform.
// The Windows arm used to expect nil here because fsyncDir was a no-op there; now
// that the flush is real the open fails like any other, and fsutil.Remove's
// idempotent tolerance of an absent parent rests on that error being not-exist
// (see TestRemoveMissingParentTolerated).
func TestSyncDirMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	err := SyncDir(missing)
	if err == nil {
		t.Fatal("SyncDir(missing) = nil, want error")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncDir(missing) = %v, want a not-exist error", err)
	}
}

// TestMkdirAllDurableFlushesEveryAncestorDespiteFailure pins the aggregation added with the
// unconditional re-flush: missingAncestors reports the created chain deepest-first, so returning
// on the FIRST syncDir failure would leave every shallower ancestor's entry unflushed - and
// the op's retry then finds the whole chain existing, computes an empty created-set and
// re-flushes only the leaf's parent, so those entries would never become durable. That is the
// same one-shot skip the unconditional re-flush closes, one level up.
func TestMkdirAllDurableFlushesEveryAncestorDespiteFailure(t *testing.T) {
	base := t.TempDir()
	leaf := filepath.Join(base, "a", "b", "c")
	deepest := filepath.Join(base, "a", "b") // the leaf's parent: flushed first, made to fail

	var calls []string
	orig := syncDir
	syncDir = func(dir string) error {
		calls = append(calls, dir)
		if dir == deepest {
			return errors.New("simulated EIO")
		}
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })

	err := MkdirAllDurable(leaf, 0o755)
	if err == nil {
		t.Fatal("MkdirAllDurable returned nil despite a flush failure")
	}
	if !strings.Contains(err.Error(), "simulated EIO") {
		t.Fatalf("error = %v, want the flush failure surfaced", err)
	}
	for _, want := range []string{deepest, filepath.Join(base, "a"), base} {
		found := false
		for _, c := range calls {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("SyncDir not attempted for %q after an earlier failure; calls = %v", want, calls)
		}
	}
}
