package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/text/unicode/norm"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// The Phase-9b regression concerns a byte-preserving filesystem (Linux, Windows) holding a file
// under its decomposed (NFD) name while the store and the queued op carry the canonical NFC
// form the scanner normalized to. nfcName/nfdName (defined in normpath_boundary_test.go) are
// the precomposed/decomposed forms of "café.txt"; the dir forms below are derived via
// norm.NFC/norm.NFD so their source bytes are guaranteed to differ regardless of editor
// normalization.
var (
	nfcDir = norm.NFC.String("über") // precomposed ü
	nfdDir = norm.NFD.String("über") // decomposed: u + combining diaeresis
)

// TestOutboundUploadResolvesNFDSource models the Phase-9b regression: a local file created with
// decomposed (NFD) bytes whose op carries the canonical NFC path. On a byte-preserving
// filesystem the literal NFC path does not exist on disk, so without the NFC-tolerant
// resolution openUploadSource would report errUploadSourceGone and the create would never
// upload. The upload must instead read the real on-disk bytes and push them under the NFC name.
func TestOutboundUploadResolvesNFDSource(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	c := &pushClient{uploadNode: map[string]actions.Node{nfcName: {FileID: "newf", Hash: "h1", MTime: 123}}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Write the file under its decomposed (NFD) name; the op carries the NFC canonical path.
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	if err := st.PutBaseline(7, nfcName, store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: nfcName,
		LocalHash: "h1", LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (NFD source must resolve to the real on-disk file)", len(c.uploads))
	}
	u := c.uploads[0]
	if u.name != nfcName || string(u.content) != "hello" {
		t.Errorf("upload call = name %q content %q, want name %q content %q", u.name, u.content, nfcName, "hello")
	}
	b, err := st.GetBaseline(7, nfcName)
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "newf" || b.LocalHash != md4.ContentHash([]byte("hello")) {
		t.Errorf("baseline = %+v, want synced newf with the uploaded content hash", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundUploadResolvesNFDDirComponent exercises the same resolution when an intermediate
// directory component (not the leaf) is the one stored byte-for-byte in NFD form.
func TestOutboundUploadResolvesNFDDirComponent(t *testing.T) {
	if nfcDir == nfdDir {
		t.Fatalf("test setup: NFC %q and NFD %q dir names must differ", nfcDir, nfdDir)
	}
	c := &pushClient{uploadNode: map[string]actions.Node{"a.txt": {FileID: "newf", Hash: "h1", MTime: 123}}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, nfdDir), 0o755); err != nil {
		t.Fatalf("seed NFD dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, nfdDir, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The op addresses the file through the NFC directory name. Seed the parent dir's synced
	// baseline so resolveParentID finds its server id.
	if err := st.PutBaseline(7, nfcDir, store.Baseline{
		FileType: "dir", FileID: "dir1", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	nfcPath := nfcDir + "/a.txt"
	if err := st.PutBaseline(7, nfcPath, store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 2, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: nfcPath,
		LocalHash: "h1", LocalSize: 2, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (NFD dir component must resolve)", len(c.uploads))
	}
	if string(c.uploads[0].content) != "hi" {
		t.Errorf("uploaded content = %q, want %q", c.uploads[0].content, "hi")
	}
}

// TestOpenUploadSourceNFDSwappedForSymlink models a symlink-swap regression: a local file
// created with decomposed (NFD) bytes whose op carries the canonical NFC path is replaced by a
// symlink in the scan→apply window. The NFC-tolerant fallback resolves the real on-disk (NFD)
// name and the no-follow open then fails closed on the symlink. The symlink classification must
// inspect that real path - not the canonical NFC abs, which does not exist on disk - so the
// failure maps to errUploadSourceGone (reconciled next scan) rather than a hard error that
// wedges the op forever.
func TestOpenUploadSourceNFDSwappedForSymlink(t *testing.T) {
	requireSymlinks(t)
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	_, _, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	e := &Engine{syncRoot: filepath.Dir(viewDir), readDir: os.ReadDir}

	// Place a symlink under the decomposed (NFD) name. The literal NFC path does not exist, so
	// the fallback must resolve to this NFD entry, and the no-follow open must then classify the
	// symlink against the real path.
	if err := os.Symlink(filepath.Join(viewDir, "elsewhere"), filepath.Join(viewDir, nfdName)); err != nil {
		t.Fatalf("seed NFD symlink: %v", err)
	}

	nfcAbs := filepath.Join(viewDir, nfcName)
	_, _, err := e.openUploadSource(nfcAbs)
	if !errors.Is(err, errUploadSourceGone) {
		t.Fatalf("openUploadSource(%q) err = %v, want errUploadSourceGone", nfcAbs, err)
	}
}

// TestResolveNFCPathAbsentSource confirms a genuinely missing source still resolves to false
// (no spurious match) so the op is reported gone rather than guessed at.
func TestResolveNFCPathAbsentSource(t *testing.T) {
	_, _, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	e := &Engine{syncRoot: filepath.Dir(viewDir), readDir: os.ReadDir}
	if _, ok := e.resolveNFCPath(filepath.Join("Photos", "nope.txt")); ok {
		t.Error("resolveNFCPath matched a nonexistent file")
	}
}

// TestOnDiskAbs covers the conflict-path resolver: it maps an NFC canonical absolute path to
// the real on-disk NFD path, and returns the input unchanged when the literal path exists or
// nothing matches.
func TestOnDiskAbs(t *testing.T) {
	_, _, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	e := &Engine{syncRoot: filepath.Dir(viewDir), readDir: os.ReadDir}

	if err := os.WriteFile(filepath.Join(viewDir, nfdName), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	t.Run("nfd spelling", func(t *testing.T) {
		nfcAbs := filepath.Join(viewDir, nfcName)
		// Same probe as TestResolveNFCComponentsNFDFallbackUnchanged: a
		// normalization-INSENSITIVE filesystem (case-insensitive APFS - a stock macOS
		// volume) resolves the literal NFC path onto the NFD file, so onDiskAbs' Lstat
		// fast path returns the input unchanged and there is no rewritten spelling to
		// assert. Probed, not branched on GOOS: a case-SENSITIVE APFS volume is
		// normalization-sensitive and does exercise the fallback.
		if _, err := os.Lstat(nfcAbs); err == nil {
			t.Skip("filesystem normalizes names; the NFD fallback cannot be exercised here")
		}
		got := e.onDiskAbs(nfcAbs)
		want := filepath.Join(viewDir, nfdName)
		if got != want {
			t.Errorf("onDiskAbs(%q) = %q, want the real NFD path %q", nfcAbs, got, want)
		}
	})

	// A path that exists literally is returned unchanged.
	plain := filepath.Join(viewDir, "plain.txt")
	if err := os.WriteFile(plain, []byte("y"), 0o644); err != nil {
		t.Fatalf("seed plain file: %v", err)
	}
	if got := e.onDiskAbs(plain); got != plain {
		t.Errorf("onDiskAbs(%q) = %q, want unchanged", plain, got)
	}

	// A genuinely absent path with no normalize-equivalent is returned unchanged.
	missing := filepath.Join(viewDir, "gone.txt")
	if got := e.onDiskAbs(missing); got != missing {
		t.Errorf("onDiskAbs(%q) = %q, want unchanged", missing, got)
	}
}
