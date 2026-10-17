package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// writeFileWithMtime writes content to p (creating it) and stamps its mtime to the given
// Unix-seconds value, so a test controls the (size, mtime) pair the cheap scan compares.
func writeFileWithMtime(t *testing.T, p string, content []byte, mtime int64) {
	t.Helper()
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := os.Chtimes(p, time.Unix(mtime, 0), time.Unix(mtime, 0)); err != nil {
		t.Fatalf("chtimes %s: %v", p, err)
	}
}

// putFileBaseline records a synced regular-file baseline carrying the cheap-scan facts
// (hash/size/mtime) the loadScanHints map is built from.
func putFileBaseline(t *testing.T, st *store.Store, viewID uint64, path, hash string, size, mtime int64) {
	t.Helper()
	b := store.Baseline{
		FileType:   "file",
		LocalHash:  hash,
		LocalSize:  size,
		LocalMTime: mtime,
		State:      store.StateSynced,
	}
	if err := st.PutBaseline(viewID, path, b); err != nil {
		t.Fatalf("PutBaseline %q: %v", path, err)
	}
}

// TestScanViewCheapReusesHashOnMatch proves the cheap path does NOT re-hash a file whose
// on-disk (size, mtime) match its baseline hint: the on-disk content is swapped for different
// bytes of the SAME length with the SAME mtime, yet the scan returns the stale baseline hash
// (it never read the new content). A nil-hints scan re-hashes and returns the real content.
func TestScanViewCheapReusesHashOnMatch(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_000
	original := []byte("original") // 8 bytes
	swapped := []byte("modIfied")  // also 8 bytes
	if len(original) != len(swapped) {
		t.Fatalf("test bug: contents must be equal length")
	}
	baselineHash := md4.ContentHash(original)

	p := filepath.Join(viewDir, "a.txt")
	// Record the baseline for the ORIGINAL content, then place the SWAPPED content on disk with
	// the same size and mtime. A cheap scan must trust the hint and not notice the swap.
	putFileBaseline(t, e.store, viewID, "a.txt", baselineHash, int64(len(original)), mtime)
	writeFileWithMtime(t, p, swapped, mtime)

	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView cheap: %v", err)
	}
	en, ok := findEntry(entries, "a.txt")
	if !ok {
		t.Fatalf("a.txt not scanned")
	}
	if en.md4 != baselineHash {
		t.Errorf("cheap scan md4 = %q, want reused baseline hash %q (file must NOT be re-hashed)", en.md4, baselineHash)
	}

	// A full re-hash (hints == nil) reads the real on-disk bytes.
	full, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView full: %v", err)
	}
	fen, _ := findEntry(full, "a.txt")
	if fen.md4 != md4.ContentHash(swapped) {
		t.Errorf("full scan md4 = %q, want on-disk content hash %q", fen.md4, md4.ContentHash(swapped))
	}
}

// TestScanViewCheapReHashesOnSizeChange: a changed size defeats the short-circuit, so the
// scan re-hashes and returns the actual on-disk content hash.
func TestScanViewCheapReHashesOnSizeChange(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_100
	original := []byte("original")
	baselineHash := md4.ContentHash(original)
	disk := []byte("original content is now longer") // different length

	p := filepath.Join(viewDir, "a.txt")
	putFileBaseline(t, e.store, viewID, "a.txt", baselineHash, int64(len(original)), mtime)
	writeFileWithMtime(t, p, disk, mtime) // same mtime, different size

	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, _ := findEntry(entries, "a.txt")
	if en.md4 != md4.ContentHash(disk) {
		t.Errorf("size-changed cheap scan md4 = %q, want re-hashed %q", en.md4, md4.ContentHash(disk))
	}
}

// TestScanViewCheapReHashesOnMtimeChange: a changed mtime (even with the same size) defeats
// the short-circuit, so the scan re-hashes.
func TestScanViewCheapReHashesOnMtimeChange(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const baseMtime = 1_700_000_200
	original := []byte("original")
	baselineHash := md4.ContentHash(original)
	disk := []byte("modIfied") // same length, different content

	p := filepath.Join(viewDir, "a.txt")
	putFileBaseline(t, e.store, viewID, "a.txt", baselineHash, int64(len(original)), baseMtime)
	writeFileWithMtime(t, p, disk, baseMtime+50) // newer mtime

	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, _ := findEntry(entries, "a.txt")
	if en.md4 != md4.ContentHash(disk) {
		t.Errorf("mtime-changed cheap scan md4 = %q, want re-hashed %q", en.md4, md4.ContentHash(disk))
	}
}

// TestScanViewCheapHashesNewFile: a file with no baseline (no hint) is always hashed.
func TestScanViewCheapHashesNewFile(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	disk := []byte("brand new file")
	writeFileWithMtime(t, filepath.Join(viewDir, "new.txt"), disk, 1_700_000_300)

	hints, err := e.loadScanHints(viewID) // empty: no baselines
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, ok := findEntry(entries, "new.txt")
	if !ok {
		t.Fatalf("new.txt not scanned")
	}
	if en.md4 != md4.ContentHash(disk) {
		t.Errorf("new-file cheap scan md4 = %q, want %q", en.md4, md4.ContentHash(disk))
	}
}

// TestScanViewNilHintsAlwaysHashes: hints == nil forces a full re-hash even when a matching
// baseline exists, so a swapped same-size+mtime file is detected.
func TestScanViewNilHintsAlwaysHashes(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_400
	original := []byte("original")
	swapped := []byte("modIfied")
	baselineHash := md4.ContentHash(original)

	p := filepath.Join(viewDir, "a.txt")
	putFileBaseline(t, e.store, viewID, "a.txt", baselineHash, int64(len(original)), mtime)
	writeFileWithMtime(t, p, swapped, mtime)

	entries, _, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, _ := findEntry(entries, "a.txt")
	if en.md4 != md4.ContentHash(swapped) {
		t.Errorf("nil-hints scan md4 = %q, want on-disk %q (must re-hash)", en.md4, md4.ContentHash(swapped))
	}
}

// TestScanViewCheapCaseFoldMatch: on a case-insensitive filesystem the hint keyed under a
// baseline path that differs only in case still matches the on-disk entry by its fold key, so
// the cheap path reuses the hash rather than re-hashing.
func TestScanViewCheapCaseFoldMatch(t *testing.T) {
	restore := normpath.SetCaseInsensitive(true)
	defer restore()

	e, viewID, viewDir := newScanEngine(t)

	const mtime = 1_700_000_500
	original := []byte("original")
	swapped := []byte("modIfied")
	baselineHash := md4.ContentHash(original)

	// Baseline recorded under "A.txt"; on-disk file is "a.txt" with swapped content.
	putFileBaseline(t, e.store, viewID, "A.txt", baselineHash, int64(len(original)), mtime)
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), swapped, mtime)

	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, ok := findEntry(entries, "a.txt")
	if !ok {
		t.Fatalf("a.txt not scanned")
	}
	if en.md4 != baselineHash {
		t.Errorf("case-fold cheap scan md4 = %q, want reused baseline hash %q", en.md4, baselineHash)
	}
}

// TestLoadScanHintsSkipsDirsAndUnhashed: directories and baselines without a recorded hash
// carry no hint, so files at those paths fall through to a full hash.
func TestLoadScanHintsSkipsDirsAndUnhashed(t *testing.T) {
	e, viewID, _ := newScanEngine(t)

	if err := e.store.PutBaseline(viewID, "adir", store.Baseline{FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline dir: %v", err)
	}
	if err := e.store.PutBaseline(viewID, "unhashed.txt", store.Baseline{FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("PutBaseline unhashed: %v", err)
	}
	putFileBaseline(t, e.store, viewID, "hashed.txt", "deadbeef", 4, 1_700_000_600)

	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	if _, ok := hints[normpath.Key("adir")]; ok {
		t.Error("directory baseline produced a hint")
	}
	if _, ok := hints[normpath.Key("unhashed.txt")]; ok {
		t.Error("baseline without a hash produced a hint")
	}
	if h, ok := hints[normpath.Key("hashed.txt")]; !ok || h.hash != "deadbeef" {
		t.Errorf("hashed.txt hint = %+v ok=%v, want hash deadbeef", h, ok)
	}
}
