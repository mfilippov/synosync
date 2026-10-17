//go:build !windows

package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOpenUploadSourceRefusesInRootCrossViewSymlink is the regression for a cross-view escape: an
// intermediate path component swapped for a RELATIVE symlink into a sibling view stays
// within the sync root, so os.Root would follow it and upload the other view's bytes under
// this view's path. The per-component openat(O_NOFOLLOW) walk must refuse it instead,
// surfacing the source as gone (so the next scan reconciles) and never disclosing the
// sibling view's content.
func TestOpenUploadSourceRefusesInRootCrossViewSymlink(t *testing.T) {
	root := t.TempDir()
	photos := filepath.Join(root, "Photos")
	other := filepath.Join(root, "Other")
	if err := os.MkdirAll(photos, 0o755); err != nil {
		t.Fatalf("mkdir Photos: %v", err)
	}
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatalf("mkdir Other: %v", err)
	}
	const secret = "OTHER-VIEW-SECRET"
	if err := os.WriteFile(filepath.Join(other, "secret.txt"), []byte(secret), 0o644); err != nil {
		t.Fatalf("seed secret: %v", err)
	}
	// Photos/sub -> ../Other : a relative symlink whose target stays inside the sync root.
	if err := os.Symlink("../Other", filepath.Join(photos, "sub")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Sanity check the threat is real: os.Root (the prior approach) follows this in-root link.
	if r, err := os.OpenRoot(root); err == nil {
		if f, oerr := r.OpenFile("Photos/sub/secret.txt", os.O_RDONLY, 0); oerr == nil {
			b, _ := io.ReadAll(f)
			_ = f.Close()
			if string(b) == secret {
				t.Logf("confirmed: os.Root follows the in-root cross-view symlink (this is what we must not do)")
			}
		}
		_ = r.Close()
	}

	e := New(nil, nil, root, "sess", nil)
	abs := filepath.Join(photos, "sub", "secret.txt")
	f, _, err := e.openUploadSource(abs)
	if f != nil {
		_ = f.Close()
		t.Fatalf("openUploadSource returned a descriptor across an in-root cross-view symlink")
	}
	if !errors.Is(err, errUploadSourceGone) {
		t.Fatalf("openUploadSource err = %v, want errUploadSourceGone", err)
	}

	// And the low-level opener refuses outright, never returning a descriptor on the secret.
	if lf, oerr := openUploadFileNoFollow(root, filepath.Join("Photos", "sub", "secret.txt")); oerr == nil {
		got, _ := io.ReadAll(lf)
		_ = lf.Close()
		t.Fatalf("openUploadFileNoFollow followed the cross-view symlink, read %q", got)
	}
}

// TestOpenUploadSourceFinalComponentSymlinkRefused covers a final-component symlink (the
// leaf swapped for a link), which must also be refused rather than followed.
func TestOpenUploadSourceFinalComponentSymlinkRefused(t *testing.T) {
	root := t.TempDir()
	view := filepath.Join(root, "V")
	if err := os.MkdirAll(view, 0o755); err != nil {
		t.Fatalf("mkdir view: %v", err)
	}
	if err := os.WriteFile(filepath.Join(view, "real.txt"), []byte("inside"), 0o644); err != nil {
		t.Fatalf("seed real: %v", err)
	}
	if err := os.Symlink("real.txt", filepath.Join(view, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	e := New(nil, nil, root, "sess", nil)
	f, _, err := e.openUploadSource(filepath.Join(view, "link.txt"))
	if f != nil {
		_ = f.Close()
		t.Fatalf("openUploadSource followed a final-component symlink")
	}
	if !errors.Is(err, errUploadSourceGone) {
		t.Fatalf("openUploadSource err = %v, want errUploadSourceGone", err)
	}
}

// newReplacedNodeEngine is newScanEngine with a Warn-capturing logger, for the
// replaced-tracked-path scenarios below.
func newReplacedNodeEngine(t *testing.T) (*Engine, uint64, string, *bytes.Buffer) {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	var buf bytes.Buffer
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	return e, viewID, viewDir, &buf
}

// assertTrackedReplacementProtected runs the shared assertions for a tracked path replaced by
// a non-syncable node type: the scan does not surface it as an entry but records it in the
// unsurfaced set, local reconcile emits NO ops (in particular no OpRemove that would delete
// the still-present remote file), the baseline stays StateSynced, and a Warn was logged.
func assertTrackedReplacementProtected(t *testing.T, e *Engine, viewID uint64, path string, buf *bytes.Buffer) {
	t.Helper()
	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, path); ok {
		t.Errorf("non-regular node at %q was surfaced as a scan entry", path)
	}
	if !unreadable[path] {
		t.Errorf("tracked path %q replaced by a non-syncable node was not recorded in the unsurfaced set; its baseline would diff as a local deletion", path)
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted OpRemove %d/%q for a tracked path replaced by a non-syncable node; this deletes the still-present remote file", op.ViewID, op.Path)
		}
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
	if b, err := e.store.GetBaseline(viewID, path); err != nil {
		t.Fatalf("GetBaseline %q: %v", path, err)
	} else if b.State != store.StateSynced {
		t.Errorf("baseline state for %q = %q, want synced (untouched)", path, b.State)
	}
	if !strings.Contains(buf.String(), "replaced by a non-syncable node") {
		t.Errorf("no Warn logged for the tracked replacement; log: %s", buf.String())
	}
}

// TestScanTrackedFileReplacedBySymlinkProtected is the replaced-tracked-path regression: a tracked
// file replaced on disk by a symlink must NOT be read as a local deletion. Before the fix the
// scanner silently skipped the symlink without recording it in the unsurfaced set, so
// detectRemoves diffed the baseline as vanished and pushed an OpRemove deleting the remote
// file. Policy: protect - the path is left unsynced (Warn each scan) until the user resolves.
func TestScanTrackedFileReplacedBySymlinkProtected(t *testing.T) {
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	doc := []byte("original doc content")
	putSynced(t, e, viewID, "doc.txt", "fd", "file", md4.ContentHash(doc), int64(len(doc)))
	// The tracked file is replaced by a (dangling) symlink - the walk lstats it as a symlink.
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "doc.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	assertTrackedReplacementProtected(t, e, viewID, "doc.txt", buf)
}

// TestScanTrackedFileReplacedByFifoProtected is the same scenario with a named pipe (the
// non-regular default branch rather than the symlink branch): a FIFO swapped onto a tracked
// path must be protected, not read as a local deletion.
func TestScanTrackedFileReplacedByFifoProtected(t *testing.T) {
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	doc := []byte("original doc content")
	putSynced(t, e, viewID, "doc.txt", "fd", "file", md4.ContentHash(doc), int64(len(doc)))
	if err := syscall.Mkfifo(filepath.Join(viewDir, "doc.txt"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	assertTrackedReplacementProtected(t, e, viewID, "doc.txt", buf)
}

// TestScanTrackedDirReplacedBySymlinkProtectsSubtree confirms a tracked DIRECTORY replaced by
// a symlink protects its whole subtree by prefix: neither the directory baseline nor its
// child's baseline diffs as a deletion (the walk cannot see the children at all, so only the
// directory's own unsurfaced entry shields them).
func TestScanTrackedDirReplacedBySymlinkProtectsSubtree(t *testing.T) {
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	child := []byte("child content")
	putSynced(t, e, viewID, "d", "fdir", "dir", "", 0)
	putSynced(t, e, viewID, "d/child.txt", "fc", "file", md4.ContentHash(child), int64(len(child)))
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "d")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops for a symlinked tracked directory, want 0 (subtree protected): %+v", len(ops), ops)
	}
	for _, p := range []string{"d", "d/child.txt"} {
		if b, err := e.store.GetBaseline(viewID, p); err != nil {
			t.Fatalf("GetBaseline %q: %v", p, err)
		} else if b.State != store.StateSynced {
			t.Errorf("baseline state for %q = %q, want synced (untouched)", p, b.State)
		}
	}
	if !strings.Contains(buf.String(), "replaced by a non-syncable node") {
		t.Errorf("no Warn logged for the tracked directory replacement; log: %s", buf.String())
	}
}

// TestScanTrackedFileReplacedByCaseVariantSymlinkProtected is the case-variant
// regression: on a case-insensitive filesystem a tracked file ("Doc.txt") deleted and
// replaced by a symlink created under a case-variant spelling ("doc.txt") occupies the SAME
// logical slot, but protectReplacedTracked's exact-key baseline lookup missed the stored
// spelling and read the node as an untracked stray - the merge-join then diffed the
// base-only "Doc.txt" row as vanished and pushed an OpRemove deleting the still-present
// remote file. The fold-key fallback must protect the slot under either spelling.
func TestScanTrackedFileReplacedByCaseVariantSymlinkProtected(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	doc := []byte("original doc content")
	putSynced(t, e, viewID, "Doc.txt", "fd", "file", md4.ContentHash(doc), int64(len(doc)))
	// The tracked file is gone from disk; the symlink claims the same slot under another case.
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "doc.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if _, ok := findEntry(entries, "doc.txt"); ok {
		t.Errorf("case-variant symlink was surfaced as a scan entry")
	}
	if !unreadable["doc.txt"] {
		t.Errorf("case-variant replacement was not recorded in the unsurfaced set; the tracked baseline would diff as a local deletion")
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			t.Fatalf("localReconcile emitted OpRemove %d/%q for a tracked slot replaced by a case-variant symlink; this deletes the still-present remote file", op.ViewID, op.Path)
		}
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
	if b, err := e.store.GetBaseline(viewID, "Doc.txt"); err != nil {
		t.Fatalf("GetBaseline Doc.txt: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced (untouched)", b.State)
	}
	if !strings.Contains(buf.String(), "replaced by a non-syncable node") {
		t.Errorf("no Warn logged for the tracked replacement; log: %s", buf.String())
	}
}

// TestScanTrackedFileReplacedByCaseVariantFifoProtected exercises the same fold-key
// fallback through the non-regular default branch (a FIFO rather than a symlink).
func TestScanTrackedFileReplacedByCaseVariantFifoProtected(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	doc := []byte("original doc content")
	putSynced(t, e, viewID, "Doc.txt", "fd", "file", md4.ContentHash(doc), int64(len(doc)))
	if err := syscall.Mkfifo(filepath.Join(viewDir, "doc.txt"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	_, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if !unreadable["doc.txt"] {
		t.Errorf("case-variant FIFO replacement was not recorded in the unsurfaced set")
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "replaced by a non-syncable node") {
		t.Errorf("no Warn logged for the tracked replacement; log: %s", buf.String())
	}
}

// TestScanTrackedFileReplacedByNFDSymlinkProtected pins the Unicode-form sibling of the
// case-variant fallback: the symlink replacing tracked NFC "café.txt" is created under the
// NFD spelling on a byte-preserving filesystem. scanView NFC-normalizes every observed path
// before the baseline lookup, so the exact-key path already covers this - the test guards
// that behaviour against regression by the fold-fallback change.
func TestScanTrackedFileReplacedByNFDSymlinkProtected(t *testing.T) {
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	const nfc = "caf\u00e9.txt"  // é precomposed (NFC)
	const nfd = "cafe\u0301.txt" // e + combining acute (NFD)
	doc := []byte("original doc content")
	putSynced(t, e, viewID, nfc, "fd", "file", md4.ContentHash(doc), int64(len(doc)))
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, nfd)); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	assertTrackedReplacementProtected(t, e, viewID, nfc, buf)
}

// TestScanReplacedTrackedFoldScanErrorProtects covers the unverifiable branch of the
// fold-key fallback: the exact lookup misses and the baseline stream itself fails, so the
// scanner cannot prove the node untracked - it must protect conservatively (a remote delete
// must never ride on an unverifiable "untracked" claim) and Warn.
func TestScanReplacedTrackedFoldScanErrorProtects(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	e.scanBaselines = func(uint64, func(store.PathBaseline) error) ([]string, error) {
		return nil, errors.New("stream failure")
	}
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "doc.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if !unreadable["doc.txt"] {
		t.Errorf("unverifiable replacement was not protected; a spurious remote delete could ride on the store failure")
	}
	if !strings.Contains(buf.String(), "could not check whether a non-syncable node replaced a tracked path") {
		t.Errorf("no conservative-protect Warn logged; log: %s", buf.String())
	}
}

// TestScanUntrackedCaseVariantSymlinkStillIgnored is the negative side of the fold
// fallback: under case-insensitive matching a stray symlink whose folded key matches NO
// stored baseline stays simply ignored - not protected, no Warn - so the fallback never
// over-protects genuinely untracked strays.
func TestScanUntrackedCaseVariantSymlinkStillIgnored(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "Stray")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if unreadable["Stray"] {
		t.Errorf("untracked case-variant symlink was recorded in the unsurfaced set; it should be simply ignored")
	}
	if s := buf.String(); strings.Contains(s, "non-syncable node") {
		t.Errorf("a Warn was logged for an untracked symlink; log: %s", s)
	}
}

// TestScanUntrackedSymlinkStillIgnored is the negative side of the policy: a stray symlink
// (and a stray FIFO) with NO baseline stays simply ignored - not surfaced, not protected, no
// Warn - preserving the long-standing behaviour for symlinks that were never synced content.
func TestScanUntrackedSymlinkStillIgnored(t *testing.T) {
	e, viewID, viewDir, buf := newReplacedNodeEngine(t)
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "stray")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := syscall.Mkfifo(filepath.Join(viewDir, "straypipe"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	entries, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	for _, p := range []string{"stray", "straypipe"} {
		if _, ok := findEntry(entries, p); ok {
			t.Errorf("untracked non-regular node %q was surfaced as a scan entry", p)
		}
		if unreadable[p] {
			t.Errorf("untracked non-regular node %q was recorded in the unsurfaced set; it should be simply ignored", p)
		}
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
	if s := buf.String(); strings.Contains(s, "non-syncable node") {
		t.Errorf("a Warn was logged for an untracked symlink/FIFO; log: %s", s)
	}
}

// TestOpenUploadSourceReadsLegitNestedFile confirms the no-follow walk still opens an
// ordinary nested file and reads its exact bytes (the happy path the upload relies on).
func TestOpenUploadSourceReadsLegitNestedFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "V", "a", "b")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	const content = "nested-content"
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte(content), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	e := New(nil, nil, root, "sess", nil)
	f, fi, err := e.openUploadSource(filepath.Join(dir, "f.txt"))
	if err != nil {
		t.Fatalf("openUploadSource legit file: %v", err)
	}
	defer f.Close()
	if !fi.Mode().IsRegular() || fi.Size() != int64(len(content)) {
		t.Fatalf("fileinfo = %+v, want regular size %d", fi, len(content))
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != content {
		t.Fatalf("read %q, want %q", got, content)
	}
}
