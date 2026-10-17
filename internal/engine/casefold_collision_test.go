package engine

// NFD/NFC fold-key collision on a case-sensitive byte-preserving filesystem.
// Two distinct on-disk names that are the NFD and NFC encodings of the same string fold to one
// normpath.Key, so the reconcile merge-join would collapse them (last-wins) and silently leave the
// other file unsynced. The minimal safe fix detects the collision, logs a Warn, and leaves BOTH
// files untouched (never silently clobbered/dropped) until one is renamed. These tests force the
// case-sensitive fold branch via normpath.SetCaseInsensitive(false) so they exercise the guard
// regardless of host GOOS. The integration tests additionally need a byte-preserving host FS to
// hold both names; they skip if the host coalesced them (e.g. macOS APFS).

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

const (
	nameNFC = "café.txt"  // é precomposed (U+00E9)
	nameNFD = "café.txt" // e + combining acute (U+0301)
)

// TestScanKeyCollisions is a pure unit test of the detector: two scan entries whose relPaths
// fold to the same normpath.Key on a case-sensitive FS are reported as one collision with two
// paths, while distinct keys (and an empty set) yield no collisions.
func TestScanKeyCollisions(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()

	t.Run("collision detected", func(t *testing.T) {
		entries := []scanEntry{
			{relPath: nameNFC, md4: "a"},
			{relPath: nameNFD, md4: "b"},
			{relPath: "other.txt", md4: "c"},
		}
		got := scanKeyCollisions(entries)
		if len(got) != 1 {
			t.Fatalf("got %d collisions, want 1: %+v", len(got), got)
		}
		key := normpath.Key(nameNFC)
		paths, ok := got[key]
		if !ok {
			t.Fatalf("collision not keyed by %q: %+v", key, got)
		}
		if len(paths) != 2 {
			t.Fatalf("got %d colliding paths, want 2: %+v", len(paths), paths)
		}
	})

	t.Run("no collision", func(t *testing.T) {
		entries := []scanEntry{
			{relPath: "a.txt"},
			{relPath: "b.txt"},
		}
		if got := scanKeyCollisions(entries); got != nil {
			t.Fatalf("got %+v, want nil (no collisions)", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := scanKeyCollisions(nil); got != nil {
			t.Fatalf("got %+v, want nil", got)
		}
	})
}

// distinctOnDisk writes both names and reports whether the host filesystem kept them as two
// distinct directory entries (a byte-preserving case-sensitive FS). A normalizing FS (macOS APFS)
// coalesces them, so the integration scenario cannot be reproduced there.
func distinctOnDisk(t *testing.T, viewDir string) bool {
	t.Helper()
	writeFile(t, viewDir, nameNFC, []byte("one"))
	writeFile(t, viewDir, nameNFD, []byte("two"))
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "caf") {
			n++
		}
	}
	return n == 2
}

// warnLogger returns an engine logger whose Warn output is captured in buf.
func warnLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// TestLocalReconcileNFDNFCCollisionLocalOnly: two local-only files (no baseline) colliding by
// NFC key produce NO ops (without the fix exactly one would upload, silently dropping the other)
// and a Warn is logged. Both on-disk files are left in place.
func TestLocalReconcileNFDNFCCollisionLocalOnly(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (collision must leave both files unsynced): %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "Unicode path collision") {
		t.Fatalf("expected a collision Warn, got log: %q", buf.String())
	}
	// Both files still on disk.
	for _, n := range []string{nameNFC, nameNFD} {
		if _, err := os.Stat(filepath.Join(viewDir, n)); err != nil {
			t.Errorf("file %q should be untouched: %v", n, err)
		}
	}
}

// TestLocalReconcileNFDNFCCollisionWithBaseline: a synced baseline exists at the NFC key while two
// colliding files sit on disk. The collision must NOT produce an OpRemove (a spurious delete of the
// still-present node) - both files are left untouched and a Warn is logged.
func TestLocalReconcileNFDNFCCollisionWithBaseline(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	putSynced(t, e, viewID, nameNFC, "f1", "file", md4.ContentHash([]byte("one")), 3)
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0: %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "Unicode path collision") {
		t.Fatalf("expected a collision Warn, got log: %q", buf.String())
	}
	b, err := e.store.GetBaseline(viewID, nameNFC)
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want synced (untouched)", b.State)
	}
}

// TestLocalReconcileNFDNFCDirCollisionDropsDescendants: two directories whose names are the NFD and
// NFC encodings of one string collide at the directory fold key while their children fold to
// DISTINCT keys (café/a.txt vs café/b.txt). Without dropping the descendants, detectCreatesAndModifies
// would enqueue child OpUploads whose suppressed-parent OpMkdir left no parent baseline (wedging the
// queue on errParentUnknown) or merge children from two physical directories under one canonical
// remote path. The collision must therefore produce NO ops and leave every file untouched.
func TestLocalReconcileNFDNFCDirCollisionDropsDescendants(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	const (
		dirNFC = "café"  // é precomposed (U+00E9)
		dirNFD = "café" // e + combining acute (U+0301)
	)
	writeFile(t, viewDir, dirNFC+"/a.txt", []byte("one"))
	writeFile(t, viewDir, dirNFD+"/b.txt", []byte("two"))
	// Skip on a normalizing FS (e.g. macOS APFS) that coalesced the two directory names.
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, en := range ents {
		if strings.HasPrefix(en.Name(), "caf") {
			n++
		}
	}
	if n != 2 {
		t.Skip("host filesystem coalesced NFD/NFC directory names; collision scenario not reproducible here")
	}
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (collided directory and its descendants must stay unsynced): %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "Unicode path collision") {
		t.Fatalf("expected a collision Warn, got log: %q", buf.String())
	}
	for _, rel := range []string{dirNFC + "/a.txt", dirNFD + "/b.txt"} {
		if _, err := os.Stat(filepath.Join(viewDir, rel)); err != nil {
			t.Errorf("file %q should be untouched: %v", rel, err)
		}
	}
}

// TestColdStartReconcileNFDNFCCollision: the cold-start merge pass leaves two colliding local-only
// files untouched (no OpUpload that would sync one and drop the other) and logs a Warn. Driven by
// calling coldStartReconcile directly with synthetic scan entries and an empty catalog.
func TestColdStartReconcileNFDNFCCollision(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, _ := newScanEngine(t)
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	local := []scanEntry{
		{relPath: nameNFC, md4: "h1", size: 3},
		{relPath: nameNFD, md4: "h2", size: 3},
	}
	serverNodes := map[string]actions.Event{}
	unreadable := map[string]bool{}

	ops, err := e.coldStartReconcile(viewID, serverNodes, local, unreadable, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (collision must leave both files unsynced): %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "Unicode path collision") {
		t.Fatalf("expected a collision Warn, got log: %q", buf.String())
	}
}

// TestCatalogKeyCollisions is a pure unit test of the catalog-side detector:
// two DISTINCT catalog paths folding to one normpath.Key are a collision, while
// a same-path re-report across pages and a same-node case-only rename (one shared non-empty
// file id at both spellings) keep the established last-wins.
func TestCatalogKeyCollisions(t *testing.T) {
	t.Run("distinct nodes at NFD/NFC paths", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		creates := []actions.Event{
			serverFile(nameNFD, "f1", "h1", 3),
			serverFile(nameNFC, "f2", "h2", 3),
			serverFile("other.txt", "f3", "h3", 3),
		}
		got := catalogKeyCollisions(creates)
		if len(got) != 1 {
			t.Fatalf("got %d collisions, want 1: %+v", len(got), got)
		}
		paths, ok := got[normpath.Key(nameNFC)]
		if !ok || len(paths) != 2 {
			t.Fatalf("collision = %+v, want 2 paths under key %q", got, normpath.Key(nameNFC))
		}
	})
	t.Run("same-path re-report is not a collision", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		creates := []actions.Event{
			serverFile("a.txt", "f1", "h1", 3),
			serverFile("a.txt", "f2", "h2", 4), // recreate after a delete: legit last-wins
		}
		if got := catalogKeyCollisions(creates); got != nil {
			t.Fatalf("got %+v, want nil (a single distinct path is never a collision)", got)
		}
	})
	t.Run("case-only rename of one node is not a collision", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		creates := []actions.Event{
			serverFile("a.txt", "f1", "h1", 3),
			serverFile("A.txt", "f1", "h1", 3), // same node re-reported at its renamed spelling
		}
		if got := catalogKeyCollisions(creates); got != nil {
			t.Fatalf("got %+v, want nil (one shared non-empty id is one node)", got)
		}
	})
	t.Run("distinct cases with distinct ids collide", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		creates := []actions.Event{
			serverFile("a.txt", "f1", "h1", 3),
			serverFile("A.txt", "f2", "h2", 3),
		}
		if got := catalogKeyCollisions(creates); len(got) != 1 {
			t.Fatalf("got %+v, want 1 collision", got)
		}
	})
	t.Run("ambiguous empty id stays a collision", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		creates := []actions.Event{
			serverFile("a.txt", "", "h1", 3),
			serverFile("A.txt", "", "h2", 3),
		}
		if got := catalogKeyCollisions(creates); len(got) != 1 {
			t.Fatalf("got %+v, want 1 collision (empty ids cannot prove one node)", got)
		}
	})
	t.Run("empty catalog", func(t *testing.T) {
		if got := catalogKeyCollisions(nil); got != nil {
			t.Fatalf("got %+v, want nil", got)
		}
	})
}

// coldStartCatalog drives a full coldStartView over a scripted single-page catalog against an
// empty on-disk view directory, returning the engine's captured Warn log and the enqueued ops.
func coldStartCatalog(t *testing.T, creates []actions.Event) (*bytes.Buffer, []store.QueuedOp, *store.Store) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "V"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	c := &catalogClient{tree: treeFromCatalog(creates)}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	if err := e.coldStartView(context.Background(), listViewOf(t, st, 7)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	if !listViewOf(t, st, 7).InitialSyncDone {
		t.Errorf("InitialSyncDone = false, want true (a collision must not wedge the cold start)")
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	return &buf, ops, st
}

// TestColdStartCatalogNFDNFCCollisionProtected drives coldStartView over a catalog holding two
// DISTINCT server nodes whose names are the NFD and NFC encodings of one string (a
// byte-preserving server can hold both). Before the fix the NFC serverNodes map collapsed them
// last-wins and cold start silently synced only one; now both are protected - no op, no
// baseline, a Warn naming them - while the unrelated node still syncs.
func TestColdStartCatalogNFDNFCCollisionProtected(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	buf, ops, st := coldStartCatalog(t, []actions.Event{
		serverFile(nameNFD, "f1", "h1", 3),
		serverFile(nameNFC, "f2", "h2", 3),
		serverFile("ok.txt", "f3", "h3", 2),
	})
	if len(ops) != 1 || ops[0].Path != "ok.txt" || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want only the ok.txt download (colliding nodes protected)", ops)
	}
	if !strings.Contains(buf.String(), "catalog fold-key collision") {
		t.Fatalf("expected a catalog collision Warn, got log: %q", buf.String())
	}
	for _, p := range []string{nameNFC, nameNFD} {
		if _, err := st.GetBaseline(7, p); !isNotFound(err) {
			t.Errorf("colliding path %q must have no baseline, got err %v", p, err)
		}
	}
}

// TestColdStartCatalogCaseCollisionProtected is the case-fold variant: on a case-insensitive
// client two distinct server nodes differing only in case fold to one key in the merge-join
// (the server, case-sensitive, holds both). Both are protected rather than last-wins.
func TestColdStartCatalogCaseCollisionProtected(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	buf, ops, st := coldStartCatalog(t, []actions.Event{
		serverFile("clash.txt", "f1", "h1", 3),
		serverFile("Clash.txt", "f2", "h2", 3),
		serverFile("ok.txt", "f3", "h3", 2),
	})
	if len(ops) != 1 || ops[0].Path != "ok.txt" || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want only the ok.txt download (case-colliding nodes protected)", ops)
	}
	if !strings.Contains(buf.String(), "catalog fold-key collision") {
		t.Fatalf("expected a catalog collision Warn, got log: %q", buf.String())
	}
	for _, p := range []string{"clash.txt", "Clash.txt"} {
		if _, err := st.GetBaseline(7, p); !isNotFound(err) {
			t.Errorf("colliding path %q must have no baseline, got err %v", p, err)
		}
	}
}

// TestColdStartCatalogCaseRenameSameNodeSyncs pins the exemption: one node reported at both
// case spellings in one walk (a case-only rename mid-walk, same file id) is NOT a collision -
// the planner keeps it syncing as one download, with no Warn, and the winner is
// deterministically the FIRST spelling the walk reported. The move to the other spelling is in
// the log above the cursor, so the incremental poll replays it onto that download.
func TestColdStartCatalogCaseRenameSameNodeSyncs(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	buf, ops, _ := coldStartCatalog(t, []actions.Event{
		serverFile("a.txt", "f1", "h1", 3),
		serverFile("A.txt", "f1", "h1", 3),
	})
	if len(ops) != 1 || ops[0].Type != store.OpDownload {
		t.Fatalf("ops = %+v, want exactly one download for the case-renamed node", ops)
	}
	if got := ops[0].Path; got != "a.txt" {
		t.Fatalf("download path = %q, want the first-reported spelling a.txt (deterministic first-wins)", got)
	}
	if strings.Contains(buf.String(), "collision") {
		t.Fatalf("unexpected collision Warn for a same-node case rename: %q", buf.String())
	}
}

// singleton names: U+212A KELVIN SIGN and plain ASCII K. NFC maps the first onto the second,
// so these two distinct on-disk names share one normpath.Key - a collision that has nothing to
// do with NFD/NFC composition and nothing to do with case folding. One of the two names is pure
// ASCII, which is the trap: nothing about "K.txt" suggests a Unicode hazard. The two constants
// look identical in an editor, so every test using them first asserts they differ - an editor or
// a tool that normalises this file breaks the tests loudly instead of making them vacuous.
const (
	nameSingletonKelvin = "K.txt" // U+212A KELVIN SIGN, one raw code point
	nameSingletonASCII  = "K.txt"
)

// TestLocalReconcileSingletonCollisionProtected proves the local guard's protection is keyed on
// normpath.Key rather than on anything NFD-specific: the Kelvin/ASCII-K pair collides on a
// case-SENSITIVE filesystem with case folding explicitly off, and both files must be left
// unsynced rather than one silently uploading over the other. Without a Key-based guard the
// merge-join's last-wins would pick one of the two and drop the other with no trace.
func TestLocalReconcileSingletonCollisionProtected(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	if nameSingletonKelvin == nameSingletonASCII {
		t.Fatal("test bug: the two spellings are identical; an editor normalised the source")
	}
	e, viewID, viewDir := newScanEngine(t)
	writeFile(t, viewDir, nameSingletonKelvin, []byte("kelvin"))
	writeFile(t, viewDir, nameSingletonASCII, []byte("ascii"))
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 2 {
		t.Skipf("host filesystem coalesced the singleton names (%d entries); scenario not reproducible here", len(ents))
	}
	var buf bytes.Buffer
	e.log = warnLogger(&buf)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (a singleton collision must leave both files unsynced): %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "path collision") {
		t.Fatalf("expected a collision Warn, got log: %q", buf.String())
	}
	for _, n := range []string{nameSingletonKelvin, nameSingletonASCII} {
		if _, err := os.Stat(filepath.Join(viewDir, n)); err != nil {
			t.Errorf("file %+q should be untouched: %v", n, err)
		}
	}
}

// TestCatalogKeyCollisionsSingleton is the server-side half: a catalog holding both spellings as
// two distinct nodes must be reported as one collision, so cold start protects them instead of
// letting the NFC-keyed serverNodes map collapse one onto the other.
func TestCatalogKeyCollisionsSingleton(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	if nameSingletonKelvin == nameSingletonASCII {
		t.Fatal("test bug: the two spellings are identical; an editor normalised the source")
	}
	creates := []actions.Event{
		{Path: nameSingletonKelvin, FileID: "id-kelvin"},
		{Path: nameSingletonASCII, FileID: "id-ascii"},
		{Path: "other.txt", FileID: "id-other"},
	}
	got := catalogKeyCollisions(creates)
	if len(got) != 1 {
		t.Fatalf("got %d collisions, want 1: %+v", len(got), got)
	}
	paths, ok := got[normpath.Key(nameSingletonASCII)]
	if !ok {
		t.Fatalf("collision not keyed by %+q: %+v", normpath.Key(nameSingletonASCII), got)
	}
	if len(paths) != 2 {
		t.Fatalf("got %d colliding paths, want 2: %+v", len(paths), paths)
	}
}
