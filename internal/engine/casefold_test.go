package engine

// Phase 9b: case-fold matching tests. On a case-insensitive filesystem
// (Windows/macOS) a local scan entry and a stored baseline that differ only in case name
// the same node, so the local-reconcile and cold-start diffs must NOT treat them as a
// spurious delete+create (or download+upload) pair. On a case-sensitive filesystem (Linux)
// the two are distinct nodes and the ordinary divergence applies. Each test flips the
// platform-fold behaviour via normpath.SetCaseInsensitive so both branches are exercised
// regardless of the host GOOS.

import (
	"context"
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestLocalReconcileCaseFoldUnchanged: disk "foo.txt" against a synced baseline "Foo.txt"
// with identical content produces NO op on a case-insensitive FS (same node, unchanged),
// while on a case-sensitive FS the identical content collapses into a single OpRename
// (a genuine case-rename move). Either way the baseline is never duplicated under both cases.
func TestLocalReconcileCaseFoldUnchanged(t *testing.T) {
	content := []byte("hello world")
	hash := md4.ContentHash(content)

	t.Run("case-insensitive: same node, no op", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, viewDir := newScanEngine(t)
		writeFile(t, viewDir, "foo.txt", content)
		putSynced(t, e, viewID, "Foo.txt", "f1", "file", hash, int64(len(content)))

		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile: %v", err)
		}
		if len(ops) != 0 {
			t.Fatalf("got %d ops, want 0 (case-only match, unchanged): %+v", len(ops), ops)
		}
		b, err := e.store.GetBaseline(viewID, "Foo.txt")
		if err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline state = %q, want synced (untouched)", b.State)
		}
		if _, err := e.store.GetBaseline(viewID, "foo.txt"); err == nil {
			t.Error("a second baseline was stranded under the scan's case foo.txt")
		}
	})

	t.Run("case-sensitive: distinct nodes, rename", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, viewID, viewDir := newScanEngine(t)
		writeFile(t, viewDir, "foo.txt", content)
		putSynced(t, e, viewID, "Foo.txt", "f1", "file", hash, int64(len(content)))

		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile: %v", err)
		}
		// Identical content under a different case is a move on a case-sensitive FS.
		if len(ops) != 1 || ops[0].Type != store.OpRename || ops[0].Path != "Foo.txt" || ops[0].NewPath != "foo.txt" {
			t.Fatalf("got %+v, want one OpRename Foo.txt -> foo.txt", ops)
		}
	})
}

// TestLocalReconcileCaseFoldModify: disk "foo.txt" with content diverged from the synced
// baseline "Foo.txt". On a case-insensitive FS this is a single in-place modify addressing
// the existing file id and recorded under the baseline's stored case, never a delete+create;
// on a case-sensitive FS it is the ordinary remove("Foo.txt") + upload("foo.txt") pair.
func TestLocalReconcileCaseFoldModify(t *testing.T) {
	oldContent := []byte("original")
	newContent := []byte("the user changed this")
	oldHash := md4.ContentHash(oldContent)
	newHash := md4.ContentHash(newContent)

	t.Run("case-insensitive: in-place modify", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, viewDir := newScanEngine(t)
		writeFile(t, viewDir, "foo.txt", newContent)
		putSynced(t, e, viewID, "Foo.txt", "f1", "file", oldHash, int64(len(oldContent)))

		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpUpload {
			t.Fatalf("got %+v, want one OpUpload (modify)", ops)
		}
		if ops[0].Path != "Foo.txt" {
			t.Errorf("op path = %q, want the baseline's stored case Foo.txt", ops[0].Path)
		}
		if ops[0].FileID != "f1" {
			t.Errorf("op fileID = %q, want the existing node id f1 (addressed by id, not a fresh create)", ops[0].FileID)
		}
		if ops[0].LocalHash != newHash {
			t.Errorf("op hash = %q, want the new content hash %q", ops[0].LocalHash, newHash)
		}
		b, err := e.store.GetBaseline(viewID, "Foo.txt")
		if err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		}
		if b.State != store.StatePending || b.LocalHash != newHash {
			t.Errorf("baseline = %+v, want pending with the new hash", b)
		}
		if _, err := e.store.GetBaseline(viewID, "foo.txt"); err == nil {
			t.Error("a second baseline was stranded under the scan's case foo.txt")
		}
	})

	t.Run("case-sensitive: remove + create", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, viewID, viewDir := newScanEngine(t)
		writeFile(t, viewDir, "foo.txt", newContent)
		putSynced(t, e, viewID, "Foo.txt", "f1", "file", oldHash, int64(len(oldContent)))

		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("localReconcile: %v", err)
		}
		types := map[string]store.OpType{}
		for _, op := range ops {
			types[op.Path] = op.Type
		}
		if len(ops) != 2 || types["foo.txt"] != store.OpUpload || types["Foo.txt"] != store.OpRemove {
			t.Fatalf("got %+v, want an OpUpload foo.txt and an OpRemove Foo.txt", ops)
		}
	})
}

// TestLocalReconcileCaseFoldNFDStable confirms an NFD-on-disk name against an NFC baseline
// of identical content never diffs as a change on either platform-fold setting (Unicode
// form is always folded; case folding is the only platform-dependent part).
func TestLocalReconcileCaseFoldNFDStable(t *testing.T) {
	const (
		nfc = "café.txt" // precomposed é
		nfd = "café.txt" // e + combining acute
	)
	content := []byte("stable")
	hash := md4.ContentHash(content)
	for _, ci := range []bool{true, false} {
		restore := normpath.SetCaseInsensitive(ci)
		e, viewID, viewDir := newScanEngine(t)
		writeFile(t, viewDir, nfd, content)
		putSynced(t, e, viewID, nfc, "f1", "file", hash, int64(len(content)))
		ops, err := e.localReconcile(context.Background(), viewID)
		restore()
		if err != nil {
			t.Fatalf("caseInsensitive=%v localReconcile: %v", ci, err)
		}
		if len(ops) != 0 {
			t.Fatalf("caseInsensitive=%v: got %d ops, want 0 (NFD vs NFC same node): %+v", ci, len(ops), ops)
		}
	}
}

// TestColdStartReconcileCaseFold drives the cold-start merge with the server catalog naming
// a node "Foo.txt" while disk holds "foo.txt". On a case-insensitive FS the two collapse to
// one merge entry - adopted (equal content) or kept-both (divergent) under the server's case
// - while on a case-sensitive FS they are two independent nodes (download + upload).
func TestColdStartReconcileCaseFold(t *testing.T) {
	content := []byte("shared content")
	hash := md4.ContentHash(content)
	other := md4.ContentHash([]byte("diverged content"))

	t.Run("case-insensitive equal: adopt, no transfer", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, _ := newScanEngine(t)
		server := catalogMap(serverFile("Foo.txt", "s1", hash, int64(len(content))))
		local := []scanEntry{localFile("foo.txt", hash, int64(len(content)))}
		ops, err := e.coldStartReconcile(viewID, server, local, nil, false)
		if err != nil {
			t.Fatalf("coldStartReconcile: %v", err)
		}
		if len(ops) != 0 {
			t.Fatalf("got %d ops, want 0 (case-only match adopted): %+v", len(ops), ops)
		}
		b, err := e.store.GetBaseline(viewID, "Foo.txt")
		if err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline state = %q, want synced (adopted)", b.State)
		}
		if _, err := e.store.GetBaseline(viewID, "foo.txt"); err == nil {
			t.Error("a second baseline was stranded under the local case foo.txt")
		}
	})

	t.Run("case-insensitive divergent: keep-both", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, _ := newScanEngine(t)
		server := catalogMap(serverFile("Foo.txt", "s1", hash, int64(len(content))))
		local := []scanEntry{localFile("foo.txt", other, int64(len(content)))}
		ops, err := e.coldStartReconcile(viewID, server, local, nil, false)
		if err != nil {
			t.Fatalf("coldStartReconcile: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpResolveConflict || ops[0].Path != "Foo.txt" {
			t.Fatalf("got %+v, want one OpResolveConflict at Foo.txt", ops)
		}
		b, err := e.store.GetBaseline(viewID, "Foo.txt")
		if err != nil {
			t.Fatalf("GetBaseline Foo.txt: %v", err)
		}
		if b.State != store.StateConflict {
			t.Errorf("baseline state = %q, want conflict", b.State)
		}
	})

	t.Run("case-sensitive: two independent nodes", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, viewID, _ := newScanEngine(t)
		server := catalogMap(serverFile("Foo.txt", "s1", hash, int64(len(content))))
		local := []scanEntry{localFile("foo.txt", hash, int64(len(content)))}
		ops, err := e.coldStartReconcile(viewID, server, local, nil, false)
		if err != nil {
			t.Fatalf("coldStartReconcile: %v", err)
		}
		types := map[string]store.OpType{}
		for _, op := range ops {
			types[op.Path] = op.Type
		}
		if len(ops) != 2 || types["Foo.txt"] != store.OpDownload || types["foo.txt"] != store.OpUpload {
			t.Fatalf("got %+v, want an OpDownload Foo.txt and an OpUpload foo.txt", ops)
		}
	})
}

// TestReconcileFunnelCaseFold: a remote modify for "Foo.txt" must funnel against a
// queued outbound upload of "foo.txt" on a case-insensitive FS - the inbound download is
// deferred to the keep-both resolver rather than clobbering the pending local change. On a
// case-sensitive FS the two are distinct nodes, so the download proceeds normally.
func TestReconcileFunnelCaseFold(t *testing.T) {
	seed := func(t *testing.T, st *store.Store) {
		t.Helper()
		if _, err := st.Enqueue(store.Op{
			Direction: store.DirOutbound, ViewID: 1, Type: store.OpUpload, Path: "foo.txt", FileID: "f1",
		}); err != nil {
			t.Fatalf("seed upload op: %v", err)
		}
	}
	events := []actions.Event{
		{SyncID: 20, Action: "modify", FileID: "f1", Path: "Foo.txt", Hash: "server", Size: 9, MTime: 200},
	}

	t.Run("case-insensitive: funnels, no download", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, st := newReconcileEngine(t, "self")
		seed(t, st)
		ops, cursor, err := e.reconcile(1, events, 0)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if cursor != 20 {
			t.Errorf("cursor = %d, want 20 (advances past deferred event)", cursor)
		}
		if len(ops) != 0 {
			t.Fatalf("ops = %+v, want 0 (inbound download deferred to keep-both)", ops)
		}
	})

	t.Run("case-sensitive: distinct node, downloads", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, st := newReconcileEngine(t, "self")
		seed(t, st)
		ops, _, err := e.reconcile(1, events, 0)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Path != "Foo.txt" {
			t.Fatalf("ops = %+v, want one download for Foo.txt (distinct node)", ops)
		}
	})
}

// TestExecutorSerializeKeysCaseFold: two ops whose on-disk paths differ only in
// case must produce conflicting serialization keys on a case-insensitive FS - so drainBatch
// chains them and they never apply concurrently on one file - while remaining independent
// (concurrent) on a case-sensitive FS. keysConflict over serializeKeys is exactly the
// mechanism drainBatch uses to decide whether two ops chain, so asserting on it is a faithful
// and deterministic check of the serialization behaviour.
func TestExecutorSerializeKeysCaseFold(t *testing.T) {
	opFoo := store.Op{Type: store.OpDownload, ViewID: 1, Path: "dir/Foo.txt"}
	opfoo := store.Op{Type: store.OpDownload, ViewID: 1, Path: "dir/foo.txt"}
	// An ancestor remove of "dir/Sub" vs a download under "dir/sub/x" exercises the folded
	// isAncestor prefix test, not just exact equality.
	opSubDir := store.Op{Type: store.OpRemove, ViewID: 1, Path: "dir/Sub"}
	opSubChild := store.Op{Type: store.OpDownload, ViewID: 1, Path: "dir/sub/x.txt"}

	t.Run("case-insensitive: serialise", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		if !keysConflict(serializeKeys("view", opFoo), serializeKeys("view", opfoo)) {
			t.Errorf("case-variant ops do not conflict (would race on one file)")
		}
		if !keysConflict(serializeKeys("view", opSubDir), serializeKeys("view", opSubChild)) {
			t.Errorf("case-variant ancestor/descendant ops do not conflict")
		}
	})

	t.Run("case-sensitive: independent", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		if keysConflict(serializeKeys("view", opFoo), serializeKeys("view", opfoo)) {
			t.Errorf("case-variant ops conflict on a case-sensitive FS (should run concurrently)")
		}
		if keysConflict(serializeKeys("view", opSubDir), serializeKeys("view", opSubChild)) {
			t.Errorf("case-variant ancestor/descendant ops conflict on a case-sensitive FS")
		}
	})
}

// TestSupersededByLaterDeleteFoldedAncestorMove exercises the ancestor-move re-base in
// supersededByLaterDelete on a case-insensitive FS. A download tracked under a directory
// is followed through a case-variant move of that directory and a later delete must still
// supersede it. The directory name uses a character whose case fold CHANGES byte length
// (İ U+0130, 2 bytes, folds to ASCII "i", 1 byte): the tracked path's "İdir" segment is
// 5 bytes while the move source "idir" is 4, so slicing the original cur by the move
// source's byte length would cut mid-segment ("renamedr/child.txt"). rebaseMoved must
// strip whole segments instead, yielding "renamed/child.txt".
func TestSupersededByLaterDeleteFoldedAncestorMove(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()

	// Tracked download lives under "İdir"; the batch moves the directory under its
	// ASCII lower-case fold "idir" → "renamed", then deletes "renamed".
	sorted := []actions.Event{
		{Action: "move", FromPath: "idir", Path: "renamed"}, // case fold of İdir, shorter
		{Action: "delete", Path: "renamed"},
	}
	if !supersededByLaterDelete(sorted, -1, "İdir/child.txt", "") {
		t.Error("case-insensitive ancestor move + delete should supersede the tracked download")
	}

	t.Run("case-sensitive: no fold, distinct dirs, not superseded", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		if supersededByLaterDelete(sorted, -1, "İdir/child.txt", "") {
			t.Error("case-sensitive: a case-variant move source is a different node, must not supersede")
		}
	})
}

// TestResolveParentIDCaseFold: after a case-only directory rename on a
// case-insensitive FS, localReconcile fold-matches the directory and never re-creates it, so
// the parent's baseline keeps its original case ("Foo") while a brand-new child's op path
// carries the on-disk case ("foo/bar.txt"). resolveParentID must still resolve the parent by
// folded key - otherwise the child upload wedges forever as errParentUnknown, since no parent
// op will ever run to unblock it. On a case-sensitive FS "foo" and "Foo" are genuinely
// distinct directories, so the lookup must NOT conflate them.
func TestResolveParentIDCaseFold(t *testing.T) {
	t.Run("case-insensitive: resolves folded parent", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, _ := newScanEngine(t)
		// Parent directory baseline kept under its original case.
		putSynced(t, e, viewID, "Foo", "dirid", "dir", "", 0)

		id, err := e.resolveParentID(viewID, "foo/bar.txt")
		if err != nil {
			t.Fatalf("resolveParentID: %v (want the folded parent id)", err)
		}
		if id != "dirid" {
			t.Errorf("parent id = %q, want dirid (resolved by folded key)", id)
		}
	})

	t.Run("case-insensitive: ambiguous fold refuses", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, _ := newScanEngine(t)
		// A consistent store should never hold two dirs folding to the same key, but if it
		// does the resolver must refuse to guess rather than mis-parent the child.
		putSynced(t, e, viewID, "Foo", "id1", "dir", "", 0)
		putSynced(t, e, viewID, "FOO", "id2", "dir", "", 0)

		if _, err := e.resolveParentID(viewID, "foo/bar.txt"); !errors.Is(err, errParentUnknown) {
			t.Fatalf("err = %v, want errParentUnknown (ambiguous fold must not guess)", err)
		}
	})

	t.Run("case-sensitive: distinct dirs, not resolved", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, viewID, _ := newScanEngine(t)
		putSynced(t, e, viewID, "Foo", "dirid", "dir", "", 0)

		if _, err := e.resolveParentID(viewID, "foo/bar.txt"); !errors.Is(err, errParentUnknown) {
			t.Fatalf("err = %v, want errParentUnknown (foo != Foo on case-sensitive FS)", err)
		}
	})

	// The folded-spelling half of the corrupt-parent descendant wedge. A
	// case-only directory rename keeps the baseline under "Foo" while the child op path is
	// "foo/…"; if "Foo"'s record then corrupts, the exact GetBaseline("foo") misses (no "foo"
	// key) AND the fold fallback used the materialising BaselinesForView, which hid the corrupt
	// record - so the fallback read a folded corrupt parent as simply "no parent" and returned
	// errParentUnknown, wedging the child forever. It must now surface errParentCorrupt (like the
	// exact-key branch), so the child dead-letters instead. The corrupt record reaches the fold
	// fallback only through the scan's skipped report, so the seam returns it there.
	t.Run("case-insensitive: folded corrupt parent is terminal", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(true)()
		e, viewID, _ := newScanEngine(t)
		// No healthy "foo"/"Foo" baseline; the parent record is corrupt, surfaced by the
		// stream as a skipped path whose intact key still folds to the op path's parent.
		e.scanBaselines = func(uint64, func(store.PathBaseline) error) ([]string, error) {
			return []string{"Foo"}, nil
		}
		if _, err := e.resolveParentID(viewID, "foo/bar.txt"); !errors.Is(err, errParentCorrupt) {
			t.Fatalf("err = %v, want errParentCorrupt (folded corrupt parent must be terminal, not a wedge)", err)
		}
	})

	t.Run("case-sensitive: corrupt Foo does not fold to foo", func(t *testing.T) {
		defer normpath.SetCaseInsensitive(false)()
		e, viewID, _ := newScanEngine(t)
		// On a case-sensitive FS "Foo" and "foo" are distinct, so a corrupt "Foo" is NOT this
		// child's parent - it stays a plain errParentUnknown (retry), never a false terminal.
		e.scanBaselines = func(uint64, func(store.PathBaseline) error) ([]string, error) {
			return []string{"Foo"}, nil
		}
		if _, err := e.resolveParentID(viewID, "foo/bar.txt"); !errors.Is(err, errParentUnknown) {
			t.Fatalf("err = %v, want errParentUnknown (Foo != foo on a case-sensitive FS)", err)
		}
	})
}

// TestRebaseMoved verifies the segment-count re-base is byte-length-safe under a
// length-changing case fold and for multi-segment roots.
func TestRebaseMoved(t *testing.T) {
	tests := []struct {
		name             string
		child, old, newR string
		want             string
	}{
		{"ascii single segment", "Foo/child.txt", "Foo", "renamed", "renamed/child.txt"},
		{"ascii multi segment", "a/b/child.txt", "a/b", "x/y", "x/y/child.txt"},
		{"length-changing fold", "İdir/child.txt", "i̇dir", "renamed", "renamed/child.txt"},
		{"deep suffix", "dir/a/b/c.txt", "dir", "r", "r/a/b/c.txt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := rebaseMoved(tc.child, tc.old, tc.newR); got != tc.want {
				t.Errorf("rebaseMoved(%q,%q,%q)=%q, want %q", tc.child, tc.old, tc.newR, got, tc.want)
			}
		})
	}
}
