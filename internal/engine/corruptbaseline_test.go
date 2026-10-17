package engine

// One baseline record whose stored value no longer decodes must not
// kill the whole view. These tests drive the engine over a REAL bbolt store with a
// record corrupted in place (bbolt checksums only its meta pages, so latent file
// corruption surfaces exactly like this): before the fix ScanBaselinesForView returned
// an error on the first undecodable record, so localReconcile failed every scan and the
// view's outbound sync was dead until Reset/re-pair.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// corruptBaselineRecord flips the files-bucket record at view/path to undecodable bytes
// in the raw bbolt file. The store must be closed (bbolt holds an exclusive lock); the
// caller reopens it afterwards.
func corruptBaselineRecord(t *testing.T, dbPath string, viewID uint64, path string) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	key := append(binary.BigEndian.AppendUint64(nil, viewID), 0)
	key = append(key, path...)
	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("files")).Put(key, []byte("{corrupt"))
	})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("corrupt baseline record %d/%q: %v", viewID, path, err)
	}
}

// corruptViewRecord flips the views-bucket VALUE for viewID to undecodable bytes in the
// raw bbolt file, under its intact 8-byte big-endian key. The store must be closed
// (bbolt holds an exclusive lock); the caller reopens it afterwards.
func corruptViewRecord(t *testing.T, dbPath string, viewID uint64) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	key := binary.BigEndian.AppendUint64(nil, viewID)
	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("views")).Put(key, []byte("not json{"))
	})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("corrupt view record %d: %v", viewID, err)
	}
}

// TestLocalReconcileSkipsCorruptBaseline reproduces the corrupt-baseline wedge through
// the production reconcile path: with one corrupt record among healthy ones, the pass
// must still diff the rest of the view (the healthy modify's OpUpload is emitted), must
// emit NO op for the corrupt-record path - its baseline is invisible, so without the
// drop the on-disk file would diff as a brand-new create and upload over the server
// node it tracks - and must Warn. Before the fix localReconcile returned an error and
// the whole view stopped syncing.
func TestLocalReconcileSkipsCorruptBaseline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}

	const mtime = 1_700_000_000
	// a.txt: healthy baseline, on-disk content modified - its OpUpload must still be emitted.
	oldA := []byte("old a")
	newA := []byte("modified a content")
	writeFileWithMtime(t, filepath.Join(viewDir, "a.txt"), newA, mtime+5)
	putFileBaseline(t, st, viewID, "a.txt", md4.ContentHash(oldA), int64(len(oldA)), mtime)
	// bad.txt: tracked and still on disk; its record is corrupted below.
	badContent := []byte("bad content")
	writeFileWithMtime(t, filepath.Join(viewDir, "bad.txt"), badContent, mtime)
	putFileBaseline(t, st, viewID, "bad.txt", md4.ContentHash(badContent), int64(len(badContent)), mtime)
	// c.txt: healthy and unchanged - no op.
	cContent := []byte("c content")
	writeFileWithMtime(t, filepath.Join(viewDir, "c.txt"), cContent, mtime)
	putFileBaseline(t, st, viewID, "c.txt", md4.ContentHash(cContent), int64(len(cContent)), mtime)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	corruptBaselineRecord(t, dbPath, viewID, "bad.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	var buf bytes.Buffer
	e := New(st2, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile with one corrupt baseline failed - the whole view is dead: %v", err)
	}
	sawUpload := false
	for _, op := range ops {
		if op.Path == "bad.txt" || op.NewPath == "bad.txt" {
			t.Errorf("op emitted for the corrupt-record path (its untracked-looking file would clobber the server node): %+v", op)
		}
		if op.Type == store.OpUpload && op.Path == "a.txt" {
			sawUpload = true
		}
	}
	if !sawUpload {
		t.Errorf("healthy modify a.txt produced no OpUpload; ops = %+v", ops)
	}
	if len(ops) != 1 {
		t.Errorf("got %d ops, want exactly the a.txt upload: %+v", len(ops), ops)
	}
	if !strings.Contains(buf.String(), "no longer decode") || !strings.Contains(buf.String(), "bad.txt") {
		t.Errorf("no Warn naming the corrupt record; log: %s", buf.String())
	}
}

// TestScanReplacedTrackedCorruptFoldRecordProtects covers trackedFoldIndex's corrupt-skip
// arm: the tracked slot's record no longer decodes AND the non-syncable replacement occupies
// it under a different case, so the exact-key lookup misses (the store holds "Doc.txt") and
// the fold fallback sees the record only in the stream's skipped report. The path - whose
// identity comes from the intact KEY, not the corrupt value - must still read as tracked, so
// the replacement is protected and Warned instead of ignored as an untracked stray.
func TestScanReplacedTrackedCorruptFoldRecordProtects(t *testing.T) {
	requireSymlinks(t)
	defer normpath.SetCaseInsensitive(true)()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	doc := []byte("original doc content")
	putFileBaseline(t, st, viewID, "Doc.txt", md4.ContentHash(doc), int64(len(doc)), 1_700_000_000)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "Doc.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	var buf bytes.Buffer
	e := New(st2, &fakeTransport{client: &fakeClient{}}, root, "sess", warnLogger(&buf))

	// The tracked file is gone; a symlink claims the slot under another case.
	if err := os.Symlink("elsewhere", filepath.Join(viewDir, "doc.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, unreadable, err := e.scanView(context.Background(), viewID, nil)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	if !unreadable["doc.txt"] {
		t.Errorf("case-variant replacement over a corrupt tracked record was not protected; it would be read as an untracked stray")
	}
	if !strings.Contains(buf.String(), "replaced by a non-syncable node") {
		t.Errorf("no Warn logged for the tracked replacement; log: %s", buf.String())
	}
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("localReconcile emitted %d ops, want 0: %+v", len(ops), ops)
	}
}

// TestOutboundChildOfCorruptParentDeadLetters reproduces the descendant half of the
// corrupt-baseline wedge: localReconcile drops only a corrupt baseline's
// own row (its healthy descendants keep their records), but a DIRECT child's outbound
// create/modify still resolves its parent id against the corrupt directory record. Before
// the fix resolveParentID returned that decode failure as a generic error classified
// transient, so the child op retried forever, wedging the queue. It must now dead-letter
// (observable as Dropped) - its on-disk content untouched, its baseline flipped to
// StateConflict so the blocked path stays visible in the Unresolved count instead of
// vanishing into a stale StatePending record - so the queue keeps flowing.
func TestOutboundChildOfCorruptParentDeadLetters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", NodeID: "root1", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// A tracked directory "sub" with a healthy child file baseline (a local modify enqueued
	// its OpUpload and already flipped the baseline to pending).
	if err := st.PutBaseline(viewID, "sub", store.Baseline{
		FileID: "subid", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	child := []byte("child content")
	childHash := md4.ContentHash(child)
	if err := st.PutBaseline(viewID, "sub/child.txt", store.Baseline{
		FileType: "file", LocalHash: childHash, LocalSize: int64(len(child)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Corrupt the PARENT directory's baseline record on disk (bbolt checksums only its
	// meta pages, so a value bit-flip surfaces exactly like this).
	corruptBaselineRecord(t, dbPath, viewID, "sub")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos", "sub"), 0o755); err != nil {
		t.Fatalf("mkdir view subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "Photos", "sub", "child.txt"), child, 0o644); err != nil {
		t.Fatalf("seed on-disk child: %v", err)
	}

	c := &pushClient{}
	e := New(st2, &concTransport{client: c}, root, "sess", nil)

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: viewID, Path: "sub/child.txt",
		LocalHash: childHash, LocalSize: int64(len(child)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain must complete (no wedge) and the op must leave the queue as a dead-letter.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt parent baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Errorf("child of corrupt parent not dead-lettered: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly 1 durable Dropped trace", n, err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("child was uploaded despite a corrupt parent baseline: %+v", c.uploads)
	}
	// The child's baseline is flipped to StateConflict (visible in the Unresolved count),
	// not left as a silent StatePending record; its on-disk content and local facts are
	// preserved so the divergence stays inspectable until the corruption is cleared.
	b, err := st2.GetBaseline(viewID, "sub/child.txt")
	if err != nil || b.State != store.StateConflict {
		t.Errorf("child baseline = %+v (err %v), want StateConflict", b, err)
	}
	if b.LocalHash != childHash {
		t.Errorf("child baseline LocalHash = %q, want preserved %q", b.LocalHash, childHash)
	}
}

// TestOutboundChildOfFoldedCorruptParentDeadLetters is the folded-spelling variant of the
// wedge above. After a case-only directory rename on a
// case-insensitive FS the parent baseline keeps its original case ("Sub") while the child op
// path carries the on-disk case ("sub/child.txt"). When "Sub"'s record then corrupts, the
// exact GetBaseline("sub") misses (no "sub" key) and resolveParentID's fold fallback used the
// materialising BaselinesForView, which HID the corrupt record - so the fallback read a folded
// corrupt parent as "no parent" and returned errParentUnknown, wedging the child forever. The
// fallback now inspects the scan's skipped set, classifies the folded corrupt parent as
// terminal errParentCorrupt, and the child dead-letters exactly like the exact-key case.
func TestOutboundChildOfFoldedCorruptParentDeadLetters(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", NodeID: "root1", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Parent directory baseline kept under its ORIGINAL case "Sub" (a case-only rename to "sub"
	// fold-matched in localReconcile and never rewrote it); the child's op path is lowercase.
	if err := st.PutBaseline(viewID, "Sub", store.Baseline{
		FileID: "subid", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	child := []byte("child content")
	childHash := md4.ContentHash(child)
	if err := st.PutBaseline(viewID, "sub/child.txt", store.Baseline{
		FileType: "file", LocalHash: childHash, LocalSize: int64(len(child)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Corrupt the PARENT directory's baseline record (under its stored case "Sub").
	corruptBaselineRecord(t, dbPath, viewID, "Sub")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos", "sub"), 0o755); err != nil {
		t.Fatalf("mkdir view subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "Photos", "sub", "child.txt"), child, 0o644); err != nil {
		t.Fatalf("seed on-disk child: %v", err)
	}

	c := &pushClient{}
	e := New(st2, &concTransport{client: c}, root, "sess", nil)

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: viewID, Path: "sub/child.txt",
		LocalHash: childHash, LocalSize: int64(len(child)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain must complete (no wedge) and the op must leave the queue as a dead-letter.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a folded corrupt parent baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Errorf("child of folded corrupt parent not dead-lettered: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly 1 durable Dropped trace", n, err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("child was uploaded despite a folded corrupt parent baseline: %+v", c.uploads)
	}
	b, err := st2.GetBaseline(viewID, "sub/child.txt")
	if err != nil || b.State != store.StateConflict {
		t.Errorf("child baseline = %+v (err %v), want StateConflict", b, err)
	}
}

// TestResolveConflictCorruptParentDeadLetters covers the keep-both mirror of the same
// wedge: a resolve-conflict op whose canonical path's parent directory baseline is corrupt
// resolves its parent id through the same resolveParentID chokepoint. It must dead-letter
// terminally (so it stops blocking any authoritative inbound op queued behind it on the
// shared path) rather than retrying resolveParentID against the corrupt record forever.
func TestResolveConflictCorruptParentDeadLetters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", NodeID: "root1", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(viewID, "sub", store.Baseline{
		FileID: "subid", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	diverged := []byte("diverged local content")
	if err := st.PutBaseline(viewID, "sub/child.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash(diverged), LocalSize: int64(len(diverged)),
		State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "sub")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos", "sub"), 0o755); err != nil {
		t.Fatalf("mkdir view subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "Photos", "sub", "child.txt"), diverged, 0o644); err != nil {
		t.Fatalf("seed on-disk child: %v", err)
	}

	c := &pushClient{}
	e := New(st2, &concTransport{client: c}, root, "sess", nil)

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirInbound, ViewID: viewID, Path: "sub/child.txt",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt parent baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Errorf("resolve-conflict with corrupt parent not dead-lettered: %d op(s) still queued", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly 1 durable Dropped trace", n, err)
	}
}

// TestReconcileHealsCorruptBaselineOnModify reproduces the corrupt-baseline wedge through the
// INBOUND reconcile path (distinct from the localReconcile/parent paths above): a synced
// baseline whose stored value no longer decodes must not abort the whole event batch. Before
// the fix snapshotBatchBaselines' strict GetBaseline propagated store.ErrCorruptRecord as fatal,
// so reconcile returned an error, the cursor never advanced, and the same page re-pulled and
// re-failed every cycle forever - the targeting PutBaseline that would heal the key sat below,
// unreachable. It must now treat the corrupt record as absent (fail-safe: no divergence base),
// Warn, enqueue the download, and heal the key with markUpsertPending's tolerant Put.
func TestReconcileHealsCorruptBaselineOnModify(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	orig := []byte("original content")
	// Seed WITH a file id so a byid entry exists - exercising captureNode's PathByID→GetBaseline
	// corrupt branch, not just the bare capturePath fallback.
	if err := st.PutBaseline(viewID, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: md4.ContentHash(orig),
		LocalHash: md4.ContentHash(orig), LocalSize: int64(len(orig)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "a.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	var buf bytes.Buffer
	e := New(st2, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", warnLogger(&buf))

	events := []actions.Event{
		{SyncID: 12, Action: "modify", FileID: "f1", Path: "a.txt", Hash: "h2", Size: 5, MTime: 200},
	}
	ops, cursor, err := e.reconcile(viewID, events, 0)
	if err != nil {
		t.Fatalf("reconcile aborted on a corrupt baseline - the whole view page wedges forever: %v", err)
	}
	if cursor != 12 {
		t.Errorf("cursor = %d, want 12 (the event must be acknowledged)", cursor)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Path != "a.txt" {
		t.Fatalf("ops = %+v, want one OpDownload for a.txt", ops)
	}
	// No spurious divergence base: the corrupt record yielded no last-synced hash, so the
	// download carries an empty base and the apply-time gate funnels toward preserve (a present
	// file under the now-pending baseline) rather than clobbering off a wrong base.
	if ops[0].BaseLocalHash != "" {
		t.Errorf("download BaseLocalHash = %q, want empty (a corrupt record must yield no base)", ops[0].BaseLocalHash)
	}
	// The targeting Put healed the key: it now decodes and is pending.
	b, err := st2.GetBaseline(viewID, "a.txt")
	if err != nil {
		t.Fatalf("baseline still corrupt after reconcile (heal failed): %v", err)
	}
	if b.State != store.StatePending || b.ServerHash != "h2" {
		t.Errorf("healed baseline = %+v, want pending with ServerHash h2", b)
	}
	if !strings.Contains(buf.String(), "does not decode") || !strings.Contains(buf.String(), "a.txt") {
		t.Errorf("no Warn naming the corrupt record; log: %s", buf.String())
	}
}

// TestReconcileCorruptBaselineDeletePreserves drives the corrupt-baseline DELETE end to end
// (reconcile + drain): a server delete for a path whose synced baseline is corrupt must not
// wedge, and - since the corrupt record yields no trustworthy last-synced base (BaseWasSynced
// stays false) - the on-disk file is preserved whole as a keep-both _Conflict copy (the
// never-lose direction) rather than destroyed, while applyRemove's tolerant DeleteBaseline
// heals the key. Before the fix reconcile aborted on the corrupt snapshot read and neither the
// preserve nor the heal ran.
func TestReconcileCorruptBaselineDeletePreserves(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	content := []byte("user content the server never saw")
	if err := st.PutBaseline(viewID, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: md4.ContentHash(content),
		LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "a.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed on-disk file: %v", err)
	}

	e := New(st2, &concTransport{client: &pushClient{}}, root, "sess", nil)

	events := []actions.Event{{SyncID: 12, Action: "delete", FileID: "f1", Path: "a.txt"}}
	ops, _, err := e.reconcile(viewID, events, 0)
	if err != nil {
		t.Fatalf("reconcile aborted on a corrupt baseline: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove {
		t.Fatalf("ops = %+v, want one OpRemove", ops)
	}
	if ops[0].BaseWasSynced {
		t.Errorf("OpRemove.BaseWasSynced = true; a corrupt record must read as non-synced (preserve direction)")
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt baseline: %v", err)
	}
	// The original file is set aside (never destroyed) and a durable Conflict records the copy.
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("original a.txt still present (err %v); the delete should have preserved it aside", err)
	}
	conflicts, err := st2.ListConflicts(viewID)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("got %d conflicts, want exactly 1 keep-both copy: %+v", len(conflicts), conflicts)
	}
	if _, err := os.Stat(filepath.Join(root, conflicts[0].CopyPath)); err != nil {
		// CopyPath is view-relative under the view dir; resolve via the view name.
		if _, err2 := os.Stat(filepath.Join(viewDir, conflicts[0].CopyPath)); err2 != nil {
			t.Errorf("preserved _Conflict copy not found on disk: %v / %v", err, err2)
		}
	}
	// The corrupt key is healed: the baseline is now cleanly absent (dropped by DeleteBaseline).
	if _, err := st2.GetBaseline(viewID, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetBaseline(a.txt) err = %v, want ErrNotFound (the corrupt key should be healed/dropped)", err)
	}
}

// starveClient is a scripted syncClient for the pollOnce starvation test: LongPoll reports a
// fixed changed-view set, and PullEvent fails transiently for failView while paginating a
// preset event list for every other view.
type starveClient struct {
	changed  []int64
	failView uint64
	events   map[uint64][]actions.Event
}

func (c *starveClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *starveClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *starveClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return c.changed, nil
}
func (c *starveClient) PullEvent(_ context.Context, viewID, _, since int64) ([]actions.Event, int64, error) {
	if uint64(viewID) == c.failView { // #nosec G115 -- view id is non-negative
		return nil, 0, errors.New("transient pull failure")
	}
	next := since
	var evs []actions.Event
	for _, ev := range c.events[uint64(viewID)] { // #nosec G115 -- view id is non-negative
		if ev.SyncID > since {
			evs = append(evs, ev)
			if ev.SyncID > next {
				next = ev.SyncID
			}
		}
	}
	return evs, next, nil
}
func (c *starveClient) Download(context.Context, string, int64, int64, io.Writer) (actions.Node, error) {
	return actions.Node{}, nil
}

// TestPollOnceContinuesPastFailedView pins the view-starvation fix: a non-terminal
// syncView failure for one changed view must not abort the whole poll before the remaining
// changed views are synced. Before the fix pollOnce returned on the first syncView error, so a
// view sorted earlier in `changed` that failed every cycle starved inbound sync for every later
// view. View 1's PullEvent fails transiently; view 2 must still sync (its cursor advances).
func TestPollOnceContinuesPastFailedView(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	// Both views are past cold start and carry a root id, so pollOnce reaches the subscribe/poll
	// path without re-running refreshViews.
	if err := st.PutView(1, store.View{Name: "ViewA", NodeID: "rootA", Privilege: "rw", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 1: %v", err)
	}
	if err := st.PutView(2, store.View{Name: "Docs", NodeID: "rootB", Privilege: "rw", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView 2: %v", err)
	}
	c := &starveClient{
		changed:  []int64{1, 2}, // view 1 (the failing one) is sorted first, so it would starve view 2
		failView: 1,
		events: map[uint64][]actions.Event{
			2: {{SyncID: 5, Action: "create", FileID: "d1", Path: "newdir", IsDir: true}},
		},
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)

	if err := e.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce returned a non-terminal error instead of continuing: %v", err)
	}
	// View 2 synced despite view 1 failing: its cursor advanced and the mkdir applied.
	v2, err := st.GetView(2)
	if err != nil {
		t.Fatalf("GetView(2): %v", err)
	}
	if v2.LastSeenSyncID != 5 {
		t.Errorf("view 2 cursor = %d, want 5 (view 2 was starved by view 1's failure)", v2.LastSeenSyncID)
	}
	if _, err := os.Stat(filepath.Join(root, "Docs", "newdir")); err != nil {
		t.Errorf("view 2's mkdir did not apply: %v", err)
	}
	// View 1's cursor stayed put (its pull failed), and it is left flagged for the next cycle.
	v1, err := st.GetView(1)
	if err != nil {
		t.Fatalf("GetView(1): %v", err)
	}
	if v1.LastSeenSyncID != 0 {
		t.Errorf("view 1 cursor = %d, want 0 (its pull failed)", v1.LastSeenSyncID)
	}
}

// TestViewHasBaselinesCountsCorruptRecords pins the corrupt-skip arm of viewHasBaselines:
// a record the stream skipped is still evidence the view holds tracked state, so the
// view-rename matrix keeps suppressing an ambiguous adoption instead of reading the
// corruption as "no baselines" and taking the unsafe both-directories-missing branch.
func TestViewHasBaselinesCountsCorruptRecords(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	e.scanBaselines = func(uint64, func(store.PathBaseline) error) ([]string, error) {
		return []string{"ghost.txt"}, nil
	}
	has, err := e.viewHasBaselines(viewID)
	if err != nil {
		t.Fatalf("viewHasBaselines: %v", err)
	}
	if !has {
		t.Error("viewHasBaselines = false for a view whose only record is corrupt; want true (tracked state exists)")
	}
}

// TestApplyRenameHealsCorruptSourceBaseline reproduces the corrupt-baseline apply-side wedge
// (instance 1): a node's baseline record bit-rots (byid stays intact) and the server then moves
// the node. Reconcile is fully tolerant (relocatePending skip-and-Warns, promising the apply
// heals the destination key), but applyRename's classifyRenameSource ran FIRST and its strict
// GetBaseline returned store.ErrCorruptRecord as a plain error - classified transient, so the
// op retried every drain forever and the healing Put below it never ran. The corrupt read must
// now classify as absent: the move applies on disk, the destination baseline is written, and
// the destination Put's byid drop-moved-orphan sweep clears the corrupt source key.
func TestApplyRenameHealsCorruptSourceBaseline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	content := []byte("moved content")
	if err := st.PutBaseline(viewID, "dir/a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: md4.ContentHash(content),
		LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "dir/a.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed on-disk file: %v", err)
	}

	var buf bytes.Buffer
	e := New(st2, &concTransport{client: &pushClient{}}, root, "sess", warnLogger(&buf))

	// The op reconcile enqueues for the server move (relocatePending skip-and-Warned the
	// corrupt source, so no relocation happened; DestUntracked mirrors an absent destination).
	if _, err := st2.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: viewID,
		Path: "dir/a.txt", NewPath: "dir/b.txt", FileID: "f1", DestUntracked: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt source baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Fatalf("rename with corrupt source baseline not applied: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 (the move must APPLY, not dead-letter)", n, err)
	}
	// The move ran on disk (direction preserve: the bytes travelled, nothing was deleted).
	if _, err := os.Stat(filepath.Join(viewDir, "dir", "a.txt")); !os.IsNotExist(err) {
		t.Errorf("source dir/a.txt still on disk (err %v); the move did not run", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "dir", "b.txt"))
	if err != nil || !bytes.Equal(got, content) {
		t.Errorf("destination dir/b.txt = %q (err %v), want the moved content", got, err)
	}
	// The destination key records the node and the corrupt source key is healed (swept by the
	// destination Put's byid drop-moved-orphan).
	b, err := st2.GetBaseline(viewID, "dir/b.txt")
	if err != nil || b.FileID != "f1" || b.State != store.StateSynced {
		t.Errorf("destination baseline = %+v (err %v), want synced f1", b, err)
	}
	if _, err := st2.GetBaseline(viewID, "dir/a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetBaseline(dir/a.txt) err = %v, want ErrNotFound (the corrupt key should be healed)", err)
	}
	if !strings.Contains(buf.String(), "does not decode") || !strings.Contains(buf.String(), "dir/a.txt") {
		t.Errorf("no Warn naming the corrupt record; log: %s", buf.String())
	}
}

// TestApplyRemoveDirCorruptDescendantPreserves reproduces the corrupt-baseline apply-side wedge
// (instance 2): the server deletes a directory whose subtree holds one bit-rotted descendant
// record. guardRemoveDir's subtreeHasDivergence strictly read every descendant baseline, so the
// corrupt one failed the walk with a plain error - classified transient, the remove retried every
// drain forever, and the tolerant dropDescendantBaselines that would heal the key never ran. The
// corrupt descendant must now read as unverifiable divergence: the directory is preserved whole
// as a _Conflict copy (never-lose), the descendant baselines - including the corrupt one - are
// cleared, and the op acks.
func TestApplyRemoveDirCorruptDescendantPreserves(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(viewID, "d", store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	aContent := []byte("a content")
	if err := st.PutBaseline(viewID, "d/a.txt", store.Baseline{
		FileID: "fa", FileType: "file", LocalHash: md4.ContentHash(aContent),
		LocalSize: int64(len(aContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child a: %v", err)
	}
	bContent := []byte("b content")
	if err := st.PutBaseline(viewID, "d/b.txt", store.Baseline{
		FileID: "fb", FileType: "file", LocalHash: md4.ContentHash(bContent),
		LocalSize: int64(len(bContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child b: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "d/a.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "a.txt"), aContent, 0o644); err != nil {
		t.Fatalf("seed on-disk a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "b.txt"), bContent, 0o644); err != nil {
		t.Fatalf("seed on-disk b: %v", err)
	}

	var buf bytes.Buffer
	e := New(st2, &concTransport{client: &pushClient{}}, root, "sess", warnLogger(&buf))

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: viewID,
		Path: "d", FileID: "d1", BaseWasSynced: true, BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt descendant baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Fatalf("dir remove with corrupt descendant not applied: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 (the remove must APPLY with a preserve, not dead-letter)", n, err)
	}
	// The directory was moved aside whole (never-lose), not deleted: the canonical path is
	// gone and a _Conflict copy holds both descendants' original bytes.
	if _, err := os.Stat(filepath.Join(viewDir, "d")); !os.IsNotExist(err) {
		t.Errorf("canonical d still on disk (err %v); the remove should have preserved it aside", err)
	}
	conflicts, err := st2.ListConflicts(viewID)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("got %d conflicts, want exactly 1 preserved directory: %+v", len(conflicts), conflicts)
	}
	copyDir := filepath.Join(viewDir, conflicts[0].CopyPath)
	for name, want := range map[string][]byte{"a.txt": aContent, "b.txt": bContent} {
		got, rerr := os.ReadFile(filepath.Join(copyDir, name))
		if rerr != nil || !bytes.Equal(got, want) {
			t.Errorf("preserved copy %s = %q (err %v), want original content", name, got, rerr)
		}
	}
	// Every subtree key is cleared - including the corrupt one (healed by the tolerant
	// dropDescendantBaselines sweep), so the preserved tree re-detects clean next scan.
	for _, p := range []string{"d", "d/a.txt", "d/b.txt"} {
		if _, err := st2.GetBaseline(viewID, p); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetBaseline(%s) err = %v, want ErrNotFound (stale/corrupt key must be cleared)", p, err)
		}
	}
	if !strings.Contains(buf.String(), "does not decode") || !strings.Contains(buf.String(), "d/a.txt") {
		t.Errorf("no Warn naming the corrupt record; log: %s", buf.String())
	}
}

// TestApplyMkdirHealsCorruptRelocateSourceBaseline covers the inbound OpMkdir rejected-move-
// recovery site that the apply-side corrupt-baseline sweep of inbound_rename/remove/download did
// not enumerate (inbound_mkdir.go's relocateTrackedDirForMkdir). A directory node is tracked at
// "olddir" (its byid index fd->olddir stays intact); its baseline value then bit-rots. The server
// reports a create/modify mkdir for the same fd at a different path "link/b", so applyMkdir enters
// relocateTrackedDirForMkdir, whose strict GetBaseline("olddir") returned store.ErrCorruptRecord
// as a plain error - the executor classifies that transient, so the mkdir retried every drain
// forever (blocking overlapping successors) and the healing write never ran. Per the
// apply-side rule (warnCorruptBaseline) the corrupt read must classify as absent (the isNotFound
// direction): applyMkdir falls back to its plain mkdir, whose markSynced Put at "link/b" drops the
// orphaned "olddir" record through the byid re-point (putBaselineTx's drop-moved-orphan), clearing
// the corrupt key - and nothing on disk is deleted (the non-destructive direction).
func TestApplyMkdirHealsCorruptRelocateSourceBaseline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(viewID, "olddir", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "olddir")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	// The directory physically exists at the old path; the byid index (fd->olddir) survived the
	// value corruption, so PathByID resolves it and relocateTrackedDirForMkdir is entered - where
	// GetBaseline("olddir") then hits the corrupt value.
	if err := os.MkdirAll(filepath.Join(viewDir, "olddir"), 0o755); err != nil {
		t.Fatalf("seed on-disk dir: %v", err)
	}

	var buf bytes.Buffer
	e := New(st2, &concTransport{client: &dlClient{}}, root, "sess", warnLogger(&buf))

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: viewID,
		Path: "link/b", FileID: "fd", ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt relocate-source baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Fatalf("mkdir with corrupt relocate-source baseline not applied: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 (the mkdir must APPLY, not dead-letter)", n, err)
	}
	// The directory landed at the new path with a synced baseline resolvable by file id.
	if fi, err := os.Stat(filepath.Join(viewDir, "link", "b")); err != nil || !fi.IsDir() {
		t.Errorf("link/b not created as a directory (err %v)", err)
	}
	b, err := st2.GetBaseline(viewID, "link/b")
	if err != nil || b.FileID != "fd" || b.State != store.StateSynced {
		t.Errorf("destination baseline = %+v (err %v), want synced fd", b, err)
	}
	// The corrupt source key is healed (dropped by the destination Put's byid drop-moved-orphan).
	if _, err := st2.GetBaseline(viewID, "olddir"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetBaseline(olddir) err = %v, want ErrNotFound (the corrupt key should be healed)", err)
	}
	// Non-destructive: the plain-mkdir fallback never deletes the on-disk subtree at the old path.
	if fi, err := os.Stat(filepath.Join(viewDir, "olddir")); err != nil || !fi.IsDir() {
		t.Errorf("on-disk olddir was destroyed (err %v); the corrupt-read fallback must be non-destructive", err)
	}
	if !strings.Contains(buf.String(), "does not decode") || !strings.Contains(buf.String(), "olddir") {
		t.Errorf("no Warn naming the corrupt record; log: %s", buf.String())
	}
}

// TestApplyMkdirCorruptRelocateWithDescendantsDuplicates pins the accepted degradation of the
// corrupt-relocate fallback: the no-descendants case above heals cleanly,
// but when the corrupt directory has DESCENDANT baselines the plain-mkdir fallback re-points only
// the root through byid - relocateDescendantBaselines never runs - so the descendant records stay
// stranded at the old prefix. Nothing on disk is deleted and no local edit is lost (the
// non-destructive direction), but the stranded subtree re-uploads as a fresh directory next scan,
// duplicating the server subtree now at op.Path. This test pins that actual behaviour so a future
// change to the corrupt branch is a conscious decision, not a silent regression.
func TestApplyMkdirCorruptRelocateWithDescendantsDuplicates(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	const childMtime = 1_700_000_000
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(viewID, "olddir", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	child := []byte("child content the corrupt fallback must not lose")
	childHash := md4.ContentHash(child)
	if err := st.PutBaseline(viewID, "olddir/child.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: childHash, ServerHash: childHash,
		LocalSize: int64(len(child)), LocalMTime: childMtime, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	// Corrupt only the PARENT directory's baseline record; its byid entry (fd->olddir) and the
	// child's record stay intact.
	corruptBaselineRecord(t, dbPath, viewID, "olddir")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "olddir"), 0o755); err != nil {
		t.Fatalf("seed on-disk dir: %v", err)
	}
	writeFileWithMtime(t, filepath.Join(viewDir, "olddir", "child.txt"), child, childMtime)

	var buf bytes.Buffer
	e := New(st2, &concTransport{client: &dlClient{}}, root, "sess", warnLogger(&buf))

	if _, err := st2.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: viewID,
		Path: "link/b", FileID: "fd", ServerSyncID: 6,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt relocate-source baseline with descendants: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Fatalf("mkdir not applied: %d op(s) still queued", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 (the mkdir must APPLY, not dead-letter)", n, err)
	}
	// The mkdir landed at the new path (root record healed via byid re-point); the corrupt old
	// ROOT record is dropped.
	if fi, err := os.Stat(filepath.Join(viewDir, "link", "b")); err != nil || !fi.IsDir() {
		t.Errorf("link/b not created as a directory (err %v)", err)
	}
	if _, err := st2.GetBaseline(viewID, "olddir"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetBaseline(olddir) err = %v, want ErrNotFound (the corrupt root key should be dropped)", err)
	}
	// Non-destructive: the on-disk subtree at the old path is untouched - the child file and its
	// bytes survive.
	got, err := os.ReadFile(filepath.Join(viewDir, "olddir", "child.txt"))
	if err != nil || !bytes.Equal(got, child) {
		t.Errorf("olddir/child.txt = %q (err %v); the corrupt fallback must not destroy it", got, err)
	}
	// The DESCENDANT baseline is STRANDED at the old prefix (the accepted degradation): it was not
	// relocated to the new path, so it still tracks the child at olddir/child.txt.
	cb, err := st2.GetBaseline(viewID, "olddir/child.txt")
	if err != nil || cb.FileID != "fc" {
		t.Fatalf("stranded descendant baseline = %+v (err %v), want it preserved at the old prefix", cb, err)
	}
	// Duplication tail: the stranded old subtree re-diffs as a brand-new outbound directory next
	// scan (its root has no baseline now), re-uploading it in parallel with the server node.
	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	sawOldDirMkdir := false
	for _, op := range ops {
		if op.Type == store.OpMkdir && op.Path == "olddir" {
			sawOldDirMkdir = true
		}
	}
	if !sawOldDirMkdir {
		t.Errorf("expected the stranded old subtree to re-upload as a fresh OpMkdir (the documented duplication); ops = %+v", ops)
	}
}

// TestRefreshViewsHealsCorruptViewRecord reproduces the corrupt-view-record wedge: one
// bit-rotted VIEW record must not abort the whole refresh. Before the fix GetView was
// strict (no ErrCorruptRecord tag) and refreshViews' upsert loop aborted on any
// non-ErrNotFound GetView error, every cycle forever - and with it reconcileAbsentViews
// below the loop, re-opening the absent-view spin class where a revoked HEALTHY view is
// never marked absent. It cannot heal in-band either (every PutView sits behind the
// failing GetView). The corrupt read must now classify as fresh: the upsert PutView
// heals the record with live server facts (the view re-runs cold start), the refresh
// completes, and the revoked view is marked absent.
func TestRefreshViewsHealsCorruptViewRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	// View 7 (Alpha) is healthy at seed time with a cursor and cold start done - its record
	// is corrupted below. Views 8/9 (Beta/Gamma) are healthy siblings. View 10 (Delta) is
	// healthy but the server no longer enumerates it (revoked) - it must be marked absent,
	// which only happens if the refresh reaches reconcileAbsentViews rather than aborting.
	seed := []struct {
		id   uint64
		name string
		node string
	}{
		{7, "Alpha", "r7"}, {8, "Beta", "r8"}, {9, "Gamma", "r9"}, {10, "Delta", "r10"},
	}
	for _, v := range seed {
		if err := st.PutView(v.id, store.View{
			NodeID: v.node, Name: v.name, Privilege: "rw", LastSeenSyncID: 42, InitialSyncDone: true,
		}); err != nil {
			t.Fatalf("PutView %d: %v", v.id, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptViewRecord(t, dbPath, 7)

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	var buf bytes.Buffer
	// The server enumerates 7, 8, 9 (a fresh NodeID for 7) but NOT 10 (revoked).
	// nodeErr is the revoked view's root probe: the path-error sentinel is the positive
	// absence evidence reconcileAbsentViews requires before it suppresses a view.
	// No enumerated view needs the enrichment lookup (all carry a
	// NodeID), so the probe is the only QueryNode call.
	fc := &fakeClient{nodeErr: actions.ErrPathError, views: []actions.View{
		{ViewID: 7, Name: "Alpha", Privilege: "rw", NodeID: "r7fresh"},
		{ViewID: 8, Name: "Beta", Privilege: "rw", NodeID: "r8"},
		{ViewID: 9, Name: "Gamma", Privilege: "rw", NodeID: "r9"},
	}}
	e := New(st2, &fakeTransport{client: fc}, t.TempDir(), "sess", infoLogger(&buf))

	if err := e.refreshViews(context.Background()); err != nil {
		t.Fatalf("refreshViews aborted on one corrupt view record - the whole refresh wedges forever: %v", err)
	}

	// View 7 healed via overwrite with live server facts; the corrupt value now decodes.
	v7, err := st2.GetView(7)
	if err != nil {
		t.Fatalf("view 7 still corrupt after refresh (heal failed): %v", err)
	}
	if v7.NodeID != "r7fresh" || v7.Name != "Alpha" || v7.Privilege != "rw" {
		t.Errorf("healed view 7 = %+v, want live server facts (NodeID r7fresh)", v7)
	}
	// The lost cursor / cold-start flag are the documented, accepted cost: the view re-runs
	// a safe-merge cold start rather than staying wedged forever.
	if v7.LastSeenSyncID != 0 || v7.InitialSyncDone {
		t.Errorf("healed view 7 = %+v, want cursor reset to 0 and InitialSyncDone false (re-runs cold start)", v7)
	}
	// Healthy siblings are untouched (cursor and cold-start flag preserved).
	for _, id := range []uint64{8, 9} {
		v, err := st2.GetView(id)
		if err != nil {
			t.Fatalf("GetView(%d): %v", id, err)
		}
		if v.LastSeenSyncID != 42 || !v.InitialSyncDone {
			t.Errorf("healthy sibling view %d = %+v, want cursor 42 and InitialSyncDone true preserved", id, v)
		}
	}
	// reconcileAbsentViews ran (the refresh did not abort): the revoked view 10 is absent
	// once its root probe has been corroborated across absentRootConfirmations refreshes,
	// while the enumerated views are not.
	for i := 1; i < absentRootConfirmations; i++ {
		if err := e.refreshViews(context.Background()); err != nil {
			t.Fatalf("refreshViews (absence confirmation %d): %v", i+1, err)
		}
	}
	if !e.absentViews[10] {
		t.Error("revoked view 10 not marked absent; refresh aborted before reconcileAbsentViews (absent-view spin re-opened)")
	}
	if e.absentViews[7] || e.absentViews[8] || e.absentViews[9] {
		t.Errorf("an enumerated view was marked absent: %v", e.absentViews)
	}
	if !strings.Contains(buf.String(), "does not decode") || !strings.Contains(buf.String(), "view=7") {
		t.Errorf("no Warn naming the corrupt view record; log: %s", buf.String())
	}
}

// TestApplyRemoveCorruptLiveBaselineWarnsAndPreserves covers applyRemove's OWN corrupt-read
// arm, which the reconcile-driven delete tests above never reach: reconcile's
// markPending rewrites - and thereby heals - the key before the apply runs, so only an op
// enqueued directly against a still-corrupt record exercises it. That one read feeds BOTH the
// node-identity gate and the wasDir descendant-drop signal, so a swallowed decode failure would
// skip the descendant sweep with nothing in the log to explain it (re-opening the
// outbound-OpRemove wedge). The corrupt record must therefore read as untracked (the
// never-lose direction: the on-disk file is preserved aside rather than destroyed), the op must
// ack rather than wedge, and the read must Warn naming the site and the path.
func TestApplyRemoveCorruptLiveBaselineWarnsAndPreserves(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	content := []byte("content the corrupt read must not authorise destroying")
	if err := st.PutBaseline(viewID, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: md4.ContentHash(content),
		LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, viewID, "a.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed on-disk file: %v", err)
	}

	var buf bytes.Buffer
	e := New(st2, &concTransport{client: &pushClient{}}, root, "sess", warnLogger(&buf))

	// Enqueued directly (not via reconcile), so the record is still corrupt when applyRemove
	// reads it. The op carries the node id, so the identity gate is live - a corrupt record
	// must not satisfy it.
	if _, err := st2.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: viewID,
		Path: "a.txt", FileID: "f1", BaseWasSynced: true,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue wedged on a corrupt live baseline: %v", err)
	}
	if n, _ := st2.PendingLen(); n != 0 {
		t.Fatalf("remove over a corrupt live baseline not applied: %d op(s) still queued (retrying forever)", n)
	}
	if n, err := st2.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 (the remove must APPLY with a preserve)", n, err)
	}
	// Never-lose: the unverifiable content is set aside as a keep-both copy, never deleted.
	if _, err := os.Stat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("canonical a.txt still present (err %v); the remove should have preserved it aside", err)
	}
	conflicts, err := st2.ListConflicts(viewID)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("got %d conflicts, want exactly 1 preserved copy: %+v", len(conflicts), conflicts)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, conflicts[0].CopyPath))
	if rerr != nil || !bytes.Equal(got, content) {
		t.Errorf("preserved copy = %q (err %v), want the original bytes", got, rerr)
	}
	// The corrupt key is healed (dropped) rather than left to rot.
	if _, err := st2.GetBaseline(viewID, "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetBaseline(a.txt) err = %v, want ErrNotFound (the corrupt key should be healed)", err)
	}
	// The decode failure is observable: without this Warn the skipped descendant sweep has no
	// trace in the rotating log.
	if !strings.Contains(buf.String(), "does not decode") ||
		!strings.Contains(buf.String(), "inbound-remove") ||
		!strings.Contains(buf.String(), "a.txt") {
		t.Errorf("no Warn naming the inbound-remove corrupt read; log: %s", buf.String())
	}
}
