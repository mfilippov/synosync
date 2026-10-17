package engine

// Tests for the queue-quarantine repair sweep. The store drops a
// queue record whose key or value no longer decodes so one bit-rotted record cannot
// poison every later read - but the record's view and path are exactly what is
// undecodable, so nothing can resolve the baseline reconcile pre-wrote for its op. The
// path then freezes: localReconcile derives ops only from StateSynced baselines, so
// every later local edit is silently not pushed.
//
// The corruption is injected into the real bbolt file (bbolt checksums only its meta
// pages, so latent corruption of a record is exactly what these reproduce), which needs
// the store closed - hence the open/close/reopen shape of the fixture.

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// repairFixture is a sync root plus a state.db path the test can corrupt directly.
type repairFixture struct {
	dbPath  string
	root    string
	viewDir string
}

const repairViewID uint64 = 7

func newRepairFixture(t *testing.T) *repairFixture {
	t.Helper()
	base := t.TempDir()
	f := &repairFixture{
		dbPath:  filepath.Join(base, "state.db"),
		root:    filepath.Join(base, "sync"),
		viewDir: filepath.Join(base, "sync", "Photos"),
	}
	if err := os.MkdirAll(f.viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	return f
}

// openStore opens the fixture's state.db. The caller closes it explicitly (the
// corruption steps need the file unlocked), so no cleanup hook is registered.
func (f *repairFixture) openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(f.dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(repairViewID, store.View{Name: "Photos", Privilege: "rw"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	return st
}

// engine builds an engine over st rooted at the fixture's sync root.
func (f *repairFixture) engine(st *store.Store) *Engine {
	return New(st, &fakeTransport{client: &fakeClient{}}, f.root, "sess", nil)
}

// queueKey is the store's big-endian queue key encoding.
func queueKey(seq uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, seq)
	return k
}

// withRawQueue opens the closed state.db directly and hands the queue bucket to fn, so a
// test can plant the kind of record corruption bbolt itself does not detect.
func withRawQueue(t *testing.T, dbPath string, fn func(b *bolt.Bucket) error) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatalf("raw bolt open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *bolt.Tx) error { return fn(tx.Bucket([]byte("queue"))) }); err != nil {
		t.Fatalf("raw queue mutation: %v", err)
	}
}

// seedStrandedUpload enqueues an outbound OpUpload for rel and pre-writes the pending
// baseline exactly as localReconcile does (enqueue first, baseline second), returning the
// op's queue seq.
func seedStrandedUpload(t *testing.T, st *store.Store, rel, fileID string, content []byte) uint64 {
	t.Helper()
	hash := md4.ContentHash(content)
	seq, err := st.Enqueue(store.Op{
		Type:      store.OpUpload,
		Direction: store.DirOutbound,
		ViewID:    repairViewID,
		Path:      rel,
		FileID:    fileID,
		LocalHash: hash,
		LocalSize: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("Enqueue %s: %v", rel, err)
	}
	if err := st.PutBaseline(repairViewID, rel, store.Baseline{
		FileID:    fileID,
		FileType:  "file",
		LocalHash: hash,
		LocalSize: int64(len(content)),
		State:     store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline %s: %v", rel, err)
	}
	return seq
}

// TestQuarantinedOpRepairsStrandedPendingBaseline is the red test for a stranded pending baseline:
// a queue record whose VALUE bit-rots is quarantined, and before the repair sweep its pending
// baseline was left with nothing to resolve it - freezing the path, so the user's later
// edit was never pushed and only a server event for the same path could ever heal it.
func TestQuarantinedOpRepairsStrandedPendingBaseline(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "notes.txt", "f1", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })

	// The user edits the file while the op is unreadable: this is the change that must
	// still reach the server.
	edited := []byte("the edit that must be pushed")
	writeFile(t, f.viewDir, "notes.txt", edited)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The corrupt record left the queue as a durable Dropped trace...
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead-letters = %+v, want the one quarantined record", dls)
	}
	// ...and the sweep re-derived its owner from the baseline and the on-disk file.
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("queued ops = %+v, want exactly one re-derived push", ops)
	}
	got := ops[0].Op
	wantHash := md4.ContentHash(edited)
	if got.Type != store.OpUpload || got.Direction != store.DirOutbound || got.Path != "notes.txt" {
		t.Fatalf("re-derived op = %+v, want an outbound OpUpload of notes.txt", got)
	}
	if got.FileID != "f1" {
		t.Errorf("re-derived op FileID = %q, want the tracked node id f1", got.FileID)
	}
	if got.LocalHash != wantHash || got.LocalSize != int64(len(edited)) {
		t.Errorf("re-derived op facts = (%q, %d), want the CURRENT on-disk content (%q, %d)",
			got.LocalHash, got.LocalSize, wantHash, len(edited))
	}
	// And NO divergence-base capture. Only a modify off a StateSynced record may supply one;
	// this record is StatePending, so its LocalHash is already the un-pushed edit. Handing that
	// to guardUploadTarget would compare the server against the very bytes being pushed and funnel
	// every repaired push into a spurious keep-both - silent churn on the repair path, invisible
	// to every other assertion here.
	if got.BaseLocalHash != "" {
		t.Errorf("re-derived op BaseLocalHash = %q, want empty: a StatePending record's LocalHash "+
			"is the un-pushed edit, not the last agreed content", got.BaseLocalHash)
	}
	b, err := st.GetBaseline(repairViewID, "notes.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending || b.LocalHash != wantHash {
		t.Errorf("baseline = %+v, want StatePending carrying the edited hash %q", b, wantHash)
	}

	// A second drain must not re-derive the same op again: the counter is only re-read,
	// and the path is now covered by a queued op anyway. (The drain itself fails to apply
	// the push - the fake transport offers no outbound client - so the op stays queued,
	// which is exactly the state the coverage test has to respect.)
	_ = e.drainQueue(context.Background())
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps after second drain: %v", err)
	} else if len(ops) != 1 {
		t.Fatalf("queued ops after second drain = %+v, want still exactly one", ops)
	}
}

// TestQuarantinedCorruptKeyRepairsStrandedPendingBaseline covers the corrupt-KEY arm of
// the quarantine (a key of any length can surface from latent corruption, since bbolt
// checksums only its meta pages): the record is dropped with only its byte length known,
// so the stranded baseline needs the same repair.
func TestQuarantinedCorruptKeyRepairsStrandedPendingBaseline(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "notes.txt", "", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error {
		raw := append([]byte(nil), b.Get(queueKey(seq))...)
		if err := b.Delete(queueKey(seq)); err != nil {
			return err
		}
		return b.Put([]byte("bad"), raw)
	})

	edited := []byte("edit after the key rotted")
	writeFile(t, f.viewDir, "notes.txt", edited)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "notes.txt" ||
		ops[0].LocalHash != md4.ContentHash(edited) {
		t.Fatalf("queued ops = %+v, want one re-derived upload of the edited notes.txt", ops)
	}
}

// TestQuarantineRepairLeavesCoveredPendingBaselines confirms the sweep re-derives ONLY the
// stranded record: a pending baseline whose op is alive in the queue is left alone (no
// duplicate op), and so is a directory-move descendant, which reconcile's relocatePending
// legitimately rebases with no op of its own - it is covered by the queued rename's path.
func TestQuarantineRepairLeavesCoveredPendingBaselines(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	// A live queued op with its pending baseline: this pair is healthy.
	writeFile(t, f.viewDir, "live.txt", []byte("live"))
	liveSeq := seedStrandedUpload(t, st, "live.txt", "fl", []byte("live"))
	// A queued directory rename plus the descendant baseline reconcile rebased under its
	// destination without an op of its own.
	writeFile(t, f.viewDir, "new/child.txt", []byte("c"))
	renameSeq, err := st.Enqueue(store.Op{
		Type:      store.OpRename,
		Direction: store.DirOutbound,
		ViewID:    repairViewID,
		Path:      "old",
		NewPath:   "new",
		FileID:    "fd",
		IsDir:     true,
	})
	if err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if err := st.PutBaseline(repairViewID, "new/child.txt", store.Baseline{
		FileID: "fc", FileType: "file", LocalHash: md4.ContentHash([]byte("c")), State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline descendant: %v", err)
	}
	// And the stranded one.
	orphanSeq := seedStrandedUpload(t, st, "orphan.txt", "fo", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error {
		return b.Put(queueKey(orphanSeq), []byte("{corrupt"))
	})
	writeFile(t, f.viewDir, "orphan.txt", []byte("orphan edit"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	// The drain leaves both live ops queued (the fake transport offers no outbound client),
	// which is what the coverage test must respect.
	_ = e.drainQueue(context.Background())

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	var repaired []store.Op
	for _, qo := range ops {
		if qo.Seq != liveSeq && qo.Seq != renameSeq {
			repaired = append(repaired, qo.Op)
		}
	}
	if len(repaired) != 1 || repaired[0].Path != "orphan.txt" {
		t.Fatalf("re-derived ops = %+v, want exactly one, for the stranded orphan.txt", repaired)
	}
	// The descendant of the queued rename is untouched: no op, baseline as seeded.
	if b, err := st.GetBaseline(repairViewID, "new/child.txt"); err != nil {
		t.Fatalf("GetBaseline descendant: %v", err)
	} else if b.State != store.StatePending || b.LocalHash != md4.ContentHash([]byte("c")) {
		t.Errorf("rename descendant baseline = %+v, want it left exactly as reconcile rebased it", b)
	}
}

// TestQuarantineRepairDropsPhantomPendingBaseline confirms the never-lose direction of the
// sweep: a stranded record whose node is not materialised on disk yields NO op - least of
// all a destructive one. localReconcile turns a vanished SYNCED baseline into an OpRemove
// because a synced record is proof of last agreement; a pending record is not, so the
// phantom is merely untracked and the next scan or server event re-derives it honestly.
func TestQuarantineRepairDropsPhantomPendingBaseline(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "never-landed.txt", "fp", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none (nothing is on disk to push, and a delete is never derived)", ops)
	}
	if _, err := st.GetBaseline(repairViewID, "never-landed.txt"); !isNotFound(err) {
		t.Errorf("GetBaseline = %v, want the phantom record untracked", err)
	}
}

// TestQuarantineRepairRederivesDirectoryCreate confirms a stranded pending baseline whose
// path holds a directory is re-derived as an OpMkdir (applyMkdirOut adopts a directory that
// already exists server-side, so this converges for a tracked directory too).
func TestQuarantineRepairRederivesDirectoryCreate(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: repairViewID, Path: "albums",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := st.PutBaseline(repairViewID, "albums", store.Baseline{
		FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	if err := os.MkdirAll(filepath.Join(f.viewDir, "albums"), 0o755); err != nil {
		t.Fatalf("mkdir albums: %v", err)
	}

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpMkdir || ops[0].Path != "albums" ||
		ops[0].Direction != store.DirOutbound {
		t.Fatalf("queued ops = %+v, want one re-derived outbound OpMkdir for albums", ops)
	}
}

// seedStrandedDownload pre-writes the pending baseline reconcile's markUpsertPending records
// for a remote modify - the SERVER facts and no local ones - alongside the inbound OpDownload
// that owns it, returning the op's queue seq.
func seedStrandedDownload(t *testing.T, st *store.Store, rel, fileID, serverHash string) uint64 {
	t.Helper()
	seq, err := st.Enqueue(store.Op{
		Type:      store.OpDownload,
		Direction: store.DirInbound,
		ViewID:    repairViewID,
		Path:      rel,
		FileID:    fileID,
		LocalHash: serverHash,
	})
	if err != nil {
		t.Fatalf("Enqueue %s: %v", rel, err)
	}
	// baselineFromEvent(ev, StatePending): server facts only, LocalHash deliberately empty.
	if err := st.PutBaseline(repairViewID, rel, store.Baseline{
		FileID:       fileID,
		FileType:     "file",
		ServerHash:   serverHash,
		ServerMTime:  4242,
		ServerSyncID: 99,
		State:        store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline %s: %v", rel, err)
	}
	return seq
}

// TestQuarantineRepairRederivesInboundPullForServerOwnedRecord pins the direction rule: a
// stranded record written by the INBOUND pass (server facts, no local hash) must not be
// re-derived as a push. The file on disk is the STALE pre-download content, so pushing it would
// overwrite the server's newer version - and the stale-base rejection that would nominally catch
// it is not something this client can prove (Upload sends no base version). The download is
// re-derived instead, and its own never-lose gate preserves the local bytes.
func TestQuarantineRepairRederivesInboundPullForServerOwnedRecord(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedDownload(t, st, "remote.txt", "fs", "serverhash")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })

	// The stale local content the download was going to replace is still on disk.
	writeFile(t, f.viewDir, "remote.txt", []byte("the pre-download local content"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("queued ops = %+v, want exactly one re-derived op", ops)
	}
	got := ops[0].Op
	if got.Type != store.OpDownload || got.Direction != store.DirInbound {
		t.Fatalf("re-derived op = %+v, want an INBOUND OpDownload: pushing the stale on-disk bytes would overwrite the server's newer version", got)
	}
	if got.Path != "remote.txt" || got.FileID != "fs" || got.LocalHash != "serverhash" {
		t.Errorf("re-derived op = %+v, want the server facts the record carries (remote.txt/fs/serverhash)", got)
	}
	if got.ServerSyncID != 99 {
		t.Errorf("re-derived op ServerSyncID = %d, want the record's 99 so the applied baseline keeps its position", got.ServerSyncID)
	}
	// The baseline is left StatePending exactly as reconcile wrote it - that is what
	// applyDownload's fresh-create gate reads as "reconcile-produced over untracked content".
	b, err := st.GetBaseline(repairViewID, "remote.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending || b.LocalHash != "" || b.ServerHash != "serverhash" {
		t.Errorf("baseline = %+v, want it untouched: pending, no local hash, server hash intact", b)
	}
}

// TestQuarantineRepairLeavesInboundDeleteFlipStranded pins the arm that separates a local edit
// from an INBOUND delete. flipPending - the inbound delete's baseline flip - preserves a
// previously synced record verbatim, so it carries a local hash and a file id exactly like a
// modify's planUpload record does; the difference is that its local bytes still equal the last
// agreed SERVER content, i.e. there is no local change to push at all. Reading that shape as a
// push would send the server bytes it already holds - and since Upload is parent+name addressed
// and carries no base version, it would RE-CREATE the node another client deleted, undoing that
// deletion on every client. The sweep leaves it frozen instead: the pre-repair status quo.
func TestQuarantineRepairLeavesInboundDeleteFlipStranded(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	content := []byte("the agreed content")
	hash := md4.ContentHash(content)
	// The inbound OpRemove reconcile enqueued for the server's delete...
	seq, err := st.Enqueue(store.Op{
		Type:      store.OpRemove,
		Direction: store.DirInbound,
		ViewID:    repairViewID,
		Path:      "gone.txt",
		FileID:    "fs",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// ...and the record flipPending left behind: the synced baseline, State flipped only.
	if err := st.PutBaseline(repairViewID, "gone.txt", store.Baseline{
		FileID: "fs", FileType: "file", LocalHash: hash, ServerHash: hash,
		LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	// The delete never applied, so the file is still on disk holding the agreed content.
	writeFile(t, f.viewDir, "gone.txt", content)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: re-deriving a push here re-creates the node the server deleted", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "gone.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending {
		t.Errorf("baseline = %+v, want it left pending (the file is live on disk)", b)
	}
}

// TestQuarantineRepairLeavesHashLessInboundDeleteFlipStranded pins the same refusal on a server
// that reports NO hashes. markSynced always derives a LocalHash, but applyUploadOut assigns the
// reply's hash verbatim and a hash-less cold-start catalog is adopted with none, so a perfectly
// ordinary synced record can carry an empty ServerHash. Comparing the disk against the ServerHash
// alone lets that record through the guard and re-creates the node the server deleted; the agreed
// content falls back to the record's LocalHash for the same reason removeTargetFacts's does.
func TestQuarantineRepairLeavesHashLessInboundDeleteFlipStranded(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	content := []byte("the agreed content of a hash-less server")
	seq, err := st.Enqueue(store.Op{
		Type:      store.OpRemove,
		Direction: store.DirInbound,
		ViewID:    repairViewID,
		Path:      "gone.txt",
		FileID:    "fs",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// The synced record of a node whose upload reply carried no hash: LocalHash only.
	if err := st.PutBaseline(repairViewID, "gone.txt", store.Baseline{
		FileID: "fs", FileType: "file", LocalHash: md4.ContentHash(content),
		LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "gone.txt", content)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: with no ServerHash the record still describes agreed content", ops)
	}
}

// TestQuarantineRepairPushesAnUntrackedCreate pins the carve-out that keeps the commonest repair
// working: a record with no file id names a node the server has never held, so its push cannot
// re-create anything another client deleted and the content compare has nothing to compare
// against. The first push of a local create is derivable even when the on-disk bytes still equal
// the ones its record remembers.
func TestQuarantineRepairPushesAnUntrackedCreate(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	created := []byte("a brand-new local file")
	seq := seedStrandedUpload(t, st, "fresh.txt", "", created)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "fresh.txt", created)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "fresh.txt" {
		t.Fatalf("queued ops = %+v, want the re-derived push of the never-uploaded fresh.txt", ops)
	}
}

// TestQuarantineRepairLeavesTrackedDirectoryStranded pins the directory twin of that refusal. A
// directory record IS its own content - there is no hash to compare - so for a node the server
// already tracks, a stranded outbound mkdir and an inbound delete's flipPending leave the same
// shape, and re-deriving the push would re-create a directory another client deleted.
func TestQuarantineRepairLeavesTrackedDirectoryStranded(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: repairViewID,
		Path: "albums", FileID: "fd",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := st.PutBaseline(repairViewID, "albums", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	if err := os.MkdirAll(filepath.Join(f.viewDir, "albums"), 0o755); err != nil {
		t.Fatalf("mkdir albums: %v", err)
	}

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: re-deriving a mkdir here re-creates the directory the server deleted", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "albums"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending {
		t.Errorf("baseline = %+v, want it left pending (the directory is live on disk)", b)
	}
}

// TestQuarantineRepairRederivesInboundPullForAnUnlandedDownload pins the absent-path arm for the
// inbound shape. A markUpsertPending record (server facts, no local hash) describes a node the
// download had not yet written, so "absent" is its NORMAL state, not evidence of a phantom.
// Dropping it deletes the only description of the server node: the cursor has already advanced
// past the event, so nothing re-derives it and the file never arrives.
func TestQuarantineRepairRederivesInboundPullForAnUnlandedDownload(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedDownload(t, st, "arriving.txt", "fs", "serverhash")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	// Nothing is written to disk: the download never landed.

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Direction != store.DirInbound ||
		ops[0].Path != "arriving.txt" || ops[0].FileID != "fs" {
		t.Fatalf("queued ops = %+v, want the re-derived inbound pull of arriving.txt", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "arriving.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending {
		t.Errorf("baseline = %+v, want the record kept: it describes the node being pulled", b)
	}
}

// TestQuarantineRepairLeavesUnresolvablePathStranded pins the sweep's only drop to DEFINITIVE
// evidence. A path that cannot be RESOLVED - a symlink component on the chain, and equally a
// GetView failure or an EIO/EACCES Lstat on a network mount - says nothing about whether the node
// is materialised, and the drop arm it used to take bypassed repairStrandedAbsent entirely: an
// inbound-shaped record (a file id, no local hash) whose download had not landed yet would be
// deleted, and with the cursor already past its event nothing re-derives it - the server's file
// never arrives. One operator-created symlink would have turned the recovery sweep into a silent
// mass-untrack of every stranded record beneath it.
func TestQuarantineRepairLeavesUnresolvablePathStranded(t *testing.T) {
	requireSymlinks(t)
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedDownload(t, st, "linked/arriving.txt", "fs", "serverhash")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	// "linked" is a symlink, so noSymlinkEscape refuses the whole chain: the sweep can neither
	// probe the path nor conclude anything from the refusal.
	target := filepath.Join(f.root, "elsewhere")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(f.viewDir, "linked")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: the path resolves to nothing the sweep may act on", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "linked/arriving.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending || b.FileID != "fs" {
		t.Errorf("baseline = %+v, want the record left frozen rather than dropped on a non-verdict", b)
	}
}

// TestQuarantineRepairLeavesRejectedPathStranded is the host-portable half of the same arm: a
// stored path e.abs refuses lexically (errRejectedPath - a containment violation from a corrupt
// or hostile record) reaches the identical "could not be resolved" branch, and must likewise
// leave the record alone rather than drop it on a non-verdict.
func TestQuarantineRepairLeavesRejectedPathStranded(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedDownload(t, st, "../escape.txt", "fs", "serverhash")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: the sweep never acts on a path it cannot resolve", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "../escape.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending || b.FileID != "fs" {
		t.Errorf("baseline = %+v, want the record left frozen rather than dropped on a non-verdict", b)
	}
}

// TestQuarantineRepairSkipsPushesOnAReadOnlyView pins the privilege gate. localScan skips a
// read-only view precisely so it never enqueues a doomed push; a re-derived one would drain into
// applyOutbound's read-only guard, which marks the baseline StateConflict and dead-letters the op
// - converting a recoverable freeze into a permanent surfaced conflict localReconcile never
// revisits.
func TestQuarantineRepairSkipsPushesOnAReadOnlyView(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "notes.txt", "f1", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "notes.txt", []byte("an edit that can never be pushed"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	// The account's privilege on the view is downgraded before the sweep runs (openStore seeds
	// it writable).
	if err := st.PutView(repairViewID, store.View{Name: "Photos", Privilege: "ro"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: a push into a read-only view dead-letters into a permanent conflict", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "notes.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending {
		t.Errorf("baseline = %+v, want it left pending rather than conflicted", b)
	}
}

// TestQuarantineRepairStillPullsOnAReadOnlyView pins the other half of that gate: the INBOUND arm
// still runs. Pulling the server's own content into a read-only view is exactly what the view is
// for, so its stranded download is re-derived like any other.
func TestQuarantineRepairStillPullsOnAReadOnlyView(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedDownload(t, st, "remote.txt", "fs", "serverhash")
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "remote.txt", []byte("the stale pre-download content"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	if err := st.PutView(repairViewID, store.View{Name: "Photos", Privilege: "ro"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 1 || ops[0].Type != store.OpDownload || ops[0].Direction != store.DirInbound {
		t.Fatalf("queued ops = %+v, want the re-derived inbound pull", ops)
	}
}

// TestQuarantineRepairStillPushesALocalEdit pins the other side of that rule: a modify's
// planUpload record stamps the hash of the EDIT it was pushing, so its local bytes differ from the
// last agreed server content and the push is genuinely derivable.
func TestQuarantineRepairStillPushesALocalEdit(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	edited := []byte("the local edit")
	seq := seedStrandedUpload(t, st, "edited.txt", "fs", edited)
	// The record remembers the older server content the edit is being pushed over.
	if err := st.PutBaseline(repairViewID, "edited.txt", store.Baseline{
		FileID: "fs", FileType: "file", LocalHash: md4.ContentHash(edited),
		ServerHash: md4.ContentHash([]byte("the agreed content")),
		LocalSize:  int64(len(edited)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "edited.txt", edited)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "edited.txt" {
		t.Fatalf("queued ops = %+v, want the re-derived outbound push of edited.txt", ops)
	}
}

// TestQuarantineRepairLeavesDirectionlessRecordStranded pins the third arm: a pending file
// record with neither a local hash nor a file id is a shape neither reconcile pass produces, so
// there is no evidence of direction. The sweep leaves it frozen rather than guessing - the freeze
// is the pre-repair status quo; a guess could overwrite one side.
func TestQuarantineRepairLeavesDirectionlessRecordStranded(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "mystery.txt", "", []byte("v1"))
	// Strip the only direction evidence the record had.
	if err := st.PutBaseline(repairViewID, "mystery.txt", store.Baseline{
		FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "mystery.txt", []byte("content of unknown provenance"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: no evidence of direction is not a licence to push", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "mystery.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	} else if b.State != store.StatePending {
		t.Errorf("baseline = %+v, want it left pending (never dropped: the file is live on disk)", b)
	}
}

// TestQuarantineRepairSkipsNonPendingBaselines pins the StatePending-only predicate. A
// StateSynced record is owned by localReconcile's ordinary diff, and a StateConflict record is a
// SURFACED keep-both the operator has been told about; re-deriving a push for either would
// re-upload the whole view on the next bit-rotted record, or silently overtake a conflict.
func TestQuarantineRepairSkipsNonPendingBaselines(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	synced := []byte("already agreed")
	writeFile(t, f.viewDir, "synced.txt", synced)
	if err := st.PutBaseline(repairViewID, "synced.txt", store.Baseline{
		FileID: "fs", FileType: "file", LocalHash: md4.ContentHash(synced),
		LocalSize: int64(len(synced)), ServerHash: md4.ContentHash(synced), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline synced: %v", err)
	}
	surfaced := []byte("surfaced conflict")
	writeFile(t, f.viewDir, "conflicted.txt", surfaced)
	conflictBase := store.Baseline{
		FileID: "fk", FileType: "file", LocalHash: md4.ContentHash(surfaced),
		LocalSize: int64(len(surfaced)), State: store.StateConflict,
	}
	if err := st.PutBaseline(repairViewID, "conflicted.txt", conflictBase); err != nil {
		t.Fatalf("PutBaseline conflict: %v", err)
	}
	seq := seedStrandedUpload(t, st, "orphan.txt", "fo", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "orphan.txt", []byte("orphan edit"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "orphan.txt" {
		t.Fatalf("queued ops = %+v, want exactly one, for the stranded orphan.txt only", ops)
	}
	if b, err := st.GetBaseline(repairViewID, "synced.txt"); err != nil {
		t.Fatalf("GetBaseline synced: %v", err)
	} else if b.State != store.StateSynced {
		t.Errorf("synced baseline = %+v, want it untouched", b)
	}
	if b, err := st.GetBaseline(repairViewID, "conflicted.txt"); err != nil {
		t.Fatalf("GetBaseline conflict: %v", err)
	} else if b != conflictBase {
		t.Errorf("conflict baseline = %+v, want it byte-identical to the surfaced record %+v", b, conflictBase)
	}
}

// TestQuarantineRepairOrdersParentBeforeChild pins the ascending-path repair order: the stranded
// directory's re-derived OpMkdir must be enqueued AHEAD of the stranded child's upload, which
// then resolves its parent id against the directory the mkdir has synced. Reversed, the child
// churns on errParentUnknown until its retry budget runs out.
func TestQuarantineRepairOrdersParentBeforeChild(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	childSeq := seedStrandedUpload(t, st, "albums/photo.jpg", "fc", []byte("jpeg"))
	dirSeq, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: repairViewID, Path: "albums",
	})
	if err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if err := st.PutBaseline(repairViewID, "albums", store.Baseline{
		FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline albums: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	// Both records rot, so both baselines are stranded in one sweep.
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error {
		if err := b.Put(queueKey(childSeq), []byte("{corrupt")); err != nil {
			return err
		}
		return b.Put(queueKey(dirSeq), []byte("{corrupt"))
	})
	if err := os.MkdirAll(filepath.Join(f.viewDir, "albums"), 0o755); err != nil {
		t.Fatalf("mkdir albums: %v", err)
	}
	// The child's bytes differ from the ones its record remembers as agreed, so the push is
	// genuinely derivable (repairStrandedUpload refuses a tracked record whose content still
	// equals the last agreement - that shape is an inbound delete's flip).
	writeFile(t, f.viewDir, "albums/photo.jpg", []byte("re-encoded jpeg"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("queued ops = %+v, want both stranded records re-derived", ops)
	}
	if ops[0].Type != store.OpMkdir || ops[0].Path != "albums" {
		t.Errorf("first re-derived op = %+v, want the parent OpMkdir for albums", ops[0].Op)
	}
	if ops[1].Type != store.OpUpload || ops[1].Path != "albums/photo.jpg" {
		t.Errorf("second re-derived op = %+v, want the child upload of albums/photo.jpg", ops[1].Op)
	}
}

// TestQuarantineRepairStopsOnCancelledContext pins the cancellation latency of the sweep: each
// candidate is hashed whole, so a cancelled run must return at the next candidate rather than
// making a shutdown or a pause wait out O(sum of bytes) of hashing.
func TestQuarantineRepairStopsOnCancelledContext(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	seq := seedStrandedUpload(t, st, "notes.txt", "f1", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error { return b.Put(queueKey(seq), []byte("{corrupt")) })
	writeFile(t, f.viewDir, "notes.txt", []byte("edited"))

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	// Reading the queue is what quarantines the rotted record, so the sweep is armed.
	if _, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if st.QuarantinedOps() == 0 {
		t.Fatal("QuarantinedOps() = 0, want the rotted record dropped so the sweep is armed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.repairQuarantinedBaselines(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("repairQuarantinedBaselines err = %v, want context.Canceled so the drain can return promptly", err)
	}
	// Nothing was repaired, and the counter was NOT marked done: the next drain retries.
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps after cancel: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none from a cancelled sweep", ops)
	}
	if err := e.repairQuarantinedBaselines(context.Background()); err != nil {
		t.Fatalf("retry sweep: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps after retry: %v", err)
	} else if len(ops) != 1 {
		t.Fatalf("queued ops after the retry = %+v, want the one re-derived push", ops)
	}
}

// TestQuarantineRepairIsArmedOnlyByAQuarantine confirms the sweep costs a healthy queue
// nothing: with no quarantine, a pending baseline nothing covers is left exactly as it is
// (the documented rejected-rename re-track freeze, DESIGN §13, must not be re-derived by
// every drain).
// TestQuarantineRepairStaysArmedForAnOverlappedBaseline pins the re-arm. The coverage test is
// the executor's path-OVERLAP rule, so a stranded record beneath (or above) an unrelated queued
// endpoint is skipped without ever being examined - and the sweep is armed only by an ADVANCE of
// the store's quarantine counter, which for latent corruption may never come again. Marking the
// quarantine repaired over such a candidate froze it permanently: exactly the state this sweep
// exists to end. A deferred candidate therefore leaves the counter unadvanced, and the next drain
// - once the covering op has drained - re-derives its owner.
func TestQuarantineRepairStaysArmedForAnOverlappedBaseline(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	// An unrelated op queued at the ANCESTOR directory of the stranded record's path. It owns
	// no baseline of the stranded node, but its key overlaps it.
	coverSeq, err := st.Enqueue(store.Op{
		Type:      store.OpMkdir,
		Direction: store.DirOutbound,
		ViewID:    repairViewID,
		Path:      "dir",
		IsDir:     true,
	})
	if err != nil {
		t.Fatalf("Enqueue covering op: %v", err)
	}
	strandedSeq := seedStrandedUpload(t, st, "dir/deep.txt", "fd", []byte("v1"))
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	withRawQueue(t, f.dbPath, func(b *bolt.Bucket) error {
		return b.Put(queueKey(strandedSeq), []byte("{corrupt"))
	})
	edited := []byte("the edit that must still reach the server")
	writeFile(t, f.viewDir, "dir/deep.txt", edited)

	st = f.openStore(t)
	defer func() { _ = st.Close() }()
	e := f.engine(st)
	// The first drain quarantines the rotted record; the stranded baseline is only OVERLAPPED
	// by the queued mkdir, so the sweep defers it rather than guessing.
	_ = e.drainQueue(context.Background())
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Seq != coverSeq {
		t.Fatalf("queued ops after the first drain = %+v, want only the covering op", ops)
	}

	// The covering op drains. Nothing quarantines again, so only the retained arming makes the
	// next sweep run at all.
	if err := st.Ack(coverSeq); err != nil {
		t.Fatalf("Ack covering op: %v", err)
	}
	_ = e.drainQueue(context.Background())

	ops, err = st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after the second drain: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "dir/deep.txt" ||
		ops[0].LocalHash != md4.ContentHash(edited) {
		t.Fatalf("queued ops after the second drain = %+v, want the re-derived push of the edited dir/deep.txt", ops)
	}
}

func TestQuarantineRepairIsArmedOnlyByAQuarantine(t *testing.T) {
	f := newRepairFixture(t)
	st := f.openStore(t)
	defer func() { _ = st.Close() }()
	writeFile(t, f.viewDir, "retracked.txt", []byte("still here"))
	if err := st.PutBaseline(repairViewID, "retracked.txt", store.Baseline{
		FileID: "fr", FileType: "file", LocalHash: md4.ContentHash([]byte("older")), State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}

	e := f.engine(st)
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if ops, err := st.PendingOps(); err != nil {
		t.Fatalf("PendingOps: %v", err)
	} else if len(ops) != 0 {
		t.Fatalf("queued ops = %+v, want none: no quarantine happened, so no sweep runs", ops)
	}
}
