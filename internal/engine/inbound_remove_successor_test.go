package engine

// These tests pin finding H3, measured on two live clients, 14.08.2026 (see docs/DESIGN.md §13
// for the accepted residual of the same arc): an inbound remove addressed by PATH destroyed
// a successor node the client had just created itself, taking the user's own new file off
// disk with an empty queue, zero dead-letters and a clean status.
//
// The interleaving: both clients hold tree/kept.txt. B goes offline and creates tree/fresh.txt;
// A deletes the whole directory tree. B returns, and the loop order (localScan -> drain ->
// poll) pushes fresh.txt FIRST - the server, whose tree node is gone, auto-creates a FRESH
// parent node for it. Two gates, each correct on its own, then compose into a destructive pair:
//
//   - selfEchoCreateBaseline refuses to overlay the echo of that auto-created parent, because
//     its id differs from the record's ("another node holds the path"). The reading is
//     one-directional: here it is the RECORD that names the dead node and the ECHO that names
//     the live one, so the record is left pointing at a corpse.
//   - The delete of the OLD node then arrives, baseIdentifiesNode matches it against that stale
//     record, and the recursive delete takes the live directory - with fresh.txt inside it. The
//     divergence sweep stays silent throughout: fresh.txt matches its own baseline exactly; that
//     baseline is simply agreed with a DIFFERENT, still-live server node.
//
// The fix is positional on both sides: an echo positioned above the record's ServerSyncID is the
// later statement about the path and is adopted (reconcile.go), and a delete whose position sits
// BELOW the stamp on the path - or on any descendant of a directory - is refused
// (removeSupersededByLaterObservation).

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedSuccessorTree lays out the measured H3 state on disk and in the store: the directory
// tree/ holding the peer's kept.txt (tracked against the node the peer is deleting) and our own
// fresh.txt (tracked against a node the server created for us AFTER that delete, so its record
// carries the later sync id). The directory's own record still names the dead node.
func seedSuccessorTree(t *testing.T, st *store.Store, viewDir string, freshSyncID int64) (kept, fresh []byte) {
	t.Helper()
	kept = []byte("content the peer deleted")
	fresh = []byte("the file the user created on this machine")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "kept.txt"), kept, 0o644); err != nil {
		t.Fatalf("seed kept.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "fresh.txt"), fresh, 0o644); err != nil {
		t.Fatalf("seed fresh.txt: %v", err)
	}
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "dir-old", FileType: "dir", ServerSyncID: 5, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/kept.txt", store.Baseline{
		FileID: "kept-old", FileType: "file", ServerSyncID: 6, State: store.StateSynced,
		ServerHash: md4.ContentHash(kept), LocalHash: md4.ContentHash(kept), LocalSize: int64(len(kept)),
	}); err != nil {
		t.Fatalf("seed kept baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/fresh.txt", store.Baseline{
		FileID: "fresh-new", FileType: "file", ServerSyncID: freshSyncID, State: store.StateSynced,
		ServerHash: md4.ContentHash(fresh), LocalHash: md4.ContentHash(fresh), LocalSize: int64(len(fresh)),
	}); err != nil {
		t.Fatalf("seed fresh baseline: %v", err)
	}
	return kept, fresh
}

// TestInboundRemoveSparesSuccessorDirectory is the end-to-end reproduction: the peer's delete of
// the old directory node must not take the live successor - and with it the user's own file -
// off disk. Before the fix both the directory and fresh.txt vanished locally while the server
// kept them, a permanent silent divergence.
func TestInboundRemoveSparesSuccessorDirectory(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	// fresh.txt was described by the server at 400, far above the deletes below: the server
	// cannot report a node inside a directory it has already deleted, so the directory standing
	// here is the successor.
	_, fresh := seedSuccessorTree(t, st, viewDir, 400)

	events := []actions.Event{
		{SyncID: 100, Action: "delete", FileID: "kept-old", Path: "tree/kept.txt"},
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue not drained: %d ops remain", n)
	}

	// The peer's own file is honoured: its node really was deleted.
	if _, err := os.Lstat(filepath.Join(viewDir, "tree", "kept.txt")); !os.IsNotExist(err) {
		t.Errorf("kept.txt survived its own delete: err=%v", err)
	}
	// The decisive assertions: the directory and the user's file are still there.
	if fi, err := os.Lstat(filepath.Join(viewDir, "tree")); err != nil || !fi.IsDir() {
		t.Fatalf("the successor directory was destroyed by the delete of the old node: err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "tree", "fresh.txt"))
	if err != nil || string(got) != string(fresh) {
		t.Fatalf("the user's own file = %q (err %v), want %q", got, err, fresh)
	}
	if b, err := st.GetBaseline(7, "tree/fresh.txt"); err != nil || b.FileID != "fresh-new" {
		t.Errorf("fresh.txt baseline = %+v (err %v), want the live node still tracked", b, err)
	}
	// No _Conflict copy: nothing diverged, the delete simply did not apply here.
	if recs, err := st.ListConflicts(7); err != nil || len(recs) != 0 {
		t.Errorf("conflicts = %+v (err %v), want none", recs, err)
	}
}

// TestInboundRemoveSparedDirectoryDropsItsStaleRecord pins the repair the refusal owes. Sparing
// the successor leaves the directory's own record naming the node the server just DELETED, and
// resolveParentID answers from Baseline.FileID whatever the record's state: every later push into
// that directory would be addressed `id:<dead parent>/name`, refused with 32769 - a bare
// ErrPathError, so no streak, no pacing, no dead-letter - and retried at full drain rate forever,
// with blockedKeys holding the subtree behind it. Nothing heals it either, since localReconcile
// drives ops from StateSynced records only. Dropping the record makes the untracked directory
// re-diff as a fresh create whose OpMkdir adopts the live node.
func TestInboundRemoveSparedDirectoryDropsItsStaleRecord(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	seedSuccessorTree(t, st, viewDir, 400)

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := st.GetBaseline(7, "tree"); !isNotFound(err) {
		b, _ := st.GetBaseline(7, "tree")
		t.Fatalf("the spared directory kept a record naming the deleted node %+v (err %v); "+
			"every child push would resolve its parent id to a corpse", b, err)
	}
	// The directory itself is untouched on disk, and the descendants keep their own records -
	// only the record that provably named a dead node is gone.
	if fi, err := os.Lstat(filepath.Join(viewDir, "tree")); err != nil || !fi.IsDir() {
		t.Fatalf("the spared directory left disk: err=%v", err)
	}
	if b, err := st.GetBaseline(7, "tree/fresh.txt"); err != nil || b.FileID != "fresh-new" {
		t.Errorf("descendant baseline = %+v (err %v), want it intact", b, err)
	}
}

// TestInboundRemoveSparedPathKeepsAForeignRecord is the drop's control: the repair fires only on
// the record it can PROVE stale - a directory still naming the deleted node. A record naming
// another node is the id gate's business and must survive, or a live successor would be untracked
// for no reason.
func TestInboundRemoveSparedPathKeepsAForeignRecord(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := st.PutBaseline(7, "tree", store.Baseline{
		// The record names the SUCCESSOR, not the node being deleted, and carries the later stamp.
		FileID: "dir-new", FileType: "dir", ServerSyncID: 400, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "tree",
		FileID: "dir-old", BaseIsDir: true, ServerSyncID: 101,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if b, err := st.GetBaseline(7, "tree"); err != nil || b.FileID != "dir-new" {
		t.Fatalf("baseline = %+v (err %v), want the successor's own record untouched", b, err)
	}
}

// TestInboundRemoveDeletesDirectoryWithNoLaterDescendant is the control: the ordinary recursive
// delete, where every record under the directory was last described BELOW the delete's position,
// still removes the whole subtree. The guard must not turn every directory delete into a no-op.
func TestInboundRemoveDeletesDirectoryWithNoLaterDescendant(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	// Both children were described BEFORE the delete: nothing here is a successor.
	seedSuccessorTree(t, st, viewDir, 7)

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "tree")); !os.IsNotExist(err) {
		t.Fatalf("the directory survived a delete nothing contradicts: err=%v", err)
	}
	for _, p := range []string{"tree", "tree/kept.txt", "tree/fresh.txt"} {
		if _, err := st.GetBaseline(7, p); !isNotFound(err) {
			t.Errorf("baseline %q survived the delete: err=%v", p, err)
		}
	}
}

// TestInboundRemoveSparesSuccessorAtPathItself covers the arm an ID-LESS delete depends on: with
// no file id to compare, baseIdentifiesNode passes unconditionally and the id gate cannot fire,
// so the record's own later stamp is the only thing standing between the server's stale delete
// and the successor file on disk.
func TestInboundRemoveSparesSuccessorAtPathItself(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("re-created after the delete")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300, State: store.StateSynced,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		ServerSyncID: 200, // the delete is positioned BELOW the record: it names an older node
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil || string(got) != string(body) {
		t.Fatalf("successor file = %q (err %v), want %q", got, err, body)
	}
	if b, err := st.GetBaseline(7, "a.txt"); err != nil || b.FileID != "successor" {
		t.Errorf("successor baseline = %+v (err %v), want it intact", b, err)
	}
}

// TestInboundRemoveWithoutPositionKeepsHistoricalBehaviour pins the disarm: an op carrying no
// position (enqueued before the field was threaded onto a delete, or produced by an unpositioned
// event) has nothing to compare against, so it deletes exactly as before rather than being
// frozen by a record that happens to carry a stamp.
func TestInboundRemoveWithoutPositionKeepsHistoricalBehaviour(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("tracked and agreed")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerSyncID: 300, State: store.StateSynced,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "f1",
		BaseWasSynced: true, BaseLocalHash: md4.ContentHash(body),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("an unpositioned delete no longer applies: err=%v", err)
	}
}

// TestInboundRemoveDivergentSuccessorStillPreserved pins that the new gate composes with the
// never-lose funnel instead of shadowing it: a delete that IS positioned above the record still
// reaches guardRemoveDivergence, so locally-diverged content becomes a _Conflict copy.
func TestInboundRemoveDivergentSuccessorStillPreserved(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("edited locally"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerSyncID: 100, State: store.StateSynced,
		ServerHash: "hsrv", LocalHash: md4.ContentHash([]byte("agreed")), LocalSize: 6,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 200, Action: "delete", FileID: "f1", Path: "a.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" {
		t.Fatalf("conflicts = %+v, want the diverged file preserved as a keep-both copy", recs)
	}
}

// TestSelfEchoCreateAdoptsSuccessorNode is the reconcile-side unit of the same case: the echo
// of the node the server auto-created for our push carries a HIGHER position than the record's,
// so it must re-point the record at the live node instead of leaving it naming the dead one.
func TestSelfEchoCreateAdoptsSuccessorNode(t *testing.T) {
	e, st := newReconcileEngine(t, "self")
	if err := st.PutBaseline(1, "tree", store.Baseline{
		FileID: "dir-old", FileType: "dir", ServerSyncID: 5, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 400, Action: "create", FileID: "dir-new", Path: "tree", IsDir: true,
			CommitterSessionID: "self"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	b, err := st.GetBaseline(1, "tree")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "dir-new" || b.ServerSyncID != 400 {
		t.Fatalf("record = %+v, want it adopted by the later echo (dir-new @400)", b)
	}
	if p, perr := st.PathByID(1, "dir-new"); perr != nil || p != "tree" {
		t.Errorf("PathByID(dir-new) = %q, %v; want tree indexed for the live node", p, perr)
	}
}

// TestSelfEchoCreateAdoptionDisarmsTheStaleDelete is the composed proof that the reconcile-side
// fix alone closes the measured shape: the deletes and the successor's echo arrive in ONE batch
// (as observed on the stand), so by drain time the record names the live node and the existing
// id gate refuses the delete - even with the descendant that carries the later stamp removed
// from the picture entirely.
func TestSelfEchoCreateAdoptionDisarmsTheStaleDelete(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	body := []byte("the file the user created on this machine")
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "fresh.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "dir-old", FileType: "dir", ServerSyncID: 5, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// fresh.txt is deliberately UNTRACKED here: the only evidence in play is the echo.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
		{SyncID: 400, Action: "create", FileID: "dir-new", Path: "tree", IsDir: true,
			CommitterSessionID: "sess"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if fi, err := os.Lstat(filepath.Join(viewDir, "tree")); err != nil || !fi.IsDir() {
		t.Fatalf("the successor directory was destroyed: err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "tree", "fresh.txt"))
	if err != nil || string(got) != string(body) {
		t.Fatalf("the user's own file = %q (err %v), want %q", got, err, body)
	}
}

// TestInboundRemoveRefusalSurfacesTheFrozenRecord covers the arm neither the directory drop nor
// the restore can repair. An ID-LESS delete of a DIRECTORY flips its record to StatePending
// (baseIdentifiesNode passes unconditionally with no id to compare); the positional guard then
// refuses it on a DESCENDANT's later stamp while the directory's own record carries none, so
// nothing proves that record current - it may still name a corpse. localReconcile drives ops from
// StateSynced records only, so it emits no further work - and it used to do so INVISIBLY: no
// dead-letter, nothing in Stuck, nothing in Unresolved. That is the silent-unsyncable class
// finding H6 closes, so the refusal marks it StateConflict.
func TestInboundRemoveRefusalSurfacesTheFrozenRecord(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	body := []byte("the child the peer described after the delete")
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "fresh.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The directory's own record is stamped BELOW the delete: nothing here says it is current.
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "dir-old", FileType: "dir", ServerSyncID: 50, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// The child carries the later stamp - the only evidence the directory on disk is a successor.
	if err := st.PutBaseline(7, "tree/fresh.txt", store.Baseline{
		FileID: "fresh-new", FileType: "file", ServerSyncID: 400, State: store.StateSynced,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	// Through reconcile, so the id-less delete's own flipPending runs - that flip is what leaves
	// the record unsynchronised, and seeding the op directly would skip it.
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 200, Action: "delete", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if b, err := st.GetBaseline(7, "tree"); err != nil || b.State != store.StatePending {
		t.Fatalf("after reconcile baseline = %+v (err %v), want StatePending", b, err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "tree", "fresh.txt")); err != nil || string(got) != string(body) {
		t.Fatalf("successor child = %q (err %v), want %q untouched", got, err, body)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "dir-old" {
		t.Errorf("baseline = %+v, want the record's facts preserved", b)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict so the frozen path shows in Unresolved", b.State)
	}
}

// TestInboundRemoveRefusalRestoresAProvenCurrentRecord covers the arm above it. Here the record
// names a DIFFERENT node than the delete and is stamped strictly ABOVE it, so the server described
// this path after the delete: the record is current, and the refused remove was the only op
// suspending it. Marking such a record StateConflict would freeze a healthy path and report
// Unresolved forever with nothing an operator could clear.
func TestInboundRemoveRefusalRestoresAProvenCurrentRecord(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("re-created after the delete")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300, State: store.StateSynced,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 200, Action: "delete", Path: "a.txt"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if b, err := st.GetBaseline(7, "a.txt"); err != nil || b.State != store.StatePending {
		t.Fatalf("after reconcile baseline = %+v (err %v), want StatePending", b, err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(body) {
		t.Fatalf("successor file = %q (err %v), want %q untouched", got, err, body)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "successor" {
		t.Errorf("baseline = %+v, want the successor's facts preserved", b)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want StateSynced: the refusal proved the record current", b.State)
	}
}

// TestInboundRemoveRefusalDoesNotForgeAgreementForAQueuedNode is the counter-case to the restore
// above. That arm rests on ONE premise - this remove is the op that suspended the record, so the
// op leaving the queue restores the agreement it suspended - which holds when the successor
// arrived as a SELF-ECHO (it enqueues nothing) and is false the moment it arrived as an ordinary
// inbound event, which enqueues a download of its own. Restoring there forges StateSynced for a
// node whose bytes were never transferred: the record claims agreement with an empty LocalHash
// while the predecessor's content still stands on disk, so a scan can diff the path into an
// upload of the deleted file's bytes under the successor's identity - and a download that leaves
// the queue without applying is preserved by dropStrandedBaseline precisely BECAUSE the record
// claims agreement. The queue is the discriminator, exactly as it is for the directory-delete
// precondition.
func TestInboundRemoveRefusalDoesNotForgeAgreementForAQueuedNode(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("the deleted node's content, still on disk")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The record reconcile pre-wrote for a PEER's create at the freed path: the successor's
	// identity and position, no content facts at all, and a queued download that still owes them.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "dead", ServerSyncID: 200,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "successor", ServerSyncID: 300,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}

	// The download fails against the bare fake and stays queued; only the remove's repair matters.
	_ = e.drainQueue(context.Background())

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateSynced {
		t.Fatalf("baseline = %+v: the refusal forged agreement for a node whose download is still queued", b)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict: unresolvable, but visible in Unresolved "+
			"and never less protective than the pending record it replaces", b.State)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "a.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("file = %q (err %v), want the refused delete to leave it untouched", got, rerr)
	}
}

// TestInboundRemoveRefusalDoesNotForgeAgreementForAnIDLessRecord closes the door the queue-based
// discriminator above could not see. The restore arm tests `live.FileID != op.FileID`, which reads
// an EMPTY record id as a DIFFERENT node - the exact opposite of what baseIdentifiesNode makes of
// it everywhere else (no identity to compare, therefore no evidence of a successor at all) - and
// queuedInboundCoversNode answers false for an empty id by construction, so the guard that is
// supposed to see the still-queued download is inert.
//
// Reconcile writes exactly such a record: baselineFromEvent stamps the event's ServerSyncID with
// its empty FileID for every inbound create/modify the server reports without an id, so one poll
// batch of [delete F@p, modify@p (no id)] left the refusal forging StateSynced for content that
// was never transferred.
func TestInboundRemoveRefusalDoesNotForgeAgreementForAnIDLessRecord(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("the deleted node's content, still on disk")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The record reconcile pre-wrote for an ID-LESS create/modify event at the freed path: a
	// position, no identity, no content facts, and a queued download that still owes them.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", ServerSyncID: 300, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "dead", ServerSyncID: 200,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		ServerSyncID: 300,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}

	// The download fails against the bare fake and stays queued; only the remove's repair matters.
	_ = e.drainQueue(context.Background())

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateSynced {
		t.Fatalf("baseline = %+v: the refusal forged agreement from a record naming no node at all", b)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict: unresolvable, but visible in Unresolved "+
			"and never less protective than the pending record it replaces", b.State)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "a.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("file = %q (err %v), want the refused delete to leave it untouched", got, rerr)
	}
}

// TestSelfEchoAdoptedRecordIsNotLeftFrozen is the id gate's half of the same repair, on the shape
// the stand measured. The self-echo adopts the record to the live successor while preserving
// StatePending, so at drain time the IDS differ and the id gate - not the positional one - refuses
// the delete. Wiring the repair to the positional gate alone left this record pending forever:
// frozen (localReconcile drives StateSynced records only) and invisible (nothing in Unresolved).
func TestSelfEchoAdoptedRecordIsNotLeftFrozen(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "dir-old", FileType: "dir", ServerSyncID: 5, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
		{SyncID: 400, Action: "create", FileID: "dir-new", Path: "tree", IsDir: true,
			CommitterSessionID: "sess"},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "dir-new" {
		t.Fatalf("baseline = %+v, want the adopted successor's facts", b)
	}
	if b.State == store.StatePending {
		t.Fatalf("baseline state = %q: the refusal left it frozen AND invisible", b.State)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want StateSynced: the record names the live node the "+
			"server described at 400, well after the delete at 101", b.State)
	}
}

// TestInboundRemoveRefusalKeepsASyncedRecordSynced is that arm's control. A record that is still
// StateSynced is not frozen at all - localReconcile drives it normally - so there is nothing to
// surface, and downgrading it to StateConflict would be the thing that STOPS the path syncing.
func TestInboundRemoveRefusalKeepsASyncedRecordSynced(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("still synchronising normally")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300, State: store.StateSynced,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", ServerSyncID: 200,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if b, err := st.GetBaseline(7, "a.txt"); err != nil || b.State != store.StateSynced {
		t.Errorf("baseline = %+v (err %v), want it left StateSynced", b, err)
	}
}

// TestInboundRemoveRefusalLeavesARenamedNodeToItsQueuedRename is the counter-case to the
// download-covered arm above, and the one where the StateConflict mark is not merely redundant but
// PERMANENT. A download or a mkdir ends in markSynced, which overwrites whatever state it finds -
// so marking a record the owner is about to advance costs nothing there. applyRename deliberately
// does the opposite: it PRESERVES StateConflict across a relocation (a divergence the server's move
// carries with it is still a divergence), so a record marked here comes out of the move still
// conflicted, and localReconcile drives only StateSynced records with no further server event owed
// for a move that has already applied. The path would never sync again - the silent-unsyncable
// class the repair exists to close, reached through the repair itself.
//
// The shape is one poll batch of [delete F@p, move N: q->p]: reconcile relocates N's facts onto p
// and queues the rename, the remove holds the lower seq and drains first, and the id gate refuses
// it.
func TestInboundRemoveRefusalLeavesARenamedNodeToItsQueuedRename(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("the moved node's content")
	if err := os.WriteFile(filepath.Join(viewDir, "q.txt"), body, 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	// What reconcile leaves behind: the moved node's facts relocated onto the freed path, still
	// pending, with the rename that owes the on-disk move queued behind the delete.
	if err := st.PutBaseline(7, "p.txt", store.Baseline{
		FileID: "N", FileType: "file", ServerSyncID: 71, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed relocated baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "p.txt",
		FileID: "F", ServerSyncID: 70,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "q.txt", NewPath: "p.txt",
		FileID: "N", ServerSyncID: 71,
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	b, err := st.GetBaseline(7, "p.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %q, want StateSynced: the rename that owed the node applied, "+
			"and a conflict mark stamped by the refused delete survives the relocation forever", b.State)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "p.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("destination = %q (err %v), want the moved node's content", got, rerr)
	}
}

// TestPositionalGatesAreStrictlyAbove pins the one-character mutation both H3 gates hinge on.
// Every scenario fixture in this file positions the record and the op at DIFFERENT sync ids, so
// flipping either comparison to >= would not fail a single one - in the very code paths that
// arbitrate destructive deletes:
//
//   - >= in selfEchoSupersedesRecord makes an echo at the SAME position adopt a foreign record,
//     which is the "any id mismatch means another node holds the path" rewrite H3 was filed
//     against, reintroduced from the other side;
//   - >= in removeSupersededByLaterObservation freezes every delete whose OWN event stamped the
//     record it is deleting (the ordinary shape: reconcile stamps the record from the event, then
//     the op carrying that same sync id applies), so no inbound delete would ever run again.
func TestPositionalGatesAreStrictlyAbove(t *testing.T) {
	t.Run("selfEchoSupersedesRecord", func(t *testing.T) {
		tests := []struct {
			name   string
			record int64
			echo   int64
			want   bool
		}{
			{name: "echo above the record supersedes", record: 100, echo: 101, want: true},
			{name: "echo at the same position is not later", record: 100, echo: 100},
			{name: "echo below the record is the stale one", record: 100, echo: 99},
			{name: "an unpositioned echo never supersedes", record: 0, echo: 0},
			{name: "an unpositioned echo over a stamped record never supersedes", record: 100, echo: 0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := selfEchoSupersedesRecord(
					store.Baseline{FileID: "old", ServerSyncID: tc.record},
					actions.Event{SyncID: tc.echo, FileID: "new"})
				if got != tc.want {
					t.Errorf("selfEchoSupersedesRecord(record=%d, echo=%d) = %v, want %v",
						tc.record, tc.echo, got, tc.want)
				}
			})
		}
	})

	t.Run("removeSupersededByLaterObservation", func(t *testing.T) {
		tests := []struct {
			name   string
			record int64
			op     int64
			want   bool
		}{
			{name: "a record stamped above the delete proves a successor", record: 101, op: 100, want: true},
			{name: "a record stamped by the delete's own event is not a successor", record: 100, op: 100},
			{name: "a record stamped below the delete is the node being deleted", record: 99, op: 100},
			{name: "an unpositioned delete disarms the gate", record: 500, op: 0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e, _, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
				live := store.Baseline{FileID: "live", FileType: "file", ServerSyncID: tc.record}
				got, at, err := e.removeSupersededByLaterObservation(
					store.Op{ViewID: 7, Path: "p.txt", FileID: "dead", ServerSyncID: tc.op},
					true, live, false)
				if err != nil {
					t.Fatalf("removeSupersededByLaterObservation: %v", err)
				}
				if got != tc.want {
					t.Errorf("= (%v, %q), want %v (record=%d op=%d)", got, at, tc.want, tc.record, tc.op)
				}
			})
		}
	})

	t.Run("removeSupersededByLaterObservation/descendant", func(t *testing.T) {
		// The directory arm reads the same comparison off a DESCENDANT record, and it is the arm
		// the measured shape needs - the directory's own record still named the dead node.
		for _, tc := range []struct {
			name  string
			child int64
			op    int64
			want  bool
		}{
			{name: "a child stamped above the delete proves a live successor", child: 101, op: 100, want: true},
			{name: "a child stamped by the delete's own event is not a successor", child: 100, op: 100},
		} {
			t.Run(tc.name, func(t *testing.T) {
				e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
				if err := st.PutBaseline(7, "tree/child.txt", store.Baseline{
					FileID: "c1", FileType: "file", ServerSyncID: tc.child, State: store.StateSynced,
				}); err != nil {
					t.Fatalf("seed child: %v", err)
				}
				got, at, err := e.removeSupersededByLaterObservation(
					store.Op{ViewID: 7, Path: "tree", FileID: "dead", ServerSyncID: tc.op},
					// The directory's own record still names the dead node, stamped below the
					// delete: only the child can testify.
					true, store.Baseline{FileID: "dead", FileType: "dir", ServerSyncID: 1}, true)
				if err != nil {
					t.Fatalf("removeSupersededByLaterObservation: %v", err)
				}
				if got != tc.want {
					t.Errorf("= (%v, %q), want %v (child=%d op=%d)", got, at, tc.want, tc.child, tc.op)
				}
			})
		}
	})
}

// TestInboundRemoveRefusalLeavesAnIDLessDirectoryPending is the one record shape the surfacing
// mark is NOT free for. resolveParentID answers from the PARENT directory's record and reads an
// id-less one as TERMINAL when it is StateConflict (errParentConflicted) and merely transient when
// it is not (errParentUnknown) - so marking an id-less directory here dead-letters every queued op
// beneath it, permanently, since localReconcile drives StateSynced records only. That record means
// "the mkdir that will give it an id has not applied yet", so its children must keep waiting.
func TestInboundRemoveRefusalLeavesAnIDLessDirectoryPending(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	body := []byte("the child waiting for its parent's server id")
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed child file: %v", err)
	}
	// The directory record reconcile pre-wrote for an ID-LESS event: a position, no identity.
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileType: "dir", ServerSyncID: 300, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "tree",
		FileID: "dead", ServerSyncID: 200, BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	// The child push that resolves its parent id from that very record.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "tree/a.txt",
		LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("Enqueue child upload: %v", err)
	}

	_ = e.drainQueue(context.Background())

	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateConflict {
		t.Errorf("directory baseline state = %q: a conflict mark on an id-less directory makes "+
			"every queued op beneath it terminal", b.State)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 1 {
		t.Errorf("pending ops = %d (err %v), want the child upload still queued", n, perr)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	for _, dl := range dls {
		if dl.Path == "tree/a.txt" {
			t.Fatalf("the child was dead-lettered (%q); its parent's mkdir still owes the id", dl.Reason)
		}
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "tree", "a.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("child file = %q (err %v), want the refused delete to leave it untouched", got, rerr)
	}
}

// TestInboundRemoveRefusalKeepsAConflictedRecordConflicted is the half of the early return the
// restore arm's premise depends on. A record already surfaced as StateConflict is a KNOWN
// divergence; the restore below it only proves the delete is not the node standing here, never
// that the divergence was resolved. Falling through would forge StateSynced over an unresolved
// conflict - agreement fabricated for content nothing transferred - which is precisely what this
// repair claims never to do.
func TestInboundRemoveRefusalKeepsAConflictedRecordConflicted(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	body := []byte("a divergence an operator has already been shown")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300, State: store.StateConflict,
		ServerHash: md4.ContentHash(body), LocalHash: md4.ContentHash(body), LocalSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "dead", ServerSyncID: 200,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict left untouched: the refused delete "+
			"proves nothing about a divergence already surfaced", b.State)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "a.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("file = %q (err %v), want the refused delete to leave it untouched", got, rerr)
	}
}

// TestInboundRemoveRefusalLeavesAnOutboundOwnedRecordAlone closes the restore arm's last blind
// spot. The arm's premise is that THIS delete is what suspended the record, and queuedInboundCoversNode
// verifies it against inbound work only - but markLocalPending suspends a record too, when
// localReconcile enqueues a PUSH, and it preserves the server facts (FileID, ServerSyncID) while
// writing the UN-PUSHED local bytes into LocalHash.
//
// The shape: flipPending deliberately leaves a record naming a different node alone, so the record
// sits at StateSynced while the delete of the dead node is already queued; the next scan's local
// edit then hands it to an OpUpload. The delete holds the lower seq, serialises ahead of the push
// and reaches the repair with every restore condition satisfied. Restoring there writes StateSynced
// over content the server has never received - and if the push later drops, localReconcile re-derives
// nothing (disk == LocalHash) while every never-lose gate reads the record as agreed content.
func TestInboundRemoveRefusalLeavesAnOutboundOwnedRecordAlone(t *testing.T) {
	// The push fails transiently and stays queued, so the record the repair left behind is
	// observable: a succeeding upload would markSynced over it and hide what the repair wrote.
	e, st, viewDir := outboundEngine(t, &pushClient{uploadErr: actions.ErrConnBroken}, "Photos", "root1")
	body := []byte("the local edit that has not been pushed yet")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// markLocalPending's record: the successor's server facts preserved, LocalHash already the
	// un-pushed edit, State forced to pending by the QUEUED UPLOAD - not by the delete below.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "successor", FileType: "file", ServerSyncID: 300,
		ServerHash: "oldserverhash", LocalHash: "newlocalhash", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "dead", ServerSyncID: 200,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "successor",
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}

	// The upload fails against the bare fake and stays queued; only the remove's repair matters.
	_ = e.drainQueue(context.Background())

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateSynced {
		t.Fatalf("baseline = %+v: the refusal forged agreement for bytes a queued push has not sent", b)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %q, want StatePending: the record belongs to its queued outbound "+
			"op, which is neither frozen nor unowned, and whose apply ends in markSynced", b.State)
	}
	if b.LocalHash != "newlocalhash" {
		t.Errorf("LocalHash = %q, want the un-pushed edit preserved verbatim", b.LocalHash)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "a.txt")); rerr != nil || string(got) != string(body) {
		t.Errorf("file = %q (err %v), want the refused delete to leave it untouched", got, rerr)
	}
}

// TestQueuedOutboundOwnsPathMatchesARenameDestination pins the endpoint rule: localReconcile
// relocates a renamed node's baseline to the DESTINATION, so an outbound rename owes its NewPath
// as much as its Path, and a repair consulting only the source key would restore a record its
// queued push still owns.
func TestQueuedOutboundOwnsPathMatchesARenameDestination(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7, Path: "old.txt",
		NewPath: "new.txt", FileID: "n1",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"new.txt", true},
		{"old.txt", true},
		{"other.txt", false},
	} {
		got, err := e.queuedOutboundOwnsPath(7, tc.path)
		if err != nil {
			t.Fatalf("queuedOutboundOwnsPath(%q): %v", tc.path, err)
		}
		if got != tc.want {
			t.Errorf("queuedOutboundOwnsPath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
	// A view mismatch never matches: the queue is global and the repair is per-view.
	if got, err := e.queuedOutboundOwnsPath(8, "new.txt"); err != nil || got {
		t.Errorf("queuedOutboundOwnsPath(view 8) = %v (err %v), want false", got, err)
	}
}

// TestInboundRemoveRefusalSurfacesTheDoomedSubtree pins the SUBTREE half of the refusal repair.
//
// Refusing the delete spares the successor directory, which is right - but the records BENEATH it
// describe the OLD directory's children, and those died with it: `remove` is recursive on the
// server, and a recursive remove reports only the directory it names (measured 27.08.2026,
// TestIntegration_RecursiveRemoveTombstones), so no later event will ever mention them. Left at
// StateSynced they claim agreement with dead nodes over local files that are still on disk, and
// nothing can notice: localReconcile drives StateSynced records and sees disk == LocalHash, the
// server never re-describes the nodes, cold start does not re-run. The subtree looks synchronised
// in the only replica that still holds it - the silent-unsyncable class H6 closes, reached through
// the refusal path.
//
// The repair marks them StateConflict: protected (every gate tests != StateSynced), counted by
// `Unresolved`, and never pushed - resurrecting a peer's deliberate deletion is a mutation this
// client cannot prove. The local bytes are never touched.
func TestInboundRemoveRefusalSurfacesTheDoomedSubtree(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	kept, fresh := seedSuccessorTree(t, st, viewDir, 400)

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// kept.txt was described at 6, below the delete at 101: its node went with the directory the
	// server removed recursively, so its record must no longer claim agreement.
	b, err := st.GetBaseline(7, "tree/kept.txt")
	if err != nil {
		t.Fatalf("kept.txt baseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("kept.txt baseline state = %q, want %q: its server node died with the recursively "+
			"removed directory and no later event can ever name it, so a synced record leaves the file "+
			"looking synchronised in the only replica that still holds it (no _Conflict, no dead-letter, "+
			"nothing in Unresolved)", b.State, store.StateConflict)
	}
	// The bytes are never touched - this repair surfaces, it does not act.
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "tree", "kept.txt")); rerr != nil || string(got) != string(kept) {
		t.Errorf("kept.txt on disk = %q (err %v), want it untouched (%q)", got, rerr, kept)
	}

	// The control: fresh.txt was described at 400, ABOVE the delete, so it belongs to the successor
	// standing here and must be left synced. Marking it would freeze a path that syncs perfectly.
	fb, err := st.GetBaseline(7, "tree/fresh.txt")
	if err != nil {
		t.Fatalf("fresh.txt baseline: %v", err)
	}
	if fb.State != store.StateSynced || fb.FileID != "fresh-new" {
		t.Errorf("fresh.txt baseline = %+v, want it still synced against fresh-new: a later stamp "+
			"proves the server described it AFTER the delete, so it is the successor's own child", fb)
	}
	if got, rerr := os.ReadFile(filepath.Join(viewDir, "tree", "fresh.txt")); rerr != nil || string(got) != string(fresh) {
		t.Errorf("fresh.txt on disk = %q (err %v), want it untouched", got, rerr)
	}
}

// TestInboundRemoveRefusalLeavesADoomedDescendantToItsQueuedPush is the subtree sweep's deference
// control. A descendant a queued OUTBOUND op still owes carries un-pushed local bytes in
// LocalHash (markLocalPending preserved the server facts and stamped the scan's hash over them),
// and its owner's apply expects the state it left. Marking it here would be the same forged
// verdict the node-level arm refuses to write - and the push is exactly what still has a chance
// of putting the content back on the server.
func TestInboundRemoveRefusalLeavesADoomedDescendantToItsQueuedPush(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	seedSuccessorTree(t, st, viewDir, 400)

	// A queued push for the doomed descendant, enqueued before the delete is reconciled.
	if _, err := st.Enqueue(store.Op{
		ViewID: 7, Type: store.OpUpload, Direction: store.DirOutbound, Path: "tree/kept.txt",
	}); err != nil {
		t.Fatalf("enqueue the push: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.surfaceSupersededDescendants(store.Op{
		ViewID: 7, Path: "tree", FileID: "dir-old", ServerSyncID: 101,
	}); err != nil {
		t.Fatalf("surfaceSupersededDescendants: %v", err)
	}

	b, err := st.GetBaseline(7, "tree/kept.txt")
	if err != nil {
		t.Fatalf("kept.txt baseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("kept.txt baseline state = %q, want it left at %q for its queued push: the owner's "+
			"apply expects the state it left, and the push is what can still restore the node",
			b.State, store.StateSynced)
	}
}

// TestInboundRemoveRefusalSparesAnUnstampedDescendant is the subtree sweep's evidence control,
// and it covers the shape the sweep's positional rule is most likely to get wrong.
//
// A descendant carrying NO server stamp is the ABSENCE of positional evidence, never proof that
// the recursive delete took it. Two ordinary paths produce one: a cold start stamps its whole
// catalog 0, and a file this client created itself carries 0 until its own create echo comes back
// (markSynced records no server position). The mixed-stamp subtree makes it reachable rather than
// theoretical - the delete is refused as soon as ANY descendant is stamped above it, and the
// sweep then sees the whole subtree, including a sibling whose upload echo has not been polled
// yet. Marking that file freezes a live, in-sync file: localReconcile drives StateSynced records
// only, so the user's later edits to it would never reach the server.
func TestInboundRemoveRefusalSparesAnUnstampedDescendant(t *testing.T) {
	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	seedSuccessorTree(t, st, viewDir, 400) // fresh.txt at 400 is what refuses the delete

	// The just-uploaded sibling: synced, live under the successor, echo not yet polled.
	fresh := []byte("uploaded moments ago; its create echo has not been polled")
	if err := os.WriteFile(filepath.Join(viewDir, "tree", "unstamped.txt"), fresh, 0o644); err != nil {
		t.Fatalf("seed unstamped.txt: %v", err)
	}
	if err := st.PutBaseline(7, "tree/unstamped.txt", store.Baseline{
		FileID: "unstamped-new", FileType: "file", ServerSyncID: 0, State: store.StateSynced,
		ServerHash: md4.ContentHash(fresh), LocalHash: md4.ContentHash(fresh), LocalSize: int64(len(fresh)),
	}); err != nil {
		t.Fatalf("seed unstamped baseline: %v", err)
	}

	if _, _, err := e.reconcile(7, []actions.Event{
		{SyncID: 101, Action: "delete", FileID: "dir-old", Path: "tree", IsDir: true},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	b, err := st.GetBaseline(7, "tree/unstamped.txt")
	if err != nil {
		t.Fatalf("unstamped.txt baseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("unstamped.txt baseline state = %q, want %q: a 0 stamp is the ABSENCE of positional "+
			"evidence, not proof the recursive delete took the node, and freezing a live in-sync file "+
			"stops the user's later edits from ever reaching the server", b.State, store.StateSynced)
	}
	// The genuinely doomed sibling (stamped 6, below the delete) is still surfaced.
	if kb, kerr := st.GetBaseline(7, "tree/kept.txt"); kerr != nil || kb.State != store.StateConflict {
		t.Errorf("kept.txt baseline = %+v (err %v), want %q: the stamp-0 exemption must not disarm "+
			"the sweep for a descendant the delete provably took", kb, kerr, store.StateConflict)
	}
}
