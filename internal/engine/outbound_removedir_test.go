package engine

// Two-client finding H7 (measured 14.08.2026; docs/INVARIANTS.md "recursive remove"): a live DSM's
// `remove` of a DIRECTORY is RECURSIVE, so an outbound directory delete destroys children this
// client never saw - the peer's file vanished from the server AND both replicas in 2.5 seconds,
// with empty queues and zero dead-letters. These tests pin the precondition that closes it: the
// subtree is enumerated SERVER-side before the destructive turn, and anything the baseline cannot
// account for refuses the delete instead of taking the peer's node with it.
//
// The fake is deliberately no kinder than the measured server: pushClient.Remove now drops the
// whole subtree from the scripted server state (§7's lesson - a guard "verified" against a fake
// that refuses to delete a non-empty directory is verified against a fiction).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// seedDirRemove stages the shape every case below starts from: a tracked, synced directory
// "tree" holding one tracked file "tree/kept.txt" last agreed at hash h1, both flipped
// StatePending by the local scan that saw the directory deleted on disk, plus the queued
// outbound OpRemove of the directory itself.
func seedDirRemove(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/kept.txt", store.Baseline{
		FileID: "fk", FileType: "file", ServerHash: "h1", LocalHash: "h1", LocalSize: 4,
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
}

// dirRemoveClient builds the push fake with the server tree the case scripts, always answering
// the pre-turn QueryNode of the directory itself with a plain directory node (so the single-node
// precondition passes and the subtree check is what decides).
func dirRemoveClient(listing map[string][]actions.Node) *pushClient {
	return &pushClient{
		queryNode:  map[string]actions.Node{"tree": {FileID: "fd", IsDir: true}},
		listResult: listing,
	}
}

// assertDirRemoveRefused pins the whole refusal contract: no destructive turn ran, the op left
// the queue with a durable trace naming the offending child, and the directory's baseline is
// StateConflict so the divergence stays visible in the Unresolved count.
func assertDirRemoveRefused(t *testing.T, st *store.Store, c *pushClient, wantChild string) {
	t.Helper()
	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the recursive delete must never run against an unproven subtree", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want 0 (the refusal must resolve, not wedge)", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead-letters = %d, want 1 (the refusal must leave a trace)", len(dls))
	}
	if !strings.Contains(dls[0].Reason, wantChild) {
		t.Errorf("dead-letter reason = %q, want it to name %q", dls[0].Reason, wantChild)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline(tree): %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("directory baseline state = %q, want StateConflict", b.State)
	}
}

// TestOutboundRemoveDirRefusesForeignChild is the red test for H7: the baseline knows one child,
// the server lists TWO (a peer added the second while this client was away), and the recursive
// `remove` would take the peer's file with it. Before the fix the delete went ahead and the file
// was destroyed on the server and in every replica with no _Conflict copy and no trace.
func TestOutboundRemoveDirRefusesForeignChild(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {
			{FileID: "fk", Name: "kept.txt", Hash: "h1"},
			{FileID: "ff", Name: "fresh.txt", Hash: "h2"},
		},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove refused and resolved, not wedged)", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/fresh.txt")
	// The peer's node is untouched in the modelled server state - the property the whole
	// precondition exists for.
	if len(c.listResult["tree"]) != 2 {
		t.Errorf("server subtree = %+v, want both children intact", c.listResult["tree"])
	}
}

// TestOutboundRemoveDirRefusesModifiedChild covers the second divergence shape: the child IS
// tracked, but the server holds content that is not the version this client last agreed on (a
// peer's edit landed after the local delete was scanned). Deleting would destroy that edit with
// no keep-both copy - the same never-lose rule the single-file precondition enforces.
func TestOutboundRemoveDirRefusesModifiedChild(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h2-peer-edit"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/kept.txt")
}

// TestOutboundRemoveDirRefusesModifiedChildWithoutServerHash pins the ServerHash→LocalHash
// fallback the divergence test rests on. A DSM that omits the hash from its upload reply leaves
// ServerHash empty on every locally created or modified file - the majority of a push-heavy view
// - and without the fallback `base` is empty, the comparison short-circuits, and a peer's edit to
// such a child is waved straight into the RECURSIVE delete. That is finding H7 with the whole
// suite green, because every other fixture here happens to seed a ServerHash.
func TestOutboundRemoveDirRefusesModifiedChildWithoutServerHash(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h2-peer-edit"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)
	// Re-seed the child the way a server with a hash-less upload reply leaves it: the agreed
	// content is recorded only as the local hash.
	if err := st.PutBaseline(7, "tree/kept.txt", store.Baseline{
		FileID: "fk", FileType: "file", ServerHash: "", LocalHash: "h1", LocalSize: 4,
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("re-seed child baseline: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/kept.txt")
}

// TestOutboundRemoveDirCorruptDescendantRefuses drives the arm the corrupt-TARGET test cites as
// its precedent but which nothing exercised: a record UNDER the directory that no longer decodes
// says nothing about whether the server node it describes is agreed content, so the delete must
// refuse rather than enumerate around it. Note the child is deliberately absent from the server
// listing, so only the pre-walk corrupt-record sweep can catch it.
func TestOutboundRemoveDirCorruptDescendantRefuses(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	seedDirRemove(t, st)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, 7, "tree/kept.txt")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	c := dirRemoveClient(map[string][]actions.Node{"tree": {}})
	e := New(st2, &concTransport{client: c}, root, "sess", nil)

	if err := drainSettled(t, e, st2); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st2, c, "tree/kept.txt")
}

// TestOutboundRemoveDirRefusesForeignNodeAtTrackedPath covers the takeover shape: the path is
// tracked, but a DIFFERENT server node holds it now (delete + recreate by a peer). Its content is
// not ours to delete either.
func TestOutboundRemoveDirRefusesForeignNodeAtTrackedPath(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "other", Name: "kept.txt", Hash: "h1"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/kept.txt")
}

// TestOutboundRemoveDirRefusesForeignChildInSubdirectory pins that the walk descends: the foreign
// node sits one level down, where a single non-recursive listing would never see it.
func TestOutboundRemoveDirRefusesForeignChildInSubdirectory(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree":     {{FileID: "fs", Name: "sub", IsDir: true}},
		"tree/sub": {{FileID: "fk", Name: "kept.txt", Hash: "h1"}, {FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed subdir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub/kept.txt", store.Baseline{
		FileID: "fk", FileType: "file", ServerHash: "h1", LocalHash: "h1", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/sub/fresh.txt")
}

// TestOutboundRemoveDirUnevidencedListingKeepsOpQueued pins the inconclusive arm: a successful
// turn that enumerated NOTHING (an errno-only reply, actions.Client.List's third state) is not
// proof the directory is empty. Reading its zero-length slice as "empty" is exactly how an
// unproven emptiness would authorise the recursive wipe, so the op stays queued and nothing is
// deleted or dead-lettered.
func TestOutboundRemoveDirUnevidencedListingKeepsOpQueued(t *testing.T) {
	c := dirRemoveClient(nil)
	c.listUnevidenced = map[string]bool{"tree": true}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue: want a transient error for the unverifiable subtree, got nil")
	}
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Errorf("drainQueue err = %v, want errRemoveDirUnverified", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v on an unevidenced listing", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0 (an inconclusive probe is transient)", dl)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline(tree): %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("directory baseline state = %q, want it left StatePending", b.State)
	}
}

// TestOutboundRemoveDirUnverifiedIsPacedAndEscalates pins that the unverifiable arm is not a
// silent spin. It carries no server result code of its own - an errno-only reply leaves a plain
// error, so ServerErrorCode answers false - and without a synthetic stand-in the streak cleared
// every drain, deferOpRetry armed nothing, and the op re-walked the WHOLE server subtree (one
// `list` turn per directory, on a worker lease) on every drain forever: finding H4's spin on the
// most expensive turn in the engine. Folded in under removeDirUnverifiedCode it paces like any
// repeat rejection and takes the ordinary observable exit once the evidence is in.
func TestOutboundRemoveDirUnverifiedIsPacedAndEscalates(t *testing.T) {
	c := dirRemoveClient(nil)
	c.listUnevidenced = map[string]bool{"tree": true}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	now := time.Unix(1_700_000_000, 0)
	e.now = func() time.Time { return now }

	// The first failure is never paced (a genuine transient must retry at once); the second arms
	// the geometric pause, which is what makes the wall-clock floor reachable without a busy loop.
	if err := e.drainQueue(context.Background()); !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue #1 = %v, want errRemoveDirUnverified", err)
	}
	if _, armed := e.opRetryPending(1); armed {
		t.Error("a first failure armed a pause; a genuine transient must retry immediately")
	}
	if err := e.drainQueue(context.Background()); !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue #2 = %v, want errRemoveDirUnverified", err)
	}
	if _, armed := e.opRetryPending(1); !armed {
		t.Fatal("the repeated non-answer armed no pause; the op re-walks the subtree every drain")
	}

	// Drive the streak to the conservative unknown-code window: the drop is decided by the
	// evidence, so both the count and the wall clock have to be satisfied.
	for range unmappedServerErrDrains + 2 {
		now = now.Add(unmappedServerErrMinAge)
		_ = e.drainQueue(context.Background())
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want the unverifiable delete dropped rather than wedging its subtree", n)
	}
	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; an unproven subtree must never be deleted, whatever the exit", c.removes)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "could not be enumerated") {
		t.Fatalf("dead-letters = %+v, want one naming the unenumerable subtree", dls)
	}
}

// TestOutboundRemoveDirOnDiskAgainRevertsAndAcks pins the door the queued-inbound discriminator
// cannot see. When the peer's child download was enqueued BEFORE the local delete was scanned, it
// holds the lower seq and applies FIRST - recreating the directory and markSync-ing the child - so
// by verification time that child is a synced, hash-matching baseline no queued op owes and the
// enumeration waves it through, recursively destroying the peer's node everywhere: H7 again. The
// directory standing on disk is the local proof of exactly that state, and it costs one Lstat
// before the lease.
func TestOutboundRemoveDirOnDiskAgainRevertsAndAcks(t *testing.T) {
	// The server subtree looks perfectly agreed - this is the shape the enumeration passes.
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h1"}},
	})
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)
	// The inbound download that raced ahead of this remove put the directory back.
	if err := os.MkdirAll(filepath.Join(viewDir, "tree"), 0o755); err != nil {
		t.Fatalf("seed re-materialised dir: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (the stale delete must resolve, not wedge)", err)
	}
	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the local deletion the op was derived from is no longer true", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want the stale delete acked", n)
	}
	// A race resolution, not a failure and not a divergence: no durable trace, and the baseline is
	// reverted to StateSynced so the next scan re-diffs the tree as it now is.
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0 (a race resolution leaves no trace)", dl)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline(tree): %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("directory baseline state = %q, want StateSynced so the next scan re-derives", b.State)
	}
}

// TestOutboundRemoveDirTypeReplacedOnDiskStillDeletes is the on-disk guard's control: detectRemoves
// deliberately emits an OpRemove for a path whose on-disk TYPE changed, so a FILE standing where
// the tracked directory was must NOT be read as "the directory came back" - that delete has to run
// before the replacement can be created.
func TestOutboundRemoveDirTypeReplacedOnDiskStillDeletes(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h1"}},
	})
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)
	if err := os.WriteFile(filepath.Join(viewDir, "tree"), []byte("a file replaced the directory"), 0o644); err != nil {
		t.Fatalf("seed replacement file: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want the stale directory node deleted so the replacement can be created", c.removes)
	}
}

// TestOutboundRemoveDirFailedListingKeepsOpQueued is the sibling arm: a listing that FAILS (a
// transport blip) is no evidence either, so the destructive turn does not run.
func TestOutboundRemoveDirFailedListingKeepsOpQueued(t *testing.T) {
	c := dirRemoveClient(nil)
	c.listErrByPath = map[string]error{"tree": actions.ErrConnBroken}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue: want a transient error for the failed listing, got nil")
	}
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Errorf("drainQueue err = %v, want errRemoveDirUnverified", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v on a failed listing", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1", n)
	}
}

// TestOutboundRemoveDirAgreedSubtreeStillDeletes is the regression pin: a directory whose server
// subtree holds exactly the content the baseline recorded is deleted exactly as before. The
// precondition must gate the peer's node, not the ordinary delete.
func TestOutboundRemoveDirAgreedSubtreeStillDeletes(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h1"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want a single remove of tree", c.removes)
	}
	if _, err := st.GetBaseline(7, "tree"); !isNotFound(err) {
		t.Errorf("GetBaseline(tree) err = %v, want not-found (the baseline is dropped on a clean delete)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want 0", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0 on a clean delete", dl)
	}
	// The fake models the measured recursive verb, so the whole subtree is gone server-side.
	if _, ok := c.listResult["tree"]; ok {
		t.Errorf("server subtree survived the recursive remove: %+v", c.listResult)
	}
}

// TestOutboundRemoveDirRefusesChildOwedByQueuedInboundOp pins the shape the first cut of this
// precondition let through, which is H7 again with a different door: reconcile records a
// StatePending baseline carrying the event's own FileID and ServerHash for an inbound create it
// has queued but NOT applied, so a peer's brand-new file - one this client has never held a byte
// of - passed the identity and hash tests exactly like agreed content and the recursive delete
// took it. The queue is the discriminator, and the ordering is not a coincidence: the directory's
// remove serialises AHEAD of the download (ancestor path, lower seq), so the owed op is always
// still queued when the destructive turn is about to run.
func TestOutboundRemoveDirRefusesChildOwedByQueuedInboundOp(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {
			{FileID: "fk", Name: "kept.txt", Hash: "h1"},
			{FileID: "ff", Name: "fresh.txt", Hash: "h2"},
		},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)
	// Exactly what reconcile leaves for an inbound create it has queued: server facts, no local
	// ones, StatePending - indistinguishable from an agreed record on identity and hash alone.
	if err := st.PutBaseline(7, "tree/fresh.txt", store.Baseline{
		FileID: "ff", FileType: "file", ServerHash: "h2", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed peer-create baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7,
		Path: "tree/fresh.txt", FileID: "ff",
	}); err != nil {
		t.Fatalf("Enqueue owed download: %v", err)
	}

	_ = e.drainQueue(context.Background())

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the peer's not-yet-downloaded file must never be wiped recursively", c.removes)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "tree/fresh.txt") {
		t.Fatalf("dead-letters = %+v, want one refusal naming tree/fresh.txt", dls)
	}
	if len(c.listResult["tree"]) != 2 {
		t.Errorf("server subtree = %+v, want both children intact", c.listResult["tree"])
	}
}

// TestOutboundRemoveDirUnlistableDescendantKeepsOpQueued pins that only the ROOT of the walk may
// read an ErrPathError listing as "nothing here". A DESCENDANT the parent's listing just
// enumerated as present is known to exist, and 28672 conflates a real absence with a server-side
// timeout everywhere in this codebase - so treating one such answer as "this directory holds
// nothing" would authorise the recursive wipe of a whole subtree from a single timed-out probe.
func TestOutboundRemoveDirUnlistableDescendantKeepsOpQueued(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fs", Name: "sub", IsDir: true}},
	})
	c.listErrByPath = map[string]error{"tree/sub": actions.ErrPathError}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed subdir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified for the unlistable descendant", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v on a subtree that could not be enumerated", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1 (an unprovable subtree is transient)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0", dl)
	}
}

// TestOutboundRemoveDirAbsentRootStillDeletes is the other side of that rule: the walk's ROOT
// answering ErrPathError still enumerates nothing - but only once the absence is CORROBORATED by
// repeated probes of the directory's own path, exactly like every other absence verdict. The
// delete then proceeds and the Remove's own postcondition arbitrates the path as before.
func TestOutboundRemoveDirAbsentRootStillDeletes(t *testing.T) {
	c := dirRemoveClient(nil)
	c.listErrByPath = map[string]error{"tree": actions.ErrPathError}
	// The directory is genuinely gone, so its own path answers the sentinel too - which is what
	// separates this case from the timed-out listing below.
	c.queryNode = nil
	c.queryNodeErrByPath = map[string]error{"tree": actions.ErrPathError}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want the delete to proceed against a corroborated absent root", c.removes)
	}
}

// TestOutboundRemoveDirAbsentRootUncorroboratedRefuses is the red test for the one door the H7
// precondition left open. 28672 conflates "no such path" with a server-side timeout, so a single
// ErrPathError listing of the walk's ROOT is not evidence the subtree is empty - and here the
// pre-turn QueryNode has just answered POSITIVELY for the very same path, so the contradiction is
// explicit. The walk used to read the listing as "nothing to enumerate" and hand the RECURSIVE
// Remove a subtree nothing had proved agreed, destroying a peer's node from one timed-out probe.
func TestOutboundRemoveDirAbsentRootUncorroboratedRefuses(t *testing.T) {
	c := dirRemoveClient(nil) // QueryNode("tree") answers a live directory node
	c.listErrByPath = map[string]error{"tree": actions.ErrPathError}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified while a node still answers for the root", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v on a subtree whose root listing was never corroborated", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1 (an unprovable subtree is transient)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0", dl)
	}
}

// TestOutboundRemoveFileTakesNoSubtreeListing pins that the precondition is DIRECTORY-only: a
// file delete must not pay a listing turn it cannot need (the baseline says "file", so there is
// no subtree to prove).
func TestOutboundRemoveFileTakesNoSubtreeListing(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"a.txt": {FileID: "fa"}}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h1", LocalHash: "h1", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", FileID: "fa",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "a.txt" {
		t.Fatalf("removes = %v, want a single remove of a.txt", c.removes)
	}
	if len(c.listCalls) != 0 {
		t.Errorf("listCalls = %v, want none for a file delete", c.listCalls)
	}
}

// TestOutboundRemoveDirRefusesDottedChildName is the red test for the untrusted-name arm. `list`
// is a SERVER reply, and path.Join CLEANS its result, so a child reported as "." resolves back
// onto the directory being enumerated: before the fix the walk pushed that directory onto its own
// queue again and re-listed it forever, on a held worker lease, with limits.op_turn_timeout_s
// shipped disabled and no errno ever arriving to classify. The test would hang rather than fail,
// so it is bounded by its own context.
func TestOutboundRemoveDirRefusesDottedChildName(t *testing.T) {
	for _, name := range []string{".", "..", "sub/deeper", ""} {
		t.Run("name="+name, func(t *testing.T) {
			c := dirRemoveClient(map[string][]actions.Node{
				"tree":     {{FileID: "fs", Name: "sub", IsDir: true}},
				"tree/sub": {{FileID: "fx", Name: name}},
			})
			e, st, _ := outboundEngine(t, c, "Photos", "root1")
			seedDirRemove(t, st)
			if err := st.PutBaseline(7, "tree/sub", store.Baseline{
				FileID: "fs", FileType: "dir", State: store.StatePending,
			}); err != nil {
				t.Fatalf("seed subdir baseline: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err := e.drainQueue(ctx)
			if ctx.Err() != nil {
				t.Fatalf("the subtree walk did not terminate on a child named %q", name)
			}
			if !errors.Is(err, errRemoveDirUnverified) {
				t.Errorf("drainQueue err = %v, want errRemoveDirUnverified for a child named %q", err, name)
			}
			if len(c.removes) != 0 {
				t.Errorf("Remove called %v on a subtree the walk could not read", c.removes)
			}
			if n, _ := st.PendingLen(); n != 1 {
				t.Errorf("op not left queued: %d, want 1", n)
			}
		})
	}
}

// TestOutboundRemoveDirAcceptsABackslashChildName pins the other side of safeChildName: the wire
// separator is "/" and only "/", so a backslash is an ordinary rune in a name a POSIX client can
// perfectly well hold. Refusing it made the walk unverifiable forever, so the directory above any
// such name could never be deleted again - the delete was paced and finally dead-lettered for a
// child the client had itself synced. Here the child IS known, so the walk must prove the subtree
// agreed and let the recursive delete through.
func TestOutboundRemoveDirAcceptsABackslashChildName(t *testing.T) {
	const odd = `a\b.txt`
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fodd", Name: odd, Hash: "h1"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)
	// seedDirRemove's own child is not part of this shape: replace the server listing's single
	// entry with the backslash-named one and record it as agreed content.
	if err := st.DeleteBaseline(7, "tree/kept.txt"); err != nil {
		t.Fatalf("drop the default child baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/"+odd, store.Baseline{
		FileID: "fodd", FileType: "file", ServerHash: "h1", LocalHash: "h1", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed the backslash child baseline: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Errorf("Remove calls = %v, want the directory delete to proceed on a proved subtree", c.removes)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("dead letters = %d, want 0; a backslash in a name is not an unidentifiable child", n)
	}
}

// TestOutboundRemoveDirRefusesOverSizedBaselineSubtree pins the LOCAL half of the same ceiling.
// The server walk was always capped; the baseline snapshot feeding it was not, so a directory
// whose recorded subtree dwarfs the budget paid the whole O(subtree) allocation on a destructive
// op under a worker lease, on every attempt, before any budget could refuse it. Over the cap is a
// divergence for the same reason the walk's own ceiling is: an unverifiable subtree must never be
// deleted, and re-reading it every drain would only re-hit the ceiling.
func TestOutboundRemoveDirRefusesOverSizedBaselineSubtree(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", Hash: "h1"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	e.limits.ColdStartCatalogCap = 1
	seedDirRemove(t, st)
	// A second recorded child the server listing does not even mention: the snapshot is over the
	// cap before the walk issues its first `list`.
	if err := st.PutBaseline(7, "tree/extra.txt", store.Baseline{
		FileID: "fx", FileType: "file", ServerHash: "h2", LocalHash: "h2", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed the over-cap child baseline: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree")
	dls, _ := st.ListDeadLetters()
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "baseline subtree exceeds") {
		t.Errorf("dead-letter reason = %+v, want it to name the baseline subtree cap", dls)
	}
	if len(c.listCalls) != 0 {
		t.Errorf("List calls = %v, want none: the refusal must precede the server walk", c.listCalls)
	}
}

// TestOutboundRemoveDirRefusesOverCap pins the ceiling arm. removeDirVerifyCap bounds the walk
// unconditionally, but the assertion here rides the opt-in limits.ColdStartCatalogCap because a
// test that seeded 50 000 nodes would prove the same thing far more slowly. Exceeding it is a
// DIVERGENCE, not a transient: an unverifiable subtree must never be deleted, and re-walking it
// every drain would only re-hit the same ceiling.
func TestOutboundRemoveDirRefusesOverCap(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {
			{FileID: "fk", Name: "kept.txt", Hash: "h1"},
			{FileID: "fk2", Name: "kept2.txt", Hash: "h1"},
		},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	e.limits.ColdStartCatalogCap = 1
	seedDirRemove(t, st)
	// Only the FIRST child is recorded, so the local snapshot stays inside the same budget and
	// the ceiling this case pins is the server walk's own: the second listed child is what
	// overruns it. (The baseline snapshot's twin ceiling has its own case above.)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree")
	dls, _ := st.ListDeadLetters()
	// "verification cap" alone would ALSO match the baseline-snapshot ceiling's message, so a
	// budget off-by-one routing this scenario through the sibling branch would still pass. Match
	// the half of the reason that only the SERVER walk produces.
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "the server subtree exceeds") {
		t.Errorf("dead-letter reason = %+v, want it to name the SERVER subtree's verification cap", dls)
	}
}

// TestSubtreeBaselinesStopsAtTheCap pins the bounded-work property the caller's refusal rests on:
// the stream stops as soon as the budget-th record is in hand, and reports the over-cap fact
// EXPLICITLY. Deleting the early return would leave every assertion about the refusal green while
// the snapshot silently paid the full O(subtree) allocation it exists to bound - under a worker
// lease, on a destructive op, on every attempt.
func TestSubtreeBaselinesStopsAtTheCap(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		if err := st.PutBaseline(7, "tree/"+name, store.Baseline{
			FileID: "f" + name, FileType: "file", ServerHash: "h", LocalHash: "h", State: store.StateSynced,
		}); err != nil {
			t.Fatalf("seed %q: %v", name, err)
		}
	}

	known, skipped, overCap, err := e.subtreeBaselines(7, "tree", 2)
	if err != nil {
		t.Fatalf("subtreeBaselines: %v", err)
	}
	if !overCap {
		t.Fatal("overCap = false for a 4-record subtree under a budget of 2")
	}
	if len(known) != 2 {
		t.Errorf("snapshot = %d records, want the stream to stop at the budget of 2 (it read the whole subtree)", len(known))
	}
	if len(skipped) != 0 {
		t.Errorf("skipped = %v, want none: every seeded record decodes", skipped)
	}

	// At the cap exactly, nothing is truncated and nothing is refused.
	known, _, overCap, err = e.subtreeBaselines(7, "tree", 4)
	if err != nil {
		t.Fatalf("subtreeBaselines at the cap: %v", err)
	}
	if overCap || len(known) != 4 {
		t.Errorf("at the cap: overCap=%v snapshot=%d, want false/4 (a subtree exactly at the budget is verifiable)",
			overCap, len(known))
	}
}

// TestQueuedInboundUnderPrefixSeesARenameDestination pins the arm the divergence discriminator
// needs for a node a peer is MOVING INTO the doomed directory. Such a node's queued op names the
// directory only through its NewPath - the source lies outside - so a check reading Path alone
// would find nothing owed, wave the enumeration through, and let the recursive server remove take
// the peer's node: finding H7 through the rename door.
func TestQueuedInboundUnderPrefixSeesARenameDestination(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		Path: "elsewhere/moved.txt", NewPath: "tree/moved.txt", FileID: "fm",
	}); err != nil {
		t.Fatalf("Enqueue inbound rename: %v", err)
	}
	// An OUTBOUND rename into the same directory proves nothing about the server holding a node
	// this client does not: only inbound work is owed.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "elsewhere/ours.txt", NewPath: "tree/ours.txt", FileID: "fo",
	}); err != nil {
		t.Fatalf("Enqueue outbound rename: %v", err)
	}

	owed, err := e.queuedInboundUnderPrefix(7, "tree")
	if err != nil {
		t.Fatalf("queuedInboundUnderPrefix: %v", err)
	}
	if _, ok := owed[normpath.Key("tree/moved.txt")]; !ok {
		t.Errorf("owed = %v, want the inbound rename's DESTINATION; only its source lies outside the directory", owed)
	}
	if _, ok := owed[normpath.Key("tree/ours.txt")]; ok {
		t.Errorf("owed = %v, want the OUTBOUND rename excluded; it owes the server nothing", owed)
	}
	if _, ok := owed[normpath.Key("elsewhere/moved.txt")]; ok {
		t.Errorf("owed = %v, want the source outside the prefix excluded", owed)
	}
}

// corruptTargetRemoveEngine stages the shape the three cases below share: a tracked directory
// "tree" whose own record has been bit-rotted after the outbound OpRemove was queued, and an op
// carrying NO BaseIsDir - so the record and the durable capture both fail to place the target and
// the pre-turn probe is the only witness left. Each case scripts that probe differently.
func corruptTargetRemoveEngine(t *testing.T, c *pushClient) (*Engine, *store.Store) {
	t.Helper()
	return corruptTargetRemoveEngineID(t, c, "fd")
}

// corruptTargetRemoveEngineID is corruptTargetRemoveEngine with the op's server file id under the
// caller's control, so the cases below can stage the ID-LESS shape: a DSM whose replies omit
// file_id leaves every synced record without one, and the probe that places a corrupt target's
// kind used to be gated behind exactly that field.
func corruptTargetRemoveEngineID(t *testing.T, c *pushClient, fileID string) (*Engine, *store.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: fileID,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, 7, "tree")

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	return New(st2, &concTransport{client: c}, root, "sess", nil), st2
}

// TestOutboundRemoveUnplacedTargetRefuses pins the arm that used to fail toward DELETING. The KIND
// of the delete target is exactly what undecodable bytes cannot supply, so a directory whose own
// record bit-rotted read "not a directory", skipped the whole recursive-delete precondition, and
// issued the destructive turn with nothing proved about its subtree. verifyServerSubtreeAgrees
// already refuses on a corrupt record UNDER the directory; the record OF the directory cannot be
// the exception.
//
// The refusal belongs to an UNPLACED target, though, not to an unreadable record: here the probe
// answers with a node asserting nothing (no is_dir, no hash - a sparse reply), so all three
// witnesses have failed and a recursive directory delete cannot be ruled out.
func TestOutboundRemoveUnplacedTargetRefuses(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"tree": {}}}
	e, st := corruptTargetRemoveEngine(t, c)

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified for a target no witness placed", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v; a delete must never be decided from an unplaced target", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1", n)
	}
}

// TestOutboundRemoveCorruptRecordStillVerifiesProbedDirectory pins that an unreadable record does
// not disarm the recursive-delete precondition when the PROBE places the target: is_dir is a fact
// the server cannot echo back from a path-addressed query_node, so a positive one arms the subtree
// walk on its own, and the peer's node under the directory is what refuses the delete.
func TestOutboundRemoveCorruptRecordStillVerifiesProbedDirectory(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	e, st := corruptTargetRemoveEngine(t, c)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree/fresh.txt")
}

// TestOutboundRemoveCorruptRecordDeletesProbedFile pins the other half of the same narrowing. An
// unreadable record used to refuse EVERY outbound remove, a plain file's included - and
// errRemoveDirUnverified is terminal by evidence (removeDirUnverifiedCode is no
// serverTransientRejection), so the delete dead-lettered and markOutboundConflict left a
// StateConflict record at a path with no local file, which localReconcile never drives again: the
// user's delete never reached the server and the server node was never brought back either.
//
// A file has no recursive-delete hazard to protect against, and the probe places it positively by
// the witness the stand measured - a file carries a content hash and a directory carries none.
func TestOutboundRemoveCorruptRecordDeletesProbedFile(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"tree": {FileID: "fd", Hash: "h1"}}}
	e, st := corruptTargetRemoveEngine(t, c)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the file delete to proceed)", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want the file delete to have run once", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want 0", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 0 {
		t.Errorf("dead-letters = %+v, want none; the delete succeeded", dls)
	}
}

// TestOutboundRemoveDirCapturedKindVerifiesWithoutRecord pins that an ABSENT target record does
// not disarm the recursive-delete precondition. The record is the kind's usual witness, but it is
// exactly the thing another path can drop between enqueue and apply (planRemove's own flip
// crashed, an ancestor's inbound teardown cascaded dropDescendantBaselines, a success-path
// DeleteBaseline whose Ack then failed). Read as "not a directory" it skipped the whole subtree
// verification and issued the RECURSIVE delete - the corrupt-record arm's fix with the same hole
// one branch up. op.BaseIsDir is planRemove's durable copy of the vanished record's kind, and it
// arms the walk on its own; the probe here deliberately reports no is_dir, so the op field is what
// the case measures.
func TestOutboundRemoveDirCapturedKindVerifiesWithoutRecord(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	c.queryNode = map[string]actions.Node{"tree": {FileID: "fd"}} // no is_dir: only the op knows
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// No baseline at all for "tree" - the record another path dropped.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
		FileID: "fd", BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; a recordless directory delete must still prove its subtree", c.removes)
	}
	if len(c.listResult["tree"]) != 1 {
		t.Errorf("server subtree = %+v, want the peer's node intact", c.listResult["tree"])
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "tree/fresh.txt") {
		t.Errorf("dead-letters = %+v, want one naming tree/fresh.txt", dls)
	}
}

// TestOutboundRemoveDirProbedKindVerifiesWithoutRecord is the same hole closed from the other
// side: an op produced before the kind was captured (BaseIsDir false) whose record is gone leaves
// the pre-turn QueryNode as the only witness. A positive is_dir is proof - a sparse reply omitting
// it is not, which is why it only ever ARMS the precondition and never disarms it.
func TestOutboundRemoveDirProbedKindVerifiesWithoutRecord(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the server's own is_dir must arm the subtree precondition", c.removes)
	}
	if len(c.listResult["tree"]) != 1 {
		t.Errorf("server subtree = %+v, want the peer's node intact", c.listResult["tree"])
	}
}

// TestOutboundRemoveDirIDLessProbesKindWithoutRecord closes the last door onto the RECURSIVE
// delete, the one where NONE of the three kind witnesses is even consulted. The op is id-less (a
// DSM that omits file_id leaves every synced record without one, so every outbound remove takes
// that path), its record is gone by apply time (planRemove's own flip crashed, an ancestor's
// teardown cascaded, a success-path DeleteBaseline whose Ack then failed) and it carries no
// BaseIsDir - the shape of an op queued before planRemove carried the stamp, or of a record that
// mistyped a directory because a sparse create event omitted is_dir.
//
// The pre-turn probe is then the only witness left, exactly as it is for a CORRUPT record - but
// the gate asked "was the record corrupt?" rather than "has anything placed the target?", and an
// absent record answers neither. So no probe ran, targetIsDir stayed false, the corrupt-record
// refusal could not fire either, and the destructive turn went out over a subtree nothing had
// proved agreed: finding H7, reached through the one combination every other test seeds away.
func TestOutboundRemoveDirIDLessProbesKindWithoutRecord(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	// The probe's is_dir is the only thing that can place the target; the reply carries no
	// file_id either, mirroring the server shape that produced the id-less op.
	c.queryNode = map[string]actions.Node{"tree": {IsDir: true}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// No baseline for "tree", no FileID and no BaseIsDir on the op.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; an id-less remove whose record is gone must still place its "+
			"target before a RECURSIVE delete", c.removes)
	}
	if len(c.listResult["tree"]) != 1 {
		t.Errorf("server subtree = %+v, want the peer's node intact", c.listResult["tree"])
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "tree/fresh.txt") {
		t.Errorf("dead-letters = %+v, want one naming tree/fresh.txt", dls)
	}
}

// TestOutboundRemoveDirRefusesChildReplacedByDirectory covers the type mismatch the walk used to
// descend straight through: the base records tree/kept.txt as a FILE and the server now reports a
// DIRECTORY there (a peer replaced the node). Descending proves nothing - an EMPTY replacement
// enumerates no children, so the whole verification returned nil and the recursive remove
// destroyed the peer's node everywhere: finding H7 for a path the base does know about.
func TestOutboundRemoveDirRefusesChildReplacedByDirectory(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fk", Name: "kept.txt", IsDir: true}},
		// "tree/kept.txt" is deliberately unscripted: the replacement directory is EMPTY, which
		// is what made the descend arm wave the delete through.
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/kept.txt")
}

// TestOutboundRemoveDirRefusesChildReplacedByFile is the MIRROR of the case above, and the one the
// is_dir flag cannot decide: the baseline records a DIRECTORY at the child and a peer replaced it
// with a file. A sparse reply omits is_dir, so !ch.IsDir is no evidence and the walk descends -
// which used to rest the whole never-lose guarantee on an UNMEASURED premise, that `list` of a
// FILE errors into the safe exit. This fake answers the other way (an unscripted path enumerates
// as an empty SUCCESSFUL listing, exactly as the sibling case's empty replacement directory does),
// and that is the shape that let the recursive delete destroy the peer's file: zero children seen,
// verification nil, remove issued.
//
// The evidence the refusal keys on is measured rather than assumed: a file's listing carries a
// content hash and "a DIRECTORY carries none" (docs/INTEGRATION.md).
func TestOutboundRemoveDirRefusesChildReplacedByFile(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		// Hash-bearing and without is_dir: a peer replaced the tracked directory with a file.
		// "tree/sub" itself is deliberately unscripted, so descending into it enumerates nothing.
		"tree": {{FileID: "fs", Name: "sub", Hash: "h9"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/sub")
}

// TestRemoveDirDivergedErrorMessageNamesTheChild pins the operator-facing text of a refusal that
// only ever reaches a human through a log line or a wrapped chain: every other case reads the
// structured fields, so the message itself was produced nowhere. It is the diagnostic of last
// resort for a destructive op the client deliberately refused to perform.
func TestRemoveDirDivergedErrorMessageNamesTheChild(t *testing.T) {
	err := error(&removeDirDivergedError{child: "tree/peer.txt", reason: "the server holds a node the baseline never recorded"})
	msg := fmt.Errorf("wrapped: %w", err).Error()
	for _, want := range []string{"tree/peer.txt", "the server holds a node the baseline never recorded", "refused"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not mention %q", msg, want)
		}
	}
}

// TestOutboundRemoveDirDoesNotDeleteOnAnAbsenceItsOwnProbeRefuted closes the last door the root
// arm of verifyServerSubtreeAgrees left open. That arm reads an ErrPathError LISTING of the
// directory as "nothing to enumerate" once the absence is corroborated by repeated path-addressed
// probes - but the corroboration streak was fed ONLY by the corroborating probes themselves, and
// applyRemoveOut's own pre-turn QueryNode of the very same path never touched it. So a server
// answering the path POSITIVELY moments before each listing failure still accumulated a clean run
// of sentinels, and two conflated 28672s authorised the recursive delete of a subtree nothing had
// enumerated - finding H7 through the contradiction the arm claims to resolve.
//
// The fake alternates deliberately: odd calls (the pre-turn probe) describe a live directory, even
// calls (the corroboration) answer the sentinel, which is what a briefly overloaded DSM looks like.
// Any answer that is not the sentinel breaks the run, so the absence is never proved and the
// destructive turn never runs.
func TestOutboundRemoveDirDoesNotDeleteOnAnAbsenceItsOwnProbeRefuted(t *testing.T) {
	var probes int
	c := dirRemoveClient(nil)
	c.listErrByPath = map[string]error{"tree": actions.ErrPathError}
	c.queryNodeHook = func(p string, _ int) (actions.Node, error, bool) {
		if p != "tree" {
			return actions.Node{}, nil, false
		}
		probes++
		if probes%2 == 1 {
			return actions.Node{FileID: "fd", IsDir: true}, nil, true
		}
		return actions.Node{}, actions.ErrPathError, true
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	// drainSettled runs the passes the corroboration needs, advancing the wall clock past
	// absenceStreakMinAge between them - a genuine absence converges inside it.
	_ = drainSettled(t, e, st)

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the recursive delete ran on an absence the same apply's own probe had just refuted", c.removes)
	}
	if probes < 2*absentNodeConfirmations {
		t.Errorf("probes = %d, want the walk to have kept asking (at least %d)", probes, 2*absentNodeConfirmations)
	}
}

// TestOutboundRemoveDirDescendsIntoADirectoryReportedWithAHash pins the kind-mismatch mirror's
// exclusion. The arm refuses base=dir/server=file on the CONTENT HASH, because a sparse reply
// omits is_dir and !ch.IsDir is therefore no evidence - but a POSITIVE is_dir is proof, exactly as
// the sibling arm above it trusts it, and the base agrees the child is a directory. Without the
// exclusion a server that decorated a folder listing with any hash-shaped value refused every
// directory delete with a subdirectory in it, TERMINALLY (dead-letter, no retry), abandoning the
// user's deletion for good on evidence of nothing.
func TestOutboundRemoveDirDescendsIntoADirectoryReportedWithAHash(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		// is_dir AND a hash: the kinds agree, so the walk must descend rather than read the
		// hash as "a peer replaced the directory with a file".
		"tree":     {{FileID: "fs", Name: "sub", IsDir: true, Hash: "h9"}},
		"tree/sub": {},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 {
		t.Fatalf("Remove calls = %v, want the proved-agreed subtree deleted once", c.removes)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letters = %d, want 0: the kinds agreed, so nothing diverged", dl)
	}
}

// TestOutboundRemoveDirRefusalSynthesisesADirectoryRecord covers the record a refusal leaves
// behind when there is none to flip: localReconcile enqueues the op BEFORE flipping the baseline,
// so a crash in between leaves the op with no record at all and markOutboundConflict rebuilds one
// from the op's own facts. Typed from the verb alone that rebuild says FILE, and a file-typed
// record at a DIRECTORY path is not inert: verifyServerSubtreeAgrees reads a server child reported
// as a directory over a file-typed baseline as "a peer replaced the node" and refuses TERMINALLY,
// so one mistyped record poisons the deletion of every ancestor above it. planRemove's durable
// capture (op.BaseIsDir) is the witness that survives the missing record.
func TestOutboundRemoveDirRefusalSynthesisesADirectoryRecord(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// No baseline for "tree" at all - only the op, carrying the kind planRemove captured.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
		FileID: "fd", BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the subtree held a node the baseline never recorded", c.removes)
	}
	b, err := st.GetBaseline(7, "tree")
	if err != nil {
		t.Fatalf("GetBaseline(tree): %v", err)
	}
	if b.FileType != fileType(true) {
		t.Errorf("synthesised record FileType = %q, want a directory: a file-typed record at a "+
			"directory path refuses every ancestor's delete terminally", b.FileType)
	}
	if b.State != store.StateConflict {
		t.Errorf("synthesised record state = %q, want StateConflict", b.State)
	}
}

// TestOutboundRemoveDirRefusesTargetReplacedByFile covers the kind-change divergence on the delete
// TARGET itself, the mirror of the child-level arm verifyServerSubtreeAgrees already refuses. The
// base places "tree" as a directory (record and durable capture agree) while the pre-turn probe
// answers a content hash with is_dir unset - the measured witness of a FILE (query_node carries a
// file's hash and a directory carries none) - because a peer deleted the directory and put a file
// there.
//
// Without the refusal the two witnesses simply disagreed and the directory arm won: the subtree
// walk `list`ed a path that is a FILE, and a server answering that the way it answers an EMPTY
// DIRECTORY (the shape the stand did measure) enumerates nothing, the verification returns nil, and
// the RECURSIVE remove destroys the peer's replacement on the server and in every replica. That
// premise - what `list` of a file does - is the unmeasured one the child arm exists not to rest on,
// so the fake models the destructive answer here deliberately.
func TestOutboundRemoveDirRefusesTargetReplacedByFile(t *testing.T) {
	c := dirRemoveClient(nil) // no listing for "tree": an enumeration that finds nothing
	// The peer's file, reported the way a file is: a hash, and no is_dir.
	c.queryNode = map[string]actions.Node{"tree": {FileID: "fd", Hash: "peer-h"}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	seedDirRemove(t, st)

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the delete refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree")
}

// TestOutboundRemoveFileProceedsWhenTheProbeReportsAFile is the companion that keeps the refusal
// above from being a liveness hole: the same probe answer (a hash, no is_dir) on a target the base
// ALSO records as a file is the ordinary shape of every file delete, and it must still delete.
func TestOutboundRemoveFileProceedsWhenTheProbeReportsAFile(t *testing.T) {
	c := &pushClient{
		queryNode:  map[string]actions.Node{"f.txt": {FileID: "fa", Hash: "h1"}},
		listResult: map[string][]actions.Node{".": nil},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "f.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h1", LocalHash: "h1", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "f.txt", FileID: "fa",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "f.txt" {
		t.Fatalf("Remove calls = %v, want the file delete to proceed", c.removes)
	}
}

// TestOutboundRemoveDirDescendsEachDirectoryOnce pins the walk's visited set. A `list` reply is
// untrusted input: a server that names the same child directory twice - or two spellings the host
// folds to one key - would otherwise be enumerated once per mention, and a cycle among such names
// would never terminate, all on a held worker lease with no turn timeout and no errno ever
// arriving. The de-dup is invisible in every other test (each fixture lists each child once), so
// removing it leaves the suite green.
func TestOutboundRemoveDirDescendsEachDirectoryOnce(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {
			{FileID: "fs", Name: "sub", IsDir: true},
			{FileID: "fs", Name: "sub", IsDir: true}, // the same child, reported twice
		},
		"tree/sub": nil,
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child dir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (the subtree agrees; the delete must proceed)", err)
	}
	n := 0
	for _, p := range c.listCalls {
		if p == "tree/sub" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("list calls for the twice-named child = %d (%q), want exactly 1: a reply naming a "+
			"directory twice must be enumerated once", n, c.listCalls)
	}
	if len(c.removes) != 1 {
		t.Fatalf("Remove calls = %v, want the proven-agreed subtree deleted once", c.removes)
	}
}

// TestOutboundRemoveAbsenceUnprovedIsPaced pins the DELETE-axis twin of the upload guard's absence
// pacing. The post-turn classification answers inconclusively (the parent listing itself fails, so
// neither absence nor a move is proved), and the op stays queued to re-classify later - correct,
// but the arm returned the bare mapped ErrPathError sentinel, which carries no server result code,
// so noteServerErr cleared the streak AND the pause and the op re-issued `remove` plus the parent
// `list` plus the corroborating probe on every drain for the whole corroboration window
// (absentNodeConfirmations probes spaced by absenceStreakMinAge). Every child of a directory the
// server's recursive remove already took enters exactly this state, which makes it the most
// frequently reached instance of finding H4's spin.
func TestOutboundRemoveAbsenceUnprovedIsPaced(t *testing.T) {
	// The parent listing fails with a non-sentinel error, so neither the ancestor walk nor a
	// corroborating probe can settle anything: the classification stays inconclusive however long
	// it runs, which is what lets the escalation window below be driven without the wait ending.
	c := &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"docs": actions.ErrConnBroken},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	advance := stubClock(e)
	seedRemoveOpAt(t, st, "docs/f.txt")

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveAbsenceUnproved) {
		t.Fatalf("drainQueue #1 = %v, want errRemoveAbsenceUnproved", err)
	}
	if !errors.Is(err, actions.ErrPathError) {
		t.Fatalf("drainQueue #1 = %v, want the mapped sentinel still visible to every existing classifier", err)
	}
	if _, armed := e.opRetryPending(1); armed {
		t.Error("a first failure armed a pause; a genuine transient must retry immediately")
	}
	if err := e.drainQueue(context.Background()); !errors.Is(err, errRemoveAbsenceUnproved) {
		t.Fatalf("drainQueue #2 = %v, want errRemoveAbsenceUnproved", err)
	}
	if _, armed := e.opRetryPending(1); !armed {
		t.Fatal("the repeated unproved absence armed no pause; the op re-issues remove + list + probe every drain")
	}
	// TRANSIENT: no run of it may ever dead-letter the delete - the streak is this client's own
	// evidence-gathering, not a server verdict.
	if !serverTransientRejection(removeAbsenceUnprovedCode) {
		t.Fatal("removeAbsenceUnprovedCode must be transient; a delete must not be dropped for its own postcondition")
	}
	for range unmappedServerErrDrains + 2 {
		advance(unmappedServerErrMinAge)
		_ = e.drainQueue(context.Background())
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("ops queued = %d, want the delete still queued: an unproved fate is not a verdict", n)
	}
	if dls, err := st.ListDeadLetters(); err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	} else if len(dls) != 0 {
		t.Errorf("dead-letters = %+v, want none", dls)
	}
}

// TestLocalReconcileStampsBaseIsDirOnAVanishedDirectory pins the PRODUCER of the durable kind
// capture every test above hand-seeds. planRemove stamps op.BaseIsDir from the vanished record, and
// that stamp is the witness the recursive-delete precondition falls back on when the record itself
// is gone by apply time (a crash between Enqueue and the baseline flip, an ancestor's teardown, a
// success-path DeleteBaseline whose Ack failed). Un-stamped, removeTargetFacts' absent record reads
// "not a directory", verifyServerSubtreeAgrees never runs, and the RECURSIVE server delete goes out
// with nothing proved about the subtree - finding H7 through the one door the capture closes. With
// every H7 test seeding the field by hand, dropping the stamp left the suite green.
func TestLocalReconcileStampsBaseIsDirOnAVanishedDirectory(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	// One file that IS on disk, so the scan is not empty (an empty scan is debounced, not diffed).
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "fk", "file", md4.ContentHash(keep), int64(len(keep)))
	// Two synced records whose entities the user deleted: a directory and a file.
	putSynced(t, e, viewID, "gone-dir", "fd", "dir", "", 0)
	putSynced(t, e, viewID, "gone.txt", "fg", "file", "h1", 4)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	got := map[string]store.Op{}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			got[op.Path] = op
		}
	}
	if len(got) != 2 {
		t.Fatalf("removes = %+v, want one for each vanished record", ops)
	}
	if !got["gone-dir"].BaseIsDir {
		t.Errorf("OpRemove(gone-dir).BaseIsDir = false, want true: without the capture a lost record "+
			"disarms the recursive-delete precondition (op = %+v)", got["gone-dir"])
	}
	if got["gone.txt"].BaseIsDir {
		t.Errorf("OpRemove(gone.txt).BaseIsDir = true, want false: a file must not arm a directory "+
			"precondition (op = %+v)", got["gone.txt"])
	}
}

// TestOutboundRemoveDirUnlistableDescendantRefusesEvenWithAnAbsentRoot is the discriminating half
// of the root/descendant rule. Its sibling above leaves the root ANSWERING, so the corroboration
// refuses on both the correct code and one that dropped the `dir == op.Path` guard - the probe
// always addresses op.Path, never dir, so a descendant taking the root arm would be corroborated
// against the wrong path. Here the root's own absence IS corroborated (its query_node answers the
// sentinel every pass) while a descendant the root listing just enumerated as present cannot be
// listed. A walk that let the descendant take the root arm would read that corroboration as
// permission to enumerate nothing, fall through, and issue the RECURSIVE Remove over a subtree it
// never proved agreed - finding H7 through the one token that separates the two arms.
func TestOutboundRemoveDirUnlistableDescendantRefusesEvenWithAnAbsentRoot(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fs", Name: "sub", IsDir: true}},
	})
	c.listErrByPath = map[string]error{"tree/sub": actions.ErrPathError}
	// The root's path-addressed probe agrees it is gone, on every pass: the corroboration the
	// ROOT arm needs is fully satisfied, so only the arm selection can decide this case.
	c.queryNode = nil
	c.queryNodeErrByPath = map[string]error{"tree": actions.ErrPathError}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed subdir baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	err := drainSettled(t, e, st)
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified: a descendant's unlistable subtree "+
			"is never proved by the ROOT's corroborated absence", err)
	}
	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v; the descendant's subtree was never enumerated", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1 (an unprovable subtree is transient)", n)
	}
}

// TestOutboundRemoveDirSparseChildStillDeletes pins the EMPTINESS guards on the child comparison
// arms. Each case is a tracked child the server lists without one of the three facts the arms
// compare - no file id, no content hash, or a baseline that never recorded one - and in every case
// nothing contradicts the baseline, so the delete must proceed. Dropping any of those guards turns
// a missing fact into "differs", which refuses TERMINALLY (dead-letter, no retry) and abandons the
// user's deletion for good against a DSM whose listings are sparse - the same "evidence of nothing"
// failure the kind-change arm's is_dir exclusion exists to avoid.
func TestOutboundRemoveDirSparseChildStillDeletes(t *testing.T) {
	cases := []struct {
		name  string
		child actions.Node
		base  store.Baseline
	}{
		{
			name:  "listing omits the file id",
			child: actions.Node{Name: "kept.txt", Hash: "h1"},
			base:  store.Baseline{FileID: "fk", FileType: "file", ServerHash: "h1", LocalHash: "h1"},
		},
		{
			name:  "listing omits the content hash",
			child: actions.Node{FileID: "fk", Name: "kept.txt"},
			base:  store.Baseline{FileID: "fk", FileType: "file", ServerHash: "h1", LocalHash: "h1"},
		},
		{
			name:  "baseline recorded no hash at all",
			child: actions.Node{FileID: "fk", Name: "kept.txt", Hash: "h1"},
			base:  store.Baseline{FileID: "fk", FileType: "file"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dirRemoveClient(map[string][]actions.Node{"tree": {tc.child}})
			e, st, _ := outboundEngine(t, c, "Photos", "root1")
			if err := st.PutBaseline(7, "tree", store.Baseline{
				FileID: "fd", FileType: "dir", State: store.StatePending,
			}); err != nil {
				t.Fatalf("seed dir baseline: %v", err)
			}
			tc.base.State = store.StatePending
			if err := st.PutBaseline(7, "tree/kept.txt", tc.base); err != nil {
				t.Fatalf("seed child baseline: %v", err)
			}
			if _, err := st.Enqueue(store.Op{
				Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
			}); err != nil {
				t.Fatalf("Enqueue remove: %v", err)
			}

			if err := drainSettled(t, e, st); err != nil {
				t.Fatalf("drainQueue: %v", err)
			}

			if len(c.removes) != 1 || c.removes[0] != "tree" {
				t.Fatalf("removes = %v, want the delete to proceed: a fact the listing omits is not a divergence", c.removes)
			}
			if dl, _ := st.DeadLetterLen(); dl != 0 {
				t.Errorf("dead-letters = %d, want 0: a sparse listing must not refuse terminally", dl)
			}
		})
	}
}

// TestOutboundRemoveCorruptRecordIDLessDeletesProbedFile closes the last door the corrupt-record
// narrowing left open. The kind-placing probe used to be gated behind op.FileID, so an ID-LESS
// remove could never be placed BY CONSTRUCTION: no probe ran, the refusal fired on every attempt,
// and - errRemoveDirUnverified being terminal by evidence - the delete dead-lettered with a
// StateConflict record at a path holding no local file, which localReconcile never drives again.
//
// The shape is not exotic: a DSM whose replies omit file_id (a shape modelled
// throughout, and the one that made the identity precondition inert) leaves EVERY synced record
// id-less, so on such a server one bit-rotted record froze the user's delete permanently.
func TestOutboundRemoveCorruptRecordIDLessDeletesProbedFile(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"tree": {Hash: "h1"}}}
	e, st := corruptTargetRemoveEngineID(t, c, "")

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the id-less file delete to proceed)", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want the file delete to have run once", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want 0", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 0 {
		t.Errorf("dead-letters = %+v, want none; the delete succeeded", dls)
	}
}

// TestOutboundRemoveCorruptRecordIDLessVerifiesProbedDirectory pins the protective half of the
// same ungating: an id-less probe that answers is_dir arms the recursive-delete precondition just
// as an id-ful one does, and the peer's node under the directory refuses the delete. Placing the
// target may never become a licence to skip the subtree walk.
func TestOutboundRemoveCorruptRecordIDLessVerifiesProbedDirectory(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	c.queryNode = map[string]actions.Node{"tree": {IsDir: true}} // no file_id: the DSM omits it
	e, st := corruptTargetRemoveEngineID(t, c, "")

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree/fresh.txt")
}

// TestOutboundRemoveCorruptRecordIDLessUnplacedStillRefuses pins that the ungating ADDS a witness
// and removes no refusal: a sparse id-less reply (no is_dir, no hash) still places nothing, and a
// recursive directory delete still cannot be ruled out.
func TestOutboundRemoveCorruptRecordIDLessUnplacedStillRefuses(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"tree": {}}}
	e, st := corruptTargetRemoveEngineID(t, c, "")

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified for a target no witness placed", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v; a delete must never be decided from an unplaced target", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1", n)
	}
}

// TestOutboundRemoveAbsentRecordUnplacedTargetRefuses closes the unplaced-target gate's other
// door. The gate is "no witness PLACED the target", and an ABSENT record answers the kind exactly
// as little as a corrupt one does - but the refusal keyed on the corrupt case alone, so the absent
// record's zero value fell through as a placed FILE.
//
// The record is gone at apply time through a documented window (a crash between localReconcile's
// Enqueue and planRemove's flip, an ancestor's teardown, a success-path DeleteBaseline whose Ack
// failed); op.BaseIsDir is false, as it is for any op queued before that stamp existed or one
// whose record mistyped a directory; and the probe answers a sparse node carrying neither is_dir
// nor a hash. All three witnesses have failed, so the RECURSIVE server remove - which takes every
// child, including ones no local baseline ever saw - must not go out.
func TestOutboundRemoveAbsentRecordUnplacedTargetRefuses(t *testing.T) {
	c := &pushClient{queryNode: map[string]actions.Node{"tree": {}}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// No baseline at all: the record is the witness that is missing.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	err := e.drainQueue(context.Background())
	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v, want errRemoveDirUnverified for a target no witness placed", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("Remove called %v; a delete must never be decided from an unplaced target", c.removes)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d, want 1", n)
	}
}

// TestOutboundRemoveAbsentRecordPathErrorStillProceeds pins the fall-through the gate above must
// NOT swallow. With the record merely absent (not corrupt) and the probe reporting the path-error
// sentinel, the ordinary reading is that the node is gone: the Remove and its parent-listing
// postcondition are the arbiters that were always meant to settle it. Refusing here instead would
// freeze the idempotent re-delete of an already-deleted node - the delete dead-letters on
// removeDirUnverifiedCode and markOutboundConflict leaves a StateConflict record at a path with no
// local file, which localReconcile never drives again.
func TestOutboundRemoveAbsentRecordPathErrorStillProceeds(t *testing.T) {
	c := &pushClient{
		queryNodeErr: actions.ErrPathError, // ambiguous: absent or a server-side timeout
		removeErr:    actions.ErrPathError, // and the delete itself finds nothing to remove
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f1",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	// The delete is issued. Its own postcondition then owns the outcome - here the parent listing
	// cannot yet place the target, so the op stays queued as the TRANSIENT errRemoveAbsenceUnproved
	// (paced, never dead-lettered), which is the machinery that was always meant to arbitrate the
	// sentinel. What must not happen is the unplaced-target refusal pre-empting it.
	err := e.drainQueue(context.Background())
	if errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("drainQueue err = %v; an absent record plus the path-error sentinel must not be "+
			"refused as an unplaced target, or an already-deleted node can never be re-deleted", err)
	}
	if !errors.Is(err, errRemoveAbsenceUnproved) {
		t.Fatalf("drainQueue err = %v, want the transient absence-unproved classification", err)
	}
	if len(c.removes) != 1 {
		t.Fatalf("removes = %v, want the delete to have been attempted", c.removes)
	}
}

// TestOutboundRemoveDirBareChildRecordStillDeletes pins the subtree kind-change arm on the
// evidence it is meant to key on. A tracked child DIRECTORY whose record carries no FileType -
// the bare store.Baseline{FileID: …} applyRename writes on its overwrite/no-facts arms, forced
// StateSynced - recorded nothing about the kind; it did not record a FILE. Reading the unset type
// as positive file evidence refused the delete TERMINALLY (applyOutbound dead-letters a
// removeDirDivergedError on the first occurrence), so the user's deletion of every ancestor above
// such a child was abandoned for good, with a StateConflict record left at a path holding no local
// directory - which localReconcile never drives again.
func TestOutboundRemoveDirBareChildRecordStillDeletes(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		"tree": {{FileID: "fs", Name: "sub", IsDir: true}},
		// "tree/sub" is deliberately unscripted: an empty SUCCESSFUL listing, so the descend arm
		// verifies the replacement subtree rather than the refusal short-circuiting it.
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// The bare record: an id and nothing else, exactly as applyRename leaves it.
	if err := st.PutBaseline(7, "tree/sub", store.Baseline{
		FileID: "fs", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed bare child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree", FileID: "fd",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "tree" {
		t.Fatalf("removes = %v, want a single remove of tree (a bare record states no kind, so the walk verifies it)", c.removes)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		dls, _ := st.ListDeadLetters()
		t.Fatalf("dead-letters = %d (%+v), want 0: an unset FileType is not evidence of a file", dl, dls)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("ops still queued: %d, want 0", n)
	}
}

// TestOutboundRemoveDirIDLessTargetReplacedByFileRefuses keeps the delete target's own
// kind-change refusal REACHABLE for the shape that reaches it in production. Gating the pre-turn
// probe on "no witness has placed the kind yet" made it unreachable for an id-less op whose record
// decoded: the record placed the kind, so no probe ran and targetProvenFile could never be set -
// and against a DSM whose replies omit file_id every outbound remove is id-less. A peer replacing
// the tracked directory with a FILE then went undetected, verifyServerSubtreeAgrees `list`ed a
// path that is a file, and a server answering that the way it answers an empty directory
// enumerates nothing and lets the RECURSIVE remove destroy the replacement.
func TestOutboundRemoveDirIDLessTargetReplacedByFileRefuses(t *testing.T) {
	c := dirRemoveClient(nil) // no listing for "tree": an enumeration that would find nothing
	// The peer's file, reported the way a file is: a content hash and no is_dir.
	c.queryNode = map[string]actions.Node{"tree": {FileID: "fd", Hash: "peer-h"}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// The id-less op: the record placed the kind, so the OLD gate skipped the probe entirely.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the delete refused and resolved, not wedged)", err)
	}
	assertDirRemoveRefused(t, st, c, "tree")
}
