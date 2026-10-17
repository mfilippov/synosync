package engine

// These tests pin an evidence axis both destructive-remove preconditions once read in the
// wrong order. A query_node reply that carries a content hash but omits file_id (decodeNode
// tolerates the omission - actions/types.go - and the live DSM's reply shape is unconfirmed) is
// PROVEN divergence: the path holds content nobody agreed on, whoever owns it. Both consumers
// reached their verdict from the missing id BEFORE ever comparing the hash, so the proof was
// discarded: applyRemoveOut fell through to the destructive pc.Remove, and
// serverDivergedForVanishedUpload answered "not diverged" (propagating the local delete over the
// server's edit). On a DSM that systematically omits the id, the whole v14 precondition was inert
// for every delete.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundRemoveIdlessDivergentReplyFunnelsKeepBoth is the red test for the first half: the
// pre-turn probe answers with the server's NEW content hash but no file id. That difference is
// identity-free proof the path moved off our base, so the delete must funnel into the keep-both
// recovery (recoverDivergedRemove) instead of destroying the other client's edit. Before the fix
// the id-less arm short-circuited ahead of the hash comparison and pc.Remove ran.
func TestOutboundRemoveIdlessDivergentReplyFunnelsKeepBoth(t *testing.T) {
	body := []byte("another client edit, reported without a file id")
	c := &pushClient{
		// A present node whose content moved to h2 - but the reply omits file_id.
		queryNode: map[string]actions.Node{
			"gone.txt": {Hash: "h2", Size: int64(len(body)), MTime: 555},
		},
		// The recovery download is id-addressed and falls back to the id the op asserts.
		downloadContent: map[string][]byte{"f2": body},
		downloadNode: map[string]actions.Node{
			"f2": {FileID: "f2", Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: 555},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - proven divergence must never be deleted even without a file id", c.removes)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "gone.txt"))
	if err != nil {
		t.Fatalf("read recovered file: %v (the server version was not downloaded)", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("recovered content = %q, want %q", got, body)
	}
	b, err := st.GetBaseline(7, "gone.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	wantHash := md4.ContentHash(body)
	if b.State != store.StateSynced || b.LocalHash != wantHash || b.ServerHash != wantHash {
		t.Errorf("baseline = %+v, want synced at the recovered hash %s", b, wantHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-letter trace written for a race resolution: %d", dl)
	}
}

// TestOutboundRemoveIdlessMatchingHashStillDeletes pins the other side of the reordering: an
// id-less reply whose hash still MATCHES our base proves nothing diverged, so the genuine local
// delete reaches the server exactly as before. The new branch keys on inequality only.
func TestOutboundRemoveIdlessMatchingHashStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"gone.txt": {Hash: "h1", Size: 4, MTime: 111}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundRemoveIdlessHashLessReplyUnchanged is the documented-rule regression pin required
// by the plan: an id-less reply that carries NO hash either supplies no evidence at all, so the
// v14 rule ("with either hash missing, node identity decides") stands untouched and the delete
// proceeds. The fix must widen the divergence proof, never the guessing.
func TestOutboundRemoveIdlessHashLessReplyUnchanged(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"gone.txt": {Size: 4, MTime: 111}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt (the id-less/hash-less rule is unchanged)", c.removes)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the remove")
	}
}

// TestOutboundRemoveIdlessEmptyBaseStillDeletes pins the second guard on the new branch: with an
// EMPTY base (removeTargetFacts found neither a ServerHash nor a LocalHash) a present server
// hash must not read as divergence - that would stall every delete of a node we never recorded a
// hash for behind a pointless recovery download.
func TestOutboundRemoveIdlessEmptyBaseStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"gone.txt": {Hash: "h2", Size: 4, MTime: 111}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRaceBase(t, e, st, viewDir, "f2", "", "")

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v, want a single delete of gone.txt", c.removes)
	}
}

// TestOutboundRemoveForeignIdWinsOverHashDivergence pins the ONE ordering the fix deliberately
// leaves alone: a reply carrying a DIFFERENT file id is not missing identity, it is contradicting
// identity, and its arc (nodeGoneByID → idempotent ack or the MOVED wait) destroys nothing either
// way. Routing it into the divergence funnel instead would materialise a stranger's node on our
// path and lose the user's delete, so the different-id branch stays ahead of the hash comparison
// even when the stranger's content also differs from our base.
func TestOutboundRemoveForeignIdWinsOverHashDivergence(t *testing.T) {
	c := &pushClient{
		// A stranger's node, whose content is of course also unequal to our base.
		queryNode: map[string]actions.Node{
			"gone.txt": {FileID: "foreign", Hash: "hz", Size: 9, MTime: 777},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	seedRemoveDivergenceRace(t, e, st, viewDir)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 0 {
		t.Fatalf("remove calls = %+v, want none - a foreign node must never be deleted", c.removes)
	}
	if c.downloadCalls != 0 {
		t.Errorf("download calls = %d, want none - a foreign occupant is not a divergence to recover", c.downloadCalls)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the resolved stale delete")
	}
}

// TestVanishedUploadIdlessDivergentReplyNotPropagatedAsDelete is the mirror red test
// (outbound_vanished.go): an upload whose source vanished probes the server, which answers with a
// diverged content hash but no file id. Before the fix the id-less arm returned "not diverged"
// first, so the deferred remote update was dropped, the baseline was reverted to StateSynced and
// the next scan pushed an OpRemove over the server's edit. The hash proof now wins: the op
// funnels to the keep-both resolver and the local delete is not propagated.
func TestVanishedUploadIdlessDivergentReplyNotPropagatedAsDelete(t *testing.T) {
	c := &pushClient{
		// The server holds newer content at edit.txt, reported without a file id.
		queryNode: map[string]actions.Node{"edit.txt": {Hash: "serverh2", MTime: 555, Size: 12}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The resolve op the funnel enqueues cannot finish against an id-less reply (the resolver
	// requires a file id and leaves the op queued), so the drain reports the transient - the
	// point is that nothing was dropped or propagated as a delete.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the funnelled resolve to stay queued, got nil")
	}

	var pending []store.QueuedOp
	if err := st.ScanPending(func(qo store.QueuedOp) error {
		pending = append(pending, qo)
		return nil
	}); err != nil {
		t.Fatalf("ScanPending: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpResolveConflict || pending[0].Path != "edit.txt" {
		t.Fatalf("pending = %+v, want a single OpResolveConflict for edit.txt (the deferred remote update was dropped before the fix)", pending)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %v, want StatePending - reverting to synced makes the next scan push a delete over the server edit", b.State)
	}
}

// TestVanishedUploadIdlessMatchingHashStillDeletes pins the unchanged half: an id-less reply whose
// hash matches the base proves nothing was deferred, so the vanish is still a genuine local delete
// - the baseline reverts to synced for the next scan's OpRemove and no resolve op is created.
func TestVanishedUploadIdlessMatchingHashStillDeletes(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"edit.txt": {Hash: "basehash", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("local delete was resurrected (stat err = %v), want the file to stay absent", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "ef" {
		t.Errorf("baseline = %+v, want reverted to synced ef so the next scan emits the remove", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadIdlessHashLessReplyUnchanged pins the documented v14 rule on the mirror: with
// no hash on the reply the decision still falls to node identity, and an id-less reply supplies
// none - historical behaviour is a genuine local delete.
func TestVanishedUploadIdlessHashLessReplyUnchanged(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"edit.txt": {MTime: 555, Size: 9}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %v, want the unchanged local-delete resolution", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadFreshCreateIdlessReplyRecoversRemoteCreate covers the fresh-create half of
// the same branch-order bug. A create has no file id and no recorded base, so the hash-divergence
// arm cannot speak for it; the only question it asks is whether ANYTHING occupies its path, and
// anything that does is the remote create reconcile deferred to this upload's funnel (advancing
// the event cursor on that promise). Behind the id-less arm, a reply carrying content facts but no
// file_id read as "nothing to recover": the local delete was propagated and the remote create
// never materialised locally at all.
func TestVanishedUploadFreshCreateIdlessReplyRecoversRemoteCreate(t *testing.T) {
	c := &pushClient{
		// A node answers for new.txt with content facts but no file id.
		queryNode: map[string]actions.Node{"new.txt": {Hash: "serverh2", MTime: 555, Size: 12}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// A fresh create's shapes: no file id anywhere, no recorded server hash.
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileType: "file", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "new.txt", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// As in the modify case, the funnelled resolve cannot finish against an id-less reply and
	// stays queued; the point is that the recovery was enqueued at all.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the funnelled resolve to stay queued, got nil")
	}
	var pending []store.QueuedOp
	if err := st.ScanPending(func(qo store.QueuedOp) error {
		pending = append(pending, qo)
		return nil
	}); err != nil {
		t.Fatalf("ScanPending: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpResolveConflict || pending[0].Path != "new.txt" {
		t.Fatalf("pending = %+v, want a single OpResolveConflict for new.txt (the deferred remote create was dropped before the fix)", pending)
	}
}

// TestVanishedUploadFreshCreateEmptyReplyStillLocalDelete pins the evidence gate on that arm: a
// tolerant `file: {}` decodes to the zero node exactly like an absent one, and a bare {} is never
// proof. With nothing positive to read, the vanish stays a genuine local delete.
func TestVanishedUploadFreshCreateEmptyReplyStillLocalDelete(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"new.txt": {}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{
		FileType: "file", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "new.txt", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d - an empty reply is no evidence to recover from", n)
	}
}

// TestVanishedUploadServerDirStillLocalDelete pins the directory short-circuit kept ahead of the
// new branch: keep-both's recovery downloads a FILE onto the canonical path, so a path the server
// replaced with a directory is genuinely out of scope regardless of any hash on the reply.
func TestVanishedUploadServerDirStillLocalDelete(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "dirnode", IsDir: true, Hash: "serverh2"}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %v, want the unchanged local-delete resolution for a server directory", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}
