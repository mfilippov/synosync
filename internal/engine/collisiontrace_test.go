package engine

// Tests for the durable trace behind a fold-key collision (two-client finding H6): two on-disk
// names that fold to one normpath.Key were already protected - neither is synced, neither is
// clobbered - but nothing told the operator. The stand measured the result: a file that looks
// synchronised, is excluded from every pass, and would never be backed up, with `status` clean.
//
// The collision fixtures need a byte-preserving host filesystem to hold both the NFD and the NFC
// spelling; a normalizing one (macOS APFS) coalesces them, so those tests probe and skip rather
// than branch on GOOS.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// collisionTraces returns the dead-letter records the engine recorded for fold collisions.
func collisionTraces(t *testing.T, st *store.Store) []store.DeadLetteredOp {
	t.Helper()
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	return dls
}

// TestFoldCollisionWithSyncedTwinIsTraced is the red test: the NFC spelling is synced, the NFD
// one sits beside it on disk, and before the fix the second file produced no op, no dead letter
// and no stuck count - nothing an operator could see. The pass must now leave a durable trace
// naming the already-synced twin, and it must record exactly ONE however long the collision
// lasts (a record per 5s scan would retire the whole FIFO ring within the hour).
func TestFoldCollisionWithSyncedTwinIsTraced(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	putSynced(t, e, viewID, nameNFC, "f1", "file", md4.ContentHash([]byte("one")), 3)

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0 (both files stay protected): %+v", len(ops), ops)
	}
	dls := collisionTraces(t, e.store)
	if len(dls) != 1 {
		t.Fatalf("got %d dead letters, want 1 (the collision must be observable): %+v", len(dls), dls)
	}
	dl := dls[0]
	if dl.ViewID != viewID || dl.Path != nameNFC {
		t.Errorf("trace = view %d path %q, want view %d path %q", dl.ViewID, dl.Path, viewID, nameNFC)
	}
	// The type is the trace's own, not a placeholder borrowed from a queue op: it is what
	// seedCollisionNoted recognises in the durable ring after a restart, and pinning it here is
	// what keeps that recognition from resting on the wording of the operator-facing Reason.
	if dl.Type != store.OpFoldCollision || dl.Direction != store.DirOutbound {
		t.Errorf("trace = %s/%s, want the fold-collision type outbound (the push that can never be made)", dl.Type, dl.Direction)
	}
	if !strings.Contains(dl.Reason, "already-synced") || !strings.Contains(dl.Reason, nameNFC) {
		t.Errorf("reason = %q, want it to name the already-synced twin", dl.Reason)
	}
	if dl.RecordedAt == 0 {
		t.Error("trace has no RecordedAt stamp")
	}
	// DeadLetterLen is what `status` reports as Dropped - the whole point of the trace is
	// that the operator can see the file the client will never sync.
	if n, err := e.store.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = (%d, %v), want (1, nil): status must count the collision", n, err)
	}

	// A persisting collision is traced once, not once per scan.
	for i := 0; i < 3; i++ {
		if _, err := e.localReconcile(context.Background(), viewID); err != nil {
			t.Fatalf("repeat localReconcile: %v", err)
		}
	}
	if got := len(collisionTraces(t, e.store)); got != 1 {
		t.Fatalf("got %d dead letters after four passes, want 1 (one trace per collision)", got)
	}
}

// TestFoldCollisionTraceSurvivesARestart pins that the once-per-key rule is durable, not merely
// process-local. The condition it suppresses is on-disk state that outlives the daemon, so a memo
// held only in memory re-recorded a trace for every still-colliding key on every start - and a
// supervised daemon in a restart loop churned exactly the 1000-entry FIFO ring the rule exists to
// protect, evicting the real op-drop traces while `Dropped` climbed for a condition that never
// recurred. A restart is modelled the way the engine sees one: the same store, a fresh memo.
func TestFoldCollisionTraceSurvivesARestart(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	putSynced(t, e, viewID, nameNFC, "f1", "file", md4.ContentHash([]byte("one")), 3)
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if got := len(collisionTraces(t, e.store)); got != 1 {
		t.Fatalf("got %d dead letters, want 1 before the restart", got)
	}

	for i := 0; i < 3; i++ {
		e.collisionNoted = make(map[uint64]map[string]bool)
		e.collisionSeeded = make(map[uint64]bool)
		if _, err := e.localReconcile(context.Background(), viewID); err != nil {
			t.Fatalf("post-restart localReconcile %d: %v", i, err)
		}
		if got := len(collisionTraces(t, e.store)); got != 1 {
			t.Fatalf("got %d dead letters after restart %d, want 1 (the durable ring is the memory)", got, i)
		}
	}
	if !e.collisionNoted[viewID][normpath.Key(nameNFC)] {
		t.Error("the memo was not seeded from the durable ring; the next pass would re-record")
	}
}

// TestFoldCollisionTraceClearsAfterRename pins the other end of the memo: once the user renames
// one of the two files the key stops colliding, the engine forgets it, and a LATER collision at
// the same key is news again. Without the forgetting, a user who fixed the collision and later
// re-created it would get no trace at all.
func TestFoldCollisionTraceClearsAfterRename(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	putSynced(t, e, viewID, nameNFC, "f1", "file", md4.ContentHash([]byte("one")), 3)
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if got := len(collisionTraces(t, e.store)); got != 1 {
		t.Fatalf("got %d dead letters, want 1 before the rename", got)
	}

	// The user renames the NFD twin out of the way: the key is free again.
	if err := os.Rename(filepath.Join(viewDir, nameNFD), filepath.Join(viewDir, "renamed.txt")); err != nil {
		t.Fatalf("rename the colliding twin: %v", err)
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("post-rename localReconcile: %v", err)
	}
	if got := len(collisionTraces(t, e.store)); got != 1 {
		t.Fatalf("got %d dead letters, want 1 (a resolved collision records nothing new)", got)
	}
	if e.collisionNoted[viewID][normpath.Key(nameNFC)] {
		t.Error("the resolved collision is still remembered; a recurrence would go untraced")
	}

	// It comes back - a new trace, because this is a new incident.
	writeFile(t, viewDir, nameNFD, []byte("two again"))
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("recurrence localReconcile: %v", err)
	}
	if got := len(collisionTraces(t, e.store)); got != 2 {
		t.Fatalf("got %d dead letters, want 2 (the recurrence must be traced afresh)", got)
	}
}

// TestFoldCollisionMemoSurvivesAnUnobservedScan pins the distinction the forget loop rests on: an
// EMPTY collision set means "the scan proved these keys free" only when the scan actually
// enumerated the view. protectCollisions runs BEFORE the empty-scan debounce, so a pass over an
// unavailable root (unmounted, missing, transiently unreadable) arrives with an empty set that
// proves nothing. Retiring the memo there re-armed the recording for every still-colliding key,
// and seedCollisionNoted - already run for the view - could not correct it: one mount flap per
// pass wrote a duplicate trace and evicted the genuine op-drop traces the ring exists for.
func TestFoldCollisionMemoSurvivesAnUnobservedScan(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	key := normpath.Key("café.txt")
	collisions := map[string][]string{key: {"café.txt", "café.txt"}}

	e.noteCollisionTraces(7, collisions, true)
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1 after the first observed pass", n)
	}

	// The mount flaps: the scan enumerates nothing, so it observed nothing about this key.
	e.noteCollisionTraces(7, nil, false)
	if !e.collisionNoted[7][key] {
		t.Fatal("the memo was retired by a pass that enumerated nothing; the key would be re-traced")
	}
	// It comes back on the next healthy pass - still one trace, not two.
	e.noteCollisionTraces(7, collisions, true)
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1: a mount flap must not duplicate the trace", n)
	}

	// An OBSERVED pass that finds the key free still retires it, so a recurrence is news again.
	e.noteCollisionTraces(7, nil, true)
	if e.collisionNoted[7][key] {
		t.Fatal("an observed collision-free pass must retire the memo")
	}
	e.noteCollisionTraces(7, collisions, true)
	if n, _ := st.DeadLetterLen(); n != 2 {
		t.Errorf("DeadLetterLen = %d, want 2: the recurrence is a new incident", n)
	}
}

// TestFoldCollisionLocalOnlyTraced covers the other reason shape: neither name is tracked, so
// there is no "already-synced" twin to blame and both files are equally stranded.
func TestFoldCollisionLocalOnlyTraced(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	dls := collisionTraces(t, e.store)
	if len(dls) != 1 {
		t.Fatalf("got %d dead letters, want 1: %+v", len(dls), dls)
	}
	if r := dls[0].Reason; strings.Contains(r, "already-synced") || !strings.Contains(r, "fold to one sync key") {
		t.Errorf("reason = %q, want the untracked-pair wording", r)
	}
}

// TestFoldCollisionPendingBaselineIsNotTheSyncedTwin: a record still in flight is work, not an
// agreement the second file is losing to, so it must not be reported as the synced holder of
// the key.
func TestFoldCollisionPendingBaselineIsNotTheSyncedTwin(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	if !distinctOnDisk(t, viewDir) {
		t.Skip("host filesystem coalesced NFD/NFC names; collision scenario not reproducible here")
	}
	if err := e.store.PutBaseline(viewID, nameNFC, store.Baseline{
		FileID: "f1", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	dls := collisionTraces(t, e.store)
	if len(dls) != 1 {
		t.Fatalf("got %d dead letters, want 1: %+v", len(dls), dls)
	}
	if strings.Contains(dls[0].Reason, "already-synced") {
		t.Errorf("reason = %q, want no synced twin named for a pending record", dls[0].Reason)
	}
}

// TestNoCollisionRecordsNothing is the containment guard: a healthy tree must never grow a
// dead-letter trace out of the collision path.
func TestNoCollisionRecordsNothing(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, viewDir := newScanEngine(t)
	writeFile(t, viewDir, "a.txt", []byte("one"))
	writeFile(t, viewDir, "b.txt", []byte("two"))
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if got := len(collisionTraces(t, e.store)); got != 0 {
		t.Fatalf("got %d dead letters on a collision-free tree, want 0", got)
	}
}

// TestColdStartFoldCollisionTraced: the cold-start merge protects colliding local names exactly
// like the incremental pass, so it must make them just as visible - a first-ever sync of a tree
// holding both spellings is precisely when the user has no other way to notice.
func TestColdStartFoldCollisionTraced(t *testing.T) {
	defer normpath.SetCaseInsensitive(false)()
	e, viewID, _ := newScanEngine(t)

	local := []scanEntry{
		{relPath: nameNFC, md4: "h1", size: 3},
		{relPath: nameNFD, md4: "h2", size: 3},
	}
	ops, err := e.coldStartReconcile(viewID, map[string]actions.Event{}, local, map[string]bool{}, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("got %d ops, want 0: %+v", len(ops), ops)
	}
	if got := len(collisionTraces(t, e.store)); got != 1 {
		t.Fatalf("got %d dead letters, want 1 from the cold-start pass", got)
	}
}

// TestFoldCollisionCorruptBaselineStillTraces pins the read arm. A record that no longer decodes
// is durable state, not a hiccup, so reporting it as a store failure left the collision UNTRACED
// on every 5s pass forever - invisible in `status`, which is the exact silent-unsyncable class the
// trace closes - while adding a Warn per scan. It is treated as absent, like every other engine
// read site treats it, and the trace is written with the no-synced-holder wording.
func TestFoldCollisionCorruptBaselineStillTraces(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, nameNFC, store.Baseline{
		FileID: "f1", FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, 7, nameNFC)
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	e := New(st2, &concTransport{client: &pushClient{}}, t.TempDir(), "sess", nil)

	e.noteCollisionTraces(7, map[string][]string{
		normpath.Key(nameNFC): {nameNFC, nameNFD},
	}, true)

	dls, lerr := st2.ListDeadLetters()
	if lerr != nil {
		t.Fatalf("ListDeadLetters: %v", lerr)
	}
	if len(dls) != 1 {
		t.Fatalf("got %d traces, want 1: an undecodable record must not silence the collision", len(dls))
	}
	if !strings.Contains(dls[0].Reason, "none of them can sync") {
		t.Errorf("reason = %q, want the no-synced-holder wording (no record can prove a holder)", dls[0].Reason)
	}
}
