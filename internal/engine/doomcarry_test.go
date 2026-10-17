package engine

// Tests for the cross-pass doom carry. A doomed rename whose doom
// could not be RECORDED (the stranded-baseline cleanup or the atomic trace+ack failed at the
// store level) stays queued, while the feeder that proved the doom has already left the queue
// with its own trace. Both halves of the doom evidence - drainBatch's in-window feeder scan and
// the deadOutputs set - are pass-local, so the next drainQueue pass used to re-read the rename
// with no feeder and no dead mark and apply it, hauling whatever same-kind untracked entity sat
// at its source onto the destination under the moved node's identity.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDoomSurvivesPassAfterCleanupFailure is the red test: pass 1 dooms the rename and fails to
// record it, the user's own file then occupies the rename's source, and pass 2 must still refuse
// to move it. Once the store heals, the doom resolves with a durable trace and the user's file is
// still untouched.
func TestDoomSurvivesPassAfterCleanupFailure(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// The feeder's download is made terminally unappliable through the fake transport's own
	// seam (dlErr), not by parking a DIRECTORY on its target. That trick stopped working when a
	// directory occupant became something the download RESOLVES -- it is preserved aside as a
	// keep-both copy and the server file lands -- so a test built on it was asserting the doom
	// machinery against a premise that no longer holds. The reason the feeder dies is not this
	// test's subject; that it dies, and what the rename behind it does afterwards, is.

	// The doom's cleanup fails at the store level (the realistic shape is ENOSPC on the bbolt
	// write), so the rename cannot be resolved and stays queued.
	cleanupFails := true
	wantErr := errors.New("injected store failure")
	orig := dropDoomedRename
	dropDoomedRename = func(e *Engine, qo store.QueuedOp) error {
		if cleanupFails {
			return wantErr
		}
		return orig(e, qo)
	}
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
		// Terminal for the FEEDER's attempt only: the compensating re-request the doom
		// leaves behind asks for the same node and must still be able to land.
		dlErrOnce: map[string]error{"f9": errRejectedPath},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	// Pass 1: the download dead-letters, the rename is doomed, its cleanup fails.
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("pass 1 drainQueue err = %v, want the injected cleanup failure", err)
	}
	if !e.carriedDoom("f9", cleanKey("Photos", "a.txt")) {
		t.Fatalf("pass 1 did not carry the unresolved doom for f9 at the rename source")
	}

	// The user's own FILE stands at the rename source - a same-kind untracked source, which is
	// what passes applyRename's remaining guards. The doom is the only thing standing between it
	// and the destination.
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("user data"), 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}

	// Pass 2: the feeder is long gone from the queue and the pass-local dead mark is gone with
	// it. The rename must still be recognised as doomed, not applied.
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("pass 2 drainQueue err = %v, want the injected cleanup failure (doom re-derived)", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("the user's file was moved away from the rename source: %v", err)
	}
	if string(got) != "user data" {
		t.Errorf("source content = %q, want %q", got, "user data")
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "c.txt")); !os.IsNotExist(err) {
		t.Errorf("c.txt exists after pass 2 (err=%v): the user's file was hauled onto the destination", err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 1 {
		t.Errorf("pending ops after pass 2 = %d, want 1 (the doomed rename retries)", n)
	}

	// Pass 3: the store heals, so the doom is finally recorded - trace + ack - and the user's
	// file is still exactly where it was.
	cleanupFails = false
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("pass 3 drainQueue: %v", err)
	}
	if n, err := st.PendingLen(); err != nil {
		t.Fatalf("PendingLen: %v", err)
	} else if n != 0 {
		t.Errorf("pending ops after pass 3 = %d, want 0 (the doom resolved)", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	var renameTraced bool
	for _, d := range dls {
		if d.Type == store.OpRename && d.Path == "a.txt" && d.NewPath == "c.txt" {
			renameTraced = true
		}
	}
	if !renameTraced {
		t.Errorf("no dead-letter trace for the doomed rename; traces=%+v", dls)
	}
	if e.carriedDoom("f9", cleanKey("Photos", "a.txt")) {
		t.Errorf("carried doom survived its own resolution")
	}
	got, err = os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil || string(got) != "user data" {
		t.Errorf("source content after pass 3 = %q (err=%v), want %q untouched", got, err, "user data")
	}
	// The doomed rename's drop now RE-REQUESTS its node at the destination (finding H2), so
	// c.txt holds the SERVER's bytes for f9 - never the user's file, which is the thing the doom
	// exists to keep where it is.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err != nil || string(got) != "f9-bytes" {
		t.Errorf("c.txt after pass 3 = %q (err=%v), want the re-requested server node %q", got, err, "f9-bytes")
	}
}

// TestDoomSurvivesWhenRenameEndpointsFoldToOneKey is the same contract for the rename shape whose
// two endpoints produce ONE conflict key: cleanKey folds to NFC on every platform (and case-folds
// on a case-insensitive filesystem), so an NFD↔NFC-only rename - or a case-only one - has
// keys[0] == keys[len-1]. carryDoom arms the source key while the post-loop clear answers the
// OUTPUT key, so the unconditional clear erased the mark in the very pass that recorded it and
// the next pass hauled whatever untracked entity sat at the source.
func TestDoomSurvivesWhenRenameEndpointsFoldToOneKey(t *testing.T) {
	// "a" + COMBINING ACUTE (NFD) and the precomposed "á" (NFC) are different byte strings that
	// cleanKey folds to the same key.
	const nfd = "á.txt"
	const nfc = "á.txt"
	if cleanKey("Photos", nfd) != cleanKey("Photos", nfc) {
		t.Fatalf("fixture assumption broken: %q and %q do not fold to one key", nfd, nfc)
	}

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// The feeder's download is made terminally unappliable through the fake transport's own
	// seam (dlErr), not by parking a DIRECTORY on its target. That trick stopped working when a
	// directory occupant became something the download RESOLVES -- it is preserved aside as a
	// keep-both copy and the server file lands -- so a test built on it was asserting the doom
	// machinery against a premise that no longer holds. The reason the feeder dies is not this
	// test's subject; that it dies, and what the rename behind it does afterwards, is.

	wantErr := errors.New("injected store failure")
	orig := dropDoomedRename
	dropDoomedRename = func(*Engine, store.QueuedOp) error { return wantErr }
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
		// Terminal for the FEEDER's attempt only: the compensating re-request the doom
		// leaves behind asks for the same node and must still be able to land.
		dlErrOnce: map[string]error{"f9": errRejectedPath},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: nfd, FileID: "f9", LocalHash: "h9", LocalSize: 8,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: nfd, NewPath: nfc, FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("pass 1 drainQueue err = %v, want the injected cleanup failure", err)
	}
	if !e.carriedDoom("f9", cleanKey("Photos", nfd)) {
		t.Fatalf("the pass that armed the doom erased it again: the rename's output key folds onto its source key")
	}

	// The user's own FILE occupies the source - a same-kind untracked entity, which is what
	// passes applyRename's remaining guards. Only the carried doom stands between it and the move.
	if err := os.WriteFile(filepath.Join(viewDir, nfd), []byte("user data"), 0o644); err != nil {
		t.Fatalf("seed user file: %v", err)
	}
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("pass 2 drainQueue err = %v, want the injected cleanup failure (doom re-derived)", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, nfd))
	if err != nil {
		t.Fatalf("the user's file was moved away from the rename source: %v", err)
	}
	if string(got) != "user data" {
		t.Errorf("source content = %q, want %q", got, "user data")
	}
}

// TestUnresolvedDoomDoesNotArmTheCrossWindowMark pins the post-loop doctrine on the failure
// arms: the doomed rename is still QUEUED when its own cleanup fails, so it must not arm the
// cross-window dead mark for its own output key. Arming it (the old `s.dead = true` left standing
// on a still-queued op) permanently dropped the NEXT hop of a move chain - a dead-letter trace
// plus the relocated baseline gone - over an op that is merely retrying. The correct outcome is
// the ordinary blocked skip: the next hop stays queued behind it with no trace.
func TestUnresolvedDoomDoesNotArmTheCrossWindowMark(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	// The feeder's download is made terminally unappliable through the fake transport's own
	// seam (dlErr), not by parking a DIRECTORY on its target. That trick stopped working when a
	// directory occupant became something the download RESOLVES -- it is preserved aside as a
	// keep-both copy and the server file lands -- so a test built on it was asserting the doom
	// machinery against a premise that no longer holds. The reason the feeder dies is not this
	// test's subject; that it dies, and what the rename behind it does afterwards, is.
	// Only the FIRST hop's cleanup fails: the second hop's doom, if it were wrongly armed, would
	// therefore be recorded with a durable trace - the observable this test forbids.
	wantErr := errors.New("injected store failure")
	orig := dropDoomedRename
	dropDoomedRename = func(e *Engine, qo store.QueuedOp) error {
		if qo.Path == "a.txt" {
			return wantErr
		}
		return orig(e, qo)
	}
	t.Cleanup(func() { dropDoomedRename = orig })

	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("f9-bytes")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 8}},
		// Terminal for the FEEDER's attempt only: the compensating re-request the doom
		// leaves behind asks for the same node and must still be able to land.
		dlErrOnce: map[string]error{"f9": errRejectedPath},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	// One op per window, so the second hop of the move chain reads the first hop's fate through
	// the cross-window sets rather than the in-window scheduler.
	e.limits.DrainBatch = 1

	ops := []store.Op{
		{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 8},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9"},
		{Type: store.OpRename, Direction: store.DirInbound, ViewID: 7, Path: "c.txt", NewPath: "d.txt", FileID: "f9"},
	}
	for _, op := range ops {
		if _, err := st.Enqueue(op); err != nil {
			t.Fatalf("Enqueue %s: %v", op.Type, err)
		}
	}
	if err := e.drainQueue(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("drainQueue err = %v, want the injected cleanup failure", err)
	}
	// Both renames must still be queued: the first because its doom could not be recorded, the
	// second because it is blocked behind it.
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(pending) != 2 {
		t.Errorf("pending ops = %d, want 2 (both hops queued); pending=%+v", len(pending), pending)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	for _, d := range dls {
		if d.Type == store.OpRename && d.Path == "c.txt" {
			t.Errorf("the second hop was dropped with a trace over a merely-retrying predecessor: %+v", d)
		}
	}
}

// TestCarriedDoomClearedByALiveProducer pins the self-heal rule: the carry follows deadOutputs'
// "an op of this node at this key that did not die is evidence the node is there" contract, so a
// producer that later genuinely materialises the node does not leave a legitimate rename doomed
// forever.
func TestCarriedDoomClearedByALiveProducer(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"f9": []byte("server")},
		nodes:   map[string]actions.Node{"f9": {Hash: "h9", Size: 6}},
		// Terminal for the FEEDER's attempt only: the compensating re-request the doom
		// leaves behind asks for the same node and must still be able to land.
		dlErrOnce: map[string]error{"f9": errRejectedPath},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	src := cleanKey("Photos", "a.txt")
	e.carryDoom("f9", src)

	// A healthy download of f9 to a.txt followed by the rename: the download really does place
	// the node at the source, so the stale carry must not doom the move.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f9", LocalHash: "h9", LocalSize: 6,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", NewPath: "c.txt", FileID: "f9",
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if e.carriedDoom("f9", src) {
		t.Errorf("a live producer at the same key did not clear the carried doom")
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "c.txt"))
	if err != nil {
		t.Fatalf("the legitimate move did not apply: %v", err)
	}
	if string(got) != "server" {
		t.Errorf("c.txt = %q, want %q", got, "server")
	}
}
