package engine

// Tests for the DEGENERATE inbound rename - one whose two endpoints fold to the same path.
//
// A server rename between two spellings of one name (an NFD -> NFC rename) reaches reconcile as a
// move whose FromPath and Path both normalize to the same NFC string, and opForEvent does not
// filter that shape out. applyRename's success path already guards its source drop with
// op.Path != op.NewPath, and relocateDescendantBaselines and dropSelfEchoMoveSource carry the same
// guard - the drop path did not, so the source cleanup deleted the very record the destination arm
// had just decided to keep, since for a degenerate move src and dst are ONE key.

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDegenerateRenameDropKeepsTheReEmissionMarker is the red test. The kept-marker arm exists so a
// dropped rename does not discard a pending local DELETE that revertRelocatedRemove re-emits at the
// destination; for a degenerate move the source cleanup then deleted that marker anyway - and, the
// node being a directory, cascaded the whole subtree's baselines away with it. The result is what
// the arm's own comment forbids: the user's deletion is silently discarded AND the live server node
// drops out of the local replica, recoverable only by a cold start.
func TestDegenerateRenameDropKeepsTheReEmissionMarker(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	putSynced(t, e, viewID, "a/x", "f1", "dir", "", 0)
	putSynced(t, e, viewID, "a/x/c.txt", "f2", "file", md4.ContentHash([]byte("c")), 1)

	dst, err := e.store.GetBaseline(viewID, "a/x")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	op := store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: viewID,
		// Both endpoints are the same path: the NFD and NFC spellings folded to one.
		Path: "a/x", NewPath: "a/x", FileID: "f1", IsDir: true,
	}

	derr := e.dropColdStartGapRename(op, true /*srcIsMoved*/, true /*movedIsDir*/, dst, nil)
	if !errors.Is(derr, errDeadLettered) {
		t.Fatalf("drop returned %v, want the dead-lettered drop signal", derr)
	}

	if _, err := e.store.GetBaseline(viewID, "a/x"); err != nil {
		t.Fatalf("the re-emission marker at a/x was deleted (%v); a degenerate move has no separate source record to drop", err)
	}
	if _, err := e.store.GetBaseline(viewID, "a/x/c.txt"); err != nil {
		t.Fatalf("the descendant baseline was cascaded away (%v); dropping the marker takes the whole subtree with it", err)
	}
}

// TestDegenerateRenameDropStillCleansAStrandedRecord pins that the guard is scoped to the ONE thing
// it must protect. When the destination record is not the kept marker (it is not StateSynced), the
// drop's own destination arm still deletes it - the degenerate guard must not turn that into a
// no-op and strand a pending record describing a node that is not on disk, which the next localScan
// would leave inert forever.
func TestDegenerateRenameDropStillCleansAStrandedRecord(t *testing.T) {
	e, viewID, _ := newScanEngine(t)
	if err := e.store.PutBaseline(viewID, "a/x", store.Baseline{
		FileID: "f1", FileType: "file", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed pending baseline: %v", err)
	}
	dst, err := e.store.GetBaseline(viewID, "a/x")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	op := store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: viewID,
		Path: "a/x", NewPath: "a/x", FileID: "f1",
	}

	if derr := e.dropColdStartGapRename(op, true, false, dst, nil); !errors.Is(derr, errDeadLettered) {
		t.Fatalf("drop returned %v, want the dead-lettered drop signal", derr)
	}

	if _, err := e.store.GetBaseline(viewID, "a/x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetBaseline(a/x) = %v, want ErrNotFound: the stranded pending record must still be swept", err)
	}
}
