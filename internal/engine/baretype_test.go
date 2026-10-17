package engine

// A baseline whose FileType is UNSET is the shape this package repeatedly declares to be no
// evidence at all ("an UNSET FileType states nothing" - outbound_removedir.go's child arms), and
// it is routine rather than exotic: applyRename writes store.Baseline{FileID: …} with no FileType
// and forces StateSynced on its overwrite/no-facts arms, so every directory relocated by an
// inbound rename carries one.
//
// Three sites read that field as though "" meant "file", and each of them is a door into finding
// H7 (the server's `remove` of a directory is RECURSIVE, so an unverified delete destroys a peer's
// nodes in both replicas). These tests pin all three shut.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestLocalReconcileBareTypeOverDirectoryEmitsNoRemove covers the SOURCE of the spurious op.
// detectRemoves emits an OpRemove for a path that is still on disk when the scanned kind
// disagrees with the recorded one - a genuine type replacement. Comparing an unset FileType
// against "dir" made that test true for every bare record standing over a directory, so a
// directory that was merely RENAMED by the server was diffed as a file→dir replacement and a
// recursive server delete of the live tree was queued for it - re-derived by every scan, since
// nothing in the drain writes the kind back.
func TestLocalReconcileBareTypeOverDirectoryEmitsNoRemove(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)

	// keep.txt keeps the scan non-empty so the mass-deletion debounce does not fire.
	keep := []byte("keep")
	writeFile(t, viewDir, "keep.txt", keep)
	putSynced(t, e, viewID, "keep.txt", "FK", "file", md4.ContentHash(keep), int64(len(keep)))

	// The bare record applyRename leaves behind: an id, StateSynced, and no kind at all.
	if err := e.store.PutBaseline(viewID, "moved", store.Baseline{
		FileID: "FM", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed bare baseline: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "moved"), 0o755); err != nil {
		t.Fatalf("mkdir moved: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	for _, op := range ops {
		if op.Type == store.OpRemove {
			t.Fatalf("got %+v; a record that states NO kind cannot disagree with the disk, and a "+
				"directory standing right where the baseline says a node lives must never be "+
				"diffed as a replacement - its remove is a RECURSIVE server delete of a live tree", op)
		}
	}
}

// TestOutboundRemoveDirBareTypeRecordProbesKind covers the apply-side door, and it is the one
// that reached the destructive turn. removeTargetFacts reported kindPlaced for any record that
// DECODED, so a bare record answered "not a directory" without saying anything: isDir was false,
// planRemove's BaseIsDir was false (it reads the same field), needKindProbe was therefore false,
// and for an ID-LESS op no probe ran either. All three witnesses went unconsulted, the
// unplaced-target refusal was disarmed by kindPlaced itself, and the RECURSIVE Remove went out
// with nothing proved about the subtree - H7 through the door that flag exists to shut.
//
// Id-less is not an exotic shape: a DSM whose replies omit file_id leaves every synced record
// id-less, so every outbound remove takes this path.
func TestOutboundRemoveDirBareTypeRecordProbesKind(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		// The peer's node, which the base has never seen and the recursive delete would take.
		"tree": {{FileID: "ff", Name: "fresh.txt", Hash: "h2"}},
	})
	// Id-less throughout, matching the op: only is_dir can place the target.
	c.queryNode = map[string]actions.Node{"tree": {IsDir: true}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	// The record DECODES but states no kind - and carries no id, so nothing else can place it.
	if err := st.PutBaseline(7, "tree", store.Baseline{State: store.StatePending}); err != nil {
		t.Fatalf("seed bare baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	assertDirRemoveRefused(t, st, c, "tree/fresh.txt")
	if len(c.listResult["tree"]) != 1 {
		t.Errorf("server subtree = %+v, want the peer's node intact", c.listResult["tree"])
	}
}

// TestOutboundRemoveDirDescendsChildNoWitnessPlaces covers the third door, inside the subtree
// walk itself. A child arrived that matched none of the divergence arms, was not the base=dir
// descend case, and whose hash compare short-circuited on an empty hash: is_dir omitted (a sparse
// listing), FileType unset (the bare record), no content hash. It was reported AGREED without a
// single fact about it ever being read - so a directory holding a peer's file was never
// enumerated, the verification returned nil, and the recursive remove took it.
func TestOutboundRemoveDirDescendsChildNoWitnessPlaces(t *testing.T) {
	c := dirRemoveClient(map[string][]actions.Node{
		// No is_dir and no hash: the listing places nothing about this child.
		"tree": {{FileID: "fs", Name: "sub"}},
		// It is in fact a directory, and it holds a node the baseline never saw.
		"tree/sub": {{FileID: "fp", Name: "peer.txt", Hash: "hp"}},
	})
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "tree", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// The bare child record: an id that matches the listing, and no kind.
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

	assertDirRemoveRefused(t, st, c, "tree/sub/peer.txt")
	if len(c.listResult["tree/sub"]) != 1 {
		t.Errorf("server subtree = %+v, want the peer's node intact", c.listResult["tree/sub"])
	}
}
