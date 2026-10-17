package engine

// The byid/baseline redirects and the outbound remove precondition each read the
// store to decide whether a DESTRUCTIVE step may run, and each carries an explicit data-safety
// claim in its doc comment: a backend failure must PROPAGATE (leaving the op queued), never be
// swallowed into "not relocated" / "no divergence base". Swallowing would skip the redirect (a
// stale upload minting a second node at a freed path) or let the divergence check pass
// vacuously (a queued delete destroying another client's edit).
//
// The arms are driven by closing the bbolt store under the engine, which makes every read
// return a plain backend error - neither ErrNotFound nor ErrCorruptRecord, the two sentinels
// these helpers deliberately treat as "no record".

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// closedStoreEngine returns an engine whose store is already closed, so every read fails with a
// backend error.
func closedStoreEngine(t *testing.T) *Engine {
	t.Helper()
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return e
}

// assertBackendError fails unless err is a real backend failure rather than one of the two
// "treat as absent" sentinels.
func assertBackendError(t *testing.T, what string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("%s: err = nil, want the backend failure propagated", what)
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrCorruptRecord):
		t.Fatalf("%s: err = %v, want a propagated backend failure, not an absent-record sentinel", what, err)
	}
}

func TestStoreReadFailurePropagatesInsteadOfDeciding(t *testing.T) {
	tests := []struct {
		name string
		// call runs the helper against a closed-store engine and returns its error plus
		// whatever "decision" value it would otherwise have produced.
		call func(t *testing.T, e *Engine) (decided bool, err error)
	}{
		{
			// An unread byid index must not report "no record relocated", which would
			// let the fresh-create promote overwrite present user content with no _Conflict copy.
			name: "downloadRecordRelocated",
			call: func(t *testing.T, e *Engine) (bool, error) {
				return e.downloadRecordRelocated(store.Op{ViewID: 7, Path: "a.txt", FileID: "G"})
			},
		},
		{
			// An unread byid index must not report "not relocated", which would send the
			// modify-upload at the stale path and mint a second server node carrying the edit.
			name: "relocatedUploadPath",
			call: func(t *testing.T, e *Engine) (bool, error) {
				cur, err := e.relocatedUploadPath(store.Op{ViewID: 7, Path: "a.txt", FileID: "G"})
				return cur != "", err
			},
		},
		{
			// Finding H1: an unread baseline must not yield an empty upload
			// base. guardUploadTarget reads an empty base as "degraded to node identity" and
			// returns nil, so the whole pre-turn precondition would silently disable itself for
			// every modify and the peer's version would be overwritten - the very loss H1 filed.
			name: "uploadDivergenceBase",
			call: func(t *testing.T, e *Engine) (bool, error) {
				base, err := e.uploadDivergenceBase(store.Op{ViewID: 7, Path: "a.txt", FileID: "G"})
				return base != "", err
			},
		},
		{
			// An unread baseline must not yield an empty divergence base, which makes
			// node identity alone decide and lets the delete destroy a concurrent server edit -
			// nor "not a directory", which would skip the recursive-delete subtree precondition
			// (finding H7). Both facts come from the one read removeTargetFacts performs.
			name: "removeTargetFacts",
			call: func(t *testing.T, e *Engine) (bool, error) {
				base, isDir, kindUnknown, kindPlaced, err := e.removeTargetFacts(store.Op{ViewID: 7, Path: "a.txt", FileID: "G"})
				// kindPlaced is the read's own claim to have ANSWERED the kind question; an
				// unread store must not make it either, or the caller skips the pre-turn probe
				// that is the only witness left.
				return base != "" || isDir || kindUnknown || kindPlaced, err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := closedStoreEngine(t)
			decided, err := tc.call(t, e)
			assertBackendError(t, tc.name, err)
			if decided {
				t.Errorf("%s decided %v alongside the error; the caller must act on the error only", tc.name, decided)
			}
		})
	}
}

// TestDestructiveRepairsRefuseToDecideFromAnUnreadStore extends the same rule to the three
// two-client repairs whose doc comments make the identical promise but whose failure arms nothing
// drove. Each one stands between a server statement and a destructive or forging act, so a
// swallowed backend error is a verdict issued on no evidence at all.
func TestDestructiveRepairsRefuseToDecideFromAnUnreadStore(t *testing.T) {
	// Finding H3's descendant arm: "a scan failure is not permission to delete". Read as
	// "no later observation" it hands the recursive inbound delete a live successor subtree.
	t.Run("removeSupersededByLaterObservation", func(t *testing.T) {
		e := closedStoreEngine(t)
		op := store.Op{ViewID: 7, Path: "tree", FileID: "dead", ServerSyncID: 200}
		superseded, at, err := e.removeSupersededByLaterObservation(op, false, store.Baseline{}, true)
		assertBackendError(t, "removeSupersededByLaterObservation", err)
		if superseded || at != "" {
			t.Errorf("= (%v, %q) alongside the error; the caller must act on the error only", superseded, at)
		}
	})
	// Finding H2's repair: "dropping the rename with the repair half-done is the one outcome
	// worse than not repairing it at all", so the drop must not commit on an unread queue.
	t.Run("compensateDroppedRename", func(t *testing.T) {
		e := closedStoreEngine(t)
		op := store.Op{
			Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
			Path: "a.txt", NewPath: "b.txt", FileID: "fmov",
		}
		comp, track, err := e.compensateDroppedRename(dropRenameCompensated(op, false), store.QueuedOp{Seq: 1, Op: op})
		assertBackendError(t, "compensateDroppedRename", err)
		if comp != nil || track != nil {
			t.Errorf("= (%+v, %+v) alongside the error; no repair may be decided from an unread store", comp, track)
		}
	})
	// Finding H6's trace: a store failure must leave the key UN-noted so the next pass retries.
	// Noting it regardless would make one transient error hide the collision permanently, which
	// is the silent-unsyncable class the trace exists to close.
	t.Run("noteCollisionTraces", func(t *testing.T) {
		e := closedStoreEngine(t)
		e.noteCollisionTraces(7, map[string][]string{"k": {"one.txt", "One.txt"}}, true)
		if e.collisionNoted[7]["k"] {
			t.Error("the key was noted despite the failed write; the collision would never be traced")
		}
	})
	// Finding H7's subtree proof reads TWO stores, and neither may be read as "nothing here":
	// an unread queue would waive the discriminator that makes a peer's not-yet-applied create
	// divergence, and an unread baseline set would make every server child look unknown (or,
	// worse under a future refactor, every one of them look known). Both must propagate.
	t.Run("queuedInboundUnderPrefix", func(t *testing.T) {
		e := closedStoreEngine(t)
		owed, err := e.queuedInboundUnderPrefix(7, "tree")
		assertBackendError(t, "queuedInboundUnderPrefix", err)
		if len(owed) != 0 {
			t.Errorf("= %v alongside the error; an unread queue is not an empty one", owed)
		}
	})
	t.Run("subtreeBaselines", func(t *testing.T) {
		e := closedStoreEngine(t)
		known, skipped, overCap, err := e.subtreeBaselines(7, "tree", removeDirVerifyCap)
		assertBackendError(t, "subtreeBaselines", err)
		if len(known) != 0 || len(skipped) != 0 {
			t.Errorf("= (%v, %v) alongside the error; an unread baseline set is not an empty one", known, skipped)
		}
		if overCap {
			t.Error("overCap = true on a backend failure; an unread subtree is not an over-cap one")
		}
	})
}

// TestCollisionTraceWriteFailureLeavesTheKeyRetryable pins the arm the closed-store row above
// cannot reach: with the reads healthy and only the dead-letter WRITE failing, the key must stay
// un-noted so the next pass records it. Marking it noted before the write is checked would let one
// transient store error hide a fold-key collision for the life of the process - the silent-
// unsyncable class H6 closes, re-opened by a single hiccup.
func TestCollisionTraceWriteFailureLeavesTheKeyRetryable(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")
	writes := 0
	restore := recordCollisionTrace
	recordCollisionTrace = func(*Engine, store.DeadLetter) (uint64, error) {
		writes++
		return 0, errors.New("backend failure")
	}
	t.Cleanup(func() { recordCollisionTrace = restore })

	collisions := map[string][]string{"k": {"one.txt", "One.txt"}}
	e.noteCollisionTraces(7, collisions, true)
	if writes != 1 {
		t.Fatalf("writes = %d, want 1 (the trace must be attempted)", writes)
	}
	if e.collisionNoted[7]["k"] {
		t.Fatal("the key was noted despite the failed write; the collision would never be traced")
	}

	// The next pass retries, and once the write lands the key is finally noted.
	recordCollisionTrace = restore
	e.noteCollisionTraces(7, collisions, true)
	if !e.collisionNoted[7]["k"] {
		t.Error("the key stayed un-noted after a successful write; the trace would repeat every scan")
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly the one retried trace", n, err)
	}
}

// TestRepairSupersededRecordWithoutARecordIsANoOp pins the early return: the refusal has nothing
// to repair AT THE PATH when no baseline stands there (the descendant arm can refuse a delete
// whose directory record is already gone), and inventing one would fabricate a claim about a node
// the store never described.
//
// It runs against a live-but-empty store rather than a closed one. The refusal also owes the
// SUBTREE a repair - the descendants of a recursively removed directory get no event of their own
// (measured, TestIntegration_RecursiveRemoveTombstones), so surfaceSupersededDescendants sweeps
// the prefix on every arm, this one included, precisely because "the directory record is already
// gone" is one of the states that leaves stale descendants behind. That sweep reads the store, so
// a closed store can no longer stand in for "wrote nothing"; the assertion is now the direct one.
func TestRepairSupersededRecordWithoutARecordIsANoOp(t *testing.T) {
	st := newTestStore(t)
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	op := store.Op{ViewID: 7, Path: "tree", FileID: "dead", ServerSyncID: 200}
	if err := e.repairSupersededRecord(op, false, store.Baseline{}); err != nil {
		t.Errorf("repairSupersededRecord = %v, want nil (nothing to repair)", err)
	}
	if _, err := st.GetBaseline(7, "tree"); !isNotFound(err) {
		t.Errorf("a record appeared at the refused path (err %v); the repair must never invent one", err)
	}
}

// TestStoreReadHelpersShortCircuitWithoutFileID pins the shared id-less exemption: with no file
// id there is no node to resolve, so each helper answers without touching the store at all -
// which is why a closed store cannot fail them.
func TestStoreReadHelpersShortCircuitWithoutFileID(t *testing.T) {
	e := closedStoreEngine(t)
	op := store.Op{ViewID: 7, Path: "a.txt"}

	if relocated, err := e.downloadRecordRelocated(op); err != nil || relocated {
		t.Errorf("downloadRecordRelocated = %v/%v, want false/nil for an id-less op", relocated, err)
	}
	if cur, err := e.relocatedUploadPath(op); err != nil || cur != "" {
		t.Errorf("relocatedUploadPath = %q/%v, want \"\"/nil for an id-less op", cur, err)
	}
	if base, err := e.uploadDivergenceBase(op); err != nil || base != "" {
		t.Errorf("uploadDivergenceBase = %q/%v, want \"\"/nil for an id-less create", base, err)
	}
	// removeTargetFacts is deliberately NOT in this set: an id-less remove needs no divergence
	// base, but it still needs the target's recorded KIND, because the recursive-delete subtree
	// precondition (finding H7) applies to an id-less directory delete exactly as it does to an
	// id-carrying one. So it reads the store regardless of the file id, and a closed store fails
	// it - pinned by the backend-error table above.
}
