//go:build integration

package integration

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_RenameOntoOccupiedPath measures the one mutating-verb semantic the harness
// has never captured: what a live DSM does when `rename`'s destination is already held by
// another node.
//
// Every other destructive verb has been measured and the client's precondition was built on the
// answer - `upload` is compare_mtime (TestIntegration_UploadConflictSemantics), `remove` of a
// directory is recursive (TestIntegration_RemoveDirectorySemantics, finding H7). `rename` was
// only ever measured for its absence code and its cursor rule, so the client had no
// precondition at all and the fake had no behaviour to model. Both plausible answers are
// destructive in different ways:
//
//   - REPLACES: a local move silently destroys another client's node, everywhere, through a
//     verb that reports no conflict. guardRenameTarget is what stands between the user and that.
//   - REFUSES: the reply is a bare path error that no client-side probe can distinguish from
//     the other path-error shapes, which is why it now carries renameUnsettledCode and is paced.
//
// The client is correct either way - it refuses to move onto an occupied destination and
// funnels to keep-both - so this scenario does not gate the fix. It exists to replace an
// assumption with a measurement, and to tell fakeDSM's dsmRenamePolicy which default is real.
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_RenameOntoOccupiedPath(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	const (
		srcRel = "renameoccupied/source.txt"
		dstRel = "renameoccupied/occupied.txt"
	)
	srcBytes := []byte("the node being moved\n")
	dstBytes := []byte("the node already standing at the destination\n")

	src := sc.remotePut(t, srcRel, srcBytes)
	dst := sc.remotePut(t, dstRel, dstBytes)
	t.Logf("source %q: file_id=%q", sc.viewRel(srcRel), src.FileID)
	t.Logf("destination %q: file_id=%q", sc.viewRel(dstRel), dst.FileID)

	err := sc.client.Rename(ctx, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel(srcRel), sc.viewRel(dstRel))

	if err != nil {
		t.Logf("VERDICT: DSM REFUSED the rename onto an occupied path: %s", describeServerErr(err))
		if errors.Is(err, actions.ErrPathError) {
			t.Logf("the refusal arrives as ErrPathError, indistinguishable from a missing " +
				"source or parent - which is exactly why the engine maps an unsettled rename " +
				"refusal to its own synthetic streak code rather than retrying it forever")
		}
		// Both nodes must have survived a refusal.
		after, ok := sc.queryRel(t, dstRel)
		if !ok || after.FileID != dst.FileID {
			t.Errorf("integration: after a REFUSED rename the destination holds %+v (present=%v), "+
				"want the original node %q untouched", after, ok, dst.FileID)
		}
		if _, ok := sc.queryRel(t, srcRel); !ok {
			t.Errorf("integration: after a REFUSED rename the source %q is gone", sc.viewRel(srcRel))
		}
		return
	}

	t.Logf("VERDICT: DSM ACCEPTED the rename onto an occupied path - the verb is DESTRUCTIVE " +
		"and offers no conflict signal, so guardRenameTarget's pre-turn probe is the only thing " +
		"preventing a local move from destroying another client's node")

	after, ok := sc.queryRel(t, dstRel)
	switch {
	case !ok:
		t.Errorf("integration: the destination %q resolves to nothing after an accepted rename",
			sc.viewRel(dstRel))
	case after.FileID == src.FileID:
		t.Logf("the destination now holds the MOVED node %q; the occupant %q was destroyed",
			src.FileID, dst.FileID)
	case after.FileID == dst.FileID:
		t.Errorf("integration: the rename reported success but the destination still holds the "+
			"occupant %q - the turn was a silent no-op, a third semantic neither the client nor "+
			"the fake models", dst.FileID)
	default:
		t.Errorf("integration: the destination holds an unexpected node %q (source %q, occupant %q)",
			after.FileID, src.FileID, dst.FileID)
	}
	if _, ok := sc.queryRel(t, srcRel); ok {
		t.Errorf("integration: the source %q still resolves after an accepted rename",
			sc.viewRel(srcRel))
	}
}
