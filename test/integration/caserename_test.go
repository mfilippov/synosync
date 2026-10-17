//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// caseCollisionErrno is the result code a live DSM answers when a create would
// produce a name that differs from an existing sibling only by case. It has no
// typed sentinel in actions/errors.go - it reaches the engine as a generic
// *ServerError - so it is recorded here as the measured constant this scenario
// pins.
const caseCollisionErrno = 24580

// TestIntegration_CaseOnlyRename settles a DESIGN §13 residual: after a
// CASE-ONLY rename on the server (`notes.txt` → `Notes.txt`), does a live DSM
// still resolve the OLD spelling on a path-addressed turn, or does it answer a
// path error?
//
// The client no longer depends on the answer - a resolve op whose `byid` index
// holds a different-but-fold-equal spelling addresses its turns by the TRACKED
// spelling (trackedSpelling, adopted in applyOp via adoptResolveSpelling) - but
// the answer decides whether the old-spelling arm is reachable at all, and it
// decides something sharper: if the server resolves BOTH spellings to one node,
// its lookup is case-INSENSITIVE, and a client that believes it created two
// distinct nodes would be overwriting one.
//
// The scenario therefore captures three things:
//
//  1. whether a case-only rename is accepted at all (a case-insensitive store
//     would see source and destination as the same name);
//  2. which spelling the parent's listing reports afterwards - the authority on
//     what actually landed;
//  3. whether query_node resolves the old spelling, and if so whether it lands
//     on the SAME node id.
//
// Then it uploads the old spelling back as a second node: succeeding with a
// distinct file id proves the share is case-SENSITIVE and both can coexist,
// which is what makes normpath's fold-key collision protection load-bearing
// rather than theoretical.
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_CaseOnlyRename(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	const (
		lower = "notes.txt"
		upper = "Notes.txt"
	)

	original := sc.remotePut(t, "case/"+lower, []byte("case-only rename capture: original\n"))
	t.Logf("created %q: file_id=%q", sc.viewRel("case/"+lower), original.FileID)

	// 1. Is the case-only rename accepted?
	err := sc.client.Rename(ctx, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("case/"+lower), sc.viewRel("case/"+upper))
	if err != nil {
		t.Fatalf("integration: VERDICT: DSM REFUSED the case-only rename %q → %q: %s - "+
			"record it in DESIGN §13: on such a server a local case-only rename can never be "+
			"pushed, and the outbound op would retry or dead-letter",
			lower, upper, describeServerErr(err))
	}
	t.Logf("VERDICT: DSM ACCEPTED the case-only rename %q → %q", lower, upper)

	// 2. What spelling does the parent's listing report?
	children, listed, err := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("case"))
	if err != nil {
		t.Fatalf("integration: list %q: %v", sc.viewRel("case"), err)
	}
	if !listed {
		t.Fatalf("integration: list %q carried no enumeration", sc.viewRel("case"))
	}
	var stored string
	for _, c := range children {
		if c.FileID == original.FileID {
			stored = c.Name
			break
		}
	}
	switch stored {
	case upper:
		t.Logf("listing reports the NEW spelling %q - the rename landed on the same node", upper)
	case "":
		t.Errorf("integration: the renamed node %s is absent from the listing of %q (%d children)",
			original.FileID, sc.viewRel("case"), len(children))
	default:
		t.Errorf("integration: after renaming %q → %q the listing reports %q - the server neither "+
			"applied the new spelling nor refused the turn", lower, upper, stored)
	}

	// 3. Does the OLD spelling still resolve?
	old, okOld := sc.queryRel(t, "case/"+lower)
	newNode, okNew := sc.queryRel(t, "case/"+upper)
	if !okNew {
		t.Errorf("integration: query_node of the NEW spelling %q reports it absent after an "+
			"accepted rename", sc.viewRel("case/"+upper))
	} else if newNode.FileID != original.FileID {
		t.Errorf("integration: query_node of %q resolved to file_id %q, want the renamed node %q",
			sc.viewRel("case/"+upper), newNode.FileID, original.FileID)
	}
	switch {
	case !okOld:
		t.Logf("VERDICT: query_node of the OLD spelling %q is a PATH ERROR - the lookup is "+
			"case-SENSITIVE, so the old-spelling arm is reachable and addressing a stale case "+
			"fails loudly", lower)
	case old.FileID == original.FileID:
		t.Logf("VERDICT: query_node of the OLD spelling %q resolves to the SAME node %q - the "+
			"lookup is case-INSENSITIVE, so a stale-case turn silently hits the renamed node; "+
			"trackedSpelling is what keeps a resolve addressing the spelling byid actually holds",
			lower, old.FileID)
	default:
		t.Logf("query_node of the OLD spelling %q resolves to a DIFFERENT node %q", lower, old.FileID)
	}

	// 4. Can both spellings coexist? A distinct file id proves a case-SENSITIVE
	//    store; a reused id proves the upload overwrote the renamed node.
	parent, ok := sc.queryRel(t, "case")
	if !ok {
		t.Fatalf("integration: query_node %q reports the scratch directory absent", sc.viewRel("case"))
	}
	second, err := sc.client.Upload(ctx, parent.FileID, lower, time.Now().Unix(),
		sc.view.RootNodeID, []byte("case-only rename capture: second\n"))
	if err != nil {
		// MEASURED (DSM 7.2.2-72806 / Drive Server 4.0.3-27892): errno 24580, a
		// code with no typed sentinel in actions/errors.go. So DSM's LOOKUP is
		// case-sensitive (step 3 above) while its UNIQUENESS is case-INsensitive:
		// two fold-equal siblings cannot coexist in a view. A case-sensitive
		// local host (ext4/APFS) holding both therefore has one file it can never
		// push - the op is an unmapped deterministic rejection, so it dead-letters
		// on the unmappedServerErr streak with the bare code in its trace.
		code, ok := actions.ServerErrorCode(err)
		t.Logf("VERDICT: re-uploading the OLD spelling %q alongside %q was REFUSED: %s",
			lower, upper, describeServerErr(err))
		if !ok || code != caseCollisionErrno {
			t.Errorf("integration: the fold-equal sibling was refused with %s, but the recorded "+
				"measurement is errno %d (docs/INTEGRATION.md). Update the record if the server changed.",
				describeServerErr(err), caseCollisionErrno)
		}
		return
	}
	switch second.FileID {
	case original.FileID:
		t.Errorf("integration: uploading %q after the node was renamed to %q reused file_id %q - "+
			"the upload OVERWROTE the renamed node instead of creating a sibling, so the two "+
			"spellings are one node on this server. Record it in DESIGN §13: the engine's "+
			"fold-key collision protection is then the only thing preventing a silent overwrite.",
			lower, upper, second.FileID)
	default:
		t.Logf("VERDICT: %q and %q COEXIST as distinct nodes (%q vs %q) - the share is "+
			"case-SENSITIVE", lower, upper, second.FileID, original.FileID)
	}

	children, listed, err = sc.client.List(ctx, sc.view.ViewID, sc.viewRel("case"))
	if err != nil {
		t.Fatalf("integration: re-list %q: %v", sc.viewRel("case"), err)
	}
	if !listed {
		t.Fatalf("integration: re-list %q carried no enumeration", sc.viewRel("case"))
	}
	names := make([]string, 0, len(children))
	for _, c := range children {
		names = append(names, c.Name)
	}
	t.Logf("listing after the second upload: %v", names)
}
