//go:build integration

package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

// envIDAlias opts into the id:-aliasing probe. It is separate from the required
// SYNOSYNC_IT_* set for two reasons, both unusual enough to demand an explicit yes:
// the probe must create its bait at the VIEW ROOT (the harness otherwise stays inside
// its scratch base), and if the server dispatches the path parameter by prefix the
// probe DELETES the victim node it created - which is precisely the outcome being
// measured.
const envIDAlias = "SYNOSYNC_IT_ID_ALIAS"

// TestIntegration_IDAliasProbe settles an addressing residual: one `path`
// parameter carries both addressing forms - a view-relative path for
// rename/remove/query_node/list, and `id:<file_id>` for download/resume_download -
// so a name like `id:12345` sitting in a view ROOT is ambiguous. If DSM dispatches
// the parameter by prefix, a local delete of that innocuous-looking file would send
// `remove(view, "id:12345")` and destroy SOMEONE ELSE'S node (recoverable only from
// the server recycle bin), and the read probes would answer about the wrong node,
// feeding confirmAbsentByParentList / ancestorConfirmsAbsent false evidence.
//
// The client is written to be safe under either parser: scanView never surfaces such
// a path and protects it, and reconcile skips a server event carrying one. DESIGN §13
// records that the literal probe - "create a file named id:<real id>, check that the
// RIGHT node died" - needs a live DSM, and that if the server parses literally the
// protection can be retired in a later round. That is what this scenario does.
//
// The bait must live at the view root: a nested `a/id:foo` does not carry the prefix
// at the start of the parameter, so it cannot reproduce the ambiguity. Both the bait
// and the victim are created by this test and removed in cleanup.
func TestIntegration_IDAliasProbe(t *testing.T) {
	if os.Getenv(envIDAlias) == "" {
		t.Skipf("integration: set %s=1 to run the id:-aliasing probe - it writes ONE node at the "+
			"view root (outside the scratch base) and, if the server aliases, deletes the victim "+
			"node it created", envIDAlias)
	}

	sc := setupScenarioNoEngine(t)

	victim := sc.remotePut(t, "idalias/victim.txt", []byte("the node that must not die\n"))
	if victim.FileID == "" {
		t.Fatalf("integration: the victim upload returned no file id")
	}
	victimRel := sc.viewRel("idalias/victim.txt")
	bait := "id:" + victim.FileID
	t.Logf("victim %s at %q; bait name %q at the view root", victim.FileID, victimRel, bait)

	ctx, cancel := actionCtx()
	defer cancel()

	baitNode, err := sc.client.Upload(ctx, sc.viewRootID(t), bait, time.Now().Unix(),
		sc.view.RootNodeID, []byte("bait\n"))
	if err != nil {
		t.Fatalf("integration: VERDICT: the server REFUSED to create a node named %q at the view "+
			"root: %s - the ambiguity is unreachable from the server side, and the client-side "+
			"protection is insurance only", bait, describeServerErr(err))
	}
	t.Logf("VERDICT: the server ACCEPTED the bait name: file_id=%q", baitNode.FileID)

	// Cleanup runs whatever survives; both removals are best-effort by design.
	t.Cleanup(func() {
		cctx, ccancel := actionCtx()
		defer ccancel()
		_ = sc.client.Remove(cctx, sc.view.ViewID, bait)
	})

	// A read first: does query_node of the bait path answer about the BAIT or about
	// the victim? This is the same ambiguity the absence provers would consume.
	// Addressed at the view root, not under the scratch base: queryRel would prefix it
	// and destroy the very property under test.
	probe, ok := serverQuery(t, sc.client, sc.view.ViewID, bait)
	switch {
	case !ok:
		t.Logf("query_node(%q): absent - the server resolved neither form", bait)
	case probe.FileID == baitNode.FileID:
		t.Logf("query_node(%q): resolves to the BAIT (literal parse)", bait)
	case probe.FileID == victim.FileID:
		t.Errorf("integration: query_node(%q) resolved to the VICTIM %s - the server dispatches "+
			"the path parameter by prefix, so a read probe on such a name answers about another "+
			"node entirely. The scan/reconcile protection is load-bearing; do NOT retire it.",
			bait, victim.FileID)
	default:
		t.Logf("query_node(%q): resolves to a THIRD node %q", bait, probe.FileID)
	}

	// The destructive half: remove the bait by its literal name and see who died.
	if err := sc.client.Remove(ctx, sc.view.ViewID, bait); err != nil {
		t.Logf("remove(%q) failed: %s - no verdict from this half", bait, describeServerErr(err))
	}

	_, victimAlive := sc.queryRel(t, "idalias/victim.txt")
	baitAlive := rootChildExists(t, sc, bait)

	switch {
	case victimAlive && !baitAlive:
		t.Logf("VERDICT: the server parses the path LITERALLY - the bait died, the victim %s is "+
			"untouched. The id:*-alias protection in scanView/reconcile guards a case this server "+
			"does not have, and DESIGN §13 allows retiring it in a later round.", victim.FileID)
	case !victimAlive && baitAlive:
		t.Errorf("integration: VERDICT: the server DISPATCHED BY PREFIX - the victim %s was "+
			"destroyed by a remove addressed at %q while the bait survived. This is the data-loss "+
			"vector the client assumed: the protection is load-bearing and must stay.",
			victim.FileID, bait)
	case !victimAlive && !baitAlive:
		t.Errorf("integration: VERDICT: BOTH nodes are gone after one remove of %q - record the "+
			"shape in DESIGN §13 before drawing any conclusion", bait)
	default:
		t.Logf("VERDICT: neither node died - the remove did not apply, so this run yields no " +
			"verdict on the parser")
	}
}

// rootChildExists reports whether the view ROOT lists a child with the given name.
// The lookup goes through the parent listing rather than query_node on purpose: the
// whole question is whether a path-addressed turn on this name is ambiguous, so the
// existence check must not use the ambiguous form.
func rootChildExists(t *testing.T, sc *scenario, name string) bool {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	// "/" and not "": an EMPTY path is input-invalid and answers 28672 (the same
	// validation class docs/INTEGRATION.md records), while the view root is addressed
	// as "/" - confirmed against a live DSM, mirroring engine's view-root probe.
	nodes, listed, err := sc.client.List(ctx, sc.view.ViewID, "/")
	if err != nil {
		t.Fatalf("integration: list the view root: %v", err)
	}
	if !listed {
		t.Fatalf("integration: the view root listing carried no enumeration")
	}
	for _, n := range nodes {
		if n.Name == name || strings.TrimLeft(n.Path, "/") == name {
			return true
		}
	}
	return false
}
