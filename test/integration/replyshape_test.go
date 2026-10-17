//go:build integration

package integration

import (
	"testing"
)

// TestIntegration_ListReplyShape pins the ENUMERATION EVIDENCE a live DSM gives
// for the `list` action - the residual DESIGN §13 left open when
// List gained its third state.
//
// The doctrine (DESIGN §6, "Reply evidence") makes an emptiness a prover only
// when the server actually ENUMERATED: List returns (nodes, listed, err), and
// every absence prover (resolveServerNodeAbsence, nodeGoneByID,
// confirmAbsentByParentList, ancestorConfirmsAbsent) treats listed == false as
// inconclusive and leaves its op queued. That is the never-lose direction, but
// it has a cost the residual named explicitly: if a live DSM answers an EMPTY
// directory with a bare result verdict and NO list key, then listed == false is
// unconvincing identically on every retry, and an outbound remove whose
// post-condition listing lands on such a directory retries forever (Stuck,
// blocking its whole path-overlapping chain) instead of acking idempotently.
//
// So the question is not decidable from the client's own code - only the server
// answers it. This scenario asks it directly:
//
//   - an empty server directory must come back listed with zero children
//     (positive proof of emptiness), NOT as an errno-only reply;
//   - a populated directory must come back listed with exactly its children,
//     which pins the reply's array key (a live DSM sends "node_list"; the
//     "file_list" fallback in catalog.go exists only for the in-memory fakes
//     written against the earlier assumption).
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_ListReplyShape(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	// An empty directory: created and never populated.
	emptyDir := sc.remoteMkdirAll(t, "empty")
	if emptyDir.FileID == "" {
		t.Fatalf("integration: server mkdir %q returned no file id", sc.viewRel("empty"))
	}
	nodes, listed, err := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("empty"))
	if err != nil {
		t.Fatalf("integration: list empty dir %q: %v", sc.viewRel("empty"), err)
	}
	if !listed {
		t.Fatalf("integration: live DSM answered an EMPTY directory %q with no enumeration "+
			"(listed=false) - the DESIGN §13 residual is real: every absence prover reads "+
			"this as inconclusive on every retry, so an outbound remove whose post-condition "+
			"listing lands here never acks. Record the measured shape in DESIGN §13 and give "+
			"the provers an exit (id-addressed proof or a dead-letter bound).",
			sc.viewRel("empty"))
	}
	if len(nodes) != 0 {
		t.Fatalf("integration: list of empty dir %q returned %d children, want 0",
			sc.viewRel("empty"), len(nodes))
	}

	// A populated directory: two files and a subdirectory. This pins the array
	// key the live server actually uses - a reply whose children rode under an
	// unrecognised key would decode as listed=false here, not merely as an empty
	// slice.
	sc.remotePut(t, "full/a.txt", []byte("a"))
	sc.remotePut(t, "full/b.txt", []byte("b"))
	sc.remoteMkdirAll(t, "full/sub")

	nodes, listed, err = sc.client.List(ctx, sc.view.ViewID, sc.viewRel("full"))
	if err != nil {
		t.Fatalf("integration: list populated dir %q: %v", sc.viewRel("full"), err)
	}
	if !listed {
		t.Fatalf("integration: list of populated dir %q carried no enumeration (listed=false) - "+
			"the reply's children ride under a key catalog.go does not read", sc.viewRel("full"))
	}
	got := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		got[n.Name] = n.IsDir
	}
	for name, wantDir := range map[string]bool{"a.txt": false, "b.txt": false, "sub": true} {
		isDir, ok := got[name]
		if !ok {
			t.Errorf("integration: list of %q omits child %q (got %v)", sc.viewRel("full"), name, got)
			continue
		}
		if isDir != wantDir {
			t.Errorf("integration: child %q is_dir=%v, want %v", name, isDir, wantDir)
		}
	}
	if len(nodes) != 3 {
		t.Errorf("integration: list of %q returned %d children, want exactly 3 (got %v)",
			sc.viewRel("full"), len(nodes), got)
	}
}
