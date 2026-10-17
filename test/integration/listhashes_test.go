//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// TestIntegration_ListCarriesContentHashes settles the premise a tree-walk cold start
// rests on: does `list` report a per-file content hash?
//
// TestIntegration_NodeHashes already pinned that query_node and the pull_event log carry
// one. That is enough for a cold start built on the LOG, which is what exists today. A
// cold start built on a tree WALK reads its nodes from `list` instead, and the hash is
// what lets it adopt a local file that already matches the server without transferring a
// byte. Without one, adoption degrades to size+kind - the weaker evidence coldStartContentMatch
// warns about - or, worse, re-downloads a tree that is already on disk.
//
// The scenario writes files of known, DISTINCT content, lists their parent, and asserts
// every file node carries the hash of what was written. Directories are expected to carry
// none (query_node reports the same), so they are excluded rather than asserted on.
func TestIntegration_ListCarriesContentHashes(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := actionCtx()
	defer cancel()

	want := map[string]string{}
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("h%02d.txt", i)
		// Distinct content per file, so a server echoing one hash for all of them is caught.
		body := []byte(fmt.Sprintf("content number %d, padded to differ in length%s\n", i, string(make([]byte, i*7))))
		node := sc.remotePut(t, "listhash/"+name, body)
		want[name] = node.Hash
		t.Logf("uploaded %s: size=%d hash=%q", name, node.Size, node.Hash)
	}
	sc.remoteMkdirAll(t, "listhash/subdir")

	nodes, listed, err := sc.client.List(ctx, sc.view.ViewID, "/"+sc.viewRel("listhash"))
	if err != nil {
		t.Fatalf("integration: list: %v", err)
	}
	if !listed {
		t.Fatalf("integration: list carried no evidence")
	}
	t.Logf("list returned %d node(s)", len(nodes))

	var files, withHash, matching int
	for _, n := range nodes {
		if n.IsDir {
			t.Logf("  dir  %-14s hash=%q (directories are expected to carry none)", n.Name, n.Hash)
			continue
		}
		files++
		if n.Hash != "" {
			withHash++
		}
		if w, ok := want[n.Name]; ok && w != "" && n.Hash == w {
			matching++
		}
		t.Logf("  file %-14s size=%-4d hash=%q", n.Name, n.Size, n.Hash)
	}

	t.Logf("VERDICT: list reported %d file node(s), %d with a hash, %d matching the upload reply",
		files, withHash, matching)

	if files != len(want) {
		t.Fatalf("list returned %d files, want %d", files, len(want))
	}
	if withHash != files {
		t.Errorf("%d of %d file nodes carried no hash: a tree-walk cold start would fall back to "+
			"size+kind evidence for them, or re-download content already on disk", files-withHash, files)
	}
	if matching != files {
		t.Errorf("%d of %d hashes disagreed with the upload reply for the same content", files-matching, files)
	}
}
