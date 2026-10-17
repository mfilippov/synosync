//go:build integration

package integration

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_TrailingDotSpaceNames settles a naming residual:
// does a live DSM store a node whose leaf name ends in a dot or a space?
//
// The name is legal on ext4/APFS and a Linux/macOS peer can create it, but
// Win32 SILENTLY STRIPS both from every non-`\\?\` path - so on Windows such a
// name is addressed as one thing and lands as another, with no errno anywhere
// for a classifier to catch. The client therefore refuses the class outright
// (reconcile skips the event, an already-queued op dead-letters with a trace,
// hostRepresentablePath). Whether that arm is ever REACHABLE from the server
// side depends entirely on this answer:
//
//   - if DSM refuses the create, the whole class is unreachable from the server
//     and only a local create remains, which goes out as an ordinary upload;
//   - if DSM stores it verbatim, the dead-letter trace plus "rename it on the
//     server/peer" is the operator's remedy, and the open question becomes how
//     often real peers produce such names.
//
// The scenario also asks the sharper follow-up: does the server RESOLVE the
// stripped spelling (`notes` for a stored `notes.`)? A server that does would
// turn Windows' silent strip into a silent hit on a DIFFERENT node - the exact
// "addressed ≠ landed" failure the gate exists to prevent - while a server that
// answers a path error makes the mangling merely noisy.
//
// It is capture-first: the outcome is recorded rather than presumed, and only
// the client-relevant invariant of each branch is asserted.
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_TrailingDotSpaceNames(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	parent := sc.remoteMkdirAll(t, "names")
	if parent.FileID == "" {
		t.Fatalf("integration: server mkdir %q returned no file id", sc.viewRel("names"))
	}

	cases := []struct {
		label    string
		name     string // the name as created
		stripped string // what Win32 would silently address it as
	}{
		{label: "trailing dot", name: "dot.", stripped: "dot"},
		{label: "trailing space", name: "space ", stripped: "space"},
		{label: "trailing dot on an extension", name: "notes.txt.", stripped: "notes.txt"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			ctx, cancel := actionCtx()
			defer cancel()

			content := []byte("trailing-name capture: " + tc.label + "\n")
			node, err := sc.client.Upload(ctx, parent.FileID, tc.name, time.Now().Unix(),
				sc.view.RootNodeID, content)
			if err != nil {
				t.Logf("VERDICT: DSM REFUSED the create of %q: %s", tc.name, describeServerErr(err))
				// A refusal is a complete answer for this name: the class is
				// unreachable from the server side. Nothing further to probe.
				return
			}
			t.Logf("VERDICT: DSM ACCEPTED the create of %q: file_id=%q reported name=%q size=%d",
				tc.name, node.FileID, node.Name, node.Size)

			// Did it store the name VERBATIM, or rewrite it? The listing is the
			// authority - the upload reply may echo back what we sent.
			children, listed, err := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("names"))
			if err != nil {
				t.Fatalf("integration: list %q: %v", sc.viewRel("names"), err)
			}
			if !listed {
				t.Fatalf("integration: list %q carried no enumeration", sc.viewRel("names"))
			}
			var stored string
			for _, c := range children {
				if c.FileID == node.FileID {
					stored = c.Name
					break
				}
			}
			switch stored {
			case "":
				t.Errorf("integration: the created node %q (%s) is absent from its parent's listing "+
					"(%d children) - created but not enumerated", tc.name, node.FileID, len(children))
			case tc.name:
				t.Logf("stored VERBATIM as %q", stored)
			default:
				t.Logf("stored REWRITTEN: sent %q, listed as %q", tc.name, stored)
			}

			// Addressing it by the exact spelling.
			exact, okExact := sc.queryRel(t, "names/"+tc.name)
			t.Logf("query_node(exact %q): present=%v file_id=%q", tc.name, okExact, exact.FileID)

			// Addressing it by the STRIPPED spelling - what a Windows client
			// would actually put on the wire if the gate were absent. Resolving
			// to the same node is the dangerous answer.
			strippedNode, okStripped := sc.queryRel(t, "names/"+tc.stripped)
			switch {
			case !okStripped:
				t.Logf("query_node(stripped %q): ABSENT - the server does not alias the stripped "+
					"spelling, so a Windows-mangled path fails loudly rather than hitting another node",
					tc.stripped)
			case strippedNode.FileID == node.FileID:
				t.Errorf("integration: query_node of the STRIPPED spelling %q resolved to the SAME node "+
					"as %q (file_id %s) - the server aliases the two spellings, so a Windows client whose "+
					"path was silently stripped would address this node believing it addressed another. "+
					"hostRepresentablePath is what stands between that and a wrong-node write; record the "+
					"measurement in DESIGN §13.", tc.stripped, tc.name, node.FileID)
			default:
				t.Logf("query_node(stripped %q): present but a DIFFERENT node (file_id=%q)",
					tc.stripped, strippedNode.FileID)
			}
		})
	}
}

// describeServerErr renders a server rejection for the capture log: the typed
// sentinel when the client mapped one, plus the raw numeric code when the reply
// carried an unmapped errno.
func describeServerErr(err error) string {
	var parts []string
	switch {
	case errors.Is(err, actions.ErrPathError):
		parts = append(parts, "ErrPathError (28672/32769/40961)")
	case errors.Is(err, actions.ErrHashMismatch):
		parts = append(parts, "ErrHashMismatch")
	case errors.Is(err, actions.ErrConflictVersion):
		parts = append(parts, "ErrConflictVersion (24579)")
	}
	if code, ok := actions.ServerErrorCode(err); ok {
		parts = append(parts, "errno="+strconv.FormatInt(code, 10))
	}
	parts = append(parts, err.Error())
	return strings.Join(parts, "; ")
}
