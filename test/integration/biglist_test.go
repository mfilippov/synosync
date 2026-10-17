//go:build integration

package integration

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// envBigList opts into the large-directory listing capture and sets how many children
// to create. It is separate from the required SYNOSYNC_IT_* set because this scenario is
// minutes long where every other one is seconds: `list` is a single turn, but BUILDING the
// directory is one upload per child and tearing it down is one remove per child (the verb
// is single-node), so a 10k run costs ~20k round trips. Leaving it opt-in keeps
// `make test-integration` at its ~20 s and still lets the question be settled on demand.
const envBigList = "SYNOSYNC_IT_BIG_LIST"

// TestIntegration_BigListPagination settles a residual (reinforcing
// the `list`/`view_list` completeness residual in DESIGN §13): does a live DSM's `list`
// reply PAGE or TRUNCATE its children array?
//
// This is the single unconfirmed premise under every absence verdict. The client's own
// corroboration is pagination-proof by construction - an absence derived from a listing is
// re-proved by a path-addressed QueryNode of the node itself (absenceCorroboratedByNodeProbe),
// twice agreeing - so the answer changes the measured SEVERITY of the class, not the
// client's behaviour. What a truncating server would break is the OTHER direction: a
// listing that silently omits children makes confirmAbsentByParentList "prove" an absence
// for a node that is present but past the cut, and it makes removeTreeRemote-style
// enumeration incomplete.
//
// Run it explicitly with a child count that exceeds any plausible page:
//
//	SYNOSYNC_IT_BIG_LIST=10000 go test -tags integration -run BigList ./test/integration/...
//
// The scenario asserts the two things a truncation would break:
//
//  1. every created child is present in the reply (count AND membership - a reply that
//     merely capped the count would still be caught by the missing names);
//  2. two identical calls return the same slice, so a server that pages internally cannot
//     hand back a different window each time without the test seeing it.
//
// It deliberately does NOT try to read a cursor/end marker out of the frame: the typed
// client discards unknown keys, so that question needs a raw-turn hook and is only worth
// building if this scenario shows a truncation at all.
func TestIntegration_BigListPagination(t *testing.T) {
	raw := os.Getenv(envBigList)
	if raw == "" {
		t.Skipf("integration: set %s=<child count> to run the large-directory listing capture "+
			"(minutes: one upload per child to build, one remove per child to tear down)", envBigList)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("integration: %s=%q is not a positive child count", envBigList, raw)
	}

	sc := setupScenarioNoEngine(t)

	parent := sc.remoteMkdirAll(t, "big")
	if parent.FileID == "" {
		t.Fatalf("integration: server mkdir %q returned no file id", sc.viewRel("big"))
	}

	want := make(map[string]bool, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%06d.txt", i)
		func() {
			ctx, cancel := actionCtx()
			defer cancel()
			if _, err := sc.client.Upload(ctx, parent.FileID, name, time.Now().Unix(),
				sc.view.RootNodeID, []byte("x")); err != nil {
				t.Fatalf("integration: upload child %d/%d (%q): %v", i+1, n, name, err)
			}
		}()
		want[name] = true
		if (i+1)%1000 == 0 {
			t.Logf("created %d/%d children in %s", i+1, n, time.Since(start).Round(time.Second))
		}
	}
	t.Logf("built %d children in %s", n, time.Since(start).Round(time.Second))

	list := func(label string) []string {
		ctx, cancel := actionCtx()
		defer cancel()
		nodes, listed, err := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("big"))
		if err != nil {
			t.Fatalf("integration: %s list of %q: %v", label, sc.viewRel("big"), err)
		}
		if !listed {
			t.Fatalf("integration: %s list of %q carried no enumeration", label, sc.viewRel("big"))
		}
		names := make([]string, 0, len(nodes))
		for _, node := range nodes {
			names = append(names, node.Name)
		}
		return names
	}

	first := list("first")
	t.Logf("VERDICT: list of a %d-child directory returned %d children", n, len(first))

	got := make(map[string]bool, len(first))
	for _, name := range first {
		got[name] = true
	}
	var missing []string
	for name := range want {
		if !got[name] {
			missing = append(missing, name)
			if len(missing) >= 5 {
				break
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("integration: the listing of a %d-child directory OMITS children (returned %d; "+
			"missing at least %v) - the server pages or truncates `list`. Record it in DESIGN §13: "+
			"confirmAbsentByParentList can then read a present-but-omitted node as absent, and every "+
			"listing-derived enumeration is incomplete. The absence provers' node-probe corroboration "+
			"(absenceCorroboratedByNodeProbe) is what keeps this from becoming a wrong delete.",
			n, len(first), missing)
	}
	if len(first) != n {
		t.Errorf("integration: listing returned %d children, want exactly %d (extra entries mean "+
			"the scratch tree was not clean)", len(first), n)
	}

	// A server that pages internally could hand back a different window per call even when
	// one window happens to be complete.
	second := list("second")
	if len(second) != len(first) {
		t.Errorf("integration: two identical list calls returned %d and %d children - the reply is "+
			"not stable across calls", len(first), len(second))
	}
	for _, name := range second {
		if !got[name] {
			t.Errorf("integration: the second listing carries %q, absent from the first - the server "+
				"returns a different window per call", name)
			break
		}
	}
}
