//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// scrubTimeout bounds the whole scrub. Deleting is one server turn per node, so a
// tree the size BigListPagination builds (10 000 children) costs minutes.
const scrubTimeout = 30 * time.Minute

// TestIntegration_ScrubScratchBase deletes the ENTIRE scratch base
// (SYNOSYNC_IT_REMOTE_BASE, default /synosync-it) on the stand, not just one
// scenario's per-test subtree.
//
// It exists because scenario teardown is best-effort: a run killed mid-flight -
// or one whose teardown hit a transient failure - leaves its subtree behind, and
// the stand then pays for it forever. Cold start replays the view's whole catalog,
// so an abandoned 10 000-child tree (SYNOSYNC_IT_BIG_LIST) makes
// TestIntegration_KeepBothColdStart download 10 000 files before it reaches its own
// first assertion, and it fails on its budget with nothing wrong in the client.
//
// It is opt-in via SYNOSYNC_IT_SCRUB=1 because it is destructive to the scratch
// base as a whole, including a CONCURRENT run's in-flight subtree.
func TestIntegration_ScrubScratchBase(t *testing.T) {
	h := requireEnv(t)
	if os.Getenv("SYNOSYNC_IT_SCRUB") != "1" {
		t.Skip("integration: set SYNOSYNC_IT_SCRUB=1 to scrub the whole scratch base")
	}

	_, mutPair := ensurePaired(t, h)
	client, _ := newRemoteClient(t, h, mutPair.connector, mutPair.session, mutPair.deviceUUID, mutPair.dsID)
	view := pickView(t, client, h)

	base := strings.TrimLeft(strings.TrimRight(h.RemoteBase, "/"), "/")
	if base == "" {
		t.Fatalf("integration: refusing to scrub an empty scratch base")
	}

	ctx, cancel := context.WithTimeout(context.Background(), scrubTimeout)
	defer cancel()

	children, listed, err := client.List(ctx, view.ViewID, base)
	switch {
	case err != nil && errors.Is(err, actions.ErrPathError):
		t.Logf("scratch base %q is already absent in view %q", base, view.Name)
		return
	case err != nil:
		t.Fatalf("integration: list scratch base %q: %v", base, err)
	case !listed:
		t.Fatalf("integration: list scratch base %q: reply carried no listing", base)
	}
	t.Logf("scrubbing %d entries under %q in view %q", len(children), base, view.Name)

	start := time.Now()
	if err := removeTreeRemote(ctx, client, view.ViewID, base); err != nil {
		t.Fatalf("integration: scrub %q: %v", base, err)
	}
	t.Logf("scrubbed %q in %s", base, time.Since(start).Round(time.Second))
}
