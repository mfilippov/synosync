//go:build integration

package integration

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// This file characterises HOW pull_event paginates, because cold start's correctness
// USED to rest on it: the cold start read from sync_id 0 and persisted the cursor it ended on,
// and the incremental poll then reads strictly forward from there. A cursor left short of
// the log head is not a slow start - it is history replayed through the ordinary inbound
// path, where a delete recorded years ago is applied as a live one.
//
// Measured 23.08.2026 on DSM 7.2.2-72806 / Synology Drive Server 4.0.3-27892:
//
//   - pull_event pages by a FIXED-WIDTH sync_id WINDOW (2000), not by a row count. The
//     cursor advanced by exactly 2000 per page while the rows on those pages ranged from
//     137 to 2000.
//   - The log is NOT dense. TestIntegration_EventLogCompaction already showed a deleted
//     node keeps one row - the delete - at a NEW, higher sync_id, so the slot its create
//     held is vacated. A bulk deletion therefore leaves a hole in the sync_id space.
//   - A window that falls inside a hole answers with NOTHING while rows continue past it,
//     and the reply still reports next_sync_id = cursor + 2000.
//
// Which is the whole failure: an empty page is proof about ONE WINDOW, never about the
// log. A live install stopped at cursor 16000 - a window boundary - against a log head of
// 856387, and replayed ~840k rows.

// pullPage issues ONE pull_event under its own deadline. actionCtx bounds a single
// exchange, so a scenario that seeds for minutes before reading cannot share one context
// across the whole run: the first read then fails on a deadline that has nothing to do
// with the server.
func pullPage(t *testing.T, sc *scenario, cursor int64) ([]actions.Event, int64, bool, error) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	return sc.client.PullEventEvidenced(ctx, sc.view.ViewID, sc.view.RootNodeID, cursor)
}

// logHead reads get_max_sync_id, the bound cold start corroborates an empty page against.
func logHead(t *testing.T, sc *scenario) int64 {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	head, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}
	return head
}

// walkCatalog paginates from sync_id 0 twice over: once the way the log reader used to,
// stopping at the first empty page, and once the way it does now, stepping over an empty
// window that the log head disproves. It returns both stop cursors plus the page shapes.
func walkCatalog(t *testing.T, sc *scenario, head int64) (naive, corroborated int64, rows int) {
	t.Helper()
	var cursor int64
	naive = -1
	for page := 1; ; page++ {
		evs, next, evidenced, err := pullPage(t, sc, cursor)
		if err != nil {
			t.Fatalf("integration: pull_event at %d: %v", cursor, err)
		}
		if !evidenced {
			t.Fatalf("integration: pull_event at %d carried no evidence", cursor)
		}
		if len(evs) == 0 {
			if naive < 0 {
				// Where the uncorroborated walk would have called the catalog exhausted.
				naive = cursor
				t.Logf("  EMPTY window at cursor %d: next_sync_id=%d, head=%d", cursor, next, head)
			}
			if cursor >= head || next <= cursor {
				return naive, cursor, rows
			}
			cursor = next
			continue
		}
		rows += len(evs)
		t.Logf("  page %d: %4d event(s), cursor %6d -> %6d", page, len(evs), cursor, next)
		if next <= cursor {
			return naive, cursor, rows
		}
		cursor = next
	}
}

// TestIntegration_ColdStartCursorReachesTheLogHead asserts the property cold start needs
// from whatever log the stand happens to hold: the corroborated walk ends AT the head.
//
// It also reports where the uncorroborated walk would have stopped. On a fresh stand the
// two agree, and that is exactly why the naive reading survived so long - a log with no
// holes cannot expose it. TestIntegration_ColdStartCursorAcrossACompactionGap makes one.
func TestIntegration_ColdStartCursorReachesTheLogHead(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	// A little history with all three shapes a replay must survive: a live node, a node
	// that moved, and a node that is gone.
	sc.remoteMkdirAll(t, "hist/old")
	for i := 0; i < 5; i++ {
		sc.remotePut(t, fmt.Sprintf("hist/old/f%d.txt", i), []byte("payload\n"))
	}
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID,
		sc.viewRel("hist/old"), sc.viewRel("hist/new"))
	serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel("hist/new/f0.txt"))

	head := logHead(t, sc)
	naive, corroborated, rows := walkCatalog(t, sc, head)
	t.Logf("walk: %d row(s); uncorroborated stop=%d, corroborated stop=%d, head=%d",
		rows, naive, corroborated, head)

	if corroborated < head {
		t.Errorf("the corroborated walk ended at %d against a log head of %d: %d sync_id of "+
			"history is left for the incremental poll to replay as live inbound work",
			corroborated, head, head-corroborated)
	}
	if naive >= 0 && naive < head {
		t.Logf("NOTE: the uncorroborated walk would have stopped at %d, %d sync_id short of the head",
			naive, head-naive)
	}
}

// envColdStartGap opts into the compaction-GAP measurement and sets how many nodes to
// create and then delete to punch a hole in the sync_id space. It is separate from the
// required SYNOSYNC_IT_* set for the same reason SYNOSYNC_IT_BIG_LIST is: building the
// hole is one upload and one remove per node, so this scenario is minutes long where the
// rest are seconds.
//
//	SYNOSYNC_IT_COLDSTART_GAP=6000 go test -tags integration -run ColdStartCursorAcrossACompactionGap ./test/integration/...
//
// The count has to exceed the page window (2000) by enough that a whole window lands
// inside the vacated span; 6000 reproduced it, 3000 only thinned a window to 137 rows.
const envColdStartGap = "SYNOSYNC_IT_COLDSTART_GAP"

func TestIntegration_ColdStartCursorAcrossACompactionGap(t *testing.T) {
	raw := os.Getenv(envColdStartGap)
	if raw == "" {
		t.Skipf("integration: %s is not set; skipping the compaction-gap measurement", envColdStartGap)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("integration: %s=%q must be a positive integer", envColdStartGap, raw)
	}

	sc := setupScenarioNoEngine(t)

	// One directory for the whole doomed set: every directory row SURVIVES the deletion of
	// its children and so keeps the hole from vacating completely.
	sc.remoteMkdirAll(t, "gap/doomed")
	start := time.Now()
	for i := 0; i < n; i++ {
		sc.remotePut(t, fmt.Sprintf("gap/doomed/f%05d.txt", i), []byte("doomed\n"))
	}
	t.Logf("created %d node(s) in %s", n, time.Since(start).Round(time.Second))

	start = time.Now()
	for i := 0; i < n; i++ {
		serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel(fmt.Sprintf("gap/doomed/f%05d.txt", i)))
	}
	t.Logf("deleted %d node(s) in %s", n, time.Since(start).Round(time.Second))

	// A tail past the hole, so "the catalog is exhausted" is demonstrably false if the
	// walk stops inside it.
	sc.remoteMkdirAll(t, "gap/tail")
	for i := 0; i < 20; i++ {
		sc.remotePut(t, fmt.Sprintf("gap/tail/t%02d.txt", i), []byte("past the hole\n"))
	}

	head := logHead(t, sc)
	naive, corroborated, rows := walkCatalog(t, sc, head)
	t.Logf("walk: %d row(s); uncorroborated stop=%d, corroborated stop=%d, head=%d",
		rows, naive, corroborated, head)

	if naive < 0 || naive >= head {
		t.Errorf("the hole did not empty a whole page window: the uncorroborated walk reached "+
			"%d against head %d, so this run proves nothing - raise %s", naive, head, envColdStartGap)
	}
	if corroborated < head {
		t.Errorf("the corroborated walk ended at %d against a log head of %d (%d sync_id short): "+
			"stepping the empty window did not carry it to the end of the log",
			corroborated, head, head-corroborated)
	}
}
