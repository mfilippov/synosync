//go:build integration

package integration

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestIntegration_FirstSyncOfAFreshTreeTransfers is the coverage a first sync did not have.
//
// TestIntegration_KeepBothColdStart drives a real cold start, but over ONE file - and one
// file is exactly the case that hides a wrong target_sync_id, because the log head and that
// file's own sync id are then the same number. A tree of sixty files, seeded before the
// engine starts, is what separates them.
//
// The failure it pins: the cold-start walk stamped the pre-walk log head as every node's
// SyncID, which reaches the server as a download's target_sync_id - a request for a SPECIFIC
// version. Measured on DSM 7.2.2-72806 / Drive Server 4.0.3-27892, target_sync_id 0 returns
// the current version while the log head is refused outright (32772). Every download of a
// first sync therefore failed, permanently, with the queue full and one file on disk.
//
// It asserts the FILES rather than the queue: a healthy drain empties the queue between
// polls, and what a first sync owes is content on disk.
func TestIntegration_FirstSyncOfAFreshTreeTransfers(t *testing.T) {
	const n = 60
	sc := setupScenarioNoEngine(t)
	sc.remoteMkdirAll(t, "fresh")
	for i := 0; i < n; i++ {
		sc.remotePut(t, fmt.Sprintf("fresh/f%03d.txt", i), []byte(fmt.Sprintf("body %d\n", i)))
	}
	sc.start(t)

	// Assert the FILES, not the queue: a fast drain can empty the queue between polls, and
	// what a first sync owes is content on disk.
	start := time.Now()
	for i := 0; i < 40; i++ {
		got := 0
		for j := 0; j < n; j++ {
			if _, err := os.Stat(sc.localAbs(t, fmt.Sprintf("fresh/f%03d.txt", j))); err == nil {
				got++
			}
		}
		pend, _ := sc.st.PendingLen()
		// Files only: the view may hold more than this tree, and its downloads keep the queue busy.
		if got == n {
			t.Logf("all %d files on disk in %s", n, time.Since(start).Round(time.Millisecond))
			return
		}
		if i%5 == 0 {
			t.Logf("%6s  on disk %d/%d  pending=%d", time.Since(start).Round(time.Second), got, n, pend)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("first sync did not deliver the tree within %s", time.Since(start).Round(time.Second))
}
