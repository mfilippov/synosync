package actions

// Tests for the GRANULARITY of the write side of the idle watchdog.
//
// uploadidle_test.go pins that the body write reports progress at all - without it a
// two-frame upload turn sees the whole body transfer as silence and the budget is a
// fixed deadline on it. This file pins the residual that fix left: progress is
// reported only when the underlying Write RETURNS, and a net.Conn/tls.Conn Write
// returns only once the whole slice is with the kernel. Bytes the peer drains while
// that call is blocked are invisible, so the guarantee is idle-per-CHUNK and carries a
// floor rate - progressWriteChunk over the turn's budget - below which the watchdog
// fires on a live link and the op re-pushes from byte zero every drain.
//
// The floor cannot be removed on stdlib TLS (nothing reports per record), so it is
// pinned instead: an invariant over the shipped constants, and an end-to-end slow-drip
// writer whose rate sits above the new floor but far below the old one.

import (
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/sproto"
)

// writeIdleFloorBytesPerSec is the sustained write rate below which the guard's write side
// degenerates into a fixed deadline: a chunk that takes a whole budget to hand to the kernel
// reports no progress until it returns, so the watchdog fires mid-chunk on a live link. It is
// the arithmetic the doc comment on progressWriteChunk states, kept here rather than in the
// production file because nothing but these assertions ever needs to compute it. A
// non-positive chunk or budget has no floor - the guard is inert.
func writeIdleFloorBytesPerSec(chunk int, budget time.Duration) float64 {
	if chunk <= 0 || budget <= 0 {
		return 0
	}
	return float64(chunk) / budget.Seconds()
}

// TestShippedWriteIdleFloorIsBelowAnyLiveLink is the doc invariant: whatever the
// constants become, the write-side floor must stay below a rate a live TCP connection
// could plausibly sustain, and the chunk must stay within one TLS record so the finer
// granularity costs at most one extra record per chunk rather than a fragmented one.
func TestShippedWriteIdleFloorIsBelowAnyLiveLink(t *testing.T) {
	const maxFloorBytesPerSec = 20.0 // ~a slow modem line; no healthy link is under this
	const maxTLSRecord = 16 << 10

	floor := writeIdleFloorBytesPerSec(progressWriteChunk, defaultActionIdleTimeout)
	if floor <= 0 {
		t.Fatalf("shipped write-side floor = %g B/s, want a positive floor: a non-chunked write reports progress once, at the end", floor)
	}
	if floor > maxFloorBytesPerSec {
		t.Errorf("shipped write-side floor = %g B/s (chunk %d over budget %s), want <= %g: above it the guard aborts a legitimately slow but live uplink mid-chunk and the op re-pushes from byte zero every drain",
			floor, progressWriteChunk, defaultActionIdleTimeout, maxFloorBytesPerSec)
	}
	if progressWriteChunk > maxTLSRecord {
		t.Errorf("progressWriteChunk = %d, want <= %d: a chunk larger than one TLS record buys no granularity the record layer does not already lose", progressWriteChunk, maxTLSRecord)
	}
	if actionIdleTimeout != defaultActionIdleTimeout {
		t.Errorf("actionIdleTimeout = %s, want the shipped %s: this invariant is about the value the daemon runs with", actionIdleTimeout, defaultActionIdleTimeout)
	}
}

// TestSlowButLiveUploadWriterAboveTheFloorSurvivesSeveralBudgets is the behavioural
// half. The peer drains at ~102 KiB/s - orders of magnitude above the shipped floor,
// and a rate at which the transfer spans several idle budgets. With the old 64 KiB
// granularity a single write chunk took ~640 ms to hand over, more than three budgets
// with no report in between, so the guard fired mid-chunk on a socket that was moving
// the whole time. At progressWriteChunk = 4 KiB the same link reports every ~40 ms and
// the upload completes.
func TestSlowButLiveUploadWriterAboveTheFloorSurvivesSeveralBudgets(t *testing.T) {
	const idle = 200 * time.Millisecond
	shortActionIdle(t, idle)

	// The pacing (8 KiB per 80 ms = 102 KiB/s) is the point of the test: scaled to this
	// shortened budget it sits BELOW the old 64 KiB granularity's floor (327 KiB/s) and
	// far above the 4 KiB one (20 KiB/s). Nothing here asserts that arithmetic - the
	// Upload below is the assertion, so raising progressWriteChunk back over ~20 KiB
	// fails this test as the stall it causes rather than as a mismatched precondition.
	const paceChunk = 8 << 10
	const pacePause = 80 * time.Millisecond

	content := make([]byte, 128<<10)
	for i := range content {
		content[i] = byte(i * 7)
	}
	conn := serveMock(t, slowDrainServer(paceChunk, pacePause, func() sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("slowf")).
			Set("hash", sproto.String(md4.ContentHash(content)))
	}))
	c := newClient(conn, "sess")

	start := time.Now()
	node, err := c.Upload(testContext(t), "p", "slow.bin", 0, 1, content)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Upload err = %v, want nil: the peer drained %g B/s throughout, above the write-side floor of %g B/s (chunk %d over budget %s) - an idle budget must never abort a transfer that is moving",
			err, float64(paceChunk)/pacePause.Seconds(), writeIdleFloorBytesPerSec(progressWriteChunk, idle), progressWriteChunk, idle)
	}
	if elapsed <= 2*idle {
		t.Fatalf("Upload finished in %s, want a transfer outlasting several idle budgets (%s each) so the per-chunk re-arm is what carried it", elapsed, idle)
	}
	if node.FileID != "slowf" {
		t.Errorf("node.FileID = %q, want slowf", node.FileID)
	}
	if c.Broken() {
		t.Error("Broken() = true after a slow but successful upload, want the connection reusable")
	}
}
