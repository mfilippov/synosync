package engine

// Tests for the stuck-escalation Warn gates. The
// escalation used to fire only on the drain where the failure counter EQUALLED
// stuckThreshold, which handed each diagnosis to whichever error happened to land on that
// one drain: the counter is monotonic and shared by the culprit (noteOpFailure) and victim
// (noteOpBlocked) paths, and is cleared only by a success or a dead-letter - neither of
// which a permanently wedged op reaches. So an unrelated transient on the Nth failure
// permanently ate the decoder-cap diagnosis, and an op that crossed the threshold while
// blocked and only later started failing its own apply emitted neither Warn ever. The gates
// now read "at or past the threshold, once per streak", with an independent latch per Warn
// flavour.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// stuckOp builds the minimal queued op the escalation helpers read.
func stuckOp(seq uint64) store.QueuedOp {
	return store.QueuedOp{
		Seq: seq,
		Op: store.Op{
			Type: store.OpDownload, Direction: store.DirInbound,
			ViewID: 7, Path: "Photos/huge",
		},
	}
}

// TestStuckWarnCapDiagnosisSurvivesAnUnrelatedThresholdFailure is the red test for the cap
// diagnosis: an op wedged on a decoder-cap breach fails its stuckThreshold-th drain with an
// UNRELATED transient (a reconnect), and every later drain with the cap error. The cap diagnosis -
// the one line naming the real cause - must still reach the log, exactly once.
func TestStuckWarnCapDiagnosisSurvivesAnUnrelatedThresholdFailure(t *testing.T) {
	e, cap := captureEngine(t, newTestStore(t), nil)
	qo := stuckOp(41)
	capErr := fmt.Errorf("actions: list: %w", sproto.ErrFrameTooLarge)

	// Drains 1..stuckThreshold-1: cap errors below the threshold stay silent (no flood).
	for i := 1; i < stuckThreshold; i++ {
		e.noteOpFailure(qo, capErr)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 0 {
		t.Fatalf("decoder-cap warns below the threshold = %d, want 0", n)
	}
	// The threshold drain fails with something else entirely.
	e.noteOpFailure(qo, errors.New("transport: connection reset by peer"))
	if n := cap.countWarns("op stuck: kept failing"); n != 1 {
		t.Fatalf("stuck warns at the threshold = %d, want 1", n)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 0 {
		t.Fatalf("decoder-cap warns after an unrelated threshold failure = %d, want 0", n)
	}
	// Every later drain breaches the cap again: the diagnosis must land now.
	for i := 0; i < 5; i++ {
		e.noteOpFailure(qo, capErr)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 1 {
		t.Errorf("decoder-cap warns past the threshold = %d, want exactly 1 (latched once per streak)", n)
	}
	if n := cap.countWarns("op stuck: kept failing"); n != 1 {
		t.Errorf("stuck warns = %d, want 1 (one-shot per streak)", n)
	}
}

// TestStuckWarnBlockedVictimBecomingCulpritStillWarns is the red test for the blocked-victim
// mirror: noteOpBlocked bumps the SAME counter as noteOpFailure, so an op that spends its first
// stuckThreshold drains merely blocked behind a failing predecessor steps clean over an
// equality gate the moment it starts failing on its own - the log would forever blame the
// predecessor that already left the queue. Both roles must be reported, once each.
func TestStuckWarnBlockedVictimBecomingCulpritStillWarns(t *testing.T) {
	e, cap := captureEngine(t, newTestStore(t), nil)
	qo := stuckOp(42)
	culprit := &chainCulprit{seq: 7, typ: store.OpMkdir, path: "Photos"}

	for i := 0; i < stuckThreshold+3; i++ {
		e.noteOpBlocked(qo, culprit)
	}
	if n := cap.countWarns("blocked behind a conflicting op that keeps failing"); n != 1 {
		t.Fatalf("blocked warns = %d, want exactly 1 (one-shot per streak)", n)
	}
	if n := cap.countWarns("op stuck: kept failing"); n != 0 {
		t.Fatalf("culprit warns while only blocked = %d, want 0", n)
	}

	// The predecessor resolves and this op finally runs - and fails its own apply with a
	// decoder-cap breach, already far past the threshold.
	capErr := fmt.Errorf("actions: list: %w", sproto.ErrFrameTooLarge)
	e.noteOpFailure(qo, capErr)
	if n := cap.countWarns("op stuck: kept failing"); n != 1 {
		t.Errorf("culprit warns after the victim became the culprit = %d, want 1", n)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 1 {
		t.Errorf("decoder-cap warns = %d, want 1", n)
	}
	// Still no flood once both flavours have fired.
	for i := 0; i < 4; i++ {
		e.noteOpFailure(qo, capErr)
	}
	if n := cap.countWarns("op stuck: kept failing"); n != 1 {
		t.Errorf("culprit warns after more failures = %d, want 1", n)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 1 {
		t.Errorf("decoder-cap warns after more failures = %d, want 1", n)
	}
}

// TestStuckWarnFlavoursLatchIndependentlyAndResetWithTheStreak pins the anti-flood contract
// (at most one Warn of each flavour per streak, both roles reachable) and the reset: an op
// that leaves the queue clears its latch, so a fresh wedge re-earns the log lines.
func TestStuckWarnFlavoursLatchIndependentlyAndResetWithTheStreak(t *testing.T) {
	e, cap := captureEngine(t, newTestStore(t), nil)
	qo := stuckOp(43)
	capErr := fmt.Errorf("actions: list: %w", sproto.ErrFrameTooLarge)

	// A long mixed streak: culprit failures interleaved with blocked drains.
	for i := 0; i < stuckThreshold; i++ {
		e.noteOpFailure(qo, capErr)
		e.noteOpBlocked(qo, nil)
	}
	if n := cap.countWarns("op stuck: kept failing"); n != 1 {
		t.Errorf("culprit warns over a long mixed streak = %d, want 1", n)
	}
	if n := cap.countWarns("blocked behind a conflicting op from an earlier drain window"); n != 1 {
		t.Errorf("blocked (nil-culprit) warns over a long mixed streak = %d, want 1", n)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 1 {
		t.Errorf("decoder-cap warns over a long mixed streak = %d, want 1", n)
	}
	if got := e.StuckOps(); got != 1 {
		t.Errorf("StuckOps = %d, want 1", got)
	}

	// The op leaves the queue and a later op reuses the seq: the streak - and every latch -
	// is gone, so the next wedge is reported again.
	e.clearOpFailures(qo.Seq)
	if got := e.StuckOps(); got != 0 {
		t.Fatalf("StuckOps after clear = %d, want 0", got)
	}
	e.stuckMu.Lock()
	_, latched := e.opWarned[qo.Seq]
	e.stuckMu.Unlock()
	if latched {
		t.Errorf("warn latch survived clearOpFailures")
	}
	for i := 0; i < stuckThreshold; i++ {
		e.noteOpFailure(qo, capErr)
	}
	if n := cap.countWarns("op stuck: kept failing"); n != 2 {
		t.Errorf("culprit warns after a second streak = %d, want 2", n)
	}
	if n := cap.countWarns("exceeded a decoder cap"); n != 2 {
		t.Errorf("decoder-cap warns after a second streak = %d, want 2", n)
	}
}
