//go:build integration

package integration

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// envLoadProbe opts into the overload capture and sets how many SECONDS to probe for.
// Like the big-list capture it is separate from the required SYNOSYNC_IT_* set: it is
// minutes long, it only means anything while the stand is deliberately loaded from the
// outside, and a few of its probes leave tombstones in the view's event log.
const envLoadProbe = "SYNOSYNC_IT_LOAD_PROBE"

// removeProbeInterval spaces the destructive probes in WALL CLOCK rather than in rounds.
// The question needs `remove` of a LIVE node, and each such probe costs one create plus one
// delete - i.e. one permanent tombstone in the view's log (measured: the log is compacted
// per node, so a create+delete pair collapses to a single delete row). Rounds are the wrong
// unit for that budget: the read loop turns ~65 times a second on an idle stand, so a
// every-tenth-round rule spent 193 tombstones in 30 seconds, and a slow stand - exactly the
// condition this scenario creates - would spend far fewer while the operator expects the
// same coverage. Two seconds keeps a several-minute capture near a hundred, which is what
// stops it from doing to the stand what the 10k listing capture did.
const removeProbeInterval = 2 * time.Second

// TestIntegration_PathErrorUnderLoad settles a residual as far as a stand can:
// does `28672` - or any of the codes the client folds into ErrPathError - come back for a
// node that demonstrably EXISTS while the server is under load, and for how long?
//
// It matters because ErrPathError conflates "no such path" with a server-side timeout, and
// the client's absence provers consume it as evidence. That is why an absence verdict needs
// absentNodeConfirmations agreeing probes AND absenceStreakMinAge (60 s) of wall clock
// between the counted ones: the floor is sized on the assumption that an overload reply
// window spans MINUTES, more than two drains. If the stand shows the window is seconds, the
// floor is generous; if it shows minutes, absenceStreakMinAge is the knob to raise.
//
// The probe loop keeps a live file and its parent directory in place and asks about them
// over and over - query_node of the file, list of the parent - recording every verdict and
// its latency. Every removeProbeEvery-th round it also creates a node and removes it, which
// is the only way to observe the mutating verb's behaviour on a node that is certainly
// there.
//
// It ASSERTS nothing about the outcome: a path error on a live node is the finding, not a
// failure, and so is its absence. The run prints a verdict block to be copied into
// docs/INTEGRATION.md. Load the stand separately (see the same document) - probing an idle
// server measures nothing.
func TestIntegration_PathErrorUnderLoad(t *testing.T) {
	raw := os.Getenv(envLoadProbe)
	if raw == "" {
		t.Skipf("integration: set %s=<seconds> to run the overload capture (and load the stand "+
			"separately - probing an idle server measures nothing)", envLoadProbe)
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		t.Fatalf("integration: %s=%q is not a positive duration in seconds", envLoadProbe, raw)
	}
	budget := time.Duration(secs) * time.Second

	sc := setupScenarioNoEngine(t)

	live := sc.remotePut(t, "load/live.txt", []byte("live under load\n"))
	t.Logf("probing for %s against %q (file_id=%q)", budget, sc.viewRel("load/live.txt"), live.FileID)

	var (
		rounds        int
		queryPathErr  int
		queryOtherErr int
		listPathErr   int
		listUnlisted  int
		listOtherErr  int
		removes       int
		removePathErr int
		worstQuery    time.Duration
		worstList     time.Duration
		firstAnomaly  time.Time
		lastAnomaly   time.Time
	)
	noteAnomaly := func(what string, err error) {
		now := time.Now()
		if firstAnomaly.IsZero() {
			firstAnomaly = now
		}
		lastAnomaly = now
		t.Logf("ANOMALY t=%s %s: %s", now.Format("15:04:05.000"), what, describeServerErr(err))
	}

	deadline := time.Now().Add(budget)
	var lastRemoveProbe time.Time
	for time.Now().Before(deadline) {
		rounds++

		func() {
			ctx, cancel := actionCtx()
			defer cancel()

			start := time.Now()
			_, qerr := sc.client.QueryNode(ctx, sc.view.ViewID, sc.viewRel("load/live.txt"))
			if d := time.Since(start); d > worstQuery {
				worstQuery = d
			}
			switch {
			case qerr == nil:
			case errors.Is(qerr, actions.ErrPathError):
				queryPathErr++
				noteAnomaly("query_node of a LIVE file answered a path error", qerr)
			default:
				queryOtherErr++
				noteAnomaly("query_node of a LIVE file failed", qerr)
			}

			start = time.Now()
			_, listed, lerr := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("load"))
			if d := time.Since(start); d > worstList {
				worstList = d
			}
			switch {
			case lerr != nil && errors.Is(lerr, actions.ErrPathError):
				listPathErr++
				noteAnomaly("list of a LIVE directory answered a path error", lerr)
			case lerr != nil:
				listOtherErr++
				noteAnomaly("list of a LIVE directory failed", lerr)
			case !listed:
				listUnlisted++
				noteAnomaly("list of a LIVE directory carried NO enumeration", fmt.Errorf("listed=false"))
			}
		}()

		if time.Since(lastRemoveProbe) < removeProbeInterval {
			continue
		}
		lastRemoveProbe = time.Now()
		func() {
			ctx, cancel := actionCtx()
			defer cancel()
			name := fmt.Sprintf("victim-%04d.txt", removes)
			parent, ok := sc.queryRel(t, "load")
			if !ok {
				noteAnomaly("query_node of the LIVE parent reports it absent", fmt.Errorf("absent"))
				return
			}
			if _, uerr := sc.client.Upload(ctx, parent.FileID, name, time.Now().Unix(),
				sc.view.RootNodeID, []byte("victim\n")); uerr != nil {
				noteAnomaly("upload of the victim failed", uerr)
				return
			}
			removes++
			rerr := sc.client.Remove(ctx, sc.view.ViewID, sc.viewRel("load/"+name))
			if rerr != nil && errors.Is(rerr, actions.ErrPathError) {
				removePathErr++
				noteAnomaly("remove of a JUST-CREATED node answered a path error", rerr)
			} else if rerr != nil {
				noteAnomaly("remove of a JUST-CREATED node failed", rerr)
			}
		}()
	}

	t.Logf("VERDICT after %s: %d read rounds, %d remove probes", budget, rounds, removes)
	t.Logf("VERDICT query_node(live): %d path errors, %d other failures, worst latency %s",
		queryPathErr, queryOtherErr, worstQuery.Round(time.Millisecond))
	t.Logf("VERDICT list(live dir): %d path errors, %d unlisted, %d other failures, worst latency %s",
		listPathErr, listUnlisted, listOtherErr, worstList.Round(time.Millisecond))
	t.Logf("VERDICT remove(live node): %d path errors out of %d", removePathErr, removes)
	if firstAnomaly.IsZero() {
		t.Logf("VERDICT: no anomaly - under this load the server never reported a live node as " +
			"a path error, so absenceStreakMinAge is not contradicted by this run (it is also " +
			"not confirmed: absence of a repro is not a measurement of the window)")
		return
	}
	t.Logf("VERDICT anomaly window: first %s, last %s, span %s - compare against "+
		"absenceStreakMinAge (60 s): a span WIDER than the floor means a real deletion could be "+
		"corroborated by two probes that both landed inside one overload window",
		firstAnomaly.Format("15:04:05"), lastAnomaly.Format("15:04:05"),
		lastAnomaly.Sub(firstAnomaly).Round(time.Second))
}
