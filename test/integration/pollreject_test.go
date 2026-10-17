//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// pollRejectWait bounds each subscription probe. A REJECTED subscribe answers
// immediately, while an ACCEPTED one blocks for the server-side timeout
// (actions.MinPollTimeout, 300 s) - so a probe that runs out this budget has
// learned something too: the server took the subscription.
const pollRejectWait = 20 * time.Second

// TestIntegration_LongPollRejectionShape settles the long_poll rejection
// residual, which DESIGN §13 explicitly defers to this harness: what does a live DSM
// do when ONE member of a long_poll subscription is unacceptable?
//
// The engine's poison-view quarantine (engine/pollquarantine.go) is built to be safe
// under any answer - after a streak of server-stated rejections of the shared
// subscribe it probes each member with a single-view subscription and quarantines only
// the one the SERVER rejects on its own evidence. But the rejection shape itself had
// never been observed, so two things were unknown: whether such rejections happen at
// all, and whether a single-view probe reproduces one. If the reply named the culprit,
// direct attribution would be the better design and the probe round is dead weight.
//
// Three subscriptions are attempted, each bounded by pollRejectWait:
//
//	valid            - the control: the server should ACCEPT and block
//	bogus alone      - is a nonexistent view id rejected on its own?
//	valid + bogus    - does one bad member reject the WHOLE set?
//
// Nothing is asserted about which way it goes: each outcome is the measurement, and
// the quarantine is correct under all of them. What the typed client cannot show is
// whether a rejection names the offending view - LongPoll decodes only the changed
// ids - so that half stays open and is called out in the log.
func TestIntegration_LongPollRejectionShape(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	const bogusViewID = 999999
	const bogusNodeID = 999999

	probe := func(label string, subs []actions.Subscription) {
		ctx, cancel := context.WithTimeout(context.Background(), pollRejectWait)
		defer cancel()
		start := time.Now()
		ids, err := sc.client.LongPoll(ctx, subs, actions.MinPollTimeout)
		took := time.Since(start).Round(time.Millisecond)
		switch {
		case err == nil:
			t.Logf("VERDICT %-14s ACCEPTED - returned %v after %s (a clean tick or pending changes)",
				label, ids, took)
		case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
			t.Logf("VERDICT %-14s ACCEPTED - still blocking when our %s budget expired, so the "+
				"server took the subscription", label, pollRejectWait)
		default:
			code, ok := actions.ServerErrorCode(err)
			t.Logf("VERDICT %-14s REJECTED after %s: %s", label, took, describeServerErr(err))
			if !ok {
				t.Logf("    (no numeric errno - this is a transport/decode failure, NOT a server " +
					"verdict; pollServerRejected classifies exactly this distinction)")
				return
			}
			t.Logf("    server errno %d. Whether the reply also NAMES the offending view is not "+
				"visible through the typed client (LongPoll decodes only changed ids) - settling "+
				"that needs a raw turn, and it is what would decide between the current probe "+
				"round and direct attribution.", code)
		}
	}

	valid := actions.Subscription{
		ViewID: sc.view.ViewID,
		NodeID: sc.view.RootNodeID,
		SyncID: 0,
	}
	bogus := actions.Subscription{ViewID: bogusViewID, NodeID: bogusNodeID, SyncID: 0}

	probe("valid", []actions.Subscription{valid})
	probe("bogus alone", []actions.Subscription{bogus})
	probe("valid+bogus", []actions.Subscription{valid, bogus})
}
