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
	"github.com/mfilippov/synosync/internal/auth"
)

// envRestartProbe opts into the service-restart capture and sets how many SECONDS to probe
// for. The restart itself is triggered OUT OF BAND (`synopkg restart SynologyDrive` over
// ssh) while this runs - the scenario only observes, so it never touches the stand's
// package state itself.
const envRestartProbe = "SYNOSYNC_IT_RESTART_PROBE"

// restartProbeInterval paces the probe. It is slow on purpose: the interesting events are
// the TRANSITIONS across a service restart (last good turn → what the live session gets →
// when a re-dial starts working again), and a tight loop would bury them in thousands of
// identical rows without locating them any better than a second does.
const restartProbeInterval = 500 * time.Millisecond

// TestIntegration_ServiceRestartWindow captures what a Drive Server restart does to a
// session that was already accepted, and whether the service reports a LIVE node as absent
// while it comes back up. It settles two questions at once that no idle stand can answer:
//
//   - action-turn rejection: does `20481` (client version rejected) ever arrive on an ACTION
//     turn of a live session - "for instance after a DSM package upgrade that does not drop
//     the TCP connection"? The client maps the action-turn code to the same terminal
//     auth.ErrClientVersion sentinel as the connect axis, so Run stops with an "update
//     synosync" hint instead of dead-lettering one op at a time; if the connection always
//     dies first, that arm is insurance that costs nothing.
//   - path error under load, the half resource starvation could NOT reproduce
//     (TestIntegration_PathErrorUnderLoad): does a busy-coming-back service answer a path
//     error for a node that exists? A restart is the closest thing to the "package upgrade
//     window" the residual names, without the event-log churn a deliberate re-index costs.
//
// The probe holds one session and asks about a live node twice a second, recording every
// verdict CLASS and re-dialling when the connection dies - so the timeline shows the last
// good turn, exactly what the established session received, and how long a fresh connect
// kept failing afterwards. It asserts nothing: each outcome is a measurement.
//
// Run it and restart the package from the side, e.g.
//
//	SYNOSYNC_IT_RESTART_PROBE=120 go test -tags integration -run ServiceRestartWindow ./test/integration/...
//	ssh dsm 'sudo synopkg restart SynologyDrive'   # ~10 s in
func TestIntegration_ServiceRestartWindow(t *testing.T) {
	raw := os.Getenv(envRestartProbe)
	if raw == "" {
		t.Skipf("integration: set %s=<seconds> to run the service-restart capture (and restart "+
			"the Drive package from the side while it runs)", envRestartProbe)
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		t.Fatalf("integration: %s=%q is not a positive duration in seconds", envRestartProbe, raw)
	}

	sc := setupScenarioNoEngine(t)
	live := sc.remotePut(t, "restart/live.txt", []byte("alive across a restart\n"))
	rel := sc.viewRel("restart/live.txt")
	t.Logf("probing %q (file_id=%q) every %s for %ds - restart the package now",
		rel, live.FileID, restartProbeInterval, secs)

	// A client of our own over the ENGINE pairing, so re-dialling here cannot disturb the
	// mutator client the scenario helpers (and its own t.Cleanup) hold.
	dial := func() (*actions.Client, error) {
		ctx, cancel := actionCtx()
		defer cancel()
		sess, derr := sc.connector(ctx)
		if derr != nil {
			return nil, derr
		}
		ci := sc.h.clientInfo()
		return actions.NewClientWithIdentity(sess, sc.session, actions.Identity{
			Agent:         ci.Agent(sc.deviceUUID),
			ClientVersion: ci.ClientVersion(),
			ClientType:    ci.Type,
			DsID:          sc.dsID,
		}, nil), nil
	}

	client, err := dial()
	if err != nil {
		t.Fatalf("integration: initial dial: %v", err)
	}

	var (
		lastClass  string
		transition int
		dials      int
	)
	note := func(class, detail string) {
		if class == lastClass {
			return
		}
		transition++
		lastClass = class
		t.Logf("TIMELINE %2d  t=%s  %-18s %s", transition, time.Now().Format("15:04:05.000"), class, detail)
	}

	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	for time.Now().Before(deadline) {
		if client == nil {
			c, derr := dial()
			if derr != nil {
				note("dial-failed", classify(derr))
				time.Sleep(restartProbeInterval)
				continue
			}
			dials++
			client = c
			note("re-dialled", fmt.Sprintf("connect #%d succeeded", dials))
		}

		ctx, cancel := actionCtx()
		_, qerr := client.QueryNode(ctx, sc.view.ViewID, rel)
		cancel()

		switch {
		case qerr == nil:
			note("ok", "query_node of the live node answered")
		case errors.Is(qerr, auth.ErrClientVersion):
			note("CLIENT-VERSION", "20481 on an ACTION turn of an accepted session: "+classify(qerr))
			client = nil
		case errors.Is(qerr, auth.ErrSessionVanished):
			note("session-vanished", "8193 on an action turn: "+classify(qerr))
			client = nil
		case errors.Is(qerr, actions.ErrPathError):
			// The interesting one for the path-error-under-load question: the node exists.
			note("PATH-ERROR", "a LIVE node reported absent: "+classify(qerr))
		case errors.Is(qerr, actions.ErrConnBroken), errors.Is(qerr, actions.ErrTransferStalled):
			note("conn-broken", classify(qerr))
			client = nil
		default:
			note("other-failure", classify(qerr))
			client = nil
		}
		time.Sleep(restartProbeInterval)
	}

	t.Logf("VERDICT: %d transitions, %d re-dials over %ds - read the timeline above: the class "+
		"the ESTABLISHED session saw first is the answer to the action-turn rejection question, and any PATH-ERROR "+
		"row is the path-error-under-load window (compare its span against absenceStreakMinAge, 60 s)",
		transition, dials, secs)
	if dials == 0 && transition <= 1 {
		t.Logf("VERDICT: nothing happened - the service was never restarted while this ran, so " +
			"this run measured nothing")
	}
}

// classify renders an error for the timeline: the typed sentinel when one matched, plus the
// raw text, which for a transport failure is the only detail there is.
func classify(err error) string {
	if code, ok := actions.ServerErrorCode(err); ok {
		return fmt.Sprintf("errno=%d (%v)", code, err)
	}
	return err.Error()
}
