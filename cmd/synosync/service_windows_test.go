package main

// Windows-only: the SCM return-contract mapping. Built and run only on a Windows host
// (the sibling service_windows.go is GOOS-tagged), so CI's linux/macos jobs skip it.

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
)

// TestSvcExitCode pins the svc.Handler return contract. A non-zero daemon exit code must be
// reported as a SERVICE-SPECIFIC code (ssec=true) - with ssec=false SCM reads the number as a
// Win32 system error, so "1" surfaces as ERROR_INVALID_FUNCTION ("Incorrect function") in the
// service snap-in. A code outside the 0-255 process-exit range cannot be reported faithfully
// through the uint32 return, so it normalises to 1 ("failed") rather than a wrapped number.
func TestSvcExitCode(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		wantSsec bool
		wantCode uint32
	}{
		{name: "clean stop", code: 0, wantSsec: false, wantCode: 0},
		{name: "ordinary failure", code: 1, wantSsec: true, wantCode: 1},
		{name: "highest representable code", code: 255, wantSsec: true, wantCode: 255},
		{name: "out-of-range high normalises to failed", code: 256, wantSsec: true, wantCode: 1},
		{name: "negative normalises to failed", code: -1, wantSsec: true, wantCode: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ssec, code := svcExitCode(tc.code)
			if ssec != tc.wantSsec || code != tc.wantCode {
				t.Errorf("svcExitCode(%d) = %v/%d, want %v/%d", tc.code, ssec, code, tc.wantSsec, tc.wantCode)
			}
		})
	}
}

// svcResult captures the svc.Handler return pair.
type svcResult struct {
	ssec bool
	code uint32
}

// scmStatus is one SetServiceStatus call as SCM would receive it: the status the
// handler reported, plus the two exit-code fields svc.(*service).updateStatus
// derives from the exit code it holds AT THAT MOMENT. The distinction is the
// whole point of the harness - a status the handler sends over the changes
// channel is published with whatever code the dispatcher has, and the real one
// only arrives when Execute RETURNS.
type scmStatus struct {
	svc.Status
	win32ExitCode       uint32
	svcSpecificExitCode uint32
	// final marks the last SetServiceStatus of the service's life: the Stopped
	// that svc.serviceMain reports after Execute returns, from its return values.
	final bool
}

// publish mirrors svc.(*service).updateStatus's exit-code mapping (x/sys
// v0.47.0, windows/svc/service.go:180-189).
func publish(st svc.Status, ec svcResult, final bool) scmStatus {
	out := scmStatus{Status: st, final: final}
	switch {
	case ec.code == 0:
		out.win32ExitCode = uint32(windows.NO_ERROR)
		out.svcSpecificExitCode = uint32(windows.NO_ERROR)
	case ec.ssec:
		out.win32ExitCode = uint32(windows.ERROR_SERVICE_SPECIFIC_ERROR)
		out.svcSpecificExitCode = ec.code
	default:
		out.win32ExitCode = ec.code
		out.svcSpecificExitCode = uint32(windows.NO_ERROR)
	}
	return out
}

// serviceHarness drives daemonService.Execute over unbuffered channels, the same
// way the SCM dispatcher does: every status the handler reports must be read by
// the test before the handler proceeds, so the assertions below see the exact
// status sequence in order with no sampling races.
//
// It models svc.serviceMain rather than just tapping the changes channel: a
// dispatcher goroutine translates each reported status into the
// SetServiceStatus call it would produce, carrying the DEFAULT exit code
// (isSvcSpecific:true, errno:0) until Execute returns, and then emits the final
// Stopped from Execute's return values. Without that plumbing a test cannot tell
// a Stopped that reaches SCM as NO_ERROR from one that carries the daemon's exit
// code - which is exactly the defect this harness pins.
type serviceHarness struct {
	t       *testing.T
	r       chan svc.ChangeRequest
	changes chan svc.Status
	posted  chan scmStatus
	ret     chan svcResult
}

func startService(t *testing.T, run func(ctx context.Context) int) *serviceHarness {
	t.Helper()
	h := &serviceHarness{
		t:       t,
		r:       make(chan svc.ChangeRequest),
		changes: make(chan svc.Status),
		posted:  make(chan scmStatus),
		ret:     make(chan svcResult, 1),
	}
	s := &daemonService{run: run}
	exitFromHandler := make(chan svcResult, 1)
	go func() {
		ssec, code := s.Execute(nil, h.r, h.changes)
		exitFromHandler <- svcResult{ssec: ssec, code: code}
	}()
	go h.dispatch(exitFromHandler)
	return h
}

// dispatch is the serviceMain status loop: publish every handler-reported status
// with the exit code known so far, then publish the authoritative Stopped once
// Execute has returned its code.
func (h *serviceHarness) dispatch(exitFromHandler <-chan svcResult) {
	// serviceMain's `ec := exitCode{isSvcSpecific: true, errno: 0}`.
	ec := svcResult{ssec: true, code: 0}
	for {
		select {
		case st := <-h.changes:
			h.posted <- publish(st, ec, false)
		case res := <-exitFromHandler:
			ec = res
			h.posted <- publish(svc.Status{State: svc.Stopped}, ec, true)
			h.ret <- res
			return
		}
	}
}

// next reads the next SetServiceStatus call SCM would see, failing the test
// rather than hanging if the service stops reporting.
func (h *serviceHarness) next() scmStatus {
	h.t.Helper()
	select {
	case st := <-h.posted:
		return st
	case <-time.After(10 * time.Second):
		h.t.Fatal("timed out waiting for the next service status")
		return scmStatus{}
	}
}

// drainToFinal reads every status up to and including the final Stopped that
// svc reports from Execute's return values. Statuses the handler sends itself
// therefore appear BEFORE it, which is how the assertions below detect a
// premature Stopped.
func (h *serviceHarness) drainToFinal() []scmStatus {
	h.t.Helper()
	var got []scmStatus
	for {
		st := h.next()
		got = append(got, st)
		if st.final {
			return got
		}
	}
}

// stoppedCount counts how many SetServiceStatus calls in a sequence report
// SERVICE_STOPPED. MSDN leaves a second one after STOPPED undefined, so the
// contract is exactly one.
func stoppedCount(seq []scmStatus) int {
	n := 0
	for _, st := range seq {
		if st.State == svc.Stopped {
			n++
		}
	}
	return n
}

func (h *serviceHarness) send(cmd svc.Cmd, current svc.Status) {
	h.t.Helper()
	select {
	case h.r <- svc.ChangeRequest{Cmd: cmd, CurrentStatus: current}:
	case <-time.After(10 * time.Second):
		h.t.Fatalf("timed out delivering control %v", cmd)
	}
}

func (h *serviceHarness) result() svcResult {
	h.t.Helper()
	select {
	case res := <-h.ret:
		return res
	case <-time.After(10 * time.Second):
		h.t.Fatal("timed out waiting for Execute to return")
		return svcResult{}
	}
}

// awaitRunning consumes the StartPending/Running prologue.
func (h *serviceHarness) awaitRunning() svc.Status {
	h.t.Helper()
	if st := h.next(); st.State != svc.StartPending {
		h.t.Fatalf("first status = %v, want StartPending", st.State)
	}
	running := h.next()
	if running.State != svc.Running {
		h.t.Fatalf("second status = %v, want Running", running.State)
	}
	if running.Accepts&svc.AcceptStop == 0 || running.Accepts&svc.AcceptShutdown == 0 {
		h.t.Fatalf("Running accepts = %v, want Stop and Shutdown", running.Accepts)
	}
	return running.Status
}

// shortenStopCheckpoints makes the drain re-report progress fast enough for a
// test to observe several checkpoints.
func shortenStopCheckpoints(t *testing.T, d time.Duration) {
	t.Helper()
	prev := stopCheckpointInterval
	stopCheckpointInterval = d
	t.Cleanup(func() { stopCheckpointInterval = prev })
}

// TestExecuteCheckpointsStopPendingDuringSlowDrain is a
// regression test: engine teardown is not bounded by the stop wait hint (a local scan
// is only cancellable between views), so a drain that outlives the hint must keep
// reporting progress. Before the fix Execute sent ONE StopPending and then blocked
// on the run loop, and SCM parked the service in STOP_PENDING - rejecting both Stop
// and Start - until the scan finished. Now the drain re-reports StopPending with a
// strictly increasing CheckPoint until the run loop returns, then reports Stopped
// once with the run loop's exit code.
func TestExecuteCheckpointsStopPendingDuringSlowDrain(t *testing.T) {
	shortenStopCheckpoints(t, 5*time.Millisecond)

	release := make(chan struct{})
	h := startService(t, func(ctx context.Context) int {
		<-ctx.Done()
		// Stand in for a full-rehash scan that keeps running past cancellation.
		<-release
		return 3
	})
	running := h.awaitRunning()
	h.send(svc.Stop, running)

	const wantCheckpoints = 3
	var last uint32
	for i := 0; i < wantCheckpoints; i++ {
		st := h.next()
		if st.State != svc.StopPending {
			t.Fatalf("status %d = %v, want StopPending", i, st.State)
		}
		if st.CheckPoint <= last {
			t.Fatalf("checkpoint %d = %d, want > %d (SCM only restarts its clock on an increment)", i, st.CheckPoint, last)
		}
		if st.WaitHint != stopPendingWaitHintMs {
			t.Fatalf("StopPending wait hint = %d, want %d", st.WaitHint, stopPendingWaitHintMs)
		}
		last = st.CheckPoint
	}

	close(release)
	// A ticker firing concurrently with the drain finishing may report one more
	// checkpoint; every status until Stopped must still be a StopPending that
	// advances the checkpoint.
	seq := h.drainToFinal()
	for _, st := range seq[:len(seq)-1] {
		if st.State != svc.StopPending {
			t.Fatalf("trailing status = %v, want StopPending or Stopped", st.State)
		}
		if st.CheckPoint <= last {
			t.Fatalf("trailing checkpoint = %d, want > %d", st.CheckPoint, last)
		}
		last = st.CheckPoint
	}
	if n := stoppedCount(seq); n != 1 {
		t.Fatalf("SERVICE_STOPPED reported %d times, want exactly 1", n)
	}
	if final := seq[len(seq)-1]; final.svcSpecificExitCode != 3 {
		t.Fatalf("final Stopped specific code = %d, want 3", final.svcSpecificExitCode)
	}

	if res := h.result(); !res.ssec || res.code != 3 {
		t.Fatalf("Execute = %v/%d, want true/3", res.ssec, res.code)
	}
}

// TestExecuteFastStopReportsOneCheckpoint pins that a prompt teardown keeps the
// pre-fix status sequence: one StopPending, then Stopped. The checkpoint loop is
// only a fallback for drains that outlive the hint, so it must not add chatter to
// the common case.
func TestExecuteFastStopReportsOneCheckpoint(t *testing.T) {
	shortenStopCheckpoints(t, time.Hour)

	h := startService(t, func(ctx context.Context) int {
		<-ctx.Done()
		return 0
	})
	running := h.awaitRunning()
	h.send(svc.Shutdown, running)

	if st := h.next(); st.State != svc.StopPending || st.CheckPoint != 1 {
		t.Fatalf("stop status = %v/cp %d, want StopPending/cp 1", st.State, st.CheckPoint)
	}
	st := h.next()
	if st.State != svc.Stopped || !st.final {
		t.Fatalf("final status = %v (final=%v), want the authoritative Stopped", st.State, st.final)
	}
	if res := h.result(); res.ssec || res.code != 0 {
		t.Fatalf("Execute = %v/%d, want false/0 for a clean stop", res.ssec, res.code)
	}
}

// TestExecuteAnswersInterrogateWhileStopping keeps the control plane truthful
// during the drain: SCM carries the last status it saw in the request, so echoing
// it back would report Running while the service is stopping. The drain answers
// with its own current StopPending, checkpoint included.
func TestExecuteAnswersInterrogateWhileStopping(t *testing.T) {
	shortenStopCheckpoints(t, time.Hour)

	release := make(chan struct{})
	h := startService(t, func(ctx context.Context) int {
		<-ctx.Done()
		<-release
		return 0
	})
	running := h.awaitRunning()
	h.send(svc.Stop, running)

	stopping := h.next()
	if stopping.State != svc.StopPending {
		t.Fatalf("stop status = %v, want StopPending", stopping.State)
	}
	// A stale Running snapshot, exactly as SCM would carry it.
	h.send(svc.Interrogate, running)
	if st := h.next(); st.State != svc.StopPending || st.CheckPoint != stopping.CheckPoint {
		t.Fatalf("interrogate reply = %v/cp %d, want StopPending/cp %d", st.State, st.CheckPoint, stopping.CheckPoint)
	}
	// A repeated Stop while the drain runs is a no-op, not a second cancellation
	// path or a duplicate report.
	h.send(svc.Stop, stopping.Status)

	close(release)
	if st := h.next(); st.State != svc.Stopped || !st.final {
		t.Fatalf("final status = %v (final=%v), want the authoritative Stopped", st.State, st.final)
	}
	if res := h.result(); res.ssec || res.code != 0 {
		t.Fatalf("Execute = %v/%d, want false/0", res.ssec, res.code)
	}
}

// TestExecuteReportsSelfExit covers the daemon terminating on its own (a vanished
// session, say): Stopped is reported with its exit code and no StopPending is
// involved, since nobody asked the service to stop.
//
// This is the regression test for that defect. Execute used to send
// `svc.Status{State: svc.Stopped}` before returning, and svc.serviceMain
// publishes a handler-sent status with the exit code it holds AT THAT MOMENT -
// still the default 0. So SCM received SERVICE_STOPPED/NO_ERROR first and the
// daemon's code only in a second SetServiceStatus after STOPPED, which MSDN
// leaves undefined: the terminal failure was recorded as a CLEAN stop and the
// packaging's failure actions (`sc.exe failure … restart` + `failureflag 1`)
// never fired - sync stopped silently, with no restart and nothing in the
// service state to say why. Now the only STOPPED is the one svc reports from
// Execute's return, so it carries the code.
func TestExecuteReportsSelfExit(t *testing.T) {
	exit := make(chan int)
	h := startService(t, func(context.Context) int { return <-exit })
	h.awaitRunning()

	exit <- 1
	seq := h.drainToFinal()
	if n := stoppedCount(seq); n != 1 {
		t.Fatalf("SERVICE_STOPPED reported %d times, want exactly 1 (a second one after STOPPED is undefined)", n)
	}
	st := seq[len(seq)-1]
	if st.State != svc.Stopped || !st.final {
		t.Fatalf("last status = %v (final=%v), want the authoritative Stopped", st.State, st.final)
	}
	// The failure must be visible to SCM as a failure: ERROR_SERVICE_SPECIFIC_ERROR
	// with the daemon's code, never NO_ERROR - failureflag 1 keys the restart on
	// exactly this.
	if st.win32ExitCode != uint32(windows.ERROR_SERVICE_SPECIFIC_ERROR) || st.svcSpecificExitCode != 1 {
		t.Fatalf("Stopped exit codes = win32 %d / specific %d, want %d / 1",
			st.win32ExitCode, st.svcSpecificExitCode, uint32(windows.ERROR_SERVICE_SPECIFIC_ERROR))
	}
	if res := h.result(); !res.ssec || res.code != 1 {
		t.Fatalf("Execute = %v/%d, want true/1", res.ssec, res.code)
	}
}

// TestExecuteNeverReportsStoppedItself is the invariant behind the test above,
// stated once for every terminal arm: whatever route Execute takes out, the last
// status it writes to the changes channel is a PENDING one. A Stopped written
// there can only be published with the wrong exit code, since the right one does
// not exist until Execute returns.
func TestExecuteNeverReportsStoppedItself(t *testing.T) {
	tests := []struct {
		name string
		// stop drives the service to its terminal arm and returns the exit code
		// the run loop yields.
		stop func(h *serviceHarness, running svc.Status, exit chan int)
		code int
	}{
		{
			name: "self exit with a terminal error",
			stop: func(_ *serviceHarness, _ svc.Status, exit chan int) { exit <- 8 },
			code: 8,
		},
		{
			name: "stop control",
			stop: func(h *serviceHarness, running svc.Status, _ chan int) { h.send(svc.Stop, running) },
			code: 0,
		},
		{
			name: "shutdown control",
			stop: func(h *serviceHarness, running svc.Status, _ chan int) { h.send(svc.Shutdown, running) },
			code: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shortenStopCheckpoints(t, time.Hour)
			exit := make(chan int)
			h := startService(t, func(ctx context.Context) int {
				select {
				case code := <-exit:
					return code
				case <-ctx.Done():
					return tc.code
				}
			})
			running := h.awaitRunning()
			tc.stop(h, running, exit)

			seq := h.drainToFinal()
			for _, st := range seq[:len(seq)-1] {
				if st.State == svc.Stopped {
					t.Fatalf("Execute reported Stopped itself; SCM would see it with exit code win32 %d / specific %d",
						st.win32ExitCode, st.svcSpecificExitCode)
				}
			}
			if n := stoppedCount(seq); n != 1 {
				t.Fatalf("SERVICE_STOPPED reported %d times, want exactly 1", n)
			}
		})
	}
}

// TestExecuteCleanStopReportsNoError is the other half of the exit-code contract:
// an operator-requested stop that drains cleanly must publish its single
// SERVICE_STOPPED with NO_ERROR in BOTH exit-code fields. failureflag 1 makes SCM
// treat any non-zero code as a failure, so a clean stop that leaked a code would
// have the failure actions restart a service the operator just stopped.
func TestExecuteCleanStopReportsNoError(t *testing.T) {
	for _, cmd := range []struct {
		name string
		cmd  svc.Cmd
	}{{"stop", svc.Stop}, {"shutdown", svc.Shutdown}} {
		t.Run(cmd.name, func(t *testing.T) {
			shortenStopCheckpoints(t, time.Hour)
			h := startService(t, func(ctx context.Context) int {
				<-ctx.Done()
				return 0
			})
			running := h.awaitRunning()
			h.send(cmd.cmd, running)

			seq := h.drainToFinal()
			if n := stoppedCount(seq); n != 1 {
				t.Fatalf("SERVICE_STOPPED reported %d times, want exactly 1", n)
			}
			st := seq[len(seq)-1]
			if st.win32ExitCode != uint32(windows.NO_ERROR) || st.svcSpecificExitCode != uint32(windows.NO_ERROR) {
				t.Fatalf("clean-stop exit codes = win32 %d / specific %d, want NO_ERROR/NO_ERROR",
					st.win32ExitCode, st.svcSpecificExitCode)
			}
			if res := h.result(); res.ssec || res.code != 0 {
				t.Fatalf("Execute = %v/%d, want false/0", res.ssec, res.code)
			}
		})
	}
}

// TestExecuteAnswersInterrogateWhileRunning pins the pre-existing running-state
// echo, so the drain-time change above is not mistaken for the general rule.
func TestExecuteAnswersInterrogateWhileRunning(t *testing.T) {
	h := startService(t, func(ctx context.Context) int {
		<-ctx.Done()
		return 0
	})
	running := h.awaitRunning()
	h.send(svc.Interrogate, running)
	if st := h.next(); st.State != svc.Running {
		t.Fatalf("interrogate reply = %v, want Running", st.State)
	}
	h.send(svc.Stop, running)
	if st := h.next(); st.State != svc.StopPending {
		t.Fatalf("stop status = %v, want StopPending", st.State)
	}
	if st := h.next(); st.State != svc.Stopped {
		t.Fatalf("final status = %v, want Stopped", st.State)
	}
	_ = h.result()
}

// TestStopCheckpointIntervalLeavesHintHeadroom pins the doctrine the comment in
// service_windows.go states: SCM must see a checkpoint increment well before the
// wait hint expires, or the drain looks unresponsive exactly once. Half the hint
// is the conventional headroom.
func TestStopCheckpointIntervalLeavesHintHeadroom(t *testing.T) {
	hint := time.Duration(stopPendingWaitHintMs) * time.Millisecond
	if stopCheckpointInterval <= 0 || stopCheckpointInterval > hint/2 {
		t.Fatalf("stopCheckpointInterval = %v, want in (0, %v]", stopCheckpointInterval, hint/2)
	}
}
