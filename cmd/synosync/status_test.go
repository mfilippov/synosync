package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
)

// stubDaemonSocket starts a real in-process IPC server on a temp socket whose
// `status` method returns report, points resolveDaemonPaths at its directory, and
// registers cleanup. It returns nothing - the caller drives `status` through run().
func stubDaemonSocket(t *testing.T, report ipc.StatusReport) {
	t.Helper()
	dir := sockDir(t)
	paths := config.Paths{StateDir: dir}

	ln, err := ipc.Listen(paths.DaemonSock())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := ipc.NewServer()
	srv.Handle("status", func(context.Context, json.RawMessage) (any, error) { return report, nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })
}

func TestStatusRenders(t *testing.T) {
	report := ipc.StatusReport{
		Server:     "drive.example.com:6690",
		PendingOps: 4,
		Connection: ipc.ConnState{Paused: false, NumWorkers: 3},
		Views: []ipc.ViewStatus{
			{ID: 1, Name: "team", LastSyncID: 42, InitialSyncDone: true, Conflicts: 2},
			{ID: 2, Name: "home", LastSyncID: 0, InitialSyncDone: false, Conflicts: 0},
		},
	}
	stubDaemonSocket(t, report)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"Syncing with drive.example.com - 4 items to go",
		"Folder:    team",
		"Folder:    home",
		"Conflicts: 2 file(s) kept as _Conflict copies (synosync conflicts)",
		"Server:    connected",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestStatusRendersDeadLettersAndDiskFull(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:       "host:6690",
		PendingOps:   1,
		DeadLettered: 3,
		DiskFull:     true,
		Stuck:        2,
		Connection:   ipc.ConnState{NumWorkers: 2},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		// "item(s)", not "operation(s)": two of the classes counted here never had a queued op
		// (a refused directory delete, a fold-key collision the path-keyed queue cannot
		// even represent), so calling every entry an operation misreports the count.
		"Skipped:   3 item(s) the client gave up on (synosync drops)",
		"Retrying:  2 operation(s) failing repeatedly, still retrying",
		"Disk:      full",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout missing %q\nfull output:\n%s", want, out)
		}
	}
}

// TestStatusOmitsDeadLetterLinesWhenClean confirms the new lines are suppressed when
// there is nothing to report (no dead-letters, disk not full), keeping the common
// status terse.
func TestStatusOmitsDeadLetterLinesWhenClean(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{NumWorkers: 2},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	if strings.Contains(out, "Skipped:") || strings.Contains(out, "Disk:") || strings.Contains(out, "Retrying:") {
		t.Errorf("clean status should omit dead-letter/disk/stuck lines, got:\n%s", out)
	}
}

// TestStatusPaused checks the pause line names the cause the daemon classified. The
// three terminal causes need three different actions, and this line used to say
// "re-login needed" for all of them: wrong advice for a changed server certificate, and
// actively misleading for a rejected client version, where re-pairing cannot help.
// TestPausedConnText covers the other causes and the unclassified fallback.
func TestStatusPaused(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{Paused: true, NumWorkers: 2, PauseReason: ipc.PauseSessionExpired},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "paused (session expired") {
		t.Errorf("stdout = %q, want the session-expired pause message", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Folder:    none") {
		t.Errorf("stdout = %q, want no-views line", stdout.String())
	}
}

// TestStatusRendersConnecting confirms the third connection state: while the
// daemon's pool is still dialing (bring-up backoff, e.g. the server is
// unreachable at boot) the CLI must render "connecting..." rather than falsely
// claim "connected".
func TestStatusRendersConnecting(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{Connecting: true, NumWorkers: 3},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "connecting...") {
		t.Errorf("stdout missing connecting line, got:\n%s", out)
	}
	if strings.Contains(out, "connected") {
		t.Errorf("stdout must not claim connected while connecting, got:\n%s", out)
	}
}

// TestStatusRendersReconnecting confirms the mid-run outage state:
// the pool is up but a dropped connection is being re-dialed under
// backoff, so the CLI must render "reconnecting..." rather than falsely claim
// "connected" for the whole downtime.
func TestStatusRendersReconnecting(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{Reconnecting: true, NumWorkers: 2},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "reconnecting...") {
		t.Errorf("stdout missing reconnecting line, got:\n%s", out)
	}
	if strings.Contains(out, "connected") {
		t.Errorf("stdout must not claim connected while reconnecting, got:\n%s", out)
	}
}

// TestStatusDaemonBusy exercises the busy-vs-absent dial split: an error the
// per-OS classifier reports as "busy" (the Windows named-pipe connect timeout -
// injected through the seams, since that shape cannot be produced on a Unix
// host) prints the busy hint, not the misleading "daemon is not running".
func TestStatusDaemonBusy(t *testing.T) {
	dir := sockDir(t)
	origPaths := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return config.Paths{StateDir: dir}, nil }
	t.Cleanup(func() { resolveDaemonPaths = origPaths })

	sentinel := errors.New("pipe busy timeout")
	origDial := ipcDial
	ipcDial = func(string) (*ipc.Client, error) { return nil, sentinel }
	t.Cleanup(func() { ipcDial = origDial })

	origBusy := daemonBusy
	daemonBusy = func(err error) bool { return errors.Is(err, sentinel) }
	t.Cleanup(func() { daemonBusy = origBusy })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "busy or slow") {
		t.Errorf("stderr = %q, want busy message", stderr.String())
	}
	if strings.Contains(stderr.String(), "not running") {
		t.Errorf("stderr = %q, must not claim daemon absent", stderr.String())
	}
}

func TestStatusDaemonNotRunning(t *testing.T) {
	// A paired install with no socket: set up, daemon down. A state database that merely
	// EXISTS is not enough - read-only commands create one - so the fixture records an
	// identity, which is what the hint actually asks about. Without it this is a fresh install
	// and the CLI names the pairing step instead - see
	// TestControlVerbsReportPairingOnAFreshInstall.
	pointAtPairedDir(t)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not running") {
		t.Errorf("stderr = %q, want not-running message", stderr.String())
	}
}

func TestStatusBadArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

// daemonNotRunningCase is one dial-error classification case. The shared cases
// are platform-independent; the connection-refused cases differ by OS (a refused
// connection means "absent" only for the Unix AF_UNIX socket, not the Windows
// named pipe) and live in daemonNotRunningPlatformCases per build tag.
type daemonNotRunningCase struct {
	name string
	err  error
	want bool
}

func TestDaemonNotRunning(t *testing.T) {
	// daemonNotRunning classifies a dial error: a missing socket (ENOENT) means
	// "not running" on every platform; any other dial failure (e.g. a permission
	// error) is a real problem to surface. The connection-refused cases are
	// OS-specific (see daemonNotRunningPlatformCases).
	cases := []daemonNotRunningCase{
		{"missing socket", fs.ErrNotExist, true},
		{"enoent syscall", syscall.ENOENT, true},
		{"permission denied", syscall.EACCES, false},
		{"generic error", errors.New("boom"), false},
	}
	cases = append(cases, daemonNotRunningPlatformCases...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := daemonNotRunning(tc.err); got != tc.want {
				t.Errorf("daemonNotRunning(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestStatusRendersProgressOnlyOnceSomethingMoved: the line answers "slow or stuck", so it
// must not appear before there is an answer - "0 B in 0 op(s)" on a daemon that has not
// started transferring reads as a stalled sync when nothing has been asked of it yet.
func TestStatusRendersProgressOnlyOnceSomethingMoved(t *testing.T) {
	var idle bytes.Buffer
	renderStatus(&idle, ipc.StatusReport{Server: "nas:6690"})
	if strings.Contains(idle.String(), "This run:") {
		t.Errorf("idle status rendered a progress line:\n%s", idle.String())
	}

	var moving bytes.Buffer
	renderStatus(&moving, ipc.StatusReport{
		Server:           "nas:6690",
		AppliedOps:       4821,
		TransferredBytes: 13314398618,
		MovingFor:        5 * time.Minute,
	})
	out := moving.String()
	for _, want := range []string{"This run:", "12.4 GiB", "4,821 items", "in 5 min"} {
		if !strings.Contains(out, want) {
			t.Errorf("status is missing %q:\n%s", want, out)
		}
	}
}

// TestHumanBytesKeepsThreeSignificantFigures pins the rendering an operator compares against
// du and a progress bar.
func TestHumanBytesKeepsThreeSignificantFigures(t *testing.T) {
	cases := map[int64]string{
		0:             "0 B",
		512:           "512 B",
		1024:          "1.0 KiB",
		1536:          "1.5 KiB",
		1048576:       "1.0 MiB",
		13314398618:   "12.4 GiB",
		1099511627776: "1.0 TiB",
	}
	for n, want := range cases {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

// TestHumanRateDoesNotRoundASlowSyncToZero is the point of the whole line: an operator uses
// it to tell a slow sync from a stuck one, so a daemon steadily applying an op every few
// seconds must not read as "0 op/s".
func TestHumanRateDoesNotRoundASlowSyncToZero(t *testing.T) {
	cases := map[float64]string{
		0:      "0",
		0.0001: "<0.01",
		0.18:   "0.18",
		1.5:    "1.5",
		9.94:   "9.9",
		42.3:   "42",
	}
	for r, want := range cases {
		if got := humanRate(r); got != want {
			t.Errorf("humanRate(%v) = %q, want %q", r, got, want)
		}
	}
}

// TestStatusNamesStuckOperations pins that the Stuck line is followed by which files are wedged
// and why. The count alone told an operator that something was wrong and nothing actionable;
// the paths and reasons were in the daemon log only.
func TestStatusNamesStuckOperations(t *testing.T) {
	var buf bytes.Buffer
	renderStatus(&buf, ipc.StatusReport{
		Stuck: 3,
		StuckOps: []ipc.StuckEntry{
			{ViewID: 7, Op: "rename", Direction: "outbound", Path: "a.txt", NewPath: "b.txt",
				Failures: 9, Reason: "the destination is held by another node"},
			{ViewID: 7, Op: "download", Direction: "inbound", Path: "c.txt", Failures: 4},
		},
	})
	out := buf.String()
	for _, want := range []string{
		"Retrying:  3 operation(s) failing repeatedly, still retrying",
		"move (on the server)  a.txt",
		"moving to: b.txt",
		"the destination is held by another node",
		"download from the server (on this device)  c.txt",
		"...and 1 more",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestStatusStuckWithoutDetailsStillReportsTheCount pins the degraded path: a daemon that could
// not list the ops still reports the number, and adds no dangling "and N more".
func TestStatusStuckWithoutDetailsStillReportsTheCount(t *testing.T) {
	var buf bytes.Buffer
	renderStatus(&buf, ipc.StatusReport{Stuck: 2})
	out := buf.String()
	if !strings.Contains(out, "Retrying:  2 operation(s)") {
		t.Errorf("output missing the count:\n%s", out)
	}
	if strings.Contains(out, "more") {
		t.Errorf("output invented a truncation note with nothing listed:\n%s", out)
	}
}

// TestPausedConnText pins each pause cause to its own remedy, and pins the fallback to
// naming none. A wrong remedy is worse than a missing one: the operator acts on it.
func TestPausedConnText(t *testing.T) {
	cases := []struct {
		reason string
		want   string
		absent string
	}{
		{ipc.PauseSessionExpired, "synosync pair", "upgrade"},
		{ipc.PausePinMismatch, "certificate changed", "session expired"},
		{ipc.PauseClientRejected, "upgrade it", "synosync pair"},
		{"", "paused", "synosync pair"},
		{"something_this_build_does_not_know", "paused", "synosync pair"},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			got := pausedConnText(tc.reason)
			if !strings.Contains(got, tc.want) {
				t.Errorf("pausedConnText(%q) = %q, want it to mention %q", tc.reason, got, tc.want)
			}
			if strings.Contains(got, tc.absent) {
				t.Errorf("pausedConnText(%q) = %q, must not mention %q", tc.reason, got, tc.absent)
			}
		})
	}
}

// TestStatusRendersLastContact: the connection line describes a socket, not an exchange.
// A half-open connection, or a server that accepts and never answers, reads as
// "connected" for as long as it lasts, and on an idle sync - where a sync client spends
// most of its life - nothing else in the report moves. So the contact line is the only
// evidence that anything is working, and it must be absent, not "never", before the
// first reply.
func TestStatusRendersLastContact(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:      "host:6690",
		Connection:  ipc.ConnState{NumWorkers: 2},
		LastContact: time.Now().Add(-90 * time.Second),
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "last reply 1m ago") {
		t.Errorf("stdout = %q, want a contact line reading 1m ago", stdout.String())
	}
}

// TestStatusOmitsLastContactBeforeFirstReply: a daemon three seconds into its bring-up
// has nothing to age, and printing "never" there would read as a fault.
func TestStatusOmitsLastContactBeforeFirstReply(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{Connecting: true, NumWorkers: 2},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "last reply") {
		t.Errorf("stdout = %q, want no contact line before the first reply", stdout.String())
	}
}

// TestHumanAge pins the precision: enough to decide whether the last contact was recent,
// and no more. A negative age - the daemon's clock ahead of ours across a clock
// adjustment - must read as "just now" rather than as a nonsense figure.
func TestHumanAge(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{-5 * time.Second, "just now"},
		{0, "just now"},
		{1500 * time.Millisecond, "just now"},
		{45 * time.Second, "45s ago"},
		{90 * time.Second, "1m ago"},
		{59 * time.Minute, "59m ago"},
		{90 * time.Minute, "1h30m ago"},
		{25 * time.Hour, "25h0m ago"},
	}
	for _, tc := range cases {
		if got := humanAge(tc.d); got != tc.want {
			t.Errorf("humanAge(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// TestStatusRendersFreshContactWithoutAgo pins the reading a healthy daemon produces on
// almost every status call: a reply within the last two seconds. It printed
// "Contact:    just now ago", found on the first end-to-end run against a live stand.
func TestStatusRendersFreshContactWithoutAgo(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:      "host:6690",
		Connection:  ipc.ConnState{NumWorkers: 2},
		LastContact: time.Now(),
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "last reply just now\n") {
		t.Errorf("stdout = %q, want a contact line reading just now, with nothing after it", stdout.String())
	}
}

// TestStatusRendersAnInitialSyncListing: while a first sync walks the server tree nothing is
// pending and nothing has moved, so this line is the only sign the daemon is working.
func TestStatusRendersAnInitialSyncListing(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server:     "host:6690",
		Connection: ipc.ConnState{NumWorkers: 3},
		Listing:    &ipc.ListingProgress{View: "home", Dirs: 1200, Entries: 48000, For: 2 * time.Minute},
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"First sync with host", "Listing:   home: 1,200 folders, 48,000 items found in 2 min (400 items/s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
}

// TestStatusHeadlineAnswersWhetherItIsSynced pins the first line in each state, the one a person
// reads to decide whether to do anything.
func TestStatusHeadlineAnswersWhetherItIsSynced(t *testing.T) {
	cases := []struct {
		r    ipc.StatusReport
		want string
	}{
		{ipc.StatusReport{Server: "nas:6690", LastApplied: time.Now().Add(-3 * time.Minute)}, "Up to date with nas - last change 3m ago"},
		{ipc.StatusReport{Server: "nas:6690", PendingOps: 109973, Recent: &ipc.RecentRate{OpsPerSec: 97, BytesPerSec: 15 << 20}},
			"Syncing with nas - 109,973 items to go, about 19 min at the current pace"},
		{ipc.StatusReport{Server: "nas:6690", PendingOps: 5}, "Syncing with nas - 5 items to go\n"},
		{ipc.StatusReport{Server: "nas:6690", Connection: ipc.ConnState{Reconnecting: true}}, "No connection to nas - reconnecting..."},
		{ipc.StatusReport{Server: "nas:7000"}, "Up to date with nas:7000"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		renderStatus(&buf, c.r)
		if !strings.Contains(buf.String(), c.want) {
			t.Errorf("status = %q, want it to contain %q", buf.String(), c.want)
		}
	}
}

// TestStatusShowsTheCurrentRateAndTheFolder: "Now" is the last minute, not the run's average,
// and the folder line names where the files are.
func TestStatusShowsTheCurrentRateAndTheFolder(t *testing.T) {
	var buf bytes.Buffer
	renderStatus(&buf, ipc.StatusReport{
		Server: "nas:6690", PendingOps: 10, AppliedOps: 59317, TransferredBytes: 110 << 30, MovingFor: 25 * time.Minute,
		Recent:        &ipc.RecentRate{OpsPerSec: 97, BytesPerSec: 15 << 20},
		ExcludedNames: 17,
		Views:         []ipc.ViewStatus{{ID: 1, Name: "home", LocalPath: `C:\Users\me\Synology`, Excluded: []string{"Photos", "Backup"}}},
	})
	out := buf.String()
	for _, want := range []string{
		"Now:       97 items/s, 15.0 MiB/s",
		"This run:  110.0 GiB, 59,317 items in 25 min",
		`Folder:    C:\Users\me\Synology  <->  home`,
		"Left out:  /Photos, /Backup and 17 name rule(s) (synosync exclude list)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "last_sync_id") || strings.Contains(out, "worker") {
		t.Errorf("the everyday view shows internals:\n%s", out)
	}
}

// TestStatusVerboseShowsTheInternals: -v adds what a bug report needs.
func TestStatusVerboseShowsTheInternals(t *testing.T) {
	stubDaemonSocket(t, ipc.StatusReport{
		Server: "drive.example.com:6690", PendingOps: 4, AppliedOps: 10, TransferredBytes: 1 << 20, MovingFor: time.Minute,
		Connection: ipc.ConnState{NumWorkers: 3},
		Views:      []ipc.ViewStatus{{ID: 1, Name: "team", LastSyncID: 42, InitialSyncDone: true, Conflicts: 2}},
	})
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "-v"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"server address:  drive.example.com:6690",
		"connections:     3 worker(s)",
		"queued ops:      4",
		"average rate:",
		"view [1] team:  last_sync_id=42  initial_sync=done  conflicts=2  unresolved=0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status -v lacks %q:\n%s", want, out)
		}
	}
}

// TestGroupDigitsAndHumanDuration pins the number and time renderings.
func TestGroupDigitsAndHumanDuration(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 109973: "109,973", 1234567: "1,234,567"} {
		if got := groupDigits(n); got != want {
			t.Errorf("groupDigits(%d) = %q, want %q", n, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		45 * time.Second: "45 s", 25 * time.Minute: "25 min", 90 * time.Minute: "1 h 30 min", 72 * time.Hour: "3 days",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%v) = %q, want %q", d, got, want)
		}
	}
}
