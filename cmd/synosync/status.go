package main

// The `status` subcommand is the read-only control verb: it dials the running
// daemon's local IPC socket, asks for the live StatusReport, and renders it to
// stdout. When the daemon is not running (the socket is missing or refuses the
// connection) it prints a friendly hint and exits non-zero rather than a raw
// dial error.

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mfilippov/synosync/internal/ipc"
)

// statusHelp is the `status` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var statusHelp = helpSpec{
	name:    "status",
	summary: "show sync status",
	usage:   []string{"synosync status [-v] [--json]"},
	describe: `Asks the running daemon how the sync is going. The first line says whether
everything is up to date, or how many items are left and roughly how long they
will take, or that the first sync is still listing the server, or that syncing
is paused or not connected.

The lines under it: the rate over the last minute, what this run has moved,
the local folder of each view, what is left out, and the server connection.
Conflicts, unresolved changes, repeatedly failing operations (named, with the
reason for each), skipped items and a full disk appear only when there are
any. A quiet report means there is nothing to act on.

-v adds the internals a bug report needs: connections, the queued-operation
count, average rates and each view's sync position.

Needs a running daemon. Without one it says so and exits 1.`,
	examples: []string{
		"synosync status",
		"synosync status -v       the same plus connections, queue and sync ids",
		"synosync status --json    machine-readable, same fields as the daemon's reply",
	},
}

// cmdStatus implements the `status` subcommand.
func cmdStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	asJSON := addJSONFlag(fs)
	verbose := fs.Bool("v", false, "also print the internals: connections, queued operations, average rates, per-view sync ids")
	if code, ok := parseFlags(fs, statusHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(statusHelp, fs.Args(), stderr); !ok {
		return code
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var report ipc.StatusReport
	if err := client.Call("status", nil, &report); err != nil {
		fmt.Fprintf(stderr, "synosync: status: %s\n", userErr(err))
		return 1
	}

	if *asJSON {
		return writeJSON(stdout, stderr, "status", report)
	}
	if *verbose {
		renderStatusVerbose(stdout, report)
	} else {
		renderStatus(stdout, report)
	}
	return 0
}

// renderStatus writes the report for a person: the first line answers "is everything synced?",
// the lines under it are what can be acted on.
func renderStatus(w io.Writer, r ipc.StatusReport) { renderStatusOpts(w, r, false) }

// renderStatusVerbose is renderStatus plus the Details block (status -v): the internals a bug
// report or a curious operator wants, kept out of the everyday view.
func renderStatusVerbose(w io.Writer, r ipc.StatusReport) { renderStatusOpts(w, r, true) }

func renderStatusOpts(w io.Writer, r ipc.StatusReport, verbose bool) {
	host := strings.TrimSuffix(r.Server, ":6690")
	fmt.Fprintln(w, headline(r, host))

	line := func(label, format string, a ...any) {
		fmt.Fprintf(w, "  %-11s%s\n", label+":", fmt.Sprintf(format, a...))
	}
	if r.Recent != nil && r.Recent.OpsPerSec > 0 {
		line("Now", "%s items/s, %s/s", humanRate(r.Recent.OpsPerSec), humanBytes(int64(r.Recent.BytesPerSec)))
	}
	// Only once something has moved: "0 B in 0 items" on a daemon that has not started
	// transferring says less than no line at all.
	if r.AppliedOps > 0 {
		s := fmt.Sprintf("%s, %s items", humanBytes(r.TransferredBytes), groupDigits(r.AppliedOps))
		if r.MovingFor > time.Second {
			s += " in " + humanDuration(r.MovingFor)
		}
		line("This run", "%s", s)
	}
	// A first sync lists the whole server tree; on a large view that is minutes, and how far
	// it has got is the one figure that says how much of the tree the client knows about yet.
	if l := r.Listing; l != nil {
		s := fmt.Sprintf("%s: %s folders, %s items found", l.View, groupDigits(l.Dirs), groupDigits(l.Entries))
		if l.For > time.Second {
			s += fmt.Sprintf(" in %s (%s items/s)", humanDuration(l.For), humanRate(float64(l.Entries)/l.For.Seconds()))
		}
		line("Listing", "%s", s)
	}
	if len(r.Views) == 0 {
		line("Folder", "none - no view is selected to sync")
	}
	multi := len(r.Views) > 1
	var left []string
	conflicts, unresolved := 0, 0
	for _, v := range r.Views {
		if v.LocalPath != "" {
			line("Folder", "%s  <->  %s", v.LocalPath, v.Name)
		} else {
			line("Folder", "%s", v.Name)
		}
		for _, p := range v.Excluded {
			if multi {
				left = append(left, v.Name+":/"+p)
			} else {
				left = append(left, "/"+p)
			}
		}
		for _, p := range v.Rescanning {
			line("Fetching", "/%s in %s (brought back into the sync; walking it on the server)", p, v.Name)
		}
		conflicts += v.Conflicts
		unresolved += v.Unresolved
	}
	if len(left) > 0 || r.ExcludedNames > 0 {
		s := strings.Join(left, ", ")
		if r.ExcludedNames > 0 {
			if s != "" {
				s += " and "
			}
			s += fmt.Sprintf("%d name rule(s)", r.ExcludedNames)
		}
		line("Left out", "%s (synosync exclude list)", s)
	}
	if conflicts > 0 {
		line("Conflicts", "%d file(s) kept as _Conflict copies (synosync conflicts)", conflicts)
	}
	if unresolved > 0 {
		line("Unresolved", "%d change(s) could not be reconciled automatically (synosync log)", unresolved)
	}
	if r.DeadLettered > 0 {
		line("Skipped", "%d item(s) the client gave up on (synosync drops)", r.DeadLettered)
	}
	if r.Stuck > 0 {
		line("Retrying", "%d operation(s) failing repeatedly, still retrying", r.Stuck)
		// Naming them is the difference between a number and something to act on. The daemon
		// caps the list, so say when there is more than what is shown.
		for _, s := range r.StuckOps {
			fmt.Fprintf(w, "    %s  %s\n", describeStuck(s), s.Path)
			if s.NewPath != "" {
				fmt.Fprintf(w, "      moving to: %s\n", s.NewPath)
			}
			if s.Reason != "" {
				fmt.Fprintf(w, "      reason:    %s\n", s.Reason)
			}
		}
		if n := r.Stuck - len(r.StuckOps); n > 0 && len(r.StuckOps) > 0 {
			fmt.Fprintf(w, "    ...and %d more\n", n)
		}
	}
	if r.DiskFull {
		line("Disk", "full - transfers are retrying; free some space to resume")
	}
	server := connText(r.Connection)
	// Only once the server has actually answered: before that there is nothing to age, and
	// "never" on a daemon three seconds into its bring-up would read as a fault.
	if !r.LastContact.IsZero() {
		server += ", last reply " + humanAge(time.Since(r.LastContact))
	}
	line("Server", "%s", server)

	if !verbose {
		return
	}
	fmt.Fprintln(w, "Details:")
	fmt.Fprintf(w, "  server address:  %s\n", r.Server)
	fmt.Fprintf(w, "  connections:     %d worker(s)\n", r.Connection.NumWorkers)
	fmt.Fprintf(w, "  queued ops:      %d\n", r.PendingOps)
	if r.AppliedOps > 0 && r.MovingFor > time.Second {
		fmt.Fprintf(w, "  average rate:    %s/s, %s op/s over %s\n",
			humanBytes(int64(float64(r.TransferredBytes)/r.MovingFor.Seconds())),
			humanRate(float64(r.AppliedOps)/r.MovingFor.Seconds()), r.MovingFor.Round(time.Second))
	}
	for _, v := range r.Views {
		initial := "pending"
		if v.InitialSyncDone {
			initial = "done"
		}
		// A stale count is marked rather than hidden: the numbers are the last ones the
		// daemon managed to compute (or zero), so presenting them as current would be a
		// quiet lie on exactly the stores whose count scan cannot keep up.
		stale := ""
		if v.CountsStale {
			stale = "  (counts not refreshed - see the daemon log)"
		}
		fmt.Fprintf(w, "  view [%d] %s:  last_sync_id=%d  initial_sync=%s  conflicts=%d  unresolved=%d%s\n",
			v.ID, v.Name, v.LastSyncID, initial, v.Conflicts, v.Unresolved, stale)
	}
}

// headline is status's first line: whether everything is synced, and if not, what is going on.
func headline(r ipc.StatusReport, host string) string {
	switch {
	case r.Paused:
		return "Paused - nothing syncs until you run: synosync resume (" + host + ")"
	case r.Connection.Connecting || r.Connection.Reconnecting || r.Connection.Paused:
		return "No connection to " + host + " - " + connText(r.Connection)
	case r.Listing != nil:
		s := "First sync with " + host + " - listing the server, transferring as it goes"
		if r.PendingOps > 0 {
			s += fmt.Sprintf("; %s items queued", groupDigits(int64(r.PendingOps)))
		}
		return s
	case r.PendingOps > 0:
		s := fmt.Sprintf("Syncing with %s - %s items to go", host, groupDigits(int64(r.PendingOps)))
		if r.Recent != nil && r.Recent.OpsPerSec > 0 {
			eta := time.Duration(float64(r.PendingOps) / r.Recent.OpsPerSec * float64(time.Second))
			s += ", about " + humanDuration(eta) + " at the current pace"
		}
		return s
	default:
		s := "Up to date with " + host
		if !r.LastApplied.IsZero() {
			s += " - last change " + humanAge(time.Since(r.LastApplied))
		}
		return s
	}
}

// connText words the connection state.
func connText(c ipc.ConnState) string {
	switch {
	case c.Connecting:
		// The pool is not up yet (bring-up/backoff, e.g. the server is unreachable).
		return "connecting..."
	case c.Paused:
		return pausedConnText(c.PauseReason)
	case c.Reconnecting:
		// The pool is up but a dropped connection is being re-dialed under backoff.
		return "reconnecting..."
	default:
		return "connected"
	}
}

// groupDigits renders a count with thousands separators: 109973 reads as 109,973.
func groupDigits(n int64) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return s
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// humanDuration renders a length of time for a person: seconds, minutes, then hours and minutes.
func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%d s", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%d min", int(d.Minutes()+0.5))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h %d min", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%d days", int(d.Hours()/24+0.5))
	}
}

// humanBytes renders a byte count for a human reading a status line: three significant
// figures at most, and the unit that keeps it there. Binary units, because that is what
// every other size an operator compares this against uses.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// humanRate renders an operations-per-second figure without rounding a slow sync to zero.
//
// That rounding is not cosmetic here: this line exists so an operator can tell a slow sync
// from a stuck one, and "0 op/s" on a daemon steadily applying an op every few seconds says
// exactly the wrong thing. Sub-unit rates keep a decimal (or two); anything from 10 upwards
// is read as a magnitude and does not need one.
func humanRate(r float64) string {
	switch {
	case r >= 10:
		return fmt.Sprintf("%.0f", r)
	case r >= 1:
		return fmt.Sprintf("%.1f", r)
	case r >= 0.01:
		return fmt.Sprintf("%.2f", r)
	case r > 0:
		return "<0.01"
	default:
		return "0"
	}
}

// describeStuck names a stuck operation the same way `drops` names a dropped one, so the two
// listings read alike: they answer the same question about the same pipeline, one for work
// still being retried and one for work given up on.
func describeStuck(s ipc.StuckEntry) string {
	return describeOp(s.Op, s.Direction)
}

// pausedConnText words the pool's pause for the user. The daemon classifies the cause
// (it holds the sentinels) and this decides what to say about it, because the three
// terminal causes need three different actions and the line used to name only one of
// them: "re-login needed" is wrong advice for a changed server certificate, and worse
// than useless for a rejected client version, where re-pairing cannot help at all.
//
// The fallback deliberately names no fix. A pause this build cannot classify is still
// worth reporting, but guessing at the remedy is how the old line got it wrong.
func pausedConnText(reason string) string {
	switch reason {
	case ipc.PauseSessionExpired:
		return `paused (session expired; run "synosync pair" to re-authenticate)`
	case ipc.PausePinMismatch:
		return `paused (the server's certificate changed; verify it is expected, then run "synosync pair")`
	case ipc.PauseClientRejected:
		return "paused (the server rejected this version of synosync; upgrade it)"
	default:
		return "paused"
	}
}

// humanAge renders how long ago something happened, for a reader deciding whether it is
// recent enough. Precision that does not change that decision is dropped: seconds while
// the answer is "just now", then minutes, then hours. A negative age (the daemon's clock
// ahead of ours, which a status call across a clock adjustment can produce) reads as
// "just now" rather than as a nonsense figure.
//
// It returns the whole phrase, "ago" included, because "just now" takes none: a caller
// appending " ago" printed "just now ago" on every status of a healthy daemon.
func humanAge(d time.Duration) string {
	switch {
	case d < 2*time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh%dm ago", int(d.Hours()), int(d.Minutes())%60)
	}
}
