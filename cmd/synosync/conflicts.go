package main

// The `conflicts` subcommand lists the outstanding keep-both conflict copies the
// daemon has recorded: it dials the local IPC socket, asks for the ConflictReport,
// and renders one line per conflict (view, the canonical path the server won, the
// preserved local copy, and when it was detected). When there are none it prints a
// reassuring "no conflicts" line.

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mfilippov/synosync/internal/ipc"
)

// conflictsHelp is the `conflicts` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var conflictsHelp = helpSpec{
	name:    "conflicts",
	summary: "list keep-both conflict copies",
	usage:   []string{"synosync conflicts [--json]"},
	describe: `Lists the keep-both copies the engine created when it could not decide between
two versions of a file. Each line names the view, the canonical path, the copy
path and when it was detected.

Nothing here is deleted automatically. Resolve a conflict by keeping whichever
file you want under the canonical name and removing the _Conflict copy
yourself.

Needs a running daemon.`,
	examples: []string{
		"synosync conflicts",
		"synosync conflicts --json    machine-readable, same fields as the daemon's reply",
	},
}

// cmdConflicts implements the `conflicts` subcommand.
func cmdConflicts(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("conflicts", flag.ContinueOnError)
	asJSON := addJSONFlag(fs)
	if code, ok := parseFlags(fs, conflictsHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(conflictsHelp, fs.Args(), stderr); !ok {
		return code
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var report ipc.ConflictReport
	if err := client.Call("conflicts", nil, &report); err != nil {
		fmt.Fprintf(stderr, "synosync: conflicts: %s\n", userErr(err))
		return 1
	}

	if *asJSON {
		return writeJSON(stdout, stderr, "conflicts", report)
	}
	renderConflicts(stdout, report)
	return 0
}

// renderConflicts writes a human-readable rendering of the daemon's ConflictReport.
// A truncated report (the daemon caps the list so it fits the IPC reply limit)
// says how many of the total are shown; the listed entries are the newest.
func renderConflicts(w io.Writer, r ipc.ConflictReport) {
	if len(r.Conflicts) == 0 {
		if r.Truncated && r.Total > 0 {
			fmt.Fprintf(w, "%d conflict(s) (report too large to list).\n", r.Total)
			return
		}
		fmt.Fprintln(w, "No conflicts.")
		return
	}
	if r.Truncated {
		fmt.Fprintf(w, "%d conflict(s), showing the newest %d:\n", r.Total, len(r.Conflicts))
	} else {
		fmt.Fprintf(w, "%d conflict(s):\n", len(r.Conflicts))
	}
	for _, c := range r.Conflicts {
		detected := time.Unix(c.DetectedAt, 0).Format(time.RFC3339)
		fmt.Fprintf(w, "  [%d] %s\n", c.ViewID, c.ViewName)
		fmt.Fprintf(w, "      path:     %s\n", c.CanonicalPath)
		fmt.Fprintf(w, "      copy:     %s\n", c.CopyPath)
		fmt.Fprintf(w, "      detected: %s\n", detected)
	}
}
