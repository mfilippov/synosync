package main

// The `drops` subcommand lists work the engine gave up on, with the reason it recorded.
//
// `status` has always shown a count and pointed at the log. The engine records a precise
// reason for every drop and the store has kept it since schema v6 - this is what puts it in
// front of a person without grepping a rotating log file.

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mfilippov/synosync/internal/ipc"
)

// dropsHelp is the `drops` help page.
var dropsHelp = helpSpec{
	name:    "drops",
	summary: "list work the engine gave up on, and why",
	usage:   []string{"synosync drops [--json]"},
	describe: `Lists the operations synosync stopped retrying, newest first, with the reason
recorded for each. Each line names the view, the path, what was being done and
why it was abandoned.

Most entries mean the work cannot succeed as it stands: a name the local
filesystem cannot represent, a path blocked by something else, or a change the
server refused the same way for long enough to prove it will keep refusing.
Nothing here is retried on its own, and nothing is deleted from either side
because of it - the two copies simply stop converging on that path until the
cause is removed. Renaming the file, on whichever side is at fault, is usually
what clears it.

Needs a running daemon.`,
	examples: []string{
		"synosync drops",
		"synosync drops --json    machine-readable, same fields as the daemon's reply",
	},
}

// cmdDrops implements the `drops` subcommand.
func cmdDrops(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("drops", flag.ContinueOnError)
	asJSON := addJSONFlag(fs)
	if code, ok := parseFlags(fs, dropsHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(dropsHelp, fs.Args(), stderr); !ok {
		return code
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var report ipc.DropReport
	if err := client.Call("drops", nil, &report); err != nil {
		fmt.Fprintf(stderr, "synosync: drops: %s\n", userErr(err))
		return 1
	}

	if *asJSON {
		return writeJSON(stdout, stderr, "drops", report)
	}
	renderDrops(stdout, report)
	return 0
}

// renderDrops writes a human-readable rendering of the daemon's DropReport.
func renderDrops(w io.Writer, r ipc.DropReport) {
	if len(r.Drops) == 0 {
		fmt.Fprintln(w, "Nothing dropped.")
		return
	}
	if r.Truncated {
		fmt.Fprintf(w, "%d dropped, showing the newest %d:\n", r.Total, len(r.Drops))
	} else {
		fmt.Fprintf(w, "%d dropped:\n", len(r.Drops))
	}
	for _, d := range r.Drops {
		name := d.ViewName
		if name == "" {
			name = "(unknown view)"
		}
		fmt.Fprintf(w, "  [%d] %s\n", d.ViewID, name)
		// A fold-collision overflow record stands for a set of names rather than one path,
		// so it carries none; saying so beats printing an empty field.
		if d.Path == "" {
			fmt.Fprintf(w, "      path:     (several names at once)\n")
		} else {
			fmt.Fprintf(w, "      path:     %s\n", d.Path)
		}
		if d.NewPath != "" {
			fmt.Fprintf(w, "      moving to: %s\n", d.NewPath)
		}
		fmt.Fprintf(w, "      action:   %s\n", describeDrop(d))
		fmt.Fprintf(w, "      reason:   %s\n", d.Reason)
		fmt.Fprintf(w, "      recorded: %s\n", time.Unix(d.RecordedAt, 0).Format(time.RFC3339))
	}
}

// opVerbs renders an op type in the user's terms rather than the engine's, so a reader who has
// never seen the queue's vocabulary can still tell what was being attempted.
var opVerbs = map[string]string{
	"download":             "download from the server",
	"upload":               "upload to the server",
	"mkdir":                "create a folder",
	"rename":               "move",
	"remove":               "delete",
	"resolve_conflict":     "resolve a conflict",
	"fold_collision":       "two names that the filesystem cannot tell apart",
	"unrepresentable_name": "a server name this computer's filesystem cannot hold",
}

// describeOp names the operation and which copy of the user's files it applies to.
func describeOp(op, direction string) string {
	verb := opVerbs[op]
	if verb == "" {
		verb = op
	}
	switch direction {
	case "inbound":
		return verb + " (on this device)"
	case "outbound":
		return verb + " (on the server)"
	default:
		return verb
	}
}

// describeDrop names a dropped operation.
func describeDrop(d ipc.DropEntry) string { return describeOp(d.Op, d.Direction) }
