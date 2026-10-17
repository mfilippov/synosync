package main

// The `resume` subcommand resumes a paused daemon: it dials the local IPC socket
// and calls the `resume` control method, which wakes the gated sync loop so the
// next cycle runs immediately.

import (
	"flag"
	"fmt"
	"io"

	"github.com/mfilippov/synosync/internal/ipc"
)

// resumeHelp is the `resume` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var resumeHelp = helpSpec{
	name:    "resume",
	summary: "resume synchronization",
	usage:   []string{"synosync resume"},
	describe: `Clears a pause set by ` + "`" + `synosync pause` + "`" + ` and lets the daemon sync again. Resuming
an already-running daemon is harmless.`,
	examples: []string{
		"synosync resume",
	},
}

// cmdResume implements the `resume` subcommand.
func cmdResume(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	if code, ok := parseFlags(fs, resumeHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(resumeHelp, fs.Args(), stderr); !ok {
		return code
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var ack ipc.ControlAck
	if err := client.Call("resume", nil, &ack); err != nil {
		fmt.Fprintf(stderr, "synosync: resume: %s\n", userErr(err))
		return 1
	}

	fmt.Fprintln(stdout, "sync resumed")
	return 0
}
