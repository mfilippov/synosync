package main

// The `pause` subcommand pauses synchronization on the running daemon: it dials
// the local IPC socket and calls the `pause` control method, which gates the sync
// loop at the next cycle boundary (an in-flight long-poll finishes first). The
// daemon persists the pause, so it survives a daemon restart
// until an explicit `synosync resume`.

import (
	"flag"
	"fmt"
	"io"

	"github.com/mfilippov/synosync/internal/ipc"
)

// pauseHelp is the `pause` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var pauseHelp = helpSpec{
	name:    "pause",
	summary: "pause synchronization",
	usage:   []string{"synosync pause"},
	describe: `Stops the daemon from starting new sync work. The pause is durable: it is
recorded in state.db and survives a daemon restart until ` + "`" + `synosync resume` + "`" + `.

The daemon stays connected and keeps serving status, so a paused client still
reports what it would do.`,
	examples: []string{
		"synosync pause",
	},
}

// cmdPause implements the `pause` subcommand.
func cmdPause(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pause", flag.ContinueOnError)
	if code, ok := parseFlags(fs, pauseHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(pauseHelp, fs.Args(), stderr); !ok {
		return code
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var ack ipc.ControlAck
	if err := client.Call("pause", nil, &ack); err != nil {
		fmt.Fprintf(stderr, "synosync: pause: %s\n", userErr(err))
		return 1
	}

	fmt.Fprintln(stdout, "sync paused")
	return 0
}
