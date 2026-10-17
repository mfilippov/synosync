package main

// The `sync` subcommand triggers an immediate sync cycle on the running daemon:
// it dials the local IPC socket and calls the `sync` control method, which
// interrupts an in-flight long-poll and forces the next local scan. An optional
// `now` argument is accepted for readability (`synosync sync now`) and behaves
// identically to the bare verb.

import (
	"flag"
	"fmt"
	"io"

	"github.com/mfilippov/synosync/internal/ipc"
)

// syncHelp is the `sync` help page.
var syncHelp = helpSpec{
	name:    "sync",
	summary: "trigger an immediate sync cycle",
	usage:   []string{"synosync sync [now]"},
	describe: `Tells the running daemon to start a sync pass immediately instead of waiting
for the next scheduled one. The word ` + "`now`" + ` is accepted as a no-op, so both
spellings read naturally.

The command returns as soon as the daemon accepts the request; it does not wait
for the pass to finish. Watch ` + "`synosync status`" + ` or the log for progress.

Needs a running daemon.`,
	examples: []string{
		"synosync sync",
		"synosync sync now",
	},
}

// cmdSync implements the `sync` subcommand.
func cmdSync(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	if code, ok := parseFlags(fs, syncHelp, args, stdout, stderr); !ok {
		return code
	}
	rest := fs.Args()
	if len(rest) > 1 || (len(rest) == 1 && rest[0] != "now") {
		// `sync`'s positional word is OPTIONAL rather than a required action, so
		// rejectExtraArgs does not fit - its message would say the command takes
		// no positional arguments. (`config` and `service` also route on a
		// positional, but theirs is mandatory and each owns its own message.)
		fmt.Fprintf(stderr, "synosync: sync accepts only the optional word \"now\", got %s\n", quoteArgs(rest))
		writeUsageHint(stderr, syncHelp)
		return 2
	}

	client, err := dialDaemon(stderr)
	if err != nil {
		return 1
	}
	defer func() { _ = client.Close() }()

	var ack ipc.ControlAck
	if err := client.Call("sync", nil, &ack); err != nil {
		fmt.Fprintf(stderr, "synosync: sync: %s\n", userErr(err))
		return 1
	}

	fmt.Fprintln(stdout, "sync triggered")
	return 0
}
