package main

// The `log` subcommand prints the daemon log file (the rotating writer's current
// synosync.log under the platform log directory). With -f/--follow it tails the
// file, polling for appended data and re-reading from the start when the file is
// rotated or truncated underneath it. With -n N it prints only the last N lines.
// A missing log file (the daemon has never run) yields a friendly hint and a
// non-zero exit rather than a raw open error. Side effects are routed through the
// shared resolveDaemonPaths injection (the log path) and the logFollowContext
// injection (the follow loop's termination), so the command is unit-tested
// without writing into a real log directory or looping forever.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// noLogHint is printed when the log file does not exist yet. An absent file does
// NOT prove the daemon has never run: `daemon --log-stderr` (what the shipped
// systemd unit passes) writes no file at all and hands its output to the
// supervisor's journal instead, so the hint names both explanations rather than
// asserting the one it cannot verify.
const noLogHint = "synosync: no log file at %s\n" +
	"synosync: either the daemon has not run (start it with `synosync daemon`), or it runs with `--log-stderr`\n" +
	"synosync: (the shipped systemd unit does) and logs to the supervisor's journal - try `journalctl --user -u synosync`"

// Injection points overridden by `log` tests. In production logFollowContext is
// cancelled on SIGINT/SIGTERM so a follow loop ends on Ctrl-C; tests inject an
// already-cancelled context so follow prints the existing tail and returns
// without looping. logFollowPollInterval is the wait between follow reads.
var (
	logFollowContext = func() (context.Context, context.CancelFunc) {
		return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	}
	logFollowPollInterval = 500 * time.Millisecond
)

// logHelp is the `log` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var logHelp = helpSpec{
	name:    "log",
	summary: "show daemon logs",
	usage:   []string{"synosync log [flags]"},
	describe: `Prints the daemon's rotating log file, which lives in the platform log
directory and rolls over past roughly 10 MiB.

A missing file does not prove the daemon never ran: a daemon started with
--log-stderr writes to its supervisor's journal instead, which is what the
shipped systemd unit does. When the file is absent this command names the path
it looked for and both explanations.`,
	examples: []string{
		"synosync log",
		"synosync log -n 100        # the last 100 lines (--lines also works)",
		"synosync log -n 100 -f     # the last 100 lines, then follow",
		"journalctl --user -u synosync    # when the daemon runs with --log-stderr",
	},
}

// cmdLog implements the `log` subcommand.
func cmdLog(args []string, stdout, stderr io.Writer) int {
	fset := flag.NewFlagSet("log", flag.ContinueOnError)
	var follow bool
	// -f and --follow share one variable AND one usage string; collectFlags folds
	// them into a single "-f, --follow" row on that basis, so the two spellings
	// must stay byte-identical here.
	fset.BoolVar(&follow, "f", false, "follow the log file, printing new lines as they are appended")
	fset.BoolVar(&follow, "follow", false, "follow the log file, printing new lines as they are appended")
	var nLines int
	fset.IntVar(&nLines, "n", 0, "print only the last `N` lines (0 = all)")
	fset.IntVar(&nLines, "lines", 0, "print only the last `N` lines (0 = all)")
	if code, ok := parseFlags(fset, logHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(logHelp, fset.Args(), stderr); !ok {
		return code
	}

	paths, err := resolveDaemonPaths()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	path := paths.LogFile()

	if follow {
		return followLog(path, nLines, stdout, stderr)
	}
	return printLog(path, nLines, stdout, stderr)
}

// printLog writes the log file to stdout, optionally trimmed to its last nLines
// lines. A missing file yields the friendly hint and a non-zero exit.
func printLog(path string, nLines int, stdout, stderr io.Writer) int {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(stderr, noLogHint+"\n", path)
			return 1
		}
		fmt.Fprintf(stderr, "synosync: read log: %s\n", userErr(err))
		return 1
	}
	if nLines > 0 {
		data = lastLines(data, nLines)
	}
	_, _ = stdout.Write(data)
	return 0
}

// followLog prints the initial tail (all, or the last nLines lines) and then
// polls the file for appended data until the follow context is cancelled
// (SIGINT/SIGTERM in production). It tolerates rotation/truncation by restarting
// from the start of the file when it shrinks below the read offset.
func followLog(path string, nLines int, stdout, stderr io.Writer) int {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(stderr, noLogHint+"\n", path)
			return 1
		}
		fmt.Fprintf(stderr, "synosync: read log: %s\n", userErr(err))
		return 1
	}
	initial := data
	if nLines > 0 {
		initial = lastLines(data, nLines)
	}
	_, _ = stdout.Write(initial)
	offset := int64(len(data))

	ctx, cancel := logFollowContext()
	defer cancel()

	ticker := time.NewTicker(logFollowPollInterval)
	defer ticker.Stop()

	for {
		offset, err = tailFrom(path, offset, stdout)
		if err != nil {
			fmt.Fprintf(stderr, "synosync: read log: %s\n", userErr(err))
			return 1
		}
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
		}
	}
}

// tailFrom copies any bytes of path beyond offset to w and returns the new
// offset. A file that shrank below offset (rotated/truncated) is re-read from the
// start; a momentarily-missing file (rotated away) is a no-op for this tick.
func tailFrom(path string, offset int64, w io.Writer) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return offset, nil
		}
		return offset, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return offset, err
	}
	if info.Size() < offset {
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}
	n, err := io.Copy(w, f)
	if err != nil {
		return offset, err
	}
	return offset + n, nil
}

// lastLines returns the last n lines of data, preserving its trailing newline (if
// any). It returns data unchanged when it holds n lines or fewer.
func lastLines(data []byte, n int) []byte {
	if n <= 0 || len(data) == 0 {
		return data
	}
	// Ignore a single trailing newline when locating line boundaries so a
	// newline-terminated file is not counted as an extra empty line.
	searchEnd := len(data)
	if data[searchEnd-1] == '\n' {
		searchEnd--
	}
	count := 0
	for i := searchEnd - 1; i >= 0; i-- {
		if data[i] == '\n' {
			count++
			if count == n {
				return data[i+1:]
			}
		}
	}
	return data
}
