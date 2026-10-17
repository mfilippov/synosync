// Command synosync is the cross-platform two-way Synology Drive sync client.
//
// Subcommands are routed through a small registry (commands).
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
)

// version is the build version of the binary. It is overridable at build time
// via -ldflags "-X main.version=...".
var version = "0.0.0-dev"

// buildVersion is version, or - when the release build did not stamp it - the module version
// the Go toolchain recorded, so `go install ...@v0.1.0` reports 0.1.0 rather than 0.0.0-dev. A
// build from a git checkout records a pseudo-version naming the commit (with +dirty for local
// changes), which is more use in a bug report than the default; one without VCS information
// records "(devel)" and keeps the default.
func buildVersion() string {
	return resolveVersion(version, debug.ReadBuildInfo)
}

func resolveVersion(stamped string, read func() (*debug.BuildInfo, bool)) string {
	if stamped != "0.0.0-dev" {
		return stamped
	}
	if bi, ok := read(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return strings.TrimPrefix(bi.Main.Version, "v")
	}
	return stamped
}

// command describes a single CLI subcommand.
type command struct {
	name    string
	summary string
	// run executes the subcommand. It returns a process exit code.
	run func(args []string, stdout, stderr io.Writer) int
}

// commands is the registry of known subcommands, in definition order.
func commands() []command {
	return []command{
		{name: "version", summary: "print the synosync version", run: cmdVersion},
		{name: "daemon", summary: "run the sync daemon in the foreground", run: cmdDaemon},
		{name: "service", summary: "install the daemon under the system service manager", run: cmdService},
		{name: "pair", summary: "pair with a Synology Drive server", run: cmdPair},
		{name: "login", summary: "re-authenticate with a paired server", run: cmdLogin},
		{name: "status", summary: "show sync status", run: cmdStatus},
		{name: "sync", summary: "trigger an immediate sync cycle", run: cmdSync},
		{name: "pause", summary: "pause synchronization", run: cmdPause},
		{name: "resume", summary: "resume synchronization", run: cmdResume},
		{name: "conflicts", summary: "list keep-both conflict copies", run: cmdConflicts},
		{name: "drops", summary: "list work the engine gave up on, and why", run: cmdDrops},
		{name: "exclude", summary: "choose server folders that are not synced", run: cmdExclude},
		{name: "config", summary: "get or set configuration values", run: cmdConfig},
		{name: "log", summary: "show daemon logs", run: cmdLog},
		{name: "unlink", summary: "unlink the device and clear credentials", run: cmdUnlink},
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand. It is the testable entrypoint: it takes the
// argument list (without the program name) and explicit writers, and returns
// the process exit code instead of calling os.Exit.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}

	name := args[0]
	switch name {
	case "--version", "-V":
		// Every other tool accepts this; here it used to be indistinguishable from a typo,
		// dumping the whole usage page to stderr with exit 2. It shares printVersion with the
		// subcommand so the version has one rendering, and rejects trailing arguments the same
		// way rather than silently ignoring them.
		if code, ok := rejectExtraArgs(versionHelp, args[1:], stderr); !ok {
			return code
		}
		printVersion(stdout)
		return 0
	case "-h", "--help", "help":
		// `synosync help <cmd>` is the same page as `synosync <cmd> --help`,
		// because it IS that page: helpFor delegates to the subcommand rather
		// than rendering a second copy that could drift from it.
		if len(args) > 1 && !isHelpArg(args[1]) {
			if len(args) > 2 {
				fmt.Fprintf(stderr, "synosync: help takes one subcommand, got %s\n", quoteArgs(args[1:]))
				return 2
			}
			return helpFor(args[1], stdout, stderr)
		}
		usage(stdout)
		return 0
	}

	for _, c := range commands() {
		if c.name == name {
			return c.run(args[1:], stdout, stderr)
		}
	}

	fmt.Fprintf(stderr, "synosync: unknown subcommand %q\n", name)
	usage(stderr)
	return 2
}

// versionHelp is the `version` help page. It parses no FlagSet, so writeHelp is
// called with a nil one and the Flags block is omitted.
var versionHelp = helpSpec{
	name:    "version",
	summary: "print the synosync version",
	usage:   []string{"synosync version"},
	describe: `Prints the version this binary was built from, then exits. A release build
carries its tag; a build from an untagged commit carries that commit's short
hash instead. The Go toolchain and the OS/architecture follow in parentheses;
quote the whole line in a bug report.`,
	examples: []string{"synosync version"},
}

// cmdVersion implements the `version` subcommand. It takes no flags or
// positional arguments, so anything trailing is a usage error rather than
// silently ignored - except a help request, which every
// other subcommand answers with exit 0 through flag.ErrHelp (clihelp.go).
// `version` parses no FlagSet, so it honours -h/--help itself rather than being
// the one command where asking for help is an error.
func cmdVersion(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && isHelpArg(args[0]) {
		writeHelp(stdout, versionHelp, nil)
		return 0
	}
	if code, ok := rejectExtraArgs(versionHelp, args, stderr); !ok {
		return code
	}
	printVersion(stdout)
	return 0
}

// printVersion is the single rendering of the version string, shared by the `version`
// subcommand and the `--version` alias.
func printVersion(stdout io.Writer) {
	fmt.Fprintln(stdout, versionLine())
}

// helpFor renders one subcommand's help page by asking that subcommand for it,
// so `synosync help pair` and `synosync pair --help` are the same bytes from the
// same code path.
func helpFor(name string, stdout, stderr io.Writer) int {
	for _, c := range commands() {
		if c.name == name {
			return c.run([]string{"--help"}, stdout, stderr)
		}
	}
	fmt.Fprintf(stderr, "synosync: unknown subcommand %q\n", name)
	usage(stderr)
	return 2
}

// usage prints a short help message listing the available subcommands.
func usage(w io.Writer) {
	var b strings.Builder
	b.WriteString("synosync - two-way Synology Drive sync client\n\n")
	// The subcommand list below is alphabetical, which puts `config` first and
	// `pair` sixth - no help at all to somebody running this for the first time,
	// who must pair before anything else works. State the order once, up front.
	b.WriteString("Getting started:\n")
	b.WriteString("  1. synosync pair --host <host> --user <name> --sync-root <dir>\n")
	// Not an unconditional promise: on Windows `service install` prints the
	// elevated command to run rather than registering anything, because a
	// service there needs a run-as account this CLI cannot safely collect.
	b.WriteString("  2. synosync service install    sync in the background (Windows: prints the command to run)\n")
	b.WriteString("  3. synosync status    check that it connected\n\n")
	b.WriteString("Usage:\n  synosync <subcommand> [flags]\n\n")
	b.WriteString("Subcommands:\n")

	cmds := commands()
	sort.Slice(cmds, func(i, j int) bool { return cmds[i].name < cmds[j].name })
	width := 0
	for _, c := range cmds {
		if len(c.name) > width {
			width = len(c.name)
		}
	}
	for _, c := range cmds {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, c.name, c.summary)
	}
	b.WriteString("\nRun `synosync <subcommand> --help` for that subcommand's flags and\n")
	b.WriteString("examples; `synosync help <subcommand>` prints the same page.\n")
	_, _ = io.WriteString(w, b.String())
}

// versionLine is the version string with the toolchain and platform it was built for. A
// bug report quotes this line, and the version alone does not say which of the six
// release binaries was running, nor which Go runtime the TLS stack came from.
func versionLine() string {
	return fmt.Sprintf("synosync %s (%s, %s/%s)", buildVersion(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
