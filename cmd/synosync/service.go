package main

// The `service` subcommand installs, removes and reports the daemon under the
// host's service manager. The work lives in internal/service; this file is the
// CLI surface: route the action, print the report, choose the exit code.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mfilippov/synosync/internal/service"
)

// Injection seams so the subcommand is driven end-to-end in tests without a
// service manager, a real home directory, or an installed unit.
var (
	serviceDeps      = service.DefaultDeps
	serviceInstall   = service.Install
	serviceUninstall = service.Uninstall
	serviceStatus    = service.Status
	serviceStart     = service.Start
	serviceStop      = service.Stop
	serviceRestart   = service.Restart
)

// serviceHelp is the `service` help page. `service` routes on a positional
// action rather than flags, so writeHelp is called with a nil FlagSet and the
// Flags block is omitted.
var serviceHelp = helpSpec{
	name:    "service",
	summary: "install the daemon under the system service manager",
	usage: []string{
		"synosync service install",
		"synosync service uninstall",
		"synosync service status",
		"synosync service start|stop|restart",
	},
	describe: `Runs the daemon in the background under the platform's own service manager,
so syncing starts at login and survives a closed terminal.

` + "`start`" + `, ` + "`stop`" + ` and ` + "`restart`" + ` drive an already-installed service without
touching whether it starts at login. ` + "`stop`" + ` is what the "stop the daemon and
retry" hint asks for when ` + "`config set`" + ` finds the state database in use, and
` + "`restart`" + ` applies a changed setting, which the daemon reads once at startup.

` + "`install`" + ` writes the unit with THIS binary's real path, then enables and
starts it. That is the part worth automating: the templates have to guess where
the binary lives, and a wrong guess is the most common reason a hand-installed
unit never starts.

Linux uses a systemd user unit under ~/.config/systemd/user and macOS a launchd
LaunchAgent under ~/Library/LaunchAgents. Neither needs root: both already run
as the user who installed them.

Windows is different. A service runs under an account of its own, and it must
be the one you paired with - the state database, the keychain entries and the
control pipe all live in that user's profile. There is no safe way to collect a
password here (a password passed as a process argument is visible to every
other process), so on Windows ` + "`install`" + ` prints the elevated
install-service.ps1 command to run instead of registering anything.
` + "`status`" + `, ` + "`start`" + `, ` + "`stop`" + ` and ` + "`uninstall`" + ` work normally there,
though controlling a service is an administrative right: an access-denied answer
says so and repeats the elevated command.

Pair FIRST, and as the same user the service will run as: the daemon reads that
user's stored credentials and serves a control plane only that user can reach.

The same unit files ship in packaging/ inside the release archive, for anyone
who would rather install by hand.`,
	examples: []string{
		"synosync service install",
		"synosync service status",
		"synosync service restart      # after a `config set`, which needs the daemon stopped",
		"synosync service uninstall",
	},
}

// cmdService implements the `service` subcommand.
func cmdService(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "synosync: service needs an action")
		writeUsageHint(stderr, serviceHelp)
		return 2
	}
	if isHelpArg(args[0]) {
		writeHelp(stdout, serviceHelp, nil)
		return 0
	}

	action := args[0]
	// An action is required rather than defaulting to `status`, matching
	// `config`. It also means `install` - which writes a unit and enables a
	// background service - can never be reached by a bare `synosync service`.
	var run func(context.Context, service.Deps) (service.Report, error)
	switch action {
	case "install":
		run = serviceInstall
	case "uninstall":
		run = serviceUninstall
	case "status":
		run = serviceStatus
	case "start":
		run = serviceStart
	case "stop":
		run = serviceStop
	case "restart":
		run = serviceRestart
	default:
		fmt.Fprintf(stderr, "synosync: unknown service action %q, want one of "+
			"\"install\", \"uninstall\", \"status\", \"start\", \"stop\" or \"restart\"\n", action)
		writeUsageHint(stderr, serviceHelp)
		return 2
	}
	// Not rejectExtraArgs: its message is "service takes no positional
	// arguments", which contradicts itself here - the user just passed one and it
	// was accepted as the action. Name the action instead.
	if rest := args[1:]; len(rest) > 0 {
		fmt.Fprintf(stderr, "synosync: service %s takes no further arguments, got %s\n", action, quoteArgs(rest))
		writeUsageHint(stderr, serviceHelp)
		return 2
	}

	deps, err := serviceDeps()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}

	rep, err := run(context.Background(), deps)
	// The report is printed even on failure: the steps that DID run are how a
	// user knows what state the machine was left in, and a half-finished install
	// that reports nothing is the worst outcome of all.
	writeServiceReport(stdout, rep)
	if err != nil {
		if errors.Is(err, service.ErrUnsupported) {
			fmt.Fprintln(stderr, "synosync: this platform has no service-manager integration; run `synosync daemon` under your own supervisor")
			return 1
		}
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	return 0
}

// writeServiceReport renders a Report: what was done, where things stand, and
// what the user must still do themselves.
func writeServiceReport(stdout io.Writer, rep service.Report) {
	for _, s := range rep.Steps {
		fmt.Fprintln(stdout, s)
	}
	if rep.State != "" {
		if len(rep.Steps) > 0 {
			fmt.Fprintln(stdout)
		}
		name := rep.UnitPath
		if name == "" {
			name = "service"
		}
		fmt.Fprintf(stdout, "%s: %s\n", serviceDisplayName(name), rep.State)
	}
	for _, n := range rep.Notes {
		fmt.Fprintf(stdout, "note: %s\n", n)
	}
}

// serviceDisplayName shortens a unit path to the name a user recognises - the
// file name for a unit or agent, the service name as-is on Windows. Both
// separators are matched because a report can carry either, whatever the host.
func serviceDisplayName(unit string) string {
	if i := strings.LastIndexAny(unit, `/\`); i >= 0 {
		return unit[i+1:]
	}
	return unit
}
