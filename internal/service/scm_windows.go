package service

// Windows: the Service Control Manager, driven through the shipped PowerShell
// installer rather than a second implementation in Go.
//
// install-service.ps1 already does more than registering a service: it grants
// SeServiceLogonRight to a named run-as account (which CreateService does NOT
// do, so without it SCM refuses to start the service with error 1069),
// configures sc.exe failure/restart actions, and resolves the binary beside
// itself. It has its own self-test running in CI. Re-implementing that here
// would create a second thing to keep correct, so this drives it.
//
// Both operations work from the EMBEDDED copy of the script, written to a temp
// file: a `go install` build has no packaging/ directory beside it, and a
// release binary invoked through PATH need not be run from the directory it was
// unpacked into. `Uninstall` runs that copy; `Install` refuses (see below) and
// prints its path so the command it hands back always names a file that exists.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// serviceName is the Windows service the script registers.
const serviceName = "synosync"

// scriptName is the embedded installer.
const scriptName = "install-service.ps1"

// stageScript writes the embedded installer to a temp file and returns its path
// plus a cleanup. The binary is passed explicitly with -BinPath, so the script's
// own sibling-lookup never has to guess from a temp directory.
func stageScript() (path string, cleanup func(), err error) {
	body, err := template(scriptName)
	if err != nil {
		return "", func() {}, err
	}
	dir, err := os.MkdirTemp("", "synosync-svc")
	if err != nil {
		return "", func() {}, fmt.Errorf("service: temp dir: %w", err)
	}
	path = filepath.Join(dir, scriptName)
	// 0600: the staged copy lives in a shared temp directory, and a script this
	// process is about to run with an elevated token must not be writable by
	// anybody else in the window between writing it and executing it.
	if err := writeFile(path, body, 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return "", func() {}, err
	}
	return path, func() { _ = os.RemoveAll(dir) }, nil
}

// runScript invokes the staged installer with the given action.
func runScript(ctx context.Context, d Deps, action string) ([]byte, error) {
	script, cleanup, err := stageScript()
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return d.Run(ctx, "powershell",
		"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
		"-File", script, "-Action", action, "-BinPath", d.Exe)
}

// elevationNote is the follow-up for the one thing this cannot do for the user.
// Creating, deleting, starting and stopping a Windows service all require an
// elevated token; sc.exe answers an unelevated caller with "Access is denied"
// (error 5), which is not obviously about elevation.
const elevationNote = "creating or removing a Windows service needs an elevated console: " +
	"run this from PowerShell started with \"Run as administrator\""

// ErrNeedsRunAsAccount reports that a Windows install needs an explicit run-as
// account, which this command cannot collect.
var ErrNeedsRunAsAccount = errors.New(
	"service: a Windows service must be told which account to run as")

// Install refuses, and hands back the command that works.
//
// The refusal is the point. install-service.ps1 attaches a credential only when
// -User is given; without one, New-Service registers an Automatic-start
// LocalSystem service. LocalSystem has its own profile, so it sees none of the
// paired user's state database, none of their keychain entries, and cannot open
// the control-plane named pipe, whose DACL grants exactly that user's SID. The
// service would start, find no config, exit - and the failure actions the script
// configures would restart it on a loop, forever.
//
// Registering that and reporting a state for it is worse than doing nothing:
// the user ends up with a service that looks installed, never syncs, and churns.
// So this returns the exact elevated invocation instead. Supplying -Password
// from here is what would remove the extra step, and it is deliberately not
// done: a password passed as a process argument is visible to every other
// process on the machine, and the script takes a SecureString precisely to
// avoid that.
//
// Linux and macOS need no such account - a user unit and a LaunchAgent already
// run as the user who installed them.
func Install(_ context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	account := runAsAccount(d)
	// Write the embedded script out and name THAT path. Pointing at
	// .\packaging\install-service.ps1 assumed the user was standing in an
	// unpacked archive: a `go install` build has no such directory anywhere on
	// the machine, and a release binary run through PATH from elsewhere would be
	// handed a path that does not resolve. The copy is left in place on purpose
	// - the user has to be able to run it after this process exits.
	script, _, err := stageScript()
	if err != nil {
		return Report{}, err
	}
	rep := Report{
		UnitPath: serviceName,
		Notes: []string{
			"a Windows service runs under an account of its own, and it must be the " +
				"one you paired with: the state database, the keychain entries and the " +
				"control pipe all live in that user's profile, and the default " +
				"LocalSystem account can reach none of them.",
			"from an elevated PowerShell, run:",
			"    & " + psQuote(script) + " -Action install -BinPath " + psQuote(d.Exe) +
				" -User " + psQuote(account) + " -Password (Read-Host -AsSecureString)",
			"`synosync service status` and `synosync service uninstall` work as usual once it is registered.",
		},
	}
	// The same blindness the Linux and macOS renderers work around, except the
	// SCM takes no environment from this process and install-service.ps1 has no
	// way to set one: a service launched by the SCM will not see these, so the
	// daemon would resolve the DEFAULT state directory and find no pairing. Say
	// so rather than let it fail silently.
	if len(d.StateEnv) > 0 {
		for _, k := range sortedKeys(d.StateEnv) {
			rep.Notes = append(rep.Notes, "you paired with "+k+"="+d.StateEnv[k]+
				", which a service launched by the SCM will NOT inherit: set it "+
				"for the service account (setx /m, or the service's Environment "+
				"registry value) or the daemon will look in the default location.")
		}
	}
	return rep, ErrNeedsRunAsAccount
}

// psQuote renders a value as a PowerShell single-quoted literal.
//
// Without it the printed command was the one Windows path the CLI offers, and
// it failed for the most ordinary install location there is: pasting
// -BinPath C:\Program Files\synosync\synosync.exe binds only "C:\Program" and
// treats the rest as another argument. A single-quoted literal expands nothing,
// so a path holding a space, $ or backtick survives verbatim; an embedded quote
// is doubled, which is how PowerShell escapes it.
func psQuote(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// Uninstall stops and removes the service.
func Uninstall(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	rep := Report{UnitPath: serviceName}
	// The step is recorded only once the script has actually run. Claiming it
	// up front misreported an unelevated attempt - Assert-Admin throws before
	// touching sc.exe - as a half-completed uninstall, and a stageScript failure
	// as a run that never happened at all.
	out, err := runScript(ctx, d, "uninstall")
	if err != nil {
		if needsElevation(out) {
			rep.Notes = append(rep.Notes, elevationNote)
		}
		return rep, err
	}
	rep.Steps = append(rep.Steps, "ran     install-service.ps1 -Action uninstall")
	rep.State = StateNotInstalled
	return rep, nil
}

// Status reports the service's state. Querying needs no elevation, so this is
// the one action an ordinary console can always perform.
func Status(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	return Report{UnitPath: serviceName, State: queryState(ctx, d)}, nil
}

// queryState asks sc.exe directly rather than through the script: a state query
// is a single well-defined call, and going straight to it keeps `service status`
// working even where the PowerShell execution policy blocks a staged script.
func queryState(ctx context.Context, d Deps) State {
	out, err := d.Run(ctx, "sc.exe", "query", serviceName)
	text := string(out)
	if err != nil {
		// 1060 is "the specified service does not exist"; anything else leaves
		// the answer genuinely unknown rather than proving absence.
		if strings.Contains(text, "1060") {
			return StateNotInstalled
		}
		return StateUnknown
	}
	switch {
	case strings.Contains(text, "RUNNING"):
		return StateRunning
	case strings.Contains(text, "STOPPED"), strings.Contains(text, "START_PENDING"),
		strings.Contains(text, "STOP_PENDING"), strings.Contains(text, "PAUSED"):
		return StateInstalled
	}
	return StateUnknown
}

// needsElevation spots the unelevated-caller failure in a command's output so
// the report can name elevation instead of leaving the user with error 5.
//
// The script's OWN check is what fires in practice: install-service.ps1 calls
// Assert-Admin before any sc.exe or New-Service call and throws "This action
// requires an elevated (Administrator) PowerShell session." - text carrying
// neither "access is denied" nor "error 5", so matching only those two meant
// the note never appeared on the very path it was written for. sc.exe's own
// wording is kept for the case where something gets past Assert-Admin.
func needsElevation(out []byte) bool {
	text := strings.ToLower(string(out))
	for _, marker := range []string{
		"requires an elevated",
		"administrator",
		"access is denied",
		"error 5",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// Start, Stop and Restart drive an already-registered service.
//
// They exist because every hint that says "stop the daemon and retry" - `config set` refuses
// while the state database is held - had no answer once the daemon ran as a service. The user
// was left to know the sc.exe invocation, or to uninstall and reinstall around a settings
// change.
//
// Unlike Install these do NOT refuse up front: they need no run-as account, only the
// administrative right to control a service, which the DACL may already grant. So the attempt
// is made and an access-denied answer carries the same elevation note the other verbs use.
func Start(ctx context.Context, d Deps) (Report, error) { return runServiceVerb(ctx, d, "start") }

// Stop halts a running service, leaving it registered and set to start at boot.
func Stop(ctx context.Context, d Deps) (Report, error) { return runServiceVerb(ctx, d, "stop") }

// Restart applies a changed setting without an uninstall/reinstall round trip.
func Restart(ctx context.Context, d Deps) (Report, error) { return runServiceVerb(ctx, d, "restart") }

// runServiceVerb runs one control action through the staged script and reports the resulting
// state. An absent service is reported as ErrNotInstalled rather than as a script failure,
// since "it is not installed" is the answer the user needs and the raw error buries it.
func runServiceVerb(ctx context.Context, d Deps, verb string) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	rep := Report{UnitPath: serviceName}
	if queryState(ctx, d) == StateNotInstalled {
		rep.State = StateNotInstalled
		return rep, ErrNotInstalled
	}
	out, err := runScript(ctx, d, verb)
	if err != nil {
		if needsElevation(out) {
			rep.Notes = append(rep.Notes, elevationNote)
		}
		return rep, err
	}
	rep.Steps = append(rep.Steps, "ran     install-service.ps1 -Action "+verb)
	rep.State = queryState(ctx, d)
	return rep, nil
}

// runAsAccount renders the account the printed install command should name.
//
// A bare login name is ambiguous on Windows and resolves the wrong way where it matters:
// NTAccount("alice") prefers the DOMAIN account over the local one, so a printed -User alice can
// name an account that holds none of the pairing this install is for. The script's own
// documentation and its SID resolver both expect a qualified form.
//
// So the host's own spelling is used when it carries a domain, and a bare name is qualified with
// the ".\" local prefix that install-service.ps1's Resolve-AccountSidBytes already understands.
func runAsAccount(d Deps) string {
	if strings.Contains(d.QualifiedUser, `\`) {
		return d.QualifiedUser
	}
	name := d.User
	if name == "" {
		name = "<account>"
		return name
	}
	return `.\` + name
}
