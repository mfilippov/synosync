// Package service installs, removes and reports synosync's supervised service
// under the host's own service manager: a systemd user unit on Linux, a launchd
// LaunchAgent on macOS, and the Windows Service Control Manager via the shipped
// PowerShell installer.
//
// It exists because the templates alone were not enough. Both of them ask the
// operator to fill in a value the program already knows: the systemd unit
// hardcodes ExecStart=%h/.local/bin/synosync, so the binary had to be moved to
// exactly that path or the unit edited, and the launchd plist requires USERNAME
// to be substituted into two absolute paths because launchd does not expand ~.
// A first-time user therefore had to copy a file, edit it, reload a manager and
// know about `loginctl enable-linger` - six steps and two hidden prerequisites
// - to get the daemon to survive a logout.
//
// The templates in templates/ are the SINGLE source: they are embedded here and
// rendered with the real values at install time, and scripts/release.sh ships
// the very same files into each archive under packaging/ for anyone who prefers
// to install by hand. The file a user edits and the file this package renders
// cannot drift, because they are the same bytes.
//
// Everything the platform-specific code needs from the outside - the binary
// path, the home directory, the user name, and command execution - arrives
// through Deps, so the install flows are exercised in tests without a service
// manager anywhere near them.
package service

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// templates holds the service-manager templates. Listed file by file rather
// than as a directory so install-service_test.ps1 - a development self-test,
// not an operator tool - stays out of the binary.
//
//go:embed templates/synosync.service
//go:embed templates/me.filippov.synosync.plist
//go:embed templates/install-service.ps1
var templates embed.FS

// ErrUnsupported reports that this GOOS has no service integration. Returned by
// the fallback build so the CLI can print a clear message instead of pretending
// to have installed something.
var ErrUnsupported = errors.New("service: no service manager integration for this platform")

// ErrNotInstalled reports that a start/stop/restart was asked of a service that is not
// registered. It is distinguished from a service-manager failure so the CLI can answer "it is
// not installed" - which is what the user needs to hear - instead of relaying an error whose
// wording differs per platform and buries that fact.
var ErrNotInstalled = errors.New("service: not installed")

// commandTimeout bounds a single service-manager invocation. systemctl,
// launchctl and sc.exe are all local and answer in milliseconds; a hang means
// something is wrong with the manager, and a CLI that never returns is worse
// than one that reports a timeout.
const commandTimeout = 30 * time.Second

// State is what Status found.
type State string

const (
	// StateNotInstalled means no unit/agent/service is registered.
	StateNotInstalled State = "not installed"
	// StateInstalled means the unit is registered but not currently running.
	StateInstalled State = "installed, not running"
	// StateRunning means the service manager reports it as running.
	StateRunning State = "running"
	// StateUnknown means the unit is registered but the manager's answer could
	// not be classified. Reported honestly rather than guessed at.
	StateUnknown State = "installed, state unknown"
)

// Report is what an operation did, in terms the CLI can print without knowing
// anything platform-specific.
type Report struct {
	// Steps are the actions actually taken, in order, already phrased for a
	// human ("wrote ~/.config/systemd/user/synosync.service").
	Steps []string
	// State is what the service manager reports afterwards, including after
	// Uninstall, which reports StateNotInstalled. It is empty only when the
	// operation could not establish one at all - an install that failed before
	// the manager was queried.
	State State
	// Notes are follow-ups the user must perform themselves - the things no
	// installer can do for them, such as enabling linger or elevating.
	Notes []string
	// UnitPath is the file the operation wrote or removed, when there is one.
	UnitPath string
}

// Deps are the collaborators the platform code needs. Every one is a seam: the
// tests supply a temp home and a recording Run, so no test ever reaches a real
// service manager.
type Deps struct {
	// Exe is the absolute path of the synosync binary the service must run.
	Exe string
	// Home is the user's home directory; every unit path is derived from it.
	Home string
	// User is the current user's login name. It names the account in the Linux
	// linger hint and in the Windows run-as invocation; on Windows it must be in
	// a form the SCM can resolve. The launchd log paths are built from Home, not
	// from this.
	User string
	// QualifiedUser is the login name as the host reported it, before the bare-name trim:
	// DOMAIN\user on a domain-joined Windows machine, the plain name elsewhere. Only the
	// Windows run-as hint uses it, and it must: an unqualified name is ambiguous there, and
	// NTAccount resolves a bare "alice" to the DOMAIN account in preference to the local one,
	// so a printed -User alice can name an account the pairing does not belong to.
	QualifiedUser string
	// Run executes a service-manager command and returns its combined output.
	// A non-nil error carries the output, since systemctl and launchctl explain
	// themselves on stderr.
	Run func(ctx context.Context, name string, args ...string) ([]byte, error)
	// StateEnv are environment variables the rendered unit must carry, because
	// the service manager cannot inherit them from the shell that installs the
	// service. Empty in the ordinary case.
	//
	// config.DefaultPaths honours an absolute XDG_DATA_HOME on Linux AND macOS,
	// so a user who paired with it set has their state somewhere the default
	// lookup will not find. `systemd --user` is started by pam_systemd before any
	// login shell reads a profile, and launchd starts agents with a minimal
	// environment, so neither manager sees a shell-only export: without carrying
	// it into the unit the daemon opens the DEFAULT state directory, finds no
	// pairing and exits - on a restart loop, while the install reports "running"
	// because the state was queried moments after enable.
	StateEnv map[string]string
}

// stateEnvVars are the variables worth carrying into a rendered unit: the ones
// config.DefaultPaths reads to place the state directory.
var stateEnvVars = []string{"XDG_DATA_HOME", "XDG_STATE_HOME"}

// collectStateEnv returns the state-directory overrides currently in force.
// Only an ABSOLUTE value is carried: config.DefaultPaths ignores a relative one,
// so propagating it would make the service resolve a different directory from
// the pairing that installed it.
func collectStateEnv(getenv func(string) string) map[string]string {
	var out map[string]string
	for _, name := range stateEnvVars {
		v := getenv(name)
		if v == "" || !filepath.IsAbs(v) {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(stateEnvVars))
		}
		out[name] = v
	}
	return out
}

// DefaultDeps resolves the real collaborators. The binary path comes from
// os.Executable and is symlink-resolved: a unit that pointed at a symlink in a
// directory the user later cleans up would fail at next boot with a message
// about a file the user never installed.
func DefaultDeps() (Deps, error) {
	exe, err := os.Executable()
	if err != nil {
		return Deps{}, fmt.Errorf("service: locate this binary: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Deps{}, fmt.Errorf("service: locate the home directory: %w", err)
	}
	u, err := user.Current()
	if err != nil {
		return Deps{}, fmt.Errorf("service: current user: %w", err)
	}
	// On Windows the name arrives as DOMAIN\user; the bare name is what the
	// paths and hints want. Harmless elsewhere - a Unix login name has no
	// backslash.
	name := u.Username
	if i := strings.LastIndexByte(name, '\\'); i >= 0 {
		name = name[i+1:]
	}
	return Deps{
		Exe:           exe,
		Home:          home,
		User:          name,
		QualifiedUser: u.Username,
		Run:           runCommand,
		StateEnv:      collectStateEnv(os.Getenv),
	}, nil
}

// runCommand is the real Run: it bounds the call and returns combined output so
// a failure carries the manager's own explanation.
func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("service: %s %v: %w: %s", name, args, err, trimOutput(out))
	}
	return out, nil
}

// trimOutput bounds a command's output in an error message. A service manager
// that dumps a page of diagnostics should not push the actionable first line off
// the user's screen.
func trimOutput(out []byte) string {
	const max = 400
	s := strings.TrimRight(string(out), "\r\n")
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// validate checks the fields the platform code will dereference, so a caller
// that forgot one gets a clear error instead of an install written to "/".
func (d Deps) validate() error {
	switch {
	case d.Exe == "":
		return errors.New("service: no binary path")
	case !filepath.IsAbs(d.Exe):
		return fmt.Errorf("service: binary path %q is not absolute", d.Exe)
	case d.Home == "":
		return errors.New("service: no home directory")
	case d.Run == nil:
		return errors.New("service: no command runner")
	case strings.ContainsAny(d.Exe, "\n\r\x00"):
		// A newline cannot be carried by either format: systemd treats it as the
		// end of the directive and a plist would need a character reference the
		// path can never round-trip through. Refusing beats writing a unit that
		// silently starts something else.
		return fmt.Errorf("service: binary path %q contains a line break", d.Exe)
	}
	return nil
}

// template returns one embedded template's bytes.
func template(name string) (string, error) {
	b, err := templates.ReadFile("templates/" + name)
	if err != nil {
		return "", fmt.Errorf("service: read template %s: %w", name, err)
	}
	return string(b), nil
}

// writeFile writes content to path with perm, creating the parent directory. It
// replaces any existing file, so a re-install picks up a moved binary. The mode
// is the caller's: each call site documents why it chose the one it did.
func writeFile(path, content string, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("service: create %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), perm); err != nil {
		return fmt.Errorf("service: write %s: %w", path, err)
	}
	return nil
}

// tildeHome shortens a path under home to ~/… for display. The Steps a user
// reads are more legible with the home directory folded away, and it keeps a
// long login name from wrapping every line.
func tildeHome(path, home string) string {
	if home == "" || !strings.HasPrefix(path, home) {
		return path
	}
	rest := strings.TrimPrefix(path, home)
	// Require a separator so /home/alicia is not shortened against /home/alice.
	if rest == "" || (rest[0] != '/' && rest[0] != '\\') {
		return path
	}
	return "~" + rest
}

// sortedKeys returns m's keys in a stable order, so a rendered unit and the
// steps reporting it do not shuffle between runs.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
