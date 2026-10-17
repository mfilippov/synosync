package service

// macOS: a per-user launchd LaunchAgent. No root - same reason as the systemd
// user unit: the daemon reads the user's keychain and serves a per-user control
// plane, so it belongs to the login session, not to the system.

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// agentLabel is the launchd job label, and must match the Label in the
	// template.
	agentLabel = "me.filippov.synosync"
	// agentFile is the plist's file name under ~/Library/LaunchAgents.
	agentFile = agentLabel + ".plist"
	// templateExe and templateLogDir are the placeholder values the shipped
	// template carries so it is usable by hand. Rendering replaces both; if
	// either stops appearing, the template changed shape and install refuses
	// rather than writing an agent that runs the wrong binary or logs into
	// somebody else's home.
	templateExe    = "/usr/local/bin/synosync"
	templateLogDir = "/Users/USERNAME/Library/Logs/"
)

// agentPath returns where the LaunchAgent lives.
func agentPath(d Deps) string {
	return filepath.Join(d.Home, "Library", "LaunchAgents", agentFile)
}

// renderAgent substitutes the real binary path and log directory into the
// template. launchd requires absolute paths and expands neither ~ nor $HOME,
// which is exactly why the template has to carry a literal USERNAME for hand
// installs - and exactly what this removes.
func renderAgent(tmpl, exe, home string, env map[string]string) (string, error) {
	// Substitution is anchored to <string> ELEMENTS, never to the bare path.
	//
	// templateExe also appears in the header comment ("edit ProgramArguments[0]
	// below if the binary is not at /usr/local/bin/synosync"), and a blanket
	// ReplaceAll spliced the real path into that XML comment too. XML forbids
	// "--" inside a comment and escaping cannot help - a hyphen is not a
	// metacharacter - so a binary under /Users/bob/dev--tools produced an
	// ill-formed plist, and one containing "-->" closed the comment early. Both
	// leave a permanently broken agent on disk, which is exactly what the
	// escaping below exists to prevent. Anchoring also keeps the promise the
	// systemd renderer makes: only directives are rewritten, the template's own
	// prose survives into the installed file.
	var exes, logs int
	out := stringElement.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := m[len("<string>") : len(m)-len("</string>")]
		switch {
		case inner == templateExe:
			exes++
			return "<string>" + xmlText(exe) + "</string>"
		case strings.HasPrefix(inner, templateLogDir):
			logs++
			rest := strings.TrimPrefix(inner, templateLogDir)
			return "<string>" + xmlText(filepath.Join(home, "Library", "Logs")+"/"+rest) + "</string>"
		}
		return m
	})
	// The same drift guard renderUnit applies: an agent that runs the wrong
	// binary, or logs into somebody else's home, is worse than a refusal.
	if exes != 1 {
		return "", fmt.Errorf("service: template has %d <string>%s</string> elements, want exactly 1", exes, templateExe)
	}
	if logs != 2 {
		return "", fmt.Errorf("service: template has %d log-path elements under %s, want exactly 2", logs, templateLogDir)
	}
	return withStateEnv(out, env)
}

// stringElement matches one <string>…</string> element's whole text. The plist
// carries no nested markup inside them, so the character class is enough.
var stringElement = regexp.MustCompile(`<string>[^<]*</string>`)

// withStateEnv adds an EnvironmentVariables dict carrying the state-directory
// overrides. Nothing is added when there are none, which is the ordinary case.
//
// launchd starts an agent with a minimal environment and inherits nothing from
// the shell that installed it, so a user who paired with XDG_DATA_HOME set would
// otherwise have the agent open the DEFAULT state directory, find no pairing and
// exit - repeatedly, under the template's KeepAlive.
func withStateEnv(plist string, env map[string]string) (string, error) {
	if len(env) == 0 {
		return plist, nil
	}
	// The agent's own dict closes with the document; insert before it.
	const closing = "</dict>\n</plist>"
	if !strings.Contains(plist, closing) {
		return "", fmt.Errorf("service: template does not end in %q", closing)
	}
	var b strings.Builder
	b.WriteString("\t<!-- launchd inherits no shell environment; the daemon resolves its\n")
	b.WriteString("\t     state directory from these. -->\n")
	b.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
	for _, k := range sortedKeys(env) {
		fmt.Fprintf(&b, "\t\t<key>%s</key>\n\t\t<string>%s</string>\n", xmlText(k), xmlText(env[k]))
	}
	b.WriteString("\t</dict>\n\n")
	return strings.Replace(plist, closing, b.String()+closing, 1), nil
}

// xmlText escapes s for use as XML character data.
func xmlText(s string) string {
	var b strings.Builder
	if err := xml.EscapeText(&b, []byte(s)); err != nil {
		// EscapeText only fails when the writer does, and a strings.Builder
		// never does.
		return s
	}
	return b.String()
}

// Install writes the LaunchAgent with real paths and loads it.
func Install(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	tmpl, err := template(agentFile)
	if err != nil {
		return Report{}, err
	}
	plist, err := renderAgent(tmpl, d.Exe, d.Home, d.StateEnv)
	if err != nil {
		return Report{}, err
	}

	path := agentPath(d)
	if err := writeFile(path, plist, 0o644); err != nil {
		return Report{}, err
	}
	rep := Report{
		UnitPath: path,
		Steps: []string{
			"wrote   " + tildeHome(path, d.Home),
			"        " + d.Exe + " daemon",
		},
	}
	for _, k := range sortedKeys(d.StateEnv) {
		rep.Steps = append(rep.Steps, "        "+k+"="+d.StateEnv[k])
	}

	// Reload rather than load: launchctl refuses to load an already-loaded
	// label, so a re-install onto a moved binary would fail on the very step
	// meant to pick the move up. Unload first, ignoring "was not loaded".
	_, _ = d.Run(ctx, "launchctl", "unload", "-w", path)
	if _, err := d.Run(ctx, "launchctl", "load", "-w", path); err != nil {
		return rep, err
	}
	rep.Steps = append(rep.Steps, "ran     launchctl load -w "+tildeHome(path, d.Home))
	rep.State = queryState(ctx, d, path)
	return rep, nil
}

// Uninstall unloads the agent and removes the plist. Every step tolerates "it
// was not there", so uninstall converges on a clean machine from any starting
// state, including a half-finished install.
func Uninstall(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := agentPath(d)
	rep := Report{UnitPath: path}

	// Only launchctl's "it was not loaded" answer is ignored. Any other unload
	// failure must NOT be followed by removing the plist and reporting
	// StateNotInstalled: the agent would stay loaded and the daemon running,
	// while every later status call saw the missing plist and called it
	// uninstalled. The systemd arm draws the same line.
	if out, err := d.Run(ctx, "launchctl", "unload", "-w", path); err != nil {
		if !mentionsUnloadedAgent(out, err) {
			return rep, err
		}
		rep.Steps = append(rep.Steps, "agent was not loaded (nothing to unload)")
	} else {
		rep.Steps = append(rep.Steps, "ran     launchctl unload -w "+tildeHome(path, d.Home))
	}

	switch err := os.Remove(path); {
	case err == nil:
		rep.Steps = append(rep.Steps, "removed "+tildeHome(path, d.Home))
	case errors.Is(err, fs.ErrNotExist):
		rep.Steps = append(rep.Steps, "no agent at "+tildeHome(path, d.Home)+" (nothing to remove)")
	default:
		return rep, fmt.Errorf("service: remove %s: %w", path, err)
	}
	rep.State = StateNotInstalled
	return rep, nil
}

// mentionsUnloadedAgent reports whether an unload failure means the job was
// simply not loaded. launchctl says so in words rather than with a distinct
// exit code, so the message is what has to be read.
func mentionsUnloadedAgent(out []byte, err error) bool {
	text := strings.ToLower(string(out) + " " + err.Error())
	return strings.Contains(text, "could not find specified service") ||
		strings.Contains(text, "no such file or directory") ||
		strings.Contains(text, "not find") ||
		strings.Contains(text, "no such process")
}

// Status reports whether the agent is installed and loaded.
func Status(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := agentPath(d)
	return Report{UnitPath: path, State: queryState(ctx, d, path)}, nil
}

// pidLine matches the PID entry of the plist-style dict `launchctl list <label>`
// prints, e.g. `	"PID" = 4242;`.
var pidLine = regexp.MustCompile(`"PID"\s*=\s*(\d+)\s*;`)

// queryState maps the plist's presence plus `launchctl list <label>` onto a
// State.
//
// The PID key is the whole signal, and it is present ONLY while the job runs:
// an idle job's dict carries LastExitStatus and no PID line at all. The "-"
// placeholder belongs to the tabular `launchctl list` with no argument, which is
// a different command - reading it here made every loaded-but-idle agent report
// "state unknown", which is exactly the case a user runs `service status` to ask
// about (this template sets KeepAlive.SuccessfulExit=false and a 300 s
// ThrottleInterval, so idle is entirely normal).
func queryState(ctx context.Context, d Deps, path string) State {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return StateNotInstalled
	}
	out, err := d.Run(ctx, "launchctl", "list", agentLabel)
	if err != nil {
		// Not loaded: the plist is on disk but launchd does not know it.
		return StateInstalled
	}
	switch {
	case pidLine.Match(out):
		return StateRunning
	case len(strings.TrimSpace(string(out))) > 0:
		// launchctl knows the label and answered, but the job is not running.
		return StateInstalled
	}
	// A successful call that said nothing at all is not something to classify.
	return StateUnknown
}

// Start, Stop and Restart drive an already-installed agent.
//
// They exist because every hint that says "stop the daemon and retry" - `config set` refuses
// while the state database is held - had no answer once the daemon ran as an agent. The user
// was left to know the launchctl invocation, or to uninstall and reinstall around a settings
// change.
//
// These use load/unload WITHOUT -w, unlike Install and Uninstall. The flag writes the job's
// disabled state, so `-w` on a stop would also cancel the login autostart - stopping is not
// disabling, and anyone who stops the agent to change a setting expects it back at next login.
func Start(ctx context.Context, d Deps) (Report, error) { return runAgentVerb(ctx, d, "start") }

// Stop unloads a running agent, leaving its enabled state alone.
func Stop(ctx context.Context, d Deps) (Report, error) { return runAgentVerb(ctx, d, "stop") }

// Restart applies a changed setting without an uninstall/reinstall round trip.
func Restart(ctx context.Context, d Deps) (Report, error) { return runAgentVerb(ctx, d, "restart") }

// runAgentVerb applies one verb to the installed agent and reports the resulting state.
func runAgentVerb(ctx context.Context, d Deps, verb string) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := agentPath(d)
	rep := Report{UnitPath: path}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		rep.State = StateNotInstalled
		return rep, ErrNotInstalled
	}

	if verb == "stop" || verb == "restart" {
		// An agent that is not loaded is not an error for a stop: the end state asked for is
		// the one already in place. Only launchctl's own "not loaded" answer is tolerated.
		if out, err := d.Run(ctx, "launchctl", "unload", path); err != nil && !mentionsUnloadedAgent(out, err) {
			return rep, err
		}
		rep.Steps = append(rep.Steps, "ran     launchctl unload "+tildeHome(path, d.Home))
	}
	if verb == "start" || verb == "restart" {
		if _, err := d.Run(ctx, "launchctl", "load", path); err != nil {
			return rep, err
		}
		rep.Steps = append(rep.Steps, "ran     launchctl load "+tildeHome(path, d.Home))
	}
	rep.State = queryState(ctx, d, path)
	return rep, nil
}
