package service

// Linux: a systemd USER unit. No root, no system-wide install - the daemon
// holds per-user credentials in the user's keychain and serves a control plane
// only that user can reach, so a system service would be the wrong shape even
// if it were easier.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// unitName is the systemd unit this package installs.
const unitName = "synosync.service"

// unitPath returns where the user unit lives: ~/.config/systemd/user,
// unconditionally, resolved off Deps.Home so a test's temp home is honoured.
//
// systemd's user search path comes from $XDG_CONFIG_HOME, but from the
// environment of the `systemd --user` MANAGER, not of this process. The manager
// is started by pam_systemd at session open, before any login shell reads a
// profile, so a shell-only export does not move it and ~/.config stays right.
// Reading the variable here would therefore be wrong for the common case; a
// setup that really does move it (environment.d, a PAM module,
// `systemctl --user import-environment`) would need the value the manager
// reports, not the one this process inherited.
func unitPath(d Deps) string {
	return filepath.Join(d.Home, ".config", "systemd", "user", unitName)
}

// renderUnit substitutes the real binary path into the template's ExecStart.
//
// The template ships ExecStart=%h/.local/bin/synosync so it also works when
// copied by hand, but that path is a guess about where the user put the binary
// - and the single most common way a hand-installed unit fails is that the
// binary is somewhere else. Replace the whole directive with the path this
// process was actually launched from.
//
// Exactly one ExecStart line must be present. Zero or several means the
// template changed shape, and silently installing a unit that starts the wrong
// thing (or nothing) is worse than refusing.
func renderUnit(tmpl, exe string) (string, error) {
	lines := strings.Split(tmpl, "\n")
	found := 0
	for i, l := range lines {
		if !strings.HasPrefix(l, "ExecStart=") {
			continue
		}
		found++
		lines[i] = "ExecStart=" + quoteExecArg(exe) + " daemon --log-stderr"
	}
	if found != 1 {
		return "", fmt.Errorf("service: template has %d ExecStart lines, want exactly 1", found)
	}
	return strings.Join(lines, "\n"), nil
}

// withStateEnv inserts an Environment= line per state-directory override, just
// above ExecStart so a reader sees what the unit runs with. Nothing is inserted
// when there is no override, which is the ordinary case.
//
// The value is double-quoted and its % doubled for the same reasons the exec
// path is: systemd splits on whitespace and expands % as a specifier.
func withStateEnv(unit string, env map[string]string) string {
	if len(env) == 0 {
		return unit
	}
	names := make([]string, 0, len(env))
	for k := range env {
		names = append(names, k)
	}
	sort.Strings(names)

	lines := strings.Split(unit, "\n")
	out := make([]string, 0, len(lines)+len(names)+1)
	for _, l := range lines {
		if strings.HasPrefix(l, "ExecStart=") {
			out = append(out,
				"# The manager does not inherit these from the shell that installed the",
				"# service, and the daemon resolves its state directory from them.")
			for _, k := range names {
				out = append(out, "Environment="+quoteExecArg(k+"="+env[k]))
			}
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// quoteExecArg renders one ExecStart word so systemd runs the path verbatim.
//
// Two things bite an unquoted path. systemd splits an ExecStart value on
// whitespace, so a binary under "/home/bob/My Tools" would exec "/home/bob/My"
// and fail 203/EXEC; and it expands "%" as a specifier introducer everywhere in
// a unit value - the template's own ExecStart relies on %h - so a "%" in the
// path would be swallowed or misread. Double-quoting fixes the first (systemd
// honours C-style escapes inside, so a literal " or \ must be escaped), and
// doubling "%" to "%%" fixes the second.
func quoteExecArg(exe string) string {
	esc := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(exe)
	esc = strings.ReplaceAll(esc, "%", "%%")
	return `"` + esc + `"`
}

// Install writes the user unit with this binary's real path, reloads the user
// manager and enables plus starts the service.
func Install(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	tmpl, err := template("synosync.service")
	if err != nil {
		return Report{}, err
	}
	unit, err := renderUnit(tmpl, d.Exe)
	if err != nil {
		return Report{}, err
	}
	unit = withStateEnv(unit, d.StateEnv)

	path := unitPath(d)
	// 0644: systemd reads the unit as the user, and a unit file carries no
	// secret - only paths. Matching what a hand-copied unit would get keeps a
	// later manual edit from being surprised by the mode.
	if err := writeFile(path, unit, 0o644); err != nil {
		return Report{}, err
	}
	rep := Report{
		UnitPath: path,
		Steps: []string{
			"wrote   " + tildeHome(path, d.Home),
			"        ExecStart=" + quoteExecArg(d.Exe) + " daemon --log-stderr",
		},
	}
	for _, k := range sortedKeys(d.StateEnv) {
		rep.Steps = append(rep.Steps, "        Environment="+k+"="+d.StateEnv[k])
	}

	for _, args := range [][]string{
		{"--user", "daemon-reload"},
		{"--user", "enable", "--now", unitName},
	} {
		if _, err := d.Run(ctx, "systemctl", args...); err != nil {
			return rep, err
		}
		rep.Steps = append(rep.Steps, "ran     systemctl "+strings.Join(args, " "))
	}

	rep.State = queryState(ctx, d, path)
	rep.Notes = append(rep.Notes, lingerNote(ctx, d)...)
	return rep, nil
}

// Uninstall stops and disables the service, then removes the unit.
//
// It converges on a clean machine from any starting state, including a
// half-finished install, so an ABSENT unit is success rather than an error the
// user has to work around. That tolerance is deliberately narrow: only a
// failure that positively identifies a missing unit is ignored.
func Uninstall(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := unitPath(d)
	rep := Report{UnitPath: path}

	// Only a positively-identified absent unit is ignored. Any other failure -
	// an SSH shell that cannot reach the user bus while a lingered service is
	// still running, say - must NOT be followed by removing the unit and
	// reporting StateNotInstalled: the daemon would keep running and holding the
	// state database while this command claimed it was gone.
	if out, err := d.Run(ctx, "systemctl", "--user", "disable", "--now", unitName); err != nil {
		if !mentionsAbsentUnit(out, err) {
			return rep, err
		}
		rep.Steps = append(rep.Steps, "no unit registered with systemd (nothing to disable)")
	} else {
		rep.Steps = append(rep.Steps, "ran     systemctl --user disable --now "+unitName)
	}

	switch err := os.Remove(path); {
	case err == nil:
		rep.Steps = append(rep.Steps, "removed "+tildeHome(path, d.Home))
	case errors.Is(err, fs.ErrNotExist):
		rep.Steps = append(rep.Steps, "no unit at "+tildeHome(path, d.Home)+" (nothing to remove)")
	default:
		return rep, fmt.Errorf("service: remove %s: %w", path, err)
	}

	if _, err := d.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		// The unit is already gone, so the removal stands; report the reload
		// failure rather than hiding it, since systemd still lists the old unit
		// until something reloads it.
		rep.State = StateNotInstalled
		return rep, err
	}
	rep.Steps = append(rep.Steps, "ran     systemctl --user daemon-reload")
	rep.State = StateNotInstalled
	return rep, nil
}

// mentionsAbsentUnit reports whether a disable failure means the unit was simply
// not there. systemctl says so in words rather than with a distinct exit code,
// so the message is what has to be read.
//
// Reachability is decided FIRST, and a bare ENOENT is not evidence of absence.
// "No such file or directory" is exactly what `systemctl --user` prints when it
// cannot reach the user bus - "Failed to connect to user scope bus via local
// transport: No such file or directory" - which happens whenever XDG_RUNTIME_DIR
// does not point at the live bus: `sudo -u alice`, `su alice -c`, a cron job, a
// detached session, a container exec. Treating that as absence made Uninstall
// delete the unit and report "not installed" while the daemon kept running and
// holding the state database, with the unit file now gone so no later uninstall
// could stop it. systemctl's genuine absence messages NAME the unit, so require
// that too.
func mentionsAbsentUnit(out []byte, err error) bool {
	text := strings.ToLower(string(out) + " " + err.Error())
	// Any doubt about reaching the manager is a real failure, never absence.
	if strings.Contains(text, "failed to connect") || strings.Contains(text, "bus") {
		return false
	}
	if strings.Contains(text, "not loaded") {
		return true
	}
	// "Unit file synosync.service does not exist."
	return strings.Contains(text, "does not exist") && strings.Contains(text, strings.ToLower(unitName))
}

// Status reports whether the unit is installed and running.
func Status(ctx context.Context, d Deps) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := unitPath(d)
	rep := Report{UnitPath: path, State: queryState(ctx, d, path)}
	if rep.State != StateNotInstalled {
		rep.Notes = append(rep.Notes, lingerNote(ctx, d)...)
	}
	return rep, nil
}

// queryState maps the unit file's presence plus `systemctl is-active` onto a
// State. is-active exits non-zero for every not-running answer, so the exit
// code says nothing on its own and the OUTPUT word is what is classified.
func queryState(ctx context.Context, d Deps, path string) State {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return StateNotInstalled
	}
	// The error is deliberately dropped: is-active exits non-zero for every
	// not-running answer, so it carries no information the word does not.
	out, _ := d.Run(ctx, "systemctl", "--user", "is-active", unitName)
	word := strings.TrimSpace(string(out))
	switch word {
	case "active", "activating":
		return StateRunning
	case "inactive", "failed", "deactivating":
		return StateInstalled
	}
	// Anything else - an unrecognised word, or systemctl itself failing to run
	// because it is absent or there is no user manager - leaves the unit on disk
	// with nothing here justifying a claim that it runs.
	return StateUnknown
}

// lingerNote returns the linger follow-up when it applies.
//
// A systemd user manager is torn down at the user's last logout unless linger
// is enabled, so a service installed from an SSH session or a desktop terminal
// stops syncing the moment the session ends - silently, and long after the
// install looked successful. It is the one prerequisite no installer can
// satisfy for the user: enabling linger needs privileges this process does not
// have. Ask loginctl rather than assuming; when loginctl cannot answer, say so
// conditionally instead of ordering the user around.
func lingerNote(ctx context.Context, d Deps) []string {
	out, err := d.Run(ctx, "loginctl", "show-user", d.User, "--property=Linger")
	answer := strings.TrimSpace(string(out))
	switch {
	case err == nil && answer == "Linger=yes":
		return nil
	case err == nil && answer == "Linger=no":
		return []string{fmt.Sprintf("syncing stops at logout until linger is enabled: sudo loginctl enable-linger %s", d.User)}
	default:
		return []string{fmt.Sprintf("if syncing stops when you log out, enable linger: sudo loginctl enable-linger %s", d.User)}
	}
}

// Start, Stop and Restart drive an already-installed unit.
//
// They exist because every hint that says "stop the daemon and retry" - `config set` refuses
// while the state database is held - had no answer once the daemon ran as a service. The user
// was left to know `systemctl --user stop synosync.service` themselves, or to uninstall and
// reinstall around a settings change.
//
// The unit's ENABLED state is deliberately untouched: stopping is not disabling, and a stop
// that also cancelled the login autostart would surprise anyone who ran it to change a setting.
func Start(ctx context.Context, d Deps) (Report, error) { return runUnitVerb(ctx, d, "start") }

// Stop halts a running unit, leaving it enabled for the next login.
func Stop(ctx context.Context, d Deps) (Report, error) { return runUnitVerb(ctx, d, "stop") }

// Restart applies a changed setting without an uninstall/reinstall round trip.
func Restart(ctx context.Context, d Deps) (Report, error) { return runUnitVerb(ctx, d, "restart") }

// runUnitVerb applies one systemctl verb to the installed unit and reports the resulting state.
// An absent unit is reported as such rather than as a systemctl failure, since "it is not
// installed" is the answer the user needs and the raw error buries it.
func runUnitVerb(ctx context.Context, d Deps, verb string) (Report, error) {
	if err := d.validate(); err != nil {
		return Report{}, err
	}
	path := unitPath(d)
	rep := Report{UnitPath: path}
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		rep.State = StateNotInstalled
		return rep, ErrNotInstalled
	}
	if out, err := d.Run(ctx, "systemctl", "--user", verb, unitName); err != nil {
		if mentionsAbsentUnit(out, err) {
			rep.State = StateNotInstalled
			return rep, ErrNotInstalled
		}
		return rep, err
	}
	rep.Steps = append(rep.Steps, "ran     systemctl --user "+verb+" "+unitName)
	rep.State = queryState(ctx, d, path)
	return rep, nil
}
