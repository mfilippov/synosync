package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInstallWritesTheUnitWithThisBinarysPath is the reason this package
// exists: the shipped template hardcodes ExecStart=%h/.local/bin/synosync, and a
// binary anywhere else produces a unit that fails to start. Install must write
// the path the process was actually launched from.
func TestInstallWritesTheUnitWithThisBinarysPath(t *testing.T) {
	d, r := testDeps(t)
	r.outputs["systemctl --user is-active synosync.service"] = "active\n"
	r.outputs["loginctl show-user alice --property=Linger"] = "Linger=yes\n"

	rep, err := Install(context.Background(), d)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	body, err := os.ReadFile(unitPath(d))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	want := `ExecStart="` + d.Exe + `" daemon --log-stderr`
	if !strings.Contains(string(body), want) {
		t.Errorf("unit does not contain %q:\n%s", want, body)
	}
	if strings.Contains(string(body), "ExecStart=%h/.local/bin/synosync") {
		t.Error("unit still carries the template's placeholder ExecStart")
	}
	// The template's own documentation must survive: it explains the restart
	// bounds and the sandbox choices, and a user reading the installed unit
	// should see the same reasoning as one reading the shipped copy.
	for _, keep := range []string{"StartLimitBurst=5", "NoNewPrivileges=yes", "WantedBy=default.target"} {
		if !strings.Contains(string(body), keep) {
			t.Errorf("unit lost %q from the template", keep)
		}
	}

	for _, cmd := range []string{
		"systemctl --user daemon-reload",
		"systemctl --user enable --now synosync.service",
	} {
		if !r.ran(cmd) {
			t.Errorf("install did not run %q; ran %v", cmd, r.calls)
		}
	}
	if rep.State != StateRunning {
		t.Errorf("State = %q, want %q", rep.State, StateRunning)
	}
	if rep.UnitPath != unitPath(d) {
		t.Errorf("UnitPath = %q, want %q", rep.UnitPath, unitPath(d))
	}
	if len(rep.Notes) != 0 {
		t.Errorf("Notes = %v, want none when linger is already on", rep.Notes)
	}
}

// TestInstallReinstallsOverAnExistingUnit confirms a re-install picks up a moved
// binary rather than refusing or leaving the old path in place.
func TestInstallReinstallsOverAnExistingUnit(t *testing.T) {
	d, r := testDeps(t)
	r.outputs["systemctl --user is-active synosync.service"] = "active\n"
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("first Install: %v", err)
	}

	moved := d
	moved.Exe = filepath.Join(d.Home, "elsewhere", "synosync")
	if _, err := Install(context.Background(), moved); err != nil {
		t.Fatalf("second Install: %v", err)
	}
	body, err := os.ReadFile(unitPath(d))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	if !strings.Contains(string(body), `ExecStart="`+moved.Exe+`" `) {
		t.Errorf("re-install kept the old ExecStart:\n%s", body)
	}
}

// TestInstallReportsAFailingServiceManager confirms a systemctl failure is
// surfaced, and that the steps already taken are still reported - a user whose
// enable failed needs to know the unit file is on disk.
func TestInstallReportsAFailingServiceManager(t *testing.T) {
	d, r := testDeps(t)
	r.errs["systemctl --user enable --now synosync.service"] = errors.New("Failed to connect to bus")

	rep, err := Install(context.Background(), d)
	if err == nil {
		t.Fatal("Install = nil error, want the systemctl failure")
	}
	if _, statErr := os.Stat(unitPath(d)); statErr != nil {
		t.Errorf("unit was not written before the failure: %v", statErr)
	}
	if len(rep.Steps) == 0 {
		t.Error("Report carries no steps; the user cannot tell what was done")
	}
}

func TestRenderUnit(t *testing.T) {
	tmpl, err := template("synosync.service")
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	out, err := renderUnit(tmpl, "/opt/synosync")
	if err != nil {
		t.Fatalf("renderUnit: %v", err)
	}
	if n := strings.Count(out, "\nExecStart="); n != 1 {
		t.Errorf("rendered unit has %d ExecStart lines, want 1", n)
	}
	if !strings.Contains(out, `ExecStart="/opt/synosync" daemon --log-stderr`) {
		t.Error("renderUnit did not substitute the binary path")
	}

	// Only the directive is rewritten. The header comment mentions the default
	// location as prose, and mangling documentation is not this function's job.
	if !strings.Contains(out, "~/.local/bin/synosync") {
		t.Error("renderUnit rewrote the template's prose, not just the directive")
	}
}

// TestRenderUnitRefusesAChangedTemplate pins the drift guard: installing a unit
// that starts nothing (or something twice) is worse than refusing.
func TestRenderUnitRefusesAChangedTemplate(t *testing.T) {
	for name, tmpl := range map[string]string{
		"no ExecStart":  "[Service]\nType=simple\n",
		"two ExecStart": "[Service]\nExecStart=/a\nExecStart=/b\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := renderUnit(tmpl, "/opt/synosync"); err == nil {
				t.Error("renderUnit = nil error, want a refusal")
			}
		})
	}
}

func TestUninstallRemovesTheUnit(t *testing.T) {
	d, r := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}

	rep, err := Uninstall(context.Background(), d)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if _, err := os.Stat(unitPath(d)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("unit still present after uninstall: %v", err)
	}
	if !r.ran("systemctl --user disable --now synosync.service") {
		t.Errorf("uninstall did not disable the unit; ran %v", r.calls)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
}

// TestUninstallOnACleanMachine pins convergence: uninstall must succeed with
// nothing installed, so a user recovering from a half-finished install is never
// stuck arguing with an error.
func TestUninstallOnACleanMachine(t *testing.T) {
	d, r := testDeps(t)
	r.errs["systemctl --user disable --now synosync.service"] = errors.New("Unit synosync.service does not exist")

	rep, err := Uninstall(context.Background(), d)
	if err != nil {
		t.Fatalf("Uninstall on a clean machine = %v, want nil", err)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
	joined := strings.Join(rep.Steps, "\n")
	if !strings.Contains(joined, "nothing to remove") {
		t.Errorf("Steps = %q, want it to say there was nothing to remove", joined)
	}
}

func TestStatusStates(t *testing.T) {
	t.Run("not installed", func(t *testing.T) {
		d, _ := testDeps(t)
		rep, err := Status(context.Background(), d)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if rep.State != StateNotInstalled {
			t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
		}
		if len(rep.Notes) != 0 {
			t.Errorf("Notes = %v, want none for an uninstalled service", rep.Notes)
		}
	})

	cases := map[string]State{
		"active\n":       StateRunning,
		"activating\n":   StateRunning,
		"inactive\n":     StateInstalled,
		"failed\n":       StateInstalled,
		"deactivating\n": StateInstalled,
		"whatever\n":     StateUnknown,
	}
	for out, want := range cases {
		t.Run(strings.TrimSpace(out), func(t *testing.T) {
			d, r := testDeps(t)
			if err := writeFile(unitPath(d), "[Service]\n", 0o644); err != nil {
				t.Fatal(err)
			}
			r.outputs["systemctl --user is-active synosync.service"] = out
			if want != StateRunning {
				// is-active exits non-zero for every not-running answer, so the
				// classification must come from the word, not the exit code.
				r.errs["systemctl --user is-active synosync.service"] = errors.New("exit status 3")
			}
			rep, err := Status(context.Background(), d)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if rep.State != want {
				t.Errorf("is-active %q -> %q, want %q", strings.TrimSpace(out), rep.State, want)
			}
		})
	}

	t.Run("systemctl missing", func(t *testing.T) {
		d, r := testDeps(t)
		if err := writeFile(unitPath(d), "[Service]\n", 0o644); err != nil {
			t.Fatal(err)
		}
		r.errs["systemctl --user is-active synosync.service"] = errors.New("executable file not found")
		rep, err := Status(context.Background(), d)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if rep.State != StateUnknown {
			t.Errorf("State = %q, want %q - a unit on disk is not proof it runs", rep.State, StateUnknown)
		}
	})
}

// TestLingerNote covers the one prerequisite no installer can satisfy: without
// linger the user manager is torn down at logout and syncing stops silently,
// long after the install looked successful.
func TestLingerNote(t *testing.T) {
	cases := []struct {
		name, out string
		err       error
		wantNote  bool
		wantWord  string
	}{
		{"enabled", "Linger=yes\n", nil, false, ""},
		{"disabled", "Linger=no\n", nil, true, "syncing stops at logout"},
		{"loginctl absent", "", errors.New("not found"), true, "if syncing stops"},
		{"unparseable", "something else\n", nil, true, "if syncing stops"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, r := testDeps(t)
			r.outputs["loginctl show-user alice --property=Linger"] = c.out
			if c.err != nil {
				r.errs["loginctl show-user alice --property=Linger"] = c.err
			}
			notes := lingerNote(context.Background(), d)
			if c.wantNote != (len(notes) > 0) {
				t.Fatalf("notes = %v, wantNote = %v", notes, c.wantNote)
			}
			if !c.wantNote {
				return
			}
			if !strings.Contains(notes[0], c.wantWord) {
				t.Errorf("note = %q, want it to contain %q", notes[0], c.wantWord)
			}
			if !strings.Contains(notes[0], "enable-linger alice") {
				t.Errorf("note = %q, want the exact command with the user name", notes[0])
			}
		})
	}
}

func TestUnitPathIsUnderTheGivenHome(t *testing.T) {
	d, _ := testDeps(t)
	want := filepath.Join(d.Home, ".config", "systemd", "user", "synosync.service")
	if got := unitPath(d); got != want {
		t.Errorf("unitPath = %q, want %q", got, want)
	}
}

// TestRenderUnitQuotesAwkwardPaths covers the paths an unquoted ExecStart gets
// wrong. systemd splits the value on whitespace, so "/home/bob/My Tools/synosync"
// would exec "/home/bob/My" and fail 203/EXEC; and it expands "%" as a specifier
// introducer anywhere in a unit value - the template's own default relies on %h
// - so a literal "%" has to be doubled.
func TestRenderUnitQuotesAwkwardPaths(t *testing.T) {
	tmpl, err := template("synosync.service")
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	cases := map[string]string{
		"/home/bob/My Tools/synosync": `ExecStart="/home/bob/My Tools/synosync" daemon --log-stderr`,
		"/opt/100%/synosync":          `ExecStart="/opt/100%%/synosync" daemon --log-stderr`,
		`/opt/a"b/synosync`:           `ExecStart="/opt/a\"b/synosync" daemon --log-stderr`,
		`/opt/a\b/synosync`:           `ExecStart="/opt/a\\b/synosync" daemon --log-stderr`,
		"/opt/synosync":               `ExecStart="/opt/synosync" daemon --log-stderr`,
	}
	for exe, want := range cases {
		out, err := renderUnit(tmpl, exe)
		if err != nil {
			t.Errorf("renderUnit(%q): %v", exe, err)
			continue
		}
		if !strings.Contains(out, want) {
			t.Errorf("renderUnit(%q) did not produce %q", exe, want)
		}
	}
}

// TestInstallReportsTheQuotedExecStart confirms the step line a user reads shows
// what actually lands in the unit, not an unquoted approximation of it.
func TestInstallReportsTheQuotedExecStart(t *testing.T) {
	d, _ := testDeps(t)
	d.Exe = filepath.Join(d.Home, "My Tools", "synosync")
	rep, err := Install(context.Background(), d)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	joined := strings.Join(rep.Steps, "\n")
	if !strings.Contains(joined, `ExecStart="`+d.Exe+`" daemon --log-stderr`) {
		t.Errorf("Steps = %q, want the quoted ExecStart", joined)
	}
}

// TestUninstallKeepsTheUnitWhenDisableFailsForARealReason is the case that made
// uninstall lie: an SSH shell that cannot reach the user bus while a lingered
// service is still running. Removing the unit and reporting "not installed"
// there leaves the daemon running and holding the state database.
func TestUninstallKeepsTheUnitWhenDisableFailsForARealReason(t *testing.T) {
	// Both spellings of the SAME scenario. The second is the one that mattered:
	// "No such file or directory" is what systemctl prints when it cannot reach
	// the user bus, and the absent-unit matcher used to accept exactly that
	// substring - so it deleted the unit and reported "not installed" while the
	// daemon kept running and holding the state database. The original fixture
	// ("No medium found") happened to contain none of the matcher's substrings,
	// so it passed while the real message slipped through.
	buses := []string{
		"Failed to connect to bus: No medium found",
		"Failed to connect to user scope bus via local transport: No such file or directory",
		"Failed to connect to bus: No such file or directory",
	}
	for _, msg := range buses {
		t.Run(msg, func(t *testing.T) {
			d, r := testDeps(t)
			if _, err := Install(context.Background(), d); err != nil {
				t.Fatalf("Install: %v", err)
			}
			r.errs["systemctl --user disable --now synosync.service"] = errors.New(msg)

			rep, err := Uninstall(context.Background(), d)
			if err == nil {
				t.Fatal("Uninstall = nil error, want the bus failure surfaced")
			}
			if _, statErr := os.Stat(unitPath(d)); statErr != nil {
				t.Errorf("unit was removed despite the failure: %v", statErr)
			}
			if rep.State == StateNotInstalled {
				t.Error("Uninstall claimed the service is gone after a failure that did not remove it")
			}
		})
	}
}

// TestMentionsAbsentUnit pins the line between "there was nothing to disable"
// and "the manager could not be reached", which decides whether Uninstall may
// remove the unit.
func TestMentionsAbsentUnit(t *testing.T) {
	absent := []string{
		"Failed to disable unit: Unit file synosync.service does not exist.",
		"Unit synosync.service not loaded.",
	}
	for _, msg := range absent {
		if !mentionsAbsentUnit([]byte(msg), errors.New("exit status 1")) {
			t.Errorf("mentionsAbsentUnit(%q) = false, want true", msg)
		}
	}
	real := []string{
		"Failed to connect to bus: No such file or directory",
		"Failed to connect to user scope bus via local transport: No such file or directory",
		"Failed to connect to bus: No medium found",
		// Names no unit, so it is not evidence this unit is absent.
		"some other file does not exist",
	}
	for _, msg := range real {
		if mentionsAbsentUnit([]byte(msg), errors.New("exit status 1")) {
			t.Errorf("mentionsAbsentUnit(%q) = true, want false", msg)
		}
	}
}

// TestInstallCarriesTheStateEnvIntoTheUnit covers the override the manager
// cannot inherit. `systemd --user` is started by pam_systemd before any login
// shell reads a profile, so a user who paired with XDG_DATA_HOME exported would
// otherwise get a service that opens the DEFAULT state directory, finds no
// pairing and exits on a restart loop - while the install reported "running",
// because the state is queried moments after enable.
func TestInstallCarriesTheStateEnvIntoTheUnit(t *testing.T) {
	d, _ := testDeps(t)
	d.StateEnv = map[string]string{"XDG_DATA_HOME": "/srv/state dir", "XDG_STATE_HOME": "/srv/s%tate"}

	rep, err := Install(context.Background(), d)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	body, err := os.ReadFile(unitPath(d))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	text := string(body)
	// Quoted and %-doubled for the same reasons the exec path is.
	for _, want := range []string{
		`Environment="XDG_DATA_HOME=/srv/state dir"`,
		`Environment="XDG_STATE_HOME=/srv/s%%tate"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("unit lacks %q:\n%s", want, text)
		}
	}
	// Above ExecStart, so a reader sees what the unit runs with.
	if strings.Index(text, "Environment=") > strings.Index(text, "ExecStart=") {
		t.Error("Environment= lines belong above ExecStart")
	}
	if !strings.Contains(strings.Join(rep.Steps, "\n"), "XDG_DATA_HOME=/srv/state dir") {
		t.Errorf("Steps = %v, want the carried environment reported", rep.Steps)
	}
}

// TestInstallWithoutStateEnvIsUnchanged keeps the ordinary case byte-identical.
func TestInstallWithoutStateEnvIsUnchanged(t *testing.T) {
	d, _ := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	body, err := os.ReadFile(unitPath(d))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	if strings.Contains(string(body), "Environment=") {
		t.Error("a unit with no overrides gained an Environment= line")
	}
}

// TestUninstallReportsAFailingReload confirms a daemon-reload failure is not
// swallowed: the unit is gone, but systemd still lists it until something
// reloads, so the user needs to know.
func TestUninstallReportsAFailingReload(t *testing.T) {
	d, r := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	r.errs["systemctl --user daemon-reload"] = errors.New("Failed to connect to bus")

	rep, err := Uninstall(context.Background(), d)
	if err == nil {
		t.Fatal("Uninstall = nil error, want the reload failure surfaced")
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q - the unit really was removed", rep.State, StateNotInstalled)
	}
}

// TestControlVerbsDriveSystemctl pins that start, stop and restart apply their own verb to the
// installed unit. They exist because every "stop the daemon and retry" hint had no answer once
// the daemon ran as a service.
func TestControlVerbsDriveSystemctl(t *testing.T) {
	cases := map[string]func(context.Context, Deps) (Report, error){
		"start":   Start,
		"stop":    Stop,
		"restart": Restart,
	}
	for verb, fn := range cases {
		t.Run(verb, func(t *testing.T) {
			d, r := testDeps(t)
			if err := writeFile(unitPath(d), "[Unit]\n", 0o644); err != nil {
				t.Fatalf("seed unit: %v", err)
			}
			r.outputs["systemctl --user is-active "+unitName] = "active"

			rep, err := fn(context.Background(), d)
			if err != nil {
				t.Fatalf("%s: %v", verb, err)
			}
			if !r.ran("systemctl --user " + verb + " " + unitName) {
				t.Errorf("%s did not run its systemctl verb; ran %v", verb, r.calls)
			}
			// The enabled state is deliberately untouched: stopping is not disabling.
			for _, c := range r.calls {
				if strings.Contains(c, "disable") || strings.Contains(c, "enable") {
					t.Errorf("%s changed the unit's enabled state: %q", verb, c)
				}
			}
			if rep.State != StateRunning {
				t.Errorf("State = %q, want the post-verb state", rep.State)
			}
		})
	}
}

// TestControlVerbsReportAnAbsentUnit pins that a service that was never installed reports that
// rather than systemctl's own failure, whose wording buries the one fact the user needs.
func TestControlVerbsReportAnAbsentUnit(t *testing.T) {
	d, r := testDeps(t)

	rep, err := Stop(context.Background(), d)
	if !errors.Is(err, ErrNotInstalled) {
		t.Fatalf("Stop err = %v, want ErrNotInstalled", err)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
	if len(r.calls) != 0 {
		t.Errorf("an absent unit still ran %v", r.calls)
	}
}
