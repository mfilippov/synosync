package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStageScriptWritesTheEmbeddedInstaller confirms neither operation depends
// on a packaging/ directory beside the binary: a `go install` build has none, so
// the script is written out of the embedded copy - the same bytes the release
// archive ships. `uninstall` runs that copy; `install` prints its path.
func TestStageScriptWritesTheEmbeddedInstaller(t *testing.T) {
	path, cleanup, err := stageScript()
	if err != nil {
		t.Fatalf("stageScript: %v", err)
	}
	defer cleanup()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read staged script: %v", err)
	}
	embedded, err := template(scriptName)
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	if string(body) != embedded {
		t.Error("the staged script differs from the embedded template")
	}
	if !strings.HasSuffix(path, scriptName) {
		t.Errorf("staged at %q, want it to end in %q", path, scriptName)
	}

	cleanup()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("cleanup left the staged script behind: %v", err)
	}
}

// TestInstallRefusesWithoutARunAsAccount pins the refusal. Registering the
// default LocalSystem service would look successful and never sync: LocalSystem
// has its own profile and reaches neither the paired user's state database and
// keychain nor the control pipe, whose DACL names that user's SID - and the
// script's failure actions would restart the doomed service on a loop.
func TestInstallRefusesWithoutARunAsAccount(t *testing.T) {
	d, r := testDeps(t)

	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}
	if len(r.calls) != 0 {
		t.Errorf("refusal still ran %v; nothing must be registered", r.calls)
	}
	if rep.State != "" {
		t.Errorf("State = %q, want empty - nothing was installed", rep.State)
	}

	notes := strings.Join(rep.Notes, "\n")
	for _, want := range []string{
		// Qualified: a bare name resolves to the DOMAIN account in preference to the local
		// one, so the printed command would name an account the pairing does not belong to.
		`-User '.\` + d.User + "'",
		"-BinPath '" + d.Exe + "'",
		"-Password (Read-Host -AsSecureString)",
		"install-service.ps1",
		"elevated",
	} {
		if !strings.Contains(notes, want) {
			t.Errorf("Notes = %q, want them to contain %q", notes, want)
		}
	}
}

// TestInstallQuotesThePrintedCommand covers the most ordinary Windows install
// location there is. Unquoted, `-BinPath C:\Program Files\...` binds only
// "C:\Program" and treats the rest as another argument - so the single Windows
// path the CLI offers failed before it registered anything.
func TestInstallQuotesThePrintedCommand(t *testing.T) {
	d, _ := testDeps(t)
	d.Exe = `C:\Program Files\synosync\synosync.exe`
	d.User = "CORP user"

	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}
	notes := strings.Join(rep.Notes, "\n")
	for _, want := range []string{
		`-BinPath 'C:\Program Files\synosync\synosync.exe'`,
		`-User '.\CORP user'`,
	} {
		if !strings.Contains(notes, want) {
			t.Errorf("Notes = %q, want %q", notes, want)
		}
	}
}

// TestInstallNamesAScriptThatExists pins the handoff. Naming
// .\packaging\install-service.ps1 assumed the user was standing in an unpacked
// archive - a `go install` build has no such directory anywhere on the machine,
// and a release binary run through PATH from elsewhere would be handed a path
// that does not resolve. The embedded copy is written out and named instead.
func TestInstallNamesAScriptThatExists(t *testing.T) {
	d, _ := testDeps(t)
	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}

	var script string
	for _, n := range rep.Notes {
		if i := strings.Index(n, "& '"); i >= 0 {
			rest := n[i+3:]
			script = rest[:strings.Index(rest, "'")]
		}
	}
	if script == "" {
		t.Fatalf("Notes = %v, want a quoted script path", rep.Notes)
	}
	body, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("the printed script path does not resolve: %v", err)
	}
	embedded, err := template(scriptName)
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	if string(body) != embedded {
		t.Error("the printed script differs from the embedded installer")
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(script)) })
}

func TestPSQuote(t *testing.T) {
	cases := map[string]string{
		`C:\Tools\synosync.exe`:  `'C:\Tools\synosync.exe'`,
		`C:\Program Files\x.exe`: `'C:\Program Files\x.exe'`,
		`it's`:                   `'it''s'`,
		`$env:PATH`:              `'$env:PATH'`,
	}
	for in, want := range cases {
		if got := psQuote(in); got != want {
			t.Errorf("psQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestInstallNamesAPlaceholderWhenTheUserIsUnknown keeps the printed command
// pasteable even when the account could not be resolved.
func TestInstallNamesAPlaceholderWhenTheUserIsUnknown(t *testing.T) {
	d, _ := testDeps(t)
	d.User = ""
	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}
	// Quoted like every other value in the printed command.
	if !strings.Contains(strings.Join(rep.Notes, "\n"), "-User '<account>'") {
		t.Errorf("Notes = %v, want a quoted placeholder account", rep.Notes)
	}
}

func TestUninstallDrivesTheShippedScript(t *testing.T) {
	d, r := testDeps(t)
	rep, err := Uninstall(context.Background(), d)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if len(rep.Steps) == 0 {
		t.Error("a successful uninstall recorded no step")
	}
	var invoked string
	for _, c := range r.calls {
		if strings.HasPrefix(c, "powershell ") {
			invoked = c
		}
	}
	if !strings.Contains(invoked, "-Action uninstall") {
		t.Errorf("invocation %q does not uninstall", invoked)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
}

func TestStatusStates(t *testing.T) {
	cases := []struct {
		name string
		out  string
		err  error
		want State
	}{
		{"running", "SERVICE_NAME: synosync\n STATE : 4  RUNNING", nil, StateRunning},
		{"stopped", "SERVICE_NAME: synosync\n STATE : 1  STOPPED", nil, StateInstalled},
		{"start pending", "STATE : 2  START_PENDING", nil, StateInstalled},
		{"absent", "FAILED 1060: The specified service does not exist", errors.New("exit status 1060"), StateNotInstalled},
		{"query refused", "", errors.New("exit status 5"), StateUnknown},
		{"unclassifiable", "something else", nil, StateUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, r := testDeps(t)
			r.outputs["sc.exe query "+serviceName] = c.out
			if c.err != nil {
				r.errs["sc.exe query "+serviceName] = c.err
			}
			rep, err := Status(context.Background(), d)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if !r.ran("sc.exe query " + serviceName) {
				t.Errorf("status did not query sc.exe; ran %v", r.calls)
			}
			if rep.State != c.want {
				t.Errorf("State = %q, want %q", rep.State, c.want)
			}
		})
	}
}

func TestNeedsElevation(t *testing.T) {
	yes := []string{
		// What install-service.ps1's Assert-Admin throws - the case that
		// actually occurs, and the one the old matcher missed.
		"This action requires an elevated (Administrator) PowerShell session.",
		"Access is denied.",
		"ACCESS IS DENIED",
		"sc failed with error 5",
	}
	for _, y := range yes {
		if !needsElevation([]byte(y)) {
			t.Errorf("needsElevation(%q) = false, want true", y)
		}
	}
	for _, no := range []string{"", "error 1060", "service created"} {
		if needsElevation([]byte(no)) {
			t.Errorf("needsElevation(%q) = true, want false", no)
		}
	}
}

// TestInstallWarnsAboutAnUninheritedStateEnv covers the Windows half of the
// state-directory problem. The SCM takes no environment from this process and
// install-service.ps1 has no way to set one, so a user who paired with
// XDG_DATA_HOME set must be told, or the service silently resolves the default
// state directory and finds no pairing.
func TestInstallWarnsAboutAnUninheritedStateEnv(t *testing.T) {
	d, _ := testDeps(t)
	d.StateEnv = map[string]string{"XDG_DATA_HOME": `D:\state`}

	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}
	notes := strings.Join(rep.Notes, "\n")
	if !strings.Contains(notes, `XDG_DATA_HOME=D:\state`) {
		t.Errorf("Notes = %q, want the override named", notes)
	}
	if !strings.Contains(notes, "will NOT inherit") {
		t.Errorf("Notes = %q, want the SCM's blindness explained", notes)
	}
}

// TestInstallKeepsADomainQualifiedAccount pins that a domain account is printed as the host
// spells it, not rewritten to the local form.
//
// The two halves matter for opposite reasons. A bare "alice" resolves through NTAccount to the
// DOMAIN account in preference to the local one, so the printed command could name an account
// holding none of this pairing - hence the ".\" for an unqualified name. But rewriting
// "CORP\alice" to ".\alice" would make the same mistake in reverse, naming a local account that
// is not the paired one.
func TestInstallKeepsADomainQualifiedAccount(t *testing.T) {
	d, _ := testDeps(t)
	d.User = "alice"
	d.QualifiedUser = `CORP\alice`

	rep, err := Install(context.Background(), d)
	if !errors.Is(err, ErrNeedsRunAsAccount) {
		t.Fatalf("Install err = %v, want ErrNeedsRunAsAccount", err)
	}
	notes := strings.Join(rep.Notes, "\n")
	if !strings.Contains(notes, `-User 'CORP\alice'`) {
		t.Errorf("Notes = %q, want the domain-qualified account preserved", notes)
	}
	if strings.Contains(notes, `.\alice`) {
		t.Errorf("Notes rewrote a domain account to the local form:\n%s", notes)
	}
}

// TestControlVerbsDriveTheScript pins that start, stop and restart reach the installer script
// with their own action. They exist because every "stop the daemon and retry" hint had no answer
// once the daemon ran as a service.
func TestControlVerbsDriveTheScript(t *testing.T) {
	cases := map[string]func(context.Context, Deps) (Report, error){
		"start":   Start,
		"stop":    Stop,
		"restart": Restart,
	}
	for verb, fn := range cases {
		t.Run(verb, func(t *testing.T) {
			d, r := testDeps(t)
			r.outputs["sc.exe query "+serviceName] = "SERVICE_NAME: synosync\n STATE : 4  RUNNING"

			rep, err := fn(context.Background(), d)
			if err != nil {
				t.Fatalf("%s: %v", verb, err)
			}
			var invoked string
			for _, c := range r.calls {
				if strings.HasPrefix(c, "powershell ") {
					invoked = c
				}
			}
			if !strings.Contains(invoked, "-Action "+verb) {
				t.Errorf("invocation %q does not carry -Action %s", invoked, verb)
			}
			if len(rep.Steps) == 0 {
				t.Error("a successful control action recorded no step")
			}
		})
	}
}

// TestControlVerbsReportNotInstalled pins that an absent service reports that fact rather than
// the script's own failure, whose wording differs per platform and buries the one thing the
// user needs to know.
func TestControlVerbsReportNotInstalled(t *testing.T) {
	d, r := testDeps(t)
	r.outputs["sc.exe query "+serviceName] = "FAILED 1060: The specified service does not exist"
	r.errs["sc.exe query "+serviceName] = errors.New("exit status 1060")

	rep, err := Stop(context.Background(), d)
	if !errors.Is(err, ErrNotInstalled) {
		t.Fatalf("Stop err = %v, want ErrNotInstalled", err)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
	for _, c := range r.calls {
		if strings.HasPrefix(c, "powershell ") {
			t.Errorf("the script was run against an absent service: %q", c)
		}
	}
}

// TestControlVerbsCarryTheElevationNote pins that an access-denied answer explains itself.
// Controlling a service is an administrative right unless the DACL grants it, and sc.exe's
// "Access is denied" is not obviously about elevation.
func TestControlVerbsCarryTheElevationNote(t *testing.T) {
	d, r := testDeps(t)
	r.outputs["sc.exe query "+serviceName] = "SERVICE_NAME: synosync\n STATE : 1  STOPPED"
	// byName, because the script is invoked with a freshly staged temp path baked into its
	// arguments and the full command line is not predictable here.
	r.byName = func(name string) ([]byte, error) {
		if name == "powershell" {
			return []byte("Access is denied."), errors.New("exit status 5")
		}
		return nil, nil
	}

	rep, err := Start(context.Background(), d)
	if err == nil {
		t.Fatal("Start succeeded despite an access-denied answer")
	}
	if len(rep.Notes) == 0 || !strings.Contains(strings.Join(rep.Notes, "\n"), "administrator") {
		t.Errorf("Notes = %v, want the elevation note", rep.Notes)
	}
}
