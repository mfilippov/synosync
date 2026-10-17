package service

import (
	"context"
	"encoding/xml"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInstallWritesTheAgentWithRealPaths is the reason this package exists on
// macOS: launchd expands neither ~ nor $HOME, so the shipped template has to
// carry a literal USERNAME and a guessed /usr/local/bin path for hand installs.
// Install must substitute both.
func TestInstallWritesTheAgentWithRealPaths(t *testing.T) {
	d, r := testDeps(t)
	r.outputs["launchctl list "+agentLabel] = "{\n\t\"Label\" = \"me.filippov.synosync\";\n\t\"PID\" = 4242;\n}\n"

	rep, err := Install(context.Background(), d)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	body, err := os.ReadFile(agentPath(d))
	if err != nil {
		t.Fatalf("read agent: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "<string>"+d.Exe+"</string>") {
		t.Errorf("agent does not run this binary:\n%s", text)
	}
	// The ELEMENT must be substituted; the header prose deliberately keeps the
	// placeholder, because rendering is anchored to <string> elements and never
	// rewrites the template's documentation (TestRenderAgentLeavesTheHeaderComment
	// Alone covers why: splicing a path into an XML comment can make the document
	// ill-formed, and escaping cannot prevent it).
	if strings.Contains(text, "<string>"+templateExe+"</string>") {
		t.Error("the ProgramArguments entry still carries the placeholder binary path")
	}
	if !strings.Contains(text, "not at "+templateExe) {
		t.Error("rendering rewrote the header comment; only elements should be")
	}
	// The functional placeholder is the PATH, not the word: the template also
	// says "replace USERNAME" three times in its header, and rewriting prose is
	// not this code's job (the systemd unit is treated the same way).
	if strings.Contains(text, templateLogDir) {
		t.Errorf("agent still logs into the placeholder path %q:\n%s", templateLogDir, text)
	}
	wantLogs := filepath.Join(d.Home, "Library", "Logs") + "/"
	if !strings.Contains(text, wantLogs) {
		t.Errorf("agent log paths are not under this home; want %q:\n%s", wantLogs, text)
	}
	if !strings.Contains(text, "launchd does not expand ~") {
		t.Error("rendering mangled the template's documentation, not just its paths")
	}
	// The template's reasoning must survive - the throttle bound and the label
	// are what a reader needs to understand the agent they just installed.
	for _, keep := range []string{"<key>ThrottleInterval</key>", agentLabel} {
		if !strings.Contains(text, keep) {
			t.Errorf("agent lost %q from the template", keep)
		}
	}

	if !r.ran("launchctl load -w " + agentPath(d)) {
		t.Errorf("install did not load the agent; ran %v", r.calls)
	}
	if rep.State != StateRunning {
		t.Errorf("State = %q, want %q", rep.State, StateRunning)
	}
}

// TestInstallUnloadsBeforeLoading pins the re-install path: launchctl refuses to
// load an already-loaded label, so an install onto a moved binary would fail on
// the very step meant to pick the move up.
func TestInstallUnloadsBeforeLoading(t *testing.T) {
	d, r := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	unload := "launchctl unload -w " + agentPath(d)
	load := "launchctl load -w " + agentPath(d)

	var unloadAt, loadAt = -1, -1
	for i, c := range r.calls {
		if c == unload && unloadAt < 0 {
			unloadAt = i
		}
		if c == load && loadAt < 0 {
			loadAt = i
		}
	}
	if unloadAt < 0 || loadAt < 0 || unloadAt > loadAt {
		t.Errorf("calls = %v, want an unload before the load", r.calls)
	}
}

// TestInstallSurvivesAnUnloadThatWasNotLoaded confirms the pre-emptive unload's
// failure is ignored: on a first install there is nothing to unload.
func TestInstallSurvivesAnUnloadThatWasNotLoaded(t *testing.T) {
	d, r := testDeps(t)
	r.errs["launchctl unload -w "+agentPath(d)] = errors.New("Could not find specified service")
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
}

func TestRenderAgent(t *testing.T) {
	tmpl, err := template(agentFile)
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	out, err := renderAgent(tmpl, "/opt/synosync", "/Users/bob", nil)
	if err != nil {
		t.Fatalf("renderAgent: %v", err)
	}
	for _, want := range []string{"<string>/opt/synosync</string>", "/Users/bob/Library/Logs/"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered agent lacks %q", want)
		}
	}
	if strings.Contains(out, templateLogDir) {
		t.Errorf("rendered agent still logs into %q", templateLogDir)
	}
	if strings.Contains(out, "<string>"+templateExe+"</string>") {
		t.Error("rendered agent still runs the template's placeholder binary")
	}
}

// TestRenderAgentRefusesAChangedTemplate pins the drift guard: an agent that
// runs the wrong binary, or logs into somebody else's home, is worse than a
// refusal.
func TestRenderAgentRefusesAChangedTemplate(t *testing.T) {
	logElem := "<string>" + templateLogDir + "synosync.out.log</string>"
	cases := map[string]string{
		"no binary element":   logElem + logElem,
		"no log elements":     "<string>" + templateExe + "</string>",
		"two binary elements": "<string>" + templateExe + "</string><string>" + templateExe + "</string>" + logElem + logElem,
		// The bare path in prose must NOT satisfy the guard: only an element
		// counts, which is the whole point of anchoring.
		"binary only in prose": "<!-- " + templateExe + " -->" + logElem + logElem,
	}
	for name, tmpl := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := renderAgent(tmpl, "/opt/synosync", "/Users/bob", nil); err == nil {
				t.Error("renderAgent = nil error, want a refusal")
			}
		})
	}
}

func TestUninstallRemovesTheAgent(t *testing.T) {
	d, r := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	rep, err := Uninstall(context.Background(), d)
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if _, err := os.Stat(agentPath(d)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("agent still present after uninstall: %v", err)
	}
	if !r.ran("launchctl unload -w " + agentPath(d)) {
		t.Errorf("uninstall did not unload; ran %v", r.calls)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
}

// TestUninstallOnACleanMachine pins convergence from any starting state.
func TestUninstallOnACleanMachine(t *testing.T) {
	d, r := testDeps(t)
	r.errs["launchctl unload -w "+agentPath(d)] = errors.New("Could not find specified service")

	rep, err := Uninstall(context.Background(), d)
	if err != nil {
		t.Fatalf("Uninstall on a clean machine = %v, want nil", err)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
	if !strings.Contains(strings.Join(rep.Steps, "\n"), "nothing to remove") {
		t.Errorf("Steps = %v, want it to say there was nothing to remove", rep.Steps)
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
	})

	// `launchctl list <label>` prints a plist-style dict in which the PID key is
	// present ONLY while the job runs; an idle job carries LastExitStatus and no
	// PID line. The "-" placeholder these fixtures used to carry belongs to the
	// TABULAR `launchctl list` with no argument - a different command - so the
	// old idle fixture was a shape launchd never emits, and it hid the real idle
	// case falling through to "state unknown".
	cases := []struct {
		name    string
		out     string
		listErr error
		want    State
	}{
		{"running", "{\n\t\"Label\" = \"me.filippov.synosync\";\n\t\"PID\" = 4242;\n\t\"LastExitStatus\" = 0;\n}\n", nil, StateRunning},
		{"loaded but idle", "{\n\t\"Label\" = \"me.filippov.synosync\";\n\t\"LastExitStatus\" = 0;\n\t\"OnDemand\" = false;\n}\n", nil, StateInstalled},
		{"idle after a crash", "{\n\t\"Label\" = \"me.filippov.synosync\";\n\t\"LastExitStatus\" = 256;\n}\n", nil, StateInstalled},
		{"not loaded", "", errors.New("Could not find service"), StateInstalled},
		{"answered nothing", "   \n", nil, StateUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, r := testDeps(t)
			if err := writeFile(agentPath(d), "<plist/>", 0o644); err != nil {
				t.Fatal(err)
			}
			r.outputs["launchctl list "+agentLabel] = c.out
			if c.listErr != nil {
				r.errs["launchctl list "+agentLabel] = c.listErr
			}
			rep, err := Status(context.Background(), d)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if rep.State != c.want {
				t.Errorf("State = %q, want %q", rep.State, c.want)
			}
		})
	}
}

func TestAgentPathIsUnderTheGivenHome(t *testing.T) {
	d, _ := testDeps(t)
	want := filepath.Join(d.Home, "Library", "LaunchAgents", agentFile)
	if got := agentPath(d); got != want {
		t.Errorf("agentPath = %q, want %q", got, want)
	}
}

// TestRenderAgentEscapesXML covers the paths that produce a plist launchctl
// refuses to parse. Both values land inside <string> elements, so a bare & or <
// in a home or binary path makes the document ill-formed and `service install`
// leaves a permanently broken agent on disk.
func TestRenderAgentEscapesXML(t *testing.T) {
	tmpl, err := template(agentFile)
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	out, err := renderAgent(tmpl, "/Users/bob/Dev & Tools/synosync", "/Users/b<o>b", nil)
	if err != nil {
		t.Fatalf("renderAgent: %v", err)
	}
	for _, want := range []string{
		"<string>/Users/bob/Dev &amp; Tools/synosync</string>",
		"/Users/b&lt;o&gt;b/Library/Logs/",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered agent lacks %q:\n%s", want, out)
		}
	}
	// The proof that matters: the result still parses as XML.
	if err := xml.Unmarshal([]byte(out), new(struct {
		XMLName xml.Name
	})); err != nil {
		t.Errorf("rendered agent is not well-formed XML: %v", err)
	}
}

// TestRenderAgentLeavesTheHeaderCommentAlone is the comment-injection case. The
// template names the placeholder binary path in its header prose too, and a
// blanket ReplaceAll spliced the real path in there as well. XML forbids "--"
// inside a comment and escaping cannot help, since a hyphen is not a
// metacharacter - so a binary under /Users/bob/dev--tools produced an ill-formed
// plist and a permanently broken agent, and one containing "-->" closed the
// comment early.
func TestRenderAgentLeavesTheHeaderCommentAlone(t *testing.T) {
	tmpl, err := template(agentFile)
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	for _, exe := range []string{
		"/Users/bob/dev--tools/synosync",
		"/Users/bob/x-->y/synosync",
		"/Users/bob/plain/synosync",
	} {
		t.Run(exe, func(t *testing.T) {
			out, err := renderAgent(tmpl, exe, "/Users/bob", nil)
			if err != nil {
				t.Fatalf("renderAgent: %v", err)
			}
			if err := xml.Unmarshal([]byte(out), new(struct{ XMLName xml.Name })); err != nil {
				t.Fatalf("rendered agent is not well-formed XML: %v", err)
			}
			if !strings.Contains(out, "<string>"+xmlText(exe)+"</string>") {
				t.Error("the ProgramArguments entry was not substituted")
			}
			// The prose keeps the template's own wording, as on Linux.
			if !strings.Contains(out, "not at "+templateExe) {
				t.Error("the header comment was rewritten; only elements should be")
			}
		})
	}
}

// TestInstallCarriesTheStateEnvIntoTheAgent covers the override launchd cannot
// inherit: it starts agents with a minimal environment, so a user who paired
// with XDG_DATA_HOME set would otherwise get an agent that opens the DEFAULT
// state directory, finds no pairing and exits under KeepAlive.
func TestInstallCarriesTheStateEnvIntoTheAgent(t *testing.T) {
	d, _ := testDeps(t)
	d.StateEnv = map[string]string{"XDG_DATA_HOME": "/srv/state & more"}

	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	body, err := os.ReadFile(agentPath(d))
	if err != nil {
		t.Fatalf("read agent: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"<key>EnvironmentVariables</key>",
		"<key>XDG_DATA_HOME</key>",
		"<string>/srv/state &amp; more</string>",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("agent lacks %q:\n%s", want, text)
		}
	}
	if err := xml.Unmarshal([]byte(text), new(struct{ XMLName xml.Name })); err != nil {
		t.Errorf("agent with an environment dict is not well-formed XML: %v", err)
	}
}

// TestInstallWithoutStateEnvIsUnchanged keeps the ordinary case untouched.
func TestInstallWithoutStateEnvIsUnchanged(t *testing.T) {
	d, _ := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	body, err := os.ReadFile(agentPath(d))
	if err != nil {
		t.Fatalf("read agent: %v", err)
	}
	if strings.Contains(string(body), "EnvironmentVariables") {
		t.Error("an agent with no overrides gained an environment dict")
	}
}

// TestUninstallKeepsThePlistWhenUnloadFailsForARealReason mirrors the systemd
// guard: a transient or permission failure must not be followed by removing the
// plist and reporting "not installed", leaving the agent loaded and the daemon
// running while every later status call says it is gone.
func TestUninstallKeepsThePlistWhenUnloadFailsForARealReason(t *testing.T) {
	d, r := testDeps(t)
	if _, err := Install(context.Background(), d); err != nil {
		t.Fatalf("Install: %v", err)
	}
	r.errs["launchctl unload -w "+agentPath(d)] = errors.New("Operation not permitted")

	rep, err := Uninstall(context.Background(), d)
	if err == nil {
		t.Fatal("Uninstall = nil error, want the unload failure surfaced")
	}
	if _, statErr := os.Stat(agentPath(d)); statErr != nil {
		t.Errorf("plist was removed despite the failure: %v", statErr)
	}
	if rep.State == StateNotInstalled {
		t.Error("Uninstall claimed the agent is gone after a failure that did not remove it")
	}
}

// TestControlVerbsDriveLaunchctl pins that start, stop and restart apply their own verb to the
// installed agent, and - the part that matters - do it WITHOUT -w.
//
// The flag writes the job's disabled state, so a stop carrying it would also cancel the login
// autostart. Stopping is not disabling, and anyone who stops the agent to change a setting
// expects it back at the next login.
func TestControlVerbsDriveLaunchctl(t *testing.T) {
	cases := map[string][]string{
		"start":   {"load"},
		"stop":    {"unload"},
		"restart": {"unload", "load"},
	}
	fns := map[string]func(context.Context, Deps) (Report, error){
		"start": Start, "stop": Stop, "restart": Restart,
	}
	for verb, want := range cases {
		t.Run(verb, func(t *testing.T) {
			d, r := testDeps(t)
			path := agentPath(d)
			if err := writeFile(path, "<plist/>\n", 0o644); err != nil {
				t.Fatalf("seed agent: %v", err)
			}

			if _, err := fns[verb](context.Background(), d); err != nil {
				t.Fatalf("%s: %v", verb, err)
			}
			for _, w := range want {
				if !r.ran("launchctl " + w + " " + path) {
					t.Errorf("%s did not run launchctl %s; ran %v", verb, w, r.calls)
				}
			}
			for _, c := range r.calls {
				if strings.Contains(c, " -w ") {
					t.Errorf("%s passed -w, which also writes the disabled state: %q", verb, c)
				}
			}
		})
	}
}

// TestControlVerbsReportAnAbsentAgent pins that an agent that was never installed reports that
// rather than launchctl's own failure.
func TestControlVerbsReportAnAbsentAgent(t *testing.T) {
	d, r := testDeps(t)

	rep, err := Restart(context.Background(), d)
	if !errors.Is(err, ErrNotInstalled) {
		t.Fatalf("Restart err = %v, want ErrNotInstalled", err)
	}
	if rep.State != StateNotInstalled {
		t.Errorf("State = %q, want %q", rep.State, StateNotInstalled)
	}
	if len(r.calls) != 0 {
		t.Errorf("an absent agent still ran %v", r.calls)
	}
}
