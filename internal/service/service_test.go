package service

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeRunner records the commands the platform code would have run and replays
// canned answers, so no test reaches systemctl, launchctl or sc.exe. The key is
// the whole command line, which is also what the assertions read: a test that
// says the install ran `systemctl --user enable --now synosync.service` is
// pinning the exact invocation, not just that something was run.
type fakeRunner struct {
	calls   []string
	outputs map[string]string
	errs    map[string]error
	// byName answers on the program name alone, for a command whose full line
	// is not predictable in advance: the Windows installer is invoked with a
	// freshly staged temp script path baked into its arguments.
	byName func(name string) ([]byte, error)
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{outputs: map[string]string{}, errs: map[string]error{}}
}

func (f *fakeRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.TrimSpace(name + " " + strings.Join(args, " "))
	f.calls = append(f.calls, key)
	if f.byName != nil {
		if out, err := f.byName(name); err != nil {
			return out, err
		}
	}
	return []byte(f.outputs[key]), f.errs[key]
}

// ran reports whether the exact command line was invoked.
func (f *fakeRunner) ran(cmd string) bool {
	for _, c := range f.calls {
		if c == cmd {
			return true
		}
	}
	return false
}

// testDeps builds Deps rooted at a temp home with a recording runner.
func testDeps(t *testing.T) (Deps, *fakeRunner) {
	t.Helper()
	home := t.TempDir()
	r := newFakeRunner()
	exe := filepath.Join(home, "bin", "synosync")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	return Deps{Exe: exe, Home: home, User: "alice", Run: r.run}, r
}

func TestDepsValidate(t *testing.T) {
	ok, _ := testDeps(t)
	if err := ok.validate(); err != nil {
		t.Fatalf("validate = %v, want nil", err)
	}

	cases := map[string]func(Deps) Deps{
		"no binary path":  func(d Deps) Deps { d.Exe = ""; return d },
		"relative binary": func(d Deps) Deps { d.Exe = "synosync"; return d },
		"no home":         func(d Deps) Deps { d.Home = ""; return d },
		"no runner":       func(d Deps) Deps { d.Run = nil; return d },
	}
	for name, mangle := range cases {
		t.Run(name, func(t *testing.T) {
			if err := mangle(ok).validate(); err == nil {
				t.Error("validate = nil, want an error")
			}
		})
	}
}

// TestOperationsRejectInvalidDeps confirms the guard runs before anything
// touches the filesystem: an install with no binary path must not write a unit
// naming "" as its ExecStart.
func TestOperationsRejectInvalidDeps(t *testing.T) {
	d, r := testDeps(t)
	d.Exe = ""
	for name, op := range map[string]func(context.Context, Deps) (Report, error){
		"install":   Install,
		"uninstall": Uninstall,
		"status":    Status,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := op(context.Background(), d); err == nil {
				t.Error("op = nil error, want a validation error")
			}
		})
	}
	if len(r.calls) != 0 {
		t.Errorf("ran %v before validating", r.calls)
	}
}

// TestTemplatesAreEmbedded confirms every template the platform code asks for is
// in the binary. Losing one to a moved file would otherwise surface only when a
// user ran `service install`.
func TestTemplatesAreEmbedded(t *testing.T) {
	for _, name := range []string{
		"synosync.service",
		"me.filippov.synosync.plist",
		"install-service.ps1",
	} {
		body, err := template(name)
		if err != nil {
			t.Errorf("template(%q): %v", name, err)
			continue
		}
		if len(body) < 200 {
			t.Errorf("template(%q) is %d bytes, want a real file", name, len(body))
		}
	}
	if _, err := template("no-such-template"); err == nil {
		t.Error("template of a missing file = nil error, want an error")
	}
}

// TestDevelopmentSelfTestIsNotShipped pins that install-service_test.ps1 stays
// out of the binary: it is a development self-test, and the embed list names
// files individually to keep it out.
func TestDevelopmentSelfTestIsNotShipped(t *testing.T) {
	if _, err := template("install-service_test.ps1"); err == nil {
		t.Error("the PowerShell self-test is embedded in the binary; it should not be")
	}
}

func TestTildeHome(t *testing.T) {
	sep := string(filepath.Separator)
	home := filepath.Join(sep+"home", "alice")
	cases := []struct{ in, want string }{
		{filepath.Join(home, ".config", "x"), "~" + sep + filepath.Join(".config", "x")},
		{home, home}, // the home itself is not shortened
		{filepath.Join(sep+"home", "alicia", "x"), ""}, // a longer sibling must not match
		{filepath.Join(sep+"etc", "x"), ""},            // unrelated path
	}
	for _, c := range cases {
		want := c.want
		if want == "" {
			want = c.in
		}
		if got := tildeHome(c.in, home); got != want {
			t.Errorf("tildeHome(%q) = %q, want %q", c.in, got, want)
		}
	}
	if got := tildeHome("/etc/x", ""); got != "/etc/x" {
		t.Errorf("tildeHome with no home = %q, want the input", got)
	}
}

func TestTrimOutput(t *testing.T) {
	if got := trimOutput([]byte("boom\n\n")); got != "boom" {
		t.Errorf("trimOutput = %q, want %q", got, "boom")
	}
	long := trimOutput([]byte(strings.Repeat("x", 900)))
	if len([]rune(long)) > 401 {
		t.Errorf("trimOutput returned %d runes, want it bounded", len([]rune(long)))
	}
	if !strings.HasSuffix(long, "…") {
		t.Errorf("trimOutput = %q, want a truncation marker", long)
	}
}

// TestValidateRejectsALineBreakPath covers the one path shape neither unit
// format can carry: systemd would treat the newline as the end of the directive
// and the plist could not round-trip it, so a unit that silently starts
// something else would be written. Refusing is the only safe answer.
func TestValidateRejectsALineBreakPath(t *testing.T) {
	d, _ := testDeps(t)
	for _, bad := range []string{"/opt/syn\nosync", "/opt/syn\rosync", "/opt/syn\x00osync"} {
		d.Exe = bad
		if err := d.validate(); err == nil {
			t.Errorf("validate(%q) = nil, want a refusal", bad)
		}
	}
}

// TestDefaultDeps confirms the real resolver returns something usable: an
// absolute binary path (a unit file cannot hold a relative one) and a home.
func TestDefaultDeps(t *testing.T) {
	d, err := DefaultDeps()
	if err != nil {
		t.Fatalf("DefaultDeps: %v", err)
	}
	if !filepath.IsAbs(d.Exe) {
		t.Errorf("Exe = %q, want an absolute path", d.Exe)
	}
	if d.Home == "" || d.User == "" || d.Run == nil {
		t.Errorf("DefaultDeps left a field empty: %+v", d)
	}
	if strings.Contains(d.User, `\`) {
		t.Errorf("User = %q, want the bare login name", d.User)
	}
	if err := d.validate(); err != nil {
		t.Errorf("DefaultDeps produced deps that fail validate: %v", err)
	}
}

// TestRunCommandReportsFailure drives the real runner against a command that
// cannot exist, so the error path - the one that carries a manager's own
// diagnostic back to the user - is exercised without a service manager.
func TestRunCommandReportsFailure(t *testing.T) {
	_, err := runCommand(context.Background(), "synosync-no-such-command-exists")
	if err == nil {
		t.Fatal("runCommand of a missing binary = nil error")
	}
	if !strings.Contains(err.Error(), "synosync-no-such-command-exists") {
		t.Errorf("err = %v, want it to name the command", err)
	}
}

// TestErrUnsupportedIsComparable pins that the CLI can branch on the sentinel.
func TestErrUnsupportedIsComparable(t *testing.T) {
	if !errors.Is(ErrUnsupported, ErrUnsupported) {
		t.Error("ErrUnsupported is not comparable with errors.Is")
	}
}
