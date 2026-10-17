package main

import (
	"bytes"
	"runtime/debug"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %q)", code, stderr.String())
	}
	want := versionLine() + "\n"
	if !strings.HasPrefix(want, "synosync "+buildVersion()+" (go") {
		t.Errorf("version line %q does not lead with the version and toolchain", want)
	}
	if got := stdout.String(); got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Errorf("stderr = %q, want usage text", stderr.String())
	}
}

func TestRunUnknownSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"frobnicate"}, &stdout, &stderr)
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown subcommand") {
		t.Errorf("stderr = %q, want unknown subcommand error", stderr.String())
	}
}

func TestRunHelp(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		var stdout, stderr bytes.Buffer
		code := run([]string{arg}, &stdout, &stderr)
		if code != 0 {
			t.Errorf("%s: exit code = %d, want 0", arg, code)
		}
		if !strings.Contains(stdout.String(), "Subcommands:") {
			t.Errorf("%s: stdout = %q, want subcommands listing", arg, stdout.String())
		}
	}
}

// TestVersionFlagAliases pins that --version and -V work. Before, they were indistinguishable
// from a typo: the dispatcher dumped the whole usage page to stderr and exited 2, on the one
// flag nearly every command-line tool accepts.
func TestVersionFlagAliases(t *testing.T) {
	for _, arg := range []string{"--version", "-V"} {
		t.Run(arg, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{arg}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if got := stdout.String(); got != versionLine()+"\n" {
				t.Errorf("stdout = %q, want the version line", got)
			}
		})
	}
}

// TestVersionFlagRejectsTrailingArguments pins that the alias is as strict as the subcommand;
// silently ignoring an argument is how a mistyped command looks like it worked.
func TestVersionFlagRejectsTrailingArguments(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--version", "extra"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "takes no positional arguments") {
		t.Errorf("stderr = %q, want the usage error", stderr.String())
	}
}

// TestResolveVersionPrefersTheStampThenTheModule: a release stamps the version; `go install
// module@v0.1.0` stamps nothing but records the module version; a build without VCS information
// records "(devel)", which keeps the default.
func TestResolveVersionPrefersTheStampThenTheModule(t *testing.T) {
	info := func(v string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Main: debug.Module{Version: v}}, true }
	}
	none := func() (*debug.BuildInfo, bool) { return nil, false }
	cases := []struct {
		stamped string
		read    func() (*debug.BuildInfo, bool)
		want    string
	}{
		{"v0.1.0", info("v0.2.0"), "v0.1.0"},
		{"0.0.0-dev", info("v0.1.0"), "v0.1.0"},
		{"0.0.0-dev", info("(devel)"), "0.0.0-dev"},
		{"0.0.0-dev", none, "0.0.0-dev"},
	}
	for _, c := range cases {
		if got := resolveVersion(c.stamped, c.read); got != c.want {
			t.Errorf("resolveVersion(%q) = %q, want %q", c.stamped, got, c.want)
		}
	}
}
