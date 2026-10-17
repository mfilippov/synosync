package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

// allHelpSpecs is every subcommand help page. TestEverySubcommandHasAHelpPage
// pairs it against the registry in main.go, so a new subcommand that ships
// without a page - the state all twelve were in - fails the build rather than
// silently printing flag.FlagSet's "Usage of <cmd>:" and nothing else.
func allHelpSpecs() []helpSpec {
	return []helpSpec{
		versionHelp, daemonHelp, pairHelp, loginHelp, statusHelp, syncHelp,
		pauseHelp, resumeHelp, conflictsHelp, dropsHelp, excludeHelp, configHelp, logHelp, unlinkHelp,
		serviceHelp,
	}
}

// TestEverySubcommandHasAHelpPage confirms the registry and the help pages
// describe the same set of subcommands under the same summaries. A drifted
// summary would make `synosync help` and `synosync <cmd> --help` disagree about
// what a command does.
func TestEverySubcommandHasAHelpPage(t *testing.T) {
	specs := make(map[string]helpSpec)
	for _, s := range allHelpSpecs() {
		if _, dup := specs[s.name]; dup {
			t.Fatalf("two help pages named %q", s.name)
		}
		specs[s.name] = s
	}

	for _, c := range commands() {
		s, ok := specs[c.name]
		if !ok {
			t.Errorf("subcommand %q has no help page; add one to its command file", c.name)
			continue
		}
		if s.summary != c.summary {
			t.Errorf("%s: help summary %q != registry summary %q", c.name, s.summary, c.summary)
		}
		if len(s.usage) == 0 {
			t.Errorf("%s: help page has no usage line", c.name)
		}
		if strings.TrimSpace(s.describe) == "" {
			t.Errorf("%s: help page has no description", c.name)
		}
		delete(specs, c.name)
	}
	for name := range specs {
		t.Errorf("help page %q describes no registered subcommand", name)
	}
}

// TestHelpPagesRenderTheirParts drives every subcommand's real --help through
// run() and checks the page carries the parts the format promises. It is the
// regression guard for the four commands whose whole help output used to be the
// single line "Usage of status:".
func TestHelpPagesRenderTheirParts(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, c := range commands() {
		var stdout, stderr bytes.Buffer
		if code := run([]string{c.name, "--help"}, &stdout, &stderr); code != 0 {
			t.Errorf("%s --help exit = %d, want 0", c.name, code)
			continue
		}
		out := stdout.String()
		for _, want := range []string{
			"synosync " + c.name + " - " + c.summary,
			"Usage:\n  synosync " + c.name,
			"Examples:\n  synosync ",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("%s --help = %q, want it to contain %q", c.name, out, want)
			}
		}
		// A page that is only a title and a usage line is the failure this
		// format exists to prevent.
		if lines := strings.Count(out, "\n"); lines < 8 {
			t.Errorf("%s --help is only %d lines; the page carries no real description", c.name, lines)
		}
	}
}

// TestHelpPagesSpellFlagsWithTwoDashes pins the presentation the README uses and
// flag.PrintDefaults does not: a multi-character flag reads --host, never -host.
// The two spellings both parse, so this is about the page agreeing with the docs
// and with every error message in the package.
func TestHelpPagesSpellFlagsWithTwoDashes(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	cases := map[string][]string{
		"pair":   {"--host <host>", "--port <port>", "--user <name>", "--sync-root <path>", "--expect-pin <pin>"},
		"login":  {"--host <host>", "--port <port>", "--user <name>"},
		"daemon": {"--log-stderr"},
		"unlink": {"--yes"},
	}
	for cmd, wants := range cases {
		var stdout, stderr bytes.Buffer
		if code := run([]string{cmd, "--help"}, &stdout, &stderr); code != 0 {
			t.Fatalf("%s --help exit = %d", cmd, code)
		}
		out := stdout.String()
		for _, w := range wants {
			if !strings.Contains(out, w) {
				t.Errorf("%s --help = %q, want it to contain %q", cmd, out, w)
			}
		}
		if strings.Contains(out, " -host") || strings.Contains(out, " -sync-root") {
			t.Errorf("%s --help = %q, want two-dash spelling", cmd, out)
		}
	}
}

// TestLogHelpFoldsTheFollowAlias covers the alias case: -f and --follow are one
// option, and flag.PrintDefaults listing them as two unrelated entries made
// `log --help` read as four options where there are two.
func TestLogHelpFoldsTheFollowAlias(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"log", "--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("log --help exit = %d", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "-f, --follow") {
		t.Errorf("log --help = %q, want the alias folded into one row", out)
	}
	if n := strings.Count(out, "follow the log file"); n != 1 {
		t.Errorf("log --help repeats the follow help %d times, want 1", n)
	}
	// -n now folds with its long form the way -f does, and still shows the placeholder.
	if !strings.Contains(out, "-n, --lines <N>") {
		t.Errorf("log --help = %q, want -n folded with --lines and showing its value placeholder", out)
	}
	if n := strings.Count(out, "print only the last"); n != 1 {
		t.Errorf("log --help repeats the lines help %d times, want 1", n)
	}
}

// TestHelpForDelegatesToTheSubcommand confirms `synosync help <cmd>` and
// `synosync <cmd> --help` are the same bytes, which they are because the former
// runs the latter rather than rendering a second copy.
func TestHelpForDelegatesToTheSubcommand(t *testing.T) {
	setupCLI(t, &fakePrompter{})

	for _, c := range commands() {
		var viaHelp, viaFlag, discard bytes.Buffer
		if code := run([]string{"help", c.name}, &viaHelp, &discard); code != 0 {
			t.Errorf("help %s exit = %d, want 0", c.name, code)
			continue
		}
		if code := run([]string{c.name, "--help"}, &viaFlag, &discard); code != 0 {
			t.Errorf("%s --help exit = %d, want 0", c.name, code)
			continue
		}
		if viaHelp.String() != viaFlag.String() {
			t.Errorf("help %s and %s --help differ", c.name, c.name)
		}
	}
}

// TestHelpForUnknownSubcommand confirms an unrecognised name is a usage error
// naming the offender, not a silently-printed top-level page.
func TestHelpForUnknownSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help", "frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `unknown subcommand "frobnicate"`) {
		t.Errorf("stderr = %q, want the unknown name reported", stderr.String())
	}
}

// TestHelpRejectsMoreThanOneSubcommand pins that `help a b` is a usage error
// rather than quietly describing only the first.
func TestHelpRejectsMoreThanOneSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help", "pair", "login"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "help takes one subcommand") {
		t.Errorf("stderr = %q, want the arity reported", stderr.String())
	}
}

// TestTopLevelHelpPointsAtSubcommandHelp confirms the root page tells the reader
// where the detail is. Without it the twelve one-line summaries are a dead end.
func TestTopLevelHelpPointsAtSubcommandHelp(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 0 {
			t.Fatalf("%v exit = %d, want 0", args, code)
		}
		if !strings.Contains(stdout.String(), "synosync <subcommand> --help") {
			t.Errorf("%v = %q, want a pointer at per-subcommand help", args, stdout.String())
		}
	}
}

func TestSpell(t *testing.T) {
	cases := map[string]string{"n": "-n", "f": "-f", "host": "--host", "sync-root": "--sync-root"}
	for in, want := range cases {
		if got := spell(in); got != want {
			t.Errorf("spell(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWrap(t *testing.T) {
	if got := wrap("", 10); got != nil {
		t.Errorf("wrap(\"\") = %v, want nil", got)
	}
	got := wrap("one two three four", 8)
	want := []string{"one two", "three", "four"}
	if len(got) != len(want) {
		t.Fatalf("wrap = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wrap = %q, want %q", got, want)
		}
	}
	// A word wider than the column is emitted whole: breaking a pin or a URL
	// would make it uncopyable, which is worse than an overlong line.
	long := wrap("spki-sha256:0123456789abcdef", 8)
	if len(long) != 1 || long[0] != "spki-sha256:0123456789abcdef" {
		t.Errorf("wrap of an overlong word = %q, want it intact on one line", long)
	}
}

// TestWriteFlagsGivesAnOverlongNameItsOwnLine covers the alignment escape hatch:
// one very wide flag must not drag every other row's help column right with it.
func TestWriteFlagsGivesAnOverlongNameItsOwnLine(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.String("a-really-very-long-flag-name-here", "", "the long `one`")
	fs.String("host", "", "server `host`")

	var b strings.Builder
	writeFlags(&b, collectFlags(fs))
	out := b.String()

	if !strings.Contains(out, "  --a-really-very-long-flag-name-here <one>\n") {
		t.Errorf("flags = %q, want the overlong name alone on its line", out)
	}
	if !strings.Contains(out, "  --host <host>  server host") {
		t.Errorf("flags = %q, want the short row to keep its own column", out)
	}
}

// TestCollectFlagsFoldsOnlyMatchingMeanings confirms the folding is driven by the
// documented meaning: two flags with different help stay two rows even when they
// share a default value.
func TestCollectFlagsFoldsOnlyMatchingMeanings(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	var shared bool
	fs.BoolVar(&shared, "v", false, "be verbose")
	fs.BoolVar(&shared, "verbose", false, "be verbose")
	fs.Bool("quiet", false, "say nothing")

	entries := collectFlags(fs)
	if len(entries) != 2 {
		t.Fatalf("collectFlags = %d rows, want 2", len(entries))
	}
	// Sorted by longest name: "quiet" before "verbose".
	if got := entries[0].left(); got != "--quiet" {
		t.Errorf("row 0 = %q, want --quiet", got)
	}
	if got := entries[1].left(); got != "-v, --verbose" {
		t.Errorf("row 1 = %q, want the alias folded, short name first", got)
	}
}

// TestWriteHelpWithoutFlags confirms a nil FlagSet simply omits the block, the
// shape `version` and `config` use.
func TestWriteHelpWithoutFlags(t *testing.T) {
	var b bytes.Buffer
	writeHelp(&b, versionHelp, nil)
	if strings.Contains(b.String(), "Flags:") {
		t.Errorf("version help = %q, want no Flags block", b.String())
	}
	if !strings.Contains(b.String(), "synosync version - print the synosync version") {
		t.Errorf("version help = %q, want the title", b.String())
	}
}

func TestIsHelpArg(t *testing.T) {
	for _, yes := range []string{"-h", "-help", "--help", "help"} {
		if !isHelpArg(yes) {
			t.Errorf("isHelpArg(%q) = false, want true", yes)
		}
	}
	for _, no := range []string{"", "get", "--helpful", "h"} {
		if isHelpArg(no) {
			t.Errorf("isHelpArg(%q) = true, want false", no)
		}
	}
}
