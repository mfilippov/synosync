package main

// Shared help rendering for the CLI subcommands.
//
// Every subcommand used to fall back to flag.FlagSet's own PrintDefaults, which
// gave four of them ("status", "sync", "pause", "conflicts" - the ones with no
// flags) a help page consisting of the single line "Usage of status:" and
// nothing else. PrintDefaults also spells every flag with ONE dash while the
// README and every error message in this package say --host, and it prints an
// alias pair (log's -f and --follow) as two unrelated options.
//
// writeHelp replaces it with one format for all of them: title, usage, a
// description, the flags, and examples. The flag block is read back off the
// FlagSet rather than restated in the spec, so a help page cannot drift from the
// flags the command actually parses.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
)

// helpWidth is the column the description, flag help and examples wrap at.
const helpWidth = 78

// helpSpec is a subcommand's help page minus its flags. A zero examples or
// describe field simply omits that block.
type helpSpec struct {
	// name is the subcommand word, e.g. "pair".
	name string
	// summary is the one-line gloss shown in the title and in the top-level
	// subcommand list. Keep it identical to the registry entry in main.go -
	// TestEverySubcommandHasAHelpPage pins that.
	summary string
	// usage holds the invocation forms, without the leading indent, e.g.
	// "synosync config get [key]".
	usage []string
	// describe is free prose, re-wrapped to helpWidth. A blank line in the
	// source starts a new paragraph.
	describe string
	// examples are shown verbatim (never re-wrapped), so a line can carry a
	// trailing comment or a shell continuation.
	examples []string
	// hint, when set, turns the first rejected positional argument into a
	// "did you mean" line. Only pair and login set it: a bare hostname is the
	// overwhelmingly likely thing to be sitting there, and the corrected command
	// line is worth more than a second copy of the usage.
	hint func(arg string) string
}

// flagEntry is one rendered row of the Flags block: every name that documents
// the same option on the left, its shared help on the right.
type flagEntry struct {
	names       []string
	placeholder string
	help        string
}

// longest returns the entry's longest name, which is what the block is sorted
// on so an alias pair sorts under its readable spelling ("follow", not "f").
func (e flagEntry) longest() string {
	out := ""
	for _, n := range e.names {
		if len(n) > len(out) {
			out = n
		}
	}
	return out
}

// collectFlags reads fs back into rendered rows.
//
// Names that document the same option are folded into one row. The grouping key
// is the usage text plus the default value, NOT the identity of the flag.Value:
// an alias pair registered as BoolVar(&follow, "f", …) + BoolVar(&follow,
// "follow", …) does share one *bool, but comparing flag.Value interfaces would
// panic the moment somebody registers a custom fs.Var whose value type is not
// comparable. Grouping on the documented meaning cannot panic, and its
// worst-case failure - folding two distinct flags that happen to share a help
// string - is cosmetic.
func collectFlags(fs *flag.FlagSet) []flagEntry {
	var entries []*flagEntry
	byMeaning := make(map[string]*flagEntry)

	fs.VisitAll(func(f *flag.Flag) {
		placeholder, help := flag.UnquoteUsage(f)
		key := f.Usage + "\x00" + f.DefValue
		if e, ok := byMeaning[key]; ok {
			e.names = append(e.names, f.Name)
			return
		}
		e := &flagEntry{names: []string{f.Name}, placeholder: placeholder, help: help}
		byMeaning[key] = e
		entries = append(entries, e)
	})

	out := make([]flagEntry, 0, len(entries))
	for _, e := range entries {
		// Short names first, so a row reads "-f, --follow" the way the README
		// and every other Unix tool spell an alias pair.
		sort.Slice(e.names, func(i, j int) bool {
			if len(e.names[i]) != len(e.names[j]) {
				return len(e.names[i]) < len(e.names[j])
			}
			return e.names[i] < e.names[j]
		})
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].longest() < out[j].longest() })
	return out
}

// spell renders one flag name with the dash count its length calls for: a
// single-character flag takes one dash (-n), anything longer takes two
// (--sync-root). flag.FlagSet accepts either spelling for either, so this is a
// presentation choice only - and it is the one the README makes.
func spell(name string) string {
	if len(name) == 1 {
		return "-" + name
	}
	return "--" + name
}

// left renders an entry's name column: every spelling, then the value
// placeholder once at the end (`-n, --lines <N>`), since the placeholder
// belongs to the option and not to any one of its names.
func (e flagEntry) left() string {
	parts := make([]string, 0, len(e.names))
	for _, n := range e.names {
		parts = append(parts, spell(n))
	}
	out := strings.Join(parts, ", ")
	if e.placeholder != "" {
		out += " <" + e.placeholder + ">"
	}
	return out
}

// wrap breaks s into lines of at most width runes, splitting on spaces only. A
// word longer than width (a URL, a long pin) is emitted on its own overlong line
// rather than being cut, since breaking it would make it uncopyable.
func wrap(s string, width int) []string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return nil
	}
	var (
		lines []string
		cur   strings.Builder
	)
	for _, w := range words {
		switch {
		case cur.Len() == 0:
			cur.WriteString(w)
		case cur.Len()+1+len(w) <= width:
			cur.WriteByte(' ')
			cur.WriteString(w)
		default:
			lines = append(lines, cur.String())
			cur.Reset()
			cur.WriteString(w)
		}
	}
	if cur.Len() > 0 {
		lines = append(lines, cur.String())
	}
	return lines
}

// writeFlags renders the Flags block, aligning the help column against the
// widest name. A very wide name column would leave no room for the help text, so
// past maxNameCol the help simply starts one space after the name instead of
// pushing every other row out with it.
func writeFlags(b *strings.Builder, entries []flagEntry) {
	const (
		indent     = "  "
		gap        = 2
		maxNameCol = 28
	)
	nameCol := 0
	for _, e := range entries {
		if n := len(e.left()); n > nameCol && n <= maxNameCol {
			nameCol = n
		}
	}

	b.WriteString("Flags:\n")
	for _, e := range entries {
		name := e.left()
		helpCol := len(indent) + nameCol + gap
		lines := wrap(e.help, helpWidth-helpCol)
		if len(lines) == 0 {
			fmt.Fprintf(b, "%s%s\n", indent, name)
			continue
		}
		if len(name) > nameCol {
			// Too wide to align against: give the name its own line so the help
			// keeps the shared column rather than dragging it right.
			fmt.Fprintf(b, "%s%s\n", indent, name)
			for _, l := range lines {
				fmt.Fprintf(b, "%s%s\n", strings.Repeat(" ", helpCol), l)
			}
			continue
		}
		fmt.Fprintf(b, "%s%-*s%s%s\n", indent, nameCol, name, strings.Repeat(" ", gap), lines[0])
		for _, l := range lines[1:] {
			fmt.Fprintf(b, "%s%s\n", strings.Repeat(" ", helpCol), l)
		}
	}
}

// writeHelp renders spec's help page, reading the flag block off fs. A nil fs
// (or one with no flags) simply omits the Flags block.
func writeHelp(w io.Writer, spec helpSpec, fs *flag.FlagSet) {
	var b strings.Builder

	fmt.Fprintf(&b, "synosync %s - %s\n", spec.name, spec.summary)

	if len(spec.usage) > 0 {
		b.WriteString("\nUsage:\n")
		for _, u := range spec.usage {
			fmt.Fprintf(&b, "  %s\n", u)
		}
	}

	if spec.describe != "" {
		for _, para := range strings.Split(strings.TrimSpace(spec.describe), "\n\n") {
			b.WriteString("\n")
			for _, l := range wrap(para, helpWidth) {
				fmt.Fprintf(&b, "%s\n", l)
			}
		}
	}

	if fs != nil {
		if entries := collectFlags(fs); len(entries) > 0 {
			b.WriteString("\n")
			writeFlags(&b, entries)
		}
	}

	if len(spec.examples) > 0 {
		b.WriteString("\nExamples:\n")
		for _, e := range spec.examples {
			fmt.Fprintf(&b, "  %s\n", e)
		}
	}

	_, _ = io.WriteString(w, b.String())
}

// parseFlags parses args for a subcommand and routes the two outcomes flag
// conflates.
//
// flag.FlagSet calls its Usage hook for BOTH a requested --help and a genuine
// parse error, and writes both to the same Output. That put every help page on
// stderr, where a caller doing `synosync pair --help > cheatsheet` got an empty
// file. Here the Usage hook is silenced: flag.ErrHelp prints the full page to
// stdout and exits 0, while a real parse error keeps the message flag already
// wrote to stderr, gains a pointer at --help, and exits 2.
//
// ok=false means the caller must return code immediately.
func parseFlags(fs *flag.FlagSet, spec helpSpec, args []string, stdout, stderr io.Writer) (code int, ok bool) {
	fs.SetOutput(stderr)
	fs.Usage = func() {}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			writeHelp(stdout, spec, fs)
			return 0, false
		}
		fmt.Fprintf(stderr, "Run `synosync %s --help` for usage.\n", spec.name)
		return 2, false
	}
	return 0, true
}

// isHelpArg reports whether arg asks for help. It exists for the subcommands
// that route on a positional word before any FlagSet sees the arguments
// (config, service, version), so asking them for help is not the one place in
// the CLI where -h is a usage error.
func isHelpArg(arg string) bool {
	switch arg {
	case "-h", "-help", "--help", "help":
		return true
	}
	return false
}
