package main

// Shared argument-checking helpers for the CLI subcommands. The help pages
// themselves, and the --help/parse-error split, live in clihelp.go.

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// quoteArgs renders rejected positional arguments for an error message, quoted
// so an empty or space-bearing argument is still visible as one token.
func quoteArgs(args []string) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		parts = append(parts, strconv.Quote(a))
	}
	return strings.Join(parts, " ")
}

// writeUsageHint prints the invocation forms and a pointer at --help, the tail
// every usage error in this package ends with.
func writeUsageHint(stderr io.Writer, spec helpSpec) {
	if len(spec.usage) > 0 {
		fmt.Fprintf(stderr, "\nUsage:\n")
		for _, u := range spec.usage {
			fmt.Fprintf(stderr, "  %s\n", u)
		}
	}
	fmt.Fprintf(stderr, "\nRun `synosync %s --help` for the full list of flags.\n", spec.name)
}

// rejectExtraArgs reports a usage error when a subcommand that takes no
// positional arguments was given some. It returns ok=false (with the exit code)
// when extra arguments are present, ok=true otherwise.
//
// The message it replaces was the bare "synosync: usage: synosync pair", which
// named neither what had been rejected nor what to do instead - and
// `synosync pair nas.example.com`, the first thing anyone types, is exactly the
// case that hit it. Name the rejected argument, offer the corrected command line
// where the spec knows how to build one, then show the usage and --help.
func rejectExtraArgs(spec helpSpec, rest []string, stderr io.Writer) (code int, ok bool) {
	if len(rest) == 0 {
		return 0, true
	}
	fmt.Fprintf(stderr, "synosync: %s takes no positional arguments, got %s\n", spec.name, quoteArgs(rest))
	if spec.hint != nil {
		if h := spec.hint(rest[0]); h != "" {
			fmt.Fprintf(stderr, "\n  Did you mean:\n    %s\n", h)
		}
	}
	writeUsageHint(stderr, spec)
	return 2, false
}
