package main

// Machine-readable output for the reporting subcommands.
//
// The daemon already speaks JSON over IPC and its report types carry stable snake_case tags,
// so `--json` is a re-encode of the wire struct rather than a second rendering with its own
// vocabulary to drift. That is the point: anything monitoring synosync previously had to scrape
// lines like "Pending:    3 operation(s)", which is a format written for a person to read and
// therefore free to change.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
)

// addJSONFlag registers the shared --json flag on a reporting subcommand's FlagSet.
func addJSONFlag(fs *flag.FlagSet) *bool {
	return fs.Bool("json", false, "print the report as JSON instead of text")
}

// writeJSON encodes a report to w, indented and newline-terminated so the output is readable
// on a terminal as well as pipeable. An encoding failure is reported on stderr and turns into a
// non-zero exit, rather than leaving the caller with a half-written document that parses as
// truncated JSON.
func writeJSON(stdout, stderr io.Writer, what string, v any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(stderr, "synosync: %s: encode json: %s\n", what, userErr(err))
		return 1
	}
	return 0
}
