//go:build windows

package main

import "testing"

// TestRawPasswordLeavesNewline pins the Windows half of the build-tagged pair.
//
// The authority is golang.org/x/term's readPasswordLine (terminal.go:1052-1061): on
// this GOOS it returns on '\r' and explicitly ignores '\n', so a console's CRLF leaves
// its '\n' unread. It cannot be probed from a test - term.ReadPassword needs a real
// console, which `go test` does not have - so the constant is pinned instead. This is
// what fails if someone "unifies" the pair into one value.
func TestRawPasswordLeavesNewline(t *testing.T) {
	if !rawPasswordLeavesNewline {
		t.Fatal("rawPasswordLeavesNewline is false on windows: x/term's password read stops " +
			"at '\r' here and leaves the '\n', which silently skips the following OTP prompt")
	}
}
