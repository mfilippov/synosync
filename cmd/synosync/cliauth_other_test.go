//go:build !windows

package main

import "testing"

// TestRawPasswordLeavesNewline pins the non-Windows half of the build-tagged pair.
//
// golang.org/x/term's readPasswordLine (terminal.go:1052-1061) returns on '\n' here and
// ignores '\r', so the line ending is consumed whole and nothing is left for the next
// prompted read. Dropping a leading newline on this GOOS would eat a genuine empty
// line - an operator answering "no OTP" - so the constant must stay false.
func TestRawPasswordLeavesNewline(t *testing.T) {
	if rawPasswordLeavesNewline {
		t.Fatal("rawPasswordLeavesNewline is true off windows: x/term's password read consumes " +
			"the '\n' here, so dropping one would swallow a genuine empty line")
	}
}
