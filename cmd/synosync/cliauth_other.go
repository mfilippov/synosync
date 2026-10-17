//go:build !windows

package main

// rawPasswordLeavesNewline records that a raw terminal password read consumes the
// terminal's line ending whole on this platform, leaving nothing behind for the next
// prompted read to trip over.
//
// golang.org/x/term's readPasswordLine (terminal.go:1052-1061) returns on '\n' here
// and ignores '\r', so the line ending is fully consumed either way. See the Windows
// half of this pair for what the orphaned byte costs when it is not.
const rawPasswordLeavesNewline = false
