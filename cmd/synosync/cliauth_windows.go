//go:build windows

package main

// rawPasswordLeavesNewline records that a raw terminal password read stops one byte
// SHORT of consuming the terminal's line ending on this platform.
//
// golang.org/x/term's readPasswordLine (terminal.go:1052-1061) is asymmetric by
// GOOS: on Windows it returns on '\r' and explicitly ignores '\n', everywhere else
// it returns on '\n' and ignores '\r'. A console sends CRLF, so on Windows the '\n'
// is left unread in the console input buffer.
//
// That orphan byte is not harmless here: promptCredentials reads the password and
// then IMMEDIATELY prompts for an OTP, and readLine - which reads byte by byte and
// treats '\n' as end of line - consumed the orphan as a complete empty line. The OTP
// prompt therefore returned "" instantly, without ever waiting for input. Measured
// interactively: the operator's typed code was echoed by the console after a prompt
// that had already returned, went unread by pair, and was left in the buffer for the
// parent shell to pick up - leaking a one-time code in plaintext.
//
// It stayed invisible because it is masked from both sides: an account without 2FA
// wants an empty OTP anyway, and an account with 2FA is rescued by pairWithOTPRetry,
// whose second prompt is not preceded by a raw password read and so works. Only the
// FIRST prompt is broken.
const rawPasswordLeavesNewline = true
