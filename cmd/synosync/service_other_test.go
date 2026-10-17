//go:build !windows

package main

import "testing"

// On non-Windows platforms effectiveLogStderr honours the --log-stderr flag
// verbatim: a supervisor (systemd/launchd) captures the foreground process's
// stderr, so the flag is the operator's choice and is never overridden.
func TestEffectiveLogStderrNonWindows(t *testing.T) {
	for _, want := range []bool{true, false} {
		if got := effectiveLogStderr(want); got != want {
			t.Errorf("effectiveLogStderr(%v) = %v, want %v", want, got, want)
		}
	}
}

// On non-Windows platforms the pre-logger start-failure fallback is a no-op:
// stderr is already observable in the foreground and under systemd/launchd.
func TestServiceStartFallbackNonWindowsNoop(t *testing.T) {
	serviceStartFallback("boom") // must not panic or produce side effects
}
