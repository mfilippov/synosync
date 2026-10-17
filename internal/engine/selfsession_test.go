package engine

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestNewWarnsOnEmptySelfSessionID confirms that constructing an engine with no
// session id flags the degraded mode (self-echo suppression disabled) at Warn, so
// the operationally-relevant gap is observable rather than silent.
func TestNewWarnsOnEmptySelfSessionID(t *testing.T) {
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	New(nil, nil, t.TempDir(), "", log)

	if got := logbuf.String(); !strings.Contains(got, "empty self session id") {
		t.Errorf("expected a Warn about the empty self session id, got %q", got)
	}
}

// TestNewQuietWithSelfSessionID confirms the degradation warning does NOT fire for the
// ordinary case where a session id is present.
func TestNewQuietWithSelfSessionID(t *testing.T) {
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	New(nil, nil, t.TempDir(), "sess", log)

	if got := logbuf.String(); strings.Contains(got, "empty self session id") {
		t.Errorf("unexpected empty-session warning with a session id present: %q", got)
	}
}
