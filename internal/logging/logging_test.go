package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want slog.Level
		ok   bool
	}{
		{"debug", "debug", slog.LevelDebug, true},
		{"info", "info", slog.LevelInfo, true},
		{"warn", "warn", slog.LevelWarn, true},
		{"warning alias", "warning", slog.LevelWarn, true},
		{"error", "error", slog.LevelError, true},
		{"uppercase", "INFO", slog.LevelInfo, true},
		{"padded", "  debug  ", slog.LevelDebug, true},
		{"unknown", "trace", 0, false},
		{"empty", "", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLevel(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("ParseLevel(%q) unexpected error: %v", tc.in, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("ParseLevel(%q) expected error, got none", tc.in)
			}
			if tc.ok && got != tc.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestNewTextFormatAndLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, Options{Level: "warn", Format: FormatText})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("below threshold")
	if buf.Len() != 0 {
		t.Fatalf("info record should be filtered at warn level, got: %q", buf.String())
	}

	log.Warn("visible", "key", "value")
	out := buf.String()
	if !strings.Contains(out, "visible") || !strings.Contains(out, "key=value") {
		t.Fatalf("text output missing expected fields: %q", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("text output missing level: %q", out)
	}
}

func TestNewJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, Options{Level: "info", Format: FormatJSON})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("hello", "n", 42)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v (%q)", err, buf.String())
	}
	if rec["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", rec["msg"])
	}
	if rec["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", rec["level"])
	}
	if rec["n"] != float64(42) {
		t.Fatalf("n = %v, want 42", rec["n"])
	}
}

func TestNewDefaults(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, Options{})
	if err != nil {
		t.Fatalf("New with defaults: %v", err)
	}
	// Default level is info, so info records are emitted in text format.
	log.Info("default")
	out := buf.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "default") {
		t.Fatalf("default options should log info as text, got: %q", out)
	}
}

func TestNewInvalidLevel(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, Options{Level: "loud"}); err == nil {
		t.Fatal("expected error for invalid level")
	}
}

func TestNewInvalidFormat(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, Options{Format: Format("xml")}); err == nil {
		t.Fatal("expected error for invalid format")
	}
}
