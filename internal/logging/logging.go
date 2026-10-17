// Package logging configures the synosync structured logger built on the
// standard library log/slog. It provides a small helper to construct a
// *slog.Logger from a textual level name (matching the config schema) and an
// output format, so the rest of the program never touches slog handler
// construction directly.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Format selects the encoding used by the logger's handler.
type Format string

const (
	// FormatText emits human-readable key=value lines (slog.TextHandler).
	FormatText Format = "text"
	// FormatJSON emits one JSON object per record (slog.JSONHandler).
	FormatJSON Format = "json"
)

// ParseLevel maps a level name ("debug", "info", "warn", "error") to a
// slog.Level. The name is case-insensitive and surrounding whitespace is
// ignored. An unknown name yields an error.
func ParseLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging: invalid level %q", name)
	}
}

// Options configures New.
type Options struct {
	// Level is the textual level name ("debug", "info", "warn", "error"). An
	// empty value defaults to "info".
	Level string
	// Format selects the handler encoding. An empty value defaults to
	// FormatText.
	Format Format
	// AddSource includes the source file and line of each log call.
	AddSource bool
}

// New constructs a *slog.Logger writing to w using the supplied options. It
// returns an error if the level name is not recognised.
func New(w io.Writer, opts Options) (*slog.Logger, error) {
	level := opts.Level
	if level == "" {
		level = "info"
	}
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: opts.AddSource,
	}

	var handler slog.Handler
	switch opts.Format {
	case FormatJSON:
		handler = slog.NewJSONHandler(w, handlerOpts)
	case FormatText, "":
		handler = slog.NewTextHandler(w, handlerOpts)
	default:
		return nil, fmt.Errorf("logging: invalid format %q", opts.Format)
	}

	return slog.New(handler), nil
}
