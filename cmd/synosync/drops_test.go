package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/ipc"
)

func TestRenderDropsEmpty(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{})
	if got := buf.String(); got != "Nothing dropped.\n" {
		t.Errorf("output = %q, want the reassuring empty line", got)
	}
}

// TestRenderDropsNamesPathReasonAndSide pins that the rendering answers the three questions
// the old "see logs" line left to the operator: which file, what was being attempted, and why
// it stopped.
func TestRenderDropsNamesPathReasonAndSide(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{
		Total: 1,
		Drops: []ipc.DropEntry{{
			ViewID: 7, ViewName: "Photos", Op: "download", Direction: "inbound",
			Path: "trip/notes.txt", Reason: "the name cannot be created on this filesystem",
			RecordedAt: 1_700_000_000,
		}},
	})
	out := buf.String()
	for _, want := range []string{
		"1 dropped:",
		"[7] Photos",
		"trip/notes.txt",
		"download from the server (on this device)",
		"the name cannot be created on this filesystem",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

// TestRenderDropsRenameShowsBothEndpoints pins that a move names where it was going; the path
// alone does not describe it.
func TestRenderDropsRenameShowsBothEndpoints(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{
		Total: 1,
		Drops: []ipc.DropEntry{{
			ViewID: 7, ViewName: "Photos", Op: "rename", Direction: "outbound",
			Path: "a.txt", NewPath: "b.txt", Reason: "the source is gone on the server",
		}},
	})
	out := buf.String()
	if !strings.Contains(out, "moving to: b.txt") {
		t.Errorf("output does not name the move's destination\n%s", out)
	}
	if !strings.Contains(out, "move (on the server)") {
		t.Errorf("output does not say which side the move applied to\n%s", out)
	}
}

// TestRenderDropsFoldCollisionHasNoSinglePath pins the one record that stands for a set of
// names rather than one path: printing an empty field for it would read as a bug.
func TestRenderDropsFoldCollisionHasNoSinglePath(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{
		Total: 1,
		Drops: []ipc.DropEntry{{
			ViewID: 7, ViewName: "Photos", Op: "fold_collision",
			Reason: "several names share one sync key",
		}},
	})
	if out := buf.String(); !strings.Contains(out, "(several names at once)") {
		t.Errorf("output does not explain the empty path\n%s", out)
	}
}

// TestRenderDropsTruncationSaysSo pins that a capped report admits it, so a reader does not
// take the listed entries for the whole set.
func TestRenderDropsTruncationSaysSo(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{
		Total: 900, Truncated: true,
		Drops: []ipc.DropEntry{{ViewID: 7, ViewName: "Photos", Op: "upload", Path: "x", Reason: "r"}},
	})
	if out := buf.String(); !strings.Contains(out, "900 dropped, showing the newest 1:") {
		t.Errorf("output does not disclose the truncation\n%s", out)
	}
}

// TestWriteJSONEmitsTheWireFields pins that --json re-encodes the daemon's own report rather
// than inventing a second vocabulary. Anything monitoring synosync would otherwise have to
// scrape lines written for a person to read, which are free to change.
func TestWriteJSONEmitsTheWireFields(t *testing.T) {
	var out, errBuf bytes.Buffer
	code := writeJSON(&out, &errBuf, "drops", ipc.DropReport{
		Total: 1,
		Drops: []ipc.DropEntry{{
			ViewID: 7, ViewName: "Photos", Op: "download", Direction: "inbound",
			Path: "a.txt", Reason: "blocked", RecordedAt: 42,
		}},
	})
	if code != 0 || errBuf.Len() != 0 {
		t.Fatalf("code = %d, stderr = %q", code, errBuf.String())
	}

	var back ipc.DropReport
	if err := json.Unmarshal(out.Bytes(), &back); err != nil {
		t.Fatalf("output does not round-trip: %v\n%s", err, out.String())
	}
	if back.Total != 1 || len(back.Drops) != 1 || back.Drops[0].Path != "a.txt" {
		t.Errorf("round-tripped report = %+v", back)
	}
	// The snake_case tags are the contract a consumer keys on.
	for _, want := range []string{`"view_id"`, `"recorded_at"`, `"reason"`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing the wire field %s\n%s", want, out.String())
		}
	}
}

// TestRenderDropsNamesAnUnrepresentableServerName pins the wording for a skipped server name, the
// drop a Windows user meets first ("Report | 2025.pdf"): it must say what is wrong in their terms.
func TestRenderDropsNamesAnUnrepresentableServerName(t *testing.T) {
	var buf bytes.Buffer
	renderDrops(&buf, ipc.DropReport{
		Total: 1,
		Drops: []ipc.DropEntry{{
			ViewID: 1, ViewName: "home", Op: "unrepresentable_name", Direction: "inbound",
			Path: "Documents/Report | 2025.pdf", Reason: "rename it on the server",
		}},
	})
	if out := buf.String(); !strings.Contains(out, "a server name this computer's filesystem cannot hold (on this device)") {
		t.Errorf("output does not explain the skipped name\n%s", out)
	}
}
