package watcher

import (
	"path/filepath"
	"testing"
)

func TestShouldIgnore(t *testing.T) {
	tests := []struct {
		name string
		base string
		want bool
	}{
		// Staging temps (".<base>.dl-<digits>") - suppressed.
		{"staging temp", ".report.docx.dl-123456789", true},
		{"staging temp hidden base", ".data.dl-42", true},
		// ".dl-" without a trailing digit run is a real user file.
		{"dl marker no digits", ".notes.dl-draft", false},
		{"dl marker not hidden", "report.dl-123", false},
		{"dl marker empty suffix", ".x.dl-", false},

		// Keep-both conflict copies - suppressed.
		{"conflict copy with ext", "report_host_Jun-12-150405-2006_Conflict.docx", true},
		{"conflict copy no ext", "report_host_Jun-12-150405-2006_Conflict", true},
		{"conflict copy dotfile", ".bashrc_host_Jun-12-150405-2006_Conflict", true},
		// withCopyCounter appends "_<n>" on a same-second collision.
		{"conflict copy counter", "report_host_Jun-12-150405-2006_Conflict_1.docx", true},
		{"conflict copy counter no ext", "report_host_Jun-12-150405-2006_Conflict_12", true},
		{"conflict marker not at end", "_Conflict_notes.txt", false},

		// Transient temp/partial/editor files - suppressed.
		{"tmp suffix", "draft.tmp", true},
		{"temp suffix", "draft.temp", true},
		{"upper tmp", "DRAFT.TMP", true},
		{"vim swap", ".main.go.swp", true},
		{"vim swap o", ".main.go.swo", true},
		{"vim swap x", ".main.go.swx", true},
		{"part suffix", "movie.part", true},
		{"partial suffix", "movie.partial", true},
		{"chrome download", "movie.mp4.crdownload", true},
		{"safari download", "movie.mp4.download", true},
		{"tilde backup", "notes.txt~", true},
		{"vim probe", "4913", true},

		// Ordinary user files - not filtered.
		{"plain text", "notes.txt", false},
		{"go source", "main.go", false},
		{"dotfile", ".bashrc", false},
		{"no extension", "Makefile", false},
		{"docx", "report.docx", false},
		{"contains tmp not suffix", "tmphelper.go", false},
		{"contains conflict word", "conflict-notes.txt", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldIgnore(filepath.Join("/sync/root", tc.base)); got != tc.want {
				t.Errorf("shouldIgnore(%q) = %v, want %v", tc.base, got, tc.want)
			}
		})
	}
}

func TestShouldIgnoreRootPathsNotFiltered(t *testing.T) {
	for _, p := range []string{".", "/", ""} {
		if shouldIgnore(p) {
			t.Errorf("shouldIgnore(%q) = true, want false", p)
		}
	}
}
