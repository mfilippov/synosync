package config

import (
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
)

// TestNormalizeExcludedPath pins the stored spelling and the refusals.
func TestNormalizeExcludedPath(t *testing.T) {
	for in, want := range map[string]string{
		"Photos": "Photos", "/Photos/": "Photos", "Photos//2024/": "Photos/2024",
		`Photos\2024`: "Photos/2024", "./Photos": "Photos",
	} {
		if got, err := NormalizeExcludedPath(in); err != nil || got != want {
			t.Errorf("NormalizeExcludedPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "/", ".", "../x", "a/../../x", "a/.."} {
		if got, err := NormalizeExcludedPath(bad); err == nil {
			t.Errorf("NormalizeExcludedPath(%q) = %q, want refused", bad, got)
		}
	}
}

// TestExcludesMatchesTheSubtreeByFoldKey: a path matches itself and its descendants, folded,
// and never a sibling that merely shares a prefix.
func TestExcludesMatchesTheSubtreeByFoldKey(t *testing.T) {
	ex := ExcludedPath{ViewID: 1, Path: "Photos"}
	for rel, want := range map[string]bool{
		"Photos": true, "Photos/a.jpg": true,
		"photos/2024/b.jpg": normpath.Key("photos") == normpath.Key("Photos"), // case folds only where the host does
		"PhotosOld":         false, "Docs/Photos": false, "": false,
	} {
		if got := ex.Excludes(1, rel); got != want {
			t.Errorf("Excludes(%q) = %v, want %v", rel, got, want)
		}
	}
	if ex.Excludes(2, "Photos") {
		t.Error("an exclusion matched another view")
	}
}

// TestValidateRefusesOverlappingExclusions: an inner exclusion would be dead weight that outlives
// the outer one.
func TestValidateRefusesOverlappingExclusions(t *testing.T) {
	v := Views{Excluded: []ExcludedPath{{ViewID: 1, Path: "Photos"}, {ViewID: 1, Path: "Photos/2024"}}}
	if err := v.Validate(); err == nil {
		t.Error("overlapping exclusions validated")
	}
	v = Views{Excluded: []ExcludedPath{{ViewID: 1, Path: "Photos"}, {ViewID: 2, Path: "Photos"}}}
	if err := v.Validate(); err != nil {
		t.Errorf("the same path in two views: %v", err)
	}
	v = Views{Excluded: []ExcludedPath{{ViewID: 1, Path: "/Photos"}}}
	if err := v.Validate(); err == nil {
		t.Error("a non-normal path validated")
	}
}

// TestNameRules pins the matching rules: system names always, case-insensitive globs, a matching
// folder anywhere in the path, and files-only rules that never hide a folder or an unknown kind.
func TestNameRules(t *testing.T) {
	v := Views{ExcludedNames: DefaultExcludedNames()}
	cases := []struct {
		rel  string
		kind NodeKind
		want bool
	}{
		{"Photos/@eaDir/x.jpg", KindFile, true},
		{"a/#RECYCLE", KindDir, true},
		{"docs/Thumbs.db", KindFile, true},
		{"docs/.DS_Store", KindFile, true},
		{"docs/report.TMP", KindFile, true},
		{"docs/~lock.docx", KindFile, true},
		{"docs/tmp", KindDir, false},
		{"docs/tmp", KindUnknown, false},
		{"docs/tmp/a.txt", KindFile, false},
		{"docs/tmp", KindFile, true},
		{"docs/report.tmp", KindUnknown, false},
		{"docs/.gitignore", KindFile, false},
		{"System Volume Information/x", KindFile, true},
		{"docs/notes.txt", KindFile, false},
	}
	for _, c := range cases {
		if got := v.NameExcluded(c.rel, c.kind); got != c.want {
			t.Errorf("NameExcluded(%q, %d) = %v, want %v", c.rel, c.kind, got, c.want)
		}
	}
	if err := v.Validate(); err != nil {
		t.Errorf("defaults do not validate: %v", err)
	}
	if _, err := NormalizeNamePattern("a/b"); err == nil {
		t.Error("a pattern with a separator was accepted")
	}
	if _, err := NormalizeNamePattern("[x"); err == nil {
		t.Error("a malformed glob was accepted")
	}
	dup := Views{ExcludedNames: []ExcludedName{{Pattern: "*.tmp"}, {Pattern: "*.TMP"}}}
	if err := dup.Validate(); err == nil {
		t.Error("a rule listed twice validated")
	}
}
