package engine

import (
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
)

// TestKeyContainment pins the two things seven hand-written copies of this predicate each
// had to get right on their own: a path is not beneath itself, and a sibling whose name
// merely starts with the same characters is not beneath it either. The second is the one
// that bites - "photos-old" against "photos" - and a bare string prefix gets it wrong.
func TestKeyContainment(t *testing.T) {
	cases := []struct {
		child, dir       string
		under, atOrUnder bool
	}{
		{"photos/a.txt", "photos", true, true},
		{"photos/sub/a.txt", "photos", true, true},
		{"photos", "photos", false, true},
		{"photos-old/a.txt", "photos", false, false},
		{"photosx", "photos", false, false},
		{"other/a.txt", "photos", false, false},
		{"photos/a.txt", "", false, false},
		{"", "", false, true},
	}
	for _, tc := range cases {
		if got := keyUnder(tc.child, tc.dir); got != tc.under {
			t.Errorf("keyUnder(%q, %q) = %v, want %v", tc.child, tc.dir, got, tc.under)
		}
		if got := keyAtOrUnder(tc.child, tc.dir); got != tc.atOrUnder {
			t.Errorf("keyAtOrUnder(%q, %q) = %v, want %v", tc.child, tc.dir, got, tc.atOrUnder)
		}
	}
}

// TestRelocateUnderMove pins the rule three history walks used to each state for
// themselves: a move carries the node itself and everything beneath it, a sibling whose
// name shares a prefix is untouched, and an unrelated path is reported as not moved so
// the caller can tell "stayed put" from "moved to the same place".
func TestRelocateUnderMove(t *testing.T) {
	cases := []struct {
		cur, from, to string
		want          string
		moved         bool
	}{
		{"photos", "photos", "pictures", "pictures", true},
		{"photos/a.txt", "photos", "pictures", "pictures/a.txt", true},
		{"photos/sub/a.txt", "photos", "pictures", "pictures/sub/a.txt", true},
		{"photos/sub/a.txt", "photos/sub", "photos/other", "photos/other/a.txt", true},
		{"photos-old/a.txt", "photos", "pictures", "photos-old/a.txt", false},
		{"other/a.txt", "photos", "pictures", "other/a.txt", false},
		// Reversed, the way the backward pre-batch walk calls it.
		{"pictures/a.txt", "pictures", "photos", "photos/a.txt", true},
	}
	for _, tc := range cases {
		got, moved := relocateUnderMove(tc.cur, tc.from, tc.to)
		if got != tc.want || moved != tc.moved {
			t.Errorf("relocateUnderMove(%q, %q, %q) = (%q, %v), want (%q, %v)",
				tc.cur, tc.from, tc.to, got, moved, tc.want, tc.moved)
		}
	}
}

// TestRelocateUnderMoveFoldsCase covers what makes the shared helper worth having: the
// prefix match folds, so a fold that CHANGES byte length must not be used to slice the
// original path. U+0130 lowercases to two runes, so a byte-length slice cuts mid-name.
func TestRelocateUnderMoveFoldsCase(t *testing.T) {
	defer normpath.SetCaseInsensitive(true)()
	got, moved := relocateUnderMove("İstanbul/a.txt", "istanbul", "cities")
	if !moved {
		t.Fatal("a case-only difference must still match the moved directory")
	}
	if got != "cities/a.txt" {
		t.Fatalf("relocated to %q, want %q - the re-base sliced by byte length", got, "cities/a.txt")
	}
}
