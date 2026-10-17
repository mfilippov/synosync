package engine

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/normpath"
)

// fsIsCaseInsensitive independently determines whether dir's filesystem folds
// case, so probeSyncRootCaseFold can be validated against a ground truth on any
// host FS without re-using the probe's own logic. It creates a lowercase file and
// checks whether the same name upper-cased resolves.
func fsIsCaseInsensitive(t *testing.T, dir string) bool {
	t.Helper()
	lower := filepath.Join(dir, "casecheck-groundtruth")
	if err := os.WriteFile(lower, nil, 0o600); err != nil {
		t.Fatalf("write ground-truth file: %v", err)
	}
	defer func() { _ = os.Remove(lower) }()
	_, err := os.Lstat(filepath.Join(dir, "CASECHECK-GROUNDTRUTH"))
	return err == nil
}

// TestProbeSyncRootCaseFoldMatchesHost checks the probe reports the same
// case-fold verdict as an independent ground-truth check on the CI filesystem
// (case-sensitive on Linux CI, case-insensitive on a default APFS/NTFS host).
// The behavioural direction (the wrong-fold spurious-rename bug) cannot be
// reproduced on a case-sensitive CI FS - the false→true branch has no on-disk
// case-pair to trip it - so this pins the probe's verdict, and TestApplyCaseFold
// exercises both flag outcomes through the injected seam.
func TestProbeSyncRootCaseFoldMatchesHost(t *testing.T) {
	dir := t.TempDir()
	want := fsIsCaseInsensitive(t, dir)
	got, err := probeSyncRootCaseFold(dir)
	if err != nil {
		t.Fatalf("probeSyncRootCaseFold: %v", err)
	}
	if got != want {
		t.Errorf("probeSyncRootCaseFold(%q) = %v, ground truth = %v", dir, got, want)
	}
	// The probe must leave nothing behind on disk.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), "casefold-probe") {
			t.Errorf("probe left a file behind: %s", e.Name())
		}
	}
}

func TestProbeSyncRootCaseFoldMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := probeSyncRootCaseFold(missing); err == nil {
		t.Fatal("expected an error probing a missing directory")
	}
	if _, err := probeSyncRootCaseFold(""); err == nil {
		t.Fatal("expected an error probing an empty root")
	}
}

// TestProbeFileIsStagingFiltered pins that the probe's temp name is suppressed by
// the scanner's staging filter, so a probe file left behind by a crash between
// create and remove is never scanned as user content (and, via the mirror filter,
// never wakes the watcher).
func TestProbeFileIsStagingFiltered(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, casefoldProbePattern)
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(name) }()
	base := filepath.Base(name)
	if !isStagingName(base) {
		t.Errorf("probe name %q is not staging-filtered by the scanner", base)
	}
	// The flipped name must differ (so the probe actually flips at least one letter).
	if flipASCIICase(base) == base {
		t.Errorf("probe name %q has no ASCII letter to flip", base)
	}
}

func TestFlipASCIICase(t *testing.T) {
	tests := map[string]string{
		"abc":                  "ABC",
		"ABC":                  "abc",
		"aBc123":               "AbC123",
		".synosync-probe.dl-0": ".SYNOSYNC-PROBE.DL-0",
		"café":                 "CAFé", // é is non-ASCII, left untouched
		"123-_.":               "123-_.",
		"":                     "",
	}
	for in, want := range tests {
		if got := flipASCIICase(in); got != want {
			t.Errorf("flipASCIICase(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestApplyCaseFold exercises the three branches of applyCaseFold through the
// injected probe seam: a probe verdict of true/false sets the process-wide
// normpath flag accordingly, and a probe error falls back to the GOOS default and
// warns. The flag is a process global, so the original value is restored on exit.
func TestApplyCaseFold(t *testing.T) {
	orig := normpath.CaseInsensitive()
	defer normpath.UseCaseInsensitive(orig)

	newEng := func(probe func(string) (bool, error), log *slog.Logger) *Engine {
		return &Engine{log: log, syncRoot: t.TempDir(), caseFoldProbe: probe}
	}

	t.Run("probe true sets case-insensitive", func(t *testing.T) {
		normpath.UseCaseInsensitive(false)
		e := newEng(func(string) (bool, error) { return true, nil }, warnLogger(&bytes.Buffer{}))
		e.applyCaseFold()
		if !normpath.CaseInsensitive() {
			t.Error("expected case-insensitive after probe returned true")
		}
	})

	t.Run("probe false sets case-sensitive", func(t *testing.T) {
		normpath.UseCaseInsensitive(true)
		e := newEng(func(string) (bool, error) { return false, nil }, warnLogger(&bytes.Buffer{}))
		e.applyCaseFold()
		if normpath.CaseInsensitive() {
			t.Error("expected case-sensitive after probe returned false")
		}
	})

	t.Run("probe error falls back to GOOS default and warns", func(t *testing.T) {
		// Start from the opposite of the GOOS default so the fallback is observable.
		normpath.UseCaseInsensitive(!normpath.DefaultCaseInsensitive())
		var buf bytes.Buffer
		e := newEng(func(string) (bool, error) { return false, errors.New("boom") }, warnLogger(&buf))
		e.applyCaseFold()
		if normpath.CaseInsensitive() != normpath.DefaultCaseInsensitive() {
			t.Errorf("probe error: flag = %v, want GOOS default %v",
				normpath.CaseInsensitive(), normpath.DefaultCaseInsensitive())
		}
		if !strings.Contains(buf.String(), "case-fold probe failed") {
			t.Errorf("expected a probe-failure warning, got: %q", buf.String())
		}
	})

	t.Run("nil probe leaves flag unchanged", func(t *testing.T) {
		normpath.UseCaseInsensitive(true)
		e := &Engine{log: warnLogger(&bytes.Buffer{}), syncRoot: t.TempDir(), caseFoldProbe: nil}
		e.applyCaseFold()
		if !normpath.CaseInsensitive() {
			t.Error("nil probe should leave the flag unchanged")
		}
	})

	t.Run("empty syncRoot skips the probe", func(t *testing.T) {
		normpath.UseCaseInsensitive(true)
		e := &Engine{
			log:           warnLogger(&bytes.Buffer{}),
			syncRoot:      "",
			caseFoldProbe: func(string) (bool, error) { return false, nil },
		}
		e.applyCaseFold()
		if !normpath.CaseInsensitive() {
			t.Error("empty syncRoot should skip the probe and leave the flag unchanged")
		}
	})
}
