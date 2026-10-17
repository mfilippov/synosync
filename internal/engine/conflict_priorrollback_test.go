package engine

// The keep-both rollback must not resurrect a crash-stranded speculative prior.
//
// preserveAndRecord pre-writes the provenance row BEFORE the aside-rename, so a crash in that
// window leaves a row naming a _Conflict copy that was never created. The next attempt reads that
// row as `prior`, and when its own rename fails terminally (a name the mount cannot hold)
// the rollback used to restore the prior blindly. The op then dead-letters, so
// nothing ever overwrites the row again: `conflicts` claims a copy that does not exist, forever -
// exactly the artefact the rollback exists to prevent. The restore is now evidence-based, on the
// same probe the rest of the rollback uses.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// TestResolveConflictSpeculativePriorNotResurrected is the red test: the prior row names a copy
// that is provably not on disk (the earlier attempt crashed between its pre-write and its rename),
// and this attempt's rename is refused with ENAMETOOLONG, so the op drops terminally. The phantom
// row must be deleted, not restored.
func TestResolveConflictSpeculativePriorNotResurrected(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	// The stranded row of an earlier attempt: no such file was ever created.
	if err := st.PutConflict(7, store.Conflict{
		CanonicalPath: "a.txt",
		CopyPath:      "a_Conflict_laptop_20260610_101010.txt",
		DetectedAt:    1780000000,
	}); err != nil {
		t.Fatalf("seed speculative prior: %v", err)
	}
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want none: the prior names a copy that was never created "+
			"and the op just dead-lettered, so nothing will ever correct it", recs)
	}
	// The drop is unchanged and the divergent content is still at its canonical path - which is
	// what makes deleting the row correct rather than a loss of evidence.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
}

// TestResolveConflictInconclusivePriorProbeKeepsRecord pins the protected direction: when the probe
// of the prior's copy is inconclusive rather than negative (the same failing mount's EIO), the row
// survives. A row for content that really is parked is the one thing that must never be lost.
func TestResolveConflictInconclusivePriorProbeKeepsRecord(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, _, _ := conflictFixture(t, host, ts)
	prior := store.Conflict{
		CanonicalPath: "a.txt",
		CopyPath:      "a_Conflict_oldhost_20260101_000000.txt",
		CopyFileID:    "oldcopy",
		DetectedAt:    1767225600,
	}
	if err := st.PutConflict(7, prior); err != nil {
		t.Fatalf("seed prior conflict: %v", err)
	}
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})
	// The mount that just refused the rename cannot answer the probe either. (The rename's own
	// candidate is proven absent by the name rejection itself, ahead of any probe, so this stub
	// only reaches the prior's arm.)
	stubConflictCopyLstat(t, e, func(string) (os.FileInfo, error) {
		return nil, fmt.Errorf("lstat: %w", syscall.EIO)
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0] != prior {
		t.Errorf("conflict records = %+v, want the prior %+v kept: its absence is unproven", recs, prior)
	}
}

// TestPriorConflictCopyProvablyAbsent tables the prior-restore evidence rule directly, including
// the arms the end-to-end tests cannot reach: a record naming no copy at all, a copy parked under
// its decomposed (NFD) on-disk spelling while the record carries the canonical NFC one, and a
// recorded path that no longer resolves under the view at all.
func TestPriorConflictCopyProvablyAbsent(t *testing.T) {
	e, _, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "parked.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed parked copy: %v", err)
	}
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	if err := os.WriteFile(filepath.Join(viewDir, nfdName), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed NFD copy: %v", err)
	}

	ioErr := fmt.Errorf("lstat: %w", syscall.EIO)
	tooLong := fmt.Errorf("lstat: %w", syscall.ENAMETOOLONG)

	tests := []struct {
		name  string
		copy  string
		probe func(string) (os.FileInfo, error)
		want  bool
	}{
		// A row claiming no copy strands nothing when it is dropped.
		{"no copy path", "", os.Lstat, true},
		{"copy parked on disk", "parked.txt", os.Lstat, false},
		{"copy never created", "gone.txt", os.Lstat, true},
		// The copy is on disk under the byte-preserving spelling the record does not carry.
		{"copy parked under its NFD spelling", nfcName, os.Lstat, false},
		// Inconclusive is not proof - fail toward protected.
		{"probe inconclusive", "gone.txt", func(string) (os.FileInfo, error) { return nil, ioErr }, false},
		// A name the host cannot represent can never name an existing entry.
		{"probe reports the name rejection", "gone.txt", func(string) (os.FileInfo, error) { return nil, tooLong }, true},
		// Containment refuses the recorded path: no probe ran, so nothing is proven.
		{"path escapes the view", "../outside.txt", os.Lstat, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stubConflictCopyLstat(t, e, tc.probe)
			got := e.priorConflictCopyProvablyAbsent(7, store.Conflict{
				CanonicalPath: "a.txt", CopyPath: tc.copy,
			})
			if got != tc.want {
				t.Errorf("priorConflictCopyProvablyAbsent(%q) = %v, want %v", tc.copy, got, tc.want)
			}
		})
	}
}
