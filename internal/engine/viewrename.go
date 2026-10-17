package engine

// This file carries the view-rename guard: a server-side view
// rename must never be READ as a mass local deletion. refreshViews used to overwrite
// the stored view name unconditionally; the scanner then looked under the NEW name
// while every baseline described a file under the OLD one, so as soon as the new
// directory materialised (the first inbound apply creates it) the diff read every
// old-name baseline as vanished and pushed an OpRemove for each - deleting the whole
// view server-side. adoptViewRename decides when the new name is safe to adopt
// (moving the on-disk directory itself when that move is what makes it safe); a
// rename that cannot be adopted yet is deferred - the old name stays in force, so
// scan and inbound apply remain mutually consistent, the view's local scan is
// suppressed, and every scan pass re-attempts the adoption and re-logs the operator
// hint (retryPendingViewRename).

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// viewRenameOldName resolves the "old" directory name the adoption matrix should
// treat as current: the stored view name, or - for a pre-existing record that
// synced under an empty server-supplied name - the numeric-id fallback directory
// the scanner and executor materialise such a view under (viewDir). Without the
// fallback, a name later supplied for a name-less view would be adopted blindly,
// re-pointing the scan away from the directory the baselines describe - the same
// mass-delete mechanism adoptViewRename guards against.
func viewRenameOldName(rec store.View, viewID uint64) string {
	if rec.Name != "" {
		return rec.Name
	}
	return strconv.FormatUint(viewID, 10)
}

// viewNameSubdir maps a view name to the OS-separated relative directory it
// materialises under the sync root, mirroring abs's containment checks
// (filepath.IsLocal, the "."-collapse rejection, the embedded-NUL rejection and the
// host-representability gate). ok is false for a name that does not map to a proper,
// isolated subdirectory.
//
// The representability gate matters most here: a name the host silently rewrites (on
// Windows one whose component ends in a dot or a space - namemangle.go) would let the
// adoption matrix "confirm" a rename that never happened on disk. Its Lstat probes of the
// new name alias the STRIPPED directory - which for a rename "Team" -> "Team." is the old
// directory itself - so the probe answers, the rename is adopted, and the view's scan then
// looks under a name no baseline describes. Refusing the name defers the adoption instead
// (the old name stays in force and the operator hint is re-logged each pass).
func viewNameSubdir(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	rel := filepath.FromSlash(name)
	if !filepath.IsLocal(rel) || filepath.Clean(rel) == "." || strings.IndexByte(rel, 0) >= 0 {
		return "", false
	}
	if !hostRepresentablePath(rel) {
		return "", false
	}
	return rel, true
}

// errStopBaselineScan is the sentinel viewHasBaselines returns from the scan
// callback to stop after the first record.
var errStopBaselineScan = errors.New("engine: stop baseline scan")

// viewHasBaselines reports whether the view has at least one stored baseline,
// streaming at most one record off the store. A record the stream skipped as
// corrupt counts: it is still evidence the view holds tracked
// state, and the check gates a rename adoption that must stay suppressed when
// baselines exist (an unmounted volume), so reading corruption as "empty" would
// fail toward the unsafe branch.
func (e *Engine) viewHasBaselines(viewID uint64) (bool, error) {
	found := false
	skipped, err := e.scanBaselines(viewID, func(store.PathBaseline) error {
		found = true
		return errStopBaselineScan
	})
	if err != nil && !errors.Is(err, errStopBaselineScan) {
		return false, err
	}
	return found || len(skipped) > 0, nil
}

// maxRenameAdoptProbes bounds the Lstat probes newNameTreeConfirmed performs. The
// check runs at most once per scan pass for a view whose name the server changed, and
// one matching record is all the evidence it needs, so a small window keeps the cost
// flat for a million-baseline view; a view whose first probed records all happen to be
// stale merely defers (with the operator hint re-logged each pass), never mass-deletes.
//
// It bounds the Lstat work ONLY: the stream keeps running past the budget until a synced
// FILE baseline is seen, because that fact (hashableSynced) gates the weak directory-only
// evidence grade and reading it from a truncated prefix would invert the rule.
//
// The budget is charged per EVIDENCE KIND (files and directories each get their own), so a
// view whose leading records happen to all be directories - baselines stream in path-byte
// order, and a chain or a row of empty directories easily fills 64 of them - still gets its
// file candidates probed. Charging both kinds against one counter made the file record that
// WOULD confirm arrive with the budget already spent, and since seeing it also set
// hashableSynced (retiring the directory grade), the view could never confirm on any pass:
// the deferral was permanent rather than a retry, freezing outbound sync for a view whose
// operator had already applied the documented remedy. For the same reason a record that
// STRUCTURALLY cannot confirm (a zero-length file, whose size match discriminates nothing)
// is skipped before it charges the budget rather than after.
const maxRenameAdoptProbes = 64

// newNameTreeConfirmed reports whether the directory that already carries a view's NEW
// server-side name (newAbs) carries positive evidence of being the tree this view's
// baselines describe, and whether the view tracks any synced baseline at all. It is the
// confirmation the newExists adoption branch requires, the cheap
// mirror of the non-empty scan's scanConfirmsTrackedTree.
//
// hasSynced gates the whole check: only StateSynced baselines drive localReconcile's
// detectRemoves, so a view with none of them has nothing a mis-diff could destroy and is
// adopted unconditionally - a slightly narrower gate than a plain "viewHasBaselines"
// wording, chosen so a view holding only pending records (an interrupted first sync) is
// never frozen for evidence it structurally cannot supply.
//
// Evidence is graded like scanConfirmsTrackedTree, but from an Lstat alone - no file is
// opened or hashed:
//   - a synced FILE baseline whose path resolves under newAbs to a regular file of the
//     SAME NON-ZERO size. A bare name match is weak (a foreign dataset easily holds a
//     "README.md" at the same relative path - exactly the shape this guards), so the
//     recorded size must agree; it comes free with the Lstat. Zero is excluded because it
//     discriminates nothing: every tree holds empty files (.gitkeep, __init__.py, a lock
//     file), so a 0 == 0 match would confirm a stranger's directory. A view tracking only
//     empty files therefore supplies no file evidence and keeps deferring - the
//     fail-toward-protected direction, mirroring scanConfirmsTrackedTree's empty-hash skip.
//   - only when the view tracks no synced FILE baseline at all (a directories-only view)
//     does a synced DIRECTORY resolving to a directory count - the strongest evidence
//     such a view can offer.
//
// The probe deliberately Lstats the joined path rather than folding names itself: on a
// case-insensitive filesystem the OS lookup already folds, and on a byte-preserving one a
// non-NFC spelling merely yields no evidence from that record (fail toward deferral).
// A record the stream skipped as corrupt supplies no evidence either.
func (e *Engine) newNameTreeConfirmed(viewID uint64, newAbs string) (confirmed, hasSynced bool, err error) {
	var fileProbes, dirProbes int
	var hashableSynced, dirMatch bool
	_, serr := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		if pb.State != store.StateSynced {
			return nil
		}
		hasSynced = true
		if pb.FileType == "dir" {
			// One match is all the directory grade can ever offer, so stop probing directories
			// once one matched or their own budget is spent - and never charge these Lstats to
			// the file budget below, which decides the strong grade.
			if dirMatch || dirProbes >= maxRenameAdoptProbes {
				return nil
			}
			dirProbes++
			if fi, lerr := os.Lstat(filepath.Join(newAbs, filepath.FromSlash(pb.Path))); lerr == nil && fi.IsDir() {
				dirMatch = true
			}
			return nil
		}
		hashableSynced = true
		if pb.LocalSize == 0 {
			// A zero-length record can never confirm (see the grading rules above: 0 == 0
			// discriminates nothing), so it must not charge the probe budget - nor waste an
			// Lstat. Charging it made the deferral PERMANENT rather than a retry: empty files
			// are ubiquitous and lexicographically early (.gitkeep, __init__.py, a lock file),
			// so 64 of them in path-byte order spend the budget before the record that WOULD
			// confirm is reached, and seeing them already set hashableSynced (retiring the
			// directory grade) - the baseline stream order is deterministic, so every later
			// pass reproduces the same verdict and the view's outbound sync stays suppressed
			// forever, while the operator has already applied the remedy the Warn's hint names.
			// This is the class the per-kind budget split closed for the directory arm, and the
			// reason scanConfirmsTrackedTree skips an emptyContentMD4 candidate BEFORE charging
			// maxDeviceConfirmHashes. hashableSynced deliberately stays set: the record IS a
			// synced file baseline, so the weak directory-only grade is correctly retired.
			return nil
		}
		if fileProbes >= maxRenameAdoptProbes {
			// The file budget is spent, and hashableSynced is now known - which retires the weak
			// directory-only grade - so no later record can change the answer. Reading on would
			// only be needed to DISCOVER hashableSynced, which this very record just supplied.
			return errStopBaselineScan
		}
		fileProbes++
		fi, lerr := os.Lstat(filepath.Join(newAbs, filepath.FromSlash(pb.Path)))
		if lerr != nil {
			return nil // absent (or unreadable): no evidence from this record
		}
		if fi.Mode().IsRegular() && fi.Size() == pb.LocalSize {
			confirmed = true
			return errStopBaselineScan
		}
		return nil
	})
	if serr != nil && !errors.Is(serr, errStopBaselineScan) {
		return false, false, serr
	}
	return confirmed || (dirMatch && !hashableSynced), hasSynced, nil
}

// adoptViewRename decides whether a server-side view rename (the stored oldName →
// the server's newName, both NFC-normalized, known non-empty and different) can be
// adopted safely, performing the on-disk directory move when that move is what
// makes it safe. It returns true when the caller may record newName as the view's
// name; false defers the adoption - the caller keeps oldName in force and
// suppresses the view's scan - after logging a Warn with the operator instruction
// (re-logged every scan pass by retryPendingViewRename).
//
// The decision matrix (baselines are view-relative, so they stay valid across a
// directory move and never need rewriting):
//
//   - fold-equal names (a case-only rename under case-insensitive matching): both
//     names address the same on-disk directory, so the rename is only a spelling
//     fix - attempted best-effort and adopted regardless of its outcome.
//   - old directory exists, new absent: move old → new (RenameNoReplace, so a
//     concurrently-appearing destination is never clobbered) and adopt.
//   - both exist: ambiguous - which tree is current cannot be decided here, so
//     defer until the operator leaves only one of them.
//   - old absent, new exists: the operator already moved the tree (the natural
//     manual fix) - adopt, but only once the directory under the new name carries
//     positive evidence of being the tracked tree (newNameTreeConfirmed). Without
//     that check the branch adopted on the mere EXISTENCE of a same-named
//     directory, asymmetric with the both-missing branch below, which defers on
//     exactly the unmounted-volume hypothesis: a view whose volume is unmounted
//     (old absent) plus any foreign directory that happens to carry the new name
//     re-pointed the scan at a stranger's tree, and - the non-empty scan running
//     with neither the empty-scan debounce nor, at the time, a device
//     guard - the very next pass emitted an OpRemove for every tracked file and
//     cross-uploaded the foreign content. A new-named directory
//     that is instead empty is covered by the standing empty-scan debounce +
//     device-id guard, like any other view root whose content vanished.
//   - neither exists: with no baselines there is nothing local to protect - adopt
//     (a fresh, never-materialised view). With baselines the old directory most
//     likely sits on an unmounted volume, and adopting would let the remount be
//     read as a mass delete - defer.
func (e *Engine) adoptViewRename(viewID uint64, oldName, newName string) bool {
	// A flat layout has no view directory: syncRoot/<name> is INSIDE the view's
	// own content. The whole decision matrix below is expressed over that
	// directory, so running it here either defers forever on a path that never
	// existed (freezing the view's sync), or - worse - renames a top-level folder
	// the user happens to have called after the view. Nothing needs moving:
	// baselines are view-relative and the sync root does not change, so adopt the
	// new name and touch no disk.
	if e.views.Flat {
		return true
	}

	oldRel, oldOK := viewNameSubdir(oldName)
	newRel, newOK := viewNameSubdir(newName)
	if !oldOK || !newOK {
		e.log.Warn("engine: view renamed on server but a name does not map to a local subdirectory; keeping the old name and suppressing this view's scan",
			"view", viewID, "old", oldName, "new", newName)
		return false
	}
	// The engine never creates symlinks, so a symlink along either directory path
	// could only redirect the move outside the sync root - refuse and defer, like
	// every other apply path (noSymlinkEscape).
	if err := e.noSymlinkEscape(oldRel, ""); err != nil {
		e.log.Warn("engine: view renamed on server but the old directory path is unsafe; keeping the old name and suppressing this view's scan",
			"view", viewID, "old", oldName, "new", newName, "err", err)
		return false
	}
	if err := e.noSymlinkEscape(newRel, ""); err != nil {
		e.log.Warn("engine: view renamed on server but the new directory path is unsafe; keeping the old name and suppressing this view's scan",
			"view", viewID, "old", oldName, "new", newName, "err", err)
		return false
	}
	oldAbs := filepath.Join(e.syncRoot, oldRel)
	newAbs := filepath.Join(e.syncRoot, newRel)
	if normpath.Key(oldName) == normpath.Key(newName) {
		// A case-only rename under case-insensitive matching: old and new address the
		// SAME directory (the engine's matching keys fold identically on such a
		// platform), so there is no mass-delete window - only the on-disk spelling to
		// fix, best-effort.
		if err := fsutil.Rename(oldAbs, newAbs); err != nil {
			e.log.Warn("engine: case-only view rename: adjusting the directory spelling failed; adopting the new name anyway",
				"view", viewID, "old", oldName, "new", newName, "err", err)
		}
		e.log.Info("engine: adopted case-only server view rename", "view", viewID, "old", oldName, "new", newName)
		return true
	}
	oldExists, err := lstatExists(oldAbs)
	if err != nil {
		e.log.Warn("engine: view renamed on server but the old directory cannot be inspected; keeping the old name and suppressing this view's scan",
			"view", viewID, "old", oldAbs, "err", err)
		return false
	}
	newExists, err := lstatExists(newAbs)
	if err != nil {
		e.log.Warn("engine: view renamed on server but the new directory cannot be inspected; keeping the old name and suppressing this view's scan",
			"view", viewID, "new", newAbs, "err", err)
		return false
	}
	switch {
	case oldExists && newExists:
		e.log.Warn("engine: view renamed on server but directories exist under BOTH names; keeping the old name and suppressing this view's scan",
			"view", viewID, "old", oldAbs, "new", newAbs,
			"hint", "merge the two directories and remove one of them; the engine adopts the new name once exactly one remains")
		return false
	case oldExists:
		if err := fsutil.RenameNoReplace(oldAbs, newAbs); err != nil {
			e.log.Warn("engine: view renamed on server; moving the local view directory failed; keeping the old name and suppressing this view's scan",
				"view", viewID, "from", oldAbs, "to", newAbs, "err", err)
			return false
		}
		e.log.Info("engine: view renamed on server; moved the local view directory to match",
			"view", viewID, "from", oldAbs, "to", newAbs)
		return true
	case newExists:
		confirmed, hasSynced, err := e.newNameTreeConfirmed(viewID, newAbs)
		if err != nil {
			e.log.Warn("engine: view renamed on server; inspecting the directory under the new name for tracked content failed; keeping the old name and suppressing this view's scan",
				"view", viewID, "old", oldName, "new", newName, "err", err)
			return false
		}
		if hasSynced && !confirmed {
			e.log.Warn("engine: view renamed on server and a directory carries the new name, but it holds none of this view's tracked files while the old directory is missing (unmounted volume with a foreign directory under the new name?); keeping the old name and suppressing this view's scan",
				"view", viewID, "old", oldAbs, "new", newAbs,
				"hint", "restore or remount the old directory (or move its content under the new name); the engine then adopts the rename")
			return false
		}
		e.log.Info("engine: view renamed on server; the local directory already carries the new name; adopting",
			"view", viewID, "old", oldName, "new", newName)
		return true
	default:
		has, err := e.viewHasBaselines(viewID)
		if err != nil {
			e.log.Warn("engine: view renamed on server; checking for tracked baselines failed; keeping the old name and suppressing this view's scan",
				"view", viewID, "old", oldName, "new", newName, "err", err)
			return false
		}
		if has {
			e.log.Warn("engine: view renamed on server but the old local directory is missing while files are still tracked (unmounted volume?); keeping the old name and suppressing this view's scan",
				"view", viewID, "old", oldAbs, "new", newAbs,
				"hint", "restore or remount the old directory (or move it to the new name); the engine then adopts the rename")
			return false
		}
		e.log.Info("engine: view renamed on server; nothing is materialised locally; adopting the new name",
			"view", viewID, "old", oldName, "new", newName)
		return true
	}
}

// lstatExists reports whether path exists (without following a final symlink),
// distinguishing a clean not-exist from a stat failure the caller must treat as
// "cannot decide".
func lstatExists(path string) (bool, error) {
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// retryPendingViewRename re-attempts adopting a deferred server-side view rename at
// the scan boundary: localScan skips a deferred view's scan and calls this instead,
// so the operator hint is re-logged and the adoption re-evaluated every pass. On
// success the new name is persisted and the deferral cleared; the caller still
// skips the view for the CURRENT pass, since the pass's shared dir cache and
// queued-op keys were built under the old name - the next pass scans cleanly under
// the new one.
func (e *Engine) retryPendingViewRename(viewID uint64, newName string) {
	rec, err := e.store.GetView(viewID)
	if err != nil {
		e.log.Warn("engine: reload view for a deferred rename failed; will retry next scan", "view", viewID, "err", err)
		return
	}
	if rec.Name == newName {
		delete(e.pendingViewRenames, viewID)
		return
	}
	// An empty stored name routes through the matrix with the numeric-id fallback
	// directory as the "old" one (viewRenameOldName), never adopted blindly.
	if !e.adoptViewRename(viewID, viewRenameOldName(rec, viewID), newName) {
		return
	}
	rec.Name = newName
	if err := e.store.PutView(viewID, rec); err != nil {
		e.log.Warn("engine: persisting an adopted view rename failed; will retry next scan", "view", viewID, "err", err)
		return
	}
	delete(e.pendingViewRenames, viewID)
}
