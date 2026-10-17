package engine

// The nested-unmount guard.
//
// The empty-scan debounce protects a view ROOT that goes empty (localreconcile.go). A mount
// point one level down had nothing: `<view>/Photos` on a USB disk, an NFS export or a bind
// mount reads, once unmounted, as an ordinary empty directory. detectRemoves then diffs every
// baseline beneath it as vanished and emits an OpRemove per child; each precondition passes,
// because "the server still holds exactly what the baseline recorded" is precisely what an
// untouched subtree looks like; and `remove` is RECURSIVE, so the subtree goes on the server and
// then everywhere. Nothing about that sequence is a bug in any single gate - the deletions are
// perfectly well evidenced. The premise is what is false: nobody deleted anything.
//
// The first discriminator is the DIRECTORY ITSELF. Deleting a folder removes the folder:
// `rm -rf dir` leaves dir ABSENT, and the scan surfaces no entry for it. An unmount leaves it
// PRESENT and EMPTY. So a directory that is still there, has no children on disk, and has synced
// baselines recording children is worth a second look before its subtree is deleted on the server.
//
// That shape is also what the commonest deletion of all produces: removing the last file in a
// folder, or `rm -rf dir/*`. Charging those the debounce cost two minutes per emptied folder
// (measured on a live stand, 11.10.2026: every scenario that deleted a folder's last entry failed
// its budget). The second discriminator separates them: the directory's IDENTITY - (st_dev,
// st_ino) on Unix, (volume serial, file index) on Windows. An unmount always changes the object
// at the path, since the covered directory is another inode (a bind mount from the same
// filesystem included, which the device check cannot see); emptying a folder leaves the same
// object in place. So an emptied directory that is the very object this run saw populated is
// propagated at once (noteDirIdentities), and only one without that evidence - first seen after
// a restart, or whose identity cannot be read - waits for the debounce.
//
// The evidence rule is copied from that guard deliberately: a COUNT of scans is not a duration
// (a watcher wake resets the scan throttle, and an unmount's own fsnotify burst delivers several
// scans within seconds), so a counted observation also needs emptyScanConfirmMinSpacing of wall
// clock since the last one. In-memory, like the view-root streak's own spacing state: a daemon
// restart starts the run again, which delays a genuine empty-out and never shortens the
// evidence for one.

import (
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/store"
)

// emptiedDirKey identifies one directory's streak.
type emptiedDirKey struct {
	viewID uint64
	path   string
}

// emptiedDirs returns the view-relative paths of directories the scan surfaced as PRESENT while
// surfacing nothing beneath them.
//
// Only directories that a baseline records children under can matter, so the caller passes the
// rows and this walks them once: a present directory becomes a candidate, and any present entry
// under some prefix clears that prefix.
func emptiedDirs(rows []joinRow) map[string]bool {
	candidates := make(map[string]bool)
	for _, r := range rows {
		if r.scan != nil && r.scan.isDir {
			candidates[r.scan.relPath] = true
		}
	}
	if len(candidates) == 0 {
		return candidates
	}
	for _, r := range rows {
		if r.scan == nil {
			continue
		}
		// Every present entry proves each of its ancestors is non-empty.
		for _, anc := range ancestorPaths(r.scan.relPath) {
			delete(candidates, anc)
		}
	}
	return candidates
}

// ancestorPaths lists the strict ancestor directories of a view-relative slash path, nearest
// first. The view root ("") is not one: its emptiness is the view-root guard's subject, and
// counting it here would run two debounces over the same evidence.
func ancestorPaths(p string) []string {
	var out []string
	for {
		i := strings.LastIndex(p, "/")
		if i <= 0 {
			return out
		}
		p = p[:i]
		out = append(out, p)
	}
}

// suspectEmptiedDirs folds this pass's observations into the streaks and returns the set of
// directories whose emptiness is NOT yet corroborated. A remove of anything beneath one of them
// is deferred.
//
// A directory that is empty but records no synced children is not a candidate at all: there is
// nothing under it to delete, so there is nothing to protect and no reason to keep a streak.
func (e *Engine) suspectEmptiedDirs(viewID uint64, rows []joinRow, threshold int) map[string]bool {
	empty := emptiedDirs(rows)
	inPlace := e.noteDirIdentities(viewID, rows, empty)
	withChildren := make(map[string]bool, len(empty))
	for _, r := range rows {
		if r.base == nil || r.base.State != store.StateSynced {
			continue
		}
		for _, anc := range ancestorPaths(r.base.Path) {
			if empty[anc] {
				withChildren[anc] = true
			}
		}
	}

	// Retire the streaks of directories that are no longer in the suspicious state, so a mount
	// that came back does not leave a half-finished run to be completed by a later unmount.
	for k := range e.emptiedDirScans {
		if k.viewID == viewID && !withChildren[k.path] {
			delete(e.emptiedDirScans, k)
		}
	}

	suspect := make(map[string]bool, len(withChildren))
	for dir := range withChildren {
		if inPlace[dir] {
			// The same directory object this run saw holding content: emptied, not uncovered.
			delete(e.emptiedDirScans, emptiedDirKey{viewID: viewID, path: dir})
			continue
		}
		if !e.noteEmptiedDirScan(viewID, dir, threshold) {
			suspect[dir] = true
		}
	}
	return suspect
}

// noteDirIdentities records the identity of every directory this scan saw NON-empty, and
// returns the emptied directories that still carry the identity they had when last seen
// holding content - directories emptied in place.
//
// That is the discriminator the debounce otherwise has to stand in for. An unmount always
// changes the object at the path: the directory the mount covered is a different inode, and
// usually on a different device, so a bind mount from the same filesystem - invisible to the
// device check (statdev_unix.go) - is caught too. Emptying a directory leaves the same object in
// place. So an identity match is positive evidence of a deletion, and anything short of one - a
// directory first seen empty after a restart, an identity that cannot be read - keeps the debounce.
//
// Entries for directories absent from this scan are dropped, so the memory is bounded by the
// directories that currently exist and a deleted-and-recreated directory starts unknown.
func (e *Engine) noteDirIdentities(viewID uint64, rows []joinRow, empty map[string]bool) map[string]bool {
	if e.dirSeenNonEmpty == nil {
		e.dirSeenNonEmpty = make(map[emptiedDirKey]fileIdent)
	}
	// The view's absolute directory, resolved only if some directory needs its identity read
	// by path - never on Unix, where the scan already carries every identity.
	var viewAbs string
	var viewResolved bool
	identityByPath := func(rel string) (fileIdent, bool) {
		if !viewResolved {
			viewResolved = true
			if _, abs, err := e.resolveViewDir(viewID); err == nil {
				viewAbs = abs
			}
		}
		if viewAbs == "" {
			return fileIdent{}, false
		}
		id, err := osDirIdentity(filepath.Join(viewAbs, filepath.FromSlash(rel)))
		return id, err == nil
	}

	present := make(map[string]bool)
	inPlace := make(map[string]bool)
	for _, r := range rows {
		if r.scan == nil || !r.scan.isDir {
			continue
		}
		rel := r.scan.relPath
		present[rel] = true
		key := emptiedDirKey{viewID: viewID, path: rel}
		prev, seen := e.dirSeenNonEmpty[key]
		if !empty[rel] {
			// Refresh when the scan supplies the identity for free; otherwise read it once per
			// run, the first time the directory is seen populated.
			if r.scan.hasDirID {
				e.dirSeenNonEmpty[key] = r.scan.dirID
			} else if !seen {
				if id, ok := identityByPath(rel); ok {
					e.dirSeenNonEmpty[key] = id
				}
			}
			continue
		}
		if !seen {
			continue // never seen populated this run: no evidence either way
		}
		cur, ok := r.scan.dirID, r.scan.hasDirID
		if !ok {
			cur, ok = identityByPath(rel)
		}
		if ok && cur == prev {
			inPlace[rel] = true
		}
	}
	for k := range e.dirSeenNonEmpty {
		if k.viewID == viewID && !present[k.path] {
			delete(e.dirSeenNonEmpty, k)
		}
	}
	return inPlace
}

// noteEmptiedDirScan folds one observation into a directory's streak and reports whether the
// emptiness is now corroborated well enough to act on.
func (e *Engine) noteEmptiedDirScan(viewID uint64, dir string, threshold int) bool {
	key := emptiedDirKey{viewID: viewID, path: dir}
	now := e.now()
	st, tracked := e.emptiedDirScans[key]
	if tracked && now.Sub(st.counted) < emptyScanConfirmMinSpacing {
		// Repeated inside the floor: the same state seen twice, not two observations of a
		// persisting one. The entry is left in place so the next scan past the floor advances
		// the run rather than restarting it.
		return st.count >= threshold
	}
	st = emptyScanStreak{count: st.count + 1, counted: now}
	e.emptiedDirScans[key] = st
	if st.count >= threshold {
		e.log.Warn("engine: directory confirmed empty across scans spanning the debounce floor; propagating its deletions",
			"view", viewID, "path", dir, "consecutive_empty_scans", st.count, "min_spacing", emptyScanConfirmMinSpacing)
		return true
	}
	e.log.Warn("engine: directory is present but empty while its contents are still recorded; deferring their deletion pending confirmation (an unmounted mount point looks exactly like this)",
		"view", viewID, "path", dir, "consecutive_empty_scans", st.count, "threshold", threshold)
	return false
}

// underSuspectDir reports whether p lies beneath a directory whose emptiness is unconfirmed.
func underSuspectDir(suspect map[string]bool, p string) bool {
	if len(suspect) == 0 {
		return false
	}
	for _, anc := range ancestorPaths(p) {
		if suspect[anc] {
			return true
		}
	}
	return false
}
