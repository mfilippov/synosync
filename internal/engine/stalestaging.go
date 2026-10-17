package engine

import (
	"io/fs"
	"sync"
	"time"
)

// Leftover download staging temps.
//
// promoteStagedDownload removes its staging temp on every exit path, but a process that
// is killed or crashes mid-download never reaches that defer, so the hidden
// ".<base>.dl-<digits>" file stays behind - for a large download, gigabytes of partial
// content. The scanner already refuses to surface such a name as user content, so the
// leftover is never pushed; what it lacked was any way to reach a person.
//
// It is reported, never deleted. isStagingName matches the engine's generated shape
// precisely, but a user file can carry exactly that name, and nothing on disk proves
// which one a given file is; removing content of unproven provenance is what the
// never-lose doctrine forbids. So the scanner logs one Warn per leftover per run, naming
// the path and size, and the decision stays with the user.
//
// "Leftover" needs two facts, because neither alone is proof. The file must not be a
// temp THIS run created and still holds (stagingTracker.live): a download in flight is
// mid-fill, not abandoned. And its mtime must predate this engine (startedAt): between
// CreateTemp returning and the temp being registered there is a window in which a live
// temp is not yet in the set, and in that window its mtime is "now". The mtime test alone
// would not do either: promoteStagedDownload stamps the SERVER's mtime on the temp before
// the promote, so a live temp briefly looks arbitrarily old.

// stagingTracker records the staging temps this run holds and the leftovers already
// reported. Downloads run on parallel workers while the scanner runs on the loop, so it
// carries its own lock.
type stagingTracker struct {
	mu sync.Mutex
	// startedAt is when the engine was built; a leftover's mtime predates it.
	startedAt time.Time
	live      map[string]struct{}
	warned    map[string]bool
}

func newStagingTracker(now time.Time) *stagingTracker {
	return &stagingTracker{startedAt: now, live: make(map[string]struct{}), warned: make(map[string]bool)}
}

// hold registers a staging temp this run has just created; the returned func releases it
// and must run only after the temp has been removed or promoted.
func (t *stagingTracker) hold(abs string) (release func()) {
	t.mu.Lock()
	t.live[abs] = struct{}{}
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		delete(t.live, abs)
		t.mu.Unlock()
	}
}

// leftover reports whether the staging-named file at abs, last modified at mtime, was
// left by an earlier run and has not been reported yet; a true return latches the report.
func (t *stagingTracker) leftover(abs string, mtime time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.live[abs]; ok || t.warned[abs] || !mtime.Before(t.startedAt) {
		return false
	}
	t.warned[abs] = true
	return true
}

// noteStagingName is the scanner's hook for a staging-named regular file at abs: it logs
// the leftover once, and does nothing for a temp a download of this run still holds.
func (e *Engine) noteStagingName(viewID uint64, abs string, d fs.DirEntry) {
	info, err := d.Info()
	if err != nil {
		return // gone between the readdir and now - a promote or a cleanup, either way not a leftover
	}
	if !e.staging.leftover(abs, info.ModTime()) {
		return
	}
	e.log.Warn("engine: leftover partial download from an interrupted run; it is never synced and can be deleted",
		"view", viewID, "path", abs, "bytes", info.Size())
}
