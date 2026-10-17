package engine

// This file makes a server name this host cannot hold OBSERVABLE.
//
// containable() refuses a server path whose name the local filesystem cannot spell - on
// Windows a component carrying <>:"|?*, a reserved device name, or a trailing dot or space
// (namemangle.go). That skip is the safe thing: no op, no baseline, so nothing is ever pushed
// back. But it used to be only a log line per path, so a user with "Report | 2025.pdf" on the
// NAS saw a synced tree that silently lacked the file. Like a fold-key collision
// (collisiontrace.go) it is skipped work with no queued op, so it gets the same bare
// dead-letter: `status` counts it under Dropped and `synosync drops` names it.
//
// The record names the FIRST unrepresentable component: a folder called "a|b" holding 10,000
// files is one thing to rename, not 10,000. Recording is once per (view, that path) via a memo
// seeded from the durable ring, and at most unrepresentableTraceCap records per view per
// process, past which one summary record stands for the rest - the ring holds 1000 traces and
// every other one is a genuine dropped op.

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// unrepresentableTraceCap bounds how many individual unrepresentable-name traces one process
// writes per view (see collisionTraceBurstCap for the same ring arithmetic).
const unrepresentableTraceCap = 32

// unrepresentablePrefix returns the view-relative slash path up to and including the first
// component this host cannot hold as a name, or "" when p has none - an empty, absolute or
// ".."-escaping path is a containment failure, not a name the user can fix by renaming.
func unrepresentablePrefix(p string) string {
	if p == "" || strings.HasPrefix(p, "/") {
		return ""
	}
	parts := strings.Split(p, "/")
	for i, c := range parts {
		switch c {
		case "", ".", "..":
			return ""
		}
		// filepath.IsLocal on a single component rejects what the host reserves (a colon or
		// a device name like NUL on Windows); componentHostRepresentable adds what Win32
		// refuses or silently strips; a literal backslash is a separator on Windows.
		if !filepath.IsLocal(c) || !componentHostRepresentable(c) || strings.ContainsRune(c, filepath.Separator) {
			return strings.Join(parts[:i+1], "/")
		}
	}
	return ""
}

// skipUncontainableServerPath logs and, when the path is a name the user could fix, durably
// traces a server path containable() refused. pass names the caller for the log.
func (e *Engine) skipUncontainableServerPath(viewID uint64, pass, p string) {
	prefix := unrepresentablePrefix(p)
	if prefix == "" {
		e.log.Warn("engine: skipping a server path that escapes the sync root",
			"view", viewID, "pass", pass, "path", p)
		return
	}
	e.seedUnrepresentableNoted(viewID)
	noted := e.unrepresentableNoted[viewID]
	if noted == nil {
		noted = make(map[string]bool)
		e.unrepresentableNoted[viewID] = noted
	}
	if noted[prefix] {
		return
	}
	dl := store.DeadLetter{
		Type:       store.OpUnrepresentableName,
		Direction:  store.DirInbound,
		ViewID:     viewID,
		Path:       prefix,
		RecordedAt: e.now().Unix(),
		Reason: "the server name is not valid on this computer's filesystem (Windows refuses " +
			`<>:"|?*, reserved names like NUL, and a trailing dot or space); it and anything ` +
			"under it are not synced here - rename it on the server to sync it",
	}
	if e.unrepresentableTraced[viewID] >= unrepresentableTraceCap {
		if e.unrepresentableTraced[viewID] > unrepresentableTraceCap {
			noted[prefix] = true // the summary is already durable
			e.log.Warn("engine: skipping a server name this filesystem cannot hold; rename it on the server to sync it",
				"view", viewID, "pass", pass, "path", prefix)
			return
		}
		dl.Path = ""
		dl.Reason = fmt.Sprintf("further server names not valid on this computer's filesystem "+
			"were skipped beyond the first %d traced; the daemon log names each one", unrepresentableTraceCap)
	}
	if _, err := e.store.RecordDeadLetter(dl); err != nil {
		e.log.Warn("engine: cannot record the skipped server name; it will be retried",
			"view", viewID, "path", prefix, "error", err)
		return
	}
	e.unrepresentableTraced[viewID]++
	noted[prefix] = true
	e.log.Warn("engine: skipping a server name this filesystem cannot hold; rename it on the server to sync it",
		"view", viewID, "pass", pass, "path", prefix)
}

// seedUnrepresentableNoted primes the memo from the durable ring once per view per process,
// so a restart (or the next cold start) does not re-trace every still-unfixed name.
func (e *Engine) seedUnrepresentableNoted(viewID uint64) {
	if e.unrepresentableSeeded[viewID] {
		return
	}
	traces, err := e.store.ListDeadLetters()
	if err != nil {
		e.log.Warn("engine: cannot read the dead-letter ring to seed the skipped-name memo",
			"view", viewID, "error", err)
		return
	}
	noted := make(map[string]bool)
	for _, t := range traces {
		if t.ViewID == viewID && t.Type == store.OpUnrepresentableName && t.Path != "" {
			noted[t.Path] = true
		}
	}
	e.unrepresentableNoted[viewID] = noted
	e.unrepresentableSeeded[viewID] = true
}

// skipUncontainableEvent is skipUncontainableServerPath for an incremental event: a delete of
// a name this client never held has nothing to report, and a move is traced at whichever
// endpoint it cannot hold (a move OFF such a name was already rescued as a create).
func (e *Engine) skipUncontainableEvent(viewID uint64, ev actions.Event) {
	switch {
	case ev.Action == "delete":
		e.log.Info("engine: ignoring the delete of a server path this client cannot hold",
			"view", viewID, "sync_id", ev.SyncID, "path", ev.Path)
	case ev.Action == "move" && containable(ev.Path):
		e.skipUncontainableServerPath(viewID, "incremental", ev.FromPath)
	default:
		e.skipUncontainableServerPath(viewID, "incremental", ev.Path)
	}
}
