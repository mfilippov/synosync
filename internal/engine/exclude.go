package engine

// This file is selective sync: server paths the user excluded (`synosync exclude`,
// config.Views.Excluded) are invisible to the engine in both directions.
//
//   - The server walk neither reports nor enters them (walkServerSubtree), so a first sync
//     skips the subtree outright - on a NAS whose Photos holds the phone backups that is most
//     of the walk and most of the bytes.
//   - Their events are dropped before any batch pass reads them (scopeExcluded), and the cursor
//     still advances past them.
//   - The local scan neither uploads nor diffs them (scanViewFrom), and PROTECTS them, so a
//     baseline that outlived the exclusion reads as still-present, never as a local delete.
//
// Excluding never deletes. `exclude add` drops the subtree's baselines and queued ops in one
// store transaction (store.DropSubtree) and leaves the files where they are, untracked.
//
// Crossing the boundary is where the care is:
//
//   - A server move OUT of scope (included → excluded) is the node leaving this replica, so it
//     is applied as a delete of the source - through the ordinary remove gates, which keep any
//     local edit the base does not account for.
//   - A server move INTO scope (excluded → included) cannot be a create: a directory move emits
//     no events for its children, so the subtree would arrive empty. Its destination is queued
//     for a RESCAN instead (View.Rescan), as is a path whose exclusion is lifted: a walk of that
//     subtree merged with the disk by the first-sync rules (rescanPending), after which the
//     incremental poll carries on. The cursor is left alone; the walk runs on the loop before the
//     view's next poll, so everything the poll replays afterwards lands on the merged state.
//
// Exclusions are by PATH. A server rename of an excluded directory's ancestor moves the subtree
// to a path that is no longer excluded; the move is the ancestor's, so nothing rescans the
// subtree. That residual is recorded in docs/ENGINE.md.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// excluded reports whether rel (view-relative, slash-separated) is out of scope: under an
// excluded path, or with a component that is a system name or falls under a name rule. kind is
// what rel names when the caller knows; a files-only rule needs it to be KindFile.
func (e *Engine) excluded(viewID uint64, rel string, kind config.NodeKind) bool {
	return e.views.Excludes(viewID, rel, kind)
}

// kindOf is an event's node kind where the event can be trusted to say: a create or modify
// always reports it (opForEvent dispatches on IsDir alone), a move only when KindReported.
func kindOf(ev actions.Event) config.NodeKind {
	if ev.Action == "create" || ev.Action == "modify" || ev.KindReported {
		if ev.IsDir {
			return config.KindDir
		}
		return config.KindFile
	}
	return config.KindUnknown
}

// baselineKind is the kind the record at rel holds, or KindUnknown.
func (e *Engine) baselineKind(viewID uint64, rel string) config.NodeKind {
	b, err := e.store.GetBaseline(viewID, rel)
	switch {
	case err != nil:
		return config.KindUnknown
	case b.FileType == "dir":
		return config.KindDir
	default:
		return config.KindFile
	}
}

// pathUnder reports whether rel is root or lies beneath it, by fold key.
func pathUnder(rel, root string) bool {
	if root == "" {
		return true
	}
	k, r := normpath.Key(root), normpath.Key(rel)
	return r == k || strings.HasPrefix(r, k+"/")
}

// scopeExcluded drops the events that concern only excluded paths, rewrites a move out of scope
// as a delete of its source, and returns the destinations of moves into scope for a rescan. It
// runs right after normaliseBatch, before any batch-wide pass reads the events. seen is the
// batch's highest sync id: the cursor has to pass the events it drops as well.
func (e *Engine) scopeExcluded(viewID uint64, sorted []actions.Event) (out []actions.Event, rescans []string, seen int64) {
	// No shortcut for an empty exclusion list: the system names apply in every view.
	out = sorted[:0]
	for _, ev := range sorted {
		seen = max(seen, ev.SyncID)
		switch ev.Action {
		case "create", "modify", "delete":
			if e.excluded(viewID, ev.Path, kindOf(ev)) {
				continue
			}
		case "move":
			src := ev.FromPath
			if src == "" && ev.FileID != "" {
				// A sparse move names no source; the node's tracked path is its source.
				if p, err := e.store.PathByID(viewID, ev.FileID); err == nil {
					src = p
				}
			}
			// One node at both ends: the event's kind if it reports one, else the record's.
			kind := kindOf(ev)
			if kind == config.KindUnknown && src != "" {
				kind = e.baselineKind(viewID, src)
			}
			dstOut := e.excluded(viewID, ev.Path, kind)
			srcOut := src != "" && e.excluded(viewID, src, kind)
			switch {
			case dstOut && (srcOut || src == ""):
				continue
			case dstOut:
				e.log.Info("engine: a server move took a node into an excluded path; removing it here",
					"view", viewID, "from", src, "to", ev.Path, "sync_id", ev.SyncID)
				ev.Action, ev.Path, ev.FromPath, ev.FromFileID = "delete", src, "", ""
			case srcOut:
				if containable(ev.Path) {
					e.log.Info("engine: a server move brought a node in from an excluded path; rescanning it",
						"view", viewID, "from", src, "to", ev.Path, "sync_id", ev.SyncID)
					rescans = append(rescans, ev.Path)
				}
				continue
			}
		}
		out = append(out, ev)
	}
	return out, rescans, seen
}

// addRescans records paths for rescanPending. It runs before the batch's cursor can advance, so
// a crash between the two re-pulls the move rather than losing the subtree.
func (e *Engine) addRescans(viewID uint64, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	v, err := e.store.GetView(viewID)
	if err != nil {
		return fmt.Errorf("engine: load view %d to record a rescan: %w", viewID, err)
	}
	v.Rescan = store.MergeRescans(v.Rescan, paths)
	if err := e.store.PutView(viewID, v); err != nil {
		return fmt.Errorf("engine: record rescan for view %d: %w", viewID, err)
	}
	return nil
}

// rescanPending walks and merges every recorded rescan of every synced view, before the
// incremental poll touches the view. A view still waiting for its first sync is skipped: the
// cold start covers its whole tree and clears the list. A failed rescan stays recorded and is
// retried next cycle; only a terminal error is returned.
func (e *Engine) rescanPending(ctx context.Context) error {
	views, err := e.store.ListViews()
	if err != nil {
		return fmt.Errorf("engine: list views for rescan: %w", err)
	}
	for _, lv := range views {
		if !lv.InitialSyncDone || len(lv.Rescan) == 0 || e.viewSuppressed(lv.ViewID) {
			continue
		}
		for _, p := range lv.Rescan {
			if err := ctx.Err(); err != nil {
				return err
			}
			if p != "" && e.excluded(lv.ViewID, p, config.KindUnknown) {
				// Kept, not dropped: `exclude remove` records the rescan BEFORE it lifts the
				// exclusion, so a crash between the two leaves a rescan that waits here for the
				// rerun rather than one that is lost. `exclude add` clears entries under its path.
				continue
			}
			// "" is the whole view: lifting a NAME rule can bring back files anywhere in the tree.
			if p == "" || containable(p) {
				if err := e.rescanSubtree(ctx, lv, p); err != nil {
					if isTerminal(err) {
						return err
					}
					if errors.Is(err, errColdStartPaused) {
						return nil
					}
					e.log.Warn("engine: rescan failed; will retry next cycle", "view", lv.ViewID, "path", p, "err", err)
					continue
				}
			}
			if err := e.dropRescan(lv.ViewID, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// rescanSubtree merges one subtree with the server by the first-sync rules: a walk of it (from
// its parent, so the subtree root itself is planned too) against a fully hashed scan of it.
func (e *Engine) rescanSubtree(ctx context.Context, lv store.ListView, p string) error {
	if p == "" {
		e.log.Info("engine: rescanning the whole view (a name rule was lifted)", "view", lv.ViewID)
	} else {
		e.log.Info("engine: rescanning a subtree", "view", lv.ViewID, "path", p)
	}
	scan := func() (*coldPlanner, error) {
		local, unreadable, err := e.scanViewFrom(ctx, lv.ViewID, p, nil)
		if err != nil {
			return nil, fmt.Errorf("engine: rescan scan view %d %q: %w", lv.ViewID, p, err)
		}
		return e.newScopedColdPlanner(lv.ViewID, local, unreadable, isReadOnlyView(lv.Privilege), p == "")
	}
	pl, err := e.streamColdStartFrom(ctx, lv.ViewID, p, scan)
	if err != nil {
		if errors.Is(err, actions.ErrPathError) {
			// The subtree's parent is gone from the server: there is nothing to merge, and the
			// parent's own delete reaches this replica through the poll.
			e.log.Info("engine: rescan found the subtree's parent gone from the server", "view", lv.ViewID, "path", p)
			return nil
		}
		return err
	}
	if err := pl.finish(); err != nil {
		return fmt.Errorf("engine: rescan view %d %q: %w", lv.ViewID, p, err)
	}
	e.log.Info("engine: rescan complete", "view", lv.ViewID, "path", p, "nodes", pl.nodes)
	return nil
}

// dropRescan removes one completed rescan from the view's list.
func (e *Engine) dropRescan(viewID uint64, p string) error {
	v, err := e.store.GetView(viewID)
	if err != nil {
		return fmt.Errorf("engine: load view %d to clear a rescan: %w", viewID, err)
	}
	v.Rescan = slices.DeleteFunc(v.Rescan, func(h string) bool { return h == p })
	if err := e.store.PutView(viewID, v); err != nil {
		return fmt.Errorf("engine: clear rescan for view %d: %w", viewID, err)
	}
	return nil
}

// nodeKindOf is the kind of a node whose directory-ness is known for certain (a listing or the
// disk walk reports it).
func nodeKindOf(isDir bool) config.NodeKind {
	if isDir {
		return config.KindDir
	}
	return config.KindFile
}
