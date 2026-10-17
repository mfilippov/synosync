package engine

// This file defines the serialisation invariant the executor's scheduler enforces
// (DESIGN §6, §13): which durable ops must apply in order and which may run
// concurrently. Each op declares the path(s) it touches (a rename touches both its
// source and its destination), resolved to the location under the sync root the op
// actually applies to - the view's on-disk directory joined with the view-relative
// path - so two views that map to the same directory (a duplicate or colliding view
// name) serialise rather than racing on the same files. Two ops conflict when one's
// resolved path equals, contains or is contained by the other's; conflicting ops
// apply in FIFO (enqueue, i.e. ascending sync_id) order while independent paths
// proceed concurrently. The invariant lives here in one place so the scheduler in
// executor.go and the outbound reconcile in localreconcile.go share a single
// definition of "these two ops touch overlapping locations".

import (
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// serializeKeys is the set of conflict keys an op is serialised on, each the op's path
// resolved under the sync root (the view's on-disk directory joined with the
// view-relative path) so collisions are computed against the location actually written,
// not the abstract view id. Every op touches its own path; a rename additionally
// touches its destination, so it is ordered against ops on either endpoint (and on any
// ancestor or descendant of them).
func serializeKeys(viewDir string, op store.Op) []string {
	keys := []string{cleanKey(viewDir, op.Path)}
	if op.Type == store.OpRename && op.NewPath != op.Path {
		keys = append(keys, cleanKey(viewDir, op.NewPath))
	}
	return keys
}

// opSerializeKeys is the scheduler's key resolver: the pure serializeKeys set plus - for an
// inbound download carrying a file id whose node is tracked at a DIFFERENT path than the op
// records - the byid-resolved CURRENT path (the narrow adoption of the
// "wider keys" option recorded in the decision block below). applyDownload may redirect such a
// download to that current path (the self-echo-move relocation, downloadRelocatedBySelfEcho),
// and a redirect target outside the declared key set would let the download mutate a location
// the scheduler never ordered it against - the structural violation of the invariant above.
// With the extra key the redirected write is always covered; for a non-redirected download the
// extra key only over-serialises (conservative, never unsafe). The store read runs once per
// download op per drain, on the scheduling goroutine; an untracked id adds nothing, and a
// transient store failure degrades to the base keys with a Warn - the apply-time redirect
// re-resolves the path itself and keeps the op queued on its own store failure, so the
// degradation is bounded to one drain's ordering being the historical one.
func (e *Engine) opSerializeKeys(viewDir string, op store.Op) []string {
	keys := serializeKeys(viewDir, op)
	if op.Direction != store.DirInbound || op.Type != store.OpDownload || op.FileID == "" {
		return keys
	}
	cur, err := e.store.PathByID(op.ViewID, op.FileID)
	switch {
	case isNotFound(err):
	case err != nil:
		e.log.Warn("engine: resolving a download's current path for serialization failed; scheduling on the enqueue-time path only",
			"view", op.ViewID, "file_id", op.FileID, "path", op.Path, "err", err)
	case cur != op.Path:
		keys = append(keys, cleanKey(viewDir, cur))
	}
	return keys
}

// cleanKey joins a view's on-disk directory with a view-relative server path
// (slash-separated) and normalises the result into an anchored, cleaned form suitable
// for prefix-based ancestor tests across views. It cleans with the OS path semantics
// the apply itself uses (filepath, via FromSlash) rather than slash-only cleaning, so a
// serialisation key matches the disk location the write actually lands on: on Windows,
// where the backslash is also a separator, server paths like "a/b" and "a\b" resolve to
// the same file and so must produce the same key and serialise rather than racing.
//
// The cleaned key is then folded (normpath.Fold) - NFC on every platform, plus a
// case-fold on case-insensitive filesystems (Windows/macOS) - so two ops whose on-disk
// paths differ only in case ("Foo" vs "foo") or Unicode encoding (NFD vs NFC) collide on
// the same key and serialise rather than racing on one file. The key is used only for
// conflict serialisation, never for disk access (abs() resolves the real path), so
// folding it cannot redirect a write.
func cleanKey(viewDir, p string) string {
	sep := string(filepath.Separator)
	cleaned := filepath.Clean(sep + filepath.FromSlash(viewDir) + sep + filepath.FromSlash(p))
	return normpath.Fold(cleaned)
}

// keysConflict reports whether any key in a overlaps any key in b: a resolved path that
// equals, contains or is contained by the other. Conflicting ops must apply in enqueue
// order; non-conflicting ops may run concurrently.
func keysConflict(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if pathsOverlap(x, y) {
				return true
			}
		}
	}
	return false
}

// pathsOverlap reports whether two anchored, cleaned paths name the same node or one
// contains the other.
func pathsOverlap(a, b string) bool {
	return a == b || isAncestor(a, b) || isAncestor(b, a)
}

// isAncestor reports whether anc is a strict ancestor directory of desc, both in
// anchored, cleaned form (OS separators, as cleanKey produces). The synthetic root (a
// lone separator) is an ancestor of every other path.
func isAncestor(anc, desc string) bool {
	sep := string(filepath.Separator)
	if anc == sep {
		return desc != sep
	}
	return strings.HasPrefix(desc, anc+sep)
}

// ── byid-resolved side effects are outside the conflict-key set (accepted invariant gap) ──
//
// serializeKeys declares only the op's own server path(s): op.Path, plus op.NewPath for a
// rename. Five helpers (four inbound, one outbound) additionally touch a *byid-resolved*
// location P that is NOT among those keys - the node's still-reachable prior/relocated path
// (a rejected rename left it there, or an inbound ancestor rename relocated it), found through
// the stable file id rather than op.Path:
//
//   - removeRelocatedNode / dropRemoveBaselineAt (inbound_remove.go, deadletter.go): an
//     inbound remove whose op.Path holds nothing removes the file and drops the baseline at
//     the byid-resolved path instead.
//   - removeRelocatedPriorFile (inbound_remove.go): before markSynced re-points the index,
//     deletes the stale on-disk file the node left at its byid-resolved prior path.
//   - relocateTrackedDirForMkdir (inbound_mkdir.go): renames a tracked directory's subtree
//     from its byid-resolved old path to op.Path (only op.Path is a serialisation key).
//   - revertRelocatedRemove (outbound_mutate.go): an outbound OpRemove
//     whose target node an inbound ancestor rename already relocated server-side reverts the
//     baseline at the byid-resolved CURRENT path back to StateSynced (store-only, NO disk
//     mutation) so the next scan re-emits the remove on the new path.
//
// Because P is not in serializeKeys, drainBatch does NOT chain this op behind a concurrent
// op in the same batch whose own path is P - the two can run in parallel. This is a real
// gap in the stated invariant, accepted here rather than closed, with the convergence
// argument below.
//
// DECISION: document the exception (this block + a note at each of
// the four sites) rather than resolve byid at planning time. The alternative - feeding a
// store.PathByID lookup for every op carrying a file id into serializeKeys - would put a
// store read on the scheduler's hot path (run once per op, every drain) to serialise a rare,
// self-healing anomaly (a rejected-rename relocation is off the common apply path). The
// narrower doc option was chosen; the wider keys option is recorded for a future gate that
// needs it. The wider option was later adopted NARROWLY, for exactly one op shape:
// an inbound OpDownload carrying a file id (opSerializeKeys above) - the one op whose apply
// path REBASES its own target to the byid-resolved location (the self-echo-move redirect),
// so the resolved path is not merely a side effect's address but the op's actual write
// target. The four helpers below remain covered by the documented exception.
//
// CONVERGENCE - every traced interleaving degrades transient or direction-safe, never a
// silent loss (no concrete data-loss interleaving was found; this is why the gap is
// accepted, not why it is harmless in some future variant - see the caveat):
//
//  1. The delete-side helpers all run the cluster-A keep-both guard (preserveRelocatedDivergence)
//     BEFORE any fsutil.Remove, so on-disk content at P that diverges from - or cannot be
//     verified against - its baseline is set aside as a _Conflict copy first. A concurrent
//     write racing in at P is therefore preserved, never destroyed: the never-lose direction.
//  2. Every store mutation is one atomic bbolt transaction, so a baseline drop/relocation
//     racing a concurrent write at P cannot corrupt the index; the worst residue is a stale
//     baseline the next authoritative localScan re-derives (a duplicate re-upload that
//     self-heals), never a silent delete.
//  3. relocateTrackedDirForMkdir reaches its disk rename only when op.Path (a serialisation
//     key, hence serialised) is appliable and the byid-resolved old path still holds the
//     node's own subtree; moving it to op.Path leaves at worst the source re-scanned as
//     vanished and re-tracked by file id - again self-healing.
//  4. revertRelocatedRemove performs NO disk mutation at P - it only flips a StatePending
//     baseline back to StateSynced (idempotent; an already-synced or dropped record is a
//     no-op), so it cannot destroy on-disk content racing in at P. The worst residue is the
//     next localScan re-emitting an OpRemove that the authoritative apply path re-guards
//     (its never-lose keep-both check runs against live baselines) - self-healing, never a
//     silent loss: the outbound direction-safe half of the same relocation the inbound
//     helpers above cover.
//  5. The two OUTBOUND relocation arms in outbound.go are store-only for the same reason and
//     carry the same argument verbatim: revertRelocatedOutbound (the uploadNodeRelocatedError
//     handler) and dropGoneParentBaseline's relocation branch both act on a byid-resolved
//     current path, and neither touches disk - they drop or revert a record, so the worst
//     residue is again a re-derived op the authoritative apply path re-guards.
//
// CAVEAT for future work: any NEW destructive gate added to these helpers that acts on P
// must either add P to serializeKeys (resolving byid at planning time, the alternative above)
// or independently justify its own direction-safety - the scheduler gives it no ordering
// guarantee against a concurrent op that owns P.
